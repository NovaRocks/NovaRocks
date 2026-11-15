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

//! One logical operation owns its I/O, immutable object registrations, and cleanup.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use crate::access_binding::IcebergReadBinding;
use crate::fs_io::{
    IcebergWriteAdmission, StreamExitFailure, WritePermit, build_admitted_file_io_for_location,
};
use crate::iceberg::io::FileIO;
use crate::iceberg::spec::TableMetadata;
use crate::iceberg::{Error, ErrorKind, Result};
use crate::resources::IcebergCatalogRuntime;
use futures::StreamExt;
use novarocks_fs::FileCancellation;
use novarocks_spi::connector::{ConnectorRequestContext, ConnectorStopOwner};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::model::{
    ArtifactKind, ArtifactLedger, ArtifactOrigin, ArtifactRecord, ArtifactWriter, AttemptArtifacts,
    AttemptToken, CleanupRemainingReason, CleanupScope, IcebergCleanupReport, ObjectIdentity,
    OperationToken, ReceivedPublication, RemainingArtifact,
};

#[derive(Clone, Copy, Debug)]
pub(crate) struct CleanupBudget {
    pub time: Duration,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct OperationLimits {
    pub io_requests: u32,
    pub cleanup: CleanupBudget,
}

impl Default for OperationLimits {
    fn default() -> Self {
        Self {
            io_requests: 8,
            cleanup: CleanupBudget {
                time: Duration::from_secs(30),
            },
        }
    }
}

#[derive(Clone)]
// Design: ADR-0171 (docs/adr/ADR-0171-commit-operation-model.md)
pub(crate) struct IcebergCommitOperation {
    inner: Arc<OperationState>,
}

struct OperationState {
    token: OperationToken,
    location: String,
    binding: IcebergReadBinding,
    request: ConnectorRequestContext,
    preparation: OnceLock<ConnectorRequestContext>,
    cancellation: FileCancellation,
    runtime: IcebergCatalogRuntime,
    permits: Arc<Semaphore>,
    limits: OperationLimits,
    artifacts: Mutex<OperationArtifacts>,
    exit_failures: Mutex<BTreeMap<ObjectIdentity, StreamExitFailure>>,
    attempt_ordinal: AtomicU32,
    object_ordinal: AtomicU64,
    // The original payload pin follows retained journal/artifact backing; the
    // process cache keeps only a value pin, never an admission port/WorkOwner.
    _payload: Option<novarocks_spi::connector::ConnectorPayloadRetentionGuard>,
}

struct OperationArtifacts {
    ledger: ArtifactLedger,
    loop_entered: bool,
    stage_ids: BTreeMap<usize, i64>,
    cleanup_withheld: Option<String>,
}

fn internal(message: impl Into<String>) -> Error {
    let message = message.into();
    Error::new(ErrorKind::Unexpected, message.clone()).with_source(
        novarocks_spi::connector::ConnectorError::new(
            novarocks_spi::connector::ConnectorErrorKind::Internal,
            message,
        ),
    )
}

impl std::fmt::Debug for OperationState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergCommitOperation")
            .field("token", &self.token)
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}
impl std::fmt::Debug for IcebergCommitOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

fn invalid(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::DataInvalid, message.into())
}

pub(crate) fn stopped_error(error: novarocks_fs::FileError) -> Error {
    Error::new(ErrorKind::Unexpected, "Iceberg commit operation stopped")
        .with_source(novarocks_spi::connector::ConnectorError::from(error))
}

impl IcebergCommitOperation {
    pub(crate) fn new(
        token: OperationToken,
        location: impl Into<String>,
        binding: IcebergReadBinding,
        request: ConnectorRequestContext,
        runtime: IcebergCatalogRuntime,
        limits: OperationLimits,
    ) -> Result<Self> {
        let location = location.into();
        if location.trim().is_empty() || limits.io_requests == 0 || limits.cleanup.time.is_zero() {
            return Err(invalid(
                "Invalid Iceberg commit operation limits or location",
            ));
        }
        let cancellation = FileCancellation::from_connector_request(&request);
        let payload = request.retain_owned_metadata().map_err(|error| {
            Error::new(ErrorKind::Unexpected, "retain original metadata payload").with_source(error)
        })?;
        Ok(Self {
            inner: Arc::new(OperationState {
                token,
                location,
                binding,
                request,
                preparation: OnceLock::new(),
                cancellation,
                runtime,
                permits: Arc::new(Semaphore::new(limits.io_requests as usize)),
                limits,
                artifacts: Mutex::new(OperationArtifacts {
                    ledger: ArtifactLedger::new(token),
                    loop_entered: false,
                    stage_ids: BTreeMap::new(),
                    cleanup_withheld: None,
                }),
                exit_failures: Mutex::new(BTreeMap::new()),
                attempt_ordinal: AtomicU32::new(0),
                object_ordinal: AtomicU64::new(0),
                _payload: payload,
            }),
        })
    }

    pub(crate) fn request_context(&self) -> &ConnectorRequestContext {
        self.inner.request_context()
    }

    /// Freeze the logical retry window before any load or asynchronous owner
    /// preparation. Retries inherit this deadline; evidence and cleanup use
    /// their independent grants after publication becomes possible.
    pub(crate) fn bind_preparation_deadline(&self, deadline: Instant) -> Result<()> {
        let preparation = self
            .inner
            .preparation
            .get_or_init(|| self.inner.request.clone().with_earlier_deadline(deadline));
        if preparation.deadline() > deadline {
            return Err(invalid(
                "Commit operation preparation deadline is already bound",
            ));
        }
        Ok(())
    }

    pub(crate) fn token(&self) -> OperationToken {
        self.inner.token
    }
    pub(crate) fn runtime(&self) -> &IcebergCatalogRuntime {
        &self.inner.runtime
    }

    #[cfg(test)]
    pub(crate) fn write_admission_for_test(&self) -> Arc<dyn IcebergWriteAdmission> {
        self.inner.clone()
    }
    pub(crate) fn check_active(&self) -> Result<()> {
        self.inner.check_active()
    }

    /// An operation has one immutable preparer sequence and one commit loop.
    pub(crate) fn enter_commit_loop(&self) -> Result<()> {
        let mut artifacts = self
            .inner
            .artifacts
            .lock()
            .map_err(|_| internal("Iceberg operation artifacts poisoned"))?;
        if artifacts.loop_entered {
            return Err(internal(
                "Iceberg operation cannot enter its commit loop twice",
            ));
        }
        artifacts.loop_entered = true;
        Ok(())
    }

    fn stage_snapshot_id(&self, stage: usize, metadata: &TableMetadata) -> Result<i64> {
        let mut artifacts = self
            .inner
            .artifacts
            .lock()
            .map_err(|_| internal("Iceberg operation artifacts poisoned"))?;
        if let Some(id) = artifacts.stage_ids.get(&stage) {
            return Ok(*id);
        }
        let id = loop {
            let candidate = super::staging::new_snapshot_id(metadata);
            if !artifacts.stage_ids.values().any(|id| *id == candidate) {
                break candidate;
            }
        };
        artifacts.stage_ids.insert(stage, id);
        Ok(id)
    }

    /// A refreshed base containing our ID is ambiguous. Never delete its objects.
    pub(crate) fn check_stage_snapshot_ids(&self, metadata: &TableMetadata) -> Result<()> {
        let mut artifacts = self
            .inner
            .artifacts
            .lock()
            .map_err(|_| internal("Iceberg operation artifacts poisoned"))?;
        if let Some(id) = artifacts
            .stage_ids
            .values()
            .copied()
            .find(|id| metadata.snapshot_by_id(*id).is_some())
        {
            let reason =
                format!("Iceberg stage snapshot ID {id} already present in refreshed base");
            tracing::error!("{reason}; withholding all operation cleanup");
            artifacts
                .cleanup_withheld
                .get_or_insert_with(|| reason.clone());
            return Err(internal(reason));
        }
        Ok(())
    }

    pub(crate) fn begin_attempt(&self) -> Result<IcebergCommitAttempt> {
        self.check_active()?;
        let ordinal = self
            .inner
            .attempt_ordinal
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
            .map_err(|_| invalid("Iceberg attempt ordinal exhausted"))?;
        let token = AttemptToken::new(self.token(), ordinal);
        let admission: Arc<dyn IcebergWriteAdmission> = self.inner.clone();
        let file_io = build_admitted_file_io_for_location(
            &self.inner.location,
            self.inner
                .binding
                .for_request(self.request_context().clone()),
            admission,
        );
        Ok(IcebergCommitAttempt {
            operation: self.clone(),
            token,
            file_io,
        })
    }

    /// Received files are already complete and never gain another write admission.
    pub(crate) fn adopt_session_data(&self, object: ObjectIdentity) -> Result<()> {
        self.inner
            .artifacts
            .lock()
            .map_err(|_| internal("Iceberg artifact ledger poisoned"))?
            .ledger
            .register(ArtifactRecord::received(
                object,
                ReceivedPublication::ViaManifest,
            ))
    }

    /// The exact provider publication declares this received object outside manifests.
    pub(crate) fn adopt_operation_reference(&self, object: ObjectIdentity) -> Result<()> {
        self.inner
            .artifacts
            .lock()
            .map_err(|_| internal("Iceberg artifact ledger poisoned"))?
            .ledger
            .register(ArtifactRecord::received(
                object,
                ReceivedPublication::WithRequest,
            ))
    }

    /// Conflict withdraws only artifacts tied to this round. Deletion waits for a terminal result.
    pub(crate) fn retire_attempt_bound_artifacts(&self, attempt: AttemptToken) -> Result<()> {
        let mut artifacts = self
            .inner
            .artifacts
            .lock()
            .map_err(|_| internal("Iceberg artifact ledger poisoned"))?;
        let objects: Vec<_>=artifacts.ledger.records().filter(|record| matches!(record.origin,
            ArtifactOrigin::Produced { attempt: source, kind: ArtifactKind::ManifestList | ArtifactKind::Statistics } if source==attempt))
            .map(|record|record.object.clone()).collect();
        for object in objects {
            artifacts.ledger.retire(&object)?;
        }
        Ok(())
    }

    pub(crate) fn artifacts(&self) -> Result<Vec<ArtifactRecord>> {
        Ok(self
            .inner
            .artifacts
            .lock()
            .map_err(|_| invalid("Iceberg artifact ledger poisoned"))?
            .ledger
            .records()
            .cloned()
            .collect())
    }

    /// Unknown publication retains readable ledger facts even after a poisoned mutation lock.
    /// Reading them never repairs the ledger or grants cleanup/mutation authority.
    pub(crate) fn recovery_artifacts(&self) -> Vec<ArtifactRecord> {
        self.inner
            .artifacts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .ledger
            .records()
            .cloned()
            .collect()
    }

    pub(crate) async fn pause(&self, duration: Duration) -> Result<()> {
        self.check_active()?;
        let cancellation = self.inner.preparation_cancellation();
        tokio::select! {
            biased;
            error = cancellation.ended() => Err(stopped_error(error)),
            _ = tokio::time::sleep(duration) => self.check_active(),
        }
    }

    fn select_cleanup(
        &self,
        retained: Option<&[ObjectIdentity]>,
    ) -> std::result::Result<Vec<ArtifactRecord>, IcebergCleanupReport> {
        let mut artifacts = self
            .inner
            .artifacts
            .lock()
            .map_err(|_| IcebergCleanupReport::NotAttempted)?;
        if let Some(reason) = &artifacts.cleanup_withheld {
            return Err(IcebergCleanupReport::Withheld {
                reason: reason.clone(),
            });
        }
        let records = match retained {
            Some(references) => artifacts.ledger.settle_published(references),
            None => artifacts.ledger.selected(CleanupScope::EntireOperation),
        };
        for record in &records {
            artifacts
                .ledger
                .retire(&record.object)
                .map_err(|_| IcebergCleanupReport::NotAttempted)?;
        }
        Ok(records)
    }

    /// Only proven uncommitted operations may delete their complete remaining ledger.
    pub(crate) async fn cleanup(&self, _scope: CleanupScope) -> IcebergCleanupReport {
        self.cleanup_in(CleanupScope::EntireOperation, &self.inner.request)
            .await
    }
    pub(crate) async fn cleanup_in(
        &self,
        _scope: CleanupScope,
        current: &ConnectorRequestContext,
    ) -> IcebergCleanupReport {
        match self.select_cleanup(None) {
            Ok(records) => self.cleanup_records(records, current).await,
            Err(report) => report,
        }
    }
    /// Commit settles received objects and the exact references before deleting produced leftovers.
    pub(crate) async fn cleanup_after_commit(
        &self,
        retained: &[ObjectIdentity],
    ) -> IcebergCleanupReport {
        self.cleanup_after_commit_in(retained, &self.inner.request)
            .await
    }
    pub(crate) async fn cleanup_after_commit_in(
        &self,
        retained: &[ObjectIdentity],
        current: &ConnectorRequestContext,
    ) -> IcebergCleanupReport {
        match self.select_cleanup(Some(retained)) {
            Ok(records) => self.cleanup_records(records, current).await,
            Err(report) => report,
        }
    }

    async fn cleanup_records(
        &self,
        records: Vec<ArtifactRecord>,
        current: &ConnectorRequestContext,
    ) -> IcebergCleanupReport {
        match self.inner.artifacts.lock() {
            Ok(artifacts) => {
                if let Some(reason) = &artifacts.cleanup_withheld {
                    return IcebergCleanupReport::Withheld {
                        reason: reason.clone(),
                    };
                }
            }
            Err(_) => return IcebergCleanupReport::NotAttempted,
        }
        if records.is_empty() {
            return IcebergCleanupReport::Complete { deleted: 0 };
        }
        // Selection retired every candidate under the same lock as write admission.
        let deadline = Instant::now() + self.inner.limits.cleanup.time;
        let until = tokio::time::Instant::from_std(deadline);
        // The original stop and deadline cannot cancel cleanup. Preserve only the exact storage
        // resolver and request origin, never mint a replacement storage authority.
        let stop = ConnectorStopOwner::new();
        let mut request = match ConnectorRequestContext::try_new(
            deadline,
            stop.view(),
            self.inner.request.max_handle_payload_bytes(),
            self.inner.request.max_total_payload_bytes(),
        ) {
            Ok(request) => request.with_initiation(self.inner.request.initiation()),
            Err(_) => return IcebergCleanupReport::NotAttempted,
        };
        if let Some(resolver) = self.inner.request.storage_resolver() {
            request = request.with_storage_resolver(resolver.clone());
        }
        if current.io_admission().is_some() {
            request = current
                .for_existing_obligation(deadline, stop.view())
                .after_external_effect();
        }
        let binding = self.inner.binding.for_request(request);
        // No object is removed while an admitted writer still owns its permit. Waiting itself
        // consumes cleanup's independent budget; a held stream remains in the returned ledger.
        let all_permits = tokio::time::timeout_at(
            until,
            self.inner
                .permits
                .clone()
                .acquire_many_owned(self.inner.limits.io_requests),
        )
        .await;
        let Ok(Ok(_all_permits)) = all_permits else {
            return partial_budget(0, &records);
        };
        // The same future drives every deletion. The already-exclusive I/O
        // window bounds concurrency; no worker outlives this cleanup call.
        let mut deletions = futures::stream::iter(records.into_iter().map(|record| {
            let binding = binding.clone();
            let operation = self.clone();
            async move {
                let object = record.object.clone();
                let result = async {
                    if crate::catalog_metadata_root::is_catalog_metadata_root(object.path()) {
                        return Err(CleanupRemainingReason::DeleteFailed(
                            "catalog metadata discovery root is protected from artifact cleanup"
                                .into(),
                        ));
                    }
                    let exit_failure = match operation.inner.exit_failures.lock() {
                        Ok(failures) => failures.get(&object).cloned(),
                        Err(_) => Some(StreamExitFailure::Unconfirmed),
                    };
                    if let Some(reason) = exit_failure {
                        return Err(CleanupRemainingReason::DeleteFailed(reason.to_string()));
                    }
                    if Instant::now() >= deadline {
                        return Err(CleanupRemainingReason::BudgetExhausted);
                    }
                    let delete = async {
                        let access =
                            crate::fs_io::resolve_access_for_location(object.path(), &binding)
                                .map_err(|error| {
                                    invalid(format!("Resolve cleanup object: {error}"))
                                })?;
                        let path = access.single_relative_path().map_err(invalid)?;
                        let left = deadline.saturating_duration_since(Instant::now());
                        let operator = access.operator().layer(
                            crate::opendal::layers::TimeoutLayer::new()
                                .with_timeout(left)
                                .with_io_timeout(left),
                        );
                        #[cfg(test)]
                        crate::fs_io::note_test_storage_request("delete", object.path());
                        operator.delete(path).await.map_err(|error| {
                            Error::new(
                                ErrorKind::Unexpected,
                                "Iceberg artifact cleanup delete failed",
                            )
                            .with_source(error)
                        })
                    };
                    // Timeout is missing completion evidence. Each deleter is
                    // discarded here; an external idempotent DELETE may still finish.
                    match tokio::time::timeout_at(until, delete).await {
                        Ok(Ok(())) => Ok(()),
                        Ok(Err(_)) if Instant::now() >= deadline => {
                            Err(CleanupRemainingReason::BudgetExhausted)
                        }
                        Ok(Err(error)) => {
                            Err(CleanupRemainingReason::DeleteFailed(error.to_string()))
                        }
                        Err(_) => Err(CleanupRemainingReason::BudgetExhausted),
                    }
                }
                .await;
                (object, result)
            }
        }))
        .buffer_unordered(self.inner.limits.io_requests as usize);
        let mut deleted = 0;
        let mut remaining = Vec::new();
        while let Some((object, result)) = deletions.next().await {
            match result {
                Ok(()) => {
                    if let Ok(mut artifacts) = self.inner.artifacts.lock() {
                        artifacts.ledger.forget_removed(&object);
                        deleted += 1;
                    } else {
                        remaining.push(RemainingArtifact {
                            object,
                            reason: CleanupRemainingReason::DeleteFailed(
                                "Artifact ledger poisoned".into(),
                            ),
                        });
                    }
                }
                Err(reason) => remaining.push(RemainingArtifact { object, reason }),
            }
        }
        remaining.sort_by(|left, right| left.object.cmp(&right.object));
        if remaining.is_empty() {
            IcebergCleanupReport::Complete { deleted }
        } else {
            IcebergCleanupReport::Partial { deleted, remaining }
        }
    }
}

fn partial_budget(deleted: usize, records: &[ArtifactRecord]) -> IcebergCleanupReport {
    IcebergCleanupReport::Partial {
        deleted,
        remaining: records
            .iter()
            .map(|r| RemainingArtifact {
                object: r.object.clone(),
                reason: CleanupRemainingReason::BudgetExhausted,
            })
            .collect(),
    }
}

#[derive(Clone, Debug)]
pub(crate) struct IcebergCommitAttempt {
    operation: IcebergCommitOperation,
    token: AttemptToken,
    file_io: FileIO,
}

impl IcebergCommitAttempt {
    pub(crate) fn io_window_width(&self) -> usize {
        self.operation.inner.limits.io_requests as usize
    }

    pub(crate) fn read_binding(&self) -> IcebergReadBinding {
        self.operation
            .inner
            .binding
            .for_request(self.operation.request_context().clone())
    }

    pub(crate) fn check_stage_snapshot_ids(&self, metadata: &TableMetadata) -> Result<()> {
        self.operation.check_stage_snapshot_ids(metadata)
    }
}

impl ArtifactWriter for IcebergCommitAttempt {
    fn file_io(&self) -> &FileIO {
        &self.file_io
    }
    fn attempt_token(&self) -> AttemptToken {
        self.token
    }
    fn stage_snapshot_id(&self, stage: usize, metadata: &TableMetadata) -> Result<i64> {
        self.operation.stage_snapshot_id(stage, metadata)
    }
    fn check_active(&self) -> Result<()> {
        self.operation.check_active()
    }
    fn allocate(&self, kind: ArtifactKind) -> Result<ObjectIdentity> {
        self.check_active()?;
        let ordinal = self
            .operation
            .inner
            .object_ordinal
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
            .map_err(|_| invalid("Iceberg object ordinal exhausted"))?;
        let scope = self.token.path_component();
        let object = ObjectIdentity::new(format!(
            "{}/metadata/{}-{}.{}",
            self.operation.inner.location.trim_end_matches('/'),
            scope,
            ordinal,
            kind.extension()
        ))?;
        self.operation
            .inner
            .artifacts
            .lock()
            .map_err(|_| invalid("Iceberg artifact ledger poisoned"))?
            .ledger
            .register(ArtifactRecord::produced(object.clone(), self.token, kind))?;
        Ok(object)
    }
    fn record_manifest(
        &self,
        object: &ObjectIdentity,
        live_paths: &[String],
        replaces: &[String],
    ) -> Result<()> {
        self.operation
            .inner
            .artifacts
            .lock()
            .map_err(|_| internal("Iceberg artifact ledger poisoned"))?
            .ledger
            .record_manifest(object, live_paths, replaces)
    }
    fn record_manifest_list(&self, object: &ObjectIdentity, paths: &[String]) -> Result<()> {
        self.operation
            .inner
            .artifacts
            .lock()
            .map_err(|_| internal("Iceberg artifact ledger poisoned"))?
            .ledger
            .record_manifest_list(object, paths)
    }
    fn snapshot_artifacts(
        &self,
        lists: &[ObjectIdentity],
        statistics: &[ObjectIdentity],
    ) -> Result<AttemptArtifacts> {
        self.operation
            .inner
            .artifacts
            .lock()
            .map_err(|_| internal("Iceberg artifact ledger poisoned"))?
            .ledger
            .snapshot_for_request(self.token, lists, statistics)
    }
}

impl OperationState {
    fn request_context(&self) -> &ConnectorRequestContext {
        self.preparation.get().unwrap_or(&self.request)
    }

    fn preparation_cancellation(&self) -> FileCancellation {
        self.cancellation
            .clone()
            .with_deadline(Some(self.request_context().deadline()))
    }
}

#[async_trait::async_trait]
impl IcebergWriteAdmission for OperationState {
    fn check_active(&self) -> Result<()> {
        self.preparation_cancellation()
            .check()
            .map_err(stopped_error)
    }
    fn check_output_active(&self, object: &ObjectIdentity) -> Result<()> {
        self.check_active()?;
        self.artifacts
            .lock()
            .map_err(|_| internal("Iceberg artifact ledger poisoned"))?
            .ledger
            .check_writable(object)
    }
    fn cancellation(&self) -> FileCancellation {
        self.preparation_cancellation()
    }
    fn runtime(&self) -> IcebergCatalogRuntime {
        self.runtime.clone()
    }
    fn admit(self: Arc<Self>, path: &str) -> Result<WritePermit> {
        self.check_active()?;
        let object = ObjectIdentity::new(path)?;
        self.check_output_active(&object)?;
        self.artifacts
            .lock()
            .map_err(|_| invalid("Iceberg artifact ledger poisoned"))?
            .ledger
            .mark_writing(&object)?;
        Ok(WritePermit::new(self, object))
    }
    fn mark_written(&self, object: &ObjectIdentity) -> Result<()> {
        self.artifacts
            .lock()
            .map_err(|_| invalid("Iceberg artifact ledger poisoned"))?
            .ledger
            .mark_written(object)
    }
    fn record_exit_failure(&self, object: &ObjectIdentity, failure: StreamExitFailure) {
        if let Ok(mut failures) = self.exit_failures.lock()
            && (!matches!(failure, StreamExitFailure::Unconfirmed)
                || !failures.contains_key(object))
        {
            failures.insert(object.clone(), failure);
        }
    }
    async fn io_permit(&self) -> Result<OwnedSemaphorePermit> {
        self.check_active()?;
        let cancellation = self.preparation_cancellation();
        let permit = tokio::select! {
            biased;
            error = cancellation.ended() => return Err(stopped_error(error)),
            permit = self.permits.clone().acquire_owned() => permit.map_err(|_|
                invalid("Iceberg operation I/O admission closed"))?,
        };
        self.check_active()?;
        Ok(permit)
    }
}

impl IcebergCommitOperation {
    pub(crate) fn fixed_backing_bytes() -> Result<usize> {
        let arc = std::mem::size_of::<[usize; 2]>();
        std::mem::size_of::<OperationState>()
            .checked_add(arc)
            .and_then(|n| n.checked_add(std::mem::size_of::<Semaphore>() + arc))
            .and_then(|n| {
                n.checked_add(if cfg!(target_os = "macos") {
                    4 * 128
                } else {
                    0
                })
            })
            .ok_or_else(|| invalid("original operation backing overflow"))
    }
}

#[cfg(test)]
mod tests {
    use super::super::model::ArtifactWriteState;
    use super::*;
    use bytes::Bytes;
    use novarocks_fs::{FsAccessResolver, TokioFileIoRuntime, TokioFileIoSpawner};
    use novarocks_spi::connector::{
        ConnectorWriteOperationId, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    };

    fn operation(
        location: &str,
        stop: &ConnectorStopOwner,
        limits: OperationLimits,
        deadline: Instant,
    ) -> IcebergCommitOperation {
        let runtime = tokio::runtime::Handle::current();
        let binding = IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.clone())),
            novarocks_fs::TokioRefreshExecutor::shared(runtime.clone()),
            Arc::new(TokioFileIoSpawner::new(runtime.clone())),
        );
        IcebergCommitOperation::new(
            OperationToken::from_write(ConnectorWriteOperationId::from_bytes([1; 16])),
            location,
            binding,
            ConnectorRequestContext::try_new(
                deadline,
                stop.view(),
                MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
                MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
            )
            .unwrap(),
            IcebergCatalogRuntime::new(runtime),
            limits,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn stage_ids_are_operation_owned_and_distinct_and_the_loop_is_single_use() {
        use crate::iceberg::spec::FormatVersion;
        let fixture = crate::commit::overwrite::preparer_tests::Fixture::new();
        let metadata = fixture.metadata(FormatVersion::V3);
        let operation = &fixture.operation;
        let first = operation.begin_attempt().unwrap();
        operation.enter_commit_loop().unwrap();
        let id0 = first.stage_snapshot_id(0, &metadata).unwrap();
        let id1 = first.stage_snapshot_id(1, &metadata).unwrap();
        assert_ne!(id0, id1);
        let next = operation.begin_attempt().unwrap();
        assert_eq!(next.stage_snapshot_id(0, &metadata).unwrap(), id0);
        assert_eq!(next.stage_snapshot_id(1, &metadata).unwrap(), id1);
        let error = operation.enter_commit_loop().unwrap_err();
        assert_eq!(
            super::super::attempt::before_dispatch_failure(&error).kind(),
            novarocks_spi::connector::ConnectorMutationFailureKind::Internal
        );
    }

    #[tokio::test]
    async fn cleanup_withheld_by_a_stage_id_is_reported_by_every_cleanup_entry() {
        use crate::iceberg::TableUpdate;
        use crate::iceberg::spec::{FormatVersion, Operation, Snapshot, Summary};
        let fixture = crate::commit::overwrite::preparer_tests::Fixture::new();
        let metadata = fixture.metadata(FormatVersion::V2);
        let operation = &fixture.operation;
        let attempt = operation.begin_attempt().unwrap();
        let id = attempt.stage_snapshot_id(0, &metadata).unwrap();
        let object = super::super::staging::write_manifest_list(
            &attempt,
            &metadata,
            id,
            None,
            std::iter::empty(),
        )
        .await
        .unwrap()
        .object;
        let snapshot = Snapshot::builder()
            .with_snapshot_id(id)
            .with_sequence_number(metadata.next_sequence_number())
            .with_timestamp_ms(metadata.last_updated_ms())
            .with_manifest_list(object.path().to_owned())
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: Default::default(),
            })
            .with_schema_id(metadata.current_schema_id())
            .build();
        let metadata = TableUpdate::AddSnapshot { snapshot }
            .apply(metadata.into_builder(None))
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        assert!(operation.check_stage_snapshot_ids(&metadata).is_err());
        let current = operation.request_context();
        let reports = [
            operation.cleanup(CleanupScope::EntireOperation).await,
            operation
                .cleanup_in(CleanupScope::EntireOperation, current)
                .await,
            operation.cleanup_after_commit(&[]).await,
            operation.cleanup_after_commit_in(&[], current).await,
            operation
                .cleanup_after_commit(std::slice::from_ref(&object))
                .await,
        ];
        for report in reports {
            assert!(
                matches!(report, IcebergCleanupReport::Withheld { ref reason } if reason.contains(&id.to_string()))
            );
        }
        assert!(attempt.file_io().exists(object.path()).await.unwrap());
        assert_eq!(operation.artifacts().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn allocations_are_scoped_unique_and_admitted_only_once() {
        let dir = tempfile::tempdir().unwrap();
        let stop = ConnectorStopOwner::new();
        let operation = operation(
            &format!("file://{}", dir.path().display()),
            &stop,
            OperationLimits::default(),
            Instant::now() + Duration::from_secs(60),
        );
        let attempt = operation.begin_attempt().unwrap();
        let second = operation.begin_attempt().unwrap();
        let a = attempt.allocate(ArtifactKind::Manifest).unwrap();
        let b = second.allocate(ArtifactKind::Manifest).unwrap();
        assert_ne!(a, b);
        assert!(a.path().contains(&attempt.attempt_token().path_component()));
        let output = attempt.file_io().new_output(a.path()).unwrap();
        output.write(Bytes::from_static(b"manifest")).await.unwrap();
        assert!(
            output
                .write(Bytes::from_static(b"replacement"))
                .await
                .is_err()
        );
        assert!(
            attempt
                .file_io()
                .new_output("not-registered")
                .unwrap()
                .write(Bytes::new())
                .await
                .is_err()
        );
        assert_eq!(
            std::fs::read(a.path().trim_start_matches("file://")).unwrap(),
            b"manifest"
        );
        assert_eq!(
            operation.artifacts().unwrap()[0].write_state,
            ArtifactWriteState::Written
        );
        stop.request_stop();
        assert!(operation.begin_attempt().is_err());
        assert!(attempt.allocate(ArtifactKind::Manifest).is_err());
        assert!(output.write(Bytes::new()).await.is_err());
    }

    #[tokio::test]
    async fn permits_bound_streams_and_cancel_waiters_without_releasing_live_writer() {
        let dir = tempfile::tempdir().unwrap();
        let stop = ConnectorStopOwner::new();
        let operation = operation(
            &format!("file://{}", dir.path().display()),
            &stop,
            OperationLimits {
                io_requests: 1,
                ..OperationLimits::default()
            },
            Instant::now() + Duration::from_secs(60),
        );
        let attempt = operation.begin_attempt().unwrap();
        let path = attempt.allocate(ArtifactKind::Statistics).unwrap();
        let mut writer = attempt
            .file_io()
            .new_output(path.path())
            .unwrap()
            .writer()
            .await
            .unwrap();
        assert_eq!(operation.inner.permits.available_permits(), 0);
        let waiter_operation = operation.clone();
        let waiter = tokio::spawn(async move { waiter_operation.inner.io_permit().await });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());
        stop.request_stop();
        assert!(waiter.await.unwrap().is_err());
        assert_eq!(operation.inner.permits.available_permits(), 0);
        // Local FS can report Unsupported for abort without atomic_write_dir; even then
        // the actual writer call exits and the known-uncommitted file stays in the ledger.
        assert!(writer.write(Bytes::new()).await.is_err());
        assert_eq!(operation.inner.permits.available_permits(), 1);
        let IcebergCleanupReport::Partial {
            deleted: 0,
            remaining,
        } = operation.cleanup(CleanupScope::EntireOperation).await
        else {
            panic!("unconfirmed multipart abort must remain incomplete");
        };
        assert!(
            matches!(&remaining[0].reason, CleanupRemainingReason::DeleteFailed(reason) if reason.contains("abort"))
        );
    }

    #[tokio::test]
    async fn stopped_pause_and_expired_operations_never_start_another_attempt() {
        let dir = tempfile::tempdir().unwrap();
        let stop = ConnectorStopOwner::new();
        let operation = operation(
            &format!("file://{}", dir.path().display()),
            &stop,
            OperationLimits::default(),
            Instant::now() + Duration::from_secs(60),
        );
        let sleeping = operation.clone();
        let task = tokio::spawn(async move { sleeping.pause(Duration::from_secs(600)).await });
        stop.request_stop();
        assert!(
            tokio::time::timeout(Duration::from_secs(1), task)
                .await
                .unwrap()
                .unwrap()
                .is_err()
        );
        let expired = super::tests::operation(
            &format!("file://{}", dir.path().display()),
            &ConnectorStopOwner::new(),
            OperationLimits::default(),
            Instant::now() - Duration::from_secs(1),
        );
        assert!(expired.begin_attempt().is_err());
    }

    #[tokio::test]
    async fn cleanup_is_independent_of_request_stop_and_retains_budget_remainder() {
        assert_eq!(
            novarocks_spi::connector::ConnectorCleanupScanSafety::Unproven
                .require_scanned_object_gc()
                .unwrap_err()
                .kind(),
            novarocks_spi::connector::ConnectorErrorKind::Unsupported,
        );
        // This exact original artifact ledger remains independently eligible.
        let dir = tempfile::tempdir().unwrap();
        let stop = ConnectorStopOwner::new();
        let operation = operation(
            &format!("file://{}", dir.path().display()),
            &stop,
            OperationLimits {
                io_requests: 1,
                cleanup: CleanupBudget {
                    time: Duration::from_millis(250),
                },
            },
            Instant::now() + Duration::from_secs(60),
        );
        let attempt = operation.begin_attempt().unwrap();
        for _ in 0..3 {
            let path = attempt.allocate(ArtifactKind::Manifest).unwrap();
            attempt
                .file_io()
                .new_output(path.path())
                .unwrap()
                .write(Bytes::new())
                .await
                .unwrap();
        }
        // Hold the only I/O position. Cleanup must consume its own time budget
        // waiting for real exit, despite the original statement being stopped.
        let held = operation
            .inner
            .permits
            .clone()
            .acquire_owned()
            .await
            .unwrap();
        stop.request_stop();
        let report = operation.cleanup(CleanupScope::EntireOperation).await;
        let IcebergCleanupReport::Partial { deleted, remaining } = report else {
            panic!("budget remainder");
        };
        assert_eq!(deleted, 0);
        assert_eq!(remaining.len(), 3);
        assert!(
            remaining
                .iter()
                .all(|r| r.reason == CleanupRemainingReason::BudgetExhausted)
        );
        assert_eq!(operation.artifacts().unwrap().len(), 3);
        drop(held);
        let final_cleanup = operation.cleanup(CleanupScope::EntireOperation).await;
        assert_eq!(final_cleanup, IcebergCleanupReport::Complete { deleted: 3 });
        assert!(operation.artifacts().unwrap().is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unconfirmed_drop_abort_is_reported_as_partial_even_if_object_is_absent() {
        let dir = tempfile::tempdir().unwrap();
        let stop = ConnectorStopOwner::new();
        let operation = operation(
            &format!("file://{}", dir.path().display()),
            &stop,
            OperationLimits::default(),
            Instant::now() + Duration::from_secs(60),
        );
        let attempt = operation.begin_attempt().unwrap();
        let path = attempt.allocate(ArtifactKind::Statistics).unwrap();
        let writer = attempt
            .file_io()
            .new_output(path.path())
            .unwrap()
            .writer()
            .await
            .unwrap();
        drop(writer);
        // The local service cannot abort without an atomic staging directory. No manifest
        // absence or released permit can prove that abort succeeded.
        let IcebergCleanupReport::Partial {
            deleted: 0,
            remaining,
        } = operation.cleanup(CleanupScope::EntireOperation).await
        else {
            panic!("unconfirmed abort cannot become Complete");
        };
        assert_eq!(remaining[0].object, path);
        assert!(
            matches!(&remaining[0].reason, CleanupRemainingReason::DeleteFailed(reason)
            if reason.contains("Multipart abort failed"))
        );
        assert_eq!(operation.artifacts().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn waiting_writer_cannot_resurrect_a_path_retired_by_cleanup() {
        let dir = tempfile::tempdir().unwrap();
        let stop = ConnectorStopOwner::new();
        let operation = operation(
            &format!("file://{}", dir.path().display()),
            &stop,
            OperationLimits {
                io_requests: 1,
                ..OperationLimits::default()
            },
            Instant::now() + Duration::from_secs(60),
        );
        let attempt = operation.begin_attempt().unwrap();
        let path = attempt.allocate(ArtifactKind::Manifest).unwrap();
        let held = operation.inner.io_permit().await.unwrap();
        let output = attempt.file_io().new_output(path.path()).unwrap();
        let waiting = tokio::spawn(async move {
            output
                .write(Bytes::from_static(b"must-not-be-written"))
                .await
        });
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        let cleaning_operation = operation.clone();
        let cleaning = tokio::spawn(async move {
            cleaning_operation
                .cleanup(CleanupScope::EntireOperation)
                .await
        });
        // Observe the explicit revocation, rather than guessing a scheduling delay.
        loop {
            if operation
                .inner
                .artifacts
                .lock()
                .unwrap()
                .ledger
                .is_retired(&path)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        drop(held);
        assert!(waiting.await.unwrap().is_err());
        assert!(matches!(
            cleaning.await.unwrap(),
            IcebergCleanupReport::Complete { deleted: 1 }
        ));
        assert!(!std::path::Path::new(path.path().trim_start_matches("file://")).exists());
        assert!(
            attempt
                .file_io()
                .new_output(path.path())
                .unwrap()
                .write(Bytes::new())
                .await
                .is_err()
        );
    }

    fn delete_test_binding(address: std::net::SocketAddr, timeout_ms: u64) -> IcebergReadBinding {
        let runtime = tokio::runtime::Handle::current();
        IcebergReadBinding::new(
            Some(novarocks_fs::ObjectStoreConfig {
                endpoint: format!("http://{address}"),
                access_key_id: novarocks_fs::SecretValue::new("test"),
                access_key_secret: novarocks_fs::SecretValue::new("test"),
                session_token: None,
                enable_path_style_access: Some(true),
                region: Some("us-east-1".into()),
                retry_max_times: Some(3),
                retry_min_delay_ms: Some(10),
                retry_max_delay_ms: Some(20),
                timeout_ms: Some(timeout_ms),
                io_timeout_ms: Some(timeout_ms),
            }),
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.clone())),
            novarocks_fs::TokioRefreshExecutor::shared(runtime.clone()),
            Arc::new(TokioFileIoSpawner::new(runtime)),
        )
    }

    #[tokio::test]
    async fn cleanup_removes_more_than_a_thousand_owned_objects_without_a_count_budget() {
        let directory = tempfile::tempdir().unwrap();
        let operation = operation(
            &format!("file://{}", directory.path().display()),
            &ConnectorStopOwner::new(),
            OperationLimits::default(),
            Instant::now() + Duration::from_secs(120),
        );
        let attempt = operation.begin_attempt().unwrap();
        let mut paths = Vec::new();
        for _ in 0..1500 {
            let object = attempt.allocate(ArtifactKind::Manifest).unwrap();
            attempt
                .file_io()
                .new_output(object.path())
                .unwrap()
                .write(Bytes::new())
                .await
                .unwrap();
            paths.push(object);
        }
        let cleanup = operation.cleanup(CleanupScope::EntireOperation).await;
        assert_eq!(cleanup, IcebergCleanupReport::Complete { deleted: 1500 });
        assert!(operation.artifacts().unwrap().is_empty());
        for object in paths {
            assert!(!std::path::Path::new(object.path().strip_prefix("file://").unwrap()).exists());
        }
    }

    #[tokio::test]
    async fn cleanup_deletes_concurrently_inside_its_original_io_window() {
        use std::io::{BufRead, Write};
        use std::sync::Condvar;
        use std::sync::atomic::{AtomicBool, AtomicUsize};
        const WINDOW: u32 = 4;
        const OBJECTS: usize = 9;
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let stop_server = Arc::new(AtomicBool::new(false));
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let (request_seen, requests) = std::sync::mpsc::channel();
        let server_gate = gate.clone();
        let server_stop = stop_server.clone();
        let server_active = active.clone();
        let server_peak = peak.clone();
        let runtime = tokio::runtime::Handle::current();
        let stop = ConnectorStopOwner::new();
        let operation = IcebergCommitOperation::new(
            OperationToken::from_write(ConnectorWriteOperationId::new()),
            "s3://bucket/table",
            delete_test_binding(address, 15000),
            ConnectorRequestContext::try_new(
                Instant::now() + Duration::from_secs(30),
                stop.view(),
                MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
                MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
            )
            .unwrap(),
            IcebergCatalogRuntime::new(runtime),
            OperationLimits {
                io_requests: WINDOW,
                cleanup: CleanupBudget {
                    time: Duration::from_secs(10),
                },
            },
        )
        .unwrap();
        for ordinal in 0..OBJECTS {
            operation
                .adopt_session_data(
                    ObjectIdentity::new(format!("s3://bucket/table/data/owned-{ordinal}.parquet"))
                        .unwrap(),
                )
                .unwrap();
        }
        let server = std::thread::spawn(move || {
            let mut children = Vec::new();
            while !server_stop.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((mut socket, _)) => {
                        let gate = server_gate.clone();
                        let active = server_active.clone();
                        let peak = server_peak.clone();
                        let request_seen = request_seen.clone();
                        children.push(std::thread::spawn(move || {
                            let _ = socket.set_read_timeout(Some(Duration::from_secs(5)));
                            let mut reader = std::io::BufReader::new(&socket);
                            let mut request = String::new();
                            if reader.read_line(&mut request).is_err() || request.is_empty() {
                                return;
                            }
                            loop {
                                let mut line = String::new();
                                if reader.read_line(&mut line).is_err() || line.is_empty() {
                                    return;
                                }
                                if line == "\r\n" {
                                    break;
                                }
                            }
                            drop(reader);
                            let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                            peak.fetch_max(current, Ordering::SeqCst);
                            let _ = request_seen.send(request);
                            // All first-window DELETEs remain unanswered until
                            // the test observes them, so overlap is a fact.
                            let (released, changed) = &*gate;
                            let released = released.lock().unwrap();
                            let _ = changed.wait_timeout_while(
                                released, Duration::from_secs(8), |released| !*released,
                            ).unwrap();
                            // A successor cannot be received before this response.
                            // End the observed request before making its response visible.
                            active.fetch_sub(1, Ordering::SeqCst);
                            let _ = socket.write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                        }));
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        // Poll only the nonblocking accept, never infer overlap from time.
                        std::thread::park_timeout(Duration::from_millis(1));
                    }
                    Err(_) => break,
                }
            }
            drop(listener);
            for child in children {
                child.join().unwrap();
            }
        });
        let cleaning = operation.clone();
        let cleanup =
            tokio::spawn(async move { cleaning.cleanup(CleanupScope::EntireOperation).await });
        // Receiving facts runs off the Tokio thread that drives the request futures.
        let (first_window, requests) = tokio::task::spawn_blocking(move || {
            let mut seen = Vec::new();
            for _ in 0..WINDOW {
                match requests.recv_timeout(Duration::from_secs(5)) {
                    Ok(request) => seen.push(request),
                    Err(_) => break,
                }
            }
            (seen, requests)
        })
        .await
        .unwrap();
        let (released, changed) = &*gate;
        *released.lock().unwrap() = true;
        changed.notify_all();
        let cleanup = cleanup.await;
        stop_server.store(true, Ordering::SeqCst);
        // Always release and join the endpoint and its actual children before assertions.
        let server_exit = server.join();
        let first_count = first_window.len();
        let all_requests: Vec<_> = first_window
            .into_iter()
            .chain(requests.try_iter())
            .collect();
        server_exit.unwrap();
        assert_eq!(
            cleanup.unwrap(),
            IcebergCleanupReport::Complete { deleted: OBJECTS }
        );
        assert_eq!(first_count, WINDOW as usize);
        assert_eq!(all_requests.len(), OBJECTS);
        assert!(
            all_requests
                .iter()
                .all(|request| request.starts_with("DELETE "))
        );
        let peak = peak.load(Ordering::SeqCst);
        assert!(
            peak > 1 && peak <= WINDOW as usize,
            "observed in-flight DELETEs: {peak}"
        );
        assert_eq!(active.load(Ordering::SeqCst), 0);
        assert!(operation.artifacts().unwrap().is_empty());
    }

    #[tokio::test]
    async fn cleanup_delete_timeout_keeps_unknown_object_without_background_work() {
        use std::io::{BufRead, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let (request_seen, requests) = std::sync::mpsc::channel();
        let (release, wait_release) = std::sync::mpsc::channel();
        let runtime = tokio::runtime::Handle::current();
        let binding = delete_test_binding(address, 2000);
        let stop = ConnectorStopOwner::new();
        let operation = IcebergCommitOperation::new(
            OperationToken::from_write(ConnectorWriteOperationId::from_bytes([2; 16])),
            "s3://bucket/table",
            binding,
            ConnectorRequestContext::try_new(
                Instant::now() + Duration::from_secs(30),
                stop.view(),
                MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
                MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
            )
            .unwrap(),
            IcebergCatalogRuntime::new(runtime),
            OperationLimits {
                io_requests: 1,
                cleanup: CleanupBudget {
                    time: Duration::from_millis(250),
                },
            },
        )
        .unwrap();
        let object = ObjectIdentity::new("s3://bucket/table/data/owned.parquet").unwrap();
        operation.adopt_session_data(object.clone()).unwrap();
        // This fault oracle needs an issued DELETE with a missing response,
        // so prepare the same binding's pooled client before its finite phase.
        // Resolution builds the client only; it sends no object-store request.
        let prepared_access = crate::fs_io::resolve_access_for_location(
            object.path(),
            &operation
                .inner
                .binding
                .for_request(operation.request_context().clone()),
        )
        .unwrap();
        drop(prepared_access);
        let server = std::thread::spawn(move || {
            let until = Instant::now() + Duration::from_secs(3);
            loop {
                match listener.accept() {
                    Ok((mut socket, _)) => {
                        socket
                            .set_read_timeout(Some(Duration::from_secs(3)))
                            .unwrap();
                        // TCP reads need not contain a complete request line.
                        let mut line = String::new();
                        let _ = std::io::BufReader::new(&socket).read_line(&mut line);
                        if line.is_empty() {
                            // An empty connection issued no HTTP request. Only
                            // an actual DELETE exercises response-loss behavior.
                            continue;
                        }
                        let _ = request_seen.send(line);
                        let _ = wait_release.recv_timeout(Duration::from_secs(3));
                        let _ = socket.write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                        return;
                    }
                    Err(error)
                        if error.kind() == std::io::ErrorKind::WouldBlock
                            && Instant::now() < until =>
                    {
                        std::thread::sleep(Duration::from_millis(1))
                    }
                    _ => return,
                }
            }
        });
        stop.request_stop();
        let began = Instant::now();
        let report = operation.cleanup(CleanupScope::EntireOperation).await;
        let elapsed = began.elapsed();
        let request = requests.recv_timeout(Duration::from_secs(1));
        // Release and join even if an assertion fails below; this test owns its fake endpoint.
        let _ = release.send(());
        server.join().unwrap();
        let request = request.unwrap();
        assert!(
            request.starts_with("DELETE "),
            "Expected DELETE request line: {request:?}"
        );
        assert!(
            elapsed < Duration::from_secs(1),
            "cleanup escaped its independent budget: {elapsed:?}"
        );
        let IcebergCleanupReport::Partial {
            deleted: 0,
            remaining,
        } = report
        else {
            panic!("missing DELETE completion cannot prove removal");
        };
        assert_eq!(remaining[0].object, object);
        assert_eq!(operation.artifacts().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn cleanup_waits_for_writer_exit_and_reports_it_when_budget_expires() {
        let dir = tempfile::tempdir().unwrap();
        let stop = ConnectorStopOwner::new();
        let operation = operation(
            &format!("file://{}", dir.path().display()),
            &stop,
            OperationLimits {
                io_requests: 1,
                cleanup: CleanupBudget {
                    time: Duration::from_millis(250),
                },
            },
            Instant::now() + Duration::from_secs(60),
        );
        let attempt = operation.begin_attempt().unwrap();
        let path = attempt.allocate(ArtifactKind::Statistics).unwrap();
        let mut writer = attempt
            .file_io()
            .new_output(path.path())
            .unwrap()
            .writer()
            .await
            .unwrap();
        let report = tokio::time::timeout(
            Duration::from_secs(1),
            operation.cleanup(CleanupScope::EntireOperation),
        )
        .await
        .unwrap();
        let IcebergCleanupReport::Partial {
            deleted: 0,
            remaining,
        } = report
        else {
            panic!("writer remains live");
        };
        assert_eq!(remaining[0].object, path);
        assert_eq!(remaining[0].reason, CleanupRemainingReason::BudgetExhausted);
        assert_eq!(
            operation.artifacts().unwrap()[0].write_state,
            ArtifactWriteState::Writing
        );
        assert_eq!(operation.inner.permits.available_permits(), 0);
        writer.close().await.unwrap();
        assert!(operation.inner.exit_failures.lock().unwrap().is_empty());
        assert_eq!(operation.inner.permits.available_permits(), 1);
        assert_eq!(
            operation.artifacts().unwrap()[0].write_state,
            ArtifactWriteState::Written
        );
        let report = operation.cleanup(CleanupScope::EntireOperation).await;
        assert!(
            matches!(report, IcebergCleanupReport::Complete { deleted: 1 }),
            "closed writer cleanup: {report:?}; ledger: {:?}",
            operation.artifacts().unwrap()
        );
        assert!(operation.artifacts().unwrap().is_empty());
        assert!(
            !attempt
                .file_io()
                .new_input(path.path())
                .unwrap()
                .exists()
                .await
                .unwrap()
        );
    }
}
