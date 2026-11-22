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
use novarocks_fs::{FileCancellation, FileIdentity, FileReadRange};
use novarocks_spi::connector::{ConnectorRequestContext, ConnectorStopOwner};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::RegisteredNewFiles;
use super::model::{
    ArtifactKind, ArtifactLedger, ArtifactRecord, ArtifactWriter, AttemptArtifacts, AttemptToken,
    CleanupRemainingReason, CleanupScope, IcebergCleanupReport, ObjectIdentity, OperationAuthority,
    OperationToken, ReceivedPublication, RegisteredManifestObservation, RemainingArtifact,
};
use super::new_manifests::{RegisteredPaths, StageNewFiles, WrittenNewManifests};

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
// Design: ADR-0234 (docs/adr/ADR-0234-operation-owned-artifacts-and-bounded-publication-facts.md)
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
    row_delta: OnceLock<RowDeltaValidation>,
    exit_failures: Mutex<BTreeMap<ObjectIdentity, StreamExitFailure>>,
    attempt_ordinal: AtomicU32,
    object_ordinal: AtomicU64,
    // The original payload pin follows retained journal/artifact backing; the
    // process cache keeps only a value pin, never an admission port/WorkOwner.
    _payload: Option<novarocks_spi::connector::ConnectorPayloadRetentionGuard>,
}

struct RowDeltaValidation {
    check: super::dependency::RowDeltaWindowCheck,
    source: Option<std::sync::Weak<dyn super::dependency::RemovedDataFiles>>,
    _facts: super::row_delta_facts::RetainedRowDeltaFacts,
}

struct OperationArtifacts {
    ledger: ArtifactLedger,
    reuse: super::model::ManifestReuseIndex,
    loop_entered: bool,
    stage_ids: BTreeMap<usize, i64>,
    statistics_registration: Option<(
        AttemptToken,
        novarocks_spi::connector::ConnectorPayloadRetentionGuard,
    )>,
    statistics_copy: Option<novarocks_spi::connector::ConnectorPayloadRetentionGuard>,
    new_files: BTreeMap<usize, StageNewFiles>,
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

fn statistics_read_failure(error: novarocks_spi::connector::ConnectorError) -> Error {
    Error::new(
        ErrorKind::Unexpected,
        "Iceberg statistics source range read failed",
    )
    .with_source(error)
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
                    reuse: super::model::ManifestReuseIndex::default(),
                    loop_entered: false,
                    stage_ids: BTreeMap::new(),
                    statistics_registration: None,
                    statistics_copy: None,
                    new_files: BTreeMap::new(),
                    cleanup_withheld: None,
                }),
                row_delta: OnceLock::new(),
                exit_failures: Mutex::new(BTreeMap::new()),
                attempt_ordinal: AtomicU32::new(0),
                object_ordinal: AtomicU64::new(0),
                _payload: payload,
            }),
        })
    }

    pub(crate) fn hold_row_delta_validation(
        &self,
        check: super::dependency::RowDeltaWindowCheck,
        facts: super::row_delta_facts::RetainedRowDeltaFacts,
        source: Option<std::sync::Weak<dyn super::dependency::RemovedDataFiles>>,
    ) -> Result<()> {
        self.inner
            .row_delta
            .set(RowDeltaValidation {
                check,
                source,
                _facts: facts,
            })
            .map_err(|_| internal("Iceberg operation received its row-delta validation twice"))
    }

    pub(crate) fn row_delta_check(&self) -> Option<&super::dependency::RowDeltaWindowCheck> {
        self.inner
            .row_delta
            .get()
            .map(|validation| &validation.check)
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

    pub(crate) fn install_stage_new_files(
        &self,
        stage: usize,
        files: super::new_manifests::PendingNewFiles,
    ) -> Result<()> {
        let mut artifacts = self
            .inner
            .artifacts
            .lock()
            .map_err(|_| internal("Iceberg operation artifacts poisoned"))?;
        if artifacts.loop_entered
            || artifacts.ledger.registered_stage().is_some()
            || artifacts.new_files.contains_key(&stage)
        {
            return Err(internal(
                "Iceberg compact files can only be installed once before preparation",
            ));
        }
        artifacts
            .new_files
            .insert(stage, StageNewFiles::Pending(Arc::new(files)));
        Ok(())
    }

    pub(crate) fn install_registered_new_files(
        &self,
        stage: usize,
        files: RegisteredNewFiles,
    ) -> Result<Arc<RegisteredPaths>> {
        let mut artifacts = self
            .inner
            .artifacts
            .lock()
            .map_err(|_| internal("Iceberg operation artifacts poisoned"))?;
        if self.token().authority() != OperationAuthority::Mutation
            || artifacts.loop_entered
            || artifacts.ledger.registered_stage().is_some()
            || !artifacts.new_files.is_empty()
            || !artifacts.stage_ids.is_empty()
        {
            return Err(internal(
                "Iceberg registration requires one new mutation append stage",
            ));
        }
        let paths = files.retain_paths()?;
        artifacts
            .ledger
            .install_registered_paths(stage, Arc::clone(&paths))?;
        artifacts
            .new_files
            .insert(stage, StageNewFiles::Registered(files));
        Ok(paths)
    }

    /// The caller has driven its original preparation loop to actual exit.
    /// Unknown publication can outlive this source without retaining its paths.
    pub(crate) fn release_registered_paths(&self) {
        let paths = self
            .inner
            .artifacts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .ledger
            .take_registered_paths();
        drop(paths);
    }

    pub(crate) fn pending_stage_new_files(
        &self,
        stage: usize,
    ) -> Result<Arc<super::new_manifests::PendingNewFiles>> {
        let artifacts = self
            .inner
            .artifacts
            .lock()
            .map_err(|_| internal("Iceberg operation artifacts poisoned"))?;
        match artifacts.new_files.get(&stage) {
            Some(StageNewFiles::Pending(files)) => Ok(Arc::clone(files)),
            _ => Err(internal(
                "Iceberg materialized stage has no pending source files",
            )),
        }
    }

    pub(crate) fn take_stage_new_files(&self, stage: usize) -> Result<StageNewFiles> {
        let mut artifacts = self
            .inner
            .artifacts
            .lock()
            .map_err(|_| internal("Iceberg operation artifacts poisoned"))?;
        let state = artifacts
            .new_files
            .get_mut(&stage)
            .ok_or_else(|| internal("Iceberg operation does not own this compact stage"))?;
        Ok(std::mem::replace(state, StageNewFiles::Writing))
    }

    pub(crate) fn finish_stage_new_files(
        &self,
        stage: usize,
        snapshot: i64,
        written: WrittenNewManifests,
        verify_coverage: bool,
    ) -> Result<()> {
        let mut artifacts = self
            .inner
            .artifacts
            .lock()
            .map_err(|_| internal("Iceberg operation artifacts poisoned"))?;
        if !matches!(
            artifacts.new_files.get(&stage),
            Some(StageNewFiles::Writing)
        ) {
            return Err(internal("Iceberg compact manifest stage was not claimed"));
        }
        if artifacts.stage_ids.get(&stage) != Some(&snapshot)
            || written.is_registered() != (artifacts.ledger.registered_stage() == Some(stage))
        {
            return Err(internal(
                "Iceberg manifest stage changed its admitted source or snapshot",
            ));
        }
        if verify_coverage {
            let ledger = &mut artifacts.ledger;
            let coverage = if written.is_registered() {
                ledger.seal_registered_stage(stage, snapshot, &written.manifests, written.entries)
            } else {
                ledger.seal_compact_stage(snapshot, &written.manifests, written.entries)
            };
            coverage.inspect_err(|error| {
                tracing::error!(operation = ?self.token(), stage, snapshot, %error, "New Iceberg manifest coverage check failed");
            })?;
        }
        artifacts
            .new_files
            .insert(stage, StageNewFiles::Written(written));
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

    pub(crate) fn statistics_artifact_path_bound(&self) -> Result<usize> {
        // Matches artifact_path and AttemptToken::path_component without
        // allocating a path or reserving an object ordinal.
        let uuid = uuid::fmt::Hyphenated::LENGTH;
        let operation = self.token().authority().label().len() + "-".len() + uuid;
        let attempt =
            operation + "-attempt-".len() + (u32::MAX.ilog10() as usize + 1) + "-".len() + uuid;
        let suffix = "/metadata/".len()
            + attempt
            + "-".len()
            + (u64::MAX.ilog10() as usize + 1)
            + ".".len()
            + ArtifactKind::Statistics.extension().len();
        self.inner
            .location
            .trim_end_matches('/')
            .len()
            .checked_add(suffix)
            .ok_or_else(|| {
                let message = "Iceberg statistics artifact path is not representable";
                Error::new(ErrorKind::Unexpected, message).with_source(
                    novarocks_spi::connector::ConnectorError::new(
                        novarocks_spi::connector::ConnectorErrorKind::ResourceExhausted,
                        message,
                    ),
                )
            })
    }

    pub(crate) fn prepared_statistics_target(&self, replace_stage: usize) -> Result<i64> {
        self.inner
            .artifacts
            .lock()
            .map_err(|_| internal("Iceberg operation artifacts poisoned"))?
            .stage_ids
            .get(&replace_stage)
            .copied()
            .ok_or_else(|| internal("Iceberg statistics has no preceding Replace stage"))
    }

    pub(crate) async fn read_statistics_into(
        &self,
        path: &str,
        file_size: u64,
        target: &mut [u8],
    ) -> Result<()> {
        let _permit = self.inner.io_permit().await?;
        let binding = self
            .inner
            .binding
            .for_request(self.request_context().clone());
        let _phase = binding.admit_non_http_io(path).await.map_err(|error| {
            Error::new(
                ErrorKind::Unexpected,
                "Iceberg statistics I/O admission refused",
            )
            .with_source(error)
        })?;
        let access = binding
            .resolve_access_for_request_async(path)
            .await
            .map_err(|error| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Iceberg statistics source access failed",
                )
                .with_source(error)
            })?;
        let file = access
            .bind(0, FileIdentity::new(path, file_size, None))
            .map_err(|error| statistics_read_failure(error.into()))?;
        #[cfg(test)]
        crate::fs_io::note_test_storage_request("statistics-read", path);
        file.read_into(
            FileReadRange::WholeFile,
            target,
            &self.inner.preparation_cancellation(),
        )
        .await
        .map_err(|error| statistics_read_failure(error.into()))?;
        self.check_active()
    }

    pub(crate) fn remember_paid_statistics_copy(
        &self,
        basis: super::model::StatisticsReuseBasis,
        output: super::model::ReusableStatisticsFile,
        backing: novarocks_spi::connector::ConnectorPayloadRetentionGuard,
    ) -> Result<()> {
        let previous = {
            let mut artifacts = self
                .inner
                .artifacts
                .lock()
                .map_err(|_| internal("Iceberg operation artifacts poisoned"))?;
            let OperationArtifacts {
                ledger,
                reuse,
                statistics_copy,
                ..
            } = &mut *artifacts;
            reuse.remember_statistics(ledger, basis, output)?;
            statistics_copy.replace(backing)
        };
        drop(previous);
        Ok(())
    }

    pub(crate) fn retain_statistics_registration(
        &self,
        attempt: AttemptToken,
        backing: novarocks_spi::connector::ConnectorPayloadRetentionGuard,
    ) -> Result<()> {
        let mut artifacts = self
            .inner
            .artifacts
            .lock()
            .map_err(|_| internal("Iceberg operation artifacts poisoned"))?;
        if attempt.operation() != self.token() || artifacts.statistics_registration.is_some() {
            return Err(internal(
                "Iceberg statistics lost its original attempt registration",
            ));
        }
        artifacts.statistics_registration = Some((attempt, backing));
        Ok(())
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
        let retired = {
            let mut artifacts = self
                .inner
                .artifacts
                .lock()
                .map_err(|_| internal("Iceberg artifact ledger poisoned"))?;
            artifacts.ledger.retire_round(attempt)?;
            if artifacts
                .statistics_registration
                .as_ref()
                .is_some_and(|(owner, _)| *owner == attempt)
            {
                artifacts.statistics_registration.take()
            } else {
                None
            }
        };
        drop(retired);
        Ok(())
    }

    #[cfg(test)]
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
        retained: Option<std::collections::BTreeSet<&ObjectIdentity>>,
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
        let pending = self.inner.artifacts.lock().ok().map(|mut artifacts| {
            (
                std::mem::take(&mut artifacts.new_files),
                artifacts.ledger.take_registered_paths(),
            )
        });
        drop(pending);
        match self.select_cleanup(None) {
            Ok(records) => self.cleanup_records(records, current).await,
            Err(report) => report,
        }
    }
    /// Commit settles received objects and the exact references before deleting produced leftovers.
    pub(crate) async fn cleanup_after_commit<'a>(
        &self,
        retained: impl IntoIterator<Item = &'a ObjectIdentity> + Send,
    ) -> IcebergCleanupReport {
        self.cleanup_after_commit_in(retained, &self.inner.request)
            .await
    }
    pub(crate) async fn cleanup_after_commit_in<'a>(
        &self,
        retained: impl IntoIterator<Item = &'a ObjectIdentity> + Send,
        current: &ConnectorRequestContext,
    ) -> IcebergCleanupReport {
        match self.select_cleanup(Some(retained.into_iter().collect())) {
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
    pub(crate) fn row_delta_removed_data(
        &self,
    ) -> Result<Option<Arc<dyn super::dependency::RemovedDataFiles>>> {
        self.operation
            .inner
            .row_delta
            .get()
            .and_then(|validation| validation.source.as_ref())
            .map(|source| {
                source
                    .upgrade()
                    .ok_or_else(|| internal("Iceberg operation lost the original row-delta source"))
            })
            .transpose()
    }

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

impl IcebergCommitAttempt {
    fn artifact_path(&self, kind: ArtifactKind) -> Result<ObjectIdentity> {
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
        Ok(object)
    }
}

impl ArtifactWriter for IcebergCommitAttempt {
    fn find_reusable_manifest(
        &self,
        basis: &crate::commit::model::ManifestReuseBasis,
    ) -> Result<Option<crate::iceberg::spec::ManifestFile>> {
        let mut artifacts = self
            .operation
            .inner
            .artifacts
            .lock()
            .map_err(|_| internal("Iceberg artifact ledger poisoned"))?;
        let OperationArtifacts { ledger, reuse, .. } = &mut *artifacts;
        reuse.find(ledger, basis)
    }
    fn remember_reusable_manifest(
        &self,
        basis: crate::commit::model::ManifestReuseBasis,
        output: crate::iceberg::spec::ManifestFile,
    ) -> Result<()> {
        let mut artifacts = self
            .operation
            .inner
            .artifacts
            .lock()
            .map_err(|_| internal("Iceberg artifact ledger poisoned"))?;
        let OperationArtifacts { ledger, reuse, .. } = &mut *artifacts;
        reuse.remember(ledger, basis, output)
    }
    fn rewrite_reuse_backing_bytes(&self, stage: usize) -> Result<u64> {
        self.operation
            .inner
            .artifacts
            .lock()
            .map_err(|_| internal("Iceberg artifact ledger poisoned"))?
            .reuse
            .rewrite_backing_bytes(stage)
    }
    fn find_reusable_rewrite(
        &self,
        stage: usize,
        encoding: &[u8],
        latest: &[crate::iceberg::spec::ManifestFile],
    ) -> Result<Option<crate::commit::model::ManifestRewriteReuse>> {
        let mut artifacts = self
            .operation
            .inner
            .artifacts
            .lock()
            .map_err(|_| internal("Iceberg artifact ledger poisoned"))?;
        let OperationArtifacts { ledger, reuse, .. } = &mut *artifacts;
        reuse.find_rewrite(ledger, stage, encoding, latest)
    }
    fn remember_reusable_rewrite(
        &self,
        stage: usize,
        result: crate::commit::model::ManifestRewriteReuse,
    ) -> Result<()> {
        let mut artifacts = self
            .operation
            .inner
            .artifacts
            .lock()
            .map_err(|_| internal("Iceberg artifact ledger poisoned"))?;
        let OperationArtifacts { ledger, reuse, .. } = &mut *artifacts;
        reuse.remember_rewrite(ledger, stage, result)
    }
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
        let object = self.artifact_path(kind)?;
        self.operation
            .inner
            .artifacts
            .lock()
            .map_err(|_| internal("Iceberg artifact ledger poisoned"))?
            .ledger
            .register(ArtifactRecord::produced(object.clone(), self.token, kind))?;
        Ok(object)
    }
    fn allocate_reusable_statistics(&self) -> Result<ObjectIdentity> {
        let object = self.artifact_path(ArtifactKind::Statistics)?;
        self.operation
            .inner
            .artifacts
            .lock()
            .map_err(|_| internal("Iceberg artifact ledger poisoned"))?
            .ledger
            .register(ArtifactRecord::reusable_statistics(
                object.clone(),
                self.token,
            ))?;
        Ok(object)
    }
    fn find_reusable_statistics(
        &self,
        basis: &super::model::StatisticsReuseBasis,
    ) -> Result<Option<super::model::ReusableStatisticsFile>> {
        let mut artifacts = self
            .operation
            .inner
            .artifacts
            .lock()
            .map_err(|_| internal("Iceberg artifact ledger poisoned"))?;
        let OperationArtifacts { ledger, reuse, .. } = &mut *artifacts;
        reuse.find_statistics(ledger, basis)
    }
    #[cfg(test)]
    fn remember_reusable_statistics(
        &self,
        basis: super::model::StatisticsReuseBasis,
        output: super::model::ReusableStatisticsFile,
    ) -> Result<()> {
        let mut artifacts = self
            .operation
            .inner
            .artifacts
            .lock()
            .map_err(|_| internal("Iceberg artifact ledger poisoned"))?;
        let OperationArtifacts { ledger, reuse, .. } = &mut *artifacts;
        reuse.remember_statistics(ledger, basis, output)
    }
    fn record_manifest(
        &self,
        object: &ObjectIdentity,
        added_paths: &[String],
        existing_paths: &[String],
        replaces: &[String],
    ) -> Result<()> {
        self.operation
            .inner
            .artifacts
            .lock()
            .map_err(|_| internal("Iceberg artifact ledger poisoned"))?
            .ledger
            .record_manifest(object, added_paths, existing_paths, replaces)
    }
    fn registered_manifest_paths(&self, snapshot_id: i64) -> Result<Option<Arc<RegisteredPaths>>> {
        let artifacts = self
            .operation
            .inner
            .artifacts
            .lock()
            .map_err(|_| internal("Iceberg operation artifacts poisoned"))?;
        let Some(stage) = artifacts.ledger.registered_stage() else {
            return Ok(None);
        };
        if artifacts.stage_ids.get(&stage) != Some(&snapshot_id) {
            return Ok(None);
        }
        Ok(artifacts
            .ledger
            .registered_manifest_paths(stage, snapshot_id)?
            .cloned())
    }

    fn observe_registered_manifest(
        &self,
        snapshot_id: i64,
        replaces: &[String],
    ) -> Result<Option<RegisteredManifestObservation>> {
        let artifacts = self
            .operation
            .inner
            .artifacts
            .lock()
            .map_err(|_| internal("Iceberg operation artifacts poisoned"))?;
        let Some(stage) = artifacts.ledger.registered_stage() else {
            return Ok(None);
        };
        if artifacts.stage_ids.get(&stage) != Some(&snapshot_id) {
            return Ok(None);
        }
        artifacts
            .ledger
            .observe_registered_manifest(stage, snapshot_id, replaces)
    }

    fn record_registered_manifest(
        &self,
        object: &ObjectIdentity,
        observation: RegisteredManifestObservation,
        replaces: &[String],
    ) -> Result<()> {
        self.operation
            .inner
            .artifacts
            .lock()
            .map_err(|_| internal("Iceberg operation artifacts poisoned"))?
            .ledger
            .record_registered_manifest(object, observation, replaces)
    }

    fn record_manifest_list(
        &self,
        object: &ObjectIdentity,
        snapshot_id: i64,
        paths: &[String],
    ) -> Result<()> {
        self.operation
            .inner
            .artifacts
            .lock()
            .map_err(|_| internal("Iceberg artifact ledger poisoned"))?
            .ledger
            .record_manifest_list(object, snapshot_id, paths)
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
        let is_statistics_copy = {
            let mut artifacts = self
                .artifacts
                .lock()
                .map_err(|_| invalid("Iceberg artifact ledger poisoned"))?;
            artifacts.ledger.mark_writing(&object)?;
            artifacts.ledger.is_reusable_statistics(&object)
        };
        Ok(WritePermit::new(self, object).with_statistics_copy(is_statistics_copy))
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
mod tests;
