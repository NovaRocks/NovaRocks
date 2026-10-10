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

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::access_binding::IcebergReadBinding;
use crate::fs_io::{
    IcebergWriteAdmission, StreamExitFailure, WritePermit, build_admitted_file_io_for_location,
};
use crate::iceberg::io::FileIO;
use crate::iceberg::{Error, ErrorKind, Result};
use crate::resources::IcebergCatalogRuntime;
use novarocks_fs::FileCancellation;
use novarocks_spi::connector::{ConnectorRequestContext, ConnectorStopOwner};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::model::{
    ArtifactClass, ArtifactKind, ArtifactLedger, ArtifactRecord, ArtifactWriteState,
    ArtifactWriter, AttemptArtifacts, AttemptToken, CleanupRemainingReason, CleanupScope,
    IcebergCleanupReport, ObjectIdentity, OperationToken, RemainingArtifact,
};

#[derive(Clone, Copy, Debug)]
pub(crate) struct CleanupBudget {
    pub time: Duration,
    pub objects: usize,
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
                objects: 1024,
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
    cancellation: FileCancellation,
    runtime: IcebergCatalogRuntime,
    permits: Arc<Semaphore>,
    limits: OperationLimits,
    ledger: Mutex<ArtifactLedger>,
    publication_references: Mutex<BTreeSet<ObjectIdentity>>,
    exit_failures: Mutex<BTreeMap<ObjectIdentity, StreamExitFailure>>,
    retired_objects: Mutex<BTreeSet<ObjectIdentity>>,
    attempt_ordinal: AtomicU32,
    object_ordinal: AtomicU64,
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
        if location.trim().is_empty()
            || limits.io_requests == 0
            || limits.cleanup.time.is_zero()
            || limits.cleanup.objects == 0
        {
            return Err(invalid(
                "Invalid Iceberg commit operation limits or location",
            ));
        }
        let cancellation = FileCancellation::from_connector_request(&request);
        Ok(Self {
            inner: Arc::new(OperationState {
                token,
                location,
                binding,
                request,
                cancellation,
                runtime,
                permits: Arc::new(Semaphore::new(limits.io_requests as usize)),
                limits,
                ledger: Mutex::new(ArtifactLedger::new(token)),
                publication_references: Mutex::new(BTreeSet::new()),
                exit_failures: Mutex::new(BTreeMap::new()),
                retired_objects: Mutex::new(BTreeSet::new()),
                attempt_ordinal: AtomicU32::new(0),
                object_ordinal: AtomicU64::new(0),
            }),
        })
    }

    pub(crate) fn token(&self) -> OperationToken {
        self.inner.token
    }
    pub(crate) fn runtime(&self) -> &IcebergCatalogRuntime {
        &self.inner.runtime
    }
    pub(crate) fn check_active(&self) -> Result<()> {
        self.inner.check_active()
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
            self.inner.binding.for_request(self.inner.request.clone()),
            admission,
        );
        Ok(IcebergCommitAttempt {
            operation: self.clone(),
            token,
            file_io,
        })
    }

    /// An existing session file is adopted explicitly; registration never grants another write.
    pub(crate) fn adopt_session_data(&self, object: ObjectIdentity) -> Result<()> {
        self.inner
            .ledger
            .lock()
            .map_err(|_| invalid("Iceberg artifact ledger poisoned"))?
            .register(ArtifactRecord {
                object,
                class: ArtifactClass::SessionData,
                attempt: None,
                write_state: ArtifactWriteState::Written,
            })
    }

    /// Adopt one provider-authored publication object with an explicit lifetime
    /// beyond successful dispatch. External registered data cannot use this API.
    pub(crate) fn adopt_operation_reference(&self, object: ObjectIdentity) -> Result<()> {
        self.inner
            .ledger
            .lock()
            .map_err(|_| invalid("Iceberg artifact ledger poisoned"))?
            .register(ArtifactRecord {
                object: object.clone(),
                class: ArtifactClass::Operation,
                attempt: None,
                write_state: ArtifactWriteState::Written,
            })?;
        self.inner
            .publication_references
            .lock()
            .map_err(|_| invalid("Iceberg publication references poisoned"))?
            .insert(object);
        Ok(())
    }

    pub(crate) fn publication_references(&self) -> Result<Vec<ObjectIdentity>> {
        Ok(self
            .inner
            .publication_references
            .lock()
            .map_err(|_| invalid("Iceberg publication references poisoned"))?
            .iter()
            .cloned()
            .collect())
    }

    pub(crate) fn artifacts(&self) -> Result<Vec<ArtifactRecord>> {
        Ok(self
            .inner
            .ledger
            .lock()
            .map_err(|_| invalid("Iceberg artifact ledger poisoned"))?
            .records()
            .cloned()
            .collect())
    }

    /// Unknown publication retains readable ledger facts even after a poisoned mutation lock.
    /// Reading them never repairs the ledger or grants cleanup/mutation authority.
    pub(crate) fn recovery_artifacts(&self) -> Vec<ArtifactRecord> {
        self.inner
            .ledger
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .records()
            .cloned()
            .collect()
    }

    pub(crate) async fn pause(&self, duration: Duration) -> Result<()> {
        self.check_active()?;
        tokio::select! {
            biased;
            error = self.inner.cancellation.ended() => Err(stopped_error(error)),
            _ = tokio::time::sleep(duration) => self.check_active(),
        }
    }

    /// Call after preparation has unwound and publication has proved that this scope is uncommitted.
    /// Unknown publication must never call this method.
    pub(crate) async fn cleanup(&self, scope: CleanupScope) -> IcebergCleanupReport {
        let records = match self.inner.ledger.lock() {
            Ok(ledger) => ledger.selected(scope),
            Err(_) => return IcebergCleanupReport::NotAttempted,
        };
        self.cleanup_records(records).await
    }

    /// Only a known committed request authorizes deletion of unreachable owned objects.
    /// Physical identity is used here: a carried blob keeps its entire shared Puffin alive.
    pub(crate) async fn cleanup_after_commit(
        &self,
        retained: &[ObjectIdentity],
    ) -> IcebergCleanupReport {
        let retained: BTreeSet<_> = retained.iter().collect();
        let records = match self.inner.ledger.lock() {
            Ok(ledger) => ledger
                .records()
                .filter(|record| !retained.contains(&record.object))
                .cloned()
                .collect(),
            Err(_) => return IcebergCleanupReport::NotAttempted,
        };
        self.cleanup_records(records).await
    }

    async fn cleanup_records(&self, records: Vec<ArtifactRecord>) -> IcebergCleanupReport {
        if records.is_empty() {
            return IcebergCleanupReport::Complete { deleted: 0 };
        }
        // Revocation precedes waiting: an already-admitted writer waiting for capacity
        // cannot start writing one of these objects after cleanup releases all permits.
        match self.inner.retired_objects.lock() {
            Ok(mut retired) => retired.extend(records.iter().map(|r| r.object.clone())),
            Err(_) => return IcebergCleanupReport::NotAttempted,
        }
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
        let mut deleted = 0;
        let mut remaining = Vec::new();
        for (index, record) in records.iter().enumerate() {
            let exit_failure = match self.inner.exit_failures.lock() {
                Ok(failures) => failures.get(&record.object).cloned(),
                Err(_) => Some(StreamExitFailure::Unconfirmed),
            };
            if let Some(reason) = exit_failure {
                remaining.push(RemainingArtifact {
                    object: record.object.clone(),
                    reason: CleanupRemainingReason::DeleteFailed(reason.to_string()),
                });
                continue;
            }
            if index >= self.inner.limits.cleanup.objects || Instant::now() >= deadline {
                remaining.extend(records[index..].iter().map(|record| RemainingArtifact {
                    object: record.object.clone(),
                    reason: CleanupRemainingReason::BudgetExhausted,
                }));
                break;
            }
            let result = async {
                let access =
                    crate::fs_io::resolve_access_for_location(record.object.path(), &binding)
                        .map_err(|e| invalid(format!("Resolve cleanup object: {e}")))?;
                let path = access.single_relative_path().map_err(invalid)?;
                let left = deadline.saturating_duration_since(Instant::now());
                let operator = access.operator().layer(
                    crate::opendal::layers::TimeoutLayer::new()
                        .with_timeout(left)
                        .with_io_timeout(left),
                );
                operator.delete(path).await.map_err(|e| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "Iceberg artifact cleanup delete failed",
                    )
                    .with_source(e)
                })
            };
            // A DELETE timeout is missing completion evidence, not a successful removal. Its
            // fresh deleter is never reused after TimeoutLayer interrupts RetryWrapper::flush.
            // No background task is spawned; an external idempotent DELETE may still finish.
            match tokio::time::timeout_at(until, result).await {
                Ok(Ok(())) => {
                    if let Ok(mut ledger) = self.inner.ledger.lock() {
                        ledger.forget_removed(&record.object);
                        deleted += 1;
                    } else {
                        remaining.push(RemainingArtifact {
                            object: record.object.clone(),
                            reason: CleanupRemainingReason::DeleteFailed(
                                "Artifact ledger poisoned".into(),
                            ),
                        });
                    }
                }
                Ok(Err(error)) => remaining.push(RemainingArtifact {
                    object: record.object.clone(),
                    reason: CleanupRemainingReason::DeleteFailed(error.to_string()),
                }),
                Err(_) => remaining.push(RemainingArtifact {
                    object: record.object.clone(),
                    reason: CleanupRemainingReason::BudgetExhausted,
                }),
            }
        }
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
    pub(crate) fn operation_artifacts(&self) -> Result<Vec<ArtifactRecord>> {
        self.operation.artifacts()
    }

    pub(crate) fn publication_references(&self) -> Result<Vec<ObjectIdentity>> {
        self.operation.publication_references()
    }
}

impl ArtifactWriter for IcebergCommitAttempt {
    fn file_io(&self) -> &FileIO {
        &self.file_io
    }
    fn attempt_token(&self) -> AttemptToken {
        self.token
    }
    fn check_active(&self) -> Result<()> {
        self.operation.check_active()
    }
    fn allocate(&self, class: ArtifactClass, kind: ArtifactKind) -> Result<ObjectIdentity> {
        self.check_active()?;
        let attempt = match class {
            ArtifactClass::Attempt => Some(self.token),
            ArtifactClass::Operation => None,
            _ => {
                return Err(invalid(
                    "Only operation and attempt artifacts may be allocated",
                ));
            }
        };
        let ordinal = self
            .operation
            .inner
            .object_ordinal
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
            .map_err(|_| invalid("Iceberg object ordinal exhausted"))?;
        let scope = attempt
            .map(|t| t.path_component())
            .unwrap_or_else(|| self.operation.token().path_component());
        let object = ObjectIdentity::new(format!(
            "{}/metadata/{}-{}.{}",
            self.operation.inner.location.trim_end_matches('/'),
            scope,
            ordinal,
            kind.extension()
        ))?;
        self.operation
            .inner
            .ledger
            .lock()
            .map_err(|_| invalid("Iceberg artifact ledger poisoned"))?
            .register(ArtifactRecord {
                object: object.clone(),
                class,
                attempt,
                write_state: ArtifactWriteState::Allocated,
            })?;
        Ok(object)
    }
    fn snapshot_artifacts(&self, owned_references: &[ObjectIdentity]) -> Result<AttemptArtifacts> {
        self.operation
            .inner
            .ledger
            .lock()
            .map_err(|_| invalid("Iceberg artifact ledger poisoned"))?
            .snapshot_for_attempt(self.token, owned_references.iter().cloned())
    }
}

#[async_trait::async_trait]
impl IcebergWriteAdmission for OperationState {
    fn check_active(&self) -> Result<()> {
        self.cancellation.check().map_err(stopped_error)
    }
    fn check_output_active(&self, object: &ObjectIdentity) -> Result<()> {
        self.check_active()?;
        if self
            .retired_objects
            .lock()
            .map_err(|_| invalid("Iceberg retired artifact set poisoned"))?
            .contains(object)
        {
            return Err(invalid(
                "Iceberg artifact has entered cleanup and cannot start another write",
            ));
        }
        Ok(())
    }
    fn cancellation(&self) -> FileCancellation {
        self.cancellation.clone()
    }
    fn runtime(&self) -> IcebergCatalogRuntime {
        self.runtime.clone()
    }
    fn admit(self: Arc<Self>, path: &str) -> Result<WritePermit> {
        self.check_active()?;
        let object = ObjectIdentity::new(path)?;
        self.check_output_active(&object)?;
        self.ledger
            .lock()
            .map_err(|_| invalid("Iceberg artifact ledger poisoned"))?
            .mark_writing(&object)?;
        Ok(WritePermit::new(self, object))
    }
    fn mark_written(&self, object: &ObjectIdentity) -> Result<()> {
        self.ledger
            .lock()
            .map_err(|_| invalid("Iceberg artifact ledger poisoned"))?
            .mark_written(object)
    }
    fn record_exit_failure(&self, object: &ObjectIdentity, failure: StreamExitFailure) {
        if let Ok(mut failures) = self.exit_failures.lock() {
            if !matches!(failure, StreamExitFailure::Unconfirmed) || !failures.contains_key(object)
            {
                failures.insert(object.clone(), failure);
            }
        }
    }
    async fn io_permit(&self) -> Result<OwnedSemaphorePermit> {
        self.check_active()?;
        let permit = tokio::select! {
            biased;
            error = self.cancellation.ended() => return Err(stopped_error(error)),
            permit = self.permits.clone().acquire_owned() => permit.map_err(|_|
                invalid("Iceberg operation I/O admission closed"))?,
        };
        self.check_active()?;
        Ok(permit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use novarocks_fs::{FsAccessResolver, TokioFileIoRuntime, TokioFileTaskSpawner};
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
            Arc::new(TokioFileTaskSpawner::new(runtime.clone())),
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
        let a = attempt
            .allocate(ArtifactClass::Attempt, ArtifactKind::Manifest)
            .unwrap();
        let b = second
            .allocate(ArtifactClass::Attempt, ArtifactKind::Manifest)
            .unwrap();
        assert_ne!(a, b);
        assert!(a.path().contains(&attempt.attempt_token().path_component()));
        assert!(
            attempt
                .allocate(ArtifactClass::ExternalRegistered, ArtifactKind::Manifest)
                .is_err()
        );
        assert!(
            attempt
                .allocate(ArtifactClass::SessionData, ArtifactKind::Manifest)
                .is_err()
        );
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
        assert!(
            attempt
                .allocate(ArtifactClass::Attempt, ArtifactKind::Manifest)
                .is_err()
        );
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
        let path = attempt
            .allocate(ArtifactClass::Attempt, ArtifactKind::Statistics)
            .unwrap();
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
        let dir = tempfile::tempdir().unwrap();
        let stop = ConnectorStopOwner::new();
        let operation = operation(
            &format!("file://{}", dir.path().display()),
            &stop,
            OperationLimits {
                cleanup: CleanupBudget {
                    time: Duration::from_secs(30),
                    objects: 1,
                },
                ..OperationLimits::default()
            },
            Instant::now() + Duration::from_secs(60),
        );
        let attempt = operation.begin_attempt().unwrap();
        for _ in 0..3 {
            let path = attempt
                .allocate(ArtifactClass::Attempt, ArtifactKind::Manifest)
                .unwrap();
            attempt
                .file_io()
                .new_output(path.path())
                .unwrap()
                .write(Bytes::new())
                .await
                .unwrap();
        }
        stop.request_stop();
        let report = operation.cleanup(CleanupScope::EntireOperation).await;
        let IcebergCleanupReport::Partial { deleted, remaining } = report else {
            panic!("budget remainder");
        };
        assert_eq!(deleted, 1);
        assert_eq!(remaining.len(), 2);
        assert!(
            remaining
                .iter()
                .all(|r| r.reason == CleanupRemainingReason::BudgetExhausted)
        );
        assert_eq!(operation.artifacts().unwrap().len(), 2);
        for _ in 0..2 {
            operation.cleanup(CleanupScope::EntireOperation).await;
        }
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
        let path = attempt
            .allocate(ArtifactClass::Attempt, ArtifactKind::Statistics)
            .unwrap();
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
        let path = attempt
            .allocate(ArtifactClass::Attempt, ArtifactKind::Manifest)
            .unwrap();
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
                .retired_objects
                .lock()
                .unwrap()
                .contains(&path)
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

    #[tokio::test]
    async fn cleanup_delete_timeout_keeps_unknown_object_without_background_work() {
        use std::io::{BufRead, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let (request_seen, requests) = std::sync::mpsc::channel();
        let (release, wait_release) = std::sync::mpsc::channel();
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
        let runtime = tokio::runtime::Handle::current();
        let binding = IcebergReadBinding::new(
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
                timeout_ms: Some(2000),
                io_timeout_ms: Some(2000),
            }),
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime.clone())),
        );
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
                    objects: 1,
                },
            },
        )
        .unwrap();
        let object = ObjectIdentity::new("s3://bucket/table/data/owned.parquet").unwrap();
        operation.adopt_session_data(object.clone()).unwrap();
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
                    objects: 10,
                },
            },
            Instant::now() + Duration::from_secs(60),
        );
        let attempt = operation.begin_attempt().unwrap();
        let path = attempt
            .allocate(ArtifactClass::Attempt, ArtifactKind::Statistics)
            .unwrap();
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
