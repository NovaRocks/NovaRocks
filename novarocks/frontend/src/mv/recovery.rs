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

//! Frontend-owned convergence for historical MV staging attempts.
//!
//! The service intentionally treats every persisted attempt as historical:
//! it acquires one current control lease, asks the provider to inspect lake
//! truth, and may only perform proof-guarded staging cleanup. It never starts
//! a writer or replays publication.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use novarocks::mv::persistence::refresh::{
    FrontendMvRefreshActionPhase, FrontendMvRefreshActionState, FrontendMvRefreshCommittedVersion,
    FrontendMvRefreshEvidence, FrontendMvRefreshRecoveryBaseFact,
    FrontendMvRefreshRecoveryDisposition, FrontendMvRefreshRecoveryObservation,
    MvRefreshFinalizeRequest, MvRefreshLifecycleOwner, StoredMvRefresh,
};
use novarocks::mv::repository::{
    BeginFrontendMvRecoveryCycleRequest, FinalizeRecoveredMvRefreshRequest, MvRepository,
    RecordFrontendMvRecoveryCleanupOutcomeRequest, RecordFrontendMvRecoveryObservationRequest,
};
use novarocks_spi::connector::{
    ConnectorCancellation, ConnectorCommittedVersion, ConnectorControlRegistry,
    ConnectorHistoricalPublicationAction, ConnectorInstanceId, ConnectorMutationOperationId,
    ConnectorRequestContext, ConnectorStagedPublicationBaseFact,
    ConnectorStagedPublicationCleanupRequest, ConnectorStagedPublicationDescriptor,
    ConnectorStagedPublicationDisposition, ConnectorStagedPublicationObservation,
    ConnectorStagedPublicationPhase, ConnectorStagedPublicationPhaseState, ConnectorTableIdentity,
    ExternalMutationFinalization, ExternalMutationOutcome, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
    MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
};
use sha2::{Digest, Sha256};
use uuid::Uuid;

const RECOVERY_ACTION_DEADLINE: Duration = Duration::from_secs(30);
const MAX_RECOVERY_CANDIDATES: usize = 4096;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FrontendMvRecoverySummary {
    pub candidates: usize,
    pub resolved: usize,
    pub unresolved: usize,
    pub cleanup_backlog: usize,
}

pub(super) struct FrontendMvRecoveryDependencies {
    pub(super) connector_control: Arc<dyn ConnectorControlRegistry>,
}

pub(super) fn recover_once(
    repository: &dyn MvRepository,
    dependencies: &FrontendMvRecoveryDependencies,
) -> FrontendMvRecoverySummary {
    let mut summary = FrontendMvRecoverySummary::default();
    let candidates = match repository.list_frontend_recovery_candidates() {
        Ok(candidates) => candidates,
        Err(_) => {
            summary.unresolved = 1;
            return summary;
        }
    };
    for refresh in candidates.into_iter().take(MAX_RECOVERY_CANDIDATES) {
        summary.candidates += 1;
        match recover_one(repository, dependencies, refresh) {
            Ok(RecoveryResult::Resolved) => summary.resolved += 1,
            Ok(RecoveryResult::CleanupPending) => summary.cleanup_backlog += 1,
            Err(()) => summary.unresolved += 1,
        }
    }
    let remaining = MAX_RECOVERY_CANDIDATES.saturating_sub(summary.candidates);
    for refresh in repository
        .list_unfinished_branch_staged_iceberg_refreshes()
        .unwrap_or_default()
        .into_iter()
        .take(remaining)
    {
        summary.candidates += 1;
        match recover_legacy_one(repository, dependencies, refresh) {
            Ok(RecoveryResult::Resolved) => summary.resolved += 1,
            Ok(RecoveryResult::CleanupPending) => summary.cleanup_backlog += 1,
            Err(()) => summary.unresolved += 1,
        }
    }
    summary
}

enum RecoveryResult {
    Resolved,
    CleanupPending,
}

fn recover_one(
    repository: &dyn MvRepository,
    dependencies: &FrontendMvRecoveryDependencies,
    refresh: StoredMvRefresh,
) -> Result<RecoveryResult, ()> {
    if refresh.lifecycle_owner != MvRefreshLifecycleOwner::FrontendCurrent {
        return Err(());
    }
    let ledger = refresh.frontend_ledger.as_ref().ok_or(())?;
    let instance = ConnectorInstanceId::parse(&ledger.instance_id).map_err(|_| ())?;
    let lease = dependencies
        .connector_control
        .acquire_current(&instance)
        .map_err(|_| ())?;
    let cleanup_operation_id = refresh
        .frontend_recovery
        .as_ref()
        .map(|recovery| recovery.cleanup_operation_id.clone())
        .unwrap_or_else(|| Uuid::now_v7().into_bytes().to_vec());
    let recovered = repository
        .begin_frontend_recovery_cycle(BeginFrontendMvRecoveryCycleRequest {
            refresh_id: refresh.refresh_id,
            cycle_id: Uuid::now_v7().into_bytes().to_vec(),
            provider_id: lease
                .binding()
                .descriptor()
                .provider_id
                .as_str()
                .to_string(),
            instance_id: lease
                .binding()
                .descriptor()
                .instance_id
                .as_str()
                .to_string(),
            incarnation: lease.binding().incarnation().to_bytes().to_vec(),
            cleanup_operation_id,
        })
        .map_err(|_| ())?;
    let recovery = match lease.binding().staged_publication_recovery() {
        Some(recovery) => recovery,
        None => {
            return unresolved(
                repository,
                refresh.refresh_id,
                "connector has no staged publication recovery capability",
            );
        }
    };
    let descriptor = match descriptor_from_refresh(&recovered) {
        Ok(descriptor) => descriptor,
        Err(reason) => return unresolved(repository, refresh.refresh_id, &reason),
    };
    let context = match recovery_context() {
        Ok(context) => context,
        Err(reason) => return unresolved(repository, refresh.refresh_id, &reason),
    };
    let observation = match recovery.inspect(descriptor.clone(), context.clone()) {
        Ok(observation) => observation,
        Err(error) => return unresolved(repository, refresh.refresh_id, &error.to_string()),
    };
    let frontend_observation = match frontend_observation(&observation) {
        Ok(observation) => observation,
        Err(reason) => return unresolved(repository, refresh.refresh_id, &reason),
    };
    repository
        .record_frontend_recovery_observation(RecordFrontendMvRecoveryObservationRequest {
            refresh_id: refresh.refresh_id,
            observation: frontend_observation.clone(),
        })
        .map_err(|_| ())?;

    match observation.disposition {
        ConnectorStagedPublicationDisposition::Ambiguous => unresolved(
            repository,
            refresh.refresh_id,
            "provider could not prove MV publication disposition",
        ),
        ConnectorStagedPublicationDisposition::Published
        | ConnectorStagedPublicationDisposition::Superseded
        | ConnectorStagedPublicationDisposition::CleanupPending => {
            finalize_published(repository, &recovered, frontend_observation.clone())?;
            if observation.cleanup_required {
                match cleanup(
                    repository,
                    recovery.as_ref(),
                    &descriptor,
                    observation,
                    &recovered,
                    context,
                )? {
                    RecoveryResult::Resolved => {
                        finalize_published(repository, &recovered, frontend_observation)?;
                        Ok(RecoveryResult::Resolved)
                    }
                    pending => Ok(pending),
                }
            } else {
                Ok(RecoveryResult::Resolved)
            }
        }
        ConnectorStagedPublicationDisposition::KnownUncommitted
        | ConnectorStagedPublicationDisposition::Staged => {
            if observation.cleanup_required {
                match cleanup(
                    repository,
                    recovery.as_ref(),
                    &descriptor,
                    observation,
                    &recovered,
                    context,
                )? {
                    RecoveryResult::Resolved => {}
                    pending => return Ok(pending),
                }
            }
            repository
                .abort_recovered_uncommitted_refresh(refresh.refresh_id)
                .map_err(|_| ())?;
            Ok(RecoveryResult::Resolved)
        }
    }
}

fn recover_legacy_one(
    repository: &dyn MvRepository,
    dependencies: &FrontendMvRecoveryDependencies,
    refresh: StoredMvRefresh,
) -> Result<RecoveryResult, ()> {
    if refresh.lifecycle_owner != MvRefreshLifecycleOwner::LegacyCore {
        return Err(());
    }
    let instance =
        ConnectorInstanceId::parse(refresh.target_catalog.as_deref().ok_or(())?).map_err(|_| ())?;
    let lease = dependencies
        .connector_control
        .acquire_current(&instance)
        .map_err(|_| ())?;
    let recovery = lease.binding().staged_publication_recovery().ok_or(())?;
    let descriptor = legacy_descriptor(&refresh, lease.binding().incarnation())?;
    let context = recovery_context().map_err(|_| ())?;
    let observation = recovery
        .inspect(descriptor.clone(), context.clone())
        .map_err(|_| ())?;
    match observation.disposition {
        ConnectorStagedPublicationDisposition::Ambiguous => Err(()),
        ConnectorStagedPublicationDisposition::Published
        | ConnectorStagedPublicationDisposition::Superseded
        | ConnectorStagedPublicationDisposition::CleanupPending => {
            finalize_legacy_published(repository, &refresh, &observation)?;
            if observation.cleanup_required {
                legacy_cleanup(recovery.as_ref(), &descriptor, observation, context)
            } else {
                Ok(RecoveryResult::Resolved)
            }
        }
        ConnectorStagedPublicationDisposition::KnownUncommitted
        | ConnectorStagedPublicationDisposition::Staged => {
            if observation.cleanup_required {
                match legacy_cleanup(recovery.as_ref(), &descriptor, observation, context)? {
                    RecoveryResult::Resolved => {}
                    pending => return Ok(pending),
                }
            }
            repository
                .clear_refresh_progress(refresh.mv_id)
                .map_err(|_| ())?;
            Ok(RecoveryResult::Resolved)
        }
    }
}

fn legacy_cleanup(
    recovery: &dyn novarocks_spi::connector::ConnectorStagedPublicationRecovery,
    descriptor: &ConnectorStagedPublicationDescriptor,
    observation: ConnectorStagedPublicationObservation,
    context: ConnectorRequestContext,
) -> Result<RecoveryResult, ()> {
    let operation_id = ConnectorMutationOperationId::new();
    let outcome = recovery
        .cleanup(ConnectorStagedPublicationCleanupRequest {
            operation_id,
            descriptor_digest: descriptor.digest(),
            observation,
            context: context.clone(),
        })
        .map_err(|_| ())?;
    let outcome = match outcome {
        ExternalMutationOutcome::CommitUnknown { failure, evidence } => recovery
            .reconcile_cleanup(operation_id, evidence.clone(), context)
            .unwrap_or(ExternalMutationOutcome::CommitUnknown { failure, evidence }),
        outcome => outcome,
    };
    match outcome {
        ExternalMutationOutcome::KnownCommitted { finalization, .. } => Ok(
            if matches!(finalization, ExternalMutationFinalization::Complete) {
                RecoveryResult::Resolved
            } else {
                RecoveryResult::CleanupPending
            },
        ),
        ExternalMutationOutcome::KnownUncommitted { .. } => Err(()),
        ExternalMutationOutcome::CommitUnknown { .. } => Ok(RecoveryResult::CleanupPending),
    }
}

fn finalize_legacy_published(
    repository: &dyn MvRepository,
    refresh: &StoredMvRefresh,
    observation: &ConnectorStagedPublicationObservation,
) -> Result<(), ()> {
    let rows = i64::try_from(observation.resulting_row_count.ok_or(())?).map_err(|_| ())?;
    let snapshot = observation.target_snapshot_id.ok_or(())?;
    let finalize = MvRefreshFinalizeRequest {
        refresh_id: refresh.refresh_id,
        rows,
        base_snapshots: refresh.target_snapshots.clone(),
        base_table_uuids: refresh.base_table_uuids.clone(),
        target_snapshot_id: Some(snapshot),
    };
    use novarocks::mv::persistence::refresh::MvRefreshState;
    match refresh.state {
        MvRefreshState::IntentCreated => repository
            .record_external_commit_and_finalize(
                novarocks::mv::repository::RecordExternalCommitAndFinalizeRequest {
                    refresh_id: refresh.refresh_id,
                    external_outcome: novarocks::mv::persistence::refresh::RefreshExternalOutcome {
                        target_snapshot_id: Some(snapshot),
                        commit_id: format!("recovered-iceberg-snapshot-{snapshot}"),
                    },
                    finalize,
                },
            )
            .map_err(|_| ()),
        MvRefreshState::StagingCommitted => {
            repository
                .record_publish_commit(
                    novarocks::mv::persistence::refresh::RecordPublishCommitRequest {
                        refresh_id: refresh.refresh_id,
                        published_snapshot_id: snapshot,
                    },
                )
                .map_err(|_| ())?;
            repository.finalize_refresh(finalize).map_err(|_| ())
        }
        MvRefreshState::PublishCommitted => repository.finalize_refresh(finalize).map_err(|_| ()),
        MvRefreshState::Finalized => Ok(()),
        _ => Err(()),
    }
}

fn cleanup(
    repository: &dyn MvRepository,
    recovery: &dyn novarocks_spi::connector::ConnectorStagedPublicationRecovery,
    descriptor: &ConnectorStagedPublicationDescriptor,
    observation: ConnectorStagedPublicationObservation,
    refresh: &StoredMvRefresh,
    context: ConnectorRequestContext,
) -> Result<RecoveryResult, ()> {
    let operation_id = refresh
        .frontend_recovery
        .as_ref()
        .ok_or(())?
        .cleanup_operation_id
        .as_slice()
        .try_into()
        .map(ConnectorMutationOperationId::from_bytes)
        .map_err(|_| ())?;
    let outcome = recovery
        .cleanup(ConnectorStagedPublicationCleanupRequest {
            operation_id,
            descriptor_digest: descriptor.digest(),
            observation,
            context: context.clone(),
        })
        .map_err(|_| ())?;
    let outcome = match outcome {
        ExternalMutationOutcome::CommitUnknown { failure, evidence } => recovery
            .reconcile_cleanup(operation_id, evidence.clone(), context)
            .unwrap_or(ExternalMutationOutcome::CommitUnknown { failure, evidence }),
        outcome => outcome,
    };
    record_cleanup_outcome(repository, refresh.refresh_id, outcome)
}

fn record_cleanup_outcome(
    repository: &dyn MvRepository,
    refresh_id: i64,
    outcome: ExternalMutationOutcome<
        novarocks_spi::connector::ConnectorStagedPublicationCleanupReceipt,
    >,
) -> Result<RecoveryResult, ()> {
    match outcome {
        ExternalMutationOutcome::KnownCommitted { finalization, .. } => {
            repository
                .record_frontend_recovery_cleanup_outcome(
                    RecordFrontendMvRecoveryCleanupOutcomeRequest {
                        refresh_id,
                        state: FrontendMvRefreshActionState::KnownCommitted,
                        evidence: None,
                        provider_finalized: matches!(
                            finalization,
                            ExternalMutationFinalization::Complete
                        ),
                    },
                )
                .map_err(|_| ())?;
            Ok(
                if matches!(finalization, ExternalMutationFinalization::Complete) {
                    RecoveryResult::Resolved
                } else {
                    RecoveryResult::CleanupPending
                },
            )
        }
        ExternalMutationOutcome::KnownUncommitted { failure } => {
            repository
                .record_frontend_recovery_cleanup_outcome(
                    RecordFrontendMvRecoveryCleanupOutcomeRequest {
                        refresh_id,
                        state: FrontendMvRefreshActionState::KnownUncommitted,
                        evidence: None,
                        provider_finalized: false,
                    },
                )
                .map_err(|_| ())?;
            let _ = repository.record_frontend_recovery_unresolved(refresh_id, failure.to_string());
            Err(())
        }
        ExternalMutationOutcome::CommitUnknown { evidence, .. } => {
            let provider_payload = evidence.provider_payload();
            repository
                .record_frontend_recovery_cleanup_outcome(
                    RecordFrontendMvRecoveryCleanupOutcomeRequest {
                        refresh_id,
                        state: FrontendMvRefreshActionState::CommitUnknown,
                        evidence: Some(evidence_value(provider_payload.as_ref())),
                        provider_finalized: false,
                    },
                )
                .map_err(|_| ())?;
            // The caller may reconcile only while the retained lease is live;
            // a later process generation must re-inspect lake truth instead.
            Ok(RecoveryResult::CleanupPending)
        }
    }
}

fn finalize_published(
    repository: &dyn MvRepository,
    refresh: &StoredMvRefresh,
    observation: FrontendMvRefreshRecoveryObservation,
) -> Result<(), ()> {
    let mut recovery = refresh.frontend_recovery.clone().ok_or(())?;
    recovery.observation = Some(observation.clone());
    let rows = observation.resulting_row_count.ok_or(())?;
    let rows = i64::try_from(rows).map_err(|_| ())?;
    repository
        .finalize_recovered_published_refresh(FinalizeRecoveredMvRefreshRequest {
            finalize: MvRefreshFinalizeRequest {
                refresh_id: refresh.refresh_id,
                rows,
                base_snapshots: refresh.target_snapshots.clone(),
                base_table_uuids: refresh.base_table_uuids.clone(),
                target_snapshot_id: observation.target_snapshot_id,
            },
            recovery,
        })
        .map_err(|_| ())
}

fn unresolved(
    repository: &dyn MvRepository,
    refresh_id: i64,
    reason: &str,
) -> Result<RecoveryResult, ()> {
    repository
        .record_frontend_recovery_unresolved(refresh_id, reason.chars().take(4096).collect())
        .map_err(|_| ())?;
    Err(())
}

fn descriptor_from_refresh(
    refresh: &StoredMvRefresh,
) -> Result<ConnectorStagedPublicationDescriptor, String> {
    let ledger = refresh
        .frontend_ledger
        .as_ref()
        .ok_or_else(|| "frontend refresh is missing its v3 ledger".to_string())?;
    let table = ConnectorTableIdentity {
        instance_id: ConnectorInstanceId::parse(&ledger.instance_id)
            .map_err(|error| error.to_string())?,
        namespace: Arc::from(
            refresh
                .target_namespace
                .clone()
                .ok_or_else(|| "frontend refresh is missing target namespace".to_string())?,
        ),
        table: Arc::from(
            refresh
                .target_table
                .clone()
                .ok_or_else(|| "frontend refresh is missing target table".to_string())?,
        ),
    };
    let historical_binding = novarocks_spi::connector::ConnectorExecutionBindingKey {
        instance_id: table.instance_id.clone(),
        incarnation: ledger
            .incarnation
            .as_slice()
            .try_into()
            .map(novarocks_spi::connector::ConnectorInstanceIncarnation::from_bytes)
            .map_err(|_| {
                "frontend refresh has an invalid historical connector incarnation".to_string()
            })?,
    };
    let request_id = ledger
        .request_id
        .as_slice()
        .try_into()
        .map_err(|_| "frontend refresh has an invalid request ID".to_string())?;
    let cohorts = ledger
        .cohort_ids
        .iter()
        .map(|value| {
            hex::decode(value).map_err(|_| "frontend refresh has an invalid cohort ID".to_string())
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .map(|value| {
            value
                .as_slice()
                .try_into()
                .map_err(|_| "frontend refresh has an invalid cohort ID".to_string())
        })
        .collect::<Result<Vec<[u8; 32]>, _>>()?;
    let mut hasher = Sha256::new();
    for cohort in &cohorts {
        hasher.update(cohort);
    }
    let actions = [
        (
            FrontendMvRefreshActionPhase::StagingCreate,
            ConnectorStagedPublicationPhase::StagingCreate,
        ),
        (
            FrontendMvRefreshActionPhase::Write,
            ConnectorStagedPublicationPhase::Write,
        ),
        (
            FrontendMvRefreshActionPhase::Publication,
            ConnectorStagedPublicationPhase::Publication,
        ),
        (
            FrontendMvRefreshActionPhase::StagingDrop,
            ConnectorStagedPublicationPhase::StagingDrop,
        ),
    ]
    .into_iter()
    .map(|(phase, provider_phase)| {
        let action = ledger
            .actions
            .iter()
            .find(|action| action.phase == phase)
            .ok_or_else(|| format!("frontend refresh is missing prepared {phase:?} action"))?;
        Ok(ConnectorHistoricalPublicationAction {
            phase: provider_phase,
            state: match action.state {
                FrontendMvRefreshActionState::Prepared => {
                    ConnectorStagedPublicationPhaseState::Prepared
                }
                FrontendMvRefreshActionState::KnownUncommitted => {
                    ConnectorStagedPublicationPhaseState::KnownUncommitted
                }
                FrontendMvRefreshActionState::KnownCommitted => {
                    ConnectorStagedPublicationPhaseState::KnownCommitted
                }
                FrontendMvRefreshActionState::CommitUnknown => {
                    ConnectorStagedPublicationPhaseState::CommitUnknown
                }
            },
            operation_id: action
                .operation_id
                .as_slice()
                .try_into()
                .map(ConnectorMutationOperationId::from_bytes)
                .map_err(|_| "frontend refresh action has an invalid operation ID".to_string())?,
            committed_version: action
                .committed_version
                .as_ref()
                .map(connector_version)
                .transpose()?,
            evidence_digest: action
                .external_evidence
                .as_ref()
                .map(|evidence| evidence.digest.as_slice().try_into())
                .transpose()
                .map_err(|_| {
                    "frontend refresh action has an invalid evidence digest".to_string()
                })?,
        })
    })
    .collect::<Result<Vec<_>, String>>()?;
    let bases = refresh
        .target_snapshots
        .iter()
        .filter_map(|(table, to_version)| {
            refresh
                .base_table_uuids
                .get(table)
                .map(|uuid| ConnectorStagedPublicationBaseFact {
                    table: Arc::from(table.as_str()),
                    uuid: Arc::from(uuid.as_str()),
                    from_version: None,
                    to_version: *to_version,
                })
        })
        .collect();
    ConnectorStagedPublicationDescriptor::try_new(
        historical_binding,
        table,
        refresh
            .staging_branch
            .clone()
            .ok_or_else(|| "frontend refresh is missing staging ref".to_string())?,
        "main",
        ledger
            .expected_target_version
            .as_ref()
            .map(connector_version)
            .transpose()?,
        refresh.refresh_id,
        refresh.mv_id,
        request_id,
        refresh
            .marker
            .as_ref()
            .ok_or_else(|| "frontend refresh is missing marker".to_string())?
            .token
            .clone(),
        cohorts,
        hasher.finalize().into(),
        actions,
        bases,
    )
    .map_err(|error| error.to_string())
}

fn legacy_descriptor(
    refresh: &StoredMvRefresh,
    current_incarnation: novarocks_spi::connector::ConnectorInstanceIncarnation,
) -> Result<ConnectorStagedPublicationDescriptor, ()> {
    let instance_id =
        ConnectorInstanceId::parse(refresh.target_catalog.as_deref().ok_or(())?).map_err(|_| ())?;
    let table = ConnectorTableIdentity {
        instance_id: instance_id.clone(),
        namespace: Arc::from(refresh.target_namespace.as_deref().ok_or(())?),
        table: Arc::from(refresh.target_table.as_deref().ok_or(())?),
    };
    let marker = refresh.marker.as_ref().ok_or(())?;
    let mut identity_hasher = Sha256::new();
    identity_hasher.update(refresh.refresh_id.to_be_bytes());
    identity_hasher.update(refresh.mv_id.to_be_bytes());
    let identity: [u8; 32] = identity_hasher.finalize().into();
    let operation_id =
        ConnectorMutationOperationId::from_bytes(identity[..16].try_into().map_err(|_| ())?);
    let cohort: [u8; 32] = Sha256::digest(identity).into();
    let actions = [
        ConnectorStagedPublicationPhase::StagingCreate,
        ConnectorStagedPublicationPhase::Write,
        ConnectorStagedPublicationPhase::Publication,
        ConnectorStagedPublicationPhase::StagingDrop,
    ]
    .into_iter()
    .map(|phase| ConnectorHistoricalPublicationAction {
        phase,
        state: ConnectorStagedPublicationPhaseState::Prepared,
        operation_id,
        committed_version: None,
        evidence_digest: None,
    })
    .collect();
    let bases = refresh
        .target_snapshots
        .iter()
        .filter_map(|(table, to_version)| {
            refresh
                .base_table_uuids
                .get(table)
                .map(|uuid| ConnectorStagedPublicationBaseFact {
                    table: Arc::from(table.as_str()),
                    uuid: Arc::from(uuid.as_str()),
                    from_version: None,
                    to_version: *to_version,
                })
        })
        .collect();
    ConnectorStagedPublicationDescriptor::try_new(
        novarocks_spi::connector::ConnectorExecutionBindingKey {
            instance_id,
            incarnation: current_incarnation,
        },
        table,
        refresh.staging_branch.as_deref().ok_or(())?,
        "main",
        None,
        refresh.refresh_id,
        refresh.mv_id,
        identity[..16].try_into().map_err(|_| ())?,
        marker.token.clone(),
        vec![cohort],
        Sha256::digest(cohort).into(),
        actions,
        bases,
    )
    .map_err(|_| ())
}

fn connector_version(
    version: &FrontendMvRefreshCommittedVersion,
) -> Result<ConnectorCommittedVersion, String> {
    ConnectorCommittedVersion::try_new(
        Bytes::copy_from_slice(&version.payload),
        version.snapshot_id,
    )
    .map_err(|error| error.to_string())
}

fn frontend_observation(
    observation: &ConnectorStagedPublicationObservation,
) -> Result<FrontendMvRefreshRecoveryObservation, String> {
    Ok(FrontendMvRefreshRecoveryObservation {
        disposition: match observation.disposition {
            ConnectorStagedPublicationDisposition::KnownUncommitted => {
                FrontendMvRefreshRecoveryDisposition::KnownUncommitted
            }
            ConnectorStagedPublicationDisposition::Staged => {
                FrontendMvRefreshRecoveryDisposition::Staged
            }
            ConnectorStagedPublicationDisposition::Published => {
                FrontendMvRefreshRecoveryDisposition::Published
            }
            ConnectorStagedPublicationDisposition::Superseded => {
                FrontendMvRefreshRecoveryDisposition::Superseded
            }
            ConnectorStagedPublicationDisposition::CleanupPending => {
                FrontendMvRefreshRecoveryDisposition::CleanupPending
            }
            ConnectorStagedPublicationDisposition::Ambiguous => {
                FrontendMvRefreshRecoveryDisposition::Ambiguous
            }
        },
        digest: observation.digest().to_vec(),
        proof: evidence_value(observation.proof.payload().as_ref()),
        committed_version: observation
            .committed_version
            .as_ref()
            .map(|version| {
                FrontendMvRefreshCommittedVersion::try_new(
                    version.payload().to_vec(),
                    version.snapshot_id(),
                )
            })
            .transpose()?,
        resulting_row_count: observation
            .resulting_row_count
            .map(i64::try_from)
            .transpose()
            .map_err(|_| "recovered MV row count exceeds StateStore representation".to_string())?,
        bases: observation
            .bases
            .iter()
            .map(|base| FrontendMvRefreshRecoveryBaseFact {
                table: base.table.to_string(),
                uuid: base.uuid.to_string(),
                from_snapshot: base.from_version,
                to_snapshot: base.to_version,
            })
            .collect(),
        definition_fingerprint: observation
            .definition_fingerprint
            .as_ref()
            .map(ToString::to_string),
        staging_snapshot_id: observation.staging_snapshot_id,
        target_snapshot_id: observation.target_snapshot_id,
        cleanup_required: observation.cleanup_required,
    })
}

fn evidence_value(payload: &[u8]) -> FrontendMvRefreshEvidence {
    FrontendMvRefreshEvidence {
        payload: payload.to_vec(),
        digest: Sha256::digest(payload).to_vec(),
    }
}

fn recovery_context() -> Result<ConnectorRequestContext, String> {
    ConnectorRequestContext::try_new(
        Instant::now() + RECOVERY_ACTION_DEADLINE,
        Arc::new(NeverCancelled),
        MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .map_err(|error| error.to_string())
}

struct NeverCancelled;
impl ConnectorCancellation for NeverCancelled {
    fn is_cancelled(&self) -> bool {
        false
    }
}
