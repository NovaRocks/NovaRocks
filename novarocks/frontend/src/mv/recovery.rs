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

use crate::mv::domain::persistence::refresh::{
    FrontendMvRefreshActionPhase, FrontendMvRefreshActionState, FrontendMvRefreshCommittedVersion,
    FrontendMvRefreshEvidence, FrontendMvRefreshRecoveryBaseFact,
    FrontendMvRefreshRecoveryDisposition, FrontendMvRefreshRecoveryObservation,
    MvRefreshFinalizeRequest, MvRefreshLifecycleOwner, StoredMvRefresh,
};
use crate::mv::domain::repository::{
    BeginFrontendMvRecoveryCycleRequest, FinalizeRecoveredMvRefreshRequest, MvRepository,
    RecordFrontendMvRecoveryCleanupOutcomeRequest, RecordFrontendMvRecoveryObservationRequest,
};
use bytes::Bytes;
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
    pub(super) provider_activation: Arc<super::refresh::FrontendMvRefreshProviderActivationPort>,
    /// Cluster-wide refresh ownership, shared with the refresh path.
    ///
    /// Recovery writes durable state, so it competes for the same lease as a
    /// refresh does. It is not exempt: a crashed frontend's attempts may still be
    /// being reconciled by a *different* surviving frontend, and two reconcilers
    /// on one target is the same split-brain as two refreshers.
    pub(super) ownership: Option<super::coordination::MvRefreshOwnershipContext>,
}

/// Takes ownership of one recovery candidate's target, or declines it.
///
/// Declining is the safe answer: a candidate this frontend cannot own is left
/// for whoever does own it, and recovery reports it as unresolved rather than
/// reconciling it without the right to.
fn own_recovery_candidate(
    dependencies: &FrontendMvRecoveryDependencies,
    lease: &novarocks_spi::connector::ConnectorControlPlanningLease,
    refresh: &StoredMvRefresh,
) -> Result<Option<super::coordination::OwnedRefresh>, ()> {
    let Some(context) = dependencies.ownership.as_ref() else {
        return Ok(None);
    };
    let instance =
        ConnectorInstanceId::parse(refresh.target_catalog.as_deref().ok_or(())?).map_err(|_| ())?;
    let resource = super::coordination::resolve_target_resource_for(
        lease.binding(),
        ConnectorTableIdentity {
            instance_id: instance,
            namespace: Arc::from(refresh.target_namespace.as_deref().ok_or(())?),
            table: Arc::from(refresh.target_table.as_deref().ok_or(())?),
        },
        &recovery_context().map_err(|_| ())?,
    )
    .map_err(|_| ())?;
    // Wait out a previous owner's lease rather than skipping the candidate.
    //
    // This is the crash case recovery exists for: the frontend that staged this
    // attempt died holding the target's refresh lease, and a lease cannot be
    // reclaimed early without reintroducing the split-brain it prevents. So the
    // only correct move is to wait for it to age out.
    //
    // Skipping instead would strand the target permanently: recovery is a
    // one-shot startup pass, so a candidate declined here is never revisited, and
    // every later refresh conflicts with the attempt nobody reconciled. That is
    // not a hypothetical -- it is what this path did before the wait was added,
    // and `cross_process_three_be_mvx3_recovery_reconciles_staged_and_published_\
    // attempts` fails exactly that way without it.
    let deadline = Instant::now() + RECOVERY_OWNERSHIP_WAIT;
    loop {
        match context.block_on_acquisition(super::coordination::acquire_refresh_ownership(
            context,
            refresh.mv_id,
            resource.clone(),
        )) {
            Ok(owned) => return Ok(Some(owned)),
            // A live owner that is not us. Either it is still running and will
            // reconcile its own attempt, or it is gone and its lease is aging out.
            Err(
                super::coordination::OwnershipRefusal::Contended
                | super::coordination::OwnershipRefusal::AwaitingTakeover,
            ) if Instant::now() < deadline => {
                std::thread::sleep(RECOVERY_OWNERSHIP_POLL);
            }
            Err(_) => return Err(()),
        }
    }
}

/// How long startup recovery waits for a crashed owner's refresh lease.
///
/// Covers the frontend lease duration plus its takeover observation, with slack.
/// Recovery runs on the MV background worker, not the SQL admission path, so a
/// wait here delays reconciliation rather than the frontend accepting queries.
const RECOVERY_OWNERSHIP_WAIT: Duration = Duration::from_secs(30);
const RECOVERY_OWNERSHIP_POLL: Duration = Duration::from_millis(500);

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
    // Before any durable transition: the repository proves ownership inside each
    // transaction, so reconciling without it fails at commit rather than here.
    let _owned = own_recovery_candidate(dependencies, &lease, &refresh)?;
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
            let committed_partitioning = observation.committed_partitioning.clone();
            finalize_published(
                repository,
                dependencies,
                &recovered,
                frontend_observation.clone(),
                committed_partitioning.as_ref(),
                &context,
            )?;
            if observation.cleanup_required {
                match cleanup(
                    repository,
                    recovery.as_ref(),
                    &descriptor,
                    observation,
                    &recovered,
                    context.clone(),
                )? {
                    RecoveryResult::Resolved => {
                        // Cleanup persists its terminal evidence after the first
                        // publication finalize. Reload that state before the
                        // second finalize so the stale cycle-start value cannot
                        // overwrite KnownCommitted back to CleanupPending.
                        let recovered = repository
                            .load_refresh(refresh.refresh_id)
                            .map_err(|_| ())?
                            .ok_or(())?;
                        finalize_published(
                            repository,
                            dependencies,
                            &recovered,
                            frontend_observation,
                            committed_partitioning.as_ref(),
                            &context,
                        )?;
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
        partition_spec: None,
    };
    use crate::mv::domain::persistence::refresh::MvRefreshState;
    match refresh.state {
        MvRefreshState::IntentCreated => repository
            .record_external_commit_and_finalize(
                crate::mv::domain::repository::RecordExternalCommitAndFinalizeRequest {
                    refresh_id: refresh.refresh_id,
                    external_outcome:
                        crate::mv::domain::persistence::refresh::RefreshExternalOutcome {
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
                    crate::mv::domain::persistence::refresh::RecordPublishCommitRequest {
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
    dependencies: &FrontendMvRecoveryDependencies,
    refresh: &StoredMvRefresh,
    observation: FrontendMvRefreshRecoveryObservation,
    committed_partitioning: Option<&novarocks_spi::connector::ConnectorCommittedPartitioning>,
    connector_context: &ConnectorRequestContext,
) -> Result<(), ()> {
    let mut recovery = refresh.frontend_recovery.clone().ok_or(())?;
    recovery.observation = Some(observation.clone());
    let rows = observation.resulting_row_count.ok_or(())?;
    let rows = i64::try_from(rows).map_err(|_| ())?;
    // The current target head may have advanced beyond this publication after
    // the marker commit. Durable MV refresh facts must retain the exact marker
    // snapshot proven by the committed version, not the later observed head.
    let committed_snapshot_id = observation
        .committed_version
        .as_ref()
        .and_then(|version| version.snapshot_id)
        .ok_or(())?;
    let partition_spec = committed_partitioning
        .map(super::refresh::mv_partition_contract)
        .transpose()
        .map_err(|_| ())?;
    if let Some(committed_partitioning) = committed_partitioning {
        let partition_spec = partition_spec.as_ref().ok_or(())?;
        dependencies
            .provider_activation
            .sync_repartition_descriptor(
                refresh.mv_id,
                partition_spec.clone(),
                committed_partitioning.clone(),
                connector_context,
            )
            .map_err(|_| ())?;
    }
    repository
        .finalize_recovered_published_refresh(FinalizeRecoveredMvRefreshRequest {
            finalize: MvRefreshFinalizeRequest {
                refresh_id: refresh.refresh_id,
                rows,
                base_snapshots: refresh.target_snapshots.clone(),
                base_table_uuids: refresh.base_table_uuids.clone(),
                target_snapshot_id: Some(committed_snapshot_id),
                partition_spec,
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::num::NonZeroUsize;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::mv::domain::dependency::model::{
        MvDependencyObjectRef, MvDependencyObjectType, MvDependencyStorageEngine,
    };
    use crate::mv::domain::persistence::definition::{
        CreateMvDefinitionRequest, StoredMvRefreshPolicy,
    };
    use crate::mv::domain::persistence::dependency::CreateMvDependencyRequest;
    use crate::mv::domain::persistence::refresh::{
        FrontendMvRefreshAction, FrontendMvRefreshActionPhase, FrontendMvRefreshActionState,
        FrontendMvRefreshLedger, FrontendMvRefreshRecoveryStatus, MvRefreshState,
    };
    use crate::mv::domain::persistence::schema::{
        BaseContract, BaseFieldRecord, BaseSchemaSnapshot, ExpressionKind, ExpressionLineage,
        HiddenApplyKeyContract, MvPartitionContract, MvPartitionFieldContract,
        MvPartitionTransformContract, MvSchemaContract, OutputColumnLineage, OutputContract,
        TargetContract, TargetVisibleColumn,
    };
    use crate::mv::domain::repository::{
        BeginFrontendMvRefreshIntentRequest, CreateMvRepositoryRequest,
        InitialMvRefreshConfiguration, MvRepository,
    };
    use novarocks_spi::connector::{
        ConnectorCommittedPartitionField, ConnectorCommittedPartitioning,
        ConnectorCommittedVersion, ConnectorExecutionBindingKey, ConnectorInstanceDescriptor,
        ConnectorManagedPartitionTransform, ConnectorMutationFailure, ConnectorMutationFailureKind,
        ConnectorProviderId, ConnectorStagedPublicationCleanupReceipt,
        ConnectorStagedPublicationProof, ConnectorStagedPublicationRecovery,
        ExternalMutationEffect, ExternalMutationEvidence,
    };
    use novarocks_spi::state_store::FeDeploymentView;
    use novarocks_sql::planning::mv::ApplyKeySource;
    use novarocks_state_store::{
        StateStoreAppConfig, StateStoreConfig, StateStoreHost, StateStoreHostConfig,
        StateStoreLimitOverrides, StateStoreProviderConfig, builtin_state_store_provider_registry,
    };

    use super::*;
    use crate::connector::ConnectorControlHost;
    use crate::connector::control_host::tests::test_control_binding;
    use crate::mv::repository::StateStoreMvRepository;

    struct TestEnvironment {
        _temp: tempfile::TempDir,
        _runtime: tokio::runtime::Runtime,
        _host: StateStoreHost,
        repository: Arc<StateStoreMvRepository>,
    }

    impl TestEnvironment {
        fn open() -> Self {
            let temp = tempfile::tempdir().expect("temporary StateStore directory");
            let runtime = tokio::runtime::Runtime::new().expect("repository runtime");
            let registry =
                builtin_state_store_provider_registry().expect("built-in StateStore providers");
            let host = runtime
                .block_on(StateStoreHost::open(
                    &registry,
                    StateStoreHostConfig {
                        state_store: StateStoreAppConfig {
                            store: StateStoreConfig {
                                cluster_id: "mv-recovery-focused-test".to_string(),
                                limits: StateStoreLimitOverrides::default(),
                                provider: StateStoreProviderConfig::Sqlite {
                                    path: temp.path().join("state-store.sqlite"),
                                    deployment_owner: "mv-recovery-focused-test".to_string(),
                                },
                            },
                            mysql_client: None,
                        },
                        foundationdb_client: None,
                    },
                    FeDeploymentView {
                        active_fe_count: NonZeroUsize::new(1).expect("one FE"),
                        topology_revision: Bytes::from_static(b"mv-recovery-focused-test-r1"),
                    },
                    Instant::now() + Duration::from_secs(5),
                ))
                .expect("open SQLite StateStore host");
            let repository = runtime
                .block_on(StateStoreMvRepository::open(
                    host.state_store().expect("host exposes StateStore"),
                    runtime.handle().clone(),
                ))
                .expect("open MV repository");
            Self {
                _temp: temp,
                _runtime: runtime,
                _host: host,
                repository,
            }
        }

        fn begin_refresh(
            &self,
            table: &str,
            binding: &ConnectorExecutionBindingKey,
        ) -> StoredMvRefresh {
            let definition = self
                .repository
                .create(Uuid::now_v7(), create_request(table))
                .expect("create MV definition");
            self.repository
                .begin_frontend_refresh_intent(BeginFrontendMvRefreshIntentRequest {
                    refresh_id: definition.mv_id + 10_000,
                    mv_id: definition.mv_id,
                    target_catalog: "ice".to_string(),
                    target_namespace: "sales".to_string(),
                    target_table: table.to_string(),
                    staging_branch: format!("__nova_mv_{table}"),
                    expected_main_snapshot_id: Some(7),
                    base_snapshots: BTreeMap::from([("ice.sales.orders".to_string(), 9)]),
                    base_table_uuids: BTreeMap::from([(
                        "ice.sales.orders".to_string(),
                        "orders-uuid".to_string(),
                    )]),
                    marker_token: format!("marker-{table}"),
                    prepare_external_actions: true,
                    ledger: frontend_ledger(binding),
                })
                .expect("persist frontend refresh intent")
        }
    }

    #[derive(Clone, Copy)]
    enum CleanupMode {
        KnownCommitted,
        CommitUnknown,
    }

    struct TestRecovery {
        key: ConnectorExecutionBindingKey,
        observations: Vec<ConnectorStagedPublicationObservation>,
        inspect_calls: AtomicUsize,
        cleanup_mode: CleanupMode,
        cleanup_calls: AtomicUsize,
        reconcile_calls: AtomicUsize,
    }

    #[derive(Default)]
    struct TestDescriptorProjection {
        committed_partitioning: Mutex<Vec<ConnectorCommittedPartitioning>>,
        failure: Option<&'static str>,
    }

    impl crate::query_execution::mv_native_write::MvRefreshProviderActivation
        for TestDescriptorProjection
    {
        fn activate_write(
            &self,
            _prepared: crate::query_execution::mv_assembly::refresh_handoff::PreparedMvRefreshWrite,
            _planning_lease: &novarocks_spi::connector::ConnectorControlPlanningLease,
            _exact_lease: &novarocks_spi::connector::ConnectorWriteLease,
            _execution: &crate::common::admitted_query_context::QueryExecutionContext,
        ) -> Result<crate::query_execution::mv_native_write::PreparedMvNativeWriteAssembly, String>
        {
            unreachable!("recovery never activates a writer")
        }

        fn interpret_write_commit(
            &self,
            _intent: crate::query_execution::mv_assembly::refresh_artifact::MvRefreshPublicationIntent,
            _receipt: &novarocks_spi::connector::ConnectorWriteReceipt,
        ) -> Result<
            crate::query_execution::mv_assembly::refresh_artifact::MvRefreshCommittedFacts,
            String,
        > {
            unreachable!("recovery never interprets a live write receipt")
        }

        fn sync_repartition_descriptor(
            &self,
            _mv_id: i64,
            _partition_spec: MvPartitionContract,
            committed_partitioning: ConnectorCommittedPartitioning,
            _connector_context: &ConnectorRequestContext,
        ) -> Result<(), String> {
            self.committed_partitioning
                .lock()
                .expect("descriptor projection observation lock")
                .push(committed_partitioning);
            if let Some(failure) = self.failure {
                return Err(failure.to_string());
            }
            Ok(())
        }
    }

    impl TestRecovery {
        fn new(
            key: ConnectorExecutionBindingKey,
            observation: ConnectorStagedPublicationObservation,
            cleanup_mode: CleanupMode,
        ) -> Self {
            Self::with_observations(key, vec![observation], cleanup_mode)
        }

        fn with_observations(
            key: ConnectorExecutionBindingKey,
            observations: Vec<ConnectorStagedPublicationObservation>,
            cleanup_mode: CleanupMode,
        ) -> Self {
            assert!(
                !observations.is_empty(),
                "test recovery requires an observation"
            );
            Self {
                key,
                observations,
                inspect_calls: AtomicUsize::new(0),
                cleanup_mode,
                cleanup_calls: AtomicUsize::new(0),
                reconcile_calls: AtomicUsize::new(0),
            }
        }

        fn unknown_cleanup(
            &self,
            operation_id: ConnectorMutationOperationId,
        ) -> ExternalMutationOutcome<ConnectorStagedPublicationCleanupReceipt> {
            ExternalMutationOutcome::CommitUnknown {
                failure: ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Unavailable,
                    "cleanup response lost",
                ),
                evidence: ExternalMutationEvidence::try_new(
                    1,
                    ConnectorInstanceDescriptor {
                        provider_id: ConnectorProviderId::parse("iceberg").expect("provider ID"),
                        instance_id: self.key.instance_id.clone(),
                    },
                    self.key.incarnation,
                    operation_id,
                    "staged-publication-cleanup",
                    Bytes::from_static(b"cleanup-unknown"),
                )
                .expect("cleanup evidence"),
            }
        }
    }

    impl ConnectorStagedPublicationRecovery for TestRecovery {
        fn binding_key(&self) -> &ConnectorExecutionBindingKey {
            &self.key
        }

        fn inspect(
            &self,
            _descriptor: ConnectorStagedPublicationDescriptor,
            _context: ConnectorRequestContext,
        ) -> Result<ConnectorStagedPublicationObservation, novarocks_spi::connector::ConnectorError>
        {
            let index = self.inspect_calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.observations[index.min(self.observations.len() - 1)].clone())
        }

        fn cleanup(
            &self,
            request: ConnectorStagedPublicationCleanupRequest,
        ) -> Result<
            ExternalMutationOutcome<ConnectorStagedPublicationCleanupReceipt>,
            novarocks_spi::connector::ConnectorError,
        > {
            self.cleanup_calls.fetch_add(1, Ordering::SeqCst);
            Ok(match self.cleanup_mode {
                CleanupMode::KnownCommitted => ExternalMutationOutcome::KnownCommitted {
                    effect: ExternalMutationEffect::Applied,
                    receipt: ConnectorStagedPublicationCleanupReceipt {
                        descriptor_digest: request.descriptor_digest,
                        observation_digest: request.observation.digest(),
                    },
                    finalization: ExternalMutationFinalization::Complete,
                },
                CleanupMode::CommitUnknown => self.unknown_cleanup(request.operation_id),
            })
        }

        fn reconcile_cleanup(
            &self,
            operation_id: ConnectorMutationOperationId,
            _evidence: ExternalMutationEvidence,
            _context: ConnectorRequestContext,
        ) -> Result<
            ExternalMutationOutcome<ConnectorStagedPublicationCleanupReceipt>,
            novarocks_spi::connector::ConnectorError,
        > {
            self.reconcile_calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.unknown_cleanup(operation_id))
        }
    }

    fn dependencies(
        recovery: Arc<TestRecovery>,
    ) -> (
        FrontendMvRecoveryDependencies,
        Arc<ConnectorControlHost>,
        Arc<TestDescriptorProjection>,
    ) {
        let projection = Arc::new(TestDescriptorProjection::default());
        let (dependencies, host) = dependencies_with_projection(recovery, projection.clone());
        (dependencies, host, projection)
    }

    fn dependencies_with_projection(
        recovery: Arc<TestRecovery>,
        projection: Arc<TestDescriptorProjection>,
    ) -> (FrontendMvRecoveryDependencies, Arc<ConnectorControlHost>) {
        let binding = test_control_binding(7)
            .try_with_staged_publication_recovery(Some(recovery))
            .expect("attach staged-publication recovery");
        let host = Arc::new(ConnectorControlHost::new());
        host.register(binding).expect("register control binding");
        let provider_activation =
            Arc::new(super::super::refresh::FrontendMvRefreshProviderActivationPort::new());
        crate::query_execution::mv_native_write::MvRefreshProviderActivationSink::bind_mv_refresh_provider_activation(
            provider_activation.as_ref(),
            projection,
        )
        .expect("bind descriptor projection");
        (
            FrontendMvRecoveryDependencies {
                // Unfenced: these tests drive recovery decisions directly, and
                // ownership is exercised by the cluster tests that run two real
                // frontends against one StateStore.
                ownership: None,
                connector_control: host.clone(),
                provider_activation,
            },
            host,
        )
    }

    fn current_binding_key() -> ConnectorExecutionBindingKey {
        let binding = test_control_binding(7);
        ConnectorExecutionBindingKey {
            instance_id: binding.descriptor().instance_id.clone(),
            incarnation: binding.incarnation(),
        }
    }

    fn frontend_ledger(binding: &ConnectorExecutionBindingKey) -> FrontendMvRefreshLedger {
        FrontendMvRefreshLedger {
            request_id: Uuid::now_v7().into_bytes().to_vec(),
            provider_id: "iceberg".to_string(),
            instance_id: binding.instance_id.as_str().to_string(),
            incarnation: binding.incarnation.to_bytes().to_vec(),
            expected_target_version: None,
            staging_create_operation_id: Uuid::now_v7().into_bytes().to_vec(),
            write_operation_id: Uuid::now_v7().into_bytes().to_vec(),
            publication_operation_id: Uuid::now_v7().into_bytes().to_vec(),
            staging_drop_operation_id: Uuid::now_v7().into_bytes().to_vec(),
            cohort_ids: vec!["11".repeat(32)],
            actions: Vec::new(),
            cleanup_pending: false,
        }
    }

    fn create_request(table: &str) -> CreateMvRepositoryRequest {
        let initial_partition = partition_contract(3, 1_003, "id");
        CreateMvRepositoryRequest {
            definition: CreateMvDefinitionRequest {
                select_sql: "SELECT 1".to_string(),
                base_table_refs: vec!["ice.sales.orders".to_string()],
                primary_key_columns: vec![],
                storage_engine: "iceberg".to_string(),
                target_catalog: Some("ice".to_string()),
                target_namespace: Some("sales".to_string()),
                target_table: Some(table.to_string()),
                schema_contract: Some(schema_contract(table, initial_partition.clone())),
                partition_spec: Some(initial_partition),
                created_at_ms: 1,
            },
            refresh: InitialMvRefreshConfiguration {
                policy: StoredMvRefreshPolicy::Manual,
                ..Default::default()
            },
            dependencies: vec![CreateMvDependencyRequest {
                upstream: MvDependencyObjectRef {
                    catalog: Some("ice".to_string()),
                    database_or_namespace: "sales".to_string(),
                    name: "orders".to_string(),
                    object_type: MvDependencyObjectType::Table,
                    storage_engine: MvDependencyStorageEngine::Iceberg,
                },
                created_at_ms: 1,
            }],
        }
    }

    fn partition_contract(
        spec_id: i32,
        partition_field_id: i32,
        partition_field_name: &str,
    ) -> MvPartitionContract {
        MvPartitionContract {
            target_spec_id: spec_id,
            fields: vec![MvPartitionFieldContract {
                partition_field_id,
                partition_field_name: partition_field_name.to_string(),
                source_target_field_id: 10,
                source_column_name: "id".to_string(),
                transform: MvPartitionTransformContract::Identity,
            }],
        }
    }

    fn schema_contract(table: &str, partition: MvPartitionContract) -> MvSchemaContract {
        MvSchemaContract {
            contract_version: 1,
            base: BaseContract {
                table_fqn: "ice.sales.orders".to_string(),
                table_uuid: "11111111-1111-1111-1111-111111111111".to_string(),
                alias_at_create: Some("orders".to_string()),
                schema_id_at_create: 7,
                schema_at_create: BaseSchemaSnapshot {
                    fields: vec![BaseFieldRecord {
                        field_id: 1,
                        name_at_create: "id".to_string(),
                        type_signature: "long".to_string(),
                        required: true,
                    }],
                },
            },
            bases: vec![],
            output: OutputContract {
                columns: vec![OutputColumnLineage {
                    expression: ExpressionLineage {
                        kind: ExpressionKind::Column,
                        referenced_base_field_ids: vec![1],
                        referenced_base_fields: vec![],
                    },
                }],
                filter: None,
            },
            join: None,
            aggregate: None,
            branch: None,
            target: TargetContract {
                table_fqn: format!("ice.sales.{table}"),
                table_uuid: "22222222-2222-2222-2222-222222222222".to_string(),
                schema_id_at_create: 11,
                visible_columns: vec![TargetVisibleColumn {
                    output_name: "id".to_string(),
                    target_field_id: 10,
                    type_signature: "long".to_string(),
                    nullable: false,
                }],
                hidden_apply_key: HiddenApplyKeyContract {
                    column_name: "__nova_base_row_id".to_string(),
                    target_field_id: 99,
                    source: ApplyKeySource::BaseRowId,
                },
                partition: Some(partition),
            },
        }
    }

    fn committed_partitioning() -> ConnectorCommittedPartitioning {
        ConnectorCommittedPartitioning::try_new(
            12,
            vec![
                ConnectorCommittedPartitionField::try_new(
                    1_050,
                    "id_bucket_16",
                    10,
                    "id",
                    0,
                    ConnectorManagedPartitionTransform::Bucket { buckets: 16 },
                )
                .expect("committed partition field"),
            ],
        )
        .expect("committed partitioning")
    }

    fn observation(
        disposition: ConnectorStagedPublicationDisposition,
        cleanup_required: bool,
    ) -> ConnectorStagedPublicationObservation {
        observation_with_proof(disposition, cleanup_required, b"lake-proof")
    }

    fn observation_with_proof(
        disposition: ConnectorStagedPublicationDisposition,
        cleanup_required: bool,
        proof: &'static [u8],
    ) -> ConnectorStagedPublicationObservation {
        let published = matches!(
            disposition,
            ConnectorStagedPublicationDisposition::Published
                | ConnectorStagedPublicationDisposition::Superseded
                | ConnectorStagedPublicationDisposition::CleanupPending
        );
        ConnectorStagedPublicationObservation::try_new(
            disposition,
            published
                .then(|| ConnectorCommittedVersion::try_new(Bytes::from_static(b"v42"), Some(42)))
                .transpose()
                .expect("committed version"),
            published.then_some(2),
            vec![ConnectorStagedPublicationBaseFact {
                table: "ice.sales.orders".into(),
                uuid: "orders-uuid".into(),
                from_version: Some(9),
                to_version: 9,
            }],
            published.then(|| Arc::from("definition-v1")),
            Some(42),
            published.then_some(42),
            cleanup_required,
            ConnectorStagedPublicationProof::try_new(Bytes::from_static(proof))
                .expect("publication proof"),
        )
        .expect("staged-publication observation")
    }

    fn repartition_observation() -> ConnectorStagedPublicationObservation {
        ConnectorStagedPublicationObservation::try_new_with_committed_partitioning(
            ConnectorStagedPublicationDisposition::Published,
            Some(
                ConnectorCommittedVersion::try_new(Bytes::from_static(b"v42"), Some(42))
                    .expect("committed version"),
            ),
            Some(2),
            vec![ConnectorStagedPublicationBaseFact {
                table: "ice.sales.orders".into(),
                uuid: "orders-uuid".into(),
                from_version: Some(9),
                to_version: 9,
            }],
            Some(Arc::from("definition-v1")),
            None,
            Some(84),
            committed_partitioning(),
            false,
            ConnectorStagedPublicationProof::try_new(Bytes::from_static(b"lake-proof"))
                .expect("publication proof"),
        )
        .expect("repartition publication observation")
    }

    #[test]
    fn known_uncommitted_cleanup_then_abort_resolves() {
        let environment = TestEnvironment::open();
        let key = current_binding_key();
        let refresh = environment.begin_refresh("known_uncommitted", &key);
        let recovery = Arc::new(TestRecovery::new(
            key,
            observation(
                ConnectorStagedPublicationDisposition::KnownUncommitted,
                true,
            ),
            CleanupMode::KnownCommitted,
        ));
        let (dependencies, _host, _projection) = dependencies(recovery.clone());

        let summary = recover_once(environment.repository.as_ref(), &dependencies);

        assert_eq!(
            summary,
            FrontendMvRecoverySummary {
                candidates: 1,
                resolved: 1,
                unresolved: 0,
                cleanup_backlog: 0,
            }
        );
        assert_eq!(recovery.cleanup_calls.load(Ordering::SeqCst), 1);
        let recovered = environment
            .repository
            .load_refresh(refresh.refresh_id)
            .expect("load refresh")
            .expect("refresh exists");
        assert_eq!(recovered.state, MvRefreshState::Aborted);
        let ledger = recovered.frontend_recovery.expect("recovery ledger");
        assert_eq!(
            ledger.status,
            FrontendMvRefreshRecoveryStatus::ResolvedAborted
        );
        assert_eq!(
            ledger.cleanup_state,
            Some(FrontendMvRefreshActionState::KnownCommitted)
        );
    }

    #[test]
    fn proof_only_staging_create_then_uncommitted_write_aborts_without_cleanup() {
        let environment = TestEnvironment::open();
        let key = current_binding_key();
        let refresh = environment.begin_refresh("atomic_uncommitted", &key);
        let ledger = refresh.frontend_ledger.as_ref().expect("frontend ledger");
        environment
            .repository
            .record_frontend_refresh_action(
                refresh.refresh_id,
                FrontendMvRefreshAction {
                    phase: FrontendMvRefreshActionPhase::StagingCreate,
                    state: FrontendMvRefreshActionState::KnownCommitted,
                    operation_id: ledger.staging_create_operation_id.clone(),
                    receipt: None,
                    committed_version: None,
                    external_evidence: None,
                    provider_finalized: true,
                },
            )
            .expect("record proof-only staging-create phase");
        let recovery = Arc::new(TestRecovery::new(
            key,
            observation(
                ConnectorStagedPublicationDisposition::KnownUncommitted,
                false,
            ),
            CleanupMode::KnownCommitted,
        ));
        let (dependencies, _host, _projection) = dependencies(recovery.clone());

        let summary = recover_once(environment.repository.as_ref(), &dependencies);

        assert_eq!(summary.resolved, 1);
        assert_eq!(summary.cleanup_backlog, 0);
        assert_eq!(recovery.cleanup_calls.load(Ordering::SeqCst), 0);
        let recovered = environment
            .repository
            .load_refresh(refresh.refresh_id)
            .expect("load refresh")
            .expect("refresh exists");
        assert_eq!(recovered.state, MvRefreshState::Aborted);
        assert_eq!(
            recovered.frontend_recovery.expect("recovery ledger").status,
            FrontendMvRefreshRecoveryStatus::ResolvedAborted
        );
    }

    #[test]
    fn published_finalize_and_cleanup_resolves() {
        let environment = TestEnvironment::open();
        let key = current_binding_key();
        let refresh = environment.begin_refresh("published", &key);
        let recovery = Arc::new(TestRecovery::new(
            key,
            observation(ConnectorStagedPublicationDisposition::Published, true),
            CleanupMode::KnownCommitted,
        ));
        let (dependencies, _host, _projection) = dependencies(recovery.clone());

        let summary = recover_once(environment.repository.as_ref(), &dependencies);

        assert_eq!(summary.resolved, 1);
        assert_eq!(summary.cleanup_backlog, 0);
        assert_eq!(recovery.cleanup_calls.load(Ordering::SeqCst), 1);
        let recovered = environment
            .repository
            .load_refresh(refresh.refresh_id)
            .expect("load refresh")
            .expect("refresh exists");
        assert_eq!(recovered.state, MvRefreshState::Finalized);
        let ledger = recovered.frontend_recovery.expect("recovery ledger");
        assert_eq!(
            ledger.status,
            FrontendMvRefreshRecoveryStatus::ResolvedPublished
        );
        assert_eq!(
            ledger.cleanup_state,
            Some(FrontendMvRefreshActionState::KnownCommitted)
        );
    }

    #[test]
    fn published_repartition_finalizes_exact_partition_contract_idempotently() {
        let environment = TestEnvironment::open();
        let key = current_binding_key();
        let refresh = environment.begin_refresh("atomic_published", &key);
        let prepared = refresh
            .frontend_ledger
            .as_ref()
            .expect("frontend action ledger")
            .clone();
        environment
            .repository
            .record_frontend_refresh_action(
                refresh.refresh_id,
                FrontendMvRefreshAction {
                    phase: FrontendMvRefreshActionPhase::StagingCreate,
                    state: FrontendMvRefreshActionState::KnownCommitted,
                    operation_id: prepared.staging_create_operation_id.clone(),
                    receipt: None,
                    committed_version: None,
                    external_evidence: Some(evidence_value(b"staging-create-proof")),
                    provider_finalized: true,
                },
            )
            .expect("record proof-only staging create");
        environment
            .repository
            .record_frontend_refresh_action(
                refresh.refresh_id,
                FrontendMvRefreshAction {
                    phase: FrontendMvRefreshActionPhase::Write,
                    state: FrontendMvRefreshActionState::CommitUnknown,
                    operation_id: prepared.write_operation_id.clone(),
                    receipt: None,
                    committed_version: None,
                    external_evidence: Some(evidence_value(b"write-response-lost")),
                    provider_finalized: false,
                },
            )
            .expect("record response loss after atomic repartition write");
        let recovery = Arc::new(TestRecovery::new(
            key,
            repartition_observation(),
            CleanupMode::KnownCommitted,
        ));
        let (dependencies, _host, projection) = dependencies(recovery.clone());

        let first = recover_once(environment.repository.as_ref(), &dependencies);
        let second = recover_once(environment.repository.as_ref(), &dependencies);

        assert_eq!(first.resolved, 1);
        assert_eq!(first.cleanup_backlog, 0);
        assert_eq!(second.candidates, 0);
        assert_eq!(recovery.cleanup_calls.load(Ordering::SeqCst), 0);
        let forwarded_partitioning = projection
            .committed_partitioning
            .lock()
            .expect("descriptor projection observation lock");
        assert_eq!(
            forwarded_partitioning.as_slice(),
            &[committed_partitioning()]
        );
        let expected = MvPartitionContract {
            target_spec_id: 12,
            fields: vec![MvPartitionFieldContract {
                partition_field_id: 1_050,
                partition_field_name: "id_bucket_16".to_string(),
                source_target_field_id: 10,
                source_column_name: "id".to_string(),
                transform: MvPartitionTransformContract::Bucket { num_buckets: 16 },
            }],
        };
        let definition = environment
            .repository
            .load_by_id(refresh.mv_id)
            .expect("load definition")
            .expect("definition exists");
        assert_eq!(definition.partition_spec.as_ref(), Some(&expected));
        assert_eq!(definition.last_refreshed_iceberg_snapshot_id, Some(42));
        assert_eq!(
            definition
                .schema_contract
                .as_ref()
                .and_then(|contract| contract.target.partition.as_ref()),
            Some(&expected)
        );
        let recovered = environment
            .repository
            .load_refresh(refresh.refresh_id)
            .expect("load recovered repartition refresh")
            .expect("recovered repartition refresh exists");
        let ledger = recovered.frontend_ledger.expect("frontend action ledger");
        assert!(!ledger.cleanup_pending);
        for (phase, operation_id) in [
            (
                FrontendMvRefreshActionPhase::StagingCreate,
                &prepared.staging_create_operation_id,
            ),
            (
                FrontendMvRefreshActionPhase::Write,
                &prepared.write_operation_id,
            ),
            (
                FrontendMvRefreshActionPhase::Publication,
                &prepared.publication_operation_id,
            ),
            (
                FrontendMvRefreshActionPhase::StagingDrop,
                &prepared.staging_drop_operation_id,
            ),
        ] {
            let action = ledger
                .actions
                .iter()
                .find(|action| action.phase == phase)
                .expect("terminal action");
            assert_eq!(action.operation_id, *operation_id);
            assert_eq!(action.state, FrontendMvRefreshActionState::KnownCommitted);
            assert_eq!(
                action.external_evidence.as_ref(),
                Some(&evidence_value(b"lake-proof"))
            );
            assert!(action.provider_finalized);
            if matches!(
                phase,
                FrontendMvRefreshActionPhase::Write | FrontendMvRefreshActionPhase::Publication
            ) {
                let version = action
                    .committed_version
                    .as_ref()
                    .expect("write and publication retain the committed version");
                assert_eq!(version.payload, b"v42");
                assert_eq!(version.snapshot_id, Some(42));
            } else {
                assert_eq!(action.committed_version, None);
            }
        }
    }

    #[test]
    fn repartition_descriptor_projection_failure_keeps_recovery_fenced() {
        let environment = TestEnvironment::open();
        let key = current_binding_key();
        let refresh = environment.begin_refresh("atomic_projection_failure", &key);
        let prepared = refresh
            .frontend_ledger
            .as_ref()
            .expect("frontend action ledger")
            .clone();
        environment
            .repository
            .record_frontend_refresh_action(
                refresh.refresh_id,
                FrontendMvRefreshAction {
                    phase: FrontendMvRefreshActionPhase::StagingCreate,
                    state: FrontendMvRefreshActionState::KnownCommitted,
                    operation_id: prepared.staging_create_operation_id,
                    receipt: None,
                    committed_version: None,
                    external_evidence: Some(evidence_value(b"staging-create-proof")),
                    provider_finalized: true,
                },
            )
            .expect("record proof-only staging create");
        environment
            .repository
            .record_frontend_refresh_action(
                refresh.refresh_id,
                FrontendMvRefreshAction {
                    phase: FrontendMvRefreshActionPhase::Write,
                    state: FrontendMvRefreshActionState::CommitUnknown,
                    operation_id: prepared.write_operation_id,
                    receipt: None,
                    committed_version: None,
                    external_evidence: Some(evidence_value(b"write-response-lost")),
                    provider_finalized: false,
                },
            )
            .expect("record response loss after atomic repartition write");
        let recovery = Arc::new(TestRecovery::new(
            key,
            repartition_observation(),
            CleanupMode::KnownCommitted,
        ));
        let projection = Arc::new(TestDescriptorProjection {
            committed_partitioning: Mutex::new(Vec::new()),
            failure: Some("guarded descriptor projection conflict"),
        });
        let (dependencies, _host) = dependencies_with_projection(recovery, projection.clone());

        let summary = recover_once(environment.repository.as_ref(), &dependencies);

        assert_eq!(summary.candidates, 1);
        assert_eq!(summary.resolved, 0);
        assert_eq!(summary.unresolved, 1);
        assert_eq!(
            projection
                .committed_partitioning
                .lock()
                .expect("descriptor projection observation lock")
                .as_slice(),
            &[committed_partitioning()]
        );
        let fenced = environment
            .repository
            .load_refresh(refresh.refresh_id)
            .expect("load fenced refresh")
            .expect("fenced refresh exists");
        assert_ne!(fenced.state, MvRefreshState::Finalized);
        let definition = environment
            .repository
            .load_by_id(refresh.mv_id)
            .expect("load definition")
            .expect("definition exists");
        assert_eq!(definition.last_refreshed_iceberg_snapshot_id, None);
        assert_eq!(
            definition
                .partition_spec
                .as_ref()
                .map(|partition| partition.target_spec_id),
            Some(3)
        );
    }

    #[test]
    fn ambiguous_observation_remains_unresolved_without_cleanup() {
        let environment = TestEnvironment::open();
        let key = current_binding_key();
        let refresh = environment.begin_refresh("ambiguous", &key);
        let recovery = Arc::new(TestRecovery::new(
            key,
            observation(ConnectorStagedPublicationDisposition::Ambiguous, true),
            CleanupMode::KnownCommitted,
        ));
        let (dependencies, _host, _projection) = dependencies(recovery.clone());

        let summary = recover_once(environment.repository.as_ref(), &dependencies);

        assert_eq!(summary.unresolved, 1);
        assert_eq!(summary.resolved, 0);
        assert_eq!(recovery.cleanup_calls.load(Ordering::SeqCst), 0);
        let recovered = environment
            .repository
            .load_refresh(refresh.refresh_id)
            .expect("load refresh")
            .expect("refresh exists");
        assert_eq!(
            recovered.frontend_recovery.expect("recovery ledger").status,
            FrontendMvRefreshRecoveryStatus::Unresolved
        );
    }

    #[test]
    fn cleanup_commit_unknown_reinspects_evolved_truth_and_converges() {
        let environment = TestEnvironment::open();
        let key = current_binding_key();
        let refresh = environment.begin_refresh("cleanup_unknown", &key);
        let recovery = Arc::new(TestRecovery::with_observations(
            key,
            vec![
                observation_with_proof(
                    ConnectorStagedPublicationDisposition::Published,
                    true,
                    b"lake-proof-before-cleanup",
                ),
                observation_with_proof(
                    ConnectorStagedPublicationDisposition::Superseded,
                    false,
                    b"lake-proof-after-cleanup",
                ),
            ],
            CleanupMode::CommitUnknown,
        ));
        let (dependencies, _host, _projection) = dependencies(recovery.clone());

        let summary = recover_once(environment.repository.as_ref(), &dependencies);

        assert_eq!(summary.cleanup_backlog, 1);
        assert_eq!(summary.resolved, 0);
        assert_eq!(recovery.cleanup_calls.load(Ordering::SeqCst), 1);
        assert_eq!(recovery.reconcile_calls.load(Ordering::SeqCst), 1);
        let recovered = environment
            .repository
            .load_refresh(refresh.refresh_id)
            .expect("load refresh")
            .expect("refresh exists");
        assert_eq!(recovered.state, MvRefreshState::Finalized);
        let ledger = recovered.frontend_recovery.expect("recovery ledger");
        assert_eq!(
            ledger.status,
            FrontendMvRefreshRecoveryStatus::CleanupPending
        );
        assert_eq!(
            ledger.cleanup_state,
            Some(FrontendMvRefreshActionState::CommitUnknown)
        );
        let first_operation_id = ledger.cleanup_operation_id.clone();
        let first_evidence = ledger.cleanup_evidence.clone().expect("cleanup evidence");
        let first_observation = ledger.observation.clone().expect("first observation");

        let second = recover_once(environment.repository.as_ref(), &dependencies);
        assert_eq!(second.candidates, 1);
        assert_eq!(second.cleanup_backlog, 0);
        assert_eq!(second.resolved, 1);
        assert_eq!(recovery.inspect_calls.load(Ordering::SeqCst), 2);
        assert_eq!(recovery.cleanup_calls.load(Ordering::SeqCst), 1);
        assert_eq!(recovery.reconcile_calls.load(Ordering::SeqCst), 1);
        let recovered = environment
            .repository
            .load_refresh(refresh.refresh_id)
            .expect("load refresh after second cycle")
            .expect("refresh exists after second cycle");
        let ledger = recovered.frontend_recovery.expect("recovery ledger");
        assert_eq!(
            ledger.status,
            FrontendMvRefreshRecoveryStatus::ResolvedPublished
        );
        assert_eq!(ledger.cleanup_operation_id, first_operation_id);
        assert_eq!(ledger.cleanup_evidence.as_ref(), Some(&first_evidence));
        let second_observation = ledger.observation.expect("second observation");
        assert_eq!(
            second_observation.disposition,
            crate::mv::domain::persistence::refresh::FrontendMvRefreshRecoveryDisposition::Superseded
        );
        assert!(!second_observation.cleanup_required);
        assert_ne!(second_observation.digest, first_observation.digest);
    }
}
