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

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use novarocks_execution::runtime::fragment::{FragmentOutcome, FragmentTerminalFact};
use novarocks_execution::runtime::profile::RuntimeProfileTree;
use novarocks_execution::runtime_filter::RuntimeFilterSessionRef;
use novarocks_failpoint::QueryLifecycleFaultKind;
use novarocks_proto_codec::lifecycle::terminal::p0_max_encoded_len;
use novarocks_proto_codec::lifecycle::{
    FragmentLiveObservation, FragmentTerminalSnapshot, ParticipantAttemptRef,
    ParticipantManifestDigest, ParticipantTerminalOutcome, QueryAbortRequest, QueryControlAttach,
    QueryControlEndpoint, QueryControlEvent, QueryExecutionId, QueryInitAck, QueryInitOutcome,
    QueryInitRequest, QueryStageAck, QueryStageOutcome, QueryStageRequest, QueryStartAck,
    QueryStartOutcome, QueryStartRequest, QueryTerminalAck, QueryTerminalProfileContributionV1,
    QueryTerminalReportAck, QueryTerminalReportOutcome, QueryTerminalSnapshot, QueryTerminationAck,
    QueryTerminationReason, StageDigest,
};
use novarocks_spi::connector::{
    CatalogProperties, ConnectorError, ConnectorErrorKind, ConnectorStorageResolver,
    ResolvedVendedS3Access, StorageAccessRequest,
};
use novarocks_types::{
    BackendProcessId, LocalQuerySequence, NativeCompatibilityId, QueryIdAttribution,
    QueryProcessNamespace, UniqueId,
};
use prost::Message;
use tracing::{info, warn};

use super::credential_lease::{CredentialLeaseError, QueryCredentialLeases};
use super::entry::{
    ImmutableQueryTerminalRecord, QueryCatalogLoadState, QueryLifecycleEntry, QueryLifecyclePhase,
};
use super::{
    BackendQueryControl, CatalogPruneOutcome, QueryControlAttachment, QueryLifecycleError,
    QueryLifecycleErrorCode, QueryLifecycleIngress, QueryTerminalFallbackTransport,
    QueryTerminalFallbackTransportError,
};
use crate::BackendDataRuntime;
use crate::metrics::query_lifecycle::BackendQueryLifecycleMetricsSnapshot;
use crate::metrics::{
    publish_backend_query_execution_resource, publish_backend_query_lifecycle_metrics,
    publish_backend_query_lifecycle_terminal_limits,
};
use crate::rpc::client::BackendRpcClient;
use crate::runtime::profile_codec::encode_runtime_profile_tree;
use crate::runtime::sink_commit::SinkCommitReportSnapshot;
use crate::runtime_filter::domain::{
    BackendFrontendFeedbackOutcome, BackendFrontendFeedbackPublication, BackendFrontendFeedbackSink,
};
use crate::runtime_filter::install_decode::decode_runtime_filter_contribution;
use crate::runtime_filter::observation::{
    RuntimeFilterChannelTerminal, RuntimeFilterConsumerOutcome, RuntimeFilterObservationSnapshot,
};
use crate::runtime_filter::participant::{
    BackendRuntimeFilterParticipantFactory, RuntimeFilterParticipant,
    RuntimeFilterParticipantFactory,
};
use crate::runtime_filter::rpc::{
    BackendNativeRuntimeFilterEnvelope, BackendRuntimeFilterEnvelopeIngress,
};

const CONTROL_EVENT_BUFFER_CAPACITY: usize = 16;
const RESERVED_CONTROL_EVENT_CAPACITY: usize = 3;
const RUNTIME_FILTER_FEEDBACK_BUFFER_CAPACITY: usize = 64;

/// Attempt-local, best-effort control-stream egress for terminal logical
/// feedback. The participant retains this only weakly, and `try_send` makes a
/// congested or detached Frontend fail open without delaying execution.
struct RuntimeFilterFeedbackEgress {
    participant: ParticipantAttemptRef,
    participant_id: u32,
    events: tokio::sync::mpsc::Sender<QueryControlEvent>,
}

impl BackendFrontendFeedbackSink for RuntimeFilterFeedbackEgress {
    fn try_publish(
        &self,
        channel_id: novarocks_execution::runtime_filter::RuntimeFilterChannelId,
        deployment_epoch: u64,
        publication: &BackendFrontendFeedbackPublication,
        outcome: BackendFrontendFeedbackOutcome,
    ) {
        use novarocks_proto_models::novarocks as wire;

        #[cfg(debug_assertions)]
        let outcome = force_feedback_unavailable(validated(self.participant.execution_id()))
            .unwrap_or(outcome);

        let terminal_outcome = match outcome {
            BackendFrontendFeedbackOutcome::CanonicalDomain(domain) => {
                wire::runtime_filter_feedback_event::TerminalOutcome::CanonicalDomain(
                    domain.as_ref().to_vec(),
                )
            }
            BackendFrontendFeedbackOutcome::DomainBudget => {
                wire::runtime_filter_feedback_event::TerminalOutcome::UnavailableReason(
                    wire::RuntimeFilterFeedbackUnavailableReason::DomainBudget as i32,
                )
            }
            BackendFrontendFeedbackOutcome::TypeUnsupported => {
                wire::runtime_filter_feedback_event::TerminalOutcome::UnavailableReason(
                    wire::RuntimeFilterFeedbackUnavailableReason::TypeUnsupported as i32,
                )
            }
            BackendFrontendFeedbackOutcome::ReductionUnavailable => {
                wire::runtime_filter_feedback_event::TerminalOutcome::UnavailableReason(
                    wire::RuntimeFilterFeedbackUnavailableReason::ReductionUnavailable as i32,
                )
            }
            BackendFrontendFeedbackOutcome::ProducerUnavailable => {
                wire::runtime_filter_feedback_event::TerminalOutcome::UnavailableReason(
                    wire::RuntimeFilterFeedbackUnavailableReason::ProducerUnavailable as i32,
                )
            }
        };
        let mut contract_digest = publication.contract_digest().to_vec();
        #[cfg(debug_assertions)]
        if corrupt_feedback_contract_digest(validated(self.participant.execution_id())) {
            contract_digest[0] ^= 1;
        }
        let event =
            protocol_control_event(wire::query_control_response::Event::RuntimeFilterFeedback(
                wire::RuntimeFilterFeedbackEvent {
                    participant_attempt: Some(
                        feedback_participant_ref(&self.participant)
                            .as_proto()
                            .clone(),
                    ),
                    participant_id: self.participant_id,
                    deployment_epoch,
                    channel_id: channel_id.get(),
                    contract_digest,
                    terminal_outcome: Some(terminal_outcome),
                },
            ));
        let _ = self.events.try_send(event);
    }
}

#[cfg(debug_assertions)]
fn corrupt_feedback_contract_digest(execution_id: QueryExecutionId) -> bool {
    let Some(root) = novarocks_failpoint::configured_root() else {
        return false;
    };
    matches!(
        novarocks_failpoint::claim_matching_receiver_agnostic_fault(
            &root,
            QueryLifecycleFaultKind::RuntimeFilterFeedbackContractDigestCorrupt,
            execution_id,
        ),
        Ok(Some(_))
    )
}

#[cfg(debug_assertions)]
fn force_feedback_unavailable(
    execution_id: QueryExecutionId,
) -> Option<BackendFrontendFeedbackOutcome> {
    let root = novarocks_failpoint::configured_root()?;
    matches!(
        novarocks_failpoint::claim_matching_receiver_agnostic_fault(
            &root,
            QueryLifecycleFaultKind::RuntimeFilterFeedbackUnavailable,
            execution_id,
        ),
        Ok(Some(_))
    )
    .then_some(BackendFrontendFeedbackOutcome::ProducerUnavailable)
}

#[cfg(debug_assertions)]
fn feedback_participant_ref(participant: &ParticipantAttemptRef) -> ParticipantAttemptRef {
    let execution_id = validated(participant.execution_id());
    let Some(root) = novarocks_failpoint::configured_root() else {
        return participant.clone();
    };
    match novarocks_failpoint::claim_matching_fault_for_process(
        &root,
        QueryLifecycleFaultKind::RuntimeFilterFeedbackForeignParticipant,
        execution_id,
        validated(participant.backend_process_id()),
    ) {
        Ok(Some(scope)) => {
            eprintln!(
                "NOVAROCKS_RUNTIME_FILTER_FEEDBACK_FOREIGN_PARTICIPANT execution_id={}:{}:{} backend_index={} token={}",
                execution_id.query_id().high(),
                execution_id.query_id().low(),
                execution_id.attempt_id().get(),
                scope.backend_index,
                scope.token
            );
            ParticipantAttemptRef::new(execution_id, BackendProcessId::new_v7())
                .expect("valid generated process creates a foreign participant ref")
        }
        Ok(None) | Err(_) => participant.clone(),
    }
}

#[cfg(not(debug_assertions))]
fn feedback_participant_ref(participant: &ParticipantAttemptRef) -> ParticipantAttemptRef {
    participant.clone()
}

#[cfg(debug_assertions)]
fn observation_participant_ref(participant: &ParticipantAttemptRef) -> ParticipantAttemptRef {
    let execution_id = validated(participant.execution_id());
    let Some(root) = novarocks_failpoint::configured_root() else {
        return participant.clone();
    };
    match novarocks_failpoint::claim_matching_fault_for_process(
        &root,
        QueryLifecycleFaultKind::ObservationForeignParticipant,
        execution_id,
        validated(participant.backend_process_id()),
    ) {
        Ok(Some(scope)) => {
            eprintln!(
                "NOVAROCKS_FRAGMENT_OBSERVATION_FOREIGN_PARTICIPANT execution_id={}:{}:{} backend_index={} token={}",
                execution_id.query_id().high(),
                execution_id.query_id().low(),
                execution_id.attempt_id().get(),
                scope.backend_index,
                scope.token
            );
            ParticipantAttemptRef::new(execution_id, BackendProcessId::new_v7())
                .expect("valid generated process creates a foreign participant ref")
        }
        Ok(None) | Err(_) => participant.clone(),
    }
}

#[cfg(not(debug_assertions))]
fn observation_participant_ref(participant: &ParticipantAttemptRef) -> ParticipantAttemptRef {
    participant.clone()
}

/// One catalog manager for a registry built without an injected one.
///
/// Only the test constructors reach this. Production composes the manager
/// once and shares it, because it is a process resource that outlives any one
/// lifecycle stack.
fn default_catalog_manager() -> Arc<
    crate::connector::catalog_manager::CatalogManager<
        crate::connector::ConnectorExecutionRoleBinding,
    >,
> {
    Arc::new(
        crate::connector::catalog_manager::CatalogManager::try_new(
            crate::connector::catalog_manager::CatalogManagerConfig::default(),
        )
        .expect("the default catalog manager configuration is valid"),
    )
}

fn empty_execution_role_binding_factories()
-> Arc<crate::connector::catalog_manager::ConnectorExecutionRoleBindingFactorySet> {
    Arc::new(
        crate::connector::catalog_manager::ConnectorExecutionRoleBindingFactorySet::try_new([])
            .expect("an empty connector execution role binding factory set is valid"),
    )
}

fn protocol_contract_error(error: novarocks_proto_codec::ProtocolError) -> QueryLifecycleError {
    QueryLifecycleError::new(QueryLifecycleErrorCode::InvalidManifest, error.to_string())
}

fn safe_catalog_failure_detail(value: &str) -> String {
    let mut end = value.len().min(512);
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    let value = value[..end]
        .chars()
        .filter(|character| !character.is_control())
        .collect::<String>();
    if value.is_empty() {
        "catalog materialization failed".to_owned()
    } else {
        value
    }
}

/// Protocol wrappers have already passed structural validation at native
/// ingress.  Backend uses this boundary helper only to make that invariant
/// explicit while deriving BE-local routing IDs from generated values.
fn validated<T>(value: Result<T, novarocks_proto_codec::ProtocolError>) -> T {
    value.expect("validated Protocol lifecycle carrier must retain its required field")
}

fn generated_id(value: novarocks_proto_models::common::UniqueId) -> UniqueId {
    UniqueId::new(value.hi, value.lo)
}

#[allow(
    dead_code,
    reason = "Retained for lifecycle protocol-fixture targets that convert manifest fragment identifiers."
)]
fn expected_fragment_ids(
    manifest: &novarocks_proto_codec::lifecycle::ParticipantManifest,
) -> Vec<UniqueId> {
    manifest
        .expected_fragment_instance_ids()
        .into_iter()
        .map(generated_id)
        .collect()
}

/// Seals a Backend-produced event into the generated control carrier.  The
/// `oneof` stays generated: Backend never mirrors it in a Core enum.
fn protocol_control_event(
    event: novarocks_proto_models::novarocks::query_control_response::Event,
) -> QueryControlEvent {
    QueryControlEvent::parse(novarocks_proto_models::novarocks::QueryControlResponse {
        event: Some(event),
    })
    .expect("Backend-generated query control event satisfies the Protocol contract")
}

fn control_ready_event(catalog_load: &QueryCatalogLoadState) -> QueryControlEvent {
    use novarocks_proto_models::{catalog, novarocks as wire};
    let state = match catalog_load {
        QueryCatalogLoadState::Ready => {
            catalog::catalog_load_state::State::Ready(catalog::CatalogReady {})
        }
        QueryCatalogLoadState::Loading { .. } => {
            catalog::catalog_load_state::State::Loading(catalog::CatalogLoading {})
        }
        QueryCatalogLoadState::Failed { safe_detail } => {
            catalog::catalog_load_state::State::Failed(catalog::CatalogLoadFailed {
                reason: catalog::CatalogLoadFailureReason::InstallFailed as i32,
                safe_detail: safe_detail.clone(),
                safe_field_path: None,
            })
        }
    };
    protocol_control_event(wire::query_control_response::Event::ControlReady(
        wire::QueryControlReady {
            catalog_load_state: Some(catalog::CatalogLoadState { state: Some(state) }),
        },
    ))
}

fn catalog_ready_event() -> QueryControlEvent {
    use novarocks_proto_models::{catalog, novarocks as wire};
    protocol_control_event(wire::query_control_response::Event::CatalogReady(
        catalog::CatalogReady {},
    ))
}

fn credential_lease_prepared_event(
    epoch: u64,
    lease_id: novarocks_spi::connector::CredentialLeaseId,
) -> QueryControlEvent {
    use novarocks_proto_models::novarocks as wire;
    protocol_control_event(
        wire::query_control_response::Event::CredentialLeasePrepared(
            wire::CredentialLeasePrepared {
                lease_id: lease_id.as_bytes().to_vec(),
                epoch,
            },
        ),
    )
}

fn credential_lease_committed_event(
    epoch: u64,
    lease_id: novarocks_spi::connector::CredentialLeaseId,
) -> QueryControlEvent {
    use novarocks_proto_models::novarocks as wire;
    protocol_control_event(
        wire::query_control_response::Event::CredentialLeaseCommitted(
            wire::CredentialLeaseCommitted {
                lease_id: lease_id.as_bytes().to_vec(),
                epoch,
            },
        ),
    )
}

fn credential_lease_error(error: CredentialLeaseError) -> QueryLifecycleError {
    let code = match error {
        CredentialLeaseError::Invalid => QueryLifecycleErrorCode::InvalidManifest,
        CredentialLeaseError::Conflict => QueryLifecycleErrorCode::Conflict,
        CredentialLeaseError::Terminated => QueryLifecycleErrorCode::Terminated,
    };
    QueryLifecycleError::new(code, error.detail())
}

fn send_credential_lease_event(
    events: Option<tokio::sync::mpsc::Sender<QueryControlEvent>>,
    event: QueryControlEvent,
) -> Result<(), QueryLifecycleError> {
    let Some(events) = events else {
        return Err(QueryLifecycleError::new(
            QueryLifecycleErrorCode::Terminated,
            "query control stream is unavailable for credential lease acknowledgement",
        ));
    };
    events.try_send(event).map_err(|_| {
        QueryLifecycleError::new(
            QueryLifecycleErrorCode::Internal,
            "credential lease acknowledgement queue is unavailable",
        )
    })
}

fn catalog_load_failed_event(safe_detail: String) -> QueryControlEvent {
    use novarocks_proto_models::{catalog, novarocks as wire};
    protocol_control_event(wire::query_control_response::Event::CatalogLoadFailed(
        catalog::CatalogLoadFailed {
            reason: catalog::CatalogLoadFailureReason::InstallFailed as i32,
            safe_detail,
            safe_field_path: None,
        },
    ))
}

fn local_drained_event() -> QueryControlEvent {
    use novarocks_proto_models::novarocks as wire;
    protocol_control_event(wire::query_control_response::Event::LocalDrained(
        wire::QueryControlLocalDrained {},
    ))
}

fn heartbeat_ack_event(sequence: u64) -> QueryControlEvent {
    use novarocks_proto_models::novarocks as wire;
    protocol_control_event(wire::query_control_response::Event::HeartbeatAck(
        wire::QueryControlHeartbeatAck { sequence },
    ))
}

fn local_failure_event(code: String, detail: String) -> QueryControlEvent {
    use novarocks_proto_models::novarocks as wire;
    protocol_control_event(wire::query_control_response::Event::LocalFailure(
        wire::QueryControlLocalFailure { code, detail },
    ))
}

fn termination_accepted_event(reason: QueryTerminationReason) -> QueryControlEvent {
    use novarocks_proto_models::novarocks as wire;
    protocol_control_event(wire::query_control_response::Event::TerminationAccepted(
        wire::QueryControlTerminationAccepted {
            reason: reason as i32,
        },
    ))
}

fn terminal_outcome_event(outcome: &ParticipantTerminalOutcome) -> QueryControlEvent {
    use novarocks_proto_models::novarocks as wire;
    protocol_control_event(wire::query_control_response::Event::TerminalOutcome(
        outcome.as_proto().clone(),
    ))
}

fn terminal_outcome_matches(
    outcome: Option<&ParticipantTerminalOutcome>,
    participant: &ParticipantAttemptRef,
) -> bool {
    outcome.is_some_and(|outcome| outcome.participant() == *participant)
}

fn protocol_unique_id(value: UniqueId) -> novarocks_proto_models::common::UniqueId {
    novarocks_proto_models::common::UniqueId {
        hi: value.high(),
        lo: value.low(),
    }
}

fn participant_attempt_ref(
    execution_id: QueryExecutionId,
    manifest: &novarocks_proto_codec::lifecycle::ParticipantManifest,
) -> Result<ParticipantAttemptRef, QueryLifecycleError> {
    let backend = manifest.backend().map_err(protocol_contract_error)?;
    ParticipantAttemptRef::new(
        execution_id,
        backend.process_id().map_err(protocol_contract_error)?,
    )
    .map_err(protocol_contract_error)
}

#[expect(
    clippy::too_many_arguments,
    reason = "The sealed terminal-fragment carrier has one parameter per required protocol field."
)]
fn terminal_fragment_snapshot(
    fragment_instance_id: UniqueId,
    backend_num: i32,
    outcome: novarocks_proto_models::novarocks::QueryTerminalFragmentOutcome,
    error_code: String,
    error_detail: String,
    sink: SinkCommitReportSnapshot,
    profile: Option<RuntimeProfileTree>,
    statistics_payload: Vec<u8>,
) -> Result<FragmentTerminalSnapshot, QueryLifecycleError> {
    use novarocks_proto_codec::lifecycle::terminal::FragmentTerminalSnapshot as ProtocolFragment;
    use novarocks_proto_models::novarocks as wire;
    use wire::fragment_terminal_profile_telemetry::Telemetry;
    let profile = wire::FragmentTerminalProfileTelemetry {
        telemetry: Some(match profile {
            Some(value) => Telemetry::Available(encode_runtime_profile_tree(&value)),
            None => Telemetry::Unavailable(wire::TerminalTelemetryUnavailable {
                stage: "fragment_profile".to_owned(),
                code: "PROFILE_UNAVAILABLE".to_owned(),
            }),
        }),
    };
    ProtocolFragment::seal(wire::QueryTerminalFragmentSnapshot {
        fragment_instance_id: Some(protocol_unique_id(fragment_instance_id)),
        backend_num,
        outcome: outcome as i32,
        error_code,
        error_detail,
        error_detail_truncated: false,
        tablet_commit_infos: sink
            .tablet_commit_infos
            .into_iter()
            .map(|value| wire::QueryTerminalTabletInfo {
                tablet_id: value.tablet_id,
                backend_id: value.backend_id,
            })
            .collect(),
        tablet_fail_infos: sink
            .tablet_fail_infos
            .into_iter()
            .map(|value| wire::QueryTerminalTabletInfo {
                tablet_id: value.tablet_id,
                backend_id: value.backend_id,
            })
            .collect(),
        load_stats: Some(wire::QueryTerminalLoadStats {
            loaded_rows: sink.load_stats.loaded_rows,
            loaded_bytes: sink.load_stats.loaded_bytes,
            filtered_rows: sink.load_stats.filtered_rows,
        }),
        profile: Some(profile),
        statistics_payload,
    })
    .map_err(protocol_contract_error)
}

fn fragment_outcome(
    snapshot: &FragmentTerminalSnapshot,
) -> novarocks_proto_models::novarocks::QueryTerminalFragmentOutcome {
    snapshot.outcome()
}

fn terminal_outcome_from_snapshot(
    participant: ParticipantAttemptRef,
    facts: Vec<FragmentTerminalSnapshot>,
    profile_contribution: novarocks_proto_models::novarocks::QueryTerminalProfileContributionTelemetry,
) -> Result<(QueryTerminalSnapshot, ParticipantTerminalOutcome), QueryLifecycleError> {
    use novarocks_proto_codec::lifecycle::terminal::{
        QueryTerminalSnapshot as ProtocolSnapshot, TerminalizationProof as ProtocolProof,
    };
    use novarocks_proto_models::novarocks as wire;
    use wire::participant_terminal_outcome::Outcome;
    let snapshot = ProtocolSnapshot::parse(wire::QueryTerminalSnapshot {
        version: novarocks_proto_codec::lifecycle::terminal::QUERY_TERMINAL_SNAPSHOT_VERSION_V1,
        participant: Some(participant.as_proto().clone()),
        fragments: facts
            .into_iter()
            .map(|value| value.as_proto().clone())
            .collect(),
        profile_contribution: Some(profile_contribution),
    })
    .map_err(protocol_contract_error)?;
    let proof = ProtocolProof::parse(wire::TerminalizationProof {
        version: 1,
        participant: Some(participant.as_proto().clone()),
        fragments: snapshot
            .fragments()
            .into_iter()
            .map(|fragment| {
                let raw = fragment.as_proto();
                wire::TerminalizationProofFragment {
                    fragment_instance_id: raw.fragment_instance_id,
                    backend_num: raw.backend_num,
                    outcome: raw.outcome,
                    error_code: raw.error_code.clone(),
                    error_detail: raw.error_detail.clone(),
                    error_detail_truncated: raw.error_detail_truncated,
                }
            })
            .collect(),
    })
    .map_err(protocol_contract_error)?;
    let outcome = ParticipantTerminalOutcome::parse(wire::ParticipantTerminalOutcome {
        outcome: Some(Outcome::Proof(proof.as_proto().clone())),
        snapshot: Some(snapshot.as_proto().clone()),
    })
    .map_err(protocol_contract_error)?;
    Ok((snapshot, outcome))
}

fn negative_terminal_outcome(
    participant: ParticipantAttemptRef,
    reason: novarocks_proto_models::novarocks::NegativeAttestationReason,
    detail: String,
) -> ParticipantTerminalOutcome {
    use novarocks_proto_codec::lifecycle::terminal::NegativeAttestation as ProtocolAttestation;
    use novarocks_proto_models::novarocks as wire;
    use wire::participant_terminal_outcome::Outcome;
    let attestation = ProtocolAttestation::parse(wire::NegativeAttestation {
        participant: Some(participant.as_proto().clone()),
        reason: reason as i32,
        detail,
        detail_truncated: false,
    })
    .expect("Backend-generated negative attestation satisfies the Protocol contract");
    ParticipantTerminalOutcome::parse(wire::ParticipantTerminalOutcome {
        outcome: Some(Outcome::NegativeAttestation(attestation.as_proto().clone())),
        snapshot: None,
    })
    .expect("Backend-generated negative outcome satisfies the Protocol contract")
}

fn terminal_profile_contribution(
    snapshot: RuntimeFilterObservationSnapshot,
) -> Result<QueryTerminalProfileContributionV1, QueryLifecycleError> {
    use novarocks_proto_models::{common, novarocks as wire};
    let channels = snapshot
        .channels()
        .iter()
        .map(|channel| {
            let terminal_state = match channel.terminal() {
                None => wire::QueryTerminalRuntimeFilterChannelTerminalStateV1::Open,
                Some(RuntimeFilterChannelTerminal::Completed(_)) => {
                    wire::QueryTerminalRuntimeFilterChannelTerminalStateV1::Completed
                }
                Some(RuntimeFilterChannelTerminal::Unavailable(_)) => {
                    wire::QueryTerminalRuntimeFilterChannelTerminalStateV1::Unavailable
                }
                Some(RuntimeFilterChannelTerminal::Cancelled) => {
                    wire::QueryTerminalRuntimeFilterChannelTerminalStateV1::Cancelled
                }
            };
            let identity = channel.identity();
            wire::QueryTerminalRuntimeFilterChannelV1 {
                channel_binding_id: identity.binding_id().get(),
                channel_id: identity.channel_id().get(),
                install_state: wire::QueryTerminalRuntimeFilterChannelInstallStateV1::Installed
                    as i32,
                terminal_state: terminal_state as i32,
                latest_published_logical_version: channel
                    .latest_published_version()
                    .map(|value| value.get()),
                published_count: channel.published(),
                completed_count: channel.completed(),
                unavailable_count: channel.unavailable(),
                cancelled_count: channel.cancelled(),
            }
        })
        .collect();
    let producer_streams = snapshot
        .producer_streams()
        .iter()
        .map(|stream| {
            let identity = stream.identity();
            let channel = identity.channel();
            let fragment = identity.fragment_instance_id();
            wire::QueryTerminalRuntimeFilterProducerStreamV1 {
                channel_binding_id: channel.binding_id().get(),
                channel_id: channel.channel_id().get(),
                producer_fragment_instance_id: Some(common::UniqueId {
                    hi: fragment.high(),
                    lo: fragment.low(),
                }),
                partition_id: identity.partition_id().get(),
                latest_accepted_sequence: stream.latest_accepted_sequence(),
                accepted_count: stream.accepted(),
                duplicate_count: stream.duplicate(),
                stale_count: stream.stale(),
                conflict_count: stream.conflict(),
                resource_limit_count: stream.resource_limit(),
            }
        })
        .collect();
    let transport_routes = snapshot
        .transport_routes()
        .iter()
        .map(|route| {
            let identity = route.identity();
            let channel = identity.channel();
            wire::QueryTerminalRuntimeFilterTransportRouteV1 {
                channel_binding_id: channel.binding_id().get(),
                channel_id: channel.channel_id().get(),
                route_edge_id: identity.route_edge_id().get(),
                sent_count: route.sent(),
                sent_bytes: route.sent_bytes(),
                retried_count: route.retried(),
                retried_bytes: route.retried_bytes(),
                acked_count: route.acked(),
                acked_bytes: route.acked_bytes(),
                fail_open_count: route.failed_open(),
                fail_open_bytes: route.failed_open_bytes(),
            }
        })
        .collect();
    let consumers = snapshot
        .consumers()
        .iter()
        .map(|consumer| {
            let identity = consumer.identity();
            let subscription_terminal = match consumer.terminal() {
                Some(novarocks_execution::runtime_filter::LiveTerminal::Completed) => {
                    wire::QueryTerminalRuntimeFilterSubscriptionTerminalV1::Completed
                }
                Some(
                    novarocks_execution::runtime_filter::LiveTerminal::CompletedWithoutArtifact,
                ) => {
                    wire::QueryTerminalRuntimeFilterSubscriptionTerminalV1::CompletedWithoutArtifact
                }
                Some(novarocks_execution::runtime_filter::LiveTerminal::Unavailable(_)) => {
                    wire::QueryTerminalRuntimeFilterSubscriptionTerminalV1::Unavailable
                }
                Some(novarocks_execution::runtime_filter::LiveTerminal::Cancelled) => {
                    wire::QueryTerminalRuntimeFilterSubscriptionTerminalV1::Cancelled
                }
                None => match consumer.outcome() {
                    None => wire::QueryTerminalRuntimeFilterSubscriptionTerminalV1::Pending,
                    Some(RuntimeFilterConsumerOutcome::Acquired) => {
                        wire::QueryTerminalRuntimeFilterSubscriptionTerminalV1::Acquired
                    }
                    Some(RuntimeFilterConsumerOutcome::TimedOut) => {
                        wire::QueryTerminalRuntimeFilterSubscriptionTerminalV1::TimedOut
                    }
                    Some(RuntimeFilterConsumerOutcome::Unavailable(_)) => {
                        wire::QueryTerminalRuntimeFilterSubscriptionTerminalV1::Unavailable
                    }
                    Some(RuntimeFilterConsumerOutcome::Unsupported(_)) => {
                        wire::QueryTerminalRuntimeFilterSubscriptionTerminalV1::Unsupported
                    }
                    Some(RuntimeFilterConsumerOutcome::Cancelled) => {
                        wire::QueryTerminalRuntimeFilterSubscriptionTerminalV1::Cancelled
                    }
                },
            };
            let reasons = consumer.scan_not_evaluated_reasons();
            let channel = identity.channel();
            let fragment = identity.fragment_instance_id();
            wire::QueryTerminalRuntimeFilterConsumerV1 {
                channel_binding_id: channel.binding_id().get(),
                channel_id: channel.channel_id().get(),
                consumer_binding_id: identity.consumer_binding_id().get(),
                fragment_instance_id: Some(common::UniqueId {
                    hi: fragment.high(),
                    lo: fragment.low(),
                }),
                latest_delivered_logical_version: consumer
                    .latest_delivered_version()
                    .map(|value| value.get()),
                latest_applied_logical_version: consumer
                    .latest_applied_version()
                    .map(|value| value.get()),
                subscription_terminal: subscription_terminal as i32,
                row_evaluations: consumer.row_evaluations(),
                input_rows: consumer.row_input(),
                output_rows: consumer.row_output(),
                scan_evaluated: consumer.scan_evaluated(),
                scan_kept: consumer.scan_kept(),
                scan_pruned: consumer.scan_pruned(),
                scan_not_evaluated: consumer.scan_not_evaluated(),
                scan_not_evaluated_reasons: Some(
                    wire::QueryTerminalRuntimeFilterScanNotEvaluatedV1 {
                        unit_facts_missing: reasons.unit_facts_missing,
                        column_facts_missing: reasons.column_facts_missing,
                        data_type_unsupported: reasons.data_type_unsupported,
                        predicate_capability_unsupported: reasons.predicate_capability_unsupported,
                        resource_unavailable: reasons.resource_unavailable,
                        snapshot_unavailable: reasons.snapshot_unavailable,
                        snapshot_timed_out: reasons.snapshot_timed_out,
                        snapshot_not_published: reasons.snapshot_not_published,
                    },
                ),
            }
        })
        .collect();
    QueryTerminalProfileContributionV1::seal(wire::QueryTerminalProfileContributionV1 {
        version:
            novarocks_proto_codec::lifecycle::terminal::QUERY_TERMINAL_PROFILE_CONTRIBUTION_VERSION_V1,
        channels,
        producer_streams,
        transport_routes,
        consumers,
    })
    .map_err(protocol_contract_error)
}

// Design: ADR-0106 (docs/adr/ADR-0106-native-wire-layering-and-terminal-content-identity.md)
pub(super) fn capture_terminal_profile_contribution(
    snapshot: Option<RuntimeFilterObservationSnapshot>,
    runtime_filter_installed: bool,
) -> Result<
    novarocks_proto_models::novarocks::QueryTerminalProfileContributionTelemetry,
    QueryLifecycleError,
> {
    use novarocks_proto_models::novarocks as wire;
    use wire::query_terminal_profile_contribution_telemetry::Telemetry;
    let unavailable = |code: &str| wire::QueryTerminalProfileContributionTelemetry {
        telemetry: Some(Telemetry::Unavailable(wire::TerminalTelemetryUnavailable {
            stage: "runtime_filter_terminal_capture".to_owned(),
            code: code.to_owned(),
        })),
    };
    let Some(snapshot) = snapshot else {
        if runtime_filter_installed {
            return Ok(unavailable("PARTICIPANT_RELEASED"));
        }
        return Ok(wire::QueryTerminalProfileContributionTelemetry {
            telemetry: Some(Telemetry::Available(wire::QueryTerminalProfileContributionV1 {
                version: novarocks_proto_codec::lifecycle::terminal::QUERY_TERMINAL_PROFILE_CONTRIBUTION_VERSION_V1,
                ..Default::default()
            })),
        });
    };
    if let Some(error) = snapshot.correctness_error() {
        return Err(QueryLifecycleError::new(
            QueryLifecycleErrorCode::InvalidManifest,
            format!("runtime-filter observation correctness failure: {error}"),
        ));
    }
    match terminal_profile_contribution(snapshot) {
        Ok(contribution) => Ok(wire::QueryTerminalProfileContributionTelemetry {
            telemetry: Some(Telemetry::Available(contribution.as_proto().clone())),
        }),
        Err(error) => {
            warn!(
                target: "novarocks::query_lifecycle",
                error = %error,
                "runtime-filter terminal profile contribution is unavailable"
            );
            Ok(unavailable("CONTRIBUTION_INVALID"))
        }
    }
}

fn send_reserved_control_event(
    permit: Option<tokio::sync::mpsc::OwnedPermit<QueryControlEvent>>,
    events: Option<tokio::sync::mpsc::Sender<QueryControlEvent>>,
    event: QueryControlEvent,
) {
    if let Some(permit) = permit {
        drop(permit.send(event));
    } else if let Some(events) = events {
        // The fallback is only reachable for entries created before a permit
        // was installed or after a duplicate terminal transition. Preserve
        // the existing best-effort behavior without blocking a runtime thread.
        let _ = events.try_send(event);
    }
}

impl BackendRuntimeFilterEnvelopeIngress for QueryLifecycleRegistry {
    fn accept(
        &self,
        envelope: BackendNativeRuntimeFilterEnvelope,
    ) -> crate::runtime_filter::domain::BackendIngressResult {
        self.dispatch_runtime_filter_envelope(envelope)
    }
}

pub(crate) trait QueryLifecycleLocalRuntime: Send + Sync + 'static {
    fn quiesce_query(
        &self,
        execution_id: QueryExecutionId,
        expected_instances: &[UniqueId],
        reason: QueryTerminationReason,
        detail: &str,
    );

    fn release_query_resources(&self, execution_id: QueryExecutionId);
}

/// Backend-local ingress state that remains useful until an attempt reaches
/// its lifecycle tombstone. It is deliberately separate from execution
/// resources: a late TaskUpdate must still be able to confirm an immutable
/// split watermark after a fragment has finished.
pub(crate) trait QueryLifecycleTerminalCleanup: Send + Sync + 'static {
    fn cleanup_terminal_execution(&self, execution_id: QueryExecutionId);
}

pub(crate) trait MonotonicClock: Send + Sync + 'static {
    fn now(&self) -> Instant;
}

pub(crate) trait QueryLifecycleMetricsSink: Send + Sync + 'static {
    fn publish(
        &self,
        snapshot: BackendQueryLifecycleMetricsSnapshot,
        termination_reasons: [u64; 6],
    );
}

struct SystemMonotonicClock;

impl MonotonicClock for SystemMonotonicClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

struct PrometheusQueryLifecycleMetricsSink;

impl QueryLifecycleMetricsSink for PrometheusQueryLifecycleMetricsSink {
    fn publish(
        &self,
        snapshot: BackendQueryLifecycleMetricsSnapshot,
        termination_reasons: [u64; 6],
    ) {
        publish_backend_query_lifecycle_metrics(snapshot, termination_reasons);
    }
}

#[derive(Clone, Copy)]
pub struct QueryLifecycleRegistryConfig {
    pub(crate) max_active_entries: usize,
    pub(crate) tombstone_capacity: usize,
    pub(crate) tombstone_retention: Duration,
    pub(crate) heartbeat_timeout: Duration,
    pub(crate) pre_start_timeout: Duration,
    pub(crate) stage_max_fragments: usize,
    pub(crate) max_active_staging: usize,
    pub(crate) stage_max_encoded_bytes: usize,
    pub(crate) stage_max_inflight_encoded_bytes: usize,
    pub(crate) stage_max_dormant_workers: usize,
    pub(crate) terminal_max_encoded_bytes: usize,
    pub(crate) terminal_drain_timeout: Duration,
    pub(crate) terminal_ack_timeout: Duration,
    pub(crate) terminal_fallback_rpc_timeout: Duration,
    pub(crate) terminal_fallback_max_attempts: usize,
    pub(crate) terminal_fallback_initial_backoff: Duration,
    pub(crate) terminal_fallback_max_backoff: Duration,
    pub(crate) terminal_retention: Duration,
    pub(crate) terminal_retained_capacity: usize,
    pub(crate) terminal_max_retained_bytes: usize,
}

impl QueryLifecycleRegistryConfig {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        max_active_entries: usize,
        tombstone_capacity: usize,
        tombstone_retention: Duration,
        heartbeat_timeout: Duration,
        pre_start_timeout: Duration,
        stage_max_fragments: usize,
        max_active_staging: usize,
        stage_max_encoded_bytes: usize,
        stage_max_inflight_encoded_bytes: usize,
        stage_max_dormant_workers: usize,
        terminal_max_encoded_bytes: usize,
        terminal_drain_timeout: Duration,
        terminal_ack_timeout: Duration,
        terminal_fallback_rpc_timeout: Duration,
        terminal_fallback_max_attempts: usize,
        terminal_fallback_initial_backoff: Duration,
        terminal_fallback_max_backoff: Duration,
        terminal_retention: Duration,
        terminal_retained_capacity: usize,
        terminal_max_retained_bytes: usize,
    ) -> Self {
        Self {
            max_active_entries,
            tombstone_capacity,
            tombstone_retention,
            heartbeat_timeout,
            pre_start_timeout,
            stage_max_fragments,
            max_active_staging,
            stage_max_encoded_bytes,
            stage_max_inflight_encoded_bytes,
            stage_max_dormant_workers,
            terminal_max_encoded_bytes,
            terminal_drain_timeout,
            terminal_ack_timeout,
            terminal_fallback_rpc_timeout,
            terminal_fallback_max_attempts,
            terminal_fallback_initial_backoff,
            terminal_fallback_max_backoff,
            terminal_retention,
            terminal_retained_capacity,
            terminal_max_retained_bytes,
        }
    }
}

/// Global, backend-local accounting for QLC-3 work which exists before a
/// query is allowed to run.  The counters deliberately cover the full
/// pre-start lifetime, not only the RPC handler: a completed Stage still owns
/// decoded plans and dormant workers until Start or Abort wins the lifecycle
/// race.
#[derive(Default)]
struct StageResourceLedger {
    active_builders: usize,
    encoded_bytes: usize,
    dormant_workers: usize,
}

impl StageResourceLedger {
    fn publish_snapshot(active_builders: usize, encoded_bytes: usize, dormant_workers: usize) {
        publish_backend_query_execution_resource("stage_active_builders", active_builders);
        publish_backend_query_execution_resource("stage_encoded_bytes", encoded_bytes);
        publish_backend_query_execution_resource("stage_dormant_workers", dormant_workers);
    }
}

/// RAII reservation for one participant-local Stage bundle.  It first owns a
/// builder slot, then transfers the encoded-byte and dormant-worker portions
/// to the lifecycle entry after a successful commit.  Drop is intentionally
/// sufficient for every failure path, including panics while materializing a
/// fragment bundle.
pub(crate) struct StageResourceReservation {
    ledger: Arc<Mutex<StageResourceLedger>>,
    encoded_bytes: usize,
    dormant_workers: usize,
    builder_active: bool,
}

impl StageResourceReservation {
    fn try_acquire(
        ledger: Arc<Mutex<StageResourceLedger>>,
        config: QueryLifecycleRegistryConfig,
        encoded_bytes: usize,
        dormant_workers: usize,
    ) -> Result<Self, &'static str> {
        let mut state = ledger
            .lock()
            .expect("query lifecycle Stage resource ledger lock");
        if state.active_builders >= config.max_active_staging {
            return Err("backend has reached its active Stage builder limit");
        }
        let Some(next_bytes) = state.encoded_bytes.checked_add(encoded_bytes) else {
            return Err("backend Stage encoded-byte accounting overflowed");
        };
        if next_bytes > config.stage_max_inflight_encoded_bytes {
            return Err("backend has reached its Stage encoded-byte budget");
        }
        let Some(next_workers) = state.dormant_workers.checked_add(dormant_workers) else {
            return Err("backend Stage dormant-worker accounting overflowed");
        };
        if next_workers > config.stage_max_dormant_workers {
            return Err("backend has reached its dormant worker limit");
        }
        state.active_builders += 1;
        state.encoded_bytes = next_bytes;
        state.dormant_workers = next_workers;
        let snapshot = (
            state.active_builders,
            state.encoded_bytes,
            state.dormant_workers,
        );
        drop(state);
        StageResourceLedger::publish_snapshot(snapshot.0, snapshot.1, snapshot.2);
        Ok(Self {
            ledger,
            encoded_bytes,
            dormant_workers,
            builder_active: true,
        })
    }

    fn release_builder(&mut self) {
        if !self.builder_active {
            return;
        }
        let mut state = self
            .ledger
            .lock()
            .expect("query lifecycle Stage resource ledger lock");
        state.active_builders = state.active_builders.saturating_sub(1);
        let snapshot = (
            state.active_builders,
            state.encoded_bytes,
            state.dormant_workers,
        );
        drop(state);
        StageResourceLedger::publish_snapshot(snapshot.0, snapshot.1, snapshot.2);
        self.builder_active = false;
    }
}

impl Drop for StageResourceReservation {
    fn drop(&mut self) {
        let mut state = self
            .ledger
            .lock()
            .expect("query lifecycle Stage resource ledger lock");
        if self.builder_active {
            state.active_builders = state.active_builders.saturating_sub(1);
        }
        state.encoded_bytes = state.encoded_bytes.saturating_sub(self.encoded_bytes);
        state.dormant_workers = state.dormant_workers.saturating_sub(self.dormant_workers);
        let snapshot = (
            state.active_builders,
            state.encoded_bytes,
            state.dormant_workers,
        );
        drop(state);
        StageResourceLedger::publish_snapshot(snapshot.0, snapshot.1, snapshot.2);
    }
}

pub(crate) struct QueryLifecycleRegistry {
    state: Mutex<QueryLifecycleRegistryState>,
    local_runtime: Arc<dyn QueryLifecycleLocalRuntime>,
    /// BE-owned runtime for bounded, lifecycle-checked catalog bind work.
    /// The task never outlives its query authority: every wait and completion
    /// rechecks the entry before it can lease or publish Ready.
    catalog_install_runtime: BackendDataRuntime,
    catalog_manager: Arc<
        crate::connector::catalog_manager::CatalogManager<
            crate::connector::ConnectorExecutionRoleBinding,
        >,
    >,
    execution_role_binding_factories:
        Arc<crate::connector::catalog_manager::ConnectorExecutionRoleBindingFactorySet>,
    runtime_filter_factory: Arc<dyn RuntimeFilterParticipantFactory>,
    config: QueryLifecycleRegistryConfig,
    local_process_id: BackendProcessId,
    native_compatibility_id: NativeCompatibilityId,
    clock: Arc<dyn MonotonicClock>,
    metrics: Arc<dyn QueryLifecycleMetricsSink>,
    stage_resources: Arc<Mutex<StageResourceLedger>>,
    terminal_fallback: Arc<dyn QueryTerminalFallbackTransport>,
    terminal_cleanup: Mutex<Option<Weak<dyn QueryLifecycleTerminalCleanup>>>,
    /// Test-only local fault claims exercise the same terminal delivery
    /// transitions as runner-bound faults without sharing process environment
    /// state between unit tests.
    #[cfg(test)]
    terminal_test_faults: Mutex<BTreeMap<QueryExecutionId, Vec<QueryLifecycleFaultKind>>>,
    self_weak: Weak<QueryLifecycleRegistry>,
}

/// A context-local view of one query attempt's confidential lease state.  The
/// captured execution id is the only lookup key; it never exposes a registry
/// operation that could enumerate or resolve another attempt's values.
struct QueryCredentialStorageResolver {
    registry: Weak<QueryLifecycleRegistry>,
    execution_id: QueryExecutionId,
}

impl ConnectorStorageResolver for QueryCredentialStorageResolver {
    fn resolve_vended_s3(
        &self,
        request: &StorageAccessRequest,
    ) -> Result<ResolvedVendedS3Access, ConnectorError> {
        let registry = self.registry.upgrade().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "vended storage query attempt is no longer available",
            )
        })?;
        let entry = registry
            .state
            .lock()
            .expect("query lifecycle registry lock")
            .entries
            .get(&self.execution_id)
            .cloned()
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "vended storage query attempt is unavailable",
                )
            })?;
        let state = entry.state.lock().expect("query lifecycle entry lock");
        if matches!(
            state.phase,
            QueryLifecyclePhase::TerminalRetained
                | QueryLifecyclePhase::Terminating
                | QueryLifecyclePhase::Tombstone
        ) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "vended storage query attempt is no longer active",
            ));
        }
        state.credential_leases.resolve_vended_s3(request)
    }
}

struct GrpcQueryTerminalFallbackTransport {
    runtime: BackendDataRuntime,
}

impl QueryTerminalFallbackTransport for GrpcQueryTerminalFallbackTransport {
    fn report_query_terminal(
        &self,
        endpoint: &QueryControlEndpoint,
        outcome: ParticipantTerminalOutcome,
        timeout: Duration,
    ) -> Result<QueryTerminalReportAck, QueryTerminalFallbackTransportError> {
        let client = BackendRpcClient::new_host_port(
            self.runtime.clone(),
            endpoint.host().to_string(),
            endpoint.port(),
        )
        .map_err(QueryTerminalFallbackTransportError::unavailable)?;
        let response = client
            .blocking_report_query_terminal_with_timeout(
                novarocks_proto_models::novarocks::ReportQueryTerminalRequest {
                    outcome: Some(outcome.as_proto().clone()),
                },
                timeout,
            )
            .map_err(QueryTerminalFallbackTransportError::unavailable)?;
        let outcome = match novarocks_proto_models::novarocks::ReportQueryTerminalOutcome::try_from(
            response.outcome,
        ) {
            Ok(novarocks_proto_models::novarocks::ReportQueryTerminalOutcome::Accepted) => {
                QueryTerminalReportOutcome::Accepted
            }
            Ok(novarocks_proto_models::novarocks::ReportQueryTerminalOutcome::AlreadyAccepted) => {
                QueryTerminalReportOutcome::AlreadyAccepted
            }
            Ok(novarocks_proto_models::novarocks::ReportQueryTerminalOutcome::RejectedConflict) => {
                QueryTerminalReportOutcome::RejectedConflict
            }
            Ok(novarocks_proto_models::novarocks::ReportQueryTerminalOutcome::RejectedGone)
            | Err(_) => QueryTerminalReportOutcome::RejectedGone,
            Ok(novarocks_proto_models::novarocks::ReportQueryTerminalOutcome::Unspecified) => {
                QueryTerminalReportOutcome::RejectedGone
            }
        };
        QueryTerminalReportAck::new(outcome, response.detail)
            .map_err(|error| QueryTerminalFallbackTransportError::unavailable(error.to_string()))
    }
}

#[derive(Default)]
struct QueryLifecycleRegistryState {
    draining: bool,
    entries: BTreeMap<QueryExecutionId, Arc<QueryLifecycleEntry>>,
    fragment_executions: BTreeMap<UniqueId, QueryExecutionId>,
    tombstones: VecDeque<QueryExecutionId>,
    active_entries: usize,
    init_conflicts: u64,
    admission_rejected: u64,
    heartbeat_timeouts: u64,
    terminations: u64,
    termination_reasons: [u64; 6],
    pre_init_tombstones: BTreeMap<QueryExecutionId, PreInitTombstone>,
    terminal_retained: BTreeMap<QueryExecutionId, usize>,
    terminal_retained_bytes: usize,
    terminal_facts: u64,
    terminal_locally_drained: u64,
    terminal_records_frozen: u64,
    terminal_acknowledged: u64,
    terminal_retention_expired: u64,
    terminal_fallback_accepted: u64,
    terminal_fallback_rejected: u64,
}

struct PreInitTombstone {
    participant: ParticipantAttemptRef,
    digest: ParticipantManifestDigest,
    reason: QueryTerminationReason,
    terminated_at: Instant,
}

struct InitWorkspace {
    registry: Arc<QueryLifecycleRegistry>,
    entry: Arc<QueryLifecycleEntry>,
    execution_id: QueryExecutionId,
    digest: ParticipantManifestDigest,
}

/// Owns the single in-flight Stage build.  Dropping an uncommitted build
/// fail-closes the lifecycle entry and wakes every dormant worker through its
/// shared gate.
pub(crate) struct StageBuildPermit {
    registry: Arc<QueryLifecycleRegistry>,
    entry: Arc<QueryLifecycleEntry>,
    execution_id: QueryExecutionId,
    digest: StageDigest,
    gate: Arc<super::stage::StartGate>,
    resources: Option<StageResourceReservation>,
    committed: bool,
}

pub(crate) enum StageBuildDecision {
    Build(StageBuildPermit),
    Complete(QueryStageAck),
}

pub(crate) struct FragmentAdmissionPermit {
    registry: Weak<QueryLifecycleRegistry>,
    execution_id: QueryExecutionId,
    fragment_instance_id: UniqueId,
    entry: Arc<QueryLifecycleEntry>,
    committed: bool,
}

impl fmt::Debug for FragmentAdmissionPermit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FragmentAdmissionPermit")
            .field("execution_id", &self.execution_id)
            .field("fragment_instance_id", &self.fragment_instance_id)
            .field("committed", &self.committed)
            .finish()
    }
}

struct RegistryQueryControl {
    registry: Weak<QueryLifecycleRegistry>,
    execution_id: QueryExecutionId,
}

#[allow(
    dead_code,
    reason = "Retained for lifecycle unit targets that record legacy fragment outcomes."
)]
fn fragment_snapshot_from_outcome(
    fragment_instance_id: UniqueId,
    backend_num: i32,
    outcome: &FragmentOutcome,
) -> Result<FragmentTerminalSnapshot, QueryLifecycleError> {
    use novarocks_proto_models::novarocks::QueryTerminalFragmentOutcome;
    let (outcome, code, detail) = match outcome {
        FragmentOutcome::Succeeded => (
            QueryTerminalFragmentOutcome::Succeeded,
            String::new(),
            String::new(),
        ),
        FragmentOutcome::Failed(error) => (
            QueryTerminalFragmentOutcome::Failed,
            "FRAGMENT_EXECUTION_FAILED".to_owned(),
            error.to_string(),
        ),
        FragmentOutcome::Cancelled { reason } => (
            QueryTerminalFragmentOutcome::Cancelled,
            "CANCELLED".to_owned(),
            reason.detail().to_owned(),
        ),
    };
    terminal_fragment_snapshot(
        fragment_instance_id,
        backend_num,
        outcome,
        code,
        detail,
        SinkCommitReportSnapshot::default(),
        None,
        Vec::new(),
    )
}

impl QueryLifecycleRegistry {
    #[cfg(test)]
    pub(crate) fn hold_registry_state_lock_for_test(
        &self,
        acquired: &std::sync::Barrier,
        release: &std::sync::Barrier,
    ) {
        let _state = self.state.lock().expect("query lifecycle registry lock");
        acquired.wait();
        release.wait();
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn new(
        local_process_id: BackendProcessId,
        local_runtime: Arc<dyn QueryLifecycleLocalRuntime>,
        config: QueryLifecycleRegistryConfig,
    ) -> Arc<Self> {
        Self::new_with_clock(
            local_process_id,
            local_runtime,
            config,
            Arc::new(SystemMonotonicClock),
        )
    }

    #[cfg(test)]
    pub(crate) fn new_with_clock(
        local_process_id: BackendProcessId,
        local_runtime: Arc<dyn QueryLifecycleLocalRuntime>,
        config: QueryLifecycleRegistryConfig,
        clock: Arc<dyn MonotonicClock>,
    ) -> Arc<Self> {
        Self::new_with_clock_and_metrics(
            local_process_id,
            local_runtime,
            config,
            clock,
            Arc::new(PrometheusQueryLifecycleMetricsSink),
        )
    }

    #[cfg(test)]
    pub(crate) fn new_with_clock_and_metrics(
        local_process_id: BackendProcessId,
        local_runtime: Arc<dyn QueryLifecycleLocalRuntime>,
        config: QueryLifecycleRegistryConfig,
        clock: Arc<dyn MonotonicClock>,
        metrics: Arc<dyn QueryLifecycleMetricsSink>,
    ) -> Arc<Self> {
        Self::new_with_clock_metrics_and_terminal_fallback(
            local_process_id,
            local_runtime,
            config,
            clock,
            metrics,
            Arc::new(GrpcQueryTerminalFallbackTransport {
                runtime: crate::rpc::runtime::test_backend_data_runtime(),
            }),
        )
    }

    #[cfg(test)]
    pub(crate) fn new_with_clock_metrics_and_terminal_fallback(
        local_process_id: BackendProcessId,
        local_runtime: Arc<dyn QueryLifecycleLocalRuntime>,
        config: QueryLifecycleRegistryConfig,
        clock: Arc<dyn MonotonicClock>,
        metrics: Arc<dyn QueryLifecycleMetricsSink>,
        terminal_fallback: Arc<dyn QueryTerminalFallbackTransport>,
    ) -> Arc<Self> {
        Self::new_with_backend_identity(
            crate::rpc::runtime::test_backend_data_runtime(),
            local_process_id,
            local_runtime,
            config,
            clock,
            metrics,
            terminal_fallback,
            NativeCompatibilityId::new([0x71; 32]),
            empty_execution_role_binding_factories(),
            default_catalog_manager(),
        )
    }

    #[cfg(test)]
    #[expect(
        clippy::too_many_arguments,
        reason = "The test constructor injects every lifecycle dependency to exercise failure paths deterministically."
    )]
    pub(crate) fn new_with_clock_metrics_terminal_fallback_and_runtime_filter_factory(
        local_process_id: BackendProcessId,
        local_runtime: Arc<dyn QueryLifecycleLocalRuntime>,
        config: QueryLifecycleRegistryConfig,
        clock: Arc<dyn MonotonicClock>,
        metrics: Arc<dyn QueryLifecycleMetricsSink>,
        terminal_fallback: Arc<dyn QueryTerminalFallbackTransport>,
        runtime_filter_factory: Arc<dyn RuntimeFilterParticipantFactory>,
    ) -> Arc<Self> {
        Self::new_with_backend_identity_and_runtime_filter_factory(
            crate::rpc::runtime::test_backend_data_runtime(),
            local_process_id,
            local_runtime,
            config,
            clock,
            metrics,
            terminal_fallback,
            runtime_filter_factory,
            NativeCompatibilityId::new([0x71; 32]),
            empty_execution_role_binding_factories(),
            default_catalog_manager(),
        )
    }

    #[cfg(test)]
    pub(crate) fn new_with_process_id(
        local_process_id: BackendProcessId,
        local_runtime: Arc<dyn QueryLifecycleLocalRuntime>,
        config: QueryLifecycleRegistryConfig,
    ) -> Arc<Self> {
        let runtime = crate::rpc::runtime::test_backend_data_runtime();
        Self::new_with_backend_identity(
            runtime.clone(),
            local_process_id,
            local_runtime,
            config,
            Arc::new(SystemMonotonicClock),
            Arc::new(PrometheusQueryLifecycleMetricsSink),
            Arc::new(GrpcQueryTerminalFallbackTransport { runtime }),
            NativeCompatibilityId::new([0x71; 32]),
            empty_execution_role_binding_factories(),
            default_catalog_manager(),
        )
    }

    pub(crate) fn new_with_runtime_and_execution_role_binding_factories(
        runtime: BackendDataRuntime,
        local_runtime: Arc<dyn QueryLifecycleLocalRuntime>,
        config: QueryLifecycleRegistryConfig,
        native_compatibility_id: NativeCompatibilityId,
        execution_role_binding_factories: Arc<
            crate::connector::catalog_manager::ConnectorExecutionRoleBindingFactorySet,
        >,
        catalog_manager: Arc<
            crate::connector::catalog_manager::CatalogManager<
                crate::connector::ConnectorExecutionRoleBinding,
            >,
        >,
    ) -> Arc<Self> {
        Self::new_with_backend_identity(
            runtime.clone(),
            BackendProcessId::new_v7(),
            local_runtime,
            config,
            Arc::new(SystemMonotonicClock),
            Arc::new(PrometheusQueryLifecycleMetricsSink),
            Arc::new(GrpcQueryTerminalFallbackTransport { runtime }),
            native_compatibility_id,
            execution_role_binding_factories,
            catalog_manager,
        )
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "This constructor wires each production lifecycle dependency before selecting the runtime-filter factory."
    )]
    fn new_with_backend_identity(
        runtime: BackendDataRuntime,
        local_process_id: BackendProcessId,
        local_runtime: Arc<dyn QueryLifecycleLocalRuntime>,
        config: QueryLifecycleRegistryConfig,
        clock: Arc<dyn MonotonicClock>,
        metrics: Arc<dyn QueryLifecycleMetricsSink>,
        terminal_fallback: Arc<dyn QueryTerminalFallbackTransport>,
        native_compatibility_id: NativeCompatibilityId,
        execution_role_binding_factories: Arc<
            crate::connector::catalog_manager::ConnectorExecutionRoleBindingFactorySet,
        >,
        catalog_manager: Arc<
            crate::connector::catalog_manager::CatalogManager<
                crate::connector::ConnectorExecutionRoleBinding,
            >,
        >,
    ) -> Arc<Self> {
        Self::new_with_backend_identity_and_runtime_filter_factory(
            runtime.clone(),
            local_process_id,
            local_runtime,
            config,
            clock,
            metrics,
            terminal_fallback,
            Arc::new(BackendRuntimeFilterParticipantFactory::new(runtime)),
            native_compatibility_id,
            execution_role_binding_factories,
            catalog_manager,
        )
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "This constructor accepts the complete production lifecycle dependency set and explicit runtime-filter factory."
    )]
    fn new_with_backend_identity_and_runtime_filter_factory(
        catalog_install_runtime: BackendDataRuntime,
        local_process_id: BackendProcessId,
        local_runtime: Arc<dyn QueryLifecycleLocalRuntime>,
        config: QueryLifecycleRegistryConfig,
        clock: Arc<dyn MonotonicClock>,
        metrics: Arc<dyn QueryLifecycleMetricsSink>,
        terminal_fallback: Arc<dyn QueryTerminalFallbackTransport>,
        runtime_filter_factory: Arc<dyn RuntimeFilterParticipantFactory>,
        native_compatibility_id: NativeCompatibilityId,
        execution_role_binding_factories: Arc<
            crate::connector::catalog_manager::ConnectorExecutionRoleBindingFactorySet,
        >,
        catalog_manager: Arc<
            crate::connector::catalog_manager::CatalogManager<
                crate::connector::ConnectorExecutionRoleBinding,
            >,
        >,
    ) -> Arc<Self> {
        assert!(config.max_active_entries > 0);
        assert!(config.tombstone_capacity > 0);
        assert!(!config.tombstone_retention.is_zero());
        assert!(!config.heartbeat_timeout.is_zero());
        assert!(!config.pre_start_timeout.is_zero());
        assert!(config.stage_max_fragments > 0);
        assert!(config.max_active_staging > 0);
        assert!(config.stage_max_encoded_bytes > 0);
        assert!(config.stage_max_inflight_encoded_bytes >= config.stage_max_encoded_bytes);
        assert!(config.stage_max_dormant_workers >= config.stage_max_fragments);
        assert!(!config.terminal_ack_timeout.is_zero());
        assert!(!config.terminal_fallback_rpc_timeout.is_zero());
        assert!(config.terminal_fallback_max_attempts > 0);
        assert!(!config.terminal_retention.is_zero());
        assert!(config.terminal_retained_capacity > 0);
        assert!(config.terminal_max_retained_bytes > 0);
        publish_backend_query_lifecycle_terminal_limits(
            config.terminal_retained_capacity,
            config.terminal_max_retained_bytes,
        );
        StageResourceLedger::publish_snapshot(0, 0, 0);
        let registry = Arc::new_cyclic(|self_weak| Self {
            state: Mutex::new(QueryLifecycleRegistryState::default()),
            local_runtime,
            catalog_install_runtime,
            catalog_manager,
            execution_role_binding_factories,
            runtime_filter_factory,
            config,
            local_process_id,
            native_compatibility_id,
            clock,
            metrics,
            stage_resources: Arc::new(Mutex::new(StageResourceLedger::default())),
            terminal_fallback,
            terminal_cleanup: Mutex::new(None),
            #[cfg(test)]
            terminal_test_faults: Mutex::new(BTreeMap::new()),
            self_weak: self_weak.clone(),
        });
        registry.publish_metrics();
        registry.publish_catalog_lease_metrics();
        registry
    }

    pub(crate) const fn local_process_id(&self) -> BackendProcessId {
        self.local_process_id
    }

    /// Resolve the typed read capability only through an admitted query's
    /// exact catalog lease. Retained catalog runtimes are intentionally not an
    /// authority path for fragment decode.
    pub(crate) fn catalog_read_execution_for_query(
        &self,
        execution_id: QueryExecutionId,
        handle: &novarocks_spi::connector::CatalogHandle,
    ) -> Result<crate::connector::ConnectorExecutionReadBinding, String> {
        let runtime = self
            .catalog_manager
            .resolve_for_query(execution_id, handle)
            .ok_or_else(|| {
                format!(
                    "no query-leased catalog runtime exists for {}@{}",
                    handle.catalog_name().as_str(),
                    handle.version().short_hex()
                )
            })?;
        runtime.read().cloned().ok_or_else(|| {
            format!(
                "catalog runtime for {}@{} has no typed read capability",
                handle.catalog_name().as_str(),
                handle.version().short_hex()
            )
        })
    }

    /// Resolve the catalog-scoped writer capability only through an admitted
    /// query's exact catalog lease. Retained catalog materializations are not
    /// a fragment-decode authority path.
    pub(crate) fn catalog_write_execution_for_query(
        &self,
        execution_id: QueryExecutionId,
        handle: &novarocks_spi::connector::CatalogHandle,
    ) -> Result<crate::connector::ConnectorExecutionWriteBinding, String> {
        let runtime = self
            .catalog_manager
            .resolve_for_query(execution_id, handle)
            .ok_or_else(|| {
                format!(
                    "no query-leased catalog runtime exists for {}@{}",
                    handle.catalog_name().as_str(),
                    handle.version().short_hex()
                )
            })?;
        runtime.write().cloned().ok_or_else(|| {
            format!(
                "catalog runtime for {}@{} has no catalog-scoped write capability",
                handle.catalog_name().as_str(),
                handle.version().short_hex()
            )
        })
    }

    /// Creates a process-local resolver locked to one exact query attempt.
    /// The returned object is injected only into a connector request context;
    /// it is neither a registry facade nor a native-wire value.
    pub(crate) fn storage_resolver_for_query(
        &self,
        execution_id: QueryExecutionId,
    ) -> Arc<dyn ConnectorStorageResolver> {
        Arc::new(QueryCredentialStorageResolver {
            registry: self.self_weak.clone(),
            execution_id,
        })
    }

    /// Install the backend-local cleanup owner after application composition
    /// has created the fragment service. The registry holds only a weak link:
    /// the service already owns this registry, so a strong link would form a
    /// shutdown-retaining reference cycle.
    pub(crate) fn install_terminal_cleanup(
        &self,
        cleanup: Weak<dyn QueryLifecycleTerminalCleanup>,
    ) {
        let mut installed = self
            .terminal_cleanup
            .lock()
            .expect("query lifecycle terminal cleanup lock");
        assert!(
            installed.replace(cleanup).is_none(),
            "query lifecycle terminal cleanup installed twice"
        );
    }

    /// Reject new attempts after SIGTERM drain has begun. Existing attempts
    /// retain their normal lifecycle; drain never reassigns this process.
    pub(crate) fn begin_drain(&self) {
        self.state
            .lock()
            .expect("query lifecycle registry lock")
            .draining = true;
    }

    pub(crate) fn is_draining(&self) -> bool {
        self.state
            .lock()
            .expect("query lifecycle registry lock")
            .draining
    }

    pub(crate) fn is_drained(&self) -> bool {
        let state = self.state.lock().expect("query lifecycle registry lock");
        state.draining && state.active_entries == 0
    }

    /// The terminal owner releases catalog references in the same window as
    /// other query execution resources: after terminal facts are frozen and
    /// before their delivery.  The manager retains a warm runtime according
    /// to its own bounded policy, but this attempt no longer protects it.
    fn release_query_resources(&self, execution_id: QueryExecutionId) {
        if let Some(entry) = self
            .state
            .lock()
            .expect("query lifecycle registry lock")
            .entries
            .get(&execution_id)
            .cloned()
        {
            entry
                .state
                .lock()
                .expect("query lifecycle entry lock")
                .credential_leases
                .clear();
        }
        self.catalog_manager.release_query(execution_id);
        self.publish_catalog_lease_metrics();
        self.local_runtime.release_query_resources(execution_id);
    }

    fn publish_catalog_lease_metrics(&self) {
        let snapshot = self.catalog_manager.lease_snapshot();
        publish_backend_query_execution_resource("catalog_query_leases", snapshot.query_leases);
        publish_backend_query_execution_resource("catalog_handle_leases", snapshot.handle_leases);
    }

    /// Admission-derived authorization for the native exchange data plane.
    /// Routes exist only while the owning lifecycle entry can still execute;
    /// tombstone/terminal retention therefore automatically revokes frames.
    pub(crate) fn authorize_exchange(
        &self,
        destination_fragment_instance_id: UniqueId,
        destination_node_id: i32,
        source_fragment_instance_id: UniqueId,
        sender_ordinal: u32,
        sender_count: u32,
    ) -> Result<(), String> {
        if sender_count == 0 || sender_ordinal >= sender_count {
            return Err("exchange sender ordinal/count is invalid".to_string());
        }
        let state = self
            .state
            .lock()
            .map_err(|_| "query lifecycle registry lock is poisoned".to_string())?;
        for entry in state.entries.values() {
            let phase = entry
                .state
                .lock()
                .map_err(|_| "query lifecycle entry lock is poisoned".to_string())?
                .phase;
            if !matches!(
                phase,
                QueryLifecyclePhase::Staged | QueryLifecyclePhase::Running
            ) {
                continue;
            }
            for route in validated(entry.manifest.exchange_routes()) {
                let source = validated(route.source_fragment_instance_id());
                let destination = validated(route.destination_fragment_instance_id());
                if UniqueId::new(source.hi, source.lo) == source_fragment_instance_id
                    && UniqueId::new(destination.hi, destination.lo)
                        == destination_fragment_instance_id
                    && route.destination_node_id() == destination_node_id
                    && route.sender_ordinal() == sender_ordinal
                    && route.sender_count() == sender_count
                {
                    return Ok(());
                }
            }
        }
        Err("exchange route is absent from every active participant manifest".to_string())
    }

    pub(crate) fn init_query(&self, request: QueryInitRequest) -> QueryInitAck {
        self.init_query_inner(request, false)
    }

    /// Reached only after the BE native RPC listener has verified a concrete
    /// TLS connection. The ordinary Init entrypoint cannot accept envelopes.
    pub(crate) fn init_query_tls(&self, request: QueryInitRequest) -> QueryInitAck {
        self.init_query_inner(request, true)
    }

    fn init_query_inner(&self, request: QueryInitRequest, tls_verified: bool) -> QueryInitAck {
        let manifest = validated(request.manifest());
        let execution_id = validated(manifest.execution_id());
        let digest = validated(manifest.digest());
        let credential_leases = match QueryCredentialLeases::from_tls_init(&request) {
            Ok(leases)
                if tls_verified
                    || request
                        .credential_lease_envelopes()
                        .is_ok_and(|values| values.is_empty()) =>
            {
                leases
            }
            Ok(_) | Err(_) => {
                let ack = QueryInitAck::new(
                    execution_id,
                    digest,
                    QueryInitOutcome::QueryInitRejectedInvalidManifest,
                );
                self.log_init(&ack);
                return ack;
            }
        };
        // Decode the exact frozen set before any local lifecycle entry or
        // provider side effect exists.  The validated manifest owns the
        // conflict fence for Init retries.
        let catalog_set = validated(manifest.catalog_set());
        let _catalogs = validated(catalog_set.catalogs());
        if validated(manifest.native_compatibility_id()) != self.native_compatibility_id {
            let ack = QueryInitAck::new(
                execution_id,
                digest,
                QueryInitOutcome::QueryInitRejectedCompatibilityMismatch,
            );
            self.log_init(&ack);
            return ack;
        }
        // The admission boundary derives the manifest identity exactly once and
        // retains it on the entry; later comparisons read the retained value.
        let manifest_backend = validated(manifest.backend());
        if validated(manifest_backend.process_id()) != self.local_process_id {
            let ack = QueryInitAck::new(
                execution_id,
                digest,
                QueryInitOutcome::QueryInitRejectedBackendProcessMismatch,
            );
            self.log_init(&ack);
            return ack;
        }
        let participant = ParticipantAttemptRef::new(execution_id, self.local_process_id)
            .expect("validated manifest identity creates a participant ref");

        let entry = {
            let mut state = self.state.lock().expect("query lifecycle registry lock");
            self.clean_tombstones_locked(&mut state, self.clock.now(), 64);
            if state.draining {
                let ack = QueryInitAck::new(
                    execution_id,
                    digest,
                    QueryInitOutcome::QueryInitRejectedBackendDraining,
                );
                drop(state);
                self.log_init(&ack);
                return ack;
            }
            if let Some(tombstone) = state.pre_init_tombstones.get(&execution_id) {
                let outcome = if tombstone.participant == participant && tombstone.digest == digest
                {
                    QueryInitOutcome::QueryInitRejectedTerminated
                } else {
                    state.init_conflicts = state.init_conflicts.saturating_add(1);
                    QueryInitOutcome::QueryInitRejectedConflict
                };
                let ack = QueryInitAck::new(execution_id, digest, outcome);
                drop(state);
                self.log_init(&ack);
                self.publish_metrics();
                return ack;
            }
            if let Some(entry) = state.entries.get(&execution_id).cloned() {
                if entry.digest != digest {
                    state.init_conflicts = state.init_conflicts.saturating_add(1);
                    let ack = QueryInitAck::new(
                        execution_id,
                        digest,
                        QueryInitOutcome::QueryInitRejectedConflict,
                    );
                    drop(state);
                    self.log_init(&ack);
                    self.publish_metrics();
                    return ack;
                }
                drop(state);
                let (terminal, matches) = {
                    let state = entry.state.lock().expect("query lifecycle entry lock");
                    (
                        state.termination_reason.is_some()
                            || matches!(
                                state.phase,
                                QueryLifecyclePhase::Terminating
                                    | QueryLifecyclePhase::TerminalRetained
                                    | QueryLifecyclePhase::Tombstone
                            ),
                        state
                            .credential_leases
                            .same_initial_values(&credential_leases),
                    )
                };
                if terminal {
                    let ack = self.wait_for_existing_init(entry, execution_id, digest);
                    self.log_init(&ack);
                    return ack;
                }
                if !matches {
                    self.request_termination(
                        entry,
                        QueryTerminationReason::QueryTerminationLocalFailure,
                    );
                    let ack = QueryInitAck::new(
                        execution_id,
                        digest,
                        QueryInitOutcome::QueryInitRejectedConflict,
                    );
                    self.log_init(&ack);
                    self.publish_metrics();
                    return ack;
                }
                let ack = self.wait_for_existing_init(entry, execution_id, digest);
                self.log_init(&ack);
                return ack;
            }
            if state.active_entries >= self.config.max_active_entries {
                let ack = QueryInitAck::new(
                    execution_id,
                    digest,
                    QueryInitOutcome::QueryInitRejectedCapacity,
                );
                drop(state);
                self.log_init(&ack);
                return ack;
            }
            let entry = Arc::new(QueryLifecycleEntry::initializing(
                manifest,
                digest,
                credential_leases,
            ));
            state.entries.insert(execution_id, Arc::clone(&entry));
            state.active_entries += 1;
            entry
        };
        self.publish_metrics();
        let ack = InitWorkspace {
            registry: self
                .self_weak
                .upgrade()
                .expect("query lifecycle registry is alive during method call"),
            entry,
            execution_id,
            digest,
        }
        .install_and_publish();
        self.log_init(&ack);
        self.publish_metrics();
        ack
    }

    fn begin_catalog_install(
        self: &Arc<Self>,
        entry: Arc<QueryLifecycleEntry>,
        execution_id: QueryExecutionId,
        catalogs: Vec<CatalogProperties>,
    ) {
        let registry = Arc::clone(self);
        let runtime = registry.catalog_install_runtime.clone();
        runtime.handle().spawn_blocking(move || {
                if crate::config::debug_emit_catalog_lifecycle_marker() {
                    println!(
                        "NOVAROCKS_CATALOG_LOADING execution_id={} process_id={} catalog_count={}",
                        format_execution_id(execution_id),
                        registry.local_process_id,
                        catalogs.len(),
                    );
                }
                if let Some(hold_file) = crate::config::debug_catalog_install_hold_file() {
                    while hold_file.exists() {
                        if !registry.catalog_install_active(&entry) {
                            registry.catalog_manager.release_query(execution_id);
                            registry.publish_catalog_lease_metrics();
                            return;
                        }
                        std::thread::sleep(Duration::from_millis(10));
                    }
                }
                let result = if crate::config::debug_catalog_install_failure_file()
                    .is_some_and(|failure_file| failure_file.exists())
                {
                    Err(crate::connector::catalog_manager::CatalogManagerError::materialization_failed(
                        "runner-injected catalog install failure",
                    ))
                } else {
                    catalogs.into_iter().try_for_each(|properties| {
                        let factories = Arc::clone(&registry.execution_role_binding_factories);
                        registry
                            .catalog_manager
                            .ensure_while(execution_id, properties, || registry.catalog_install_active(&entry), move |properties| {
                                factories.bind(properties).map_err(
                                    crate::connector::catalog_manager::CatalogManagerError::from_materialization,
                                )
                            })
                            .map(|_| ())
                    })
                };
                registry.publish_catalog_lease_metrics();
                let (event, release) = {
                    let mut state = entry.state.lock().expect("query lifecycle entry lock");
                    if state.termination_reason.is_some()
                        || matches!(
                            state.phase,
                            QueryLifecyclePhase::Terminating
                                | QueryLifecyclePhase::TerminalRetained
                                | QueryLifecyclePhase::Tombstone
                        )
                    {
                        (None, true)
                    } else {
                        match result {
                            Ok(()) => {
                                state.catalog_load = QueryCatalogLoadState::Ready;
                                if crate::config::debug_emit_catalog_lifecycle_marker() {
                                    println!(
                                        "NOVAROCKS_CATALOG_READY execution_id={} process_id={}",
                                        format_execution_id(execution_id),
                                        registry.local_process_id,
                                    );
                                }
                                (Some(catalog_ready_event()), false)
                            }
                            Err(error) => {
                                let safe_detail = safe_catalog_failure_detail(&error.to_string());
                                state.catalog_load = QueryCatalogLoadState::Failed {
                                    safe_detail: safe_detail.clone(),
                                };
                                if crate::config::debug_emit_catalog_lifecycle_marker() {
                                    println!(
                                        "NOVAROCKS_CATALOG_FAILED execution_id={} process_id={}",
                                        format_execution_id(execution_id),
                                        registry.local_process_id,
                                    );
                                }
                                (Some(catalog_load_failed_event(safe_detail)), false)
                            }
                        }
                    }
                };
                if release {
                    registry.catalog_manager.release_query(execution_id);
                    registry.publish_catalog_lease_metrics();
                    return;
                }
                if let Some(event) = event
                    && let Some(events) = entry
                        .state
                        .lock()
                        .expect("query lifecycle entry lock")
                        .events
                        .clone()
                {
                    let _ = events.try_send(event);
                }
        });
    }

    fn catalog_install_active(&self, entry: &QueryLifecycleEntry) -> bool {
        let state = entry.state.lock().expect("query lifecycle entry lock");
        state.termination_reason.is_none()
            && !matches!(
                state.phase,
                QueryLifecyclePhase::Terminating
                    | QueryLifecyclePhase::TerminalRetained
                    | QueryLifecyclePhase::Tombstone
            )
            && state
                .pre_start_deadline
                .is_none_or(|deadline| Instant::now() < deadline)
    }

    fn wait_for_existing_init(
        &self,
        entry: Arc<QueryLifecycleEntry>,
        execution_id: QueryExecutionId,
        digest: ParticipantManifestDigest,
    ) -> QueryInitAck {
        let mut state = entry.state.lock().expect("query lifecycle entry lock");
        while state.phase == QueryLifecyclePhase::Initializing && state.init_outcome.is_none() {
            state = entry
                .init_completed
                .wait(state)
                .expect("query lifecycle init wait");
        }
        let outcome = match (state.phase, state.init_outcome) {
            (_, Some(outcome)) if outcome != QueryInitOutcome::QueryInitApplied => outcome,
            (
                QueryLifecyclePhase::Initialized
                | QueryLifecyclePhase::ControlAttached
                | QueryLifecyclePhase::Staging
                | QueryLifecyclePhase::Staged
                | QueryLifecyclePhase::Running,
                _,
            ) => QueryInitOutcome::QueryInitAlreadyApplied,
            (
                QueryLifecyclePhase::TerminalRetained
                | QueryLifecyclePhase::Terminating
                | QueryLifecyclePhase::Tombstone,
                _,
            ) => QueryInitOutcome::QueryInitRejectedTerminated,
            (QueryLifecyclePhase::Initializing, _) => state
                .init_outcome
                .unwrap_or(QueryInitOutcome::QueryInitRejectedInvalidManifest),
        };
        QueryInitAck::new(execution_id, digest, outcome)
    }

    pub(crate) fn abort_query(
        &self,
        request: QueryAbortRequest,
    ) -> Result<QueryTerminationAck, QueryLifecycleError> {
        let participant = validated(request.participant());
        let execution_id = validated(request.execution_id());
        let digest = validated(request.digest());
        if validated(participant.backend_process_id()) != self.local_process_id {
            return Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::Conflict,
                "abort participant process does not match this backend",
            ));
        }
        let entry = {
            let mut state = self.state.lock().expect("query lifecycle registry lock");
            self.clean_tombstones_locked(&mut state, self.clock.now(), 64);
            if let Some(entry) = state.entries.get(&execution_id).cloned() {
                Some(entry)
            } else {
                if let Some(tombstone) = state.pre_init_tombstones.get(&execution_id)
                    && (tombstone.participant != participant || tombstone.digest != digest)
                {
                    return Err(QueryLifecycleError::new(
                        QueryLifecycleErrorCode::Conflict,
                        "abort conflicts with an existing pre-init tombstone",
                    ));
                }
                let reason = state
                    .pre_init_tombstones
                    .get(&execution_id)
                    .map(|tombstone| tombstone.reason)
                    .unwrap_or(QueryTerminationReason::QueryTerminationCoordinatorAbort);
                if let std::collections::btree_map::Entry::Vacant(e) =
                    state.pre_init_tombstones.entry(execution_id)
                {
                    e.insert(PreInitTombstone {
                        participant,
                        digest,
                        reason,
                        terminated_at: self.clock.now(),
                    });
                    state.tombstones.push_back(execution_id);
                    state.terminations = state.terminations.saturating_add(1);
                    state.termination_reasons[termination_reason_index(reason)] = state
                        .termination_reasons[termination_reason_index(reason)]
                    .saturating_add(1);
                    self.enforce_tombstone_capacity_locked(&mut state);
                }
                drop(state);
                let diagnostic = QueryExecutionDiagnostic::from(execution_id);
                info!(
                    target: "novarocks::query_lifecycle",
                    query_id = ?execution_id.query_id(),
                    query_process_namespace = %diagnostic.process_namespace(),
                    query_local_sequence = %diagnostic.local_sequence(),
                    query_attempt_id = diagnostic.attempt_id(),
                    attempt_id = execution_id.attempt_id().get(),
                    process_id = %self.local_process_id,
                    digest = %format_digest(digest),
                    outcome = "terminated",
                    reason = ?reason,
                    "backend query lifecycle terminated before init"
                );
                self.publish_metrics();
                return Ok(QueryTerminationAck::new(execution_id, reason));
            }
        };
        let entry = entry.expect("existing entry");
        if entry.participant != participant {
            return Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::Conflict,
                "abort participant conflicts with initialized entry",
            ));
        }
        if entry.digest != digest {
            return Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::Conflict,
                "abort digest conflicts with initialized manifest",
            ));
        }
        let reason = self.request_termination_with_detail(
            entry,
            QueryTerminationReason::QueryTerminationCoordinatorAbort,
            None,
            request.reason().to_string(),
        );
        Ok(QueryTerminationAck::new(execution_id, reason))
    }

    pub(crate) fn attach_control(
        &self,
        attach: QueryControlAttach,
    ) -> Result<QueryControlAttachment, QueryLifecycleError> {
        let execution_id = validated(attach.execution_id());
        let participant = validated(attach.participant());
        let entry = self
            .state
            .lock()
            .expect("query lifecycle registry lock")
            .entries
            .get(&execution_id)
            .cloned();
        let Some(entry) = entry else {
            return Err(self.attach_error(
                &attach,
                QueryLifecycleErrorCode::Terminated,
                "query lifecycle entry is not active",
                "missing",
            ));
        };
        if entry.participant != participant {
            return Err(self.attach_error(
                &attach,
                QueryLifecycleErrorCode::Conflict,
                "query control participant conflicts with initialized entry",
                "participant_mismatch",
            ));
        }
        let (events_tx, events_rx) = tokio::sync::mpsc::channel(
            CONTROL_EVENT_BUFFER_CAPACITY + RESERVED_CONTROL_EVENT_CAPACITY + 1,
        );
        let (runtime_filter_feedback_tx, runtime_filter_feedback_rx) =
            tokio::sync::mpsc::channel(RUNTIME_FILTER_FEEDBACK_BUFFER_CAPACITY);
        let (observations_tx, observations_rx) = tokio::sync::watch::channel(None);
        let local_drained_event_permit =
            events_tx.clone().try_reserve_owned().map_err(|error| {
                QueryLifecycleError::new(
                    QueryLifecycleErrorCode::Internal,
                    format!("reserve LocalDrained control event failed: {error}"),
                )
            })?;
        let terminal_snapshot_event_permit =
            events_tx.clone().try_reserve_owned().map_err(|error| {
                QueryLifecycleError::new(
                    QueryLifecycleErrorCode::Internal,
                    format!("reserve TerminalSnapshot control event failed: {error}"),
                )
            })?;
        if self.claim_terminal_fault(
            QueryLifecycleFaultKind::TerminalP0DeliveryPermitExhausted,
            execution_id,
        )? {
            return Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::Capacity,
                "injected terminal P0 delivery permit exhaustion before ControlReady",
            ));
        }
        let terminal_event_permit = events_tx.clone().try_reserve_owned().map_err(|error| {
            QueryLifecycleError::new(
                QueryLifecycleErrorCode::Internal,
                format!("reserve terminal control event failed: {error}"),
            )
        })?;
        let p0_bytes =
            p0_max_encoded_len(entry.manifest.as_proto()).map_err(protocol_contract_error)?;
        if self.claim_terminal_fault(
            QueryLifecycleFaultKind::TerminalP0RetainedSlotExhausted,
            execution_id,
        )? {
            return Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::Capacity,
                "injected terminal P0 retained-record exhaustion before ControlReady",
            ));
        }
        if self.claim_terminal_fault(
            QueryLifecycleFaultKind::TerminalP0BytesExhausted,
            execution_id,
        )? {
            return Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::Capacity,
                "injected terminal P0 retained-byte exhaustion before ControlReady",
            ));
        }
        self.reserve_terminal_p0(execution_id, p0_bytes)?;
        {
            let mut state = entry.state.lock().expect("query lifecycle entry lock");
            match state.phase {
                QueryLifecyclePhase::Initialized => {}
                QueryLifecyclePhase::TerminalRetained
                | QueryLifecyclePhase::Terminating
                | QueryLifecyclePhase::Tombstone => {
                    let phase = phase_name(state.phase);
                    drop(state);
                    self.release_terminal_record(execution_id);
                    return Err(self.attach_error(
                        &attach,
                        QueryLifecycleErrorCode::Terminated,
                        "query lifecycle entry has terminated",
                        phase,
                    ));
                }
                QueryLifecyclePhase::Initializing
                | QueryLifecyclePhase::ControlAttached
                | QueryLifecyclePhase::Staging
                | QueryLifecyclePhase::Staged
                | QueryLifecyclePhase::Running => {
                    let phase = phase_name(state.phase);
                    drop(state);
                    self.release_terminal_record(execution_id);
                    return Err(self.attach_error(
                        &attach,
                        QueryLifecycleErrorCode::Conflict,
                        "query control can attach only to an initialized entry",
                        phase,
                    ));
                }
            }
            state.phase = QueryLifecyclePhase::ControlAttached;
            state.last_heartbeat = Some(self.clock.now());
            state.events = Some(events_tx.clone());
            state.observations = Some(observations_tx);
            state.local_drained_event_permit = Some(local_drained_event_permit);
            state.terminal_snapshot_event_permit = Some(terminal_snapshot_event_permit);
            state.terminal_event_permit = Some(terminal_event_permit);
            if let Some(participant) = state.runtime_filter.as_ref() {
                let feedback_sink: Arc<dyn BackendFrontendFeedbackSink> =
                    Arc::new(RuntimeFilterFeedbackEgress {
                        participant: entry.participant.clone(),
                        participant_id: participant.local_participant_id(),
                        events: runtime_filter_feedback_tx.clone(),
                    });
                participant.set_frontend_feedback_sink(Arc::downgrade(&feedback_sink));
                state.frontend_feedback_sink = Some(feedback_sink);
            }
            if entry.expected_fragment_instance_ids.is_empty() {
                state.pre_start_deadline = None;
            }
        }
        let catalog_load = entry
            .state
            .lock()
            .expect("query lifecycle entry lock")
            .catalog_load
            .clone();
        if let Err(error) = events_tx.try_send(control_ready_event(&catalog_load)) {
            let mut state = entry.state.lock().expect("query lifecycle entry lock");
            state.phase = QueryLifecyclePhase::Initialized;
            state.last_heartbeat = None;
            state.events = None;
            state.observations = None;
            state.frontend_feedback_sink = None;
            state.local_drained_event_permit = None;
            state.terminal_snapshot_event_permit = None;
            state.terminal_event_permit = None;
            drop(state);
            self.release_terminal_record(execution_id);
            return Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::Internal,
                format!("publish ControlReady failed: {error}"),
            ));
        }
        let diagnostic = QueryExecutionDiagnostic::from(execution_id);
        info!(
            target: "novarocks::query_lifecycle",
            query_id = ?execution_id.query_id(),
            query_process_namespace = %diagnostic.process_namespace(),
            query_local_sequence = %diagnostic.local_sequence(),
            query_attempt_id = diagnostic.attempt_id(),
            attempt_id = execution_id.attempt_id().get(),
            process_id = %self.local_process_id,
            digest = %format_digest(entry.digest),
            outcome = "control_attached",
            reason = "none",
            "backend query lifecycle control attached"
        );
        if query_lifecycle_test_markers_enabled() {
            eprintln!(
                "NOVAROCKS_QUERY_CONTROL_READY execution_id={} process_id={} expected_fragments={} {}",
                format_execution_id(execution_id),
                self.local_process_id,
                entry.expected_fragment_instance_ids.len(),
                QueryExecutionDiagnostic::from(execution_id),
            );
        }
        self.publish_metrics();
        Ok(QueryControlAttachment {
            control: Arc::new(RegistryQueryControl {
                registry: self.self_weak.clone(),
                execution_id,
            }),
            events: events_rx,
            runtime_filter_feedback: runtime_filter_feedback_rx,
            observations: observations_rx,
        })
    }

    /// Publishes a best-effort, latest-only fragment observation. This path is
    /// intentionally unable to wait on transport I/O or mutate correctness
    /// state: a full/stalled stream may lose observations but must still carry
    /// heartbeat acknowledgements, drain barriers, and terminal facts.
    pub(crate) fn publish_fragment_observation(
        &self,
        execution_id: QueryExecutionId,
        fragment_instance_id: UniqueId,
        input_rows: u64,
        output_rows: u64,
        elapsed_ms: u64,
        profile: Option<RuntimeProfileTree>,
    ) -> bool {
        let entry = self
            .state
            .lock()
            .expect("query lifecycle registry lock")
            .entries
            .get(&execution_id)
            .cloned();
        let Some(entry) = entry else {
            return false;
        };
        let (sender, observation) = {
            let mut state = entry.state.lock().expect("query lifecycle entry lock");
            if !matches!(
                state.phase,
                QueryLifecyclePhase::ControlAttached
                    | QueryLifecyclePhase::Staging
                    | QueryLifecyclePhase::Staged
                    | QueryLifecyclePhase::Running
            ) || !entry
                .expected_fragment_instance_ids
                .contains(&fragment_instance_id)
            {
                return false;
            }
            let sequence = state
                .observation_sequences
                .get(&fragment_instance_id)
                .copied()
                .unwrap_or_default()
                .checked_add(1);
            let Some(sequence) = sequence else {
                return false;
            };
            let Some(sender) = state.observations.clone() else {
                return false;
            };
            let observation = FragmentLiveObservation::parse(
                novarocks_proto_models::novarocks::FragmentLiveObservation {
                    participant: Some(
                        observation_participant_ref(&entry.participant)
                            .as_proto()
                            .clone(),
                    ),
                    fragment_instance_id: Some(protocol_unique_id(fragment_instance_id)),
                    sequence,
                    input_rows,
                    output_rows,
                    elapsed_ms,
                    profile: profile.as_ref().map(encode_runtime_profile_tree),
                },
            )
            .expect("registry-owned fragment observation satisfies the Protocol contract");
            state
                .observation_sequences
                .insert(fragment_instance_id, sequence);
            (sender, observation)
        };
        sender.send_replace(Some(observation));
        true
    }

    /// The runner-owned NID-2 fault needs one real control-stream observation
    /// even when a public query has profiling disabled. It is unreachable in
    /// release builds and emits only after the exact participant has started.
    #[cfg(debug_assertions)]
    fn publish_runner_owned_foreign_observation_if_armed(
        &self,
        execution_id: QueryExecutionId,
        entry: &Arc<QueryLifecycleEntry>,
    ) {
        let Some(root) = novarocks_failpoint::configured_root() else {
            return;
        };
        let Some(backend_index) = std::env::var("NOVAROCKS_SQL_TEST_QUERY_LIFECYCLE_BACKEND_INDEX")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
        else {
            return;
        };
        let trigger = novarocks_failpoint::trigger_path(
            &root,
            backend_index,
            QueryLifecycleFaultKind::ObservationForeignParticipant,
        );
        if !trigger.is_file() {
            return;
        }
        let Some(fragment_instance_id) = entry.expected_fragment_instance_ids.first().copied()
        else {
            return;
        };
        let _ =
            self.publish_fragment_observation(execution_id, fragment_instance_id, 0, 0, 0, None);
    }

    // Design: ADR-0128 (docs/adr/ADR-0128-lifecycle-canonical-engine-private-typed-digests.md)
    pub(crate) fn stage_fragments(&self, request: QueryStageRequest) -> QueryStageAck {
        match self.begin_stage(request) {
            StageBuildDecision::Build(permit) => permit.commit(),
            StageBuildDecision::Complete(ack) => ack,
        }
    }

    /// Reserves the entry for one complete local Stage build. The caller owns
    /// materialization outside registry locks and must either commit or drop
    /// the returned permit.
    pub(crate) fn begin_stage(&self, request: QueryStageRequest) -> StageBuildDecision {
        let execution_id = request.execution_id();
        let participant = request.participant();
        // Derive the stage identity exactly once. Every acknowledgement echoes
        // it, including the capacity rejection below, so this must precede the
        // backend-local fragment budget check. Protocol already bounded the
        // fragment count and encoded size before this carrier was validated.
        let fragments = request.fragments();
        let stage_digest = StageDigest::compute(participant.clone(), &fragments)
            .expect("validated QueryStageRequest always derives a stage digest");
        let fragment_count = fragments.len();
        if fragment_count > self.config.stage_max_fragments {
            return StageBuildDecision::Complete(
                QueryStageAck::new(
                    execution_id,
                    stage_digest,
                    QueryStageOutcome::RejectedCapacity,
                    "stage fragment count exceeds the backend Stage limit",
                )
                .expect("validated Stage rejection has a valid Protocol acknowledgement"),
            );
        }
        let stage_encoded_bytes = request.as_proto().encoded_len();
        if stage_encoded_bytes > self.config.stage_max_encoded_bytes {
            return StageBuildDecision::Complete(
                QueryStageAck::new(
                    execution_id,
                    stage_digest,
                    QueryStageOutcome::RejectedCapacity,
                    "stage request encoded bytes exceed the backend Stage limit",
                )
                .expect("validated Stage rejection has a valid Protocol acknowledgement"),
            );
        }
        let entry = self
            .state
            .lock()
            .expect("query lifecycle registry lock")
            .entries
            .get(&execution_id)
            .cloned();
        let Some(entry) = entry else {
            return StageBuildDecision::Complete(
                QueryStageAck::new(
                    execution_id,
                    stage_digest,
                    QueryStageOutcome::RejectedTerminated,
                    "query lifecycle entry is not active",
                )
                .expect("validated Stage rejection has a valid Protocol acknowledgement"),
            );
        };
        if entry.participant != participant {
            return StageBuildDecision::Complete(
                QueryStageAck::new(
                    execution_id,
                    stage_digest,
                    QueryStageOutcome::RejectedConflict,
                    "stage participant conflicts with initialized entry",
                )
                .expect("validated Stage rejection has a valid Protocol acknowledgement"),
            );
        }

        let requested_instances = request
            .fragments()
            .iter()
            .map(|fragment| fragment.fragment_instance_id())
            .collect::<std::collections::BTreeSet<_>>();
        let expected_instances = entry
            .expected_fragment_instance_ids
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        let mut state = entry.state.lock().expect("query lifecycle entry lock");
        let (outcome, detail, build) = match state.phase {
            QueryLifecyclePhase::ControlAttached => {
                if !matches!(state.catalog_load, QueryCatalogLoadState::Ready) {
                    if crate::config::debug_emit_catalog_lifecycle_marker() {
                        println!(
                            "NOVAROCKS_CATALOG_STAGE_BLOCKED execution_id={} process_id={} reason=catalog_not_ready",
                            format_execution_id(execution_id),
                            self.local_process_id,
                        );
                    }
                    (
                        QueryStageOutcome::RejectedInvalidState,
                        "catalog materialization is not ready for staging",
                        None,
                    )
                } else if requested_instances != expected_instances {
                    (
                        QueryStageOutcome::RejectedInvalidBatch,
                        "stage fragment set differs from participant manifest",
                        None,
                    )
                } else {
                    state.phase = QueryLifecyclePhase::Staging;
                    state.stage_digest = Some(stage_digest);
                    let gate = Arc::new(super::stage::StartGate::new());
                    state.start_gate = Some(Arc::clone(&gate));
                    (
                        QueryStageOutcome::Applied,
                        "query participant staging",
                        Some(gate),
                    )
                }
            }
            QueryLifecyclePhase::Staging if state.stage_digest == Some(stage_digest) => {
                while state.phase == QueryLifecyclePhase::Staging
                    && state.termination_reason.is_none()
                {
                    state = entry
                        .stage_completed
                        .wait(state)
                        .expect("query lifecycle entry lock");
                }
                match state.phase {
                    QueryLifecyclePhase::Staged | QueryLifecyclePhase::Running
                        if state.stage_digest == Some(stage_digest) =>
                    {
                        (
                            QueryStageOutcome::AlreadyApplied,
                            "query participant was already staged",
                            None,
                        )
                    }
                    QueryLifecyclePhase::TerminalRetained
                    | QueryLifecyclePhase::Terminating
                    | QueryLifecyclePhase::Tombstone => (
                        QueryStageOutcome::RejectedTerminated,
                        "query lifecycle entry has terminated",
                        None,
                    ),
                    _ => (
                        QueryStageOutcome::RejectedInvalidState,
                        "query participant stage did not complete",
                        None,
                    ),
                }
            }
            QueryLifecyclePhase::Staging => (
                QueryStageOutcome::RejectedConflict,
                "stage digest conflicts with in-flight participant staging",
                None,
            ),
            QueryLifecyclePhase::Staged | QueryLifecyclePhase::Running => {
                if state.stage_digest == Some(stage_digest) {
                    (
                        QueryStageOutcome::AlreadyApplied,
                        "query participant was already staged",
                        None,
                    )
                } else {
                    (
                        QueryStageOutcome::RejectedConflict,
                        "stage digest conflicts with existing staged participant",
                        None,
                    )
                }
            }
            QueryLifecyclePhase::TerminalRetained
            | QueryLifecyclePhase::Terminating
            | QueryLifecyclePhase::Tombstone => (
                QueryStageOutcome::RejectedTerminated,
                "query lifecycle entry has terminated",
                None,
            ),
            QueryLifecyclePhase::Initializing | QueryLifecyclePhase::Initialized => (
                QueryStageOutcome::RejectedInvalidState,
                "query control must attach before staging",
                None,
            ),
        };
        drop(state);
        match build {
            Some(gate) => {
                let resources =
                    match StageResourceReservation::try_acquire(
                        Arc::clone(&self.stage_resources),
                        self.config,
                        stage_encoded_bytes,
                        fragment_count,
                    ) {
                        Ok(resources) => resources,
                        Err(detail) => {
                            let mut state = entry.state.lock().expect("query lifecycle entry lock");
                            if state.phase == QueryLifecyclePhase::Staging
                                && state.stage_digest == Some(stage_digest)
                            {
                                state.phase = QueryLifecyclePhase::ControlAttached;
                                state.stage_digest = None;
                                state.start_gate = None;
                                entry.stage_completed.notify_all();
                            }
                            return StageBuildDecision::Complete(QueryStageAck::new(
                            execution_id,
                            stage_digest,
                            QueryStageOutcome::RejectedCapacity,
                            detail,
                        )
                        .expect("validated Stage rejection has a valid Protocol acknowledgement"));
                        }
                    };
                StageBuildDecision::Build(StageBuildPermit {
                    registry: self
                        .self_weak
                        .upgrade()
                        .expect("query lifecycle registry owns active entry"),
                    entry,
                    execution_id,
                    digest: stage_digest,
                    gate,
                    resources: Some(resources),
                    committed: false,
                })
            }
            None => StageBuildDecision::Complete(
                QueryStageAck::new(execution_id, stage_digest, outcome, detail)
                    .expect("validated Stage acknowledgement has a valid Protocol projection"),
            ),
        }
    }

    /// Commits the single query-owned start decision.  Releasing the gate
    /// while holding the entry lock makes `Staged -> Running` and visibility to
    /// staged workers one atomic lifecycle event.
    pub(crate) fn start_prepared_query(&self, request: QueryStartRequest) -> QueryStartAck {
        let execution_id = request.execution_id();
        let stage_digest = request.digest();
        let entry = self
            .state
            .lock()
            .expect("query lifecycle registry lock")
            .entries
            .get(&execution_id)
            .cloned();
        let Some(entry) = entry else {
            return QueryStartAck::new(
                execution_id,
                stage_digest,
                QueryStartOutcome::RejectedTerminated,
                "query lifecycle entry is not active",
            )
            .expect("validated Start rejection has a valid Protocol acknowledgement");
        };

        let mut state = entry.state.lock().expect("query lifecycle entry lock");
        let mut released_stage_resources = None;
        let mut local_drained_ready = None;
        let (outcome, detail) = match state.phase {
            QueryLifecyclePhase::Staged => {
                if state.stage_digest != Some(stage_digest) {
                    (
                        QueryStartOutcome::RejectedConflict,
                        "start digest conflicts with staged participant",
                    )
                } else if let Some(gate) = state.start_gate.clone() {
                    state.phase = QueryLifecyclePhase::Running;
                    state.pre_start_deadline = None;
                    if entry.expected_fragment_instance_ids.is_empty()
                        && !state.local_drained_emitted
                    {
                        state.local_drained_emitted = true;
                        local_drained_ready = Some((
                            state.local_drained_event_permit.take(),
                            state.events.clone(),
                        ));
                    }
                    let released = gate.release();
                    debug_assert!(released, "a staged start gate must be pending");
                    released_stage_resources = state.stage_resources.take();
                    (QueryStartOutcome::Applied, "query participant started")
                } else {
                    (
                        QueryStartOutcome::RejectedNotStaged,
                        "staged participant has no start gate",
                    )
                }
            }
            QueryLifecyclePhase::Running => {
                if state.stage_digest == Some(stage_digest) {
                    (
                        QueryStartOutcome::AlreadyStarted,
                        "query participant was already started",
                    )
                } else {
                    (
                        QueryStartOutcome::RejectedConflict,
                        "start digest conflicts with running participant",
                    )
                }
            }
            QueryLifecyclePhase::TerminalRetained
            | QueryLifecyclePhase::Terminating
            | QueryLifecyclePhase::Tombstone => (
                QueryStartOutcome::RejectedTerminated,
                "query lifecycle entry has terminated",
            ),
            QueryLifecyclePhase::Initializing
            | QueryLifecyclePhase::Initialized
            | QueryLifecyclePhase::ControlAttached
            | QueryLifecyclePhase::Staging => (
                QueryStartOutcome::RejectedNotStaged,
                "query participant has not finished staging",
            ),
        };
        drop(state);
        if let Some((permit, events)) = local_drained_ready {
            send_reserved_control_event(permit, events, local_drained_event());
        }
        #[cfg(debug_assertions)]
        if outcome == QueryStartOutcome::Applied {
            self.publish_runner_owned_foreign_observation_if_armed(execution_id, &entry);
        }
        // The gate has been released under the entry lock.  Once Running is
        // visible there can be no dormant workers or retained stage payload,
        // so return the Stage reservation outside lifecycle locks.
        drop(released_stage_resources);
        QueryStartAck::new(execution_id, stage_digest, outcome, detail)
            .expect("validated Start acknowledgement has a valid Protocol projection")
    }

    fn attach_error(
        &self,
        attach: &QueryControlAttach,
        code: QueryLifecycleErrorCode,
        detail: &'static str,
        phase: &'static str,
    ) -> QueryLifecycleError {
        let execution_id = validated(attach.execution_id());
        let participant = validated(attach.participant());
        let diagnostic = QueryExecutionDiagnostic::from(execution_id);
        warn!(
            target: "novarocks::query_lifecycle",
            query_id = ?execution_id.query_id(),
            query_process_namespace = %diagnostic.process_namespace(),
            query_local_sequence = %diagnostic.local_sequence(),
            query_attempt_id = diagnostic.attempt_id(),
            attempt_id = execution_id.attempt_id().get(),
            process_id = %self.local_process_id,
            participant_process_id = %validated(participant.backend_process_id()),
            outcome = "attach_rejected",
            reason = detail,
            phase,
            "backend query lifecycle control attach rejected"
        );
        QueryLifecycleError::new(code, detail)
    }

    pub(crate) fn admit_fragment(
        &self,
        execution_id: QueryExecutionId,
        fragment_instance_id: UniqueId,
    ) -> Result<FragmentAdmissionPermit, QueryLifecycleError> {
        let entry = self
            .state
            .lock()
            .expect("query lifecycle registry lock")
            .entries
            .get(&execution_id)
            .cloned();
        let Some(entry) = entry else {
            return Err(self.admission_error(
                execution_id,
                QueryLifecycleErrorCode::Terminated,
                "query is not active",
            ));
        };
        let mut state = entry.state.lock().expect("query lifecycle entry lock");
        if state.termination_reason.is_some()
            || matches!(
                state.phase,
                QueryLifecyclePhase::Terminating | QueryLifecyclePhase::Tombstone
            )
        {
            drop(state);
            return Err(self.admission_error(
                execution_id,
                QueryLifecycleErrorCode::Terminated,
                "query lifecycle has terminated",
            ));
        }
        if !matches!(
            state.phase,
            QueryLifecyclePhase::ControlAttached | QueryLifecyclePhase::Staging
        ) {
            drop(state);
            return Err(self.admission_error(
                execution_id,
                QueryLifecycleErrorCode::Conflict,
                "query control is not ready",
            ));
        }
        if entry.expected_fragment_instance_ids.is_empty() {
            drop(state);
            return Err(self.admission_error(
                execution_id,
                QueryLifecycleErrorCode::InvalidManifest,
                "service-only participant cannot admit fragments",
            ));
        }
        if !entry
            .expected_fragment_instance_ids
            .contains(&fragment_instance_id)
        {
            drop(state);
            return Err(self.admission_error(
                execution_id,
                QueryLifecycleErrorCode::InvalidManifest,
                "fragment instance is outside the participant manifest",
            ));
        }
        if state.accepted_fragments.contains(&fragment_instance_id)
            || !state.in_flight_fragments.insert(fragment_instance_id)
        {
            drop(state);
            return Err(self.admission_error(
                execution_id,
                QueryLifecycleErrorCode::Conflict,
                "fragment instance was already admitted",
            ));
        }
        drop(state);
        Ok(FragmentAdmissionPermit {
            registry: self.self_weak.clone(),
            execution_id,
            fragment_instance_id,
            entry,
            committed: false,
        })
    }

    /// Admits one runtime split assignment against an already staged task.
    ///
    /// Unlike `admit_fragment` this neither creates nor mutates lifecycle
    /// state: it only decides whether this exact attempt may still receive
    /// work. The delivery window opens once the participant is staged, which
    /// is strictly after the Init + ControlReady barrier froze the admitted
    /// participant set, so an assignment can never introduce a participant.
    pub(crate) fn admit_task_update(
        &self,
        execution_id: QueryExecutionId,
        fragment_instance_id: UniqueId,
    ) -> Result<(), QueryLifecycleError> {
        let entry = self
            .state
            .lock()
            .expect("query lifecycle registry lock")
            .entries
            .get(&execution_id)
            .cloned();
        let Some(entry) = entry else {
            return Err(self.admission_error(
                execution_id,
                QueryLifecycleErrorCode::Terminated,
                "query is not active",
            ));
        };
        if !entry
            .expected_fragment_instance_ids
            .contains(&fragment_instance_id)
        {
            return Err(self.admission_error(
                execution_id,
                QueryLifecycleErrorCode::InvalidManifest,
                "fragment instance is outside the participant manifest",
            ));
        }
        let state = entry.state.lock().expect("query lifecycle entry lock");
        if state.termination_reason.is_some()
            || matches!(
                state.phase,
                QueryLifecyclePhase::TerminalRetained
                    | QueryLifecyclePhase::Terminating
                    | QueryLifecyclePhase::Tombstone
            )
        {
            drop(state);
            return Err(self.admission_error(
                execution_id,
                QueryLifecycleErrorCode::Terminated,
                "query lifecycle has terminated",
            ));
        }
        if !matches!(
            state.phase,
            QueryLifecyclePhase::Staged | QueryLifecyclePhase::Running
        ) {
            drop(state);
            return Err(self.admission_error(
                execution_id,
                QueryLifecycleErrorCode::Conflict,
                "task has not been staged",
            ));
        }
        if !state.accepted_fragments.contains(&fragment_instance_id) {
            drop(state);
            return Err(self.admission_error(
                execution_id,
                QueryLifecycleErrorCode::Conflict,
                "fragment instance is not an admitted task",
            ));
        }
        drop(state);
        Ok(())
    }

    /// Returns a fragment-bound execution capability from the already
    /// initialized exact attempt. This lookup never creates, revives, or
    /// extends lifecycle retention.
    pub(crate) fn runtime_filter_session_for_fragment(
        &self,
        execution_id: QueryExecutionId,
        fragment_instance_id: UniqueId,
        required: bool,
    ) -> Result<Option<RuntimeFilterSessionRef>, QueryLifecycleError> {
        let entry = self
            .state
            .lock()
            .expect("query lifecycle registry lock")
            .entries
            .get(&execution_id)
            .cloned()
            .ok_or_else(|| {
                QueryLifecycleError::new(
                    QueryLifecycleErrorCode::Terminated,
                    "runtime filter execution attempt is not active",
                )
            })?;
        let participant = {
            let state = entry.state.lock().expect("query lifecycle entry lock");
            if !state.in_flight_fragments.contains(&fragment_instance_id) {
                return Err(QueryLifecycleError::new(
                    QueryLifecycleErrorCode::Conflict,
                    "runtime filter session requires a held fragment admission permit",
                ));
            }
            state.runtime_filter.clone()
        };
        match participant {
            Some(participant) => {
                participant.session_for_fragment(execution_id, fragment_instance_id, required)
            }
            None if required => Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::InvalidManifest,
                "fragment requires a runtime filter session but this participant has no runtime filter contribution",
            )),
            None => Ok(None),
        }
    }

    pub(crate) fn record_runtime_filter_row_effect(
        &self,
        execution_id: QueryExecutionId,
        fragment_instance_id: UniqueId,
        effect: novarocks_execution::runtime_filter::RuntimeFilterRowEffect,
    ) {
        if let Some(participant) =
            self.runtime_filter_participant_for_event(execution_id, fragment_instance_id)
        {
            participant.record_row_effect(fragment_instance_id, effect);
        }
    }

    pub(crate) fn record_runtime_filter_scan_unit_outcome(
        &self,
        execution_id: QueryExecutionId,
        fragment_instance_id: UniqueId,
        outcome: novarocks_execution::runtime_filter::scan_domain::RuntimeFilterScanUnitOutcome,
    ) {
        if let Some(participant) =
            self.runtime_filter_participant_for_event(execution_id, fragment_instance_id)
        {
            participant.record_scan_unit_outcome(fragment_instance_id, outcome);
        }
    }

    fn runtime_filter_participant_for_event(
        &self,
        execution_id: QueryExecutionId,
        fragment_instance_id: UniqueId,
    ) -> Option<Arc<RuntimeFilterParticipant>> {
        let entry = self
            .state
            .lock()
            .expect("query lifecycle registry lock")
            .entries
            .get(&execution_id)
            .cloned()?;
        let state = entry.state.lock().expect("query lifecycle entry lock");
        if !entry
            .expected_fragment_instance_ids
            .contains(&fragment_instance_id)
        {
            return None;
        }
        state.runtime_filter.clone()
    }

    /// Dispatches an already decoded envelope through an existing exact
    /// attempt. A miss is deliberately lookup-only and cannot release a gate.
    pub(crate) fn dispatch_runtime_filter_envelope(
        &self,
        envelope: BackendNativeRuntimeFilterEnvelope,
    ) -> crate::runtime_filter::domain::BackendIngressResult {
        let participant = self
            .state
            .lock()
            .expect("query lifecycle registry lock")
            .entries
            .iter()
            .find(|(execution_id, _)| {
                execution_id.query_id().high() == envelope.participant().query_id().high()
                    && execution_id.query_id().low() == envelope.participant().query_id().low()
                    && execution_id.attempt_id().get() == envelope.participant().deployment_epoch()
            })
            .map(|(_, entry)| entry)
            .and_then(|entry| {
                entry
                    .state
                    .lock()
                    .expect("query lifecycle entry lock")
                    .runtime_filter
                    .clone()
            });
        match participant {
            Some(participant) => participant.dispatch_envelope(envelope),
            None => crate::runtime_filter::domain::BackendIngressResult::rejected(
                "runtime filter ingress rejected [query-unavailable]: runtime filter query is not active or in delivery grace",
            ).expect("query-unavailable reason is non-empty"),
        }
    }

    fn admission_error(
        &self,
        execution_id: QueryExecutionId,
        code: QueryLifecycleErrorCode,
        detail: &'static str,
    ) -> QueryLifecycleError {
        let digest = {
            let mut state = self.state.lock().expect("query lifecycle registry lock");
            state.admission_rejected = state.admission_rejected.saturating_add(1);
            state
                .entries
                .get(&execution_id)
                .map(|entry| format_digest(entry.digest))
                .unwrap_or_else(|| "unknown".to_string())
        };
        let diagnostic = QueryExecutionDiagnostic::from(execution_id);
        warn!(
            target: "novarocks::query_lifecycle",
            query_id = ?execution_id.query_id(),
            query_process_namespace = %diagnostic.process_namespace(),
            query_local_sequence = %diagnostic.local_sequence(),
            query_attempt_id = diagnostic.attempt_id(),
            attempt_id = execution_id.attempt_id().get(),
            process_id = %self.local_process_id,
            digest = %digest,
            outcome = "admission_rejected",
            reason = detail,
            "backend query lifecycle fragment admission rejected"
        );
        self.publish_metrics();
        QueryLifecycleError::new(code, detail)
    }

    pub(crate) fn sweep_expired(&self, now: Instant) {
        let entries = {
            let mut state = self.state.lock().expect("query lifecycle registry lock");
            self.clean_tombstones_locked(&mut state, now, 64);
            state.entries.values().cloned().collect::<Vec<_>>()
        };
        for entry in entries {
            let (termination_retry, expiration, terminal_retention_expired) = {
                let state = entry.state.lock().expect("query lifecycle entry lock");
                if state.phase == QueryLifecyclePhase::Terminating {
                    (
                        if state.failure_drain_scheduled || state.terminal_freeze_in_flight {
                            None
                        } else {
                            state.init_outcome.and(state.termination_reason)
                        },
                        None,
                        false,
                    )
                } else if state.phase == QueryLifecyclePhase::TerminalRetained {
                    (
                        None,
                        None,
                        state.terminated_at.is_some_and(|at| {
                            now.saturating_duration_since(at) >= self.config.terminal_retention
                        }),
                    )
                } else if state.phase == QueryLifecyclePhase::Tombstone {
                    (None, None, false)
                } else if state
                    .pre_start_deadline
                    .is_some_and(|deadline| now >= deadline)
                {
                    (
                        None,
                        Some(QueryTerminationReason::QueryTerminationPreStartTimeout),
                        false,
                    )
                } else if matches!(
                    state.phase,
                    QueryLifecyclePhase::ControlAttached
                        | QueryLifecyclePhase::Staging
                        | QueryLifecyclePhase::Staged
                        | QueryLifecyclePhase::Running
                ) && state.last_heartbeat.is_some_and(|heartbeat| {
                    now.saturating_duration_since(heartbeat) >= self.config.heartbeat_timeout
                }) {
                    (
                        None,
                        Some(QueryTerminationReason::QueryTerminationCoordinatorHeartbeatTimeout),
                        false,
                    )
                } else {
                    (None, None, false)
                }
            };
            if let Some(reason) = termination_retry {
                let execution_id = validated(entry.manifest.execution_id());
                if self.try_complete_runtime_filter_cleanup(&entry, execution_id) {
                    self.publish_tombstone(&entry, execution_id, reason);
                }
                continue;
            }
            if let Some(reason) = expiration {
                self.request_termination(entry, reason);
                continue;
            }
            if terminal_retention_expired {
                let reason = {
                    let mut state = entry.state.lock().expect("query lifecycle entry lock");
                    let reason = state
                        .termination_reason
                        .unwrap_or(QueryTerminationReason::QueryTerminationCoordinatorFinalize);
                    state.terminal_record = None;
                    state.terminal_outcome = None;
                    reason
                };
                entry.terminal_delivery_completed.notify_all();
                self.release_terminal_record(validated(entry.manifest.execution_id()));
                self.increment_terminal_metric(|metrics| {
                    metrics.terminal_retention_expired =
                        metrics.terminal_retention_expired.saturating_add(1);
                });
                self.publish_tombstone(&entry, validated(entry.manifest.execution_id()), reason);
            }
        }
    }

    fn request_termination(
        &self,
        entry: Arc<QueryLifecycleEntry>,
        requested_reason: QueryTerminationReason,
    ) -> QueryTerminationReason {
        self.request_termination_with_detail(
            entry,
            requested_reason,
            None,
            termination_detail(requested_reason),
        )
    }

    fn request_termination_with_event(
        &self,
        entry: Arc<QueryLifecycleEntry>,
        requested_reason: QueryTerminationReason,
        terminal_event: Option<QueryControlEvent>,
    ) -> QueryTerminationReason {
        let detail = match terminal_event.as_ref() {
            Some(event) => match event.as_proto().event.as_ref() {
                Some(
                    novarocks_proto_models::novarocks::query_control_response::Event::LocalFailure(
                        failure,
                    ),
                ) => failure.detail.clone(),
                _ => termination_detail(requested_reason),
            },
            None => termination_detail(requested_reason),
        };
        self.request_termination_with_detail(entry, requested_reason, terminal_event, detail)
    }

    fn request_termination_with_detail(
        &self,
        entry: Arc<QueryLifecycleEntry>,
        requested_reason: QueryTerminationReason,
        terminal_event: Option<QueryControlEvent>,
        detail: String,
    ) -> QueryTerminationReason {
        let already_terminated = {
            let state = entry.state.lock().expect("query lifecycle entry lock");
            state
                .termination_reason
                .map(|reason| (reason, state.events.clone()))
        };
        if let Some((reason, events)) = already_terminated {
            // A LocalFailure consumes the reserved terminal event permit to
            // publish its cause.  A later coordinator Abort still needs an
            // Abort acknowledgement so FE cleanup can keep the stream alive
            // for the drained immutable snapshot. The entry's immutable
            // LocalFailure remains the termination fact; the control reply
            // acknowledges the command that FE is waiting on.
            if terminal_event.is_none()
                && let Some(events) = events
            {
                let acknowledgement = match (reason, requested_reason) {
                    (
                        QueryTerminationReason::QueryTerminationLocalFailure,
                        QueryTerminationReason::QueryTerminationCoordinatorAbort,
                    ) => QueryTerminationReason::QueryTerminationCoordinatorAbort,
                    _ => reason,
                };
                let _ = events.try_send(termination_accepted_event(acknowledgement));
            }
            return reason;
        }
        let (
            execution_id,
            expected_instances,
            initializing,
            schedule_failure_drain,
            terminal_event_permit,
            start_gate,
            stage_resources,
        ) = {
            let mut state = entry.state.lock().expect("query lifecycle entry lock");
            // The early check above handles the normal idempotent case. A
            // racing caller can only observe the same first-wins reason.
            if let Some(reason) = state.termination_reason {
                return reason;
            }
            state.termination_reason = Some(requested_reason);
            let initializing = state.phase == QueryLifecyclePhase::Initializing;
            let running = state.phase == QueryLifecyclePhase::Running;
            let has_admitted_fragments = !state.accepted_fragments.is_empty();
            // A termination after Start must retain a complete immutable
            // terminal record, even when the coordinator initiated the
            // abort. Pre-start failures remain QLC-3 cleanup only.
            let schedule_failure_drain = (running || has_admitted_fragments)
                && requested_reason != QueryTerminationReason::QueryTerminationCoordinatorFinalize
                && !state.failure_drain_scheduled;
            if schedule_failure_drain {
                state.failure_drain_scheduled = true;
            }
            state.phase = QueryLifecyclePhase::Terminating;
            entry.stage_completed.notify_all();
            (
                validated(entry.manifest.execution_id()),
                entry.expected_fragment_instance_ids.clone(),
                initializing,
                schedule_failure_drain,
                state.terminal_event_permit.take(),
                state.start_gate.clone(),
                state.stage_resources.take(),
            )
        };

        if let Some(gate) = start_gate {
            // A gate released before termination stays released; otherwise
            // wake every dormant worker without allowing it to start.
            gate.abort();
        }
        // Abort is terminal for a pre-start bundle.  Free the associated
        // ledger reservation only after its gate has been fail-closed.
        drop(stage_resources);
        if let Some(permit) = terminal_event_permit {
            drop(permit.send(
                terminal_event.unwrap_or_else(|| termination_accepted_event(requested_reason)),
            ));
        }
        self.publish_metrics();
        self.local_runtime.quiesce_query(
            execution_id,
            &expected_instances,
            requested_reason,
            &detail,
        );
        if query_lifecycle_test_markers_enabled() {
            eprintln!(
                "NOVAROCKS_QUERY_LIFECYCLE_TERMINATED execution_id={} process_id={} reason={} expected_fragments={} {}",
                format_execution_id(execution_id),
                self.local_process_id,
                termination_reason_name(requested_reason),
                expected_instances.len(),
                QueryExecutionDiagnostic::from(execution_id),
            );
        }
        if requested_reason == QueryTerminationReason::QueryTerminationCoordinatorHeartbeatTimeout {
            let mut state = self.state.lock().expect("query lifecycle registry lock");
            state.heartbeat_timeouts = state.heartbeat_timeouts.saturating_add(1);
        }
        if !schedule_failure_drain {
            self.release_query_resources(execution_id);
        }
        let cleanup_complete = !schedule_failure_drain
            && self.try_complete_runtime_filter_cleanup(&entry, execution_id);
        let failure_drain_pending = entry
            .state
            .lock()
            .expect("query lifecycle entry lock")
            .failure_drain_scheduled;
        if !initializing && cleanup_complete && !failure_drain_pending {
            self.publish_tombstone(&entry, execution_id, requested_reason);
        }
        if schedule_failure_drain {
            // A coordinator Abort is an authoritative instruction to stop the
            // admitted participant.  It must not share the frontend's
            // terminal-outcome deadline while a non-cooperative fragment is
            // still unwinding: retain the explicit IncompleteDrain proof now.
            // Local failures keep their bounded drain window so their own
            // terminal facts can still be captured first.
            let drain_timeout =
                if requested_reason == QueryTerminationReason::QueryTerminationCoordinatorAbort {
                    Duration::ZERO
                } else {
                    self.config.terminal_drain_timeout
                };
            self.schedule_failed_terminal_drain(entry, drain_timeout);
        }
        requested_reason
    }

    #[allow(
        dead_code,
        reason = "Retained for lifecycle unit targets that record legacy execution outcomes instead of terminal facts."
    )]
    pub(crate) fn record_fragment_terminal(
        &self,
        execution_id: QueryExecutionId,
        fragment_instance_id: UniqueId,
        outcome: &FragmentOutcome,
    ) {
        let snapshot = match fragment_snapshot_from_outcome(fragment_instance_id, 0, outcome) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                warn!(target: "novarocks::query_lifecycle", error = %error, "rejecting terminal fragment fact");
                return;
            }
        };
        self.record_fragment_terminal_snapshot(execution_id, snapshot);
    }

    pub(crate) fn record_fragment_terminal_fact(
        &self,
        execution_id: QueryExecutionId,
        fact: FragmentTerminalFact,
        backend_num: i32,
        sink: SinkCommitReportSnapshot,
    ) {
        let (outcome, code, detail) = match fact.outcome() {
            FragmentOutcome::Succeeded => (
                novarocks_proto_models::novarocks::QueryTerminalFragmentOutcome::Succeeded,
                String::new(),
                String::new(),
            ),
            FragmentOutcome::Failed(error) => (
                novarocks_proto_models::novarocks::QueryTerminalFragmentOutcome::Failed,
                "FRAGMENT_EXECUTION_FAILED".to_owned(),
                error.to_string(),
            ),
            FragmentOutcome::Cancelled { reason } => (
                novarocks_proto_models::novarocks::QueryTerminalFragmentOutcome::Cancelled,
                "CANCELLED".to_owned(),
                reason.detail().to_owned(),
            ),
        };
        let snapshot = match terminal_fragment_snapshot(
            fact.fragment_instance_id(),
            backend_num,
            outcome,
            code,
            detail,
            sink,
            fact.profile().cloned(),
            fact.statistics_payload().to_vec(),
        ) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                warn!(target: "novarocks::query_lifecycle", error = %error, "rejecting terminal fragment fact");
                return;
            }
        };
        self.record_fragment_terminal_snapshot(execution_id, snapshot);
    }

    fn record_fragment_terminal_snapshot(
        &self,
        execution_id: QueryExecutionId,
        snapshot: FragmentTerminalSnapshot,
    ) {
        let fragment_instance_id = generated_id(snapshot.fragment_instance_id());
        let outcome = fragment_outcome(&snapshot);
        let terminal_error = {
            let raw = snapshot.as_proto();
            (raw.error_code.clone(), raw.error_detail.clone())
        };
        let committed_execution_id = {
            let mut state = self.state.lock().expect("query lifecycle registry lock");
            match state
                .fragment_executions
                .get(&fragment_instance_id)
                .copied()
            {
                Some(committed_execution_id) if committed_execution_id == execution_id => {
                    state.fragment_executions.remove(&fragment_instance_id);
                    Some(committed_execution_id)
                }
                Some(committed_execution_id) => {
                    warn!(
                        target: "novarocks::query_lifecycle",
                        finst_id = %fragment_instance_id,
                        terminal_execution_id = %format_execution_id(execution_id),
                        committed_execution_id = %format_execution_id(committed_execution_id),
                        "ignoring stale fragment terminal fact for a reused fragment instance"
                    );
                    None
                }
                None => {
                    warn!(
                        target: "novarocks::query_lifecycle",
                        finst_id = %fragment_instance_id,
                        terminal_execution_id = %format_execution_id(execution_id),
                        "fragment terminal fact has no committed query lifecycle admission"
                    );
                    None
                }
            }
        };
        let Some(execution_id) = committed_execution_id else {
            return;
        };
        let entry = self
            .state
            .lock()
            .expect("query lifecycle registry lock")
            .entries
            .get(&execution_id)
            .cloned();
        let Some(entry) = entry else {
            return;
        };
        let local_drained = {
            let mut state = entry.state.lock().expect("query lifecycle entry lock");
            state.completed_fragments.insert(fragment_instance_id);
            if state
                .terminal_facts
                .insert(fragment_instance_id, snapshot)
                .is_some()
            {
                return;
            }
            let expected = &entry.expected_fragment_instance_ids;
            let complete = expected
                .iter()
                .all(|id| state.completed_fragments.contains(id));

            if complete
                && outcome
                    == novarocks_proto_models::novarocks::QueryTerminalFragmentOutcome::Succeeded
                && !state.local_drained_emitted
            {
                state.local_drained_emitted = true;
                Some((
                    state.local_drained_event_permit.take(),
                    state.events.clone(),
                ))
            } else {
                None
            }
        };
        if let Some((permit, events)) = local_drained {
            self.increment_terminal_metric(|metrics| {
                metrics.terminal_locally_drained =
                    metrics.terminal_locally_drained.saturating_add(1);
            });
            send_reserved_control_event(permit, events, local_drained_event());
        }
        self.increment_terminal_metric(|metrics| {
            metrics.terminal_facts = metrics.terminal_facts.saturating_add(1);
        });
        if outcome == novarocks_proto_models::novarocks::QueryTerminalFragmentOutcome::Succeeded {
            return;
        }
        let (code, detail) = match outcome {
            novarocks_proto_models::novarocks::QueryTerminalFragmentOutcome::Failed => {
                terminal_error
            }
            novarocks_proto_models::novarocks::QueryTerminalFragmentOutcome::Cancelled => {
                ("FRAGMENT_CANCELLED".to_owned(), terminal_error.1)
            }
            novarocks_proto_models::novarocks::QueryTerminalFragmentOutcome::IncompleteDrain => {
                ("INCOMPLETE_DRAIN".to_owned(), terminal_error.1)
            }
            _ => return,
        };
        self.request_termination_with_event(
            Arc::clone(&entry),
            QueryTerminationReason::QueryTerminationLocalFailure,
            Some(local_failure_event(code, detail)),
        );
    }

    fn schedule_failed_terminal_drain(&self, entry: Arc<QueryLifecycleEntry>, timeout: Duration) {
        let weak = self.self_weak.clone();
        std::thread::Builder::new()
            .name("query-terminal-failure-drain".to_string())
            .spawn(move || {
                let deadline = Instant::now()
                    .checked_add(timeout)
                    .unwrap_or_else(Instant::now);
                loop {
                    let complete = {
                        let state = entry.state.lock().expect("query lifecycle entry lock");
                        entry
                            .expected_fragment_instance_ids
                            .iter()
                            .all(|id| state.terminal_facts.contains_key(id))
                    };
                    if complete || Instant::now() >= deadline {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
                if let Some(registry) = weak.upgrade() {
                    registry.freeze_failed_terminal_snapshot(entry, timeout);
                }
            })
            .expect("spawn failed query terminal drain");
    }

    fn freeze_failed_terminal_snapshot(&self, entry: Arc<QueryLifecycleEntry>, timeout: Duration) {
        let execution_id = validated(entry.manifest.execution_id());
        let (facts, participant, runtime_filter_installed, termination_reason) = {
            let mut state = entry.state.lock().expect("query lifecycle entry lock");
            if state.terminal_outcome.is_some()
                || state.terminal_freeze_in_flight
                || !state.failure_drain_scheduled
            {
                return;
            }
            let backend_num = 0;
            for fragment_instance_id in &entry.expected_fragment_instance_ids {
                if !state.terminal_facts.contains_key(fragment_instance_id) {
                    let detail = format!(
                        "fragment terminal fact was not observed within {}ms after local failure",
                        timeout.as_millis()
                    );
                    let snapshot = match terminal_fragment_snapshot(
                        *fragment_instance_id,
                        backend_num,
                        novarocks_proto_models::novarocks::QueryTerminalFragmentOutcome::IncompleteDrain,
                        "INCOMPLETE_DRAIN".to_owned(),
                        detail,
                        SinkCommitReportSnapshot::default(),
                        None,
                        Vec::new(),
                    ) {
                        Ok(snapshot) => snapshot,
                        Err(error) => {
                            warn!(target: "novarocks::query_lifecycle", error = %error, "failed to synthesize incomplete terminal fact");
                            return;
                        }
                    };
                    state.terminal_facts.insert(*fragment_instance_id, snapshot);
                }
            }
            state.terminal_freeze_in_flight = true;
            (
                state.terminal_facts.values().cloned().collect::<Vec<_>>(),
                state.runtime_filter.clone(),
                state.runtime_filter_installed,
                state
                    .termination_reason
                    .unwrap_or(QueryTerminationReason::QueryTerminationLocalFailure),
            )
        };
        // The entry lock only freezes terminal facts. Canonical encoding and
        // digest construction can be expensive and must not block control,
        // fragment completion, or ACK handling.
        // Freeze the participant contribution before optional P2 telemetry
        // assembly/fault handling. A P2 fault may make the projection
        // unavailable, but must never bypass terminal observation sealing.
        let runtime_filter_snapshot = participant
            .as_ref()
            .map(|participant| participant.prepare_terminal_capture(termination_reason));
        let contribution = match self.capture_terminal_profile_contribution(
            execution_id,
            runtime_filter_snapshot,
            runtime_filter_installed,
        ) {
            Ok(contribution) => contribution,
            Err(error) => {
                self.fail_terminal_freeze(&entry, execution_id, &error);
                return;
            }
        };
        let participant = match participant_attempt_ref(execution_id, &entry.manifest) {
            Ok(participant) => participant,
            Err(error) => {
                self.fail_terminal_freeze(&entry, execution_id, &error);
                return;
            }
        };
        let (snapshot, outcome) =
            match terminal_outcome_from_snapshot(participant, facts, contribution) {
                Ok(value) => value,
                Err(error) => {
                    self.fail_terminal_freeze(&entry, execution_id, &error);
                    return;
                }
            };
        if let Err(error) = self.fail_if_terminal_p1_encode_fault(execution_id) {
            self.fail_terminal_freeze(&entry, execution_id, &error);
            return;
        }
        let record = match ImmutableQueryTerminalRecord::new(
            snapshot,
            self.config.terminal_max_encoded_bytes,
        ) {
            Ok(record) => record,
            Err(error) => {
                self.fail_terminal_freeze(&entry, execution_id, &error);
                return;
            }
        };
        if let Err(error) = self.fail_if_terminal_p1_retention_fault(execution_id) {
            self.fail_terminal_freeze(&entry, execution_id, &error);
            return;
        }
        if let Err(error) = self.reserve_terminal_record(execution_id, record.encoded_len()) {
            self.fail_terminal_freeze(&entry, execution_id, &error);
            return;
        }
        let terminal_delivery = {
            let mut state = entry.state.lock().expect("query lifecycle entry lock");
            if state.terminal_outcome.is_some()
                || !state.terminal_freeze_in_flight
                || !state.failure_drain_scheduled
            {
                state.terminal_freeze_in_flight = false;
                self.release_terminal_record(execution_id);
                return;
            }
            state.terminal_freeze_in_flight = false;
            state.terminal_record = Some(record.clone());
            state.terminal_outcome = Some(outcome.clone());
            state.phase = QueryLifecyclePhase::TerminalRetained;
            state.terminated_at = Some(self.clock.now());
            (
                state.terminal_snapshot_event_permit.take(),
                state.events.clone(),
            )
        };
        self.release_query_resources(execution_id);
        let _ = self.try_complete_runtime_filter_cleanup(&entry, execution_id);
        self.emit_terminal_retained_marker(record.snapshot(), record.encoded_len());
        self.deliver_terminal_outcome(
            entry,
            execution_id,
            outcome,
            terminal_delivery.0,
            terminal_delivery.1,
        );
        self.increment_terminal_metric(|metrics| {
            metrics.terminal_records_frozen = metrics.terminal_records_frozen.saturating_add(1);
        });
    }

    fn fail_terminal_freeze(
        &self,
        entry: &Arc<QueryLifecycleEntry>,
        execution_id: QueryExecutionId,
        error: &QueryLifecycleError,
    ) {
        warn!(
            target: "novarocks::query_lifecycle",
            error = %error,
            "failed to freeze typed query terminal contribution"
        );
        let attestation_reason = match error.code() {
            QueryLifecycleErrorCode::Capacity => {
                novarocks_proto_models::novarocks::NegativeAttestationReason::CorrectnessEvidenceRetentionExhausted
            }
            QueryLifecycleErrorCode::InvalidManifest | QueryLifecycleErrorCode::Conflict => {
                novarocks_proto_models::novarocks::NegativeAttestationReason::TerminalStateInvalid
            }
            QueryLifecycleErrorCode::StaleBackend
            | QueryLifecycleErrorCode::Terminated
            | QueryLifecycleErrorCode::Transport
            | QueryLifecycleErrorCode::Internal => {
                novarocks_proto_models::novarocks::NegativeAttestationReason::CorrectnessEvidenceEncodingFailed
            }
        };
        let participant = match participant_attempt_ref(execution_id, &entry.manifest) {
            Ok(participant) => participant,
            Err(participant_error) => {
                warn!(target: "novarocks::query_lifecycle", error = %participant_error, "failed to construct terminal participant reference");
                return;
            }
        };
        let outcome =
            negative_terminal_outcome(participant, attestation_reason, error.detail().to_string());
        let terminal_delivery = {
            let mut state = entry.state.lock().expect("query lifecycle entry lock");
            if state.terminal_outcome.is_some() {
                state.terminal_freeze_in_flight = false;
                return;
            }
            state.terminal_freeze_in_flight = false;
            state.failure_drain_scheduled = false;
            state.terminal_record = None;
            state.terminal_outcome = Some(outcome.clone());
            state.phase = QueryLifecyclePhase::TerminalRetained;
            state.terminated_at = Some(self.clock.now());
            (
                state.terminal_snapshot_event_permit.take(),
                state.events.clone(),
            )
        };
        self.release_query_resources(execution_id);
        let _ = self.try_complete_runtime_filter_cleanup(entry, execution_id);
        self.deliver_terminal_outcome(
            Arc::clone(entry),
            execution_id,
            outcome,
            terminal_delivery.0,
            terminal_delivery.1,
        );
        self.increment_terminal_metric(|metrics| {
            metrics.terminal_records_frozen = metrics.terminal_records_frozen.saturating_add(1);
        });
    }

    fn finalize_from_control(
        &self,
        execution_id: QueryExecutionId,
    ) -> Result<(), QueryLifecycleError> {
        let entry = self
            .state
            .lock()
            .expect("query lifecycle registry lock")
            .entries
            .get(&execution_id)
            .cloned()
            .ok_or_else(|| {
                QueryLifecycleError::new(
                    QueryLifecycleErrorCode::Terminated,
                    "query lifecycle entry is not active",
                )
            })?;
        let (facts, expected, participant, runtime_filter_installed) = {
            let mut state = entry.state.lock().expect("query lifecycle entry lock");
            if state.phase != QueryLifecyclePhase::Running || !state.local_drained_emitted {
                return Err(QueryLifecycleError::new(
                    QueryLifecycleErrorCode::Terminated,
                    "Finalize requires locally drained participant",
                ));
            }
            let expected = entry.expected_fragment_instance_ids.clone();
            if expected.len() != state.terminal_facts.len() {
                return Err(QueryLifecycleError::new(
                    QueryLifecycleErrorCode::Internal,
                    "locally drained participant is missing terminal facts",
                ));
            }
            if state.terminal_outcome.is_some() || state.terminal_freeze_in_flight {
                return Err(QueryLifecycleError::new(
                    QueryLifecycleErrorCode::Terminated,
                    "query terminal record is already freezing or retained",
                ));
            }
            state.terminal_freeze_in_flight = true;
            (
                state.terminal_facts.values().cloned().collect::<Vec<_>>(),
                expected.to_vec(),
                state.runtime_filter.clone(),
                state.runtime_filter_installed,
            )
        };
        self.local_runtime.quiesce_query(
            execution_id,
            &expected,
            QueryTerminationReason::QueryTerminationCoordinatorFinalize,
            "query finalized after local drain",
        );
        // As on failure, terminal observation must be sealed before an
        // optional P2 fault can decide whether it is projected.
        let runtime_filter_snapshot = participant.as_ref().map(|participant| {
            participant.prepare_terminal_capture(
                QueryTerminationReason::QueryTerminationCoordinatorFinalize,
            )
        });
        // Finish the immutable record outside the lifecycle entry lock. The
        // local-drained gate makes the cloned fact set stable.
        let contribution = self
            .capture_terminal_profile_contribution(
                execution_id,
                runtime_filter_snapshot,
                runtime_filter_installed,
            )
            .inspect_err(|error| self.fail_terminal_freeze(&entry, execution_id, error))?;
        let participant_ref = participant_attempt_ref(execution_id, &entry.manifest)
            .inspect_err(|error| self.fail_terminal_freeze(&entry, execution_id, error))?;
        let (snapshot, outcome) =
            terminal_outcome_from_snapshot(participant_ref, facts, contribution)
                .inspect_err(|error| self.fail_terminal_freeze(&entry, execution_id, error))?;
        self.fail_if_terminal_p1_encode_fault(execution_id)
            .inspect_err(|error| self.fail_terminal_freeze(&entry, execution_id, error))?;
        let record =
            ImmutableQueryTerminalRecord::new(snapshot, self.config.terminal_max_encoded_bytes)
                .inspect_err(|error| self.fail_terminal_freeze(&entry, execution_id, error))?;
        self.fail_if_terminal_p1_retention_fault(execution_id)
            .inspect_err(|error| self.fail_terminal_freeze(&entry, execution_id, error))?;
        self.reserve_terminal_record(execution_id, record.encoded_len())
            .inspect_err(|error| self.fail_terminal_freeze(&entry, execution_id, error))?;
        let terminal_delivery = {
            let mut state = entry.state.lock().expect("query lifecycle entry lock");
            if state.phase != QueryLifecyclePhase::Running
                || state.terminal_outcome.is_some()
                || !state.terminal_freeze_in_flight
            {
                state.terminal_freeze_in_flight = false;
                self.release_terminal_record(execution_id);
                return Err(QueryLifecycleError::new(
                    QueryLifecycleErrorCode::Terminated,
                    "query lifecycle changed while terminal record was being reserved",
                ));
            }
            state.terminal_freeze_in_flight = false;
            state.terminal_record = Some(record.clone());
            state.terminal_outcome = Some(outcome.clone());
            state.phase = QueryLifecyclePhase::TerminalRetained;
            state.termination_reason =
                Some(QueryTerminationReason::QueryTerminationCoordinatorFinalize);
            state.terminated_at = Some(self.clock.now());
            (
                state.terminal_snapshot_event_permit.take(),
                state.events.clone(),
            )
        };
        // All execution-owned resources are detached only after the immutable
        // contribution is retained and before it is delivered.
        self.release_query_resources(execution_id);
        let _ = self.try_complete_runtime_filter_cleanup(&entry, execution_id);
        self.emit_terminal_retained_marker(record.snapshot(), record.encoded_len());
        let terminal_events = terminal_delivery.1.clone();
        self.deliver_terminal_outcome(
            Arc::clone(&entry),
            execution_id,
            outcome,
            terminal_delivery.0,
            terminal_delivery.1,
        );
        if let Some(events) = terminal_events {
            // Retain the QLC-3 acknowledgement as a compatibility latch. The
            // immutable snapshot above is the terminal payload; FE v4 stores
            // it before acknowledging the retained record.
            let _ = events.try_send(termination_accepted_event(
                QueryTerminationReason::QueryTerminationCoordinatorFinalize,
            ));
        }
        self.increment_terminal_metric(|metrics| {
            metrics.terminal_records_frozen = metrics.terminal_records_frozen.saturating_add(1);
        });
        Ok(())
    }

    fn fail_if_terminal_p1_encode_fault(
        &self,
        execution_id: QueryExecutionId,
    ) -> Result<(), QueryLifecycleError> {
        if self.claim_terminal_fault(
            QueryLifecycleFaultKind::TerminalP1EncodeFailure,
            execution_id,
        )? {
            return Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::Internal,
                "injected terminal P1 encoding failure after admission",
            ));
        }
        Ok(())
    }

    /// P2 assembly and budgeting faults remain optional telemetry. A sealed
    /// Backend observation correctness failure is distinct: it is propagated
    /// to terminalization and becomes negative attestation evidence.
    fn capture_terminal_profile_contribution(
        &self,
        execution_id: QueryExecutionId,
        snapshot: Option<RuntimeFilterObservationSnapshot>,
        runtime_filter_installed: bool,
    ) -> Result<
        novarocks_proto_models::novarocks::QueryTerminalProfileContributionTelemetry,
        QueryLifecycleError,
    > {
        for (kind, code) in [
            (
                QueryLifecycleFaultKind::ObservationP2AssemblyFailure,
                "INJECTED_P2_ASSEMBLY_FAILURE",
            ),
            (
                QueryLifecycleFaultKind::ObservationP2BudgetPressure,
                "INJECTED_P2_BUDGET_PRESSURE",
            ),
        ] {
            match self.claim_terminal_fault(kind, execution_id) {
                Ok(true) => {
                    return Ok(novarocks_proto_models::novarocks::QueryTerminalProfileContributionTelemetry {
                        telemetry: Some(novarocks_proto_models::novarocks::query_terminal_profile_contribution_telemetry::Telemetry::Unavailable(
                            novarocks_proto_models::novarocks::TerminalTelemetryUnavailable { stage: "runtime_filter_terminal_capture".to_owned(), code: code.to_owned() },
                        )),
                    });
                }
                Ok(false) => {}
                Err(error) => {
                    warn!(
                        target: "novarocks::query_lifecycle",
                        error = %error,
                        kind = kind.file_stem(),
                        "unable to claim optional runtime-filter observation fault"
                    );
                }
            }
        }
        capture_terminal_profile_contribution(snapshot, runtime_filter_installed)
    }

    fn fail_if_terminal_p1_retention_fault(
        &self,
        execution_id: QueryExecutionId,
    ) -> Result<(), QueryLifecycleError> {
        if self.claim_terminal_fault(
            QueryLifecycleFaultKind::TerminalP1RetentionExhausted,
            execution_id,
        )? {
            return Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::Capacity,
                "injected terminal P1 retention exhaustion after admission",
            ));
        }
        Ok(())
    }

    /// P0 has already been retained before this point. Suppression is a
    /// runner-only characterization of the one intentionally unresolved
    /// outcome path, so it must disable both attached-stream and unary
    /// fallback delivery without discarding the retained record.
    fn terminal_outcome_suppressed(&self, execution_id: QueryExecutionId) -> bool {
        match self.claim_terminal_fault(
            QueryLifecycleFaultKind::TerminalOutcomeSuppress,
            execution_id,
        ) {
            Ok(suppressed) => suppressed,
            Err(error) => {
                warn!(
                    target: "novarocks::query_lifecycle",
                    error = %error,
                    "unable to claim terminal outcome suppression fault; delivering outcome"
                );
                false
            }
        }
    }

    fn deliver_terminal_outcome(
        &self,
        entry: Arc<QueryLifecycleEntry>,
        execution_id: QueryExecutionId,
        outcome: ParticipantTerminalOutcome,
        permit: Option<tokio::sync::mpsc::OwnedPermit<QueryControlEvent>>,
        events: Option<tokio::sync::mpsc::Sender<QueryControlEvent>>,
    ) {
        if self.terminal_outcome_suppressed(execution_id) {
            if query_lifecycle_test_markers_enabled() {
                eprintln!(
                    "NOVAROCKS_QUERY_TERMINAL_OUTCOME_SUPPRESSED execution_id={} process_id={}",
                    format_execution_id(execution_id),
                    self.local_process_id,
                );
            }
            return;
        }
        let stream_fault = if outcome.proof().is_some() {
            QueryLifecycleFaultKind::TerminalProofStreamDrop
        } else {
            QueryLifecycleFaultKind::TerminalAttestationStreamDrop
        };
        match self.claim_terminal_fault(stream_fault, execution_id) {
            Ok(true) => {
                // Deliberately leave the retained P0/P1 outcome intact and
                // skip only the attached control stream. The ordinary unary
                // fallback owns eventual delivery from the immutable record.
                self.schedule_terminal_fallback(entry, outcome);
                return;
            }
            Ok(false) => {}
            Err(error) => {
                warn!(
                    target: "novarocks::query_lifecycle",
                    error = %error,
                    kind = stream_fault.file_stem(),
                    "unable to claim terminal stream-drop fault; delivering on control stream"
                );
            }
        }
        send_reserved_control_event(permit, events, terminal_outcome_event(&outcome));
        self.schedule_terminal_fallback(entry, outcome);
    }

    #[cfg(debug_assertions)]
    fn claim_terminal_fault(
        &self,
        kind: QueryLifecycleFaultKind,
        execution_id: QueryExecutionId,
    ) -> Result<bool, QueryLifecycleError> {
        #[cfg(test)]
        {
            let mut faults = self
                .terminal_test_faults
                .lock()
                .expect("query lifecycle terminal test faults lock");
            if let Some(kinds) = faults.get_mut(&execution_id)
                && let Some(index) = kinds.iter().position(|candidate| *candidate == kind)
            {
                kinds.remove(index);
                if kinds.is_empty() {
                    faults.remove(&execution_id);
                }
                return Ok(true);
            }
        }
        let Some(root) = novarocks_failpoint::configured_root() else {
            return Ok(false);
        };
        novarocks_failpoint::claim_matching_fault_for_process(
            &root,
            kind,
            execution_id,
            self.local_process_id,
        )
        .map(|claimed| claimed.is_some())
        .map_err(|error| {
            QueryLifecycleError::new(
                crate::query_lifecycle::QueryLifecycleErrorCode::Internal,
                error,
            )
        })
    }

    #[cfg(not(debug_assertions))]
    fn claim_terminal_fault(
        &self,
        _kind: QueryLifecycleFaultKind,
        _execution_id: QueryExecutionId,
    ) -> Result<bool, QueryLifecycleError> {
        Ok(false)
    }

    #[cfg(test)]
    pub(crate) fn inject_terminal_fault_for_test(
        &self,
        execution_id: QueryExecutionId,
        kind: QueryLifecycleFaultKind,
    ) {
        self.terminal_test_faults
            .lock()
            .expect("query lifecycle terminal test faults lock")
            .entry(execution_id)
            .or_default()
            .push(kind);
    }

    fn reserve_terminal_record(
        &self,
        execution_id: QueryExecutionId,
        bytes: usize,
    ) -> Result<(), QueryLifecycleError> {
        let mut state = self.state.lock().expect("query lifecycle registry lock");
        let Some(previous_bytes) = state.terminal_retained.get(&execution_id).copied() else {
            return self.reserve_terminal_p0_locked(&mut state, execution_id, bytes);
        };
        if bytes <= previous_bytes {
            return Ok(());
        }
        let delta = bytes - previous_bytes;
        let next_bytes = state
            .terminal_retained_bytes
            .checked_add(delta)
            .ok_or_else(|| {
                QueryLifecycleError::new(
                    QueryLifecycleErrorCode::Capacity,
                    "query terminal retained-byte accounting overflowed",
                )
            })?;
        if next_bytes > self.config.terminal_max_retained_bytes {
            return Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::Capacity,
                "query terminal retained-byte capacity is exhausted",
            ));
        }
        state.terminal_retained.insert(execution_id, bytes);
        state.terminal_retained_bytes = next_bytes;
        Ok(())
    }

    fn reserve_terminal_p0(
        &self,
        execution_id: QueryExecutionId,
        bytes: usize,
    ) -> Result<(), QueryLifecycleError> {
        let mut state = self.state.lock().expect("query lifecycle registry lock");
        self.reserve_terminal_p0_locked(&mut state, execution_id, bytes)
    }

    fn reserve_terminal_p0_locked(
        &self,
        state: &mut QueryLifecycleRegistryState,
        execution_id: QueryExecutionId,
        bytes: usize,
    ) -> Result<(), QueryLifecycleError> {
        if state.terminal_retained.contains_key(&execution_id) {
            return Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::Conflict,
                "query terminal P0 reservation already exists",
            ));
        }
        if state.terminal_retained.len() >= self.config.terminal_retained_capacity {
            return Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::Capacity,
                "query terminal retained-record capacity is exhausted",
            ));
        }
        let next_bytes = state
            .terminal_retained_bytes
            .checked_add(bytes)
            .ok_or_else(|| {
                QueryLifecycleError::new(
                    QueryLifecycleErrorCode::Capacity,
                    "query terminal retained-byte accounting overflowed",
                )
            })?;
        if next_bytes > self.config.terminal_max_retained_bytes {
            return Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::Capacity,
                "query terminal retained-byte capacity is exhausted",
            ));
        }
        state.terminal_retained.insert(execution_id, bytes);
        state.terminal_retained_bytes = next_bytes;
        Ok(())
    }

    fn release_terminal_record(&self, execution_id: QueryExecutionId) {
        let mut state = self.state.lock().expect("query lifecycle registry lock");
        if let Some(bytes) = state.terminal_retained.remove(&execution_id) {
            state.terminal_retained_bytes = state.terminal_retained_bytes.saturating_sub(bytes);
        }
    }

    fn emit_terminal_retained_marker(&self, snapshot: &QueryTerminalSnapshot, bytes: usize) {
        if query_lifecycle_test_markers_enabled() {
            eprintln!(
                "NOVAROCKS_QUERY_TERMINAL_RETAINED execution_id={} process_id={} bytes={}",
                format_execution_id(snapshot.execution_id()),
                self.local_process_id,
                bytes,
            );
        }
    }

    fn schedule_terminal_fallback(
        &self,
        entry: Arc<QueryLifecycleEntry>,
        outcome: ParticipantTerminalOutcome,
    ) {
        let participant = outcome.participant();
        let endpoint = validated(entry.manifest.report_endpoint());
        let weak = self.self_weak.clone();
        let transport = Arc::clone(&self.terminal_fallback);
        let config = self.config;
        std::thread::Builder::new()
            .name("query-terminal-fallback".to_string())
            .spawn(move || {
                let retained = entry
                    .terminal_delivery_completed
                    .wait_timeout_while(
                        entry.state.lock().expect("query lifecycle entry lock"),
                        config.terminal_ack_timeout,
                        |state| terminal_outcome_matches(state.terminal_outcome.as_ref(), &participant),
                    )
                    .expect("query lifecycle terminal fallback wait")
                    .0
                    .terminal_outcome
                    .as_ref()
                    .is_some_and(|retained| terminal_outcome_matches(Some(retained), &participant));
                if !retained {
                    return;
                }
                let mut backoff = config.terminal_fallback_initial_backoff;
                for attempt in 0..config.terminal_fallback_max_attempts {
                    let Some(registry) = weak.upgrade() else {
                        return;
                    };
                    let retained = entry
                        .state
                        .lock()
                        .expect("query lifecycle entry lock")
                        .terminal_outcome
                        .as_ref()
                        .is_some_and(|retained| terminal_outcome_matches(Some(retained), &participant));
                    if !retained {
                        return;
                    }
                    match transport.report_query_terminal(
                        &endpoint,
                        outcome.clone(),
                        config.terminal_fallback_rpc_timeout,
                    ) {
                        Ok(ack)
                            if matches!(
                                ack.outcome(),
                                Ok(QueryTerminalReportOutcome::Accepted)
                                    | Ok(QueryTerminalReportOutcome::AlreadyAccepted)
                            ) =>
                        {
                            registry.increment_terminal_metric(|metrics| {
                                metrics.terminal_fallback_accepted = metrics
                                    .terminal_fallback_accepted
                                    .saturating_add(1);
                            });
                            if query_lifecycle_test_markers_enabled() {
                                eprintln!(
                                    "NOVAROCKS_QUERY_TERMINAL_FALLBACK_ACCEPTED execution_id={} process_id={} attempt={} outcome={:?}",
                                    format_execution_id(outcome.execution_id()),
                                    registry.local_process_id,
                                    attempt + 1,
                                    ack.outcome().expect("validated terminal fallback acknowledgement"),
                                );
                            }
                            registry.complete_terminal_delivery(
                                &entry,
                                outcome.execution_id(),
                                &participant,
                            );
                            return;
                        }
                        Ok(ack) => {
                            registry.increment_terminal_metric(|metrics| {
                                metrics.terminal_fallback_rejected = metrics
                                    .terminal_fallback_rejected
                                    .saturating_add(1);
                            });
                            if query_lifecycle_test_markers_enabled() {
                                eprintln!(
                                    "NOVAROCKS_QUERY_TERMINAL_FALLBACK_RETRY execution_id={} process_id={} attempt={} outcome={:?} detail={}",
                                    format_execution_id(outcome.execution_id()),
                                    registry.local_process_id,
                                    attempt + 1,
                                    ack.outcome(),
                                    ack.detail(),
                                );
                            }
                            warn!(
                                target: "novarocks::query_lifecycle",
                                attempt,
                                outcome = ?ack.outcome().expect("validated terminal fallback acknowledgement"),
                                detail = %ack.detail(),
                                "query terminal fallback was rejected"
                            );
                            if matches!(
                                ack.outcome(),
                                Ok(QueryTerminalReportOutcome::RejectedConflict)
                                    | Ok(QueryTerminalReportOutcome::RejectedGone)
                            ) {
                                registry.discard_terminal_record(
                                    &entry,
                                    outcome.execution_id(),
                                    &participant,
                                );
                                return;
                            }
                        }
                        Err(error) => {
                            registry.increment_terminal_metric(|metrics| {
                                metrics.terminal_fallback_rejected = metrics
                                    .terminal_fallback_rejected
                                    .saturating_add(1);
                            });
                            if query_lifecycle_test_markers_enabled() {
                                eprintln!(
                                    "NOVAROCKS_QUERY_TERMINAL_FALLBACK_RETRY execution_id={} process_id={} attempt={} transport_error={}",
                                    format_execution_id(outcome.execution_id()),
                                    registry.local_process_id,
                                    attempt + 1,
                                    error,
                                );
                            }
                            warn!(
                                target: "novarocks::query_lifecycle",
                                attempt,
                                error = %error,
                                "query terminal fallback delivery failed"
                            );
                        }
                    }
                    if attempt + 1 < config.terminal_fallback_max_attempts {
                        std::thread::sleep(backoff);
                        backoff = backoff
                            .checked_mul(2)
                            .unwrap_or(config.terminal_fallback_max_backoff)
                            .min(config.terminal_fallback_max_backoff);
                    }
                }
            })
            .expect("spawn query terminal fallback delivery");
    }

    // Design: ADR-0126 (docs/adr/ADR-0126-terminal-delivery-participant-attempt-ref.md)
    fn terminal_ack_from_control(&self, ack: QueryTerminalAck) -> Result<(), QueryLifecycleError> {
        let participant = ack.participant().map_err(protocol_contract_error)?;
        let execution_id = participant
            .execution_id()
            .map_err(protocol_contract_error)?;
        if participant
            .backend_process_id()
            .map_err(protocol_contract_error)?
            != self.local_process_id
        {
            return Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::StaleBackend,
                "query terminal ACK names a different backend process",
            ));
        }
        let entry = self
            .state
            .lock()
            .expect("query lifecycle registry lock")
            .entries
            .get(&execution_id)
            .cloned()
            .ok_or_else(|| {
                QueryLifecycleError::new(
                    QueryLifecycleErrorCode::Terminated,
                    "query terminal record is gone",
                )
            })?;
        let mut state = entry.state.lock().expect("query lifecycle entry lock");
        let outcome = state.terminal_outcome.as_ref().ok_or_else(|| {
            QueryLifecycleError::new(
                QueryLifecycleErrorCode::Terminated,
                "query terminal record is not retained",
            )
        })?;
        if outcome.participant() != participant {
            return Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::Conflict,
                "query terminal ACK identity conflicts with retained snapshot",
            ));
        }
        if query_lifecycle_test_markers_enabled() {
            let query_id = execution_id.query_id();
            eprintln!(
                "NOVAROCKS_QUERY_TERMINAL_ACK query_hi={} query_lo={} attempt={} process_id={} {}",
                query_id.high(),
                query_id.low(),
                execution_id.attempt_id().get(),
                self.local_process_id,
                QueryExecutionDiagnostic::from(execution_id),
            );
        }
        let reason = state
            .termination_reason
            .unwrap_or(QueryTerminationReason::QueryTerminationCoordinatorFinalize);
        state.terminal_record = None;
        state.terminal_outcome = None;
        drop(state);
        entry.terminal_delivery_completed.notify_all();
        self.increment_terminal_metric(|metrics| {
            metrics.terminal_acknowledged = metrics.terminal_acknowledged.saturating_add(1);
        });
        self.release_terminal_record(execution_id);
        self.publish_tombstone(&entry, execution_id, reason);
        Ok(())
    }

    fn complete_terminal_delivery(
        &self,
        entry: &Arc<QueryLifecycleEntry>,
        execution_id: QueryExecutionId,
        participant: &ParticipantAttemptRef,
    ) {
        let reason = {
            let mut state = entry.state.lock().expect("query lifecycle entry lock");
            if !terminal_outcome_matches(state.terminal_outcome.as_ref(), participant) {
                return;
            }
            state.terminal_record = None;
            state.terminal_outcome = None;
            state
                .termination_reason
                .unwrap_or(QueryTerminationReason::QueryTerminationCoordinatorFinalize)
        };
        entry.terminal_delivery_completed.notify_all();
        self.increment_terminal_metric(|metrics| {
            metrics.terminal_acknowledged = metrics.terminal_acknowledged.saturating_add(1);
        });
        self.release_terminal_record(execution_id);
        self.publish_tombstone(entry, execution_id, reason);
    }

    /// A conflict is a terminal answer from a live FE: retrying the immutable
    /// snapshot cannot change the rejected identity. Drop only this bounded
    /// delivery record; execution resources were detached before it existed.
    fn discard_terminal_record(
        &self,
        entry: &Arc<QueryLifecycleEntry>,
        execution_id: QueryExecutionId,
        participant: &ParticipantAttemptRef,
    ) {
        let reason = {
            let mut state = entry.state.lock().expect("query lifecycle entry lock");
            let retained = terminal_outcome_matches(state.terminal_outcome.as_ref(), participant);
            if !retained {
                return;
            }
            state.terminal_record = None;
            state.terminal_outcome = None;
            state
                .termination_reason
                .unwrap_or(QueryTerminationReason::QueryTerminationCoordinatorFinalize)
        };
        entry.terminal_delivery_completed.notify_all();
        self.release_terminal_record(execution_id);
        self.publish_metrics();
        self.publish_tombstone(entry, execution_id, reason);
    }

    fn try_complete_runtime_filter_cleanup(
        &self,
        entry: &Arc<QueryLifecycleEntry>,
        _execution_id: QueryExecutionId,
    ) -> bool {
        let participant = {
            let mut state = entry.state.lock().expect("query lifecycle entry lock");
            if state.runtime_filter_close_in_flight {
                return false;
            }
            if state.runtime_filter.is_none() {
                return true;
            }
            state.runtime_filter_close_in_flight = true;
            // Drop the strong egress before calling into participant cleanup.
            // The participant only retained a Weak, so neither cancellation
            // nor tombstone retention can keep a feedback queue alive.
            state.frontend_feedback_sink = None;
            state.runtime_filter.take()
        };
        let participant = participant.expect("runtime-filter participant was checked present");
        let reason = entry
            .state
            .lock()
            .expect("query lifecycle entry lock")
            .termination_reason
            .unwrap_or(QueryTerminationReason::QueryTerminationLocalFailure);
        // Service close has its own first-wins quiescence barrier. Keep it out
        // of the entry lock so inbound ingress and terminal callers cannot
        // deadlock each other around the participant owner.
        let close_result = participant.close(reason);
        let mut state = entry.state.lock().expect("query lifecycle entry lock");
        state.runtime_filter_close_in_flight = false;
        match close_result {
            Ok(()) => true,
            Err(_) => {
                // Preserve the same attempt-local owner for a later terminal
                // sweep. A failed close must not release active capacity or
                // publish a tombstone while the Service can still be live.
                state.runtime_filter = Some(participant);
                false
            }
        }
    }

    fn publish_tombstone(
        &self,
        entry: &Arc<QueryLifecycleEntry>,
        execution_id: QueryExecutionId,
        reason: QueryTerminationReason,
    ) {
        {
            let mut entry_state = entry.state.lock().expect("query lifecycle entry lock");
            if entry_state.phase == QueryLifecyclePhase::Tombstone {
                return;
            }
            if entry_state.runtime_filter.is_some() || entry_state.runtime_filter_close_in_flight {
                return;
            }
            entry_state.phase = QueryLifecyclePhase::Tombstone;
            entry_state.termination_reason.get_or_insert(reason);
            entry_state.terminated_at = Some(self.clock.now());
            entry_state
                .init_outcome
                .get_or_insert(QueryInitOutcome::QueryInitRejectedTerminated);
            entry.init_completed.notify_all();
        }
        self.release_terminal_record(execution_id);
        self.catalog_manager.release_query(execution_id);
        self.publish_catalog_lease_metrics();
        let mut state = self.state.lock().expect("query lifecycle registry lock");
        state.active_entries = state.active_entries.saturating_sub(1);
        state.tombstones.push_back(execution_id);
        state.terminations = state.terminations.saturating_add(1);
        state.termination_reasons[termination_reason_index(reason)] =
            state.termination_reasons[termination_reason_index(reason)].saturating_add(1);
        self.clean_tombstones_locked(&mut state, self.clock.now(), 64);
        self.enforce_tombstone_capacity_locked(&mut state);
        drop(state);
        let diagnostic = QueryExecutionDiagnostic::from(execution_id);
        info!(
            target: "novarocks::query_lifecycle",
            query_id = ?execution_id.query_id(),
            query_process_namespace = %diagnostic.process_namespace(),
            query_local_sequence = %diagnostic.local_sequence(),
            query_attempt_id = diagnostic.attempt_id(),
            attempt_id = execution_id.attempt_id().get(),
            process_id = %self.local_process_id,
            digest = %format_digest(entry.digest),
            outcome = "terminated",
            reason = ?reason,
            "backend query lifecycle terminated"
        );
        self.publish_metrics();
        if let Some(cleanup) = self
            .terminal_cleanup
            .lock()
            .expect("query lifecycle terminal cleanup lock")
            .clone()
            .and_then(|cleanup| cleanup.upgrade())
        {
            cleanup.cleanup_terminal_execution(execution_id);
        }
        if query_lifecycle_test_markers_enabled() {
            eprintln!(
                "NOVAROCKS_QUERY_LIFECYCLE_CLEANUP execution_id={} process_id={} active=false tombstone=true reason={reason:?} {}",
                format_execution_id(execution_id),
                self.local_process_id,
                QueryExecutionDiagnostic::from(execution_id),
            );
        }
    }

    fn clean_tombstones_locked(
        &self,
        state: &mut QueryLifecycleRegistryState,
        now: Instant,
        limit: usize,
    ) {
        let mut removed = 0;
        while removed < limit {
            let Some(execution_id) = state.tombstones.front().copied() else {
                break;
            };
            let terminated_at = state
                .pre_init_tombstones
                .get(&execution_id)
                .map(|tombstone| tombstone.terminated_at)
                .or_else(|| {
                    state.entries.get(&execution_id).and_then(|entry| {
                        entry
                            .state
                            .lock()
                            .expect("query lifecycle entry lock")
                            .terminated_at
                    })
                });
            if !terminated_at.is_some_and(|at| {
                now.saturating_duration_since(at) >= self.config.tombstone_retention
            }) {
                break;
            }
            state.tombstones.pop_front();
            Self::evict_tombstone_execution_locked(state, execution_id);
            removed += 1;
        }
    }

    fn enforce_tombstone_capacity_locked(&self, state: &mut QueryLifecycleRegistryState) {
        while state.tombstones.len() > self.config.tombstone_capacity {
            let execution_id = state
                .tombstones
                .pop_front()
                .expect("tombstone length checked");
            Self::evict_tombstone_execution_locked(state, execution_id);
        }
    }

    fn evict_tombstone_execution_locked(
        state: &mut QueryLifecycleRegistryState,
        execution_id: QueryExecutionId,
    ) {
        state.pre_init_tombstones.remove(&execution_id);
        if state.entries.get(&execution_id).is_some_and(|entry| {
            entry
                .state
                .lock()
                .expect("query lifecycle entry lock")
                .phase
                == QueryLifecyclePhase::Tombstone
        }) {
            state.entries.remove(&execution_id);
        }
        state
            .fragment_executions
            .retain(|_, mapped_execution_id| *mapped_execution_id != execution_id);
    }

    fn heartbeat(
        &self,
        execution_id: QueryExecutionId,
        sequence: u64,
    ) -> Result<(), QueryLifecycleError> {
        let entry = self.active_entry(execution_id)?;
        let events = {
            let mut state = entry.state.lock().expect("query lifecycle entry lock");
            if !matches!(
                state.phase,
                QueryLifecyclePhase::ControlAttached
                    | QueryLifecyclePhase::Staging
                    | QueryLifecyclePhase::Staged
                    | QueryLifecyclePhase::Running
            ) || state.termination_reason.is_some()
            {
                return Err(QueryLifecycleError::new(
                    QueryLifecycleErrorCode::Terminated,
                    "query control is not active",
                ));
            }
            state.last_heartbeat = Some(self.clock.now());
            state.events.clone()
        };
        if let Some(events) = events {
            events
                .try_send(heartbeat_ack_event(sequence))
                .map_err(|error| {
                    QueryLifecycleError::new(
                        QueryLifecycleErrorCode::Internal,
                        format!("publish heartbeat ack failed: {error}"),
                    )
                })?;
        }
        Ok(())
    }

    fn prepare_credential_lease(
        &self,
        execution_id: QueryExecutionId,
        envelope: novarocks_proto_codec::lifecycle::CredentialLeaseSecretEnvelope,
    ) -> Result<(), QueryLifecycleError> {
        let lease_id = envelope.lease_id();
        let entry = self.active_entry(execution_id)?;
        let (epoch, events) = {
            let mut state = entry.state.lock().expect("query lifecycle entry lock");
            if state.termination_reason.is_some()
                || !matches!(
                    state.phase,
                    QueryLifecyclePhase::ControlAttached
                        | QueryLifecyclePhase::Staging
                        | QueryLifecyclePhase::Staged
                        | QueryLifecyclePhase::Running
                )
            {
                return Err(QueryLifecycleError::new(
                    QueryLifecycleErrorCode::Terminated,
                    CredentialLeaseError::Terminated.detail(),
                ));
            }
            let epoch = state
                .credential_leases
                .prepare(envelope)
                .map_err(credential_lease_error)?;
            (epoch, state.events.clone())
        };
        send_credential_lease_event(events, credential_lease_prepared_event(epoch, lease_id))
    }

    fn commit_credential_lease(
        &self,
        execution_id: QueryExecutionId,
        lease_id: novarocks_spi::connector::CredentialLeaseId,
        epoch: u64,
    ) -> Result<(), QueryLifecycleError> {
        let entry = self.active_entry(execution_id)?;
        let events = {
            let mut state = entry.state.lock().expect("query lifecycle entry lock");
            if state.termination_reason.is_some()
                || !matches!(
                    state.phase,
                    QueryLifecyclePhase::ControlAttached
                        | QueryLifecyclePhase::Staging
                        | QueryLifecyclePhase::Staged
                        | QueryLifecyclePhase::Running
                )
            {
                return Err(QueryLifecycleError::new(
                    QueryLifecycleErrorCode::Terminated,
                    CredentialLeaseError::Terminated.detail(),
                ));
            }
            state
                .credential_leases
                .commit(lease_id, epoch)
                .map_err(credential_lease_error)?;
            state.events.clone()
        };
        send_credential_lease_event(events, credential_lease_committed_event(epoch, lease_id))
    }

    fn terminate_from_control(
        &self,
        execution_id: QueryExecutionId,
        reason: QueryTerminationReason,
    ) -> Result<(), QueryLifecycleError> {
        let entry = self.active_entry(execution_id)?;
        let repeated = entry
            .state
            .lock()
            .expect("query lifecycle entry lock")
            .termination_reason
            .is_some();
        if matches!(
            reason,
            QueryTerminationReason::QueryTerminationCoordinatorStreamLost
                | QueryTerminationReason::QueryTerminationCoordinatorHeartbeatTimeout
        ) {
            let diagnostic = QueryExecutionDiagnostic::from(execution_id);
            warn!(
                target: "novarocks::query_lifecycle",
                query_id = ?execution_id.query_id(),
                query_process_namespace = %diagnostic.process_namespace(),
                query_local_sequence = %diagnostic.local_sequence(),
                query_attempt_id = diagnostic.attempt_id(),
                attempt_id = execution_id.attempt_id().get(),
                process_id = %self.local_process_id,
                digest = %format_digest(entry.digest),
                outcome = "coordinator_lost",
                reason = ?reason,
                "backend query lifecycle coordinator lost"
            );
        }
        let accepted = self.request_termination(Arc::clone(&entry), reason);
        if repeated {
            let events = entry
                .state
                .lock()
                .expect("query lifecycle entry lock")
                .events
                .clone();
            if let Some(events) = events {
                let _ = events.try_send(termination_accepted_event(accepted));
            }
        }
        Ok(())
    }

    fn active_entry(
        &self,
        execution_id: QueryExecutionId,
    ) -> Result<Arc<QueryLifecycleEntry>, QueryLifecycleError> {
        self.state
            .lock()
            .expect("query lifecycle registry lock")
            .entries
            .get(&execution_id)
            .cloned()
            .ok_or_else(|| {
                QueryLifecycleError::new(
                    QueryLifecycleErrorCode::Terminated,
                    "query lifecycle entry is not active",
                )
            })
    }

    #[cfg(test)]
    pub(crate) fn phase(&self, execution_id: QueryExecutionId) -> Option<QueryLifecyclePhase> {
        let entry = self
            .state
            .lock()
            .expect("query lifecycle registry lock")
            .entries
            .get(&execution_id)
            .cloned()?;
        let phase = entry
            .state
            .lock()
            .expect("query lifecycle entry lock")
            .phase;
        Some(phase)
    }

    #[cfg(test)]
    pub(crate) fn was_ever_initialized(&self, execution_id: QueryExecutionId) -> bool {
        let entry = self
            .state
            .lock()
            .expect("query lifecycle registry lock")
            .entries
            .get(&execution_id)
            .cloned();
        entry.is_some_and(|entry| {
            entry
                .state
                .lock()
                .expect("query lifecycle entry lock")
                .ever_initialized
        })
    }

    #[cfg(test)]
    pub(crate) fn termination_reason(
        &self,
        execution_id: QueryExecutionId,
    ) -> Option<QueryTerminationReason> {
        let entry = self
            .state
            .lock()
            .expect("query lifecycle registry lock")
            .entries
            .get(&execution_id)
            .cloned()?;
        entry
            .state
            .lock()
            .expect("query lifecycle entry lock")
            .termination_reason
    }

    #[cfg(test)]
    pub(crate) fn contains(&self, execution_id: QueryExecutionId) -> bool {
        let state = self.state.lock().expect("query lifecycle registry lock");
        state.entries.contains_key(&execution_id)
            || state.pre_init_tombstones.contains_key(&execution_id)
    }

    #[cfg(test)]
    pub(crate) fn metrics_snapshot(&self) -> BackendQueryLifecycleMetricsSnapshot {
        let state = self.state.lock().expect("query lifecycle registry lock");
        fold_metrics_locked(&state).0
    }

    fn publish_metrics(&self) {
        let (snapshot, termination_reasons, runtime_filter_services) = {
            let state = self.state.lock().expect("query lifecycle registry lock");
            let (snapshot, termination_reasons) = fold_metrics_locked(&state);
            let runtime_filter_services = state
                .entries
                .values()
                .filter(|entry| {
                    entry
                        .state
                        .lock()
                        .expect("query lifecycle entry lock")
                        .runtime_filter
                        .is_some()
                })
                .count();
            (snapshot, termination_reasons, runtime_filter_services)
        };
        self.metrics.publish(snapshot, termination_reasons);
        publish_backend_query_execution_resource(
            "native_runtime_filter_services",
            runtime_filter_services,
        );
    }

    fn increment_terminal_metric(&self, update: impl FnOnce(&mut QueryLifecycleRegistryState)) {
        {
            let mut state = self.state.lock().expect("query lifecycle registry lock");
            update(&mut state);
        }
        self.publish_metrics();
    }

    fn log_init(&self, ack: &QueryInitAck) {
        let execution_id = validated(ack.execution_id());
        let digest = validated(ack.digest());
        let outcome = validated(ack.outcome());
        let diagnostic = QueryExecutionDiagnostic::from(execution_id);
        info!(
            target: "novarocks::query_lifecycle",
            query_id = ?execution_id.query_id(),
            query_process_namespace = %diagnostic.process_namespace(),
            query_local_sequence = %diagnostic.local_sequence(),
            query_attempt_id = diagnostic.attempt_id(),
            attempt_id = execution_id.attempt_id().get(),
            process_id = %self.local_process_id,
            digest = %format_digest(digest),
            outcome = ?outcome,
            reason = "none",
            "backend query lifecycle init"
        );
        if query_lifecycle_test_markers_enabled()
            && matches!(
                outcome,
                QueryInitOutcome::QueryInitApplied | QueryInitOutcome::QueryInitAlreadyApplied
            )
        {
            let expected_fragments = self
                .state
                .lock()
                .expect("query lifecycle registry lock")
                .entries
                .get(&execution_id)
                .map(|entry| entry.expected_fragment_instance_ids.len())
                .unwrap_or_default();
            let marker = if outcome == QueryInitOutcome::QueryInitApplied {
                "NOVAROCKS_QUERY_INIT_APPLIED"
            } else {
                "NOVAROCKS_QUERY_INIT_IDEMPOTENT"
            };
            eprintln!(
                "{marker} execution_id={} process_id={} expected_fragments={expected_fragments} {}",
                format_execution_id(execution_id),
                self.local_process_id,
                QueryExecutionDiagnostic::from(execution_id),
            );
        }
    }
}

impl InitWorkspace {
    fn install_and_publish(self) -> QueryInitAck {
        let contribution = validated(self.entry.manifest.runtime_filter());
        let install_result = contribution.map_or(Ok(None), |contribution| {
            let contribution =
                decode_runtime_filter_contribution(self.execution_id, &contribution)?;
            self.registry
                .runtime_filter_factory
                .install(self.execution_id, contribution)
                .map(Some)
        });
        if install_result.is_err() {
            let (reason, terminate_locally) = {
                let mut state = self.entry.state.lock().expect("query lifecycle entry lock");
                state.init_outcome = Some(QueryInitOutcome::QueryInitRejectedInvalidManifest);
                let terminate_locally = state.termination_reason.is_none();
                let reason = *state
                    .termination_reason
                    .get_or_insert(QueryTerminationReason::QueryTerminationLocalFailure);
                state.phase = QueryLifecyclePhase::Terminating;
                self.entry.init_completed.notify_all();
                (reason, terminate_locally)
            };
            if terminate_locally {
                let expected_instances = self.entry.expected_fragment_instance_ids.clone();
                self.registry.local_runtime.quiesce_query(
                    self.execution_id,
                    &expected_instances,
                    reason,
                    &termination_detail(reason),
                );
                self.registry.release_query_resources(self.execution_id);
            }
            if self
                .registry
                .try_complete_runtime_filter_cleanup(&self.entry, self.execution_id)
            {
                self.registry
                    .publish_tombstone(&self.entry, self.execution_id, reason);
            }
            return QueryInitAck::new(
                self.execution_id,
                self.digest,
                QueryInitOutcome::QueryInitRejectedInvalidManifest,
            );
        }

        let participant = install_result.expect("runtime-filter install result was checked");
        let catalogs = validated(validated(self.entry.manifest.catalog_set()).catalogs());
        let catalogs_ready = !catalogs.is_empty()
            && self
                .registry
                .catalog_manager
                .try_acquire_ready_catalogs(self.execution_id, &catalogs)
                .unwrap_or(false);
        let catalog_load = if catalogs.is_empty() || catalogs_ready {
            QueryCatalogLoadState::Ready
        } else {
            QueryCatalogLoadState::Loading {
                catalogs: catalogs.clone(),
            }
        };
        let terminated = {
            let mut state = self.entry.state.lock().expect("query lifecycle entry lock");
            if state.termination_reason.is_some() {
                state.runtime_filter_installed = participant.is_some();
                state.runtime_filter = participant;
                state.init_outcome = Some(QueryInitOutcome::QueryInitRejectedTerminated);
                self.entry.init_completed.notify_all();
                true
            } else {
                state.runtime_filter_installed = participant.is_some();
                state.runtime_filter = participant;
                state.catalog_load = catalog_load;
                state.phase = QueryLifecyclePhase::Initialized;
                state.ever_initialized = true;
                state.init_outcome = Some(QueryInitOutcome::QueryInitApplied);
                state.pre_start_deadline =
                    Some(self.registry.clock.now() + self.registry.config.pre_start_timeout);
                self.entry.init_completed.notify_all();
                false
            }
        };
        let ack = if terminated {
            let reason = self
                .entry
                .state
                .lock()
                .expect("query lifecycle entry lock")
                .termination_reason
                .expect("termination was observed");
            if self
                .registry
                .try_complete_runtime_filter_cleanup(&self.entry, self.execution_id)
            {
                self.registry
                    .publish_tombstone(&self.entry, self.execution_id, reason);
            }
            QueryInitAck::new(
                self.execution_id,
                self.digest,
                QueryInitOutcome::QueryInitRejectedTerminated,
            )
        } else {
            QueryInitAck::new(
                self.execution_id,
                self.digest,
                QueryInitOutcome::QueryInitApplied,
            )
        };

        if !terminated && !catalogs.is_empty() && !catalogs_ready {
            self.registry.begin_catalog_install(
                Arc::clone(&self.entry),
                self.execution_id,
                catalogs,
            );
        }
        ack
    }
}

impl StageBuildPermit {
    pub(crate) fn gate(&self) -> Arc<super::stage::StartGate> {
        Arc::clone(&self.gate)
    }

    /// The stage identity derived once when the build was reserved. Callers
    /// acknowledge with this value instead of deriving it again.
    pub(crate) const fn digest(&self) -> StageDigest {
        self.digest
    }

    pub(crate) fn commit(mut self) -> QueryStageAck {
        let mut state = self.entry.state.lock().expect("query lifecycle entry lock");
        let (outcome, detail) = if state.termination_reason.is_some()
            || matches!(
                state.phase,
                QueryLifecyclePhase::Terminating | QueryLifecyclePhase::Tombstone
            ) {
            (
                QueryStageOutcome::RejectedTerminated,
                "query lifecycle terminated during staging",
            )
        } else if state.phase == QueryLifecyclePhase::Staging
            && state.stage_digest == Some(self.digest)
        {
            let mut resources = self
                .resources
                .take()
                .expect("Stage build permit owns its resource reservation");
            resources.release_builder();
            debug_assert!(state.stage_resources.is_none());
            state.stage_resources = Some(resources);
            state.phase = QueryLifecyclePhase::Staged;
            (QueryStageOutcome::Applied, "query participant staged")
        } else {
            (
                QueryStageOutcome::RejectedInvalidState,
                "query lifecycle stage ownership was lost",
            )
        };
        self.entry.stage_completed.notify_all();
        drop(state);
        self.committed = true;
        if outcome == QueryStageOutcome::Applied
            && crate::config::debug_emit_catalog_lifecycle_marker()
        {
            println!(
                "NOVAROCKS_CATALOG_STAGE_ADMITTED execution_id={} process_id={}",
                format_execution_id(self.execution_id),
                self.registry.local_process_id,
            );
        }
        QueryStageAck::new(self.execution_id, self.digest, outcome, detail)
            .expect("validated Stage acknowledgement has a valid Protocol projection")
    }
}

impl Drop for StageBuildPermit {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        self.entry.stage_completed.notify_all();
        self.registry.request_termination(
            Arc::clone(&self.entry),
            QueryTerminationReason::QueryTerminationLocalFailure,
        );
    }
}

impl FragmentAdmissionPermit {
    #[cfg(test)]
    pub(crate) fn entry_for_test(&self) -> Arc<QueryLifecycleEntry> {
        Arc::clone(&self.entry)
    }

    pub(crate) fn commit(mut self) -> Result<(), QueryLifecycleError> {
        let registry = self
            .registry
            .upgrade()
            .ok_or_else(|| internal_error("query lifecycle registry was dropped"))?;
        let mut registry_state = registry
            .state
            .lock()
            .expect("query lifecycle registry lock");
        let mut state = self.entry.state.lock().expect("query lifecycle entry lock");
        if state.termination_reason.is_some()
            || matches!(
                state.phase,
                QueryLifecyclePhase::Terminating | QueryLifecyclePhase::Tombstone
            )
        {
            let reason = state.termination_reason;
            let expected_instances = self.entry.expected_fragment_instance_ids.clone();
            drop(state);
            drop(registry_state);
            if let Some(reason) = reason {
                // Termination may have raced ahead of the service registration/control
                // publication protected by this permit. Re-drive local termination after
                // those resources exist so the rejected admission cannot leave a live worker.
                registry.local_runtime.quiesce_query(
                    self.execution_id,
                    &expected_instances,
                    reason,
                    &termination_detail(reason),
                );
                registry.release_query_resources(self.execution_id);
            }
            return Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::Terminated,
                "query lifecycle terminated before fragment admission commit",
            ));
        }
        if !matches!(
            state.phase,
            QueryLifecyclePhase::ControlAttached | QueryLifecyclePhase::Staging
        ) {
            return Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::Conflict,
                "query control is not ready for fragment admission commit",
            ));
        }
        if !state
            .in_flight_fragments
            .contains(&self.fragment_instance_id)
        {
            return Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::Conflict,
                "fragment admission permit is no longer in flight",
            ));
        }
        if registry_state
            .fragment_executions
            .contains_key(&self.fragment_instance_id)
        {
            state.in_flight_fragments.remove(&self.fragment_instance_id);
            return Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::Conflict,
                "fragment instance already belongs to a committed query lifecycle admission",
            ));
        }
        registry_state
            .fragment_executions
            .insert(self.fragment_instance_id, self.execution_id);
        state.in_flight_fragments.remove(&self.fragment_instance_id);
        state.accepted_fragments.insert(self.fragment_instance_id);
        // A staged worker is still pre-start. Only the StartPreparedQuery
        // transition clears this deadline after releasing the shared gate.
        if state.phase == QueryLifecyclePhase::ControlAttached {
            state.pre_start_deadline = None;
        }
        drop(state);
        drop(registry_state);
        self.committed = true;
        if query_lifecycle_test_markers_enabled() {
            eprintln!(
                "NOVAROCKS_QUERY_FRAGMENT_ACCEPTED execution_id={} process_id={} finst_id={} {}",
                format_execution_id(self.execution_id),
                self.registry
                    .upgrade()
                    .map(|registry| registry.local_process_id)
                    .map(|process_id| process_id.to_string())
                    .unwrap_or_else(|| "unavailable".to_owned()),
                self.fragment_instance_id,
                QueryExecutionDiagnostic::from(self.execution_id),
            );
        }
        Ok(())
    }
}

impl Drop for FragmentAdmissionPermit {
    fn drop(&mut self) {
        if !self.committed {
            self.entry
                .state
                .lock()
                .expect("query lifecycle entry lock")
                .in_flight_fragments
                .remove(&self.fragment_instance_id);
        }
    }
}

impl BackendQueryControl for RegistryQueryControl {
    fn heartbeat(&self, sequence: u64) -> Result<(), QueryLifecycleError> {
        self.registry
            .upgrade()
            .ok_or_else(|| internal_error("query lifecycle registry was dropped"))?
            .heartbeat(self.execution_id, sequence)
    }

    fn abort(&self, _reason: String) -> Result<(), QueryLifecycleError> {
        self.registry
            .upgrade()
            .ok_or_else(|| internal_error("query lifecycle registry was dropped"))?
            .terminate_from_control(
                self.execution_id,
                QueryTerminationReason::QueryTerminationCoordinatorAbort,
            )
    }

    fn finalize(&self) -> Result<(), QueryLifecycleError> {
        let registry = self
            .registry
            .upgrade()
            .ok_or_else(|| internal_error("query lifecycle registry was dropped"))?;
        match registry.finalize_from_control(self.execution_id) {
            Ok(()) => Ok(()),
            // QLC-3 callers may still finalize an attempt which never reached
            // Running.  Preserve their fail-close cleanup path; QLC-4 only
            // freezes a snapshot after LocalDrained.
            Err(error) if error.code() == QueryLifecycleErrorCode::Terminated => registry
                .terminate_from_control(
                    self.execution_id,
                    QueryTerminationReason::QueryTerminationCoordinatorFinalize,
                ),
            Err(error) => Err(error),
        }
    }

    fn terminal_ack(&self, ack: QueryTerminalAck) -> Result<(), QueryLifecycleError> {
        self.registry
            .upgrade()
            .ok_or_else(|| internal_error("query lifecycle registry was dropped"))?
            .terminal_ack_from_control(ack)
    }

    fn credential_lease_prepare(
        &self,
        envelope: novarocks_proto_codec::lifecycle::CredentialLeaseSecretEnvelope,
    ) -> Result<(), QueryLifecycleError> {
        self.registry
            .upgrade()
            .ok_or_else(|| internal_error("query lifecycle registry was dropped"))?
            .prepare_credential_lease(self.execution_id, envelope)
    }

    fn credential_lease_commit(
        &self,
        lease_id: novarocks_spi::connector::CredentialLeaseId,
        epoch: u64,
    ) -> Result<(), QueryLifecycleError> {
        self.registry
            .upgrade()
            .ok_or_else(|| internal_error("query lifecycle registry was dropped"))?
            .commit_credential_lease(self.execution_id, lease_id, epoch)
    }

    fn coordinator_lost(&self, reason: QueryTerminationReason) -> Result<(), QueryLifecycleError> {
        if query_lifecycle_test_markers_enabled() {
            let process_id = self
                .registry
                .upgrade()
                .map(|registry| registry.local_process_id)
                .map(|process_id| process_id.to_string())
                .unwrap_or_else(|| "unavailable".to_owned());
            eprintln!(
                "NOVAROCKS_QUERY_CONTROL_COORDINATOR_LOST execution_id={} process_id={} reason={reason:?} {}",
                format_execution_id(self.execution_id),
                process_id,
                QueryExecutionDiagnostic::from(self.execution_id),
            );
        }
        self.registry
            .upgrade()
            .ok_or_else(|| internal_error("query lifecycle registry was dropped"))?
            .terminate_from_control(self.execution_id, reason)
    }
}

fn format_execution_id(execution_id: QueryExecutionId) -> String {
    format!(
        "{}:{}:{}",
        execution_id.query_id().high(),
        execution_id.query_id().low(),
        execution_id.attempt_id().get()
    )
}

/// Diagnostic-only view of the process attribution carried by a received
/// lifecycle identity.  The backend does not allocate, classify, or retain
/// ownership of this namespace; it only renders the immutable wire value.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct QueryExecutionDiagnostic {
    attribution: Option<QueryIdAttribution>,
    attempt_id: u64,
}

impl From<QueryExecutionId> for QueryExecutionDiagnostic {
    fn from(execution_id: QueryExecutionId) -> Self {
        Self {
            attribution: execution_id.query_id().process_attribution(),
            attempt_id: execution_id.attempt_id().get(),
        }
    }
}

impl QueryExecutionDiagnostic {
    fn process_namespace(self) -> QueryDiagnosticValue<QueryProcessNamespace> {
        QueryDiagnosticValue(self.attribution.map(QueryIdAttribution::namespace))
    }

    fn local_sequence(self) -> QueryDiagnosticValue<LocalQuerySequence> {
        QueryDiagnosticValue(self.attribution.map(QueryIdAttribution::sequence))
    }

    const fn attempt_id(self) -> u64 {
        self.attempt_id
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct QueryDiagnosticValue<T>(Option<T>);

impl<T: Copy + fmt::Display> fmt::Display for QueryDiagnosticValue<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Some(value) => value.fmt(f),
            None => f.write_str("unattributed"),
        }
    }
}

impl fmt::Display for QueryExecutionDiagnostic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "query_process_namespace={} query_local_sequence={} query_attempt_id={}",
            self.process_namespace(),
            self.local_sequence(),
            self.attempt_id(),
        )
    }
}

#[cfg(debug_assertions)]
pub(super) fn query_lifecycle_test_markers_enabled() -> bool {
    novarocks_failpoint::configured_root().is_some()
}

#[cfg(not(debug_assertions))]
pub(super) fn query_lifecycle_test_markers_enabled() -> bool {
    false
}

impl QueryLifecycleIngress for QueryLifecycleRegistry {
    fn backend_process_id(&self) -> BackendProcessId {
        self.local_process_id()
    }

    fn init_query(&self, request: QueryInitRequest) -> QueryInitAck {
        QueryLifecycleRegistry::init_query(self, request)
    }

    fn init_query_tls(&self, request: QueryInitRequest) -> QueryInitAck {
        QueryLifecycleRegistry::init_query_tls(self, request)
    }

    fn prune_catalogs(
        &self,
        reachable: std::collections::BTreeSet<novarocks_spi::connector::CatalogHandle>,
    ) -> CatalogPruneOutcome {
        let outcome = match self.catalog_manager.prune_unreachable(&reachable) {
            crate::connector::catalog_manager::CatalogPruneResult::Pruned { .. } => {
                CatalogPruneOutcome::Accepted
            }
            crate::connector::catalog_manager::CatalogPruneResult::Rejected { .. } => {
                CatalogPruneOutcome::Rejected {
                    safe_detail: "catalog reachability snapshot omits one or more live catalogs"
                        .to_string(),
                }
            }
        };
        self.publish_catalog_lease_metrics();
        outcome
    }

    fn authorize_exchange(
        &self,
        destination_fragment_instance_id: UniqueId,
        destination_node_id: i32,
        source_fragment_instance_id: UniqueId,
        sender_ordinal: u32,
        sender_count: u32,
    ) -> Result<(), String> {
        QueryLifecycleRegistry::authorize_exchange(
            self,
            destination_fragment_instance_id,
            destination_node_id,
            source_fragment_instance_id,
            sender_ordinal,
            sender_count,
        )
    }

    fn stage_fragments(&self, request: QueryStageRequest) -> QueryStageAck {
        QueryLifecycleRegistry::stage_fragments(self, request)
    }

    fn start_prepared_query(&self, request: QueryStartRequest) -> QueryStartAck {
        QueryLifecycleRegistry::start_prepared_query(self, request)
    }

    fn abort_query(
        &self,
        request: QueryAbortRequest,
    ) -> Result<QueryTerminationAck, QueryLifecycleError> {
        QueryLifecycleRegistry::abort_query(self, request)
    }

    fn attach_control(
        &self,
        attach: QueryControlAttach,
    ) -> Result<QueryControlAttachment, QueryLifecycleError> {
        QueryLifecycleRegistry::attach_control(self, attach)
    }
}

fn fold_metrics_locked(
    state: &QueryLifecycleRegistryState,
) -> (BackendQueryLifecycleMetricsSnapshot, [u64; 6]) {
    let mut snapshot = BackendQueryLifecycleMetricsSnapshot {
        tombstones: state.tombstones.len(),
        admission_rejected: state.admission_rejected,
        init_conflicts: state.init_conflicts,
        heartbeat_timeouts: state.heartbeat_timeouts,
        terminations: state.terminations,
        terminal_facts: state.terminal_facts,
        terminal_locally_drained: state.terminal_locally_drained,
        terminal_records_frozen: state.terminal_records_frozen,
        terminal_acknowledged: state.terminal_acknowledged,
        terminal_retention_expired: state.terminal_retention_expired,
        terminal_fallback_accepted: state.terminal_fallback_accepted,
        terminal_fallback_rejected: state.terminal_fallback_rejected,
        terminal_retained: state.terminal_retained.len(),
        terminal_retained_bytes: state.terminal_retained_bytes,
        ..BackendQueryLifecycleMetricsSnapshot::default()
    };
    for entry in state.entries.values() {
        match entry
            .state
            .lock()
            .expect("query lifecycle entry lock")
            .phase
        {
            QueryLifecyclePhase::Initializing => snapshot.initializing += 1,
            QueryLifecyclePhase::Initialized => snapshot.initialized += 1,
            QueryLifecyclePhase::ControlAttached
            | QueryLifecyclePhase::Staging
            | QueryLifecyclePhase::Staged
            | QueryLifecyclePhase::Running
            | QueryLifecyclePhase::TerminalRetained => snapshot.control_attached += 1,
            QueryLifecyclePhase::Terminating => snapshot.terminating += 1,
            QueryLifecyclePhase::Tombstone => {}
        }
    }
    (snapshot, state.termination_reasons)
}

const fn phase_name(phase: QueryLifecyclePhase) -> &'static str {
    match phase {
        QueryLifecyclePhase::Initializing => "initializing",
        QueryLifecyclePhase::Initialized => "initialized",
        QueryLifecyclePhase::ControlAttached => "control_attached",
        QueryLifecyclePhase::Staging => "staging",
        QueryLifecyclePhase::Staged => "staged",
        QueryLifecyclePhase::Running => "running",
        QueryLifecyclePhase::TerminalRetained => "terminal_retained",
        QueryLifecyclePhase::Terminating => "terminating",
        QueryLifecyclePhase::Tombstone => "tombstone",
    }
}

fn termination_reason_index(reason: QueryTerminationReason) -> usize {
    match reason {
        QueryTerminationReason::QueryTerminationCoordinatorAbort => 0,
        QueryTerminationReason::QueryTerminationCoordinatorFinalize => 1,
        QueryTerminationReason::QueryTerminationCoordinatorStreamLost => 2,
        QueryTerminationReason::QueryTerminationCoordinatorHeartbeatTimeout => 3,
        QueryTerminationReason::QueryTerminationLocalFailure => 4,
        QueryTerminationReason::QueryTerminationPreStartTimeout => 5,
        QueryTerminationReason::Unspecified => 4,
    }
}

const fn termination_reason_name(reason: QueryTerminationReason) -> &'static str {
    match reason {
        QueryTerminationReason::QueryTerminationCoordinatorAbort => "CoordinatorAbort",
        QueryTerminationReason::QueryTerminationCoordinatorFinalize => "CoordinatorFinalize",
        QueryTerminationReason::QueryTerminationCoordinatorStreamLost => "CoordinatorStreamLost",
        QueryTerminationReason::QueryTerminationCoordinatorHeartbeatTimeout => {
            "CoordinatorHeartbeatTimeout"
        }
        QueryTerminationReason::QueryTerminationLocalFailure => "LocalFailure",
        QueryTerminationReason::QueryTerminationPreStartTimeout => "PreStartTimeout",
        QueryTerminationReason::Unspecified => "Unspecified",
    }
}

fn termination_detail(reason: QueryTerminationReason) -> String {
    format!(
        "query lifecycle terminated: {}",
        termination_reason_name(reason)
    )
}

fn format_digest(digest: ParticipantManifestDigest) -> String {
    use std::fmt::Write;

    let mut formatted = String::with_capacity(64);
    for byte in digest.as_bytes() {
        write!(&mut formatted, "{byte:02x}").expect("write digest to string");
    }
    formatted
}

#[allow(dead_code)]
fn internal_error(detail: impl Into<String>) -> QueryLifecycleError {
    QueryLifecycleError::new(QueryLifecycleErrorCode::Internal, detail)
}

#[cfg(test)]
mod query_execution_diagnostic_tests {
    use novarocks_proto_codec::lifecycle::{AttemptId, QueryExecutionId};
    use novarocks_types::QueryId;

    use super::QueryExecutionDiagnostic;

    fn execution_id(namespace: u64, sequence: i64, attempt: u64) -> QueryExecutionId {
        QueryExecutionId::new(
            QueryId::new(namespace as i64, sequence),
            AttemptId::new(attempt).expect("nonzero attempt"),
        )
        .expect("nonzero query id")
    }

    #[test]
    fn diagnostic_uses_received_identity_for_tombstone_stable_attribution() {
        let execution_id = execution_id(0xfeed_face_cafe_beef, 42, 3);
        let before_tombstone = QueryExecutionDiagnostic::from(execution_id).to_string();
        let after_tombstone = QueryExecutionDiagnostic::from(execution_id).to_string();

        assert_eq!(before_tombstone, after_tombstone);
        assert_eq!(
            after_tombstone,
            "query_process_namespace=0xfeedfacecafebeef query_local_sequence=42 query_attempt_id=3"
        );
    }

    #[test]
    fn diagnostic_distinguishes_received_process_namespaces() {
        let first = QueryExecutionDiagnostic::from(execution_id(0x10, 7, 1)).to_string();
        let second = QueryExecutionDiagnostic::from(execution_id(0x20, 7, 1)).to_string();

        assert_ne!(first, second);
        assert!(first.contains("query_process_namespace=0x0000000000000010"));
        assert!(second.contains("query_process_namespace=0x0000000000000020"));
    }

    #[test]
    fn diagnostic_refuses_to_guess_non_local_sequences() {
        assert_eq!(
            QueryExecutionDiagnostic::from(execution_id(0x20, 0, 1)).to_string(),
            "query_process_namespace=unattributed query_local_sequence=unattributed query_attempt_id=1"
        );
    }
}
