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

//! Frozen protocol facts and original payload ownership from one bound manifest.

use super::{
    bridge::{qa::AttemptQaBridge, root_control::PendingRootControls},
    creation::PricedTaskCreation,
    error::TaskExecutionError,
    graph::{TaskGraph, TaskPlanSource, build_task_graph_from_manifest},
    host::transport::{AttemptEstablishMaterial, FrozenSendRequests},
    protocol_facts::{AttemptProtocolFactsPublisher, AttemptProtocolFactsReceiver},
    sources::ParticipationEstablishSource,
};
use crate::{
    control_runtime::membership::FrozenMember, query_execution::artifact::TaskManifestBinding,
};
use novarocks_attempt_protocol::{
    config::{CoordinationBudgets, FrozenTransportLimits, TaskUpdateRetryPolicy},
    effect::PumpKind,
    ids::IdSeed,
    start::{AttemptSources, AttemptStart, FrozenAttemptPlan, PayloadRef},
};
use novarocks_coordination_contract::{
    AttemptActivation, AttemptCompletionPolicy, AttemptInheritedAdmission, AttemptLifecyclePolicy,
    MonotonicInstant,
};
use novarocks_execution_contract::BackendProcessDescriptor;
use novarocks_execution_contract::{AdmissionEpochCapability, ParticipationRef, TaskIdentity};
use novarocks_query_application::api::PlanSeal;
use novarocks_query_application::coordination::{
    AcceptedRootStatusSender, AcceptedRootStatusSource, NativeAttemptDrive,
    ReplacementWorkerAdmissionEvidence,
};
use novarocks_result_contract::PerKind;
use novarocks_task_codec::TransportBudget;
use novarocks_types::{BackendProcessId, NativeCompatibilityId};
use std::{
    collections::{BTreeMap, BTreeSet},
    num::{NonZeroU64, NonZeroUsize},
    sync::Arc,
    time::Duration,
};

/// Facts owned by the projected attempt across the handoff to the original QA drive.
pub(crate) struct ManifestAttemptInputs {
    pub(crate) plan: PlanSeal,
    pub(crate) graph: TaskGraph,
    pub(crate) compatibility: NativeCompatibilityId,
    pub(crate) transport: TransportBudget,
    pub(crate) writers: BTreeSet<TaskIdentity>,
    pub(crate) summary_sources: PerKind<BTreeSet<TaskIdentity>>,
    pub(crate) admission_epochs: BTreeMap<BackendProcessId, AdmissionEpochCapability>,
    pub(crate) preparing_positions: BTreeMap<BackendProcessId, usize>,
    pub(crate) members: Arc<[FrozenMember]>,
    pub(crate) backends: Arc<[BackendProcessDescriptor]>,
}

impl ManifestAttemptInputs {
    pub(crate) fn freeze(
        manifest: &TaskManifestBinding,
        plans: &mut dyn TaskPlanSource,
        compatibility: NativeCompatibilityId,
        transport: TransportBudget,
    ) -> Result<Self, TaskExecutionError> {
        let graph = build_task_graph_from_manifest(manifest, plans, transport)?;
        let writers = manifest
            .writer_cohorts()
            .map(|cohort| cohort.task())
            .collect();
        let mut summary_sources = PerKind::empty();
        for (kind, task, _) in manifest.summary_sources() {
            if summary_sources.get(kind).is_none() {
                summary_sources.insert(kind, BTreeSet::new());
            }
            summary_sources
                .get_mut(kind)
                .expect("declared kind")
                .insert(task);
        }
        let mut admission_epochs = BTreeMap::new();
        let mut preparing_positions = BTreeMap::new();
        let mut members = Vec::with_capacity(manifest.participations().len());
        let mut backends = Vec::with_capacity(manifest.participations().len());
        for participation in manifest.participations() {
            let backend = participation.backend();
            if backend.native_compatibility_id() != compatibility {
                return Err(TaskExecutionError::Schedule(format!(
                    "manifest backend {} compatibility differs from the attempt wire identity",
                    backend.process_id()
                )));
            }
            let descriptor = backend.target().descriptor();
            preparing_positions.insert(backend.process_id(), descriptor.preparing_positions());
            if admission_epochs
                .insert(backend.process_id(), backend.admission_epoch_capability())
                .is_some()
            {
                return Err(TaskExecutionError::Schedule(format!(
                    "manifest repeats admission authority for backend {}",
                    backend.process_id()
                )));
            }
            members.push(FrozenMember {
                participation: participation.participation(),
                endpoint: descriptor.endpoint().clone(),
            });
            backends.push(descriptor.clone());
        }
        members.sort_unstable_by_key(|member| member.participation);
        Ok(Self {
            plan: manifest.plan(),
            graph,
            compatibility,
            transport,
            writers,
            summary_sources,
            admission_epochs,
            preparing_positions,
            members: members.into(),
            backends: backends.into(),
        })
    }

    /// Reserve the one public status and protocol-fact projections while the
    /// dormant owner still holds the same frozen Native manifest. The active
    /// owner will bind their publishers to the original QA drive.
    pub(crate) fn prepare_publications(
        self,
    ) -> Result<PreparedManifestAttempt, TaskExecutionError> {
        let (protocol, protocol_facts) = AttemptProtocolFactsPublisher::for_graph(
            self.plan,
            &self.graph,
            &self.writers,
            &self.summary_sources,
        )?;
        let (root_sender, root_status, root_controls) =
            PendingRootControls::project(self.graph.root_identity());
        Ok(PreparedManifestAttempt {
            inputs: self,
            publications: ManifestPublications {
                root_sender,
                root_status: Some(root_status),
                root_controls,
                protocol,
                protocol_facts: Some(protocol_facts),
            },
        })
    }
}

pub(crate) struct ManifestPublications {
    pub(crate) root_sender: AcceptedRootStatusSender,
    pub(crate) root_status: Option<AcceptedRootStatusSource>,
    pub(crate) root_controls: Arc<PendingRootControls>,
    pub(in crate::task_execution) protocol: AttemptProtocolFactsPublisher,
    pub(crate) protocol_facts: Option<AttemptProtocolFactsReceiver>,
}

pub(crate) struct PreparedManifestAttempt {
    inputs: ManifestAttemptInputs,
    pub(crate) publications: ManifestPublications,
}

impl PreparedManifestAttempt {
    pub(crate) fn participants(&self) -> usize {
        self.inputs.members.len()
    }

    pub(crate) fn graph(&self) -> &TaskGraph {
        &self.inputs.graph
    }
}

pub(crate) struct AttemptPlanPolicy {
    pub(crate) coordination: CoordinationBudgets,
    pub(crate) retry: TaskUpdateRetryPolicy,
    pub(crate) initial_lease: Duration,
    pub(crate) steady_lease: Duration,
}

pub(crate) struct AttemptActivationInputs<'a> {
    pub(crate) drive: &'a NativeAttemptDrive,
    pub(crate) replacement_admissions: Option<&'a [ReplacementWorkerAdmissionEvidence]>,
    pub(crate) ids: IdSeed,
    pub(crate) initial_now: MonotonicInstant,
    pub(crate) sources: AttemptSources,
    pub(crate) retain_diagnostics: bool,
    pub(crate) observe_dev_root: bool,
    pub(crate) observe_lifecycle_diagnostics: bool,
}

pub(crate) struct AttemptManifestProjection {
    pub(crate) start: AttemptStart,
    pub(crate) members: Arc<[FrozenMember]>,
    #[cfg(test)]
    pub(crate) backends: Arc<[BackendProcessDescriptor]>,
    pub(crate) publications: ManifestPublications,
    pub(crate) creations: BTreeMap<PayloadRef, PricedTaskCreation>,
    pub(crate) requests: FrozenSendRequests,
    pub(crate) qa: AttemptQaBridge,
}

impl AttemptManifestProjection {
    /// Move the already bound graph's creation seeds and retain the Establish
    /// backing with one payload identity sequence. Core receives only the
    /// frozen protocol projection.
    pub(crate) fn from_prepared_manifest(
        prepared: PreparedManifestAttempt,
        establish: &dyn ParticipationEstablishSource,
        activation: AttemptActivationInputs<'_>,
        policy: AttemptPlanPolicy,
        pumps: Box<[(PayloadRef, PumpKind)]>,
        next_payload: &mut NonZeroU64,
    ) -> Result<Self, TaskExecutionError> {
        let ManifestAttemptInputs {
            graph,
            compatibility,
            transport,
            writers,
            summary_sources,
            admission_epochs,
            preparing_positions,
            members,
            #[cfg(test)]
            backends,
            ..
        } = prepared.inputs;
        let identity = activation.drive.identity();
        if identity.execution() != graph.execution_id() {
            return Err(TaskExecutionError::Schedule(
                "attempt projection differs from the original bound manifest".into(),
            ));
        }
        let inherited_admissions: Box<[_]> = match activation.replacement_admissions {
            None => Box::new([]),
            Some(admissions) => {
                validate_replacement_admissions(graph.participations().copied(), admissions)?;
                admissions
                    .iter()
                    .map(|admission| AttemptInheritedAdmission {
                        participation: admission.participation(),
                        receipt: admission.receipt(),
                    })
                    .collect()
            }
        };
        let window = policy.coordination.dispatch.create_permits();
        let deployment_window = NonZeroUsize::new(window).ok_or_else(|| {
            TaskExecutionError::Schedule("the attempt deployment window is empty".into())
        })?;
        let preparation_positions = preparing_positions
            .into_iter()
            .map(|(backend, positions)| {
                NonZeroUsize::new(positions)
                    .filter(|positions| *positions >= deployment_window)
                    .map(|positions| (backend, positions))
                    .ok_or_else(|| {
                        TaskExecutionError::Schedule(format!(
                            "backend {backend} cannot cover the configured deployment window"
                        ))
                    })
            })
            .collect::<Result<_, _>>()?;
        let mut next = *next_payload;
        let (graph, creations) = graph.into_attempt_graph(&mut next)?;
        let (establish_material, establish) = AttemptEstablishMaterial::project(
            establish,
            members.iter().map(|member| member.participation),
            &mut next,
        )?;
        let establish = Arc::new(establish);
        let start = AttemptStart {
            activation: AttemptActivation {
                identity,
                policy: AttemptLifecyclePolicy {
                    completion: AttemptCompletionPolicy::AcceptedRootSuccessSeal,
                    retain_diagnostics: activation.retain_diagnostics,
                },
                inherited_admissions,
            },
            plan: FrozenAttemptPlan {
                graph,
                authorization_limits: activation.drive.authorization_limits(),
                writers,
                summary_sources,
                compatibility,
                admission_epochs,
                preparation_positions,
                deployment_window,
                establish_material,
                coordination: policy.coordination,
                transport: FrozenTransportLimits {
                    max_batch_items: transport.max_batch_items(),
                    max_batch_encoded_bytes: transport.max_batch_encoded_bytes(),
                    max_descriptor_encoded_bytes: transport.max_descriptor_encoded_bytes(),
                    max_query_backend_queued_operations: transport
                        .max_query_backend_queued_operations(),
                    max_query_backend_queued_bytes: transport.max_query_backend_queued_bytes(),
                    max_backend_queued_operations: transport.max_backend_queued_operations(),
                    max_backend_queued_bytes: transport.max_backend_queued_bytes(),
                    max_tasks_per_participation: transport.max_tasks_per_participation(),
                    max_active_tasks_per_backend: transport.max_active_tasks_per_backend(),
                    frontend_queue_residence: transport.frontend_queue_residence(),
                },
                retry: policy.retry,
                pumps,
                initial_lease: policy.initial_lease,
                steady_lease: policy.steady_lease,
            },
            ids: activation.ids,
            initial_now: activation.initial_now,
            sources: activation.sources,
            observe_dev_root: activation.observe_dev_root,
            observe_lifecycle_diagnostics: activation.observe_lifecycle_diagnostics,
        };
        *next_payload = next;
        Ok(Self {
            start,
            members,
            #[cfg(test)]
            backends,
            publications: prepared.publications,
            creations,
            requests: FrozenSendRequests::with_establish(establish.clone()),
            qa: AttemptQaBridge::new(Arc::new(activation.drive.clone()), compatibility, establish),
        })
    }
}

/// Project only the qualified receipts already retained by the logical owner.
pub(crate) fn validate_replacement_admissions(
    mut expected: impl ExactSizeIterator<Item = ParticipationRef>,
    admissions: &[ReplacementWorkerAdmissionEvidence],
) -> Result<(), TaskExecutionError> {
    let actual = admissions
        .iter()
        .map(ReplacementWorkerAdmissionEvidence::participation)
        .collect::<BTreeSet<_>>();
    if admissions.len() != actual.len()
        || expected.len() != actual.len()
        || expected.any(|participant| !actual.contains(&participant))
    {
        return Err(TaskExecutionError::Schedule(
            "qualified replacement admission set differs from the Task manifest participations"
                .into(),
        ));
    }
    Ok(())
}
