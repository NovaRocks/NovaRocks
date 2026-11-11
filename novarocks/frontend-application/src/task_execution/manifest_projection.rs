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
    creation::PricedTaskCreation,
    error::TaskExecutionError,
    graph::{TaskGraph, TaskPlanSource, build_task_graph_from_manifest},
    host::transport::AttemptEstablishMaterial,
    participation_owner::ParticipationEstablishSource,
};
use crate::query_execution::artifact::TaskManifestBinding;
use novarocks_attempt_protocol::{
    config::{CoordinationBudgets, FrozenTransportLimits, TaskUpdateRetryPolicy},
    effect::PumpKind,
    start::{FrozenAttemptPlan, PayloadRef},
};
use novarocks_coordination_contract::AttemptAuthorizationLimits;
use novarocks_execution_contract::{AdmissionEpochCapability, TaskIdentity};
use novarocks_result_contract::PerKind;
use novarocks_task_codec::TransportBudget;
use novarocks_types::{BackendProcessId, NativeCompatibilityId};
use std::{
    collections::{BTreeMap, BTreeSet},
    num::{NonZeroU64, NonZeroUsize},
    sync::Arc,
    time::Duration,
};

pub(crate) struct ManifestAttemptInputs<'a> {
    pub(crate) manifest: &'a TaskManifestBinding,
    pub(crate) graph: TaskGraph,
    pub(crate) compatibility: NativeCompatibilityId,
    pub(crate) transport: TransportBudget,
    pub(crate) writers: BTreeSet<TaskIdentity>,
    pub(crate) summary_sources: PerKind<BTreeSet<TaskIdentity>>,
    pub(crate) admission_epochs: BTreeMap<BackendProcessId, AdmissionEpochCapability>,
    pub(crate) preparing_positions: BTreeMap<BackendProcessId, usize>,
}

impl<'a> ManifestAttemptInputs<'a> {
    pub(crate) fn freeze(
        manifest: &'a TaskManifestBinding,
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
        for participation in manifest.participations() {
            let backend = participation.backend();
            if backend.native_compatibility_id() != compatibility {
                return Err(TaskExecutionError::Schedule(format!(
                    "manifest backend {} compatibility differs from the attempt wire identity",
                    backend.process_id()
                )));
            }
            preparing_positions.insert(
                backend.process_id(),
                backend.target().descriptor().preparing_positions(),
            );
            if admission_epochs
                .insert(backend.process_id(), backend.admission_epoch_capability())
                .is_some()
            {
                return Err(TaskExecutionError::Schedule(format!(
                    "manifest repeats admission authority for backend {}",
                    backend.process_id()
                )));
            }
        }
        Ok(Self {
            manifest,
            graph,
            compatibility,
            transport,
            writers,
            summary_sources,
            admission_epochs,
            preparing_positions,
        })
    }
}

pub(crate) struct AttemptPlanPolicy {
    pub(crate) authorization_limits: AttemptAuthorizationLimits,
    pub(crate) coordination: CoordinationBudgets,
    pub(crate) retry: TaskUpdateRetryPolicy,
    pub(crate) initial_lease: Duration,
    pub(crate) steady_lease: Duration,
}

pub(crate) struct AttemptManifestProjection {
    pub(crate) plan: FrozenAttemptPlan,
    pub(crate) creations: BTreeMap<PayloadRef, PricedTaskCreation>,
    pub(crate) establish: Arc<AttemptEstablishMaterial>,
}

impl AttemptManifestProjection {
    /// Move the already bound graph's creation seeds and retain the Establish
    /// backing with one payload identity sequence. Core receives only the
    /// frozen protocol projection.
    pub(crate) fn from_manifest_inputs(
        inputs: ManifestAttemptInputs<'_>,
        establish: &dyn ParticipationEstablishSource,
        policy: AttemptPlanPolicy,
        pumps: Box<[(PayloadRef, PumpKind)]>,
        next_payload: &mut NonZeroU64,
    ) -> Result<Self, TaskExecutionError> {
        let ManifestAttemptInputs {
            manifest,
            graph,
            compatibility,
            transport,
            writers,
            summary_sources,
            admission_epochs,
            preparing_positions,
        } = inputs;
        if policy.authorization_limits.activation.execution() != manifest.execution() {
            return Err(TaskExecutionError::Schedule(
                "attempt projection differs from the original bound manifest".into(),
            ));
        }
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
        let participations = graph.participations().copied().collect::<Vec<_>>();
        let (graph, creations) = graph.into_attempt_graph(&mut next)?;
        let (establish_material, establish) =
            AttemptEstablishMaterial::project(establish, participations, &mut next)?;
        *next_payload = next;
        Ok(Self {
            plan: FrozenAttemptPlan {
                graph,
                authorization_limits: policy.authorization_limits,
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
            creations,
            establish: Arc::new(establish),
        })
    }
}
