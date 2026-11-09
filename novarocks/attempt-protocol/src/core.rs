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

//! The serial owner of the frozen attempt's protocol components.

use crate::{
    capacity::StepContext,
    convergence::RemoteConvergence,
    diagnostics::controller::DiagnosticCollector,
    effect::EffectBody,
    error::TaskExecutionError,
    execution::{QueryTaskExecution, ack_phase::NativeAckPhase},
    failure::AttemptFailure,
    graph::TaskGraph,
    ids::OperationIds,
    input::{
        AttemptInputId, AttemptSourceId, ExecutorFact, NativeSourceCut, ObservationBatch,
        OperationObservation, PublicationApplied,
    },
    lease::RequestedLeaseDurations,
    observation::controller::{CoveredSubscription, ObservationDisposition, SubscriptionStatus},
    publication::{
        ProtocolPublication,
        protocol_cursor::{
            ProtocolPublicationCursor, PublicationApplyDisposition, PublicationProgress,
        },
    },
    root_control::{RootControl, SealSettlement},
    start::{AttemptSources, AttemptStart, FrozenAttemptPlan},
    terminal::TerminalController,
};
use novarocks_coordination_contract::{
    AttemptActionId, AttemptActivationIdentity, AttemptAuthorizationLimits, AttemptRequestToken,
    AttemptRootFailure, AttemptRootProjection, LogicalToAttempt, LogicalToAttemptBody,
    MonotonicInstant,
};
use novarocks_execution_contract::ParticipationRef;
use std::collections::BTreeMap;

#[derive(Debug)]
pub(crate) struct Core {
    activation: AttemptActivationIdentity,
    observation_source: AttemptSourceId,
    execution: QueryTaskExecution,
    acknowledgements: NativeAckPhase,
    subscriptions: BTreeMap<ParticipationRef, CoveredSubscription>,
    root: RootControl,
    publication: ProtocolPublicationCursor,
    terminal: TerminalController,
    convergence: RemoteConvergence,
    diagnostics: DiagnosticCollector,
    authorization_limits: AttemptAuthorizationLimits,
    establish_material: BTreeMap<ParticipationRef, crate::start::FrozenEstablish>,
    retry: crate::config::TaskUpdateRetryPolicy,
    pumps: Box<[(crate::start::PayloadRef, crate::effect::PumpKind)]>,
    writers: std::collections::BTreeSet<novarocks_execution_contract::TaskIdentity>,
    summary_sources: novarocks_result_contract::PerKind<
        std::collections::BTreeSet<novarocks_execution_contract::TaskIdentity>,
    >,
}

/// Preserve the business projection selected at the same serial cut as the
/// seal. A later frame in this batch may advance diagnostics or convergence.
#[derive(Debug)]
pub(crate) struct SealedRootCut {
    pub(crate) root_before_seal: Option<AttemptRootProjection>,
    pub(crate) settlement: SealSettlement,
}

#[derive(Debug)]
pub(crate) struct RootControlApplication {
    pub(crate) seal: Option<SealedRootCut>,
    pub(crate) terminal_control: Option<(AttemptRequestToken, Result<(), TaskExecutionError>)>,
}

impl Core {
    pub(crate) fn new(start: AttemptStart) -> Result<Self, AttemptFailure> {
        Self::from_start(start).map_err(|error| AttemptFailure::TaskProtocol(Box::new(error)))
    }

    fn from_start(start: AttemptStart) -> Result<Self, TaskExecutionError> {
        let AttemptStart {
            activation,
            plan,
            ids,
            initial_now,
            sources:
                AttemptSources {
                    native,
                    observation,
                    mailbox,
                },
        } = start;
        let FrozenAttemptPlan {
            graph,
            authorization_limits,
            writers,
            summary_sources,
            compatibility,
            admission_epochs,
            preparation_positions,
            deployment_window,
            establish_material,
            coordination,
            transport,
            retry,
            pumps,
            initial_lease,
            steady_lease,
        } = plan;
        let graph = TaskGraph::from_frozen(graph)?;
        if native == observation
            || authorization_limits.activation != activation.identity
            || deployment_window.get() != coordination.dispatch.create_permits()
            || !establish_material.keys().eq(graph.participations())
        {
            return Err(schedule(
                "attempt registration differs from the frozen protocol plan",
            ));
        }
        let mut split_sources = pumps.iter().filter_map(|(source, kind)| {
            matches!(kind, crate::effect::PumpKind::SplitAssignment).then_some(*source)
        });
        let split_source = split_sources.next();
        if split_sources.next().is_some() {
            return Err(schedule("more than one original split profile Source"));
        }
        let root = RootControl::new(activation.identity, &graph)?;
        let publication = ProtocolPublicationCursor::new(&graph);
        let convergence = RemoteConvergence::new(&graph);
        let diagnostics = DiagnosticCollector::new(
            activation.identity,
            graph.root_identity(),
            activation.policy.retain_diagnostics,
            graph.tasks().map(|task| task.identity()),
        );
        let subscriptions = graph
            .participations()
            .map(|&participation| {
                CoveredSubscription::new(
                    participation,
                    ids,
                    coordination.status_subscription_error_budget,
                )
                .map(|subscription| (participation, subscription))
            })
            .collect::<Result<_, _>>()?;
        let preparing_positions = preparation_positions
            .into_iter()
            .map(|(process, positions)| (process, positions.get()))
            .collect();
        let mut execution = QueryTaskExecution::new_with_split_profile_source(
            graph,
            coordination.dispatch,
            transport,
            compatibility,
            &admission_epochs,
            &preparing_positions,
            initial_now,
            OperationIds::new(ids),
            RequestedLeaseDurations::new(initial_lease, steady_lease),
            split_source,
        )?;
        if !activation.inherited_admissions.is_empty() {
            execution.adopt_replacement_admissions(&activation.inherited_admissions)?;
        }
        execution.activate_covered_observation();
        let acknowledgements = NativeAckPhase::new(native, mailbox)
            .map_err(|_| schedule("invalid Native acknowledgement mailbox geometry"))?;
        Ok(Self {
            activation: activation.identity,
            observation_source: observation,
            execution,
            acknowledgements,
            subscriptions,
            root,
            publication,
            terminal: TerminalController::new(),
            convergence,
            diagnostics,
            authorization_limits,
            establish_material,
            retry,
            pumps,
            writers,
            summary_sources,
        })
    }

    /// Called after pending split input and already requested close are advanced.
    pub(crate) fn begin_native_prefix(
        &mut self,
        now: MonotonicInstant,
        cut: NativeSourceCut,
    ) -> Result<(), TaskExecutionError> {
        self.execution.set_now(now)?;
        self.acknowledgements
            .begin(cut)
            .map_err(|_| schedule("invalid Native acknowledgement cut"))
    }

    pub(crate) fn apply_native(
        &mut self,
        now: MonotonicInstant,
        source: AttemptSourceId,
        input: AttemptInputId,
        fact: &ExecutorFact,
        observe_abort: impl FnOnce(
            &mut QueryTaskExecution,
            &OperationObservation,
        ) -> Result<(), TaskExecutionError>,
    ) -> Result<Option<EffectBody>, TaskExecutionError> {
        self.execution.set_now(now)?;
        self.acknowledgements
            .apply(source, input, fact, &mut self.execution, observe_abort)
            .map_err(|_| schedule("invalid fact in the Native acknowledgement prefix"))
    }

    fn verify_common_observation_phase(
        &self,
        source: AttemptSourceId,
    ) -> Result<(), TaskExecutionError> {
        if source != self.observation_source {
            return Err(schedule("ordered observation used a foreign Source"));
        }
        if self.acknowledgements.run_round_tail() != Ok(true)
            || !self.publication.permits_protocol_turn()
        {
            return Err(schedule(
                "ordered observation crossed an unfinished round phase",
            ));
        }
        Ok(())
    }

    /// The whole bounded primary claim has already been charged. Reconsider
    /// the pending seal after each frame, before applying the next one.
    pub(crate) fn apply_observations(
        &mut self,
        now: MonotonicInstant,
        source: AttemptSourceId,
        batch: ObservationBatch,
    ) -> Result<Option<SealedRootCut>, TaskExecutionError> {
        self.verify_common_observation_phase(source)?;
        self.execution.set_now(now)?;
        let mut sealed = None;
        let mut first_error = None;
        for observation in batch.into_items() {
            let result = self
                .subscriptions
                .get_mut(&observation.participation)
                .ok_or(TaskExecutionError::UnknownOperation)
                .and_then(|subscription| {
                    let previous = subscription.status();
                    let disposition = subscription.observe(observation)?;
                    if let SubscriptionStatus::Fatal(failure) = subscription.status()
                        && previous != SubscriptionStatus::Fatal(failure)
                    {
                        return Err(TaskExecutionError::NativeTransport(failure.cause));
                    }
                    Ok(disposition)
                })
                .and_then(|disposition| match disposition {
                    ObservationDisposition::ApplyEntity(accepted) => self
                        .execution
                        .apply_covered_observation(accepted)
                        .map(|_| ()),
                    ObservationDisposition::Ignore => Ok(()),
                });
            if let Err(error) = result {
                first_error.get_or_insert(error);
            }
            if first_error.is_none()
                && let Some(seal) = self.evaluate_pending_seal()
            {
                sealed = Some(seal);
            }
        }
        if let Some(error) = first_error {
            self.acknowledgements
                .fail_round_tail(error)
                .expect("an observation batch was admitted at the round tail");
        }
        Ok(sealed)
    }

    /// A host may acknowledge the original root-control claim only after
    /// this first decision, including when the same seal remains pending.
    pub(crate) fn apply_root_control(
        &mut self,
        now: MonotonicInstant,
        source: AttemptSourceId,
        input: LogicalToAttempt<AttemptFailure>,
    ) -> Result<RootControlApplication, TaskExecutionError> {
        self.verify_common_observation_phase(source)?;
        if input.activation != self.activation {
            return Err(schedule("root control used a foreign activation"));
        }
        self.execution.set_now(now)?;
        let terminal_control = match input.body {
            LogicalToAttemptBody::SealRequest { token } => {
                self.root.request(token)?;
                None
            }
            LogicalToAttemptBody::TerminalControlDemand { token, root } => {
                Some((token, self.execution.require_result_terminal_control(root)))
            }
            _ => {
                return Err(schedule(
                    "ordered root-control Source received an unrelated port message",
                ));
            }
        };
        Ok(RootControlApplication {
            seal: self.evaluate_pending_seal(),
            terminal_control,
        })
    }

    /// Called at the old round's publication point after the Root effects.
    pub(crate) fn select_protocol_publication(&mut self) -> Result<bool, TaskExecutionError> {
        if self.acknowledgements.run_round_tail() != Ok(true) {
            return Err(schedule(
                "protocol publication crossed an unfinished round phase",
            ));
        }
        self.publication
            .select(&mut self.execution, self.root.sealed())
    }

    pub(crate) fn advance_protocol_publication(
        &mut self,
        context: &mut dyn StepContext,
    ) -> Result<PublicationProgress, TaskExecutionError> {
        self.publication.advance(&self.execution, context)
    }

    /// Take only once the host can accept the publication and its result.
    pub(crate) fn take_protocol_publication(
        &mut self,
        effect: AttemptActionId,
    ) -> Option<ProtocolPublication> {
        self.publication.take_ready(effect)
    }

    pub(crate) fn protocol_publication_applied(
        &mut self,
        effect: AttemptActionId,
        result: Result<PublicationApplied, AttemptFailure>,
    ) -> PublicationApplyDisposition {
        match result {
            Ok(PublicationApplied::ProtocolBatch { published }) => {
                self.publication.applied(effect, Ok(published))
            }
            Err(failure) => self.publication.applied(effect, Err(failure)),
            Ok(_) => PublicationApplyDisposition::Stale,
        }
    }

    fn evaluate_pending_seal(&mut self) -> Option<SealedRootCut> {
        let settlement = self.root.evaluate(self.execution.root_seal_facts(false))?;
        let root_before_seal = if settlement.result.is_ok() {
            let projection = self.root_projection();
            self.execution.begin_normal_drain();
            projection
        } else {
            None
        };
        Some(SealedRootCut {
            root_before_seal,
            settlement,
        })
    }

    fn root_projection(&self) -> Option<AttemptRootProjection> {
        let root = self.execution.graph().root_identity();
        let task = self.execution.task(root.task_id())?;
        let status = task.status()?;
        if !task.installed() && !status.is_terminal() {
            return None;
        }
        let failure = match self.execution.selected_task_failure() {
            Some(selected) if selected.detail().is_derived() => AttemptRootFailure::DerivedPending,
            Some(selected) => AttemptRootFailure::Authoritative(selected.clone()),
            None => AttemptRootFailure::None,
        };
        Some(AttemptRootProjection {
            status: status.clone(),
            failure,
        })
    }
}

fn schedule(detail: &str) -> TaskExecutionError {
    TaskExecutionError::Schedule(detail.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        capacity::QueueStage,
        config::{CoordinationBudgets, DispatchLane, FrozenTransportLimits},
        ids::IdSeed,
        input::{ObservationEvent, ObservationFact, ObservationOpening, ReceivedObservation},
        observation::controller::SubscriptionDirective,
        root_control::RootSealRefusal,
        start::{FrozenEstablish, PayloadRef},
    };
    use novarocks_coordination_contract::{
        AttemptActivation, AttemptCompletionPolicy, AttemptLifecyclePolicy,
    };
    use novarocks_execution_contract::{
        AbortCause, AdmissionEpochCapability, TaskIdentity, TaskOutputFacts, TaskState, TaskStatus,
        TaskStatusVersion, TerminationDetail,
    };
    use novarocks_types::{BackendProcessId, NativeCompatibilityId};
    use std::{
        num::{NonZeroU64, NonZeroUsize},
        time::Duration,
    };

    fn id(value: u64) -> NonZeroU64 {
        NonZeroU64::new(value).unwrap()
    }

    fn start() -> AttemptStart {
        let (graph, identity) = crate::test_support::frozen_graph_with_strict_input(false);
        let mut admission_epochs = BTreeMap::new();
        let mut preparation_positions = BTreeMap::new();
        let mut establish_material = BTreeMap::new();
        let coordination = CoordinationBudgets::DEFAULT;
        let deployment_window = NonZeroUsize::new(coordination.dispatch.create_permits()).unwrap();
        for (index, task) in graph.tasks.values().enumerate() {
            let backend = task.participation.backend_process_id();
            admission_epochs.insert(
                backend,
                AdmissionEpochCapability::try_from_bytes([0x61; 16]).unwrap(),
            );
            preparation_positions.insert(backend, deployment_window);
            establish_material.insert(
                task.participation,
                FrozenEstablish {
                    material: PayloadRef(id(index as u64 + 20)),
                    queued_bytes: 256,
                },
            );
        }
        AttemptStart {
            activation: AttemptActivation {
                identity,
                policy: AttemptLifecyclePolicy {
                    completion: AttemptCompletionPolicy::AcceptedRootSuccessSeal,
                    retain_diagnostics: true,
                },
                inherited_admissions: Box::new([]),
            },
            plan: FrozenAttemptPlan {
                graph,
                authorization_limits: AttemptAuthorizationLimits {
                    activation: identity,
                    establish_per_participation: NonZeroUsize::new(3).unwrap(),
                },
                writers: Default::default(),
                summary_sources: Default::default(),
                compatibility: NativeCompatibilityId::new([0x41; 32]),
                admission_epochs,
                preparation_positions,
                deployment_window,
                establish_material,
                coordination,
                transport: FrozenTransportLimits {
                    max_batch_items: 4,
                    max_batch_encoded_bytes: 4096,
                    max_descriptor_encoded_bytes: 4096,
                    max_query_backend_queued_operations: 1024,
                    max_query_backend_queued_bytes: 1_048_576,
                    max_backend_queued_operations: 4096,
                    max_backend_queued_bytes: 4_194_304,
                    max_tasks_per_participation: 1024,
                    max_active_tasks_per_backend: 1024,
                    frontend_queue_residence: Duration::from_secs(5),
                },
                retry: Default::default(),
                pumps: Box::new([]),
                initial_lease: Duration::from_secs(30),
                steady_lease: Duration::from_secs(5),
            },
            ids: IdSeed::new(1, 100),
            initial_now: MonotonicInstant::from_origin(Duration::from_secs(2)),
            sources: AttemptSources {
                native: AttemptSourceId(id(7)),
                observation: AttemptSourceId(id(8)),
                mailbox: [4; 4],
            },
        }
    }

    fn round() -> Core {
        let start = start();
        let source = start.sources.native;
        let now = start.initial_now;
        let mut core = Core::new(start).unwrap();
        assert_eq!(core.execution.monotonic_now(), now);
        core.begin_native_prefix(now, NativeSourceCut { source, tail: None })
            .unwrap();
        core
    }

    fn seal(core: &mut Core) -> Result<RootControlApplication, TaskExecutionError> {
        core.apply_root_control(
            core.execution.monotonic_now(),
            core.observation_source,
            LogicalToAttempt {
                activation: core.activation,
                body: LogicalToAttemptBody::SealRequest {
                    token: AttemptRequestToken::new(id(90)),
                },
            },
        )
    }

    fn frame(
        core: &mut Core,
        task: TaskIdentity,
        state: TaskState,
        termination: Option<TerminationDetail>,
    ) -> ReceivedObservation {
        let participation = core
            .execution
            .graph()
            .task(task.task_id())
            .unwrap()
            .participation();
        let now = core.execution.monotonic_now();
        let subscription = core.subscriptions.get_mut(&participation).unwrap();
        subscription.activate();
        let Some(SubscriptionDirective::Open { generation }) =
            subscription.drive(now, id(1), None).unwrap()
        else {
            panic!("expected the exact original subscription opening");
        };
        ReceivedObservation {
            participation,
            generation,
            received_at: now,
            opening: ObservationOpening {
                started_at: now,
                opened_at: Some(now),
            },
            event: ObservationEvent::Fact {
                source_revision: None,
                fact: ObservationFact::Status(
                    TaskStatus::try_new(
                        task,
                        TaskStatusVersion::new(1).unwrap(),
                        state,
                        termination,
                        TaskOutputFacts::new(state == TaskState::Finished),
                    )
                    .unwrap(),
                ),
            },
        }
    }

    #[test]
    fn common_source_reconsiders_pending_seal_before_the_next_frame() {
        for failure_first in [false, true] {
            let mut core = round();
            assert!(seal(&mut core).unwrap().seal.is_none());
            let root = core.execution.graph().root_identity();
            let producer = core
                .execution
                .graph()
                .tasks()
                .find(|task| task.identity() != root)
                .unwrap()
                .identity();
            let finished = frame(&mut core, root, TaskState::Finished, None);
            let aborted = frame(
                &mut core,
                producer,
                TaskState::Aborted,
                Some(TerminationDetail::Aborted(AbortCause::QueryFailed)),
            );
            let frames = if failure_first {
                vec![aborted, finished]
            } else {
                vec![finished, aborted]
            };
            let cut = core
                .apply_observations(
                    core.execution.monotonic_now(),
                    core.observation_source,
                    ObservationBatch::new(frames).unwrap(),
                )
                .unwrap()
                .unwrap();

            assert_eq!(
                core.execution.failure_cause(),
                Some(&TerminationDetail::Aborted(AbortCause::QueryFailed))
            );
            if failure_first {
                assert_eq!(
                    cut.settlement.result,
                    Err(RootSealRefusal::TerminalWithoutSuccess)
                );
                assert!(cut.root_before_seal.is_none());
                assert!(!core.root.sealed());
            } else {
                let projection = cut.root_before_seal.unwrap();
                assert_eq!(projection.status.identity(), root);
                assert_eq!(projection.status.state(), TaskState::Finished);
                assert_eq!(projection.failure, AttemptRootFailure::None);
                assert!(cut.settlement.result.unwrap().is_empty());
                assert!(core.root.sealed());
                assert!(!core.execution.normal_drain_started());
            }
        }
    }

    struct NoTraversal;
    impl StepContext for NoTraversal {
        fn charge(&mut self, _: usize) -> bool {
            panic!("the initial empty cut has no dirty Task rows");
        }
        fn reserve_queue_stage(
            &mut self,
            _: DispatchLane,
            _: bool,
            _: BackendProcessId,
            _: usize,
            _: usize,
        ) -> QueueStage {
            panic!("publication has no Native queue reservation");
        }
    }

    #[test]
    fn only_the_actual_protocol_publication_result_releases_the_common_source() {
        let mut core = round();
        assert!(core.select_protocol_publication().unwrap());
        assert!(seal(&mut core).is_err());
        assert_eq!(
            core.advance_protocol_publication(&mut NoTraversal).unwrap(),
            PublicationProgress::Ready
        );
        let effect = AttemptActionId::new(id(40));
        assert!(core.take_protocol_publication(effect).is_some());
        assert!(seal(&mut core).is_err());
        assert_eq!(
            core.protocol_publication_applied(effect, Ok(PublicationApplied::RootStatus)),
            PublicationApplyDisposition::Stale
        );
        assert_eq!(
            core.protocol_publication_applied(
                AttemptActionId::new(id(41)),
                Ok(PublicationApplied::ProtocolBatch { published: true })
            ),
            PublicationApplyDisposition::Stale
        );
        assert!(seal(&mut core).is_err());
        assert_eq!(
            core.protocol_publication_applied(
                effect,
                Ok(PublicationApplied::ProtocolBatch { published: true })
            ),
            PublicationApplyDisposition::Applied { published: true }
        );
        assert!(seal(&mut core).unwrap().seal.is_none());
        assert!(core.root.pending());
    }
}
