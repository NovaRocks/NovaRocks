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

//! Install the original manifest and its paid Sources on the FE runtime.

use super::{
    adapter::{
        AttemptActor, AttemptActorRegistration, AttemptSendInputs, AttemptTransportBinding,
        ObservationSourceBacking,
    },
    auxiliary::AttemptAuxiliaryExecutor,
    custody::TransportRetirements,
    failure_supervisor::FailureSupervisor,
    funding::AttemptHostFunding,
    geometry::AttemptHostGeometry,
    publication::{AttemptDiagnosticSources, AttemptPublicationOwner, PublicationRecord},
    pumps::{DiagnosticRecords, DynamicFilterRecords, split::SplitPump},
    transport::{ActorTransportExecutor, AttemptSendSource, StagedSubscriptionEndpoints},
};
use crate::{
    control_runtime::{FrontendAttemptRuntime, host::EffectExecutor},
    native::{data_runtime::FrontendDataRuntime, task_result_transport::NativeTaskResultTransport},
    query_execution::{
        native_execution_adapter::LogicalAbortRoute,
        split_assignment_round::RoundSplitAssignmentPlan,
    },
    task_execution::{
        bridge::{root_control::RootControlRequests, terminal::RegisteredActiveAttempt},
        manifest_projection::{
            AttemptActivationInputs, AttemptManifestProjection, AttemptPlanPolicy,
            PreparedManifestAttempt,
        },
        profile_diagnostics::TaskFinalInfoCollector,
        sources::AttemptEstablishFacts,
        status_intake::StatusIntakeWake,
    },
};
use novarocks_attempt_protocol::{effect::PumpKind, ids::IdSeed, start::PayloadRef};
use novarocks_control_runtime::{
    completion::ReportSlot,
    exit_custody::ExitCustody,
    obligation::ReservationObligation,
    reported_work::ReportedWork,
    transaction::{RuntimeError, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    completion::ConsumerKind,
    identity::{ConsumerId, Generation},
    owned_storage::OwnedSlotNeed,
    scope::WorkKind,
};
use novarocks_coordination_contract::MonotonicClock;
use novarocks_execution_contract::{SafeDetail, failure::FailureProjection};
use novarocks_query_application::{
    api::{NativeAttemptPreparationFailure, StatementNativeHealthFailure},
    coordination::{NativeAttemptDrive, ReplacementWorkerAdmissionEvidence},
    execution_error::{ExecutionFailureInput, ExecutionFailureSite, QueryFailureReason},
};
use novarocks_workload_control::joint_admission::NativeAsyncActionAuthority;
use std::{
    num::{NonZeroU64, NonZeroUsize},
    sync::{Arc, OnceLock, Weak},
    task::Waker,
};

const GENERATION: Generation = Generation::new(1).unwrap();
const OBSERVER: ConsumerId = ConsumerId::new(1).unwrap();

#[derive(Debug)]
struct SourceWake(Waker);
impl StatusIntakeWake for SourceWake {
    fn wake(&self) {
        self.0.wake_by_ref();
    }
}

pub(crate) struct InstalledAttemptHost {
    pub(crate) bridge: RegisteredActiveAttempt,
    publication: Weak<AttemptPublicationOwner>,
    registered_cut: Option<novarocks_query_application::api::ComputationReadyToken>,
    lifecycle: Option<Arc<AttemptDiagnosticSources>>,
}

impl InstalledAttemptHost {
    pub(crate) fn take_lifecycle_rollup(
        &self,
    ) -> Option<crate::query_execution::lifecycle_diagnostics::RuntimeFilterTerminalRollupSnapshot>
    {
        self.lifecycle
            .as_ref()?
            .take_lifecycle_rollup(self.bridge.lifecycle_runtime_filter_diagnostic())
    }

    pub(crate) async fn register_cut(
        &mut self,
        ready: novarocks_query_application::api::ComputationReadyToken,
    ) -> Result<(), StatementNativeHealthFailure> {
        if self.registered_cut == Some(ready) {
            return Ok(());
        }
        if let Some(publication) = self.publication.upgrade() {
            publication
                .register_computation_cut(ready)
                .await
                .map_err(|error| {
                    StatementNativeHealthFailure::actual_exit_unproven(
                        "Native attempt lost its original diagnostic computation cut",
                        Arc::new(error),
                    )
                })?;
            self.registered_cut = Some(ready);
        }
        // If the publication Source has already exited, the bridge returns
        // the original failure or convergence outcome.
        Ok(())
    }
}

pub(crate) struct InstallAttemptHost<'a> {
    pub(crate) process: &'a FrontendAttemptRuntime,
    pub(crate) data: &'a FrontendDataRuntime,
    pub(crate) clock: Arc<dyn MonotonicClock>,
    pub(crate) drive: &'a NativeAttemptDrive,
    pub(crate) prepared: PreparedManifestAttempt,
    pub(crate) establish: &'a AttemptEstablishFacts,
    pub(crate) replacements: Option<&'a [ReplacementWorkerAdmissionEvidence]>,
    pub(crate) subscriptions: StagedSubscriptionEndpoints,
    pub(crate) authority: NativeAsyncActionAuthority,
    pub(crate) route: Arc<LogicalAbortRoute>,
    pub(crate) results: Arc<NativeTaskResultTransport>,
    pub(crate) profile: Option<TaskFinalInfoCollector>,
    pub(crate) dynamic: Option<Arc<DynamicFilterRecords>>,
    pub(crate) split: &'a mut Option<RoundSplitAssignmentPlan>,
    pub(crate) policy: AttemptPlanPolicy,
    pub(crate) ids: IdSeed,
    pub(crate) retain_diagnostics: bool,
    pub(crate) observe_dev_root: bool,
    pub(crate) observe_lifecycle: bool,
    #[cfg(novarocks_dev_controls)]
    pub(crate) root_observer: Option<crate::preparation_diagnostics::StatementObservationHandle>,
}

impl InstallAttemptHost<'_> {
    pub(crate) fn install(self) -> Result<InstalledAttemptHost, NativeAttemptPreparationFailure> {
        let Self {
            process,
            data,
            clock,
            drive,
            prepared,
            establish,
            replacements,
            subscriptions,
            authority,
            route,
            results,
            profile,
            dynamic,
            split,
            policy,
            ids,
            retain_diagnostics,
            observe_dev_root,
            observe_lifecycle,
            #[cfg(novarocks_dev_controls)]
            root_observer,
        } = self;
        let gate = process
            .begin_attempt_registration()
            .map_err(preparation_failure)?;
        let runtime = process.actor_runtime();
        let ledger = process.ledger();
        let layout = process.transport_capacity();
        let participants = NonZeroUsize::new(prepared.participants())
            .ok_or_else(|| preparation_failure("empty original Native roster"))?;
        let resources = AttemptActorRegistration::observation_resources(participants.get())
            .map_err(preparation_failure)?;
        let mut next_payload = NonZeroU64::MIN;
        let mut pump_ref = || {
            let reference = PayloadRef(next_payload);
            next_payload = next_payload.checked_add(1).unwrap();
            reference
        };
        let split_ref = split
            .as_ref()
            .is_some_and(RoundSplitAssignmentPlan::has_sources)
            .then(&mut pump_ref);
        let dynamic_ref = dynamic.is_some().then(pump_ref);
        let pumps = [
            split_ref.map(|reference| (reference, PumpKind::SplitAssignment)),
            dynamic_ref.map(|reference| (reference, PumpKind::DynamicFilters)),
        ]
        .into_iter()
        .flatten()
        .collect::<Box<[_]>>();
        let mut funding = AttemptHostFunding::reserve(ledger, None, GENERATION, resources)
            .map_err(|error| {
                let reason = if matches!(error, RuntimeError::CapacityDenied(_)) {
                    QueryFailureReason::TransportCapacity
                } else {
                    QueryFailureReason::PreparationProtocol
                };
                preparation_failure_with(reason, &error)
            })?;
        let geometry = match AttemptHostGeometry::derive(
            funding.scope().id(),
            participants,
            policy.coordination.dispatch,
            layout,
            &pumps,
            retain_diagnostics,
        ) {
            Ok(geometry) => geometry,
            Err(error) => {
                funding.retire_unpublished(ledger);
                return Err(preparation_failure(error));
            }
        };
        let task_ids = split_ref.map(|_| {
            prepared
                .graph()
                .tasks()
                .map(|row| (row.task_key(), row.identity()))
                .collect()
        });
        let projected = AttemptManifestProjection::from_prepared_manifest(
            prepared,
            establish,
            AttemptActivationInputs {
                drive,
                replacement_admissions: replacements,
                ids,
                initial_now: clock.now(),
                sources: geometry.sources,
                retain_diagnostics,
                observe_dev_root,
                observe_lifecycle_diagnostics: observe_lifecycle,
            },
            policy,
            pumps,
            &mut next_payload,
        );
        let AttemptManifestProjection {
            start,
            members,
            publications,
            creations,
            requests,
            qa,
            ..
        } = match projected {
            Ok(original) => original,
            Err(error) => {
                funding.retire_unpublished(ledger);
                return Err(NativeAttemptPreparationFailure::from_input(
                    error.preparation_input(),
                ));
            }
        };
        let config = match geometry.actor_config(&start) {
            Ok(config) => config,
            Err(error) => {
                funding.retire_unpublished(ledger);
                return Err(preparation_failure(error));
            }
        };
        let (_, payer) = funding.parent();
        let scope = funding.scope().clone();
        let (failure_work, report_author) = match reserve_authors(&scope, payer) {
            Ok(authors) => authors,
            Err(error) => {
                funding.retire_unpublished(ledger);
                return Err(preparation_failure(error));
            }
        };
        // The identities and immutable diagnostics are projected before the
        // actor owns Core. The profile lookup is filled by the same split runner.
        let split_profile = split_ref.map(|reference| (reference, Arc::new(OnceLock::new())));
        let diagnostics = AttemptDiagnosticSources::new(&start, split_profile.clone());
        let lifecycle = observe_lifecycle.then(|| diagnostics.clone());
        let mut registered = match AttemptActor::register(
            runtime,
            start,
            &members,
            scope.clone(),
            layout.clone(),
            config,
            failure_work.work(),
        ) {
            Ok(original) => original,
            Err(error) => {
                retire_unpublished_author(&scope, failure_work);
                retire_unpublished_author(&scope, report_author);
                funding.retire_unpublished(ledger);
                return Err(preparation_failure(error));
            }
        };
        // The remaining positions are derived from the same immutable
        // geometry and issued while the process registration gate is held.
        let executor = data.handle().clone();
        let retirement = TransportRetirements::new(
            runtime.clone(),
            registered.original.clone(),
            executor.clone(),
            geometry.transport_joins,
        );
        let wake = retirement.host_waker();
        let split_pump = split_profile.map(|(reference, profile)| {
            SplitPump::prepare(
                reference,
                drive.identity().execution(),
                split.take().expect("original opened split plan"),
                task_ids.expect("original split targets"),
                Arc::new(SourceWake(wake.clone())),
                data.clone(),
                profile,
            )
            .expect("the original plan has opened split sources")
        });
        let root = RootControlRequests::new(
            registered.root_control_binding().clone(),
            registered.failure_publications().clone(),
        );
        let ingress = {
            let (observation, positions, root_work) = funding.observation_and_root_control();
            registered
                .bind_observation_source(
                    runtime,
                    ObservationSourceBacking {
                        work: observation,
                        positions,
                    },
                    root_work,
                    &root,
                    clock.clone(),
                    wake,
                )
                .expect("prepaid original observation Source")
        };
        publications
            .root_controls
            .bind(registered.root_control_binding(), &root)
            .expect("original root controls bind before publication");
        let (work, payer) = funding.parent();
        let capacity = registered
            .bind_source_parent(runtime, work, payer)
            .expect("prepaid original attempt Source");
        let membership = registered
            .bind_membership(runtime, work)
            .expect("prepaid original membership Source");
        let (abort_ingress, abort) = route
            .bind_attempt_ingress(&mut registered, runtime, work)
            .expect("prepaid original Abort ingress");
        let (auxiliary, infrastructure) = registered
            .prepare_auxiliary_source(runtime, work)
            .expect("prepaid original infrastructure failure position");
        let subscriptions = registered
            .bind_staged_subscriptions(runtime, work, payer, OBSERVER, subscriptions, &ingress)
            .expect("staged original WC Sources");
        let (contexts, idle) = registered
            .prepare_send_routes(
                runtime,
                work,
                &subscriptions,
                AttemptSendInputs {
                    ledger,
                    authority: authority.clone(),
                    clock: clock.clone(),
                    observer: OBSERVER,
                    result_counts: geometry.send_results,
                },
            )
            .expect("prepaid original send routes");
        let send = AttemptSendSource {
            requests,
            contexts,
            report_author,
            idle,
        };
        let mut admission = registered
            .prepare_admission_source(
                runtime,
                work,
                payer,
                OBSERVER,
                authority,
                geometry.admission_results,
            )
            .expect("prepaid original admission Source");
        for (reference, original) in creations {
            admission
                .install_creation(reference, original)
                .unwrap_or_else(|original| std::panic::resume_unwind(Box::new(original)));
        }
        let diagnostic_reads = retain_diagnostics.then(|| DiagnosticRecords::new(results));
        #[allow(unused_mut)]
        let mut publication = PublicationRecord::new(
            drive.identity(),
            publications.root_sender,
            publications.protocol,
            profile,
            diagnostic_reads.clone(),
        );
        #[cfg(novarocks_dev_controls)]
        if let Some(original) = root_observer {
            publication.register_root_observer(original);
        }
        let publication =
            AttemptPublicationOwner::new(publication, Some(diagnostics.clone()), Some(root));
        let transport = AttemptTransportBinding {
            retirement,
            admission,
            abort,
            send,
            next_payload,
            diagnostics,
            qa: registered
                .prepare_qa_source(runtime, work, payer, OBSERVER, qa)
                .expect("prepaid original QA Source"),
            process: process
                .prepare_attempt_process_source(&registered, work, payer, OBSERVER)
                .expect("prepaid original process observation Source"),
            publication: registered
                .prepare_publication_source(runtime, work, payer, OBSERVER, publication.clone())
                .expect("prepaid original publication Source"),
            release: registered
                .prepare_transport_release_source(runtime, work, payer, OBSERVER)
                .expect("prepaid original release Source"),
            read_confirmation: registered
                .prepare_read_confirmation_source(runtime, work, payer, OBSERVER, ingress)
                .expect("prepaid original read confirmation Source"),
            subscription_control: subscriptions.control,
            subscriptions: subscriptions.original,
            pumps: registered
                .prepare_pump_source(
                    runtime,
                    work,
                    payer,
                    OBSERVER,
                    dynamic,
                    diagnostic_reads,
                    split_pump.as_ref(),
                )
                .expect("prepaid original pump Sources"),
            executor: ActorTransportExecutor(executor.clone()),
            failure: FailureSupervisor::new(
                runtime.clone(),
                registered.original.clone(),
                failure_work,
                OBSERVER,
                executor,
            ),
        };
        let executor: Arc<dyn EffectExecutor> = Arc::new(AttemptAuxiliaryExecutor);
        let mut prepared = registered
            .prepare_host(
                runtime,
                work,
                capacity,
                membership,
                Box::new(auxiliary),
                &executor,
                transport,
            )
            .unwrap_or_else(|original| std::panic::resume_unwind(Box::new(original)));
        prepared.failure = prepared.failure.with_infrastructure_health(infrastructure);
        let (startup, controls, terminal) = gate
            .register_attempt(prepared, funding)
            .unwrap_or_else(|original| std::panic::resume_unwind(Box::new(original)));
        let bridge =
            RegisteredActiveAttempt::new(startup, controls, terminal, clock, move |startup| {
                route.publish_registered(startup, abort_ingress)
            });
        Ok(InstalledAttemptHost {
            bridge,
            publication: Arc::downgrade(&publication),
            registered_cut: None,
            lifecycle,
        })
    }
}

fn reserve_authors(
    scope: &ScopeHandle,
    parent: &ReservationObligation,
) -> Result<(ReportedWork, ReportedWork), RuntimeError> {
    let reserve = |kind| {
        let slots = ReservationResources::new(
            ResourceVector::default(),
            OwnedSlotNeed {
                work_headers: 1,
                completions: 1,
                captures: 1,
                ..Default::default()
            },
        );
        let (work, mut payer) = scope
            .reserve_child_next(GENERATION, parent.id(), slots, kind)?
            .bind(kind)
            .expect("original author guard");
        let report = ReportSlot::reserve_next(
            &mut payer,
            GENERATION,
            1,
            [(OBSERVER, ConsumerKind::Observer)],
        )
        .expect("original author report");
        Ok(ReportedWork::try_new(work, payer, report).expect("same original author and report"))
    };
    let failure = reserve(WorkKind::Cleanup)?;
    match reserve(WorkKind::Settlement) {
        Ok(reports) => Ok((failure, reports)),
        Err(error) => {
            retire_unpublished_author(scope, failure);
            Err(error)
        }
    }
}

fn retire_unpublished_author(scope: &ScopeHandle, work: ReportedWork) {
    let header = work.work().retirement_observer();
    let (custody, exit) = ExitCustody::from_admitted(work);
    exit.revoke_unused_and_exit()
        .expect("unpublished original author");
    assert!(
        custody
            .try_retire()
            .expect("unpublished original author report")
    );
    assert!(
        scope
            .reap_observed_header(&header)
            .expect("unpublished original author header")
    );
}

fn preparation_failure(error: impl std::fmt::Debug) -> NativeAttemptPreparationFailure {
    preparation_failure_with(QueryFailureReason::PreparationProtocol, &error)
}

fn preparation_failure_with(
    reason: QueryFailureReason,
    original: &impl std::fmt::Debug,
) -> NativeAttemptPreparationFailure {
    NativeAttemptPreparationFailure::from_input(ExecutionFailureInput::new(
        reason.failure_fact(),
        SafeDetail::truncating_display(format_args!(
            "Native attempt installation refused: {original:?}"
        )),
        ExecutionFailureSite::Coordinator,
    ))
}
