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
    geometry::AttemptHostGeometry,
    publication::{AttemptDiagnosticSources, AttemptPublicationOwner, PublicationRecord},
    pumps::{DiagnosticRecords, DynamicFilterRecords, split::SplitPump},
    transport::{
        ActorTransportExecutor, AttemptSendSource, EstablishMaterialPreparation,
        StagedSubscriptionEndpoints,
    },
};
use crate::{
    control_runtime::{FrontendAttemptRuntime, host::EffectExecutor},
    native::{data_runtime::FrontendDataRuntime, task_result_transport::TaskResultTransport},
    query_execution::split_assignment_round::RoundSplitAssignmentPlan,
    task_execution::{
        manifest_projection::{
            AttemptActivationInputs, AttemptManifestProjection, AttemptPlanPolicy,
            BoundAttemptManifestProjection, PreparedManifestAttempt,
        },
        profile_diagnostics::TaskFinalInfoCollector,
    },
    wake::StatusIntakeWake,
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
    coordination::ReplacementWorkerAdmissionEvidence,
    execution_error::{ExecutionFailureInput, ExecutionFailureSite, QueryFailureReason},
};
use novarocks_workload_control::joint_admission::NativeAsyncActionAuthority;
use std::{
    num::{NonZeroU64, NonZeroUsize},
    sync::{Arc, OnceLock},
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
    #[cfg(test)]
    pub(crate) fixture_sources: (
        novarocks_control_runtime_core::identity::ActorId,
        novarocks_control_runtime_core::identity::ActorId,
    ),
    #[cfg(test)]
    pub(crate) fixture_exit: Arc<crate::control_runtime::host::HostExit>,
    #[cfg(test)]
    pub(crate) fixture_admission_failures: std::sync::Weak<
        std::sync::Mutex<
            std::collections::BTreeMap<
                novarocks_attempt_protocol::failure::HostFailureRef,
                crate::task_execution::error::TaskExecutionError,
            >,
        >,
    >,
    pub(crate) startup: super::startup::AttemptStartup,
}

pub(crate) struct InstallAttemptHost<'a> {
    pub(crate) initial_now: novarocks_coordination_contract::MonotonicInstant,
    #[cfg(test)]
    pub(crate) fixture_observation_budget: Option<super::observations::ObservationBudget>,
    #[cfg(all(test, novarocks_dev_controls))]
    pub(crate) fixture_establish_handoff: Option<
        &'a crate::query_execution::native_execution_adapter::original_install_fixture::OriginalInstallRow,
    >,
    pub(crate) process: &'a FrontendAttemptRuntime,
    pub(crate) data: &'a FrontendDataRuntime,
    pub(crate) clock: Arc<dyn MonotonicClock>,
    pub(crate) activation: novarocks_coordination_contract::AttemptActivationIdentity,
    pub(crate) authorization_limits: novarocks_coordination_contract::AttemptAuthorizationLimits,
    pub(crate) logical: &'a crate::query_execution::logical_binding::BoundLogicalPorts,
    pub(crate) link: &'a crate::control_runtime::logical::links::AttemptActivationLinkLoan,
    pub(crate) logical_inputs: &'a mut crate::control_runtime::logical::LogicalInputsLoan,
    pub(crate) original_exit: &'a mut Option<crate::control_runtime::host::HostExit>,
    pub(crate) prepared: PreparedManifestAttempt,
    pub(crate) establish: &'a mut EstablishMaterialPreparation,
    pub(crate) replacements: Option<&'a [ReplacementWorkerAdmissionEvidence]>,
    pub(crate) subscriptions: StagedSubscriptionEndpoints,
    pub(crate) authority: NativeAsyncActionAuthority,
    pub(crate) results: Arc<dyn TaskResultTransport>,
    pub(crate) profile: Option<TaskFinalInfoCollector>,
    pub(crate) dynamic: Option<Arc<DynamicFilterRecords>>,
    pub(crate) originals: crate::query_execution::native_execution_adapter::AttemptSourceInputs<'a>,
    pub(crate) policy: AttemptPlanPolicy,
    pub(crate) ids: IdSeed,
    pub(crate) retain_diagnostics: bool,
    pub(crate) observe_dev_root: bool,
    pub(crate) observe_lifecycle: bool,
    #[cfg(novarocks_dev_controls)]
    pub(crate) root_observer: Option<crate::preparation_diagnostics::StatementObservationHandle>,
}

impl InstallAttemptHost<'_> {
    pub(crate) fn install(
        self,
    ) -> Result<
        InstalledAttemptHost,
        novarocks_query_application::api::NativeAttemptActivationFailure,
    > {
        let Self {
            initial_now,
            #[cfg(test)]
            fixture_observation_budget,
            #[cfg(all(test, novarocks_dev_controls))]
            fixture_establish_handoff,
            process,
            data,
            clock,
            activation,
            authorization_limits,
            logical,
            link,
            logical_inputs,
            original_exit,
            prepared,
            establish,
            replacements,
            subscriptions,
            authority,
            results,
            profile,
            dynamic,
            mut originals,
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
        #[cfg(not(test))]
        let resources = AttemptActorRegistration::observation_resources(participants.get())
            .map_err(preparation_failure)?;
        #[cfg(test)]
        let budget = fixture_observation_budget
            .unwrap_or_else(super::observations::ObservationBudget::for_task_attempt);
        #[cfg(test)]
        let resources =
            AttemptActorRegistration::observation_resources_with_budget(budget, participants.get())
                .map_err(preparation_failure)?;
        let mut next_payload = NonZeroU64::MIN;
        let mut pump_ref = || {
            let reference = PayloadRef(next_payload);
            next_payload = next_payload.checked_add(1).unwrap();
            reference
        };
        let split_ref = originals
            .split_plan_mut()
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
        let dispatch = policy.coordination.dispatch;
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
                identity: activation,
                authorization_limits,
                replacement_admissions: replacements,
                ids,
                initial_now,
                retain_diagnostics,
                observe_dev_root,
                observe_lifecycle_diagnostics: observe_lifecycle,
            },
            policy,
            pumps,
            &mut next_payload,
        );
        let projected = projected.map_err(|error| {
            novarocks_query_application::api::NativeAttemptActivationFailure::new(
                NativeAttemptPreparationFailure::from_input(error.preparation_input()),
            )
        })?;
        let mut funding = link
            .reserve_attempt(ledger, GENERATION, resources)
            .map_err(|error| {
                let reason = if matches!(error, RuntimeError::CapacityDenied(_)) {
                    QueryFailureReason::TransportCapacity
                } else {
                    QueryFailureReason::PreparationProtocol
                };
                preparation_failure_with(reason, &error)
            })?;
        let mut geometry = match AttemptHostGeometry::derive(
            funding.scope().id(),
            participants,
            dispatch,
            layout,
            projected.pumps(),
            retain_diagnostics,
        ) {
            Ok(geometry) => geometry,
            Err(error) => {
                link.retire_unpublished_child(funding, ledger);
                return Err(preparation_failure(error));
            }
        };
        let logical_source = novarocks_attempt_protocol::input::AttemptSourceId(
            NonZeroU64::new(logical.registration.actor().get()).unwrap(),
        );
        geometry.sources.owner = logical_source;
        geometry.sources.qa = logical_source;
        let BoundAttemptManifestProjection {
            start,
            members,
            publications,
            creations,
            requests,
            establish,
            ..
        } = projected.bind_sources(geometry.sources);
        let config = match geometry.actor_config(&start) {
            Ok(config) => config,
            Err(error) => {
                link.retire_unpublished_child(funding, ledger);
                return Err(preparation_failure(error));
            }
        };
        let (_, payer) = funding.parent();
        let scope = funding.scope().clone();
        let (failure_work, report_author) = match reserve_authors(&scope, payer) {
            Ok(authors) => authors,
            Err(error) => {
                link.retire_unpublished_child(funding, ledger);
                return Err(preparation_failure(error));
            }
        };
        // The identities and immutable diagnostics are projected before the
        // actor owns Core. The profile lookup is filled by the same split runner.
        let split_profile = split_ref.map(|reference| (reference, Arc::new(OnceLock::new())));
        let diagnostics = AttemptDiagnosticSources::new(&start, split_profile.clone());
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
                link.retire_unpublished_child(funding, ledger);
                return Err(preparation_failure(error));
            }
        };
        #[cfg(test)]
        if fixture_observation_budget.is_some() {
            registered.freeze_observation_budget_for_fixture(budget);
        }
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
                activation.execution(),
                originals
                    .split_plan_mut()
                    .take()
                    .expect("original opened split plan"),
                task_ids.expect("original split targets"),
                Arc::new(SourceWake(wake.clone())),
                data.clone(),
                profile,
            )
            .expect("the original plan has opened split sources")
        });
        let root = super::port::root_control::RootControlRequests::new(
            registered.root_control_binding().clone(),
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
        let (work, payer) = funding.parent();
        let capacity = registered
            .bind_source_parent(runtime, work, payer)
            .expect("prepaid original attempt Source");
        let membership = registered
            .bind_membership(runtime, work)
            .expect("prepaid original membership Source");
        let startup = registered
            .prepare_startup(runtime, work)
            .expect("prepaid original startup and local-exit roles");
        let controls = registered
            .prepare_port_controls(runtime, &logical.registration, work)
            .expect("prepaid original logical control roles");
        let abort = registered
            .prepare_port_abort(runtime, &logical.registration, work)
            .expect("prepaid original logical Abort roles");
        let logical_wake = logical_inputs.host_waker();
        let author = logical_inputs
            .ap_author_from_link(link)
            .expect("same issued original AP fact author");
        let facts = super::port::facts::AttemptFactBank::reserve(
            runtime,
            &logical.registration,
            &registered.original,
            activation,
            author,
            retirement.host_waker(),
            logical_wake,
        )
        .unwrap_or_else(|original| std::panic::resume_unwind(Box::new(original)));
        let storage = link
            .reserve_port_storage()
            .expect("same original port reader funding");
        let (control, reader) = super::port::control::SourcePublishedSlot::bind(
            runtime,
            super::port::control::ControlBinding {
                lp: logical.registration.clone(),
                ap: registered.original.clone(),
                activation,
                positions: controls,
                storage,
                observer: OBSERVER,
                roots: root.clone(),
            },
        )
        .unwrap_or_else(|original| std::panic::resume_unwind(Box::new(original)));
        let requests_port = super::port::requests::AttemptRequests::new(
            activation,
            registered.original.clone(),
            logical.registration.clone(),
            NonZeroUsize::new(3).unwrap(),
            retirement.host_waker(),
        );
        let receipts = Arc::new(super::port::abort::AbortReceiptTable::new(activation));
        let endpoints = super::port::AttemptPortEndpoints::bind(
            logical.registration.clone(),
            registered.original.clone(),
            activation,
            startup.witness.clone(),
            requests_port,
            facts.clone(),
            control,
            abort,
            root.clone(),
        )
        .unwrap_or_else(|original| std::panic::resume_unwind(Box::new(original)));
        let observation = logical.bind_observation(link, activation, observe_lifecycle);
        facts.bind_observation(observation.clone());
        endpoints
            .bind_observation(&observation)
            .expect("same original control observation");
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
            closed: logical.closed.clone(),
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
        #[cfg(test)]
        let fixture_admission_failures = admission.failure_records_for_fixture();
        for (reference, original) in creations {
            admission
                .install_creation(reference, original)
                .unwrap_or_else(|original| std::panic::resume_unwind(Box::new(original)));
        }
        let diagnostic_reads = retain_diagnostics.then(|| DiagnosticRecords::new(results));
        #[allow(unused_mut)]
        let mut publication = PublicationRecord::new(
            activation,
            publications.protocol,
            profile,
            diagnostic_reads.clone(),
        );
        #[cfg(novarocks_dev_controls)]
        if let Some(original) = root_observer {
            publication.register_root_observer(original);
        }
        let publication = AttemptPublicationOwner::new(publication, Some(diagnostics.clone()));
        publication.bind_port_root(root);
        publication.bind_observation(&observation);
        let (residual, residual_sink) =
            super::failure_residual::FailureResidualReceiveObligation::new(
                registered.original.clone(),
                activation,
                members.clone(),
                retirement.host_waker(),
                auxiliary.raw_failure_publisher(),
            );
        let transport = AttemptTransportBinding {
            retirement,
            admission,
            port_binding: super::adapter::AttemptPortSourceBinding {
                endpoints: endpoints.clone(),
                establish,
                receipts,
                reader,
                residual,
                residual_sink,
                observation: observation.clone(),
            },
            send,
            next_payload,
            diagnostics,
            ports: registered
                .prepare_port_sources(runtime, &logical.registration, work, payer, OBSERVER)
                .expect("prepaid original logical port Sources"),
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
        #[cfg(test)]
        let fixture_sources = (
            registered.original.actor(),
            registered.subscription_source_for_test(),
        );
        let prepared = match registered.prepare_host(
            runtime,
            work,
            startup,
            capacity,
            membership,
            Box::new(auxiliary),
            &executor,
            transport,
            super::adapter::AttemptHostOriginals {
                logical,
                link,
                inputs: originals,
            },
        ) {
            Ok(prepared) => prepared,
            Err(original) => return Err(retain_install_refusal((original, funding))),
        };
        #[cfg(all(test, novarocks_dev_controls))]
        let prepared = if let Some(row) = fixture_establish_handoff {
            let mut prepared = prepared;
            let wake = prepared
                .binding
                .transport
                .as_ref()
                .expect("SAME original Host transport tracker")
                .host_waker();
            prepared.binding.executor =
                row.decorate_original_executor(prepared.binding.executor.clone(), wake);
            prepared
        } else {
            prepared
        };
        let mut funding = Some(funding);
        let startup = match gate.register_attempt(prepared, &mut funding, original_exit) {
            Ok(startup) => startup,
            Err(original) => return Err(retain_install_refusal((original, funding))),
        };
        let exit = original_exit
            .as_ref()
            .expect("same stable received original HostExit");
        link.bind_registered(&registered.original, exit);
        link.bind_endpoints(endpoints);
        let exit = Arc::new(original_exit.take().unwrap());
        observation.bind_source(
            link,
            &registered.original,
            exit.clone(),
            startup.clone(),
            &publication,
            infrastructure,
        );
        startup
            .release_initialization(runtime)
            .expect("same original initialization release");

        Ok(InstalledAttemptHost {
            #[cfg(test)]
            fixture_sources,
            #[cfg(test)]
            fixture_exit: exit,
            #[cfg(test)]
            fixture_admission_failures,
            startup,
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

fn preparation_failure(
    error: impl std::fmt::Debug,
) -> novarocks_query_application::api::NativeAttemptActivationFailure {
    preparation_failure_with(QueryFailureReason::PreparationProtocol, &error)
}

fn preparation_failure_with(
    reason: QueryFailureReason,
    original: &impl std::fmt::Debug,
) -> novarocks_query_application::api::NativeAttemptActivationFailure {
    novarocks_query_application::api::NativeAttemptActivationFailure::new(
        NativeAttemptPreparationFailure::from_input(ExecutionFailureInput::new(
            reason.failure_fact(),
            SafeDetail::truncating_display(format_args!(
                "Native attempt installation refused: {original:?}"
            )),
            ExecutionFailureSite::Coordinator,
        )),
    )
}

/// A whole typed constructor refusal retains its original Source/funding.
/// Dropping an observer never destroys unresolved rich objects on a poller.
struct UnprovenInstallObligation {
    _original: std::sync::Mutex<std::mem::ManuallyDrop<Box<dyn std::any::Any + Send>>>,
}
impl std::fmt::Debug for UnprovenInstallObligation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("original Native installation custody retained")
    }
}
impl std::fmt::Display for UnprovenInstallObligation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}
impl std::error::Error for UnprovenInstallObligation {}
fn retain_install_refusal(
    original: impl Send + 'static,
) -> novarocks_query_application::api::NativeAttemptActivationFailure {
    novarocks_query_application::api::NativeAttemptActivationFailure::local_exit_unproven(
        StatementNativeHealthFailure::actual_exit_unproven(
            "original Native installation did not prove local exit",
            Arc::new(UnprovenInstallObligation {
                _original: std::sync::Mutex::new(std::mem::ManuallyDrop::new(Box::new(original))),
            }),
        ),
    )
}
