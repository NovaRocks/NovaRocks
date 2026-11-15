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

//! Register a second pure actor kind on the original FE runtime and ledger.
use super::{
    adapter::LogicalActor,
    application::{LogicalApplicationExecutor, LogicalHostPorts},
    failure::LogicalInfrastructureFailure,
    funding::{LogicalFundingObligation, SourceFundingObligation},
    geometry::LogicalGeometry,
    input_bank::LogicalInputBank,
    inputs::LogicalInputs,
    installed::LogicalInstalledState,
    links::LogicalAttemptLinks,
    source::{LogicalEffectExecutor, LogicalEffectSource, LogicalSourceAssembly},
    work::LogicalJobExecutor,
};
use crate::control_runtime::{
    FrontendAttemptRuntime,
    actor::{FrontendActor, FrontendActorKind},
    host::{FrontendLanePolicy, HostBinding},
};
use novarocks_control_runtime::{
    actor_turn::{ActorRegistration, ActorRuntime},
    transaction::RuntimeError,
};
use novarocks_control_runtime_core::{
    mailbox::InputClass, owned_storage::OwnedSlotNeed, scope::WorkKind,
};
use novarocks_coordination_contract::{
    ExecutionFailureInput, ExecutionFailureSite, QueryExecutionError,
};
use novarocks_execution_contract::{
    SafeDetail,
    failure::{FailureClass, FailureFact, FailureReason, domain},
};
use novarocks_logical_execution_protocol::{LogicalExecutionCore, start::LogicalStart};
use novarocks_query_application::{
    api::logical_runtime::{
        InstalledLogicalExecution, LogicalInstallRefusal, LogicalRuntimeInstall,
    },
    coordination::executor::LogicalExecutors,
};
use std::{
    num::NonZeroU16,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Wake, Waker},
};
use tokio::runtime::Handle;

/// P10 receives the original registration and the SAME source inventory. An
/// identity alone never constructs an author or a second routing registry.
pub(crate) struct LogicalPortInstallContext<'a> {
    start: &'a LogicalStart,
    runtime: &'a ActorRuntime<FrontendActor>,
    registration: ActorRegistration,
    inputs: Arc<LogicalInputBank>,
    links: Arc<LogicalAttemptLinks>,
}
impl LogicalPortInstallContext<'_> {
    pub(crate) fn links(&self) -> Arc<LogicalAttemptLinks> {
        self.links.clone()
    }
    pub(crate) fn start(&self) -> &LogicalStart {
        self.start
    }
    pub(crate) fn runtime(&self) -> &ActorRuntime<FrontendActor> {
        self.runtime
    }
    pub(crate) fn registration(&self) -> &ActorRegistration {
        &self.registration
    }
    pub(crate) fn inputs(&self) -> Arc<LogicalInputBank> {
        self.inputs.clone()
    }
}
pub(crate) trait LogicalPortInstaller: Send + Sync {
    fn bind(&self, original: LogicalPortInstallContext<'_>) -> Arc<dyn LogicalHostPorts>;
}
pub(crate) struct LogicalRuntimeInstaller {
    process: Arc<FrontendAttemptRuntime>,
    data_runtime: Handle,
    ports: Arc<dyn LogicalPortInstaller>,
}
impl LogicalRuntimeInstaller {
    pub(crate) fn new(
        process: Arc<FrontendAttemptRuntime>,
        data_runtime: Handle,
        ports: Arc<dyn LogicalPortInstaller>,
    ) -> Self {
        Self {
            process,
            data_runtime,
            ports,
        }
    }
}
struct LogicalHostWake {
    runtime: ActorRuntime<FrontendActor>,
    registration: ActorRegistration,
    progress: Arc<AtomicBool>,
}
impl Wake for LogicalHostWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.progress.store(true, Ordering::Release);
        let _ = self.runtime.notify_host_actor(&self.registration);
    }
}
impl LogicalRuntimeInstall for LogicalRuntimeInstaller {
    fn install(
        &self,
        start: LogicalStart,
        executors: LogicalExecutors,
    ) -> Result<Arc<dyn InstalledLogicalExecution>, LogicalInstallRefusal> {
        start
            .validate()
            .map_err(LogicalInstallRefusal::InvalidStart)?;
        let _registration = self
            .process
            .begin_attempt_registration()
            .map_err(|_| LogicalInstallRefusal::RuntimeClosed)?;
        if self.process.host.contains_logical(start.actor_instance) {
            return Err(LogicalInstallRefusal::DuplicateExecution);
        }
        let funding = LogicalFundingObligation::reserve(&self.process.ledger)
            .map_err(|error| LogicalInstallRefusal::Resource(resource_failure(error)))?;
        let geometry = match LogicalGeometry::derive(funding.scope().id(), &start) {
            Ok(geometry) => geometry,
            Err(_) => {
                funding.retire_unpublished(&self.process.ledger);
                return Err(LogicalInstallRefusal::Resource(resource_failure(
                    RuntimeError::IdentityExhausted,
                )));
            }
        };
        let failure_work = funding
            .reserve_reported(WorkKind::Cleanup, 0)
            .expect("original prepaid logical owner failure work");
        let disposal_work = funding
            .reserve_reported(WorkKind::Cleanup, 0)
            .expect("original prepaid executor disposal work");
        let core = LogicalExecutionCore::new(start.clone());
        let adapter = LogicalActor::new(core, funding.step_reservations());
        let runtime = &self.process.runtime;
        let registration = runtime
            .register_supervised_bound_with_source_cut(
                geometry.actor,
                funding.scope().clone(),
                FrontendActor::Logical(Box::new(adapter)),
                geometry.config,
                failure_work.work(),
                geometry.source(0).expect("logical source geometry"),
            )
            .unwrap_or_else(|(error, _)| {
                panic!("logical runtime registration invariant failed: {error:?}")
            });
        let progress_requested = Arc::new(AtomicBool::new(true));
        let wake = Waker::from(Arc::new(LogicalHostWake {
            runtime: runtime.clone(),
            registration: registration.clone(),
            progress: progress_requested.clone(),
        }));
        let marker_author = SourceFundingObligation::start(
            funding
                .reserve_step(
                    WorkKind::Settlement,
                    OwnedSlotNeed {
                        inputs: 1,
                        work_headers: 1,
                        ..Default::default()
                    },
                )
                .expect("original logical failure marker author"),
        );
        // The normal source catalog occupies [purposes, purposes+A+6).
        let marker_source = geometry
            .source(geometry.purposes + geometry.attempts + 6)
            .expect("logical marker source geometry");
        let marker = runtime
            .reserve_admitted_input_for(
                &registration,
                marker_source,
                InputClass::Lifecycle,
                marker_author.work(),
            )
            .expect("prepaid original logical failure marker")
            .into_standing_input()
            .unwrap_or_else(|_| panic!("original logical failure marker standing position"));
        let failure = LogicalInfrastructureFailure::new(marker, wake.clone());
        let state = LogicalInstalledState::new(wake.clone(), failure.clone());
        state.bind_failure_actor(runtime, &registration);
        let inputs = LogicalInputs::reserve(
            runtime,
            registration.clone(),
            &funding,
            &geometry,
            &start,
            state.clone(),
            failure.clone(),
            wake.clone(),
        );
        let bootstrap = inputs.bootstrap();
        let inputs = LogicalInputBank::new(inputs, wake.clone());
        let links = LogicalAttemptLinks::new(geometry.attempts, wake.clone(), failure.reporter());
        let ports = self.ports.bind(LogicalPortInstallContext {
            start: &start,
            runtime,
            registration: registration.clone(),
            inputs: inputs.clone(),
            links: links.clone(),
        });
        let application: Arc<dyn LogicalJobExecutor> = Arc::new(LogicalApplicationExecutor {
            original: executors,
            ports,
            replies: state.clone(),
            failure: failure.clone(),
            bootstrap,
            consumer_attached: tokio::sync::OnceCell::new(),
        });
        let executor = Arc::new(LogicalEffectExecutor {
            runtime: self.data_runtime.clone(),
            original: Arc::downgrade(&application),
        });
        let scope = funding.scope().clone();
        let source = LogicalEffectSource::reserve(LogicalSourceAssembly {
            runtime,
            registration: registration.clone(),
            funding,
            geometry: &geometry,
            start: &start,
            failure,
            executor: application,
            wake,
            data_runtime: self.data_runtime.clone(),
            inputs,
            state: state.clone(),
            links: links.clone(),
            failure_work,
            disposal_work,
            marker_author,
            progress_requested,
        });
        let binding = HostBinding {
            scope,
            registration: registration.clone(),
            kind: FrontendActorKind::Logical,
            config: geometry.config,
            policy: FrontendLanePolicy::logical(super::geometry::OUTPUT_UNITS),
            source: Box::new(source),
            executor,
            transport: None,
            ordered_observations: None,
            startup: None,
            capacity: None,
            membership: None,
            owner_controls: None,
        };
        let exit = self
            .process
            .host
            .register_logical(binding, start.actor_instance, links)
            .unwrap_or_else(|original| {
                panic!(
                    "logical host registration invariant failed: {}",
                    original.reason
                )
            });
        state.bind_exit(exit);
        runtime
            .notify_host_actor(&registration)
            .expect("original installed logical startup");
        Ok(state)
    }
}
fn resource_failure(error: RuntimeError) -> QueryExecutionError {
    QueryExecutionError::from_execution(ExecutionFailureInput::new(
        FailureFact::new(
            FailureClass::Invariant,
            FailureReason::new(domain::QUERY, NonZeroU16::new(84).unwrap()),
        ),
        SafeDetail::truncating_display(format_args!(
            "logical runtime installation refused: {error:?}"
        )),
        ExecutionFailureSite::Coordinator,
    ))
}
