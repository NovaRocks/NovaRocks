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

//! CREATE borrows its one pure reader and original Provider publication Source.

use super::{CommandInput, MvPhaseFailure, MvWake, original_error, service};
use crate::{
    mv::domain::{
        application::{MvApplicationError, MvApplicationErrorKind, MvCreateStatement},
        iceberg_refresh::IcebergMvCreatePlan,
        staged_create::{
            CurrentProjectionMetadataCpu,
            create_original::{
                CreateOriginal, CreateOriginalInput, CreatePhaseFailure, CreatePhaseFuture,
            },
        },
    },
    query_execution::{
        internal_result_cpu::InternalResultCpu,
        maintenance::io_admission::MetadataIoAdmission,
        mv_assembly::{
            create_preparation::MvCreatePreparationInput,
            refresh_preparation::{
                async_original::{MvRefreshPreparationOriginal, MvRefreshPreparationOutput},
                driver::{MvRefreshTurnCustody, MvTurnFailure},
                io_gate::{
                    MvRefreshPreparationIo, MvRefreshPreparationServices,
                    provider_error as preparation_provider_error,
                },
            },
        },
    },
    state_store::execution::StateStoreOriginalOwner,
};
use novarocks_mv_application::{
    ports::{MvCreateProgressPort, MvProviderFailure, MvProviderFailureKind},
    product::{
        MvCommand, MvCreateCommand, MvCreatePublicationOutcome, MvCreatedTarget,
        MvOperationContext, MvProductError, MvProductErrorKind, MvStagedTarget, MvTarget,
    },
    service::MvCreateTransition,
};
use novarocks_query_application::{
    api::{StatementExternalEffectDisposition, StatementNativeHealthFailure},
    cancellation::QueryCancellationView,
    session_control::GovernedActionContext,
    session_error::QueryServiceError,
};
use novarocks_spi::connector::{
    ConnectorControlPlanningLease, ConnectorRequestContext, ExternalMutationEffect,
};
use novarocks_sql::planning::mv::SqlMvTarget;
use novarocks_state_store_api::StateStoreExecutionRequests;
use novarocks_workload_control::{ResultWindowAlias, WorkError, WorkScope};
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

struct Preparation {
    gate: Arc<MvRefreshPreparationIo>,
    reader: Arc<MvRefreshPreparationOriginal>,
    turn: Option<MvRefreshTurnCustody>,
    result: Option<Result<MvRefreshPreparationOutput, MvApplicationError>>,
    received: bool,
}

/// The command Root retains this cell before any reader or Provider poll. The
/// StateStore owner stays in that same Root; request views borrow its weak port.
pub(super) struct MvCreateStepOriginal {
    input: Arc<CommandInput>,
    io: Arc<MetadataIoAdmission>,
    root: WorkScope,
    window: ResultWindowAlias,
    target: SqlMvTarget,
    statement: MvCreateStatement,
    context: ConnectorRequestContext,
    requests: Arc<dyn StateStoreExecutionRequests>,
    cpu: InternalResultCpu,
    wake: Arc<MvWake>,
    operation: uuid::Uuid,
    planning: Option<ConnectorControlPlanningLease>,
    preparation: Option<Preparation>,
    plan: Option<Arc<IcebergMvCreatePlan>>,
    source: Option<Arc<CreateOriginal>>,
    phase: Option<CreatePhaseFuture>,
    phase_result: Option<Result<(), CreatePhaseFailure>>,
    prepared: bool,
    transition: Option<MvCreateTransition>,
    primary: Option<MvPhaseFailure>,
    selected: Option<Result<(), MvPhaseFailure>>,
    progress_refusal: Option<WorkError>,
    health: Option<StatementNativeHealthFailure>,
    in_foreign: bool,
    closed: bool,
    exit_unproven: StatementNativeHealthFailure,
}
#[derive(Debug)]
struct CreateStepExitUnproven;
impl std::fmt::Display for CreateStepExitUnproven {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("original CREATE step did not return from its foreign poll or Drop")
    }
}
impl std::error::Error for CreateStepExitUnproven {}

impl MvCreateStepOriginal {
    pub(super) fn new(
        input: Arc<CommandInput>,
        io: Arc<MetadataIoAdmission>,
        store: &StateStoreOriginalOwner,
        target: SqlMvTarget,
        statement: MvCreateStatement,
        context: ConnectorRequestContext,
        wake: Arc<MvWake>,
    ) -> Result<Self, QueryServiceError> {
        let capacity = io.root_capacity().map_err(original_error)?;
        let root = capacity.root_scope().clone();
        let window = capacity.try_window_alias().map_err(original_error)?;
        let context = context.with_io_admission(io.clone());
        let requests = Arc::new(
            store
                .execution_requests(
                    input.state_store.transaction_deadline,
                    Some(context.deadline()),
                )
                .map_err(QueryServiceError::from_failure_cause)?,
        );
        let cpu = input
            .services
            .blocking
            .provider_creator_cpu()
            .map_err(original_error)?;
        Ok(Self {
            input,
            io,
            root,
            window,
            target,
            statement,
            context,
            requests,
            cpu,
            wake,
            operation: uuid::Uuid::now_v7(),
            planning: None,
            preparation: None,
            plan: None,
            source: None,
            phase: None,
            phase_result: None,
            prepared: false,
            transition: None,
            primary: None,
            selected: None,
            progress_refusal: None,
            health: None,
            in_foreign: false,
            closed: false,
            exit_unproven: StatementNativeHealthFailure::actual_exit_unproven(
                "original CREATE phase actual exit is unproven",
                Arc::new(CreateStepExitUnproven),
            ),
        })
    }
    pub(super) fn performed_publication(&self) -> bool {
        self.source
            .as_ref()
            .and_then(|s| s.needed_committed())
            .is_some_and(|needed| needed.effect == ExternalMutationEffect::Applied)
    }
    pub(super) fn external_effect_disposition(&self) -> Option<StatementExternalEffectDisposition> {
        self.source
            .as_ref()
            .and_then(|s| s.external_effect_disposition())
    }
    /// Full cleanup may remain Unknown after every actual call has exited.
    /// The Root uses this observation to close its same StateStore inventory,
    /// while retaining this step until poll_closed proves full disposition.
    pub(super) fn physical_exit_observed(&self) -> bool {
        self.closed
            || self
                .source
                .as_ref()
                .is_some_and(|source| source.physical_exit_observed())
    }
    pub(super) fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        if self.in_foreign {
            return Some(self.exit_unproven.clone());
        }
        self.health
            .clone()
            .or_else(|| self.source.as_ref().and_then(|s| s.local_health_failure()))
            .or_else(|| {
                self.preparation
                    .as_ref()
                    .and_then(|p| p.turn.as_ref())
                    .and_then(|turn| turn.local_health_failure())
            })
            .or_else(|| {
                self.preparation
                    .as_ref()
                    .and_then(|p| p.gate.local_health_failure())
            })
    }
    fn progress(&mut self, store: &StateStoreOriginalOwner, cx: &mut Context<'_>) {
        if let Err(error) = self.io.poll_continuing(cx) {
            self.progress_refusal.get_or_insert(error);
        }
        if let Err(error) = store.poll_progress(cx) {
            self.health.get_or_insert_with(|| {
                StatementNativeHealthFailure::actual_exit_unproven(
                    "CREATE StateStore physical progress is unproven",
                    Arc::new(error),
                )
            });
        }
    }
    fn select(&mut self, result: Result<(), MvPhaseFailure>) -> Poll<Result<(), MvPhaseFailure>> {
        self.selected = Some(result.clone());
        Poll::Ready(result)
    }
    fn install_reader(&mut self, owner: &GovernedActionContext<'_>) -> Result<(), MvPhaseFailure> {
        let cancellation =
            QueryCancellationView::governed(owner.cancellation().clone(), owner.timeout_ms());
        let runtime = self.input.services.blocking.runtime().clone();
        let original = self
            .io
            .original_result_scope(&self.context)
            .map_err(original_error)?;
        let gate = Arc::new(MvRefreshPreparationIo::new(
            MvRefreshPreparationServices {
                cpu: self.cpu.clone(),
                scope: self.root.clone(),
                window: self.window.clone(),
                cancellation: cancellation.clone(),
                runtime: runtime.clone(),
                wake: self.wake.clone(),
                requests: self.requests.clone(),
            },
            original,
        ));
        let planning = crate::connector::metadata_binding_typed(
            self.input.ports.connector_control(),
            self.target
                .catalog
                .as_deref()
                .ok_or_else(|| service("CREATE requires its selected catalog"))?,
        )
        .map_err(|error| domain_failure(preparation_provider_error(error)))?;
        self.planning = Some(planning.clone());
        let reader = MvRefreshPreparationOriginal::for_create(
            MvCreatePreparationInput {
                ports: self.input.ports.clone(),
                planning,
                current_catalog: self.input.current_catalog.clone(),
                current_database: self.input.current_database.clone(),
                statement: self.statement.clone(),
                context: self.context.clone(),
            },
            gate.clone(),
        );
        let turn = MvRefreshTurnCustody::new(
            gate.clone(),
            self.cpu.clone(),
            self.root.clone(),
            self.window.clone(),
            cancellation,
            runtime,
            self.wake.clone(),
        );
        self.preparation = Some(Preparation {
            gate,
            reader,
            turn: Some(turn),
            result: None,
            received: false,
        });
        let p = self.preparation.as_mut().unwrap();
        p.turn
            .as_mut()
            .unwrap()
            .install_preparation(p.reader.clone())
            .map_err(|_| service("CREATE reader does not match its original CPU receiving").into())
    }
    pub(super) fn poll_business(
        &mut self,
        store: &StateStoreOriginalOwner,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), MvPhaseFailure>> {
        self.progress(store, cx);
        if self.local_health_failure().is_some() {
            return Poll::Pending;
        }
        let result = self.poll_business_inner(store, owner, cx);
        self.progress(store, cx);
        result
    }
    fn poll_business_inner(
        &mut self,
        store: &StateStoreOriginalOwner,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), MvPhaseFailure>> {
        if let Some(result) = &self.selected {
            return Poll::Ready(result.clone());
        }
        if self.preparation.is_none() {
            if let Some(error) = &self.progress_refusal {
                return self.select(Err(original_error(error.clone()).into()));
            }
            if let Err(error) = self.install_reader(owner) {
                return self.select(Err(error));
            }
        }
        if self.source.is_none() {
            let p = self.preparation.as_mut().unwrap();
            if !p.received {
                self.in_foreign = true;
                let result = p.turn.as_mut().unwrap().poll_ready(cx);
                self.in_foreign = false;
                match std::task::ready!(result) {
                    Ok(()) => {
                        assert!(
                            p.reader.take_result_in(&mut p.result),
                            "same original CREATE reader result"
                        );
                    }
                    Err(MvTurnFailure::Admission(error)) => {
                        self.primary = Some(original_error(error).into());
                    }
                    Err(MvTurnFailure::Cpu(error)) => {
                        self.primary = Some(
                            QueryServiceError::from_execution_failure((*error).clone()).into(),
                        );
                    }
                    Err(MvTurnFailure::Health(error)) => {
                        self.health = Some(error);
                        return Poll::Pending;
                    }
                }
                p.received = true;
                if let Some(Err(error)) = p.result.as_ref() {
                    self.primary = Some(domain_failure(error.clone()));
                }
                if matches!(p.result.as_ref(), Some(Ok(output)) if !matches!(output, MvRefreshPreparationOutput::Create(_)))
                {
                    self.primary = Some(
                        service("CREATE reader returned a different operation's output").into(),
                    );
                }
            }
            if let Some(error) = self.primary.clone() {
                // Failed readers still close their actual CPU/I/O input before
                // business handoff. Unknown StateStore responsibility stays in Root.
                self.in_foreign = true;
                let closed = p.turn.as_mut().unwrap().poll_closed(cx);
                self.in_foreign = false;
                if let Err(health) = std::task::ready!(closed) {
                    self.health = Some(health);
                    return Poll::Pending;
                }
                return self.select(Err(error));
            }
            if self.plan.is_none() {
                let Some(Ok(MvRefreshPreparationOutput::Create(plan))) = p.result.as_ref() else {
                    unreachable!()
                };
                self.plan = Some(plan.clone());
            }
            let plan = self.plan.as_ref().unwrap().clone();
            if self.target.catalog.as_deref() != Some(plan.target.catalog.as_str())
                || self.target.database != plan.target.namespace
                || self.target.name != plan.target.table
            {
                return self.select(Err(service(
                    "CREATE plan does not match its original SQL target",
                )
                .into()));
            }
            let seed = match plan.projection_seed(
                &self.statement,
                self.input.current_catalog.as_deref(),
                &self.input.current_database,
            ) {
                Ok(seed) => Arc::new(seed),
                Err(error) => {
                    return self.select(Err(domain_failure(MvApplicationError::new(
                        MvApplicationErrorKind::InvalidRequest,
                        error,
                    ))));
                }
            };
            let planning = self.planning.as_ref().unwrap().clone();
            // poll_ready received the exact marker and worker/forwarder footer;
            // only now remove the reader from this same reusable CPU custody.
            if let Err(error) = p.turn.as_mut().unwrap().retire_transferred() {
                return self.select(Err(service(error).into()));
            }
            let cancellation =
                QueryCancellationView::governed(owner.cancellation().clone(), owner.timeout_ms());
            self.in_foreign = true;
            let constructed = CreateOriginal::new(CreateOriginalInput {
                plan,
                seed: seed.clone(),
                ports: self.input.ports.clone(),
                planning,
                operation_id: self.operation,
                context: self.context.clone(),
                cancellation,
                io: self.io.clone(),
                cpu: p.turn.take().unwrap(),
            });
            self.in_foreign = false;
            let source = match constructed {
                Ok(source) => source,
                Err(error) => return self.select(Err(domain_failure(error))),
            };
            // Stable Source, immutable command and product transition precede
            // every management/Provider/CPU publication poll.
            self.source = Some(source);
            self.transition = Some(MvCreateTransition::new(
                MvOperationContext {
                    operation_id: self.operation,
                },
                MvCreateCommand {
                    target: MvTarget::from_parts(
                        self.target.catalog.as_deref(),
                        &self.target.database,
                        &self.target.name,
                    ),
                    if_not_exists: self.statement.if_not_exists,
                    definition: seed.definition.clone(),
                    refresh: seed.refresh.clone(),
                    dependencies: seed.dependencies.clone(),
                },
            ));
        }
        let source = self.source.as_ref().unwrap().clone();
        if !self.prepared {
            self.in_foreign = true;
            let result = poll_phase(
                &source,
                &mut self.phase,
                &mut self.phase_result,
                Call::Prepare,
                cx,
            );
            self.in_foreign = false;
            match std::task::ready!(result) {
                Ok(()) => self.prepared = true,
                Err(error) => return self.select(Err(phase_failure(error))),
            }
        }
        let mut port = CreatePort {
            source: &source,
            phase: &mut self.phase,
            result: &mut self.phase_result,
            primary: &mut self.primary,
            store,
            io: &self.io,
            root: &self.root,
            window: &self.window,
            cpu: &self.cpu,
            input: &self.input,
            wake: &self.wake,
        };
        self.in_foreign = true;
        let progress =
            self.input
                .product
                .poll_create(cx, self.transition.as_mut().unwrap(), &mut port);
        self.in_foreign = false;
        if progress.is_pending() {
            return Poll::Pending;
        }
        let product = self
            .transition
            .as_ref()
            .unwrap()
            .result()
            .expect("original CREATE Product terminal");
        let result = select_product_result(product, &self.primary);
        self.select(result)
    }
    pub(super) fn poll_closed(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        if let Some(health) = self.local_health_failure() {
            return Poll::Ready(Err(health));
        }
        if let Some(source) = &self.source {
            self.in_foreign = true;
            let ready = source.poll_progress(cx);
            self.in_foreign = false;
            if ready.is_pending() {
                return Poll::Pending;
            }
            if self.phase.is_some() {
                source
                    .take_phase_result_in(&mut self.phase_result)
                    .map_err(|_| self.exit_unproven.clone())?;
                self.phase.take();
            }
            self.in_foreign = true;
            let closed = source.poll_closed(cx);
            self.in_foreign = false;
            if closed.is_pending() {
                return Poll::Pending;
            }
        }
        if let Some(p) = &mut self.preparation {
            if let Some(turn) = &mut p.turn {
                self.in_foreign = true;
                let closed = turn.poll_closed(cx);
                self.in_foreign = false;
                std::task::ready!(closed)?;
            }
            self.in_foreign = true;
            let closed = p.gate.poll_closed(cx);
            self.in_foreign = false;
            std::task::ready!(closed)?;
        }
        self.closed = true;
        Poll::Ready(Ok(()))
    }
}

#[derive(Clone, Copy)]
enum Call {
    Prepare,
    Stage,
    Publish,
    Abort,
    Register,
}
fn poll_phase(
    source: &Arc<CreateOriginal>,
    phase: &mut Option<CreatePhaseFuture>,
    result: &mut Option<Result<(), CreatePhaseFailure>>,
    call: Call,
    cx: &mut Context<'_>,
) -> Poll<Result<(), CreatePhaseFailure>> {
    if phase.is_none() {
        assert!(
            result.is_none(),
            "consume original CREATE response before next phase"
        );
        *phase = Some(match call {
            Call::Prepare => source.prepare(),
            Call::Stage => source.stage(),
            Call::Publish => source.publish(),
            Call::Abort => source.abort(),
            Call::Register => source.register(),
        });
    }
    std::task::ready!(Pin::new(phase.as_mut().unwrap()).poll(cx));
    source
        .take_phase_result_in(result)
        .map_err(CreatePhaseFailure::from)?;
    phase.take();
    Poll::Ready(result.take().unwrap())
}
struct CreatePort<'a> {
    source: &'a Arc<CreateOriginal>,
    phase: &'a mut Option<CreatePhaseFuture>,
    result: &'a mut Option<Result<(), CreatePhaseFailure>>,
    primary: &'a mut Option<MvPhaseFailure>,
    store: &'a StateStoreOriginalOwner,
    io: &'a Arc<MetadataIoAdmission>,
    root: &'a WorkScope,
    window: &'a ResultWindowAlias,
    cpu: &'a InternalResultCpu,
    input: &'a Arc<CommandInput>,
    wake: &'a Arc<MvWake>,
}
impl CreatePort<'_> {
    fn receive(&mut self, result: &Result<(), CreatePhaseFailure>) {
        if let Err(error) = result {
            self.primary
                .get_or_insert_with(|| phase_failure(error.clone()));
        }
    }
}
impl MvCreateProgressPort for CreatePort<'_> {
    fn poll_stage_target(
        &mut self,
        cx: &mut Context<'_>,
        operation: MvOperationContext,
        _: &MvCommand,
    ) -> Poll<Result<MvStagedTarget, MvProviderFailure>> {
        let result = std::task::ready!(poll_phase(
            self.source,
            self.phase,
            self.result,
            Call::Stage,
            cx
        ));
        self.receive(&result);
        result.map_err(|error| provider_failure(&error))?;
        let target = self
            .source
            .staged_target()
            .map_err(|error| provider_failure(&CreatePhaseFailure::from(error)))?;
        if target.staged_operation_id != operation.operation_id {
            return Poll::Ready(Err(contract_provider("CREATE staged another operation")));
        }
        Poll::Ready(Ok(target))
    }
    fn poll_publish_staged_target(
        &mut self,
        cx: &mut Context<'_>,
        _: MvOperationContext,
        _: &MvStagedTarget,
    ) -> Poll<Result<MvCreatePublicationOutcome, MvProviderFailure>> {
        let result = std::task::ready!(poll_phase(
            self.source,
            self.phase,
            self.result,
            Call::Publish,
            cx
        ));
        self.receive(&result);
        // The actual NoOp fact dominates this phase's finalize/management Err
        // for Product ordering. Its ordinary failure remains in primary and is
        // selected after Product terminal, never converted to abort permission.
        let outcome = self
            .source
            .product_publication_outcome()
            .map_err(|error| provider_failure(&CreatePhaseFailure::from(error)))?;
        Poll::Ready(select_publication_outcome(outcome, result))
    }
    fn poll_abort_staged_target(
        &mut self,
        cx: &mut Context<'_>,
        _: MvOperationContext,
        _: &MvStagedTarget,
    ) -> Poll<Result<(), MvProviderFailure>> {
        let result = std::task::ready!(poll_phase(
            self.source,
            self.phase,
            self.result,
            Call::Abort,
            cx
        ));
        self.receive(&result);
        Poll::Ready(result.map_err(|error| provider_failure(&error)))
    }
    fn poll_install_created_projection(
        &mut self,
        cx: &mut Context<'_>,
        _: MvOperationContext,
        _: &MvCreatedTarget,
    ) -> Poll<Result<(), MvProviderFailure>> {
        if self.phase.is_none() {
            let result = (|| -> Result<(), CreatePhaseFailure> {
                let needed = self
                    .source
                    .needed_committed()
                    .ok_or_else(|| invalid("CREATE Current has no actual Needed receipt"))?;
                if needed.effect != ExternalMutationEffect::Applied {
                    return Err(invalid("CREATE NoOp cannot grant Current").into());
                }
                let context = self
                    .source
                    .existing_finalization_context()
                    .ok_or_else(|| invalid("CREATE Current has no cached finite Context"))?
                    .map_err(|error| {
                        MvApplicationError::from_connector(
                            MvApplicationErrorKind::KnownCommittedFinalizeFailed,
                            error,
                        )
                    })?;
                let obligation = self
                    .io
                    .existing_metadata_obligation(self.root, &context)
                    .map_err(|cause| CreatePhaseFailure {
                        service: QueryServiceError::from_failure_cause(cause),
                        domain: None,
                    })?;
                let requests = self
                    .store
                    .existing_obligation_requests(
                        self.input.state_store.transaction_deadline,
                        obligation,
                    )
                    .map_err(|cause| CreatePhaseFailure {
                        service: QueryServiceError::from_failure_cause(cause),
                        domain: None,
                    })?;
                let cpu = Arc::new(CurrentProjectionMetadataCpu::existing_obligation(
                    self.cpu.clone(),
                    self.root.clone(),
                    self.window.clone(),
                    self.io.clone(),
                    context,
                    self.input.services.blocking.runtime().clone(),
                    self.wake.clone(),
                ));
                *self.phase = Some(self.source.project(requests, cpu));
                Ok(())
            })();
            if let Err(error) = result {
                self.receive(&Err(error.clone()));
                return Poll::Ready(Err(provider_failure(&error)));
            }
        }
        std::task::ready!(Pin::new(self.phase.as_mut().unwrap()).poll(cx));
        self.source
            .take_phase_result_in(self.result)
            .map_err(|error| provider_failure(&CreatePhaseFailure::from(error)))?;
        self.phase.take();
        let result = self.result.take().unwrap();
        self.receive(&result);
        Poll::Ready(result.map_err(|error| provider_failure(&error)))
    }
    fn poll_register_target(
        &mut self,
        cx: &mut Context<'_>,
        _: MvOperationContext,
        _: &MvCreatedTarget,
    ) -> Poll<Result<(), MvProviderFailure>> {
        let result = std::task::ready!(poll_phase(
            self.source,
            self.phase,
            self.result,
            Call::Register,
            cx
        ));
        self.receive(&result);
        Poll::Ready(result.map_err(|error| provider_failure(&error)))
    }
}
fn select_publication_outcome(
    outcome: Option<MvCreatePublicationOutcome>,
    result: Result<(), CreatePhaseFailure>,
) -> Result<MvCreatePublicationOutcome, MvProviderFailure> {
    if matches!(outcome, Some(MvCreatePublicationOutcome::NoOp)) {
        return Ok(MvCreatePublicationOutcome::NoOp);
    }
    result.map_err(|error| provider_failure(&error))?;
    outcome.ok_or_else(|| {
        contract_provider("CREATE publish completed without an original validated verdict")
    })
}
fn phase_failure(error: CreatePhaseFailure) -> MvPhaseFailure {
    MvPhaseFailure {
        service: error.service,
        domain: error.domain,
    }
}
fn domain_failure(error: MvApplicationError) -> MvPhaseFailure {
    phase_failure(error.into())
}
fn invalid(message: impl Into<String>) -> MvApplicationError {
    MvApplicationError::new(MvApplicationErrorKind::InvalidRequest, message)
}
fn contract_provider(message: &str) -> MvProviderFailure {
    MvProviderFailure::new(MvProviderFailureKind::Corruption, message)
}
fn provider_failure(error: &CreatePhaseFailure) -> MvProviderFailure {
    let kind = match error.domain.as_ref().map(MvApplicationError::kind) {
        Some(MvApplicationErrorKind::CommitUnknown) => MvProviderFailureKind::CommitUnknown,
        Some(MvApplicationErrorKind::KnownCommittedFinalizeFailed) => {
            MvProviderFailureKind::KnownCommittedFinalizeFailed
        }
        Some(MvApplicationErrorKind::Unavailable) => MvProviderFailureKind::Unavailable,
        Some(MvApplicationErrorKind::InvalidRequest) => MvProviderFailureKind::InvalidRequest,
        Some(MvApplicationErrorKind::BindingInvalidated) => MvProviderFailureKind::TargetReplaced,
        _ => MvProviderFailureKind::KnownUncommitted,
    };
    // Product carries domain diagnostics, not an already rendered SQL code.
    // The final typed command/service boundaries own public projection.
    let message = error
        .domain
        .as_ref()
        .map(MvApplicationError::message)
        .unwrap_or_else(|| error.service.message());
    let mut failure = MvProviderFailure::new(kind, message)
        .with_compile_control(error.service.compile_control_error())
        .with_unavailable_implementation(
            error
                .domain
                .as_ref()
                .and_then(|error| error.unavailable_implementation().cloned()),
        );
    if let Some(cause) = error.service.failure_cause() {
        failure = failure.with_original_failure(cause.clone());
    }
    failure
}
fn select_product_result(
    product: &Result<novarocks_mv_application::product::MvProductResult, MvProductError>,
    primary: &Option<MvPhaseFailure>,
) -> Result<(), MvPhaseFailure> {
    match product {
        Ok(_) => primary.clone().map_or(Ok(()), Err),
        Err(error) => {
            let mut domain = MvApplicationError::from_refresh_product(error);
            if let Some(primary) = primary
                .as_ref()
                .filter(|_| error.kind() != MvProductErrorKind::Corruption)
            {
                if let Some(cause) = primary.service.failure_cause() {
                    domain = domain.with_original_failure(cause.clone());
                }
                if primary.domain.is_some() {
                    // Product owns the settled aggregate, including cleanup
                    // diagnostics and whether a pure refusal still applies.
                    Err(domain_failure(domain))
                } else {
                    // A raw execution selection retains its exact input/site;
                    // the Product's settled domain remains a separate fact.
                    Err(MvPhaseFailure {
                        service: primary.service.clone(),
                        domain: Some(domain),
                    })
                }
            } else {
                if domain.original_failure().is_none() {
                    domain = domain.with_original_failure(
                        novarocks_failure_contract::SharedFailureCause::new(error.clone()),
                    );
                }
                Err(domain_failure(domain))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_failure_contract::{FailureProjection, SharedFailureCause};
    use novarocks_mv_application::{
        persistence::definition::CreateMvDefinitionRequest,
        repository::InitialMvRefreshConfiguration,
    };
    use novarocks_query_application::persisted_query_definition::{
        PersistedQueryDefinition, PersistedQueryDialect,
    };

    fn command(if_not_exists: bool) -> MvCreateCommand {
        MvCreateCommand {
            target: MvTarget::from_parts(Some("ice"), "db", "mv"),
            if_not_exists,
            definition: CreateMvDefinitionRequest {
                query_definition: PersistedQueryDefinition::new(
                    "SELECT 1",
                    PersistedQueryDialect::StarRocks,
                    "ice",
                    "db",
                )
                .unwrap(),
                base_table_refs: Vec::new(),
                primary_key_columns: Vec::new(),
                storage_engine: "iceberg".into(),
                target_catalog: Some("ice".into()),
                target_namespace: Some("db".into()),
                target_table: Some("mv".into()),
                created_at_ms: 1,
            },
            refresh: InitialMvRefreshConfiguration::default(),
            dependencies: Vec::new(),
        }
    }
    /// This is a Product ordering fixture, not a Provider/Root capability. It
    /// consumes the SAME production result-selection author used by CreatePort.
    struct NoOpPort {
        failure: CreatePhaseFailure,
        primary: Option<MvPhaseFailure>,
        events: Vec<&'static str>,
    }
    impl MvCreateProgressPort for NoOpPort {
        fn poll_stage_target(
            &mut self,
            _: &mut Context<'_>,
            operation: MvOperationContext,
            command: &MvCommand,
        ) -> Poll<Result<MvStagedTarget, MvProviderFailure>> {
            self.events.push("stage");
            let MvCommand::Create(command) = command else {
                panic!("CREATE")
            };
            Poll::Ready(Ok(MvStagedTarget {
                target: command.target.clone(),
                staged_operation_id: operation.operation_id,
            }))
        }
        fn poll_publish_staged_target(
            &mut self,
            _: &mut Context<'_>,
            _: MvOperationContext,
            _: &MvStagedTarget,
        ) -> Poll<Result<MvCreatePublicationOutcome, MvProviderFailure>> {
            self.events.push("publish");
            self.primary = Some(phase_failure(self.failure.clone()));
            Poll::Ready(select_publication_outcome(
                Some(MvCreatePublicationOutcome::NoOp),
                Err(self.failure.clone()),
            ))
        }
        fn poll_abort_staged_target(
            &mut self,
            _: &mut Context<'_>,
            _: MvOperationContext,
            _: &MvStagedTarget,
        ) -> Poll<Result<(), MvProviderFailure>> {
            panic!("NoOp cannot abort")
        }
        fn poll_install_created_projection(
            &mut self,
            _: &mut Context<'_>,
            _: MvOperationContext,
            _: &MvCreatedTarget,
        ) -> Poll<Result<(), MvProviderFailure>> {
            panic!("NoOp cannot install Current")
        }
        fn poll_register_target(
            &mut self,
            _: &mut Context<'_>,
            _: MvOperationContext,
            _: &MvCreatedTarget,
        ) -> Poll<Result<(), MvProviderFailure>> {
            panic!("NoOp cannot register")
        }
    }
    #[tokio::test(flavor = "current_thread")]
    async fn actual_create_product_noop_never_aborts_and_preserves_the_original_phase_failure() {
        let (service, _) = crate::mv::domain::test_admission::keyed_product().await;
        let operations = service.original_operations();
        let original = novarocks_spi::connector::ConnectorError::new(
            novarocks_spi::connector::ConnectorErrorKind::Unavailable,
            "original NoOp finalization failure",
        );
        let expected = original.failure_fact();
        let cause = SharedFailureCause::new(original);
        let domain = MvApplicationError::new(MvApplicationErrorKind::Engine, cause.detail())
            .with_original_failure(cause.clone());
        let mut port = NoOpPort {
            failure: domain.into(),
            primary: None,
            events: Vec::new(),
        };
        let mut transition = MvCreateTransition::new(
            MvOperationContext {
                operation_id: uuid::Uuid::now_v7(),
            },
            command(true),
        );
        let mut cx = Context::from_waker(std::task::Waker::noop());
        assert!(
            operations
                .poll_create(&mut cx, &mut transition, &mut port)
                .is_ready()
        );
        assert!(transition.result().unwrap().is_ok());
        let selected =
            select_product_result(transition.result().unwrap(), &port.primary).unwrap_err();
        assert_eq!(selected.service.failure_cause(), Some(&cause));
        assert_eq!(
            selected.service.failure_cause().unwrap().failure_fact(),
            expected
        );
        assert_eq!(port.events, ["stage", "publish"]);
        assert!(
            operations
                .poll_create(&mut cx, &mut transition, &mut port)
                .is_ready()
        );
        assert_eq!(port.events, ["stage", "publish"]);
    }
    #[tokio::test(flavor = "current_thread")]
    async fn unexpected_noop_keeps_the_actual_product_corruption_instead_of_aborting() {
        let (service, _) = crate::mv::domain::test_admission::keyed_product().await;
        let operations = service.original_operations();
        let mut port = NoOpPort {
            failure: invalid("independent NoOp finalization failure").into(),
            primary: None,
            events: Vec::new(),
        };
        let mut transition = MvCreateTransition::new(
            MvOperationContext {
                operation_id: uuid::Uuid::now_v7(),
            },
            command(false),
        );
        let mut cx = Context::from_waker(std::task::Waker::noop());
        assert!(
            operations
                .poll_create(&mut cx, &mut transition, &mut port)
                .is_ready()
        );
        let product = transition.result().unwrap().as_ref().unwrap_err();
        assert_eq!(product.kind(), MvProductErrorKind::Corruption);
        let selected =
            select_product_result(transition.result().unwrap(), &port.primary).unwrap_err();
        let selected_cause = selected.service.failure_cause().unwrap();
        assert_eq!(
            selected_cause
                .downcast_ref::<MvProductError>()
                .unwrap()
                .kind(),
            MvProductErrorKind::Corruption
        );
        assert_eq!(selected_cause.failure_fact(), product.failure_fact());
        assert_eq!(port.events, ["stage", "publish"]);
    }
}

#[cfg(test)]
mod create_contract_tests {
    use super::*;
    use novarocks_functions::{FunctionBindingError, FunctionOverloadId};
    use novarocks_sql::compiler::SqlCompileError;
    use novarocks_type_contract::CompileControlError;

    #[test]
    fn create_preanalysis_control_survives_provider_product_and_frontend_bridges() {
        for control in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let failure =
                CreatePhaseFailure::from(MvApplicationError::from_compile(control.clone().into()));
            assert_eq!(
                failure.service.compile_control_error(),
                Some(control.clone())
            );
            assert_eq!(
                failure.domain.as_ref().unwrap().compile_control_error(),
                Some(control.clone())
            );
            let product = provider_failure(&failure).into_product_error();
            let selected = select_product_result(&Err(product), &None).unwrap_err();
            assert_eq!(
                selected.service.compile_control_error(),
                Some(control.clone())
            );
            assert_eq!(
                selected.domain.as_ref().unwrap().compile_control_error(),
                Some(control)
            );
        }
    }

    #[test]
    fn create_analysis_refusal_survives_provider_product_and_frontend_bridges() {
        let compile = SqlCompileError::from(FunctionBindingError::UnavailableImplementation(
            FunctionOverloadId::try_new("fixture/mv/create/missing-pure-v1").unwrap(),
        ));
        let SqlCompileError::Analyze(expected) = &compile else {
            panic!("missing pure implementation must be typed analysis");
        };
        let failure = CreatePhaseFailure::from(MvApplicationError::from_compile(compile.clone()));
        assert_eq!(
            failure
                .domain
                .as_ref()
                .unwrap()
                .unavailable_implementation(),
            Some(expected)
        );
        let product = provider_failure(&failure).into_product_error();
        let selected = select_product_result(&Err(product), &None).unwrap_err();
        assert_eq!(
            selected
                .domain
                .as_ref()
                .unwrap()
                .unavailable_implementation(),
            Some(expected)
        );
        assert_eq!(selected.service.compile_control_error(), None);
    }

    /// Product ordering fixture, using the current phase projection and result
    /// selection. It grants no Provider, Root, or publication capability.
    struct Effects {
        finalize: bool,
        abort_fails: bool,
        compile: SqlCompileError,
        primary: Option<MvPhaseFailure>,
        events: Vec<&'static str>,
    }
    impl Effects {
        fn refusal(&mut self) -> MvProviderFailure {
            let failure =
                CreatePhaseFailure::from(MvApplicationError::from_compile(self.compile.clone()));
            self.primary
                .get_or_insert_with(|| phase_failure(failure.clone()));
            provider_failure(&failure)
        }
    }
    impl MvCreateProgressPort for Effects {
        fn poll_stage_target(
            &mut self,
            _: &mut Context<'_>,
            operation: MvOperationContext,
            command: &MvCommand,
        ) -> Poll<Result<MvStagedTarget, MvProviderFailure>> {
            self.events.push("stage");
            let MvCommand::Create(create) = command else {
                panic!("the actual CREATE owner must pass its CREATE command");
            };
            Poll::Ready(Ok(MvStagedTarget {
                target: create.target.clone(),
                staged_operation_id: operation.operation_id,
            }))
        }
        fn poll_publish_staged_target(
            &mut self,
            _: &mut Context<'_>,
            _: MvOperationContext,
            staged: &MvStagedTarget,
        ) -> Poll<Result<MvCreatePublicationOutcome, MvProviderFailure>> {
            self.events.push("publish");
            if !self.finalize {
                return Poll::Ready(Err(self.refusal()));
            }
            Poll::Ready(Ok(MvCreatePublicationOutcome::Applied(MvCreatedTarget {
                target: staged.target.clone(),
                object_id: novarocks_spi::connector::ConnectorTableObjectId::try_new(
                    bytes::Bytes::from_static(b"mv-object"),
                )
                .unwrap(),
            })))
        }
        fn poll_abort_staged_target(
            &mut self,
            _: &mut Context<'_>,
            _: MvOperationContext,
            _: &MvStagedTarget,
        ) -> Poll<Result<(), MvProviderFailure>> {
            self.events.push("abort");
            Poll::Ready(if self.abort_fails {
                Err(MvProviderFailure::new(
                    MvProviderFailureKind::Unavailable,
                    "abort failed",
                ))
            } else {
                Ok(())
            })
        }
        fn poll_install_created_projection(
            &mut self,
            _: &mut Context<'_>,
            _: MvOperationContext,
            _: &MvCreatedTarget,
        ) -> Poll<Result<(), MvProviderFailure>> {
            self.events.push("install");
            Poll::Ready(Err(self.refusal()))
        }
        fn poll_register_target(
            &mut self,
            _: &mut Context<'_>,
            _: MvOperationContext,
            _: &MvCreatedTarget,
        ) -> Poll<Result<(), MvProviderFailure>> {
            panic!("failed projection must not register a target");
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn actual_create_cleanup_and_committed_failures_do_not_publish_pure_refusal_code() {
        use novarocks_mv_application::{
            persistence::definition::CreateMvDefinitionRequest,
            repository::InitialMvRefreshConfiguration,
        };
        use novarocks_query_application::persisted_query_definition::{
            PersistedQueryDefinition, PersistedQueryDialect,
        };
        let compile = SqlCompileError::from(FunctionBindingError::UnavailableImplementation(
            FunctionOverloadId::try_new("fixture/mv/create/final-command/missing-pure-v1").unwrap(),
        ));
        let SqlCompileError::Analyze(analyze) = &compile else {
            panic!("the binding owner must provide typed analysis");
        };
        let diagnostic = analyze.to_string();
        let public = analyze.to_user_error(None).to_string();
        let (service, _) = crate::mv::domain::test_admission::keyed_product().await;
        let operations = service.original_operations();
        for (finalize, abort_fails) in [(false, false), (false, true), (true, false)] {
            let mut effects = Effects {
                finalize,
                abort_fails,
                compile: compile.clone(),
                primary: None,
                events: Vec::new(),
            };
            let create = MvCreateCommand {
                target: MvTarget::from_parts(Some("iceberg"), "db", "mv"),
                if_not_exists: false,
                definition: CreateMvDefinitionRequest {
                    query_definition: PersistedQueryDefinition::new(
                        "SELECT 1",
                        PersistedQueryDialect::StarRocks,
                        "iceberg",
                        "db",
                    )
                    .unwrap(),
                    base_table_refs: Vec::new(),
                    primary_key_columns: Vec::new(),
                    storage_engine: "iceberg".into(),
                    target_catalog: Some("iceberg".into()),
                    target_namespace: Some("db".into()),
                    target_table: Some("mv".into()),
                    created_at_ms: 1,
                },
                refresh: InitialMvRefreshConfiguration::default(),
                dependencies: Vec::new(),
            };
            let mut transition = MvCreateTransition::new(
                MvOperationContext {
                    operation_id: uuid::Uuid::now_v7(),
                },
                create,
            );
            let mut cx = Context::from_waker(std::task::Waker::noop());
            assert!(
                operations
                    .poll_create(&mut cx, &mut transition, &mut effects)
                    .is_ready()
            );
            let failure = transition.result().unwrap().as_ref().unwrap_err();
            assert_eq!(
                effects.events,
                if finalize {
                    vec!["stage", "publish", "install"]
                } else {
                    vec!["stage", "publish", "abort"]
                }
            );
            assert_eq!(
                failure.kind(),
                if finalize {
                    MvProductErrorKind::KnownCommittedFinalizeFailed
                } else {
                    MvProductErrorKind::InvalidRequest
                }
            );
            assert_eq!(
                failure.unavailable_implementation().is_some(),
                !finalize && !abort_fails
            );
            let selected = select_product_result(transition.result().unwrap(), &effects.primary)
                .expect_err("the current CREATE selection must preserve the real product result");
            let completion = super::super::MvCommandCompletionReceiving::default();
            completion.receive_selected_failure(&Err(selected.clone()));
            let domain = completion.observed_failure().unwrap();
            assert!(completion.observed().is_none());
            if finalize {
                assert_eq!(
                    domain.kind(),
                    MvApplicationErrorKind::KnownCommittedFinalizeFailed
                );
            }
            let command = crate::mv::command::project_product_failure_for_test(domain.clone());
            assert_eq!(
                selected.service.message(),
                crate::query::product_command_service_error(command.clone()).message()
            );
            assert_eq!(selected.service.compile_control_error(), None);
            if finalize {
                assert_eq!(
                    selected.service.external_effect_disposition(),
                    Some(StatementExternalEffectDisposition::KnownCommitted)
                );
            }
            assert_eq!(command.compile_control_error(), None);
            assert_eq!(
                command.kind(),
                novarocks_query_application::api::CommandErrorKind::Failed
            );
            if finalize || abort_fails {
                assert!(
                    !command
                        .to_string()
                        .starts_with("[sql.analyze.unavailable_implementation] ")
                );
                assert_eq!(
                    command.to_string(),
                    if abort_fails {
                        format!("{diagnostic}; staged target abort failed: abort failed")
                    } else {
                        diagnostic.clone()
                    }
                );
            } else {
                assert_eq!(command.to_string(), public);
            }
        }
    }

    fn aggregate_command() -> MvCreateCommand {
        use novarocks_mv_application::{
            persistence::definition::CreateMvDefinitionRequest,
            repository::InitialMvRefreshConfiguration,
        };
        use novarocks_query_application::persisted_query_definition::{
            PersistedQueryDefinition, PersistedQueryDialect,
        };
        MvCreateCommand {
            target: MvTarget::from_parts(Some("iceberg"), "db", "mv"),
            if_not_exists: false,
            definition: CreateMvDefinitionRequest {
                query_definition: PersistedQueryDefinition::new(
                    "SELECT 1",
                    PersistedQueryDialect::StarRocks,
                    "iceberg",
                    "db",
                )
                .unwrap(),
                base_table_refs: Vec::new(),
                primary_key_columns: Vec::new(),
                storage_engine: "iceberg".into(),
                target_catalog: Some("iceberg".into()),
                target_namespace: Some("db".into()),
                target_table: Some("mv".into()),
                created_at_ms: 1,
            },
            refresh: InitialMvRefreshConfiguration::default(),
            dependencies: Vec::new(),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn settled_create_aggregate_keeps_the_same_primary_cause_and_public_projection() {
        use novarocks_failure_contract::{FailureProjection, SharedFailureCause};
        let cause = SharedFailureCause::new(novarocks_workload_control::WorkError::Capacity(
            "private original CREATE compiler source",
        ));
        let (service, _) = crate::mv::domain::test_admission::keyed_product().await;
        let operations = service.original_operations();
        for (finalize, abort_fails) in [(false, true), (true, false)] {
            let mut effects = Effects {
                finalize,
                abort_fails,
                compile: SqlCompileError::CompilationSource(cause.clone()),
                primary: None,
                events: Vec::new(),
            };
            let mut transition = MvCreateTransition::new(
                MvOperationContext {
                    operation_id: uuid::Uuid::now_v7(),
                },
                aggregate_command(),
            );
            let mut cx = Context::from_waker(std::task::Waker::noop());
            assert!(
                operations
                    .poll_create(&mut cx, &mut transition, &mut effects)
                    .is_ready()
            );
            let selected =
                select_product_result(transition.result().unwrap(), &effects.primary).unwrap_err();
            let completion = super::super::MvCommandCompletionReceiving::default();
            completion.receive_selected_failure(&Err(selected.clone()));
            let observed = completion.observed_failure().unwrap();
            assert_eq!(observed.original_failure(), Some(&cause));
            assert_eq!(selected.service.failure_cause(), Some(&cause));
            assert_eq!(
                selected.service.failure_cause().unwrap().failure_fact(),
                cause.failure_fact()
            );
            assert_eq!(
                selected.service.execution_failure(),
                effects
                    .primary
                    .as_ref()
                    .unwrap()
                    .service
                    .execution_failure()
            );
            assert_eq!(
                observed.message(),
                transition.result().unwrap().as_ref().unwrap_err().message()
            );
            assert_eq!(
                observed.kind(),
                if finalize {
                    MvApplicationErrorKind::KnownCommittedFinalizeFailed
                } else {
                    MvApplicationErrorKind::InvalidRequest
                }
            );
            let command = crate::mv::command::project_product_failure_for_test(observed.clone());
            assert_eq!(command.failure_cause(), Some(&cause));
            assert_eq!(
                selected.service.message(),
                crate::query::product_command_service_error(command.clone()).message()
            );
            assert_eq!(
                selected.service.compile_control_error(),
                effects
                    .primary
                    .as_ref()
                    .unwrap()
                    .service
                    .compile_control_error()
            );
            if finalize {
                assert_eq!(
                    selected.service.external_effect_disposition(),
                    Some(StatementExternalEffectDisposition::KnownCommitted)
                );
            }
        }
    }

    #[test]
    fn settled_product_domain_does_not_replace_the_raw_execution_input() {
        use novarocks_failure_contract::{FailureProjection, SharedFailureCause};
        use novarocks_query_application::execution_error::{
            ExecutionFailureInput, ExecutionFailureSite,
        };
        let cause = SharedFailureCause::new(novarocks_workload_control::WorkError::Capacity(
            "original CPU capacity fact",
        ));
        let input = ExecutionFailureInput::new(
            cause.failure_fact(),
            novarocks_execution_contract::SafeDetail::truncating("selected original CPU detail"),
            ExecutionFailureSite::Coordinator,
        );
        let raw = QueryServiceError::from_execution_failure(input.clone());
        let primary = MvPhaseFailure {
            service: raw.clone(),
            domain: None,
        };
        // This is a Product result-selection fixture, not a Root, Provider or
        // lifecycle author. Its typed settled aggregate grants no new effect.
        let product = MvProductError::new(
            MvProductErrorKind::ProviderKnownUncommitted,
            "original CPU refusal; staged target abort failed: abort failed",
        );
        let selected = select_product_result(&Err(product.clone()), &Some(primary)).unwrap_err();
        assert_eq!(selected.service, raw);
        assert_eq!(selected.service.execution_failure(), Some(&input));
        assert_eq!(selected.service.failure_cause(), None);
        let completion = super::super::MvCommandCompletionReceiving::default();
        completion.receive_selected_failure(&Err(selected));
        let observed = completion.observed_failure().unwrap();
        assert_eq!(observed.kind(), MvApplicationErrorKind::Engine);
        assert_eq!(observed.message(), product.message());
        assert_eq!(observed.original_failure(), None);
        assert_eq!(observed.unavailable_implementation(), None);
        assert!(completion.observed().is_none());
    }
}
