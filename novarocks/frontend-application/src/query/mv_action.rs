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

//! The original SQL Root owns the entire serial MV command and every tail.
//! A data-producing step alone borrows the common Native stage author.

mod control;
mod control_create;
mod dependencies;
pub(crate) use control::MvControlRequest;
mod native;
mod step;

use crate::mv::domain::{application::MvRefreshRequest, iceberg_refresh::IcebergMvCorePorts};
use crate::query_execution::{
    completion_facts::CompletionFactOwners, logical_execution::LogicalExecutionLauncher,
    maintenance::io_admission::MetadataIoAdmission, mv_native_write::MvRefreshProviderActivation,
};
use crate::state_store::execution::{StateStoreOriginalBinding, StateStoreOriginalOwner};
use novarocks_mv_application::{
    activity::MvActivityOwner,
    service::{MvKeyedProductService, MvProductOperations},
};
use novarocks_query_application::{
    admitted_query_context::QueryExecutionContext,
    api::{
        CommandError, CommandErrorKind, StatementExternalEffectDisposition,
        StatementNativeHealthFailure,
    },
    cancellation::QueryCancellationView,
    session_control::{
        GovernedActionContext, GovernedStatementAction, GovernedStatementResidual,
        StatementActionValue,
    },
    session_error::{QueryServiceError, QueryServiceErrorKind},
};
use novarocks_spi::connector::{ConnectorInstanceId, ConnectorRequestContext};
use novarocks_sql::{planning::mv::SqlMvTarget, semantic::IcebergPartitionFieldExpr};
use novarocks_workload_control::{WorkError, WorkScope};
use std::{
    sync::Arc,
    task::{Context, Poll},
};

/// Process composition leaves only. No query admission, owner or runtime is
/// created by copying these handles into the original command.
#[derive(Clone)]
pub(crate) struct MvExecutionServices {
    pub(crate) client: novarocks_query_application::api::QueryExecutionClient,
    #[cfg(novarocks_dev_controls)]
    pub(crate) controls: Option<Arc<crate::mv::test_controls::MvTestControls>>,
    pub(crate) launcher: Arc<dyn LogicalExecutionLauncher>,
    pub(crate) completion: CompletionFactOwners,
    pub(crate) connector_control: Arc<novarocks_catalog_application::ConnectorControlHost>,
    pub(crate) blocking: crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor,
    pub(crate) data: crate::native::data_runtime::FrontendDataRuntime,
    pub(crate) state_store: Option<StateStoreOriginalBinding>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MvCommandCompletion {
    pub(crate) performed_publication: bool,
}
#[derive(Default)]
struct MvCommandCompletionFacts {
    completed: std::sync::OnceLock<MvCommandCompletion>,
    failure: std::sync::OnceLock<crate::mv::domain::application::MvApplicationError>,
}
#[derive(Clone, Default)]
pub(crate) struct MvCommandCompletionReceiving(Arc<MvCommandCompletionFacts>);
impl MvCommandCompletionReceiving {
    pub(crate) fn observed(&self) -> Option<&MvCommandCompletion> {
        self.0.completed.get()
    }
    pub(crate) fn observed_failure(
        &self,
    ) -> Option<&crate::mv::domain::application::MvApplicationError> {
        self.0.failure.get()
    }
    fn receive_selected_failure(&self, selected: &Result<(), MvPhaseFailure>) {
        if let Err(MvPhaseFailure {
            domain: Some(domain),
            ..
        }) = selected
        {
            self.0.failure.get_or_init(|| domain.clone());
        }
    }
}

/// A phase business result keeps the original domain policy next to its
/// service projection. Native/control failures without a domain remain None.
#[derive(Clone, Debug)]
pub(super) struct MvPhaseFailure {
    service: QueryServiceError,
    domain: Option<crate::mv::domain::application::MvApplicationError>,
}
impl From<QueryServiceError> for MvPhaseFailure {
    fn from(service: QueryServiceError) -> Self {
        Self {
            service,
            domain: None,
        }
    }
}

pub(crate) struct CreateRequest {
    pub(crate) target: SqlMvTarget,
    pub(crate) statement: crate::mv::domain::application::MvCreateStatement,
}

pub(crate) enum MvCommandRequest {
    Create(Box<CreateRequest>),
    Control(MvControlRequest),
    Refresh {
        statement: MvRefreshRequest,
        requested: SqlMvTarget,
    },
    ScheduledRefresh {
        statement: MvRefreshRequest,
        requested: SqlMvTarget,
    },
    Repartition {
        statement: MvRefreshRequest,
        target: SqlMvTarget,
        fields: Vec<IcebergPartitionFieldExpr>,
    },
}
impl MvCommandRequest {
    fn target(&self) -> &SqlMvTarget {
        match self {
            Self::Create(request) => &request.target,
            Self::Control(request) => request.target(),
            Self::Refresh { requested, .. } | Self::ScheduledRefresh { requested, .. } => requested,
            Self::Repartition { target, .. } => target,
        }
    }
    fn owner(&self) -> MvActivityOwner {
        match self {
            Self::Create(_) => MvActivityOwner::Create,
            Self::Control(request) => request.owner(),
            Self::Refresh { .. } => MvActivityOwner::ManualRefresh,
            Self::ScheduledRefresh { .. } => MvActivityOwner::ScheduledRefresh,
            Self::Repartition { .. } => MvActivityOwner::Repartition,
        }
    }
}
pub(super) enum StepRequest {
    Create(Box<CreateRequest>),
    Control(MvControlRequest),
    Iceberg {
        target: SqlMvTarget,
        statement: MvRefreshRequest,
        repartition: Option<Vec<IcebergPartitionFieldExpr>>,
    },
    Unsupported {
        display_name: String,
    },
}

struct CommandInput {
    state_store: StateStoreOriginalBinding,
    current_catalog: Option<String>,
    current_database: String,
    ports: IcebergMvCorePorts,
    activation: Arc<dyn MvRefreshProviderActivation>,
    product: Arc<MvProductOperations>,
    services: MvExecutionServices,
    request: MvCommandRequest,
    execution: QueryExecutionContext,
    context: ConnectorRequestContext,
}
#[derive(Debug, Default)]
struct MvWake(futures::task::AtomicWaker);
impl crate::task_execution::status_intake::StatusIntakeWake for MvWake {
    fn wake(&self) {
        self.0.wake();
    }
}
struct MvAction {
    original: Option<MvCommandOriginal>,
}
struct MvResidual {
    original: MvCommandOriginal,
}

/// The old dependency tail remains in this SAME Root while the next serial
/// step advances. The vector records actual prior Sources, not extra permits.
struct MvCommandOriginal {
    input: Arc<CommandInput>,
    root: WorkScope,
    origin: novarocks_workload_control::ResultWindowAlias,
    retained_health: StatementNativeHealthFailure,
    wake: Arc<MvWake>,
    io: Option<Arc<MetadataIoAdmission>>,
    store: Option<StateStoreOriginalOwner>,
    graph: Option<dependencies::DependencyPreparation>,
    steps: std::collections::VecDeque<StepRequest>,
    active: Option<step::MvStepOriginal>,
    retired: Vec<step::MvStepOriginal>,
    context_for_next: ConnectorRequestContext,
    progress_refusal: Option<WorkError>,
    initialized: bool,
    graph_received: bool,
    selected: Option<Result<(), MvPhaseFailure>>,
    health: Option<StatementNativeHealthFailure>,
    completion: MvCommandCompletionReceiving,
    performed_publication: bool,
}

pub(crate) fn prepare(
    current_catalog: Option<String>,
    current_database: String,
    ports: IcebergMvCorePorts,
    activation: Arc<dyn MvRefreshProviderActivation>,
    product: Arc<MvKeyedProductService>,
    services: MvExecutionServices,
    request: MvCommandRequest,
    execution: QueryExecutionContext,
    context: ConnectorRequestContext,
) -> Result<Box<dyn GovernedStatementAction>, CommandError> {
    prepare_in(
        current_catalog,
        current_database,
        ports,
        activation,
        product.original_operations(),
        services,
        request,
        execution,
        context,
        MvCommandCompletionReceiving::default(),
    )
}

pub(crate) fn prepare_in(
    current_catalog: Option<String>,
    current_database: String,
    ports: IcebergMvCorePorts,
    activation: Arc<dyn MvRefreshProviderActivation>,
    product: Arc<MvProductOperations>,
    services: MvExecutionServices,
    request: MvCommandRequest,
    execution: QueryExecutionContext,
    context: ConnectorRequestContext,
    completion: MvCommandCompletionReceiving,
) -> Result<Box<dyn GovernedStatementAction>, CommandError> {
    let state_store = services.state_store.clone().ok_or_else(|| {
        CommandError::new(
            CommandErrorKind::Rejected,
            "MV refresh requires a configured StateStore",
        )
    })?;
    let capacity = execution.result_capacity().ok_or_else(|| {
        CommandError::new(
            CommandErrorKind::Rejected,
            "MV command has no original admitted Root",
        )
    })?;
    let root = capacity.root_scope().clone();
    let origin = capacity.try_window_alias().map_err(|error| {
        CommandError::from_failure_cause(novarocks_failure_contract::SharedFailureCause::new(error))
    })?;
    // This is only a borrowed original identity. No Native window/position is
    // acquired until a real data assembly reaches MvNativePhase.
    let context_for_next = context.clone();
    let input = Arc::new(CommandInput {
        state_store,
        current_catalog,
        current_database,
        ports,
        activation,
        product,
        services,
        request,
        execution,
        context,
    });
    Ok(Box::new(MvAction {
        original: Some(MvCommandOriginal {
            input,
            root,
            origin,
            retained_health: StatementNativeHealthFailure::actual_exit_unproven(
                "MV original action exit is unproven",
                Arc::new(MvActionExitUnproven),
            ),
            wake: Arc::new(MvWake::default()),
            io: None,
            store: None,
            graph: None,
            steps: Default::default(),
            active: None,
            retired: Vec::new(),
            context_for_next,
            progress_refusal: None,
            initialized: false,
            graph_received: false,
            selected: None,
            health: None,
            completion,
            performed_publication: false,
        }),
    }))
}
fn service(message: impl ToString) -> QueryServiceError {
    QueryServiceError::new(QueryServiceErrorKind::Internal, message.to_string())
}
fn original_error(
    error: impl novarocks_failure_contract::FailureProjection
    + std::error::Error
    + Send
    + Sync
    + 'static,
) -> QueryServiceError {
    QueryServiceError::from_failure_cause(novarocks_failure_contract::SharedFailureCause::new(
        error,
    ))
}
impl MvCommandOriginal {
    fn selected_value(&self) -> Option<Result<StatementActionValue, QueryServiceError>> {
        let result = self.selected.as_ref()?;
        if result.is_ok() {
            let observed = self
                .completion
                .0
                .completed
                .get_or_init(|| MvCommandCompletion {
                    performed_publication: self.performed_publication,
                });
            assert_eq!(
                observed.performed_publication, self.performed_publication,
                "one original MV command completion"
            );
        }
        self.completion.receive_selected_failure(result);
        Some(
            result
                .clone()
                .map(|()| StatementActionValue::Ok(novarocks_query_application::session_control::StatementCommandCompletion::new(0)))
                .map_err(|failure| match self.observed_effect() {
                    Some(disposition) => failure
                        .service
                        .attach_external_effect_disposition(disposition),
                    None => failure.service,
                }),
        )
    }
    fn observed_effect(&self) -> Option<StatementExternalEffectDisposition> {
        // Each retained original owns its current publication knowledge. A
        // dispatched operation may still prove KnownUncommitted or NoOp; its
        // provisional Unknown must not become a second, permanent fact here.
        // Retired originals remain retained, including after physical close,
        // so prior steps still contribute their own actual effects.
        let mut observed = self
            .active
            .as_ref()
            .and_then(step::MvStepOriginal::external_effect_disposition);
        for original in &self.retired {
            observed = fold_effect(observed, original.external_effect_disposition());
        }
        observed
    }
    fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        self.health
            .clone()
            .or_else(|| self.io.as_ref().and_then(|io| io.health_failure()))
            .or_else(|| {
                self.active
                    .as_ref()
                    .and_then(step::MvStepOriginal::local_health_failure)
            })
            .or_else(|| {
                self.retired
                    .iter()
                    .find_map(step::MvStepOriginal::local_health_failure)
            })
            .or_else(|| {
                self.graph
                    .as_ref()
                    .and_then(dependencies::DependencyPreparation::local_health_failure)
            })
    }
    fn poll_initialize(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), QueryServiceError>> {
        if self.initialized {
            return Poll::Ready(Ok(()));
        }
        if !self.origin.is_for_scope(owner.scope()) {
            return Poll::Ready(Err(service("MV action borrowed another original Root")));
        }
        let work = owner
            .execution_owner_slot()
            .as_ref()
            .ok_or_else(|| service("MV action lost its original Root owner"))?;
        if self.io.is_none() {
            let mut dependencies = self.input.state_store.blocking_dependency_facts().to_vec();
            if !matches!(&self.input.request, MvCommandRequest::Control(_)) {
                let catalog = self
                    .input
                    .request
                    .target()
                    .catalog
                    .as_deref()
                    .ok_or_else(|| service("MV write requires an Iceberg catalog"))?;
                let selected = self
                    .input
                    .ports
                    .connector_control()
                    .acquire_current(&ConnectorInstanceId::parse(catalog).map_err(original_error)?)
                    .map_err(original_error)?;
                let write = self
                    .input
                    .services
                    .connector_control
                    .acquire_exact_write_stack(selected.control_runtime_id())
                    .map_err(original_error)?;
                let profile = write.session().io_profile().map_err(original_error)?;
                dependencies.extend_from_slice(profile.initial_dependencies());
            }
            self.io = Some(
                MetadataIoAdmission::for_original_source(work, dependencies)
                    .map_err(original_error)?,
            );
            // Install the sole original SS owner before constructing ANY weak
            // repository request context or polling a foreign future.
            self.store = Some(
                self.input
                    .state_store
                    .for_root(self.root.clone())
                    .map_err(QueryServiceError::from_failure_cause)?,
            );
        }
        match self.io.as_ref().unwrap().poll_progress(work, cx) {
            Ok(false) => return Poll::Pending,
            Err(error) => return Poll::Ready(Err(original_error(error))),
            Ok(true) => {}
        }
        let io = self.io.as_ref().unwrap().clone();
        let capacity = io.root_capacity().map_err(original_error)?;
        let requests = Arc::new(
            self.store
                .as_ref()
                .unwrap()
                .execution_requests(
                    self.input.state_store.transaction_deadline,
                    Some(self.input.context.deadline()),
                )
                .map_err(QueryServiceError::from_failure_cause)?,
        );
        let cancellation =
            QueryCancellationView::governed(owner.cancellation().clone(), owner.timeout_ms());
        self.graph = Some(dependencies::DependencyPreparation::new(
            self.input.clone(),
            io,
            capacity,
            requests,
            cancellation,
            self.wake.clone(),
        )?);
        self.initialized = true;
        Poll::Ready(Ok(()))
    }
    fn poll_business(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatementActionValue, QueryServiceError>> {
        self.wake.0.register(cx.waker());
        if let Some(result) = self.selected_value() {
            return Poll::Ready(result);
        }
        if let Poll::Ready(Err(error)) = self.poll_initialize(owner, cx) {
            self.selected = Some(Err(error.into()));
        }
        if !self.initialized {
            return match self.selected_value() {
                Some(result) => Poll::Ready(result),
                None => Poll::Pending,
            };
        }
        // Drive the actual independent receipts before polling a domain
        // future that may currently be waiting for one of those receipts.
        self.poll_original_progress(cx);
        if self.local_health_failure().is_some() {
            return Poll::Pending;
        }
        if !self.graph_received && self.selected.is_none() {
            let graph = self.graph.as_mut().unwrap();
            match graph.poll_steps(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(steps)) => {
                    self.steps = steps.into();
                    self.graph_received = true;
                }
                Poll::Ready(Err(error)) => {
                    self.selected = Some(Err(error));
                }
            }
        }
        loop {
            // Prior business Ready does not drive its Provider or Native tail.
            // Poll each original here even while the next serial step waits;
            // ordinary Pending never gates that next step's business work.
            self.poll_retired_progress(cx);
            if self.local_health_failure().is_some() {
                return Poll::Pending;
            }
            if self.selected.is_some() {
                break;
            }
            if self.active.is_none() {
                let Some(request) = self.steps.pop_front() else {
                    self.selected = Some(Ok(()));
                    break;
                };
                match step::MvStepOriginal::new(
                    self.input.clone(),
                    self.io.as_ref().unwrap().clone(),
                    self.store.as_ref().unwrap(),
                    request,
                    self.context_for_next.clone(),
                    self.wake.clone(),
                ) {
                    Ok(step) => self.active = Some(step),
                    Err(error) => {
                        self.selected = Some(Err(error.into()));
                        break;
                    }
                }
            }
            let polled = self.active.as_mut().unwrap().poll_business(
                self.store.as_ref().unwrap(),
                owner,
                cx,
            );
            match polled {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => {
                    if result.is_ok() {
                        self.performed_publication |=
                            self.active.as_ref().unwrap().performed_publication();
                    }
                    self.retired.push(self.active.take().unwrap());
                    match result {
                        Err(error) => {
                            self.selected = Some(Err(error));
                            break;
                        }
                        Ok(()) => {
                            self.context_for_next =
                                self.context_for_next.clone().after_external_effect();
                        }
                    }
                }
            }
            // Earlier actual cleanup remains in retired; it is never used as
            // an admission condition for the next independent business step.
        }
        Poll::Ready(self.selected_value().expect("selected original MV result"))
    }
    fn poll_retired_progress(&mut self, cx: &mut Context<'_>) {
        for original in &mut self.retired {
            if let Poll::Ready(Err(error)) = original.poll_closed(cx) {
                self.health.get_or_insert(error);
            }
        }
    }
    fn poll_original_progress(&mut self, cx: &mut Context<'_>) {
        if let Some(io) = &self.io
            && let Err(error) = io.poll_continuing(cx)
        {
            // Admission refusal is not a physical-exit verdict. The
            // same admitted child and its raw outcome remain retained.
            self.progress_refusal.get_or_insert(error);
        }
        if let Some(store) = &self.store
            && let Err(cause) = store.poll_progress(cx)
        {
            self.health.get_or_insert_with(|| {
                StatementNativeHealthFailure::actual_exit_unproven(
                    "MV original StateStore progress is unproven",
                    Arc::new(cause),
                )
            });
        }
    }
    fn poll_retirement(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        self.wake.0.register(cx.waker());
        self.poll_original_progress(cx);
        if self.local_health_failure().is_some() {
            return Poll::Pending;
        }
        let mut pending = false;
        let mut failed = None;
        let mut receive = |polled| match polled {
            Poll::Pending => pending = true,
            Poll::Ready(Err(error)) => {
                failed.get_or_insert(error);
            }
            Poll::Ready(Ok(())) => {}
        };
        let mut graph_closed = true;
        if let Some(graph) = self.graph.as_mut() {
            let polled = graph.poll_closed(cx);
            graph_closed = matches!(&polled, Poll::Ready(Ok(())));
            receive(polled);
        }
        if let Some(active) = self.active.as_mut() {
            receive(active.poll_closed(cx));
        }
        for original in &mut self.retired {
            receive(original.poll_closed(cx));
        }
        if let Some(error) = failed {
            self.health = Some(error);
            return Poll::Pending;
        }
        // Logical inventory may remain in these same originals after their
        // last possible domain request. Release the StateStore and MIO physical
        // resources independently, while retaining the original lease/custody.
        let physical_closed = graph_closed
            && self
                .active
                .as_ref()
                .is_none_or(step::MvStepOriginal::physical_exit_observed)
            && self
                .retired
                .iter()
                .all(step::MvStepOriginal::physical_exit_observed);
        if pending && !physical_closed {
            return Poll::Pending;
        }
        if !self.initialized
            && let Some(io) = &self.io
        {
            match io.withdraw_unstarted_in() {
                Ok(Some(_actual_withdrawal)) => {
                    self.io = None;
                }
                Ok(None) => {}
                Err(error) => {
                    self.health = Some(StatementNativeHealthFailure::actual_exit_unproven(
                        "MV unstarted original withdrawal is unproven",
                        Arc::new(error),
                    ));
                    return Poll::Pending;
                }
            }
        }
        // No domain future or CPU creator can start another request now.
        if let Some(store) = self.store.as_mut() {
            store.request_close();
            match store.poll_closed(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(cause)) => {
                    self.health = Some(StatementNativeHealthFailure::actual_exit_unproven(
                        "MV original StateStore close is unproven",
                        Arc::new(cause),
                    ));
                    return Poll::Pending;
                }
                Poll::Ready(Ok(())) => {}
            }
        }
        if let Some(io) = &self.io {
            match io.poll_actual_quiescent_continuing(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => {
                    self.health = Some(error);
                    return Poll::Pending;
                }
                Poll::Ready(Ok(_)) => {}
            }
        }
        if pending {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }
}
impl GovernedStatementAction for MvAction {
    fn external_effect_disposition(&self) -> Option<StatementExternalEffectDisposition> {
        self.original
            .as_ref()
            .and_then(MvCommandOriginal::observed_effect)
    }
    fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        self.original
            .as_ref()
            .and_then(MvCommandOriginal::local_health_failure)
    }
    fn handoff_residual_in(
        &mut self,
        target: &mut Option<Box<dyn GovernedStatementResidual>>,
    ) -> Result<(), StatementNativeHealthFailure> {
        // The receiving slot is checked before the unique original is moved.
        if target.is_some() {
            return Err(self
                .original
                .as_ref()
                .expect("original MV handoff")
                .retained_health
                .clone());
        }
        if let Some(original) = self.original.take() {
            *target = Some(Box::new(MvResidual { original }));
        }
        Ok(())
    }
    fn poll_action(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatementActionValue, QueryServiceError>> {
        self.original
            .as_mut()
            .expect("original MV action not handed off")
            .poll_business(owner, cx)
    }
}
impl GovernedStatementResidual for MvResidual {
    fn poll_retirement(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        self.original.poll_retirement(cx)
    }
}

#[derive(Debug)]
struct MvActionExitUnproven;
impl std::fmt::Display for MvActionExitUnproven {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MV original action poll or retirement did not return normally")
    }
}
impl std::error::Error for MvActionExitUnproven {}

fn fold_effect(
    a: Option<StatementExternalEffectDisposition>,
    b: Option<StatementExternalEffectDisposition>,
) -> Option<StatementExternalEffectDisposition> {
    use StatementExternalEffectDisposition::*;
    match (a, b) {
        (Some(KnownCommitted), _) | (_, Some(KnownCommitted)) => Some(KnownCommitted),
        (Some(CommitUnknown), _) | (_, Some(CommitUnknown)) => Some(CommitUnknown),
        (Some(KnownUncommitted), _) | (_, Some(KnownUncommitted)) => Some(KnownUncommitted),
        _ => None,
    }
}
