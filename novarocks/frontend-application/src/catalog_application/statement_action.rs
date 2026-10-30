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

//! Catalog mutations retain their original StateStore calls in the SQL Source.

use super::command::{CatalogCommandExecutor, lower_semantic_create_catalog};
use crate::query_execution::maintenance::io_admission::MetadataIoAdmission;
use crate::state_store::execution::{StateStoreOriginalBinding, StateStoreOriginalOwner};
use novarocks_catalog_application::{
    CatalogApplicationError, CatalogApplicationErrorKind, CatalogDropCommand,
};
use novarocks_failure_contract::SharedFailureCause;
use novarocks_query_application::{
    api::{CommandContext, StatementNativeHealthFailure},
    session_control::{
        GovernedActionContext, GovernedStatementAction, GovernedStatementResidual,
        StatementActionValue,
    },
    session_error::{QueryServiceError, QueryServiceErrorKind},
};
use novarocks_sql::semantic::CatalogSqlCommand;
use novarocks_workload_control::{ResultWindowAlias, WorkError, WorkScope};
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

type Business = Pin<Box<dyn Future<Output = Result<(), CatalogApplicationError>> + Send>>;
enum Mutation {
    Create(novarocks_catalog_application::CatalogCreateCommand),
    Drop(CatalogDropCommand),
}
struct State {
    application: Arc<dyn novarocks_catalog_application::CatalogApplicationPort>,
    binding: StateStoreOriginalBinding,
    receipt: Arc<novarocks_catalog_application::CatalogMutationReceipt>,
    scope: WorkScope,
    deadline: std::time::Instant,
    delivery: Option<ResultWindowAlias>,
    input: Option<Mutation>,
    io: Option<Arc<MetadataIoAdmission>>,
    original: Option<StateStoreOriginalOwner>,
    future: Option<Business>,
    outcome: Option<Result<(), CatalogApplicationError>>,
    progress_failure: Option<SharedFailureCause>,
    health: Option<StatementNativeHealthFailure>,
    started: bool,
    closed: bool,
}
struct Action {
    state: Option<State>,
}
struct Residual {
    state: State,
}
fn invalid(message: impl Into<String>) -> SharedFailureCause {
    SharedFailureCause::new(CatalogApplicationError::new(
        CatalogApplicationErrorKind::InvalidRequest,
        message,
    ))
}
fn health(cause: SharedFailureCause) -> StatementNativeHealthFailure {
    StatementNativeHealthFailure::actual_exit_unproven(
        "catalog StateStore original exit is unproven",
        Arc::new(cause),
    )
}
fn work(error: WorkError) -> QueryServiceError {
    work_at(error, "catalog original workload operation failed")
}
fn work_at(error: WorkError, detail: &'static str) -> QueryServiceError {
    QueryServiceError::from_failure_cause(SharedFailureCause::from_static_detail(error, detail))
}
pub(crate) fn prepare(
    executor: &CatalogCommandExecutor,
    binding: Option<StateStoreOriginalBinding>,
    command: CatalogSqlCommand,
    context: CommandContext,
) -> Result<Box<dyn GovernedStatementAction>, SharedFailureCause> {
    prepare_for_application(
        executor.original_catalog_application(),
        binding,
        command,
        context,
    )
}

pub(crate) fn prepare_for_application(
    application: Option<Arc<dyn novarocks_catalog_application::CatalogApplicationPort>>,
    binding: Option<StateStoreOriginalBinding>,
    command: CatalogSqlCommand,
    context: CommandContext,
) -> Result<Box<dyn GovernedStatementAction>, SharedFailureCause> {
    let input = match command {
        CatalogSqlCommand::CreateCatalog(command) => {
            Mutation::Create(lower_semantic_create_catalog(&command).map_err(invalid)?)
        }
        CatalogSqlCommand::DropCatalog { name, if_exists } => Mutation::Drop(CatalogDropCommand {
            instance_id: novarocks_spi::connector::ConnectorInstanceId::parse(
                &novarocks_types::naming::normalize_identifier(&name).map_err(invalid)?,
            )
            .map_err(|error| invalid(error.to_string()))?,
            if_exists,
        }),
        _ => {
            return Err(invalid(
                "catalog command does not belong to the asynchronous mutation author",
            ));
        }
    };
    let application = application.ok_or_else(|| invalid("catalog application is unavailable"))?;
    application
        .require_catalog_mutation_admission()
        .map_err(SharedFailureCause::new)?;
    let binding = binding.ok_or_else(|| {
        novarocks_state_store_api::StateStoreOriginalUnsupported.into_original_failure()
    })?;
    let delivery = context
        .result_window_alias()
        .ok_or_else(|| invalid("catalog mutation has no original result window"))?;
    if !delivery.is_for_scope(context.scope()) {
        return Err(invalid("catalog mutation has a foreign result window"));
    }
    Ok(Box::new(Action {
        state: Some(State {
            application,
            binding,
            receipt: Arc::new(novarocks_catalog_application::CatalogMutationReceipt::default()),
            scope: context.scope().clone(),
            deadline: context.connector_context().deadline(),
            delivery: Some(delivery),
            input: Some(input),
            io: None,
            original: None,
            future: None,
            outcome: None,
            progress_failure: None,
            health: None,
            started: false,
            closed: false,
        }),
    }))
}
impl State {
    fn external_effect(
        &self,
    ) -> Option<novarocks_query_application::api::StatementExternalEffectDisposition> {
        use novarocks_query_application::api::StatementExternalEffectDisposition as Effect;
        use novarocks_state_store_api::{AttemptOutcome, CommitObservationState};
        if self.receipt.has_committed_effect() {
            return Some(Effect::KnownCommitted);
        }
        // Read only the SAME issued slot. A reserved attempt is not dispatch,
        // and a terminal slot is not a proof of physical Source exit.
        match self
            .receipt
            .commit_observation()?
            .publication_state()
            .unwrap_or_else(|cause| std::panic::panic_any(cause))
        {
            CommitObservationState::NotDispatched => None,
            CommitObservationState::Dispatched => Some(Effect::CommitUnknown),
            CommitObservationState::Terminal(AttemptOutcome::Committed(_)) => {
                Some(Effect::KnownCommitted)
            }
            CommitObservationState::Terminal(AttemptOutcome::NotCommitted) => {
                Some(Effect::KnownUncommitted)
            }
            CommitObservationState::Terminal(AttemptOutcome::Unresolved) => {
                Some(Effect::CommitUnknown)
            }
        }
    }
    fn progress(&mut self, cx: &mut Context<'_>) {
        if let Some(original) = &self.original
            && let Err(cause) = original.poll_progress(cx)
        {
            self.progress_failure.get_or_insert(cause);
        }
    }
    fn poll_business(&mut self, cx: &mut Context<'_>) {
        self.progress(cx);
        if let Some(future) = self.future.as_mut()
            && let Poll::Ready(outcome) = future.as_mut().poll(cx)
        {
            // Whole domain error, SAME observation and committed evidence
            // are retained before any public result projection or Drop.
            self.outcome = Some(outcome);
            drop(self.future.take());
        }
        // The business poll may have just bound its first physical Source.
        self.progress(cx);
    }
    fn poll_close(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        if self.closed {
            return Poll::Ready(Ok(()));
        }
        if let Some(original) = &self.original {
            original.request_close();
        }
        self.poll_business(cx);
        if self.future.is_some() {
            return Poll::Pending;
        }
        if let Some(original) = self.original.as_mut() {
            std::task::ready!(original.poll_closed(cx)).map_err(health)?;
        }
        if let Some(io) = &self.io {
            if let Some(_observed) = io
                .withdraw_unstarted_in()
                .map_err(|e| health(SharedFailureCause::new(e)))?
            {
                self.closed = true;
                return Poll::Ready(Ok(()));
            }
            let _ = io.poll_continuing(cx);
            std::task::ready!(io.poll_actual_quiescent_continuing(cx))?;
        }
        self.closed = true;
        Poll::Ready(Ok(()))
    }
    fn local_health(&self) -> Option<StatementNativeHealthFailure> {
        self.health
            .clone()
            .or_else(|| {
                self.original
                    .as_ref()
                    .and_then(StateStoreOriginalOwner::local_health_failure)
                    .map(health)
            })
            .or_else(|| self.io.as_ref().and_then(|io| io.health_failure()))
    }
    fn poll_action(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatementActionValue, QueryServiceError>> {
        if !self
            .delivery
            .as_ref()
            .is_some_and(|window| window.is_for_scope(owner.scope()))
        {
            return Poll::Ready(Err(QueryServiceError::new(
                QueryServiceErrorKind::Internal,
                "catalog action has a foreign owner",
            )));
        }
        let work_owner = owner
            .execution_owner_slot()
            .as_ref()
            .ok_or_else(|| work(WorkError::NotReady))?;
        if self.io.is_none() {
            self.io = Some(
                MetadataIoAdmission::for_original_source(
                    work_owner,
                    self.binding.blocking_dependency_facts().to_vec(),
                )
                .map_err(|error| {
                    work_at(error, "catalog original metadata source creation failed")
                })?,
            );
        }
        if !self.started {
            if !self
                .io
                .as_ref()
                .unwrap()
                .poll_progress(work_owner, cx)
                .map_err(|error| work_at(error, "catalog original metadata admission failed"))?
            {
                return Poll::Pending;
            }
            let original = self
                .binding
                .for_root(self.scope.clone())
                .map_err(QueryServiceError::from_failure_cause)?;
            let context = original.context(self.deadline);
            self.original = Some(original);
            let application = self.application.clone();
            let receipt = self.receipt.clone();
            let input = self.input.take().expect("one original catalog mutation");
            self.future = Some(Box::pin(async move {
                match input {
                    Mutation::Create(command) => application
                        .create_catalog_in(command, &context, &receipt)
                        .await
                        .map(|_| ()),
                    Mutation::Drop(command) => {
                        application
                            .drop_catalog_in(command, &context, &receipt)
                            .await
                    }
                }
            }));
            self.started = true;
        }
        self.poll_business(cx);
        if self.future.is_some() {
            return Poll::Pending;
        }
        self.progress(cx);
        if !self
            .original
            .as_ref()
            .unwrap()
            .poll_actual_quiescent(cx)
            .map_err(QueryServiceError::from_failure_cause)?
        {
            return Poll::Pending;
        }
        let observed = std::task::ready!(
            self.io
                .as_ref()
                .unwrap()
                .poll_actual_quiescent_continuing(cx)
        );
        match observed {
            Ok(observed) => {
                if let Some(error) = observed.into_ordinary_failure() {
                    return Poll::Ready(Err(work_at(
                        error,
                        "catalog original metadata settlement failed",
                    )));
                }
            }
            Err(cause) => {
                self.health = Some(cause);
                return Poll::Ready(Err(QueryServiceError::new(
                    QueryServiceErrorKind::Internal,
                    "catalog physical exit failed",
                )));
            }
        }
        let outcome = self
            .outcome
            .as_ref()
            .expect("catalog business result received");
        Poll::Ready(match outcome {
            Ok(()) => Ok(StatementActionValue::Ok(
                novarocks_query_application::session_control::StatementCommandCompletion::new(0),
            )),
            Err(error) => Err(QueryServiceError::from_failure_cause(
                SharedFailureCause::new(error.clone()),
            )),
        })
    }
}
impl GovernedStatementAction for Action {
    fn poll_action(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatementActionValue, QueryServiceError>> {
        let state = self.state.as_mut().expect("same catalog action");
        match state.poll_action(owner, cx) {
            Poll::Ready(Err(error)) => Poll::Ready(Err(match state.external_effect() {
                Some(effect) => error.attach_external_effect_disposition(effect),
                None => error,
            })),
            other => other,
        }
    }
    fn external_effect_disposition(
        &self,
    ) -> Option<novarocks_query_application::api::StatementExternalEffectDisposition> {
        self.state.as_ref().and_then(State::external_effect)
    }
    fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        self.state.as_ref().and_then(State::local_health)
    }
    fn handoff_residual_in(
        &mut self,
        target: &mut Option<Box<dyn GovernedStatementResidual>>,
    ) -> Result<(), StatementNativeHealthFailure> {
        if target.is_some() {
            return Err(health(SharedFailureCause::new(WorkError::Conflict)));
        }
        if let Some(mut state) = self.state.take() {
            let delivery = state.delivery.take();
            *target = Some(Box::new(Residual { state }));
            drop(delivery);
        }
        Ok(())
    }
}
impl GovernedStatementResidual for Residual {
    fn poll_retirement(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        self.state.poll_close(cx)
    }
}
