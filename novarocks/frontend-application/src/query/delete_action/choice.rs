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

use super::super::statement_computation::statement_work_error;
use super::super::*;
use super::{metadata_commit::DeleteCommitInput, prepare_row};
use crate::catalog_application::resolver::TargetBackend;
use crate::connector::write_target::ConnectorWriteTargetBinding;
use crate::dml::attempt::{DmlPublicationAttempt, DmlPublicationPhase};
use crate::query_execution::completion_facts::FrontendCompletionCustody;
use crate::query_execution::dml::delete::{
    DeletePath, DeleteStatementInput, OwnedDeleteStatement, select_delete_path,
};
use crate::query_execution::kernels::DmlExecutionKernel;
use crate::query_execution::maintenance::io_admission::MetadataIoAdmission;
use crate::task_execution::blocking_io::{
    ConnectorBlockingIoAdmission, ConnectorBlockingIoResponsibility, ConnectorOperationJob,
};
use novarocks_query_application::api::{
    StatementExternalEffectDisposition, StatementNativeHealthFailure,
};
use novarocks_query_application::session_control::{
    GovernedActionContext, GovernedStatementAction, GovernedStatementResidual,
    StatementActionValue, StatementCommandCompletion,
};
use novarocks_spi::connector::{
    ConnectorControlPlanningPin, ConnectorMutationOperationId, ConnectorRequestContext,
    LakePublicationFamily, LakePublicationId, LakePublicationTarget,
};
use std::task::{Context, Poll, ready};

type ChoiceResult = Result<(ConnectorWriteTargetBinding, DeletePath), QueryServiceError>;
type ChoiceFuture = Pin<Box<dyn Future<Output = ChoiceResult> + Send>>;
type CloseFuture = Pin<Box<dyn Future<Output = Result<(), StatementNativeHealthFailure>> + Send>>;
type TopologyFuture =
    Pin<Box<dyn Future<Output = Result<BackendTopologySnapshot, QueryServiceError>> + Send>>;
type Completion = Result<StatementCommandCompletion, QueryServiceError>;

pub(super) struct ChoiceStart {
    pub(super) kernel: DmlExecutionKernel,
    pub(super) input: DeleteStatementInput,
    pub(super) target: TargetBackend,
    pub(super) reference: String,
    pub(super) planning: ConnectorControlPlanningPin,
    pub(super) io: Arc<MetadataIoAdmission>,
    pub(super) context: ConnectorRequestContext,
}

pub(super) fn prepare(start: ChoiceStart) -> Box<dyn GovernedStatementAction> {
    Box::new(DeleteChoice {
        kernel: start.kernel,
        original: Some(start.input),
        target: start.target,
        reference: start.reference,
        planning: start.planning,
        io: start.io,
        context: start.context,
        facts: FrontendCompletionCustody::new(),
        choice: None,
        chosen: None,
        closing: None,
        facts_closed: false,
        bootstrap_ready: false,
        topology: None,
        action: None,
        commit_admission: None,
        commit_input: None,
        responsibility: None,
        job: None,
        attempt: None,
        answer: None,
        health: None,
    })
}

struct DeleteChoice {
    kernel: DmlExecutionKernel,
    original: Option<DeleteStatementInput>,
    target: TargetBackend,
    reference: String,
    planning: ConnectorControlPlanningPin,
    io: Arc<MetadataIoAdmission>,
    context: ConnectorRequestContext,
    facts: Arc<FrontendCompletionCustody>,
    choice: Option<ChoiceFuture>,
    chosen: Option<ChoiceResult>,
    closing: Option<CloseFuture>,
    facts_closed: bool,
    bootstrap_ready: bool,
    topology: Option<TopologyFuture>,
    action: Option<Box<dyn GovernedStatementAction>>,
    commit_admission: Option<ConnectorBlockingIoAdmission>,
    commit_input: Option<DeleteCommitInput>,
    responsibility: Option<ConnectorBlockingIoResponsibility>,
    job: Option<ConnectorOperationJob<Completion>>,
    attempt: Option<Arc<Mutex<DmlPublicationAttempt>>>,
    answer: Option<Completion>,
    health: Option<StatementNativeHealthFailure>,
}

impl DeleteChoice {
    fn start_choice(&self) -> ChoiceFuture {
        let input = self.original.as_ref().expect("DELETE original input");
        let context = input.context.clone();
        let statement = input.statement.clone();
        let kernel = self.kernel.clone();
        let target = self.target.clone();
        let planning = self.planning.clone();
        let connector = self.context.clone();
        let facts = Arc::clone(&self.facts);
        let sql = input.sql.clone();
        Box::pin(async move {
            let statement = match statement {
                OwnedDeleteStatement::Predicate(statement) => statement,
                #[cfg(novarocks_dev_controls)]
                OwnedDeleteStatement::TrinoEquality { .. } => {
                    unreachable!("the dev equality operation uses the existing row action");
                }
            };
            let convert = |error: crate::dml::error::DmlExecutionError| {
                routed_execution_service_error(
                    dml_result::<()>(Err(error.into_dml_error(Some(&sql)))).unwrap_err(),
                )
            };
            let (original, decision) = select_delete_path(
                &kernel, &target, planning, &statement, &context, &connector, &facts,
            )
            .await
            .map_err(convert)?;
            Ok((original, decision.path))
        })
    }

    fn prepare_commit(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        target: ConnectorWriteTargetBinding,
        condition: novarocks_spi::connector::ConnectorDeleteCondition,
        payload: Option<novarocks_spi::connector::ConnectorProcessLocalPayload>,
    ) -> Result<(), QueryServiceError> {
        let publication = LakePublicationId::new_v7();
        let attempt = Arc::new(Mutex::new(DmlPublicationAttempt::new(
            publication,
            LakePublicationFamily::DataMutation,
            LakePublicationTarget::try_new(
                self.target.catalog.clone(),
                self.target.namespace.clone(),
                Some(self.target.table.clone()),
                None,
            )
            .map_err(|error| internal_error(error.to_string()))?,
            None,
        )));
        let window = owner
            .result_window_alias()
            .ok_or_else(|| internal_error("metadata DELETE has no original result window"))?;
        // Loading the original table has already bound the Root's metadata
        // dependencies. Only its paid child receipt can carry them into the
        // blocking publication.
        self.commit_admission = Some(
            ConnectorBlockingIoResponsibility::request(owner.scope(), &window)
                .map_err(statement_work_error)?,
        );
        self.attempt = Some(attempt.clone());
        self.commit_input = Some(DeleteCommitInput {
            kernel: self.kernel.clone(),
            target,
            context: self.context.clone(),
            condition,
            payload,
            operation: ConnectorMutationOperationId::from_bytes(publication.to_bytes()),
            attempt,
        });
        Ok(())
    }

    fn poll_commit(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if let Some(admission) = self.commit_admission.as_mut() {
            let responsibility = ready!(admission.poll_ready(cx));
            self.commit_admission = None;
            let prepared =
                responsibility
                    .map_err(statement_work_error)
                    .and_then(|responsibility| {
                        let (blocking, _) = self.kernel.write_runtime().map_err(internal_error)?;
                        self.responsibility = Some(responsibility);
                        blocking
                            .prepare_blocking_owned_in(
                                self.responsibility.as_ref().unwrap(),
                                &mut self.commit_input,
                                DeleteCommitInput::run,
                                &mut self.job,
                            )
                            .map_err(statement_work_error)
                    });
            if let Err(error) = prepared {
                drop(self.commit_input.take());
                self.answer = Some(Err(error));
                return Poll::Ready(());
            }
        }
        self.observe_worker(cx)
    }

    fn observe_worker(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        let Some(job) = self.job.as_mut() else {
            return Poll::Ready(());
        };
        let business = ready!(job.poll_progress(cx));
        match business {
            Ok(()) => {
                self.answer = job
                    .try_take()
                    .map(|result| result.map_err(|error| internal_error(error.to_string()))?);
            }
            Err(cause) => {
                let mut completed = None;
                let _ = job.take_completed_value_in(&mut completed);
                let closed = if job.actual_exit_observed() {
                    !job.source_drop_unproven() && !job.input_or_value_retained()
                } else {
                    job.withdraw_unpublished_blocking()
                };
                if !closed {
                    self.health = Some(StatementNativeHealthFailure::actual_exit_unproven(
                        "metadata DELETE job actual exit is unproven",
                        cause.health_cause(),
                    ));
                    return Poll::Pending;
                }
                let error = cause.admission_refusal().map_or_else(
                    || internal_error(cause.to_string()),
                    |error| statement_work_error(error.clone()),
                );
                self.answer = Some(Err(completed.and_then(Result::err).unwrap_or(error)));
            }
        }
        if self.answer.is_none() {
            self.answer = Some(Err(internal_error(
                "metadata DELETE job lost its original output",
            )));
        }
        self.job = None;
        self.responsibility = None;
        Poll::Ready(())
    }
}

impl GovernedStatementAction for DeleteChoice {
    fn external_effect_disposition(&self) -> Option<StatementExternalEffectDisposition> {
        if let Some(action) = &self.action {
            return action.external_effect_disposition();
        }
        let attempt = self.attempt.as_ref()?;
        let attempt = attempt.lock().unwrap_or_else(|poison| poison.into_inner());
        attempt.terminal().map_or_else(
            || {
                (attempt.phase() == DmlPublicationPhase::DispatchPossible)
                    .then_some(StatementExternalEffectDisposition::CommitUnknown)
            },
            |terminal| Some(external_effect_from_lake(terminal.disposition())),
        )
    }

    fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        self.health
            .clone()
            .or_else(|| self.facts.local_health_failure())
            .or_else(|| self.io.health_failure())
            .or_else(|| {
                self.action
                    .as_ref()
                    .and_then(|action| action.local_health_failure())
            })
    }

    fn handoff_residual_in(
        &mut self,
        target: &mut Option<Box<dyn GovernedStatementResidual>>,
    ) -> Result<(), StatementNativeHealthFailure> {
        if let Some(action) = &mut self.action {
            return action.handoff_residual_in(target);
        }
        Ok(())
    }

    fn poll_action(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatementActionValue, QueryServiceError>> {
        if let Some(action) = &mut self.action {
            return action.poll_action(owner, cx);
        }
        if self.health.is_some() {
            return Poll::Pending;
        }
        if self.answer.is_none() {
            let progress = if self.bootstrap_ready {
                self.io.poll_actual_progress_continuing(cx)
            } else {
                match owner.execution_owner_slot().as_ref() {
                    Some(original) => self.io.poll_progress(original, cx),
                    None => Err(novarocks_workload_control::WorkError::Conflict),
                }
            };
            match progress {
                Ok(false) if !self.bootstrap_ready => return Poll::Pending,
                Ok(ready) => self.bootstrap_ready |= ready,
                Err(error)
                    if self.choice.is_none()
                        && self.job.is_none()
                        && self.commit_admission.is_none()
                        && self.closing.is_none() =>
                {
                    self.chosen = None;
                    self.topology = None;
                    self.answer = Some(Err(statement_work_error(error)));
                }
                Err(_) => {}
            }
        }

        if self.commit_admission.is_some() || self.job.is_some() {
            ready!(self.poll_commit(cx));
        }
        if self.answer.is_none() {
            if !self.facts_closed {
                if self.chosen.is_none() {
                    if self.choice.is_none() {
                        self.choice = Some(self.start_choice());
                    }
                    self.chosen = Some(ready!(self.choice.as_mut().unwrap().as_mut().poll(cx)));
                    self.choice = None;
                }
                if self.closing.is_none() {
                    let original = Arc::clone(&self.facts);
                    self.closing = Some(Box::pin(async move { original.close().await }));
                }
                let result = ready!(self.closing.as_mut().unwrap().as_mut().poll(cx));
                self.closing = None;
                if let Err(health) = result {
                    self.health = Some(health);
                    return Poll::Pending;
                }
                self.facts_closed = true;
            }
            let choice = self.chosen.take().expect("DELETE prefix produced a choice");
            match choice {
                Err(error) => self.answer = Some(Err(error)),
                Ok((_, DeletePath::Empty)) => {
                    self.answer = Some(Ok(StatementCommandCompletion::new(0)));
                }
                Ok((original, DeletePath::Metadata { condition, payload })) => {
                    if let Err(error) = self.prepare_commit(owner, original, condition, payload) {
                        self.answer = Some(Err(error));
                    } else {
                        ready!(self.poll_commit(cx));
                    }
                }
                Ok((original, DeletePath::RowLevel)) => {
                    let input = self.original.as_ref().expect("DELETE original input");
                    let row = if input.context.execution().topology().targets().is_empty() {
                        if self.topology.is_none() {
                            let topology = input.topology.clone();
                            let cancellation = input.context.execution().cancellation().clone();
                            let deadline = input.context.execution().deadline();
                            self.topology = Some(Box::pin(async move {
                                wait_for_initial_query_topology(&topology, &cancellation, deadline)
                                    .await
                            }));
                        }
                        let topology = match self.topology.as_mut().unwrap().as_mut().poll(cx) {
                            Poll::Pending => {
                                self.chosen = Some(Ok((original, DeletePath::RowLevel)));
                                return Poll::Pending;
                            }
                            Poll::Ready(result) => result,
                        };
                        self.topology = None;
                        topology
                            .and_then(|topology| {
                                let execution = input
                                    .context
                                    .execution()
                                    .clone()
                                    .with_first_compute_topology(topology)
                                    .map_err(|error| internal_error(error.to_string()))?;
                                Ok(RequestContext::new(
                                    input.context.session().clone(),
                                    execution,
                                ))
                            })
                            .map(Some)
                    } else {
                        Ok(None)
                    };
                    match row.and_then(|context| {
                        let mut input = self.original.take().expect("one original DELETE input");
                        if let Some(context) = context {
                            input.context = context;
                        }
                        prepare_row(
                            self.kernel.clone(),
                            input,
                            self.target.clone(),
                            self.reference.clone(),
                            self.planning.clone(),
                            Arc::clone(&self.io),
                            Some(original),
                        )
                        .map_err(|error| error.into_service())
                    }) {
                        Ok(action) => {
                            self.action = Some(action);
                            return self.action.as_mut().unwrap().poll_action(owner, cx);
                        }
                        Err(error) => self.answer = Some(Err(error)),
                    }
                }
            }
        }
        let mut exit = None;
        if !self.bootstrap_ready {
            match self.io.withdraw_unstarted_in() {
                Ok(observed) => exit = observed,
                Err(error) => {
                    self.health = Some(StatementNativeHealthFailure::actual_exit_unproven(
                        "metadata DELETE bootstrap withdrawal is unproven",
                        Arc::new(error),
                    ));
                    return Poll::Pending;
                }
            }
        }
        let observed = match exit {
            Some(observed) => observed,
            None => match ready!(self.io.poll_actual_quiescent_continuing(cx)) {
                Ok(observed) => observed,
                Err(health) => {
                    self.health = Some(health);
                    return Poll::Pending;
                }
            },
        };
        self.responsibility = None;
        let answer = self.answer.take().expect("one DELETE completion");
        let answer = answer.and_then(|completion| {
            let Some(error) = observed.into_ordinary_failure() else {
                return Ok(completion);
            };
            let disposition = self.external_effect_disposition();
            if matches!(error, novarocks_workload_control::WorkError::Cancelled(_))
                && disposition == Some(StatementExternalEffectDisposition::KnownCommitted)
            {
                return Ok(completion);
            }
            let error = statement_work_error(error);
            Err(match disposition {
                Some(disposition) => error.attach_external_effect_disposition(disposition),
                None => error,
            })
        });
        Poll::Ready(answer.map(StatementActionValue::Ok))
    }
}
