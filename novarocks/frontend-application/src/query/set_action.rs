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

//! One original SET action owns a single required-computation CPU turn and
//! the live-state visibility cut. Its stable tail retains the real CPU exit.

use super::{
    FrontendQuerySession, QueryCancellationView, QueryServiceError, SessionSqlState,
    StatementResult, governed_cancellation_error, internal_error, poisoned_state,
};
use crate::query::compiler::FrontendQueryCompiler;
use crate::query::session_property_computation::{
    PreparedSessionProperties, SessionPropertyCpuInput,
};
use crate::query_execution::completion_facts::FrontendPlanningCpuSource;
use crate::query_execution::planning::sql_cancellation_observation;
use crate::query_execution::required_constant_eval::RequiredConstantInterpreter;
use novarocks_parser::ast;
use novarocks_query_application::api::StatementNativeHealthFailure;
use novarocks_query_application::session_control::{
    GovernedActionContext, GovernedQueryStatementOwner, GovernedStatementAction,
    GovernedStatementResidual, GovernedStatementVisibilitySealOutcome, StatementActionValue,
};
use novarocks_query_application::sql::session::registry::SessionPropertyRegistry;
use novarocks_sql::compiler::SqlCompileControl;
use novarocks_workload_control::WorkError;
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Instant,
};

type Preparation = Pin<
    Box<
        dyn Future<
                Output = Result<
                    Result<PreparedSessionProperties, QueryServiceError>,
                    StatementNativeHealthFailure,
                >,
            > + Send,
    >,
>;

/// The same future is polled to its real receive/exit even if the client waiter
/// is cancelled. No borrowed wait owns or replaces the submitted CPU job.
struct SetNativeTail {
    planning_cpu: Arc<FrontendPlanningCpuSource>,
    preparation: Option<Preparation>,
    prepared: Option<Result<PreparedSessionProperties, QueryServiceError>>,
    health: Option<StatementNativeHealthFailure>,
}
impl SetNativeTail {
    fn start(&mut self, input: SessionPropertyCpuInput) {
        assert!(
            self.preparation.is_none() && self.prepared.is_none(),
            "one original SET CPU turn"
        );
        let cpu = Arc::clone(&self.planning_cpu);
        self.preparation = Some(Box::pin(async move { cpu.session_properties(input).await }));
    }

    fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        self.health
            .clone()
            .or_else(|| self.planning_cpu.local_health_failure())
    }

    fn poll_preparation(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if let Some(future) = self.preparation.as_mut() {
            match future.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(result)) => self.prepared = Some(result),
                Poll::Ready(Err(failure)) => self.health = Some(failure),
            }
            drop(self.preparation.take());
        }
        if self.local_health_failure().is_some() {
            return Poll::Pending;
        }
        Poll::Ready(())
    }

    fn poll_closed(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if self.poll_preparation(cx).is_pending() {
            return Poll::Pending;
        }
        match self.planning_cpu.poll_closed(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(failure)) => {
                self.health = Some(failure);
                Poll::Pending
            }
            Poll::Ready(Ok(())) => Poll::Ready(()),
        }
    }
}
impl GovernedStatementResidual for SetNativeTail {
    fn poll_retirement(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        if let Some(health) = self.local_health_failure() {
            return Poll::Ready(Err(health));
        }
        let closed = self.poll_closed(cx);
        if let Some(health) = self.local_health_failure() {
            return Poll::Ready(Err(health));
        }
        closed.map(|()| Ok(()))
    }
}

struct SetStatementAction {
    source: Arc<str>,
    set: Option<ast::SetStatement>,
    live: Arc<Mutex<SessionSqlState>>,
    compiler: FrontendQueryCompiler,
    registry: Arc<SessionPropertyRegistry>,
    interpreter: Arc<RequiredConstantInterpreter>,
    control: SqlCompileControl,
    tail: Option<Box<SetNativeTail>>,
    error: Option<QueryServiceError>,
    delivered: bool,
}
// Design: ADR-0222 (docs/adr/ADR-0222-session-property-declarations-and-required-constant-evaluation.md)
impl SetStatementAction {
    fn poll_set(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatementActionValue, QueryServiceError>> {
        let tail = self.tail.as_mut().expect("the original whole-SET residual");
        if tail.local_health_failure().is_some() {
            return Poll::Pending;
        }
        if let Some(reason) = owner.cancellation().reason() {
            self.error
                .get_or_insert_with(|| governed_cancellation_error(reason));
        }
        if self.set.is_some() && self.error.is_none() {
            // Only copy the immutable initial session under this short lock.
            // Analysis, kernels and staged projection all run in the CPU turn.
            let staged = match self.live.lock() {
                Ok(live) => Some(live.clone()),
                Err(error) => {
                    self.error = Some(poisoned_state(error));
                    None
                }
            };
            if let Some(staged) = staged {
                match owner.result_window_alias() {
                    None => {
                        self.error = Some(internal_error("SET has no original Local result window"))
                    }
                    Some(window) => {
                        let input = self.compiler.session_property_input(
                            Arc::clone(&self.source),
                            self.set.take().expect("one admitted SET payload"),
                            staged,
                            Arc::clone(&self.registry),
                            Arc::clone(&self.interpreter),
                            self.control.clone(),
                            owner.scope().clone(),
                            window,
                        );
                        tail.start(input);
                    }
                }
            }
        }
        // Ordinary business refusal and cancellation still receive the same
        // CPU turn and prove its original exit before returning any result.
        if tail.poll_closed(cx).is_pending() {
            return Poll::Pending;
        }
        if let Some(Err(error)) = tail.prepared.as_ref() {
            self.error.get_or_insert_with(|| error.clone());
        }
        if let Some(error) = self.error.as_ref() {
            self.delivered = true;
            return Poll::Ready(Err(error.clone()));
        }
        let prepared = tail
            .prepared
            .as_mut()
            .expect("the received original SET CPU result")
            .as_mut()
            .expect("business refusal selected above");
        let mut live = match self.live.lock() {
            Ok(live) => live,
            Err(error) => {
                self.delivered = true;
                return Poll::Ready(Err(poisoned_state(error)));
            }
        };
        match owner.seal_success_visibility() {
            GovernedStatementVisibilitySealOutcome::Sealed => {
                // This cut moves only complete state. The old live value stays
                // in the original tail, so no destructor runs under this lock.
                std::mem::swap(&mut *live, &mut prepared.staged);
                self.delivered = true;
                Poll::Ready(Ok(StatementActionValue::SessionChanges(std::mem::take(
                    &mut prepared.effects,
                ))))
            }
            GovernedStatementVisibilitySealOutcome::Cancelled(reason) => {
                self.error = Some(governed_cancellation_error(reason));
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            GovernedStatementVisibilitySealOutcome::Stale
            | GovernedStatementVisibilitySealOutcome::Failed => {
                self.error = Some(internal_error(
                    "governed SET lost its statement generation before state commit",
                ));
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}
impl GovernedStatementAction for SetStatementAction {
    fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        self.tail
            .as_ref()
            .and_then(|tail| tail.local_health_failure())
    }
    fn handoff_residual_in(
        &mut self,
        target: &mut Option<Box<dyn GovernedStatementResidual>>,
    ) -> Result<(), StatementNativeHealthFailure> {
        if self.delivered {
            if target.is_some() {
                return Err(StatementNativeHealthFailure::actual_exit_unproven(
                    "SET residual receiver is occupied",
                    Arc::new(WorkError::Conflict),
                ));
            }
            *target = Some(self.tail.take().expect("the original whole-SET residual"));
        }
        Ok(())
    }
    fn poll_action(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatementActionValue, QueryServiceError>> {
        self.poll_set(owner, cx)
    }
}

impl FrontendQuerySession {
    pub(super) async fn execute_observed_set(
        &self,
        source: Arc<str>,
        set: ast::SetStatement,
        deadline: Option<Instant>,
        statement: GovernedQueryStatementOwner,
    ) -> Result<StatementResult, QueryServiceError> {
        let cancellation = QueryCancellationView::governed(
            statement.cancellation().clone(),
            statement.timeout_ms(),
        );
        let control = SqlCompileControl::new(deadline, sql_cancellation_observation(cancellation));
        let action = SetStatementAction {
            source,
            set: Some(set),
            live: Arc::clone(&self.state),
            compiler: self.service.query_compiler.clone(),
            registry: Arc::clone(&self.service.session_properties),
            interpreter: Arc::clone(&self.service.required_constant_interpreter),
            control,
            tail: Some(Box::new(SetNativeTail {
                planning_cpu: self.service.query_compiler.new_planning_cpu_source(),
                preparation: None,
                prepared: None,
                health: None,
            })),
            error: None,
            delivered: false,
        };
        self.execute_observed_statement_action(statement, Box::new(action))
            .await
    }
}

#[cfg(test)]
mod tests;
