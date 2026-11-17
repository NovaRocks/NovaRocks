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

//! CREATE CATALOG prepares properties on the original planning CPU before
//! handing the evaluated command to the original StateStore statement action.

use super::{
    FrontendQuerySession, QueryCancellationView, QueryServiceError, StatementResult,
    governed_cancellation_error, internal_error, product_command_error,
    routed_execution_service_error,
};
use crate::query::{
    catalog_property_computation::CatalogPropertyCpuInput, compiler::FrontendQueryCompiler,
};
use crate::query_execution::{
    completion_facts::FrontendPlanningCpuSource, planning::sql_cancellation_observation,
    required_constant_eval::RequiredConstantInterpreter,
};
use novarocks_parser::ast;
use novarocks_query_application::{
    admitted_query_context::RequestContext,
    api::{CommandContext, StatementExternalEffectDisposition, StatementNativeHealthFailure},
    session_control::{
        GovernedActionContext, GovernedQueryStatementOwner, GovernedStatementAction,
        GovernedStatementResidual, StatementActionValue,
    },
    sql::ProductCommandRouter,
};
use novarocks_sql::{
    compiler::SqlCompileControl,
    semantic::{CatalogSqlCommand, command::CatalogCreateCommand},
};
use novarocks_workload_control::WorkError;
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Instant,
};

type Preparation = Pin<
    Box<
        dyn Future<
                Output = Result<
                    Result<CatalogCreateCommand, QueryServiceError>,
                    StatementNativeHealthFailure,
                >,
            > + Send,
    >,
>;
struct CatalogNativeTail {
    cpu: Arc<FrontendPlanningCpuSource>,
    preparation: Option<Preparation>,
    prepared: Option<Result<CatalogCreateCommand, QueryServiceError>>,
    inner: Option<Box<dyn GovernedStatementAction>>,
    inner_residual: Option<Box<dyn GovernedStatementResidual>>,
    health: Option<StatementNativeHealthFailure>,
}
impl CatalogNativeTail {
    fn local_health(&self) -> Option<StatementNativeHealthFailure> {
        self.health
            .clone()
            .or_else(|| self.cpu.local_health_failure())
            .or_else(|| {
                self.inner
                    .as_ref()
                    .and_then(|action| action.local_health_failure())
            })
    }
    fn start(&mut self, input: CatalogPropertyCpuInput) {
        assert!(self.preparation.is_none() && self.prepared.is_none());
        let cpu = self.cpu.clone();
        self.preparation = Some(Box::pin(async move { cpu.catalog_properties(input).await }));
    }
    fn poll_cpu_closed(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if let Some(future) = self.preparation.as_mut() {
            match future.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(result)) => self.prepared = Some(result),
                Poll::Ready(Err(health)) => self.health = Some(health),
            }
            drop(self.preparation.take());
        }
        if self.local_health().is_some() {
            return Poll::Pending;
        }
        match self.cpu.poll_closed(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(health)) => {
                self.health = Some(health);
                Poll::Pending
            }
            Poll::Ready(Ok(())) => Poll::Ready(()),
        }
    }
}
impl GovernedStatementResidual for CatalogNativeTail {
    fn poll_retirement(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        if let Some(health) = self.local_health() {
            return Poll::Ready(Err(health));
        }
        let cpu = self.poll_cpu_closed(cx);
        if let Some(health) = self.local_health() {
            return Poll::Ready(Err(health));
        }
        std::task::ready!(cpu);
        if let Some(residual) = self.inner_residual.as_mut() {
            return residual.poll_retirement(cx);
        }
        Poll::Ready(Ok(()))
    }
}
struct CatalogCreateAction {
    source: Arc<str>,
    pending: Option<ast::legacy::CreateCatalog>,
    compiler: FrontendQueryCompiler,
    interpreter: Arc<RequiredConstantInterpreter>,
    router: ProductCommandRouter,
    context: Option<(RequestContext, CommandContext)>,
    control: SqlCompileControl,
    tail: Option<Box<CatalogNativeTail>>,
    error: Option<QueryServiceError>,
    delivered: bool,
}
impl GovernedStatementAction for CatalogCreateAction {
    fn external_effect_disposition(&self) -> Option<StatementExternalEffectDisposition> {
        self.tail.as_ref().and_then(|tail| {
            tail.inner
                .as_ref()
                .and_then(|action| action.external_effect_disposition())
        })
    }
    fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        self.tail.as_ref().and_then(|tail| tail.local_health())
    }
    fn handoff_residual_in(
        &mut self,
        target: &mut Option<Box<dyn GovernedStatementResidual>>,
    ) -> Result<(), StatementNativeHealthFailure> {
        if !self.delivered {
            return Ok(());
        }
        if target.is_some() {
            return Err(StatementNativeHealthFailure::actual_exit_unproven(
                "catalog property residual receiver is occupied",
                Arc::new(WorkError::Conflict),
            ));
        }
        let tail = self.tail.as_mut().expect("same catalog property residual");
        if let Some(inner) = tail.inner.as_mut() {
            inner.handoff_residual_in(&mut tail.inner_residual)?;
        }
        *target = Some(self.tail.take().expect("same catalog property residual"));
        Ok(())
    }
    fn poll_action(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatementActionValue, QueryServiceError>> {
        let tail = self.tail.as_mut().expect("same catalog property residual");
        if tail.local_health().is_some() {
            return Poll::Pending;
        }
        // Once transferred, cancellation and effect custody stay with the
        // original StateStore action until its own real retirement.
        if let Some(inner) = tail.inner.as_mut() {
            let result = inner.poll_action(owner, cx);
            self.delivered |= result.is_ready();
            return result;
        }
        if let Some(reason) = owner.cancellation().reason() {
            self.error
                .get_or_insert_with(|| governed_cancellation_error(reason));
        }
        if self.pending.is_some() && self.error.is_none() {
            match owner.result_window_alias() {
                Some(window) => {
                    let context = &self.context.as_ref().expect("same catalog context").0;
                    let input = self.compiler.catalog_property_input(
                        self.source.clone(),
                        self.pending.take().expect("one catalog syntax payload"),
                        context.session().current_database().to_owned(),
                        context.execution().sql_semantics().clone(),
                        self.interpreter.clone(),
                        self.control.clone(),
                        owner.scope().clone(),
                        window,
                    );
                    tail.start(input);
                }
                None => {
                    self.error = Some(internal_error(
                        "catalog properties have no original result window",
                    ))
                }
            }
        }
        if tail.poll_cpu_closed(cx).is_pending() {
            return Poll::Pending;
        }
        if let Some(Err(error)) = tail.prepared.as_ref() {
            self.error.get_or_insert_with(|| error.clone());
        }
        if let Some(error) = self.error.as_ref() {
            self.delivered = true;
            return Poll::Ready(Err(error.clone()));
        }
        let command = tail
            .prepared
            .take()
            .expect("received catalog preparation")
            .expect("property refusal selected above");
        let (context, command_context) = self.context.take().expect("one catalog command context");
        match self.router.prepare_catalog_statement_action(
            CatalogSqlCommand::CreateCatalog(command),
            context,
            command_context,
        ) {
            Ok(inner) => tail.inner = Some(inner),
            Err(error) => {
                self.delivered = true;
                return Poll::Ready(Err(routed_execution_service_error(product_command_error(
                    error,
                ))));
            }
        }
        let result = tail
            .inner
            .as_mut()
            .expect("original catalog action")
            .poll_action(owner, cx);
        self.delivered |= result.is_ready();
        result
    }
}
impl FrontendQuerySession {
    pub(super) async fn execute_observed_catalog_create(
        &self,
        source: Arc<str>,
        pending: ast::legacy::CreateCatalog,
        context: RequestContext,
        command_context: CommandContext,
        deadline: Option<Instant>,
        statement: GovernedQueryStatementOwner,
    ) -> Result<StatementResult, QueryServiceError> {
        if let Err(cause) = crate::catalog_application::statement_action::admit_create_catalog_name(
            &pending.name.value,
        ) {
            return Ok(
                self.governed_typed_error(QueryServiceError::from_failure_cause(cause), statement)
            );
        }
        let cancellation = QueryCancellationView::governed(
            statement.cancellation().clone(),
            statement.timeout_ms(),
        );
        let action = CatalogCreateAction {
            source,
            pending: Some(pending),
            compiler: self.service.query_compiler.clone(),
            interpreter: self.service.required_constant_interpreter.clone(),
            router: self.service.product_command_router.clone(),
            context: Some((context, command_context)),
            control: SqlCompileControl::new(deadline, sql_cancellation_observation(cancellation)),
            tail: Some(Box::new(CatalogNativeTail {
                cpu: self.service.query_compiler.new_planning_cpu_source(),
                preparation: None,
                prepared: None,
                inner: None,
                inner_residual: None,
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
