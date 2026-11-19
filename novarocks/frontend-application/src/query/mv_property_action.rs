// SPDX-License-Identifier: Apache-2.0

//! Evaluate typed MV properties under the admitted Root before the original MV action.

use super::{
    FrontendQuerySession, QueryCancellationView, QueryServiceError, StatementResult,
    compiler::FrontendQueryCompiler, governed_cancellation_error, internal_error,
    product_command_error, property_action_tail::PropertyActionTail,
    property_cpu_context::PropertyCpuContext, routed_execution_service_error,
};
use crate::{
    RequiredConstantInterpreter,
    mv::{
        command::{MvCommandExecutor, TrinoMvPropertyPreparation},
        domain::application::{MvAlterStatement, MvCreateStatement},
    },
    query_execution::planning::sql_cancellation_observation,
};
use novarocks_parser::{
    Span,
    ast::{AstVec, ObjectName, StatementProperty, TrinoCreateMaterializedView},
};
use novarocks_query_application::{
    admitted_query_context::RequestContext,
    api::{CommandContext, StatementExternalEffectDisposition, StatementNativeHealthFailure},
    session_control::{
        GovernedActionContext, GovernedQueryStatementOwner, GovernedStatementAction,
        GovernedStatementResidual, StatementActionValue,
    },
};
use novarocks_spi::connector::ConnectorControlPlanningPin;
use novarocks_sql::{compiler::SqlCompileControl, planning::mv::SqlMvTarget};
use std::{
    sync::Arc,
    task::{Context, Poll},
    time::Instant,
};

pub(super) enum TrinoMvProperties {
    Create(Box<TrinoCreateMaterializedView>),
    Alter {
        name: ObjectName,
        properties: AstVec<StatementProperty>,
        span: Span,
    },
}

impl TrinoMvProperties {
    fn name(&self) -> &ObjectName {
        match self {
            Self::Create(statement) => &statement.name,
            Self::Alter { name, .. } => name,
        }
    }
}

pub(crate) enum PreparedMvProperties {
    Create(Box<MvCreateStatement>),
    Alter(SqlMvTarget, MvAlterStatement),
}

pub(crate) struct MvPropertyCpuInput {
    source: Arc<str>,
    pending: TrinoMvProperties,
    current_catalog: Option<String>,
    planning: ConnectorControlPlanningPin,
    context: PropertyCpuContext,
}

impl MvPropertyCpuInput {
    pub(crate) fn run(self) -> Result<PreparedMvProperties, QueryServiceError> {
        let mut constants = self.context.constant_port();
        let mut preparation = TrinoMvPropertyPreparation {
            source: &self.source,
            current_catalog: self.current_catalog.as_deref(),
            current_database: &self.context.current_database,
            planning: &self.planning,
            port: &mut constants,
            control: &self.context.control,
        };
        match self.pending {
            TrinoMvProperties::Create(statement) => preparation
                .create(*statement)
                .map(|statement| PreparedMvProperties::Create(Box::new(statement))),
            TrinoMvProperties::Alter {
                name,
                properties,
                span,
            } => preparation
                .alter(name, properties, span)
                .map(|(target, statement)| PreparedMvProperties::Alter(target, statement)),
        }
    }
}

struct MvPropertyAction {
    source: Arc<str>,
    pending: Option<TrinoMvProperties>,
    compiler: FrontendQueryCompiler,
    interpreter: Arc<RequiredConstantInterpreter>,
    executor: MvCommandExecutor,
    context: RequestContext,
    command: CommandContext,
    control: SqlCompileControl,
    tail: Option<Box<PropertyActionTail<PreparedMvProperties>>>,
    error: Option<QueryServiceError>,
    delivered: bool,
}

impl MvPropertyAction {
    fn cpu_input(
        &self,
        pending: TrinoMvProperties,
        owner: &GovernedActionContext<'_>,
    ) -> Result<MvPropertyCpuInput, QueryServiceError> {
        let window = owner
            .result_window_alias()
            .ok_or_else(|| internal_error("MV properties have no original result window"))?;
        let command_window = self
            .command
            .result_window_alias()
            .ok_or_else(|| internal_error("MV property command has no original result window"))?;
        if !window.is_for_scope(owner.scope())
            || !window.is_for_scope(self.command.scope())
            || !window.shares_capacity_with(&command_window)
        {
            return Err(internal_error(
                "MV property CPU differs from its original Root/window",
            ));
        }
        let planning =
            self.executor
                .trino_property_planning(pending.name(), &self.context, &self.source)?;
        let session = self.context.session();
        Ok(MvPropertyCpuInput {
            source: self.source.clone(),
            pending,
            current_catalog: session.current_catalog().map(str::to_owned),
            planning,
            context: self.compiler.property_cpu_context(
                session.current_database().to_owned(),
                self.context.execution().sql_semantics().clone(),
                self.interpreter.clone(),
                self.control.clone(),
                owner.scope().clone(),
                window,
            ),
        })
    }
}

impl GovernedStatementAction for MvPropertyAction {
    fn external_effect_disposition(&self) -> Option<StatementExternalEffectDisposition> {
        self.tail
            .as_ref()?
            .inner
            .as_ref()?
            .external_effect_disposition()
    }

    fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        self.tail.as_ref()?.local_health()
    }

    fn handoff_residual_in(
        &mut self,
        target: &mut Option<Box<dyn GovernedStatementResidual>>,
    ) -> Result<(), StatementNativeHealthFailure> {
        if self.delivered {
            PropertyActionTail::handoff(&mut self.tail, target)?;
        }
        Ok(())
    }

    fn poll_action(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatementActionValue, QueryServiceError>> {
        if self.local_health_failure().is_some() {
            return Poll::Pending;
        }
        if let Some(action) = self.tail.as_mut().unwrap().inner.as_mut() {
            let result = action.poll_action(owner, cx);
            self.delivered |= result.is_ready();
            return result;
        }
        if let Some(reason) = owner.cancellation().reason() {
            self.error
                .get_or_insert_with(|| governed_cancellation_error(reason));
        }
        if self.error.is_none()
            && let Some(pending) = self.pending.take()
        {
            match self.cpu_input(pending, owner) {
                Ok(input) => {
                    let tail = self.tail.as_mut().expect("original MV properties");
                    let cpu = tail.cpu.clone();
                    tail.start_operation(async move { cpu.mv_properties(input).await });
                }
                Err(error) => self.error = Some(error),
            }
        }
        let tail = self.tail.as_mut().expect("original MV properties");
        std::task::ready!(tail.poll_cpu_closed(cx));
        if let Some(Err(error)) = tail.prepared.as_ref() {
            self.error.get_or_insert_with(|| error.clone());
        }
        if let Some(error) = self.error.as_ref() {
            self.delivered = true;
            return Poll::Ready(Err(error.clone()));
        }
        let prepared = tail
            .prepared
            .take()
            .expect("received MV property preparation")
            .expect("property refusal already selected");
        let action = match prepared {
            PreparedMvProperties::Create(statement) => {
                self.executor
                    .prepare_evaluated_create(*statement, &self.context, &self.command)
            }
            PreparedMvProperties::Alter(target, statement) => self
                .executor
                .prepare_evaluated_alter(target, statement, &self.context, &self.command),
        };
        match action {
            Ok(inner) => {
                tail.inner = Some(inner);
                let result = tail.inner.as_mut().unwrap().poll_action(owner, cx);
                self.delivered |= result.is_ready();
                result
            }
            Err(error) => {
                self.delivered = true;
                Poll::Ready(Err(routed_execution_service_error(product_command_error(
                    error,
                ))))
            }
        }
    }
}

impl FrontendQuerySession {
    pub(super) async fn execute_observed_trino_mv_properties(
        &self,
        source: Arc<str>,
        pending: TrinoMvProperties,
        context: RequestContext,
        command: CommandContext,
        deadline: Option<Instant>,
        statement: GovernedQueryStatementOwner,
    ) -> Result<StatementResult, QueryServiceError> {
        let cancellation = QueryCancellationView::governed(
            statement.cancellation().clone(),
            statement.timeout_ms(),
        );
        let action = MvPropertyAction {
            source,
            pending: Some(pending),
            compiler: self.service.query_compiler.clone(),
            interpreter: self.service.required_constant_interpreter.clone(),
            executor: self.service.command_executor.mv_call.clone(),
            context,
            command,
            control: SqlCompileControl::new(deadline, sql_cancellation_observation(cancellation)),
            tail: Some(Box::new(PropertyActionTail::new(
                self.service.query_compiler.new_planning_cpu_source(),
            ))),
            error: None,
            delivered: false,
        };
        self.execute_observed_statement_action(statement, Box::new(action))
            .await
    }
}
