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

//! Typed CREATE SCHEMA consumes properties and publishes one namespace under
//! the admitted statement and its retained connector generation.

use super::{
    QueryCancellationView, QueryServiceError,
    compiler::FrontendQueryCompiler,
    internal_error, product_command_service_error,
    property_cpu_context::PropertyCpuContext,
    table_mutation_action::{self, PreparedTableMutationInput},
};
use crate::{
    connector::mutation::{command_error_from_connector, namespace_already_exists},
    query_execution::{
        completion_facts::FrontendPlanningCpuSource, planning::sql_cancellation_observation,
        required_constant_eval::RequiredConstantInterpreter,
    },
};
use novarocks_parser::{Span, ast::TrinoCreateSchema};
use novarocks_query_application::{
    admitted_query_context::RequestContext,
    api::{
        CatalogNamespacePropertyBinding, CommandContext, StatementExternalEffectDisposition,
        StatementNativeHealthFailure,
    },
    session_control::{
        GovernedActionContext, GovernedStatementAction, GovernedStatementResidual,
        StatementActionValue,
    },
    session_outcome::cancellation_error,
    sql::{
        ProductCommandRouter, dml_admission::DmlAdmissionError, lower_object_name,
        property_evaluation::prepare_trino_create_schema,
    },
};
use novarocks_spi::connector::{
    ConnectorCatalogMutationOperation, ConnectorCatalogMutationRequest,
    ConnectorMutationOperationId, ConnectorProviderBindingKey, CreatePolicy,
};
use novarocks_sql::{compiler::SqlCompileControl, semantic::CatalogSqlCommand};
use novarocks_workload_control::WorkError;
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, ready},
    time::Instant,
};

pub(crate) struct SchemaPropertyInput {
    source: Arc<str>,
    statement: TrinoCreateSchema,
    binding: CatalogNamespacePropertyBinding,
    command: CommandContext,
    context: PropertyCpuContext,
}

impl SchemaPropertyInput {
    pub(crate) fn run(self) -> Result<PreparedTableMutationInput, QueryServiceError> {
        let Self {
            source,
            statement,
            binding,
            command,
            context,
        } = self;
        let window = command
            .result_window_alias()
            .ok_or_else(|| internal_error("schema property CPU has no original command window"))?;
        if !context.window.is_for_scope(&context.scope)
            || !context.window.is_for_scope(command.scope())
            || !context.window.shares_capacity_with(&window)
        {
            return Err(internal_error(
                "schema property CPU differs from its original Root/window",
            ));
        }
        let declarations = binding
            .mutation()
            .schema_property_declarations()
            .map_err(command_error_from_connector)
            .map_err(product_command_service_error)?;
        let CatalogSqlCommand::CreateDatabase {
            if_not_exists,
            properties,
            ..
        } = prepare_trino_create_schema(
            &source,
            &statement,
            &declarations,
            &mut context.constant_port(),
            &context.control,
        )?
        else {
            return Err(internal_error(
                "schema property CPU returned another catalog command",
            ));
        };
        let request = ConnectorCatalogMutationRequest {
            operation_id: ConnectorMutationOperationId::new(),
            target: ConnectorProviderBindingKey {
                instance_id: binding.namespace().instance_id.clone(),
                incarnation: binding.planning().binding().incarnation(),
            },
            operation: ConnectorCatalogMutationOperation::CreateNamespace {
                namespace: binding.namespace().clone(),
                properties: properties
                    .into_iter()
                    .map(|(name, value)| (Arc::from(name), value))
                    .collect(),
                policy: if if_not_exists {
                    CreatePolicy::NoOpIfExists
                } else {
                    CreatePolicy::FailIfExists
                },
            },
            context: command.connector_context().clone(),
        };
        PreparedTableMutationInput::try_new_namespace(binding, request)
            .map_err(product_command_service_error)
    }
}

pub(super) struct SchemaCreateInput {
    pub(super) statement: TrinoCreateSchema,
    pub(super) source: Arc<str>,
    pub(super) context: RequestContext,
    pub(super) command: CommandContext,
    pub(super) deadline: Option<Instant>,
    pub(super) cancellation: QueryCancellationView,
}

pub(super) fn prepare(
    compiler: &FrontendQueryCompiler,
    interpreter: Arc<RequiredConstantInterpreter>,
    router: &ProductCommandRouter,
    input: SchemaCreateInput,
) -> Result<Box<dyn GovernedStatementAction>, QueryServiceError> {
    let SchemaCreateInput {
        statement,
        source,
        context,
        command,
        deadline,
        cancellation,
    } = input;
    let window = command
        .result_window_alias()
        .ok_or_else(|| internal_error("schema property preparation has no original window"))?;
    let capacity = context
        .execution()
        .result_capacity()
        .ok_or_else(|| internal_error("schema property preparation has no original Root"))?;
    if !window.is_for_scope(capacity.root_scope())
        || !window.shares_capacity_with(&capacity.window_alias())
    {
        return Err(internal_error(
            "schema property preparation differs from its original Root/window",
        ));
    }
    let binding = router
        .acquire_namespace_property_binding(&lower_object_name(&statement.schema_name), &context)
        .map_err(product_command_service_error)?;
    let label = format!(
        "{}.{}",
        binding.namespace().instance_id.as_str(),
        binding.namespace().namespace
    );
    let span = statement.span;
    let property_context = compiler.property_cpu_context(
        context.session().current_database().to_owned(),
        context.execution().sql_semantics().clone(),
        interpreter,
        SqlCompileControl::new(deadline, sql_cancellation_observation(cancellation.clone())),
        command.scope().clone(),
        window,
    );
    let input = SchemaPropertyInput {
        source: source.clone(),
        statement,
        binding,
        command: command.clone(),
        context: property_context,
    };
    let cpu = compiler.new_planning_cpu_source();
    let receiving = cpu.clone();
    // The existing CPU receiver distinguishes its business result from its
    // actual close. The prepared mutation does not leave it before both.
    let preparation: Preparation = Box::pin(async move {
        let prepared = receiving.schema_properties(input).await?;
        receiving.close().await?;
        Ok(prepared)
    });
    Ok(Box::new(Action {
        source,
        span,
        label,
        cpu,
        preparation: Some(preparation),
        command: Some(command),
        cancellation,
        inner: None,
        health: None,
        in_foreign: false,
    }))
}

type Preparation = Pin<
    Box<
        dyn Future<
                Output = Result<
                    Result<PreparedTableMutationInput, QueryServiceError>,
                    StatementNativeHealthFailure,
                >,
            > + Send,
    >,
>;

struct Action {
    source: Arc<str>,
    span: Span,
    label: String,
    cpu: Arc<FrontendPlanningCpuSource>,
    preparation: Option<Preparation>,
    command: Option<CommandContext>,
    cancellation: QueryCancellationView,
    inner: Option<Box<dyn GovernedStatementAction>>,
    health: Option<StatementNativeHealthFailure>,
    in_foreign: bool,
}

impl Action {
    fn present(&self, error: QueryServiceError) -> QueryServiceError {
        if !namespace_already_exists(&error) {
            return error;
        }
        let user = DmlAdmissionError::SchemaAlreadyExists.to_user_error(
            &self.source,
            self.span,
            format!("Schema '{}' already exists", self.label),
        );
        let cause = error
            .failure_cause()
            .expect("original namespace verdict")
            .clone();
        let projected = QueryServiceError::from_user_error_with_cause(user, cause)
            .with_compile_control(error.compile_control_error());
        match error.external_effect_disposition() {
            Some(effect) => projected.attach_external_effect_disposition(effect),
            None => projected,
        }
    }
}

impl GovernedStatementAction for Action {
    fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        self.health
            .clone()
            .or_else(|| self.cpu.local_health_failure())
            .or_else(|| {
                self.in_foreign.then(|| {
                    StatementNativeHealthFailure::actual_exit_unproven(
                        "schema property preparation exit is unproven",
                        Arc::new(WorkError::Conflict),
                    )
                })
            })
            .or_else(|| {
                self.inner
                    .as_ref()
                    .and_then(|inner| inner.local_health_failure())
            })
    }

    fn external_effect_disposition(&self) -> Option<StatementExternalEffectDisposition> {
        self.inner
            .as_ref()
            .and_then(|inner| inner.external_effect_disposition())
    }

    fn handoff_residual_in(
        &mut self,
        target: &mut Option<Box<dyn GovernedStatementResidual>>,
    ) -> Result<(), StatementNativeHealthFailure> {
        if let Some(inner) = self.inner.as_mut() {
            inner.handoff_residual_in(target)?;
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
        if let Some(preparation) = self.preparation.as_mut() {
            self.in_foreign = true;
            let prepared = preparation.as_mut().poll(cx);
            self.in_foreign = false;
            let prepared = ready!(prepared);
            if let Err(health) = prepared {
                self.health = Some(health);
                return Poll::Pending;
            }
            self.in_foreign = true;
            drop(self.preparation.take());
            self.in_foreign = false;
            let prepared = prepared.expect("actual schema CPU exit")?;
            // Even properties on IF NOT EXISTS must evaluate before the
            // original Provider's namespace existence decision.
            if let Some(reason) = self.cancellation.reason() {
                return Poll::Ready(Err(cancellation_error(reason)));
            }
            self.inner = Some(
                table_mutation_action::prepare_namespace(
                    prepared,
                    self.command
                        .take()
                        .expect("original schema mutation command"),
                    self.cancellation.clone(),
                )
                .map_err(product_command_service_error)?,
            );
        }
        let result = self
            .inner
            .as_mut()
            .expect("original schema mutation action")
            .poll_action(owner, cx);
        result.map(|result| result.map_err(|error| self.present(error)))
    }
}
