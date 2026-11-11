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

//! Typed DELETE input and preparation for the supervised write action.

pub(crate) mod equality;
pub(crate) mod standard;

use std::sync::Arc;

use crate::query_execution::kernels::DmlExecutionKernel;
use novarocks_proto_codec::lifecycle::QueryOptions;
use novarocks_query_application::admitted_query_context::QueryExecutionContext;

/// One parser-owned DELETE variant admitted by the typed statement router.
///
/// The source is retained separately in [`DeleteStatementInput`] and may only
/// be sliced through the selected AST node's [`novarocks_parser::Span`].
#[derive(Clone, Copy, Debug)]
pub enum DeleteStatement<'a> {
    Predicate(&'a novarocks_parser::ast::Delete),
    Equality,
}

/// The exact parser-owned statement retained by the supervised write action.
/// Source text is never reparsed to rediscover the statement or its target.
#[derive(Clone, Debug)]
pub(crate) enum OwnedDeleteStatement {
    Predicate(novarocks_parser::ast::Delete),
    Equality(novarocks_parser::ast::AddEqualityDelete),
}

/// Resolve only local catalog admission and parser-owned target facts. The
/// action calls this before creating its metadata-I/O phase.
pub(crate) fn resolve_target(
    state: &DmlExecutionKernel,
    statement: &OwnedDeleteStatement,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<
    (crate::catalog_application::resolver::TargetBackend, String),
    crate::catalog_application::resolver::TargetResolutionError,
> {
    match statement {
        OwnedDeleteStatement::Predicate(statement) => {
            standard::resolve_target(state, statement, current_catalog, current_database)
        }
        OwnedDeleteStatement::Equality(statement) => {
            equality::resolve_target(state, statement, current_catalog, current_database)
        }
    }
}

/// Frozen preparation facts retained before SQL compilation starts. The
/// session and original request context remain the statement's publication
/// authority even when a later phase fails or is cancelled.
pub(crate) struct PreparedDeleteWrite {
    state: DmlExecutionKernel,
    target: crate::catalog_application::resolver::TargetBackend,
    delete_query: novarocks_parser::ast::Query,
    write_plan: DeleteWritePlan,
    table_bindings: Arc<crate::catalog_application::query_bindings::QueryTableBindingStore>,
    match_read: Option<crate::catalog_application::query_materializer::QueryLocalTableOverlay>,
    execution: QueryExecutionContext,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
    write_session: Arc<crate::query_execution::write_session::ConnectorWriteSession>,
}

enum DeleteWritePlan {
    Direct(novarocks_sql::planning::dml::DmlWritePlanInput),
    Materialized(novarocks_spi::connector::ConnectorControlPlanningPin),
}

impl PreparedDeleteWrite {
    /// Called only after the action installed this exact Prepared in its stable
    /// source. This future borrows the already retained session and custody.
    pub(crate) async fn prepare_native_assembly_for_execution_async(
        &self,
        provider_source: &crate::query_execution::completion_facts::FrontendCompletionCustody,
        query_options: Option<QueryOptions>,
    ) -> Result<
        crate::query_execution::compiler::PreparedDmlWriteAssembly,
        crate::dml::error::DmlExecutionError,
    > {
        use super::mutation_flow::{
            RoutedMutationWrite, bind_change_stream_routes, compile_routed_mutation_write,
        };

        let sql_write_input = match &self.write_plan {
            DeleteWritePlan::Direct(input) => input.clone(),
            DeleteWritePlan::Materialized(pin) => {
                let (routes, statistics_targets) = bind_change_stream_routes(
                    &self.target,
                    &self.write_session,
                    pin,
                    &self.table_bindings,
                )?;
                return compile_routed_mutation_write(
                    RoutedMutationWrite {
                        state: &self.state,
                        target: &self.target,
                        execution: &self.execution,
                        context: &self.connector_context,
                        bindings: &self.table_bindings,
                        match_read: self
                            .match_read
                            .as_ref()
                            .ok_or("position DELETE lost its signed match read")?,
                        session: &self.write_session,
                        query: &self.delete_query,
                        kind: standard::delete_change_stream_kind(),
                        assertion: None,
                        routes,
                        statistics_targets,
                    },
                    provider_source,
                    query_options,
                )
                .await;
            }
        };
        crate::query_execution::compiler::prepare_query_as_iceberg_write_with_write_session_and_constants_async(
            &self.state,
            Some(&self.target.catalog),
            &self.target.namespace,
            &self.delete_query,
            None,
            sql_write_input,
            Arc::clone(&self.table_bindings),
            query_options,
            novarocks_sql::compiler::RootDistributionRequirement::Any,
            &self.execution,
            &self.connector_context,
            Arc::clone(&self.write_session),
            self.match_read.as_slice(),
            provider_source,
            crate::query_execution::completion_facts::SystemTableSourcePolicy::Mutation,
        ).await
    }

    pub(crate) fn finalize(&self) -> Result<(), String> {
        self.state.catalog_service().invalidate_table(
            &self.target.catalog,
            &self.target.namespace,
            &self.target.table,
        )
    }
}

pub(crate) async fn prepare_delete_write_async(
    state: &DmlExecutionKernel,
    statement: &OwnedDeleteStatement,
    target: &crate::catalog_application::resolver::TargetBackend,
    target_ref: &str,
    planning_pin: novarocks_spi::connector::ConnectorControlPlanningPin,
    execution: QueryExecutionContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    operation_id: novarocks_spi::connector::ConnectorWriteOperationId,
    begin_receiving: Arc<crate::query_execution::write_session::WriteBeginReceiving>,
) -> Result<PreparedDeleteWrite, crate::dml::error::DmlExecutionError> {
    match statement {
        OwnedDeleteStatement::Predicate(statement) => {
            standard::prepare_delete_statement_async(
                state,
                statement,
                target,
                target_ref,
                planning_pin,
                execution,
                connector_context,
                operation_id,
                begin_receiving,
            )
            .await
        }
        OwnedDeleteStatement::Equality(statement) => {
            equality::prepare_equality_delete_statement_async(
                state,
                statement,
                target,
                target_ref,
                planning_pin,
                execution,
                connector_context,
                begin_receiving,
            )
            .await
        }
    }
}

/// Load through the already admitted generation, without acquiring a new lease
/// or using a synchronous joining metadata wrapper.
async fn load_delete_target_async(
    target: &crate::catalog_application::resolver::TargetBackend,
    planning_pin: novarocks_spi::connector::ConnectorControlPlanningPin,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<crate::connector::write_target::ConnectorWriteTargetBinding, String> {
    let context =
        crate::connector::context_for_planning_pin(&planning_pin, connector_context.clone())?;
    let metadata = crate::connector::schema_creation::load_frontend_table_async(
        planning_pin.binding().metadata().as_ref(),
        novarocks_spi::connector::ConnectorTableRequest {
            table: novarocks_spi::connector::ConnectorTableIdentity {
                instance_id: planning_pin.binding().descriptor().instance_id.clone(),
                namespace: Arc::from(target.namespace.as_str()),
                table: Arc::from(target.table.as_str()),
            },
            resolution: novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
            context,
        },
    )
    .await
    .map_err(|error| error.to_string())?;
    Ok(crate::connector::write_target::ConnectorWriteTargetBinding::new(metadata, planning_pin))
}

async fn begin_delete_write_session_async(
    state: &DmlExecutionKernel,
    planning_pin: &novarocks_spi::connector::ConnectorControlPlanningPin,
    write_lease: &novarocks_spi::connector::ConnectorWrite,
    request: novarocks_spi::connector::write_stack::ConnectorWriteBeginRequest,
    begin_receiving: &crate::query_execution::write_session::WriteBeginReceiving,
) -> Result<Arc<crate::query_execution::write_session::ConnectorWriteSession>, String> {
    let catalog_properties = write_lease.catalog_properties().cloned().ok_or_else(|| {
        "connector write lease has no immutable catalog runtime identity".to_string()
    })?;
    crate::query_execution::write_session::ConnectorWriteSession::begin_async_in(
        crate::connector::write_target::derive_write_stack_lease(
            state.typed_connector_control(),
            planning_pin,
        )?,
        catalog_properties,
        request,
        begin_receiving,
    )
    .await
    .map_err(|error| format!("begin connector write session: {error}"))
}

/// DELETE statements recognized by the frontend command router.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteStatementKind {
    Predicate,
    Equality,
}

impl DeleteStatement<'_> {
    #[cfg(test)]
    pub const fn kind(self) -> DeleteStatementKind {
        match self {
            Self::Predicate(_) => DeleteStatementKind::Predicate,
            Self::Equality => DeleteStatementKind::Equality,
        }
    }
}

/// Complete original DELETE input retained by its governed statement source.
pub struct DeleteStatementInput {
    pub(crate) statement: OwnedDeleteStatement,
    pub(crate) sql: String,
    pub(crate) context: novarocks_query_application::admitted_query_context::RequestContext,
    pub(crate) query_options: QueryOptions,
    pub(crate) launcher:
        Arc<dyn crate::query_execution::logical_execution::LogicalExecutionLauncher>,
    #[cfg(novarocks_dev_controls)]
    pub(crate) observation: Option<crate::preparation_diagnostics::StatementObservationHandle>,
}

/// One-to-one core capability used only by the frontend DML application owner.
// Design: ADR-0020 (docs/adr/ADR-0020-frontend-delete-application-owner.md)
pub trait DeleteEngine: Send + Sync {
    fn prepare_statement_action(
        &self,
        input: DeleteStatementInput,
    ) -> Result<
        Box<dyn novarocks_query_application::session_control::GovernedStatementAction>,
        crate::catalog_application::resolver::TargetResolutionError,
    >;
}

impl DeleteEngine for DmlExecutionKernel {
    fn prepare_statement_action(
        &self,
        input: DeleteStatementInput,
    ) -> Result<
        Box<dyn novarocks_query_application::session_control::GovernedStatementAction>,
        crate::catalog_application::resolver::TargetResolutionError,
    > {
        crate::query::delete_action::prepare(self.clone(), input)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_parser::ast::{DmlStatement, Statement};

    #[test]
    fn typed_variant_determines_delete_kind() {
        let statements =
            novarocks_parser::parse("DELETE FROM t WHERE id = 1").expect("parse DELETE");
        let Statement::Dml(DmlStatement::Delete(delete)) = &statements[0] else {
            panic!("expected DELETE");
        };

        assert_eq!(
            DeleteStatement::Predicate(delete).kind(),
            DeleteStatementKind::Predicate
        );
    }
}
