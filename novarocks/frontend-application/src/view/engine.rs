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

//! View application ports and core engine adapter.
//!
//! The public traits and DTOs are the dependency-inversion boundary used by
//! the Frontend role application: core exposes only the engine capabilities required by
//! view DDL and declarations, without leaking the retired Core application facade, connector
//! backends, or parser-internal column definitions.

use std::sync::Arc;

use crate::catalog_application::query_catalog::CatalogServiceSource;
use crate::query_execution::kernels::ViewExecutionKernel;
use novarocks_query_application::persisted_query_definition::{
    PersistedQueryDefinition, PersistedQueryDialect,
};
use novarocks_query_application::view::{
    CreateExternalViewRequest, ResolvedExternalView, ViewColumnDefinition,
    ViewDefinitionAnalysisRequest, ViewEngine, ViewTarget,
};
#[cfg(test)]
use novarocks_query_application::view::{ViewRequestContext, ViewService, ViewStatementResult};
use novarocks_spi::connector::{
    ConnectorCatalogMutationOperation, ConnectorError, ConnectorErrorKind, ConnectorInstanceId,
    ConnectorRequestContext, ConnectorViewDefinition, ConnectorViewDialect, ConnectorViewIdentity,
    ConnectorViewRequest, CreateOrReplacePolicy, DropPolicy,
};

#[derive(Clone, Copy, Debug, Default)]
#[cfg(test)]
pub(super) struct EmptyViewService;

#[cfg(test)]
impl ViewService for EmptyViewService {
    fn catalog_snapshot(
        &self,
    ) -> Result<novarocks_query_application::view::LocalViewSnapshot, String> {
        // This fixture explicitly has no local DDL. Production supplies its
        // original process service through the composition root.
        novarocks_query_application::view::QueryViewService::new().catalog_snapshot()
    }

    fn prepare_show_views(
        &self,
        _statement: &novarocks_parser::ast::ShowViews,
        _context: ViewRequestContext<'_>,
    ) -> Result<novarocks_query_application::view::PreparedShowViews, String> {
        Err("view service is not injected".to_string())
    }

    fn execute_statement(
        &self,
        _engine: &dyn ViewEngine,
        _statement: &novarocks_parser::ast::ViewStatement,
        _context: ViewRequestContext<'_>,
    ) -> Result<ViewStatementResult, novarocks_query_application::api::CommandError> {
        Err("view service is not injected".to_string().into())
    }

    fn drop_database(&self, _catalog: &str, _database: &str) -> Result<(), String> {
        Ok(())
    }
}

/// The exact leaf dependencies needed by external view metadata operations.
/// This deliberately stays narrower than either a session or an application
/// aggregate, so frontend composition can pass its typed view kernel directly.
trait ViewExecutionContext: CatalogServiceSource + Send + Sync {
    fn function_catalog(&self) -> &novarocks_functions::EngineFunctionCatalog;
    fn connector_control(&self) -> &dyn novarocks_spi::connector::ConnectorControlRegistry;
    fn catalog_application(
        &self,
    ) -> Option<&dyn novarocks_catalog_application::CatalogApplicationPort>;
}

/// Frontend's connector/Catalog implementation of the query-application
/// external-view port. It is deliberately a narrow adapter, not a view
/// service or a Frontend application aggregate.
#[derive(Clone)]
pub(crate) struct FrontendViewEngine {
    kernel: ViewExecutionKernel,
}

impl FrontendViewEngine {
    pub(crate) fn new(kernel: ViewExecutionKernel) -> Self {
        Self { kernel }
    }
}

impl CatalogServiceSource for FrontendViewEngine {
    fn catalog_service(
        &self,
    ) -> &Arc<crate::catalog_application::query_catalog::QueryCatalogService> {
        self.kernel.catalog_service()
    }
}

impl ViewExecutionContext for FrontendViewEngine {
    fn function_catalog(&self) -> &novarocks_functions::EngineFunctionCatalog {
        self.kernel.function_catalog().as_ref()
    }

    fn connector_control(&self) -> &dyn novarocks_spi::connector::ConnectorControlRegistry {
        self.kernel.connector_control().as_ref()
    }

    fn catalog_application(
        &self,
    ) -> Option<&dyn novarocks_catalog_application::CatalogApplicationPort> {
        self.kernel.catalog_application().map(Arc::as_ref)
    }
}

impl ViewEngine for FrontendViewEngine {
    fn create_external_view(
        &self,
        request: CreateExternalViewRequest,
        context: &ConnectorRequestContext,
    ) -> Result<(), novarocks_query_application::api::CommandError> {
        let columns: Arc<[novarocks_spi::connector::ConnectorViewColumn]> = request
            .columns
            .into_iter()
            .map(|column| novarocks_spi::connector::ConnectorViewColumn {
                name: column.name,
                value_type: column.value_type,
            })
            .collect::<Vec<_>>()
            .into();
        let instance_id = ConnectorInstanceId::parse(&request.target.catalog).map_err(|error| {
            novarocks_query_application::api::CommandError::from_failure_cause(
                novarocks_failure_contract::SharedFailureCause::new(error),
            )
        })?;
        crate::connector::mutation::execute_catalog_mutation_for_command(
            self.connector_control(),
            &instance_id,
            ConnectorCatalogMutationOperation::CreateView {
                view: ConnectorViewIdentity {
                    instance_id: instance_id.clone(),
                    namespace: Arc::from(request.target.database),
                    view: Arc::from(request.target.view),
                },
                definition: ConnectorViewDefinition {
                    dialect: ConnectorViewDialect::StarRocks,
                    raw_sql: Arc::from(request.definition.raw_query_source),
                    default_catalog: Some(Arc::from(request.definition.resolution.default_catalog)),
                    default_namespace: Arc::from(request.definition.resolution.default_database),
                    columns,
                },
                comment: request.comment.map(Arc::from),
                properties: request
                    .properties
                    .into_iter()
                    .map(|(key, value)| (Arc::from(key), Arc::from(value)))
                    .collect(),
                policy: if request.or_replace {
                    CreateOrReplacePolicy::ReplaceIfExists
                } else if request.if_not_exists {
                    CreateOrReplacePolicy::NoOpIfExists
                } else {
                    CreateOrReplacePolicy::FailIfExists
                },
            },
            context.clone(),
        )
        .map(|_| ())
    }

    fn drop_external_view(
        &self,
        target: &ViewTarget,
        context: &ConnectorRequestContext,
        policy: DropPolicy,
    ) -> Result<(), novarocks_query_application::api::CommandError> {
        let instance_id = ConnectorInstanceId::parse(&target.catalog).map_err(|error| {
            novarocks_query_application::api::CommandError::from_failure_cause(
                novarocks_failure_contract::SharedFailureCause::new(error),
            )
        })?;
        crate::connector::mutation::execute_catalog_mutation_for_command(
            self.connector_control(),
            &instance_id,
            ConnectorCatalogMutationOperation::DropView {
                view: ConnectorViewIdentity {
                    instance_id: instance_id.clone(),
                    namespace: Arc::from(target.database.as_str()),
                    view: Arc::from(target.view.as_str()),
                },
                policy,
            },
            context.clone(),
        )
        .map(|_| ())
    }

    fn load_external_view(
        &self,
        target: &ViewTarget,
        context: &ConnectorRequestContext,
    ) -> Result<Option<ResolvedExternalView>, novarocks_query_application::api::CommandError> {
        let lease =
            crate::connector::metadata_binding_typed(self.connector_control(), &target.catalog)
                .map_err(crate::connector::mutation::command_error_from_connector)?;
        let context = crate::connector::context_for_planning_pin_typed(&lease, context.clone())
            .map_err(crate::connector::mutation::command_error_from_connector)?;
        let binding = lease.binding();
        let capability = binding.view_metadata().ok_or_else(|| {
            crate::connector::mutation::command_error_from_connector(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "connector control generation does not declare view metadata",
            ))
        })?;
        let instance_id = binding.descriptor().instance_id.clone();
        match crate::connector::schema_creation::load_frontend_view(
            capability.as_ref(),
            ConnectorViewRequest {
                view: ConnectorViewIdentity {
                    instance_id,
                    namespace: Arc::from(target.database.as_str()),
                    view: Arc::from(target.view.as_str()),
                },
                context,
            },
        ) {
            Ok(view) => resolved_external_view(target, view)
                .map(Some)
                .map_err(Into::into),
            Err(error) if error.kind() == ConnectorErrorKind::NotFound => Ok(None),
            Err(error) => Err(crate::connector::mutation::command_error_from_connector(
                error,
            )),
        }
    }

    fn analyze_view_definition(
        &self,
        request: ViewDefinitionAnalysisRequest<'_>,
        context: &ConnectorRequestContext,
    ) -> Result<Vec<ViewColumnDefinition>, novarocks_sql::compiler::SqlCompileError> {
        let catalog_service_snapshot =
            crate::catalog_application::query_catalog::catalog_service_snapshot(self);
        let provider =
            crate::catalog_application::query_materializer::build_catalog_service_provider(
                Some(request.catalog),
                &catalog_service_snapshot,
                self.connector_control(),
                context.clone(),
                novarocks_sql::planning::catalog::TableLookupMode::SchemaOnly,
                self.catalog_application(),
            );
        let control =
            crate::query_execution::planning::sql_compile_control_from_connector_request(context);
        let provider = provider
            .with_local_views(request.local_views.clone())
            .with_system_relations(self.kernel.system_relations().clone())
            .with_definition_inputs(
                context.clone(),
                self.kernel.constant_policy(),
                control.clone(),
            );
        let mut preparation =
            novarocks_sql::compiler::SqlCapturedCatalogPreparation::try_new_definition(
                request.query.clone(),
                request.catalog,
                request.database,
                Some(novarocks_types::naming::TableIdentity {
                    catalog: request.target.catalog.clone(),
                    namespace: request.target.database.clone(),
                    table: request.target.view.clone(),
                }),
                control.clone(),
            )?;
        while !preparation.needs().is_empty() {
            let facts = preparation
                .needs()
                .iter()
                .map(|need| provider.resolve_relation_fact(need))
                .collect::<Result<Vec<_>, novarocks_sql::compiler::SqlCompileError>>()?;
            preparation = preparation.continue_catalog(facts.into_boxed_slice())?;
        }
        preparation
            .analyze_view_definition(self.function_catalog(), self.kernel.constant_policy())
            .map(|columns| columns.into_vec())
    }
}

fn resolved_external_view(
    target: &ViewTarget,
    view: novarocks_spi::connector::ConnectorViewMetadataValue,
) -> Result<ResolvedExternalView, String> {
    let default_catalog = view
        .definition
        .default_catalog
        .as_deref()
        .unwrap_or(target.catalog.as_str());
    let definition = PersistedQueryDefinition::new(
        view.definition.raw_sql.to_string(),
        match view.definition.dialect {
            ConnectorViewDialect::StarRocks => PersistedQueryDialect::StarRocks,
        },
        default_catalog,
        &view.definition.default_namespace,
    )?;
    Ok(ResolvedExternalView {
        definition,
        comment: view.comment.map(|comment| comment.to_string()),
        properties: view
            .properties
            .into_iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect(),
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[derive(Default)]
    struct FakeViewEngine;

    impl ViewEngine for FakeViewEngine {
        fn create_external_view(
            &self,
            _request: CreateExternalViewRequest,
            _context: &ConnectorRequestContext,
        ) -> Result<(), novarocks_query_application::api::CommandError> {
            unreachable!("empty view service must not access the engine")
        }

        fn drop_external_view(
            &self,
            _target: &ViewTarget,
            _context: &ConnectorRequestContext,
            _policy: DropPolicy,
        ) -> Result<(), novarocks_query_application::api::CommandError> {
            unreachable!("empty view service must not access the engine")
        }

        fn load_external_view(
            &self,
            _target: &ViewTarget,
            _context: &ConnectorRequestContext,
        ) -> Result<Option<ResolvedExternalView>, novarocks_query_application::api::CommandError>
        {
            unreachable!("empty view service must not access the engine")
        }

        fn analyze_view_definition(
            &self,
            _request: ViewDefinitionAnalysisRequest<'_>,
            _context: &ConnectorRequestContext,
        ) -> Result<Vec<ViewColumnDefinition>, novarocks_sql::compiler::SqlCompileError> {
            unreachable!("empty view service must not access the engine")
        }
    }

    #[tokio::test]
    async fn show_views_listing_errors_preserve_connector_error_text() {
        for kind in crate::catalog_application::statement::external_listing_tests::error_kinds() {
            crate::query::view_read_action::tests::assert_listing_error_text(kind).await;
        }
    }

    #[test]
    fn view_ports_and_dtos_are_constructible_by_the_owning_module() {
        let target = ViewTarget {
            catalog: "rest".to_string(),
            database: "analytics".to_string(),
            view: "daily_sales".to_string(),
        };
        let request = CreateExternalViewRequest {
            target: target.clone(),
            columns: vec![ViewColumnDefinition {
                name: "sale_count".into(),
                value_type: novarocks_type_contract::FunctionValueType::new(
                    arrow::datatypes::DataType::Int64,
                    true,
                ),
            }],
            definition: PersistedQueryDefinition::new(
                "SELECT COUNT(*) AS sale_count FROM sales",
                PersistedQueryDialect::StarRocks,
                "rest",
                "analytics",
            )
            .expect("valid definition"),
            comment: Some("Daily sales".to_string()),
            or_replace: false,
            if_not_exists: false,
            properties: vec![],
        };
        let resolved = ResolvedExternalView {
            definition: request.definition.clone(),
            comment: request.comment.clone(),
            properties: HashMap::new(),
        };
        let context = ViewRequestContext {
            current_catalog: Some("rest"),
            current_database: "analytics",
            connector_context: None,
        };

        fn ports_are_object_safe(_service: &dyn ViewService, _engine: &dyn ViewEngine) {}
        let _ = ports_are_object_safe;
        let _ = (request, resolved, context, ViewStatementResult::Ok);
    }

    #[test]
    fn empty_view_service_rejects_view_ddl_and_exposes_an_empty_snapshot() {
        let service: Arc<dyn ViewService> = Arc::new(EmptyViewService);
        let engine = FakeViewEngine;
        let ctx = ViewRequestContext {
            current_catalog: None,
            current_database: "db",
            connector_context: None,
        };
        let statements = novarocks_parser::parse("CREATE VIEW v AS SELECT 1").expect("parse");
        let [novarocks_parser::ast::Statement::View(statement)] = statements.as_slice() else {
            panic!("expected view statement");
        };
        assert!(
            service
                .execute_statement(&engine, statement, ctx)
                .unwrap_err()
                .to_string()
                .contains("view service is not injected")
        );
        let identity = novarocks_types::naming::TableIdentity::new("default_catalog", "db", "t");
        assert!(service.catalog_snapshot().unwrap().get(&identity).is_none());
    }
}
