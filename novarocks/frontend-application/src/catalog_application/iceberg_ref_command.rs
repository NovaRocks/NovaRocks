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

//! Closed capability for semantic Iceberg branch and tag mutations.

use std::sync::Arc;

use novarocks_spi::connector::ConnectorControlRegistry;

use novarocks_query_application::protocol_delivery::QuerySessionOutput as StatementResult;
use novarocks_spi::connector::MvStorageObservationPort;

#[derive(Clone)]
pub struct IcebergRefCommandExecutor {
    connector_control: Arc<dyn ConnectorControlRegistry>,
    storage_observation: Arc<dyn MvStorageObservationPort>,
    mv_readiness: Arc<crate::mv::domain::readiness::MvReadinessPort>,
}

impl IcebergRefCommandExecutor {
    pub fn new(
        connector_control: Arc<dyn ConnectorControlRegistry>,
        storage_observation: Arc<dyn MvStorageObservationPort>,
        mv_readiness: Arc<crate::mv::domain::readiness::MvReadinessPort>,
    ) -> Self {
        Self {
            connector_control,
            storage_observation,
            mv_readiness,
        }
    }

    pub(crate) fn prepare_trino_branch(
        &self,
        source: &str,
        node: &novarocks_parser::ast::TrinoCreateBranch,
        context: &novarocks_query_application::admitted_query_context::RequestContext,
        connector: &novarocks_spi::connector::ConnectorRequestContext,
        constants: &mut dyn novarocks_query_application::sql::property_evaluation::DdlPropertyConstantPort,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        crate::mv::domain::ref_flow::PreparedRefMutation,
        novarocks_query_application::session_error::QueryServiceError,
    > {
        use novarocks_query_application::session_error::QueryServiceError;
        use novarocks_query_application::sql::{
            lower_object_name,
            trino_table_command::{prepare_trino_create_branch, validate_trino_create_branch},
        };
        use novarocks_sql::semantic::{
            CatalogSqlCommand, ObjectName, command::IcebergTableSqlAction,
        };
        let project = crate::query::product_command_service_error;
        validate_trino_create_branch(source, node)?;
        let target = novarocks_types::naming::resolve_catalog_table_name(
            &lower_object_name(&node.table_name).parts,
            context.session().current_catalog(),
            context.session().current_database(),
        )
        .map_err(|error| project(error.into()))?;
        let identity = ObjectName {
            parts: vec![target.catalog, target.namespace, target.table],
        };
        let binding = crate::mv::domain::ref_flow::prepare_target(
            self.connector_control.as_ref(),
            self.storage_observation.as_ref(),
            self.mv_readiness.as_ref(),
            &identity,
            connector,
        )
        .map_err(project)?;
        let declarations = binding
            .mutation()
            .branch_property_declarations()
            .map_err(|error| {
                QueryServiceError::from_failure_cause(
                    novarocks_failure_contract::SharedFailureCause::new(error),
                )
            })?;
        let CatalogSqlCommand::AlterIcebergTable(command) =
            prepare_trino_create_branch(source, node, &declarations, constants, control)?
        else {
            unreachable!("CREATE BRANCH produces its existing reference action");
        };
        let IcebergTableSqlAction::Reference(action) = command.action else {
            unreachable!("CREATE BRANCH produces its existing reference action");
        };
        crate::mv::domain::ref_flow::prepare_action(binding, &action).map_err(project)
    }

    pub fn execute_command(
        &self,
        statement: &novarocks_sql::semantic::command::AlterIcebergTableSqlCommand,
        current_database: &str,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<StatementResult, novarocks_query_application::api::CommandError> {
        crate::mv::domain::ref_flow::execute_with_ports(
            self.connector_control.as_ref(),
            self.storage_observation.as_ref(),
            self.mv_readiness.as_ref(),
            current_database,
            statement,
            connector_context,
        )
    }
}
