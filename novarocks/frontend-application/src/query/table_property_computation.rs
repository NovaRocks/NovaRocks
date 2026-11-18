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

//! Table property and LIKE declaration preparation on the original planning CPU.

use super::{
    property_cpu_context::PropertyCpuContext, table_mutation_action::PreparedTableMutationInput,
};
use novarocks_parser::ast::legacy::CreateTable;
use novarocks_query_application::{
    api::{CatalogTablePropertyBinding, CommandContext},
    session_error::{QueryServiceError, QueryServiceErrorKind},
    sql::{LegacyCreateTableColumnsPort, prepare_legacy_create_table},
};
use novarocks_spi::connector::{
    ConnectorCatalogMutationOperation, ConnectorCatalogMutationRequest,
    ConnectorMutationOperationId, ConnectorProviderBindingKey, ConnectorTableMetadata,
    CreatePolicy,
};
use novarocks_sql::{
    compiler::SqlCompileControl,
    semantic::{TableColumnDef, command::TableColumnSqlCommand},
};
use novarocks_type_contract::{CompilePhase, PureCompileControl};
use std::sync::Arc;

pub(crate) struct TablePropertyCpuInput {
    pub(crate) source: Arc<str>,
    pub(crate) statement: CreateTable,
    pub(crate) binding: CatalogTablePropertyBinding,
    pub(crate) command: CommandContext,
    pub(crate) like_columns: Option<Vec<TableColumnDef>>,
    pub(crate) context: PropertyCpuContext,
}
impl TablePropertyCpuInput {
    pub(crate) fn run(self) -> Result<PreparedTableMutationInput, QueryServiceError> {
        let Self {
            source,
            statement,
            binding,
            command,
            like_columns,
            context,
        } = self;
        let command_window = command
            .result_window_alias()
            .ok_or_else(|| internal("table property CPU has no original command window"))?;
        if !context.window.is_for_scope(&context.scope)
            || !context.window.is_for_scope(command.scope())
            || !context.window.shares_capacity_with(&command_window)
        {
            return Err(internal(
                "table property CPU differs from its original command Root/window",
            ));
        }
        let mut constants = context.constant_port();
        let mut columns_port = TableColumnsPort::default();
        let prepared = prepare_legacy_create_table(
            &source,
            &statement,
            binding.mutation(),
            command.connector_context(),
            &mut constants,
            &mut columns_port,
            &context.control,
        )?;
        let columns = match (prepared.like.is_some(), like_columns) {
            (false, None) => columns_port.prepared.take().ok_or_else(|| {
                internal("ordinary table columns were not prepared by their original author")
            })?,
            (true, Some(columns)) if columns_port.prepared.is_none() => columns,
            _ => {
                return Err(internal(
                    "table LIKE columns differ from their original CPU preparation branch",
                ));
            }
        };
        let columns = columns
            .iter()
            .map(crate::catalog_application::statement::connector_column)
            .collect::<Result<Vec<_>, _>>()
            .map_err(legacy_column_error)?;
        let operation = ConnectorCatalogMutationOperation::CreateTable {
            table: binding.table().clone(),
            columns,
            comment: prepared.comment.map(Arc::from),
            properties: prepared
                .properties
                .into_iter()
                .map(|(name, value)| (Arc::from(name), value))
                .collect(),
            policy: if prepared.if_not_exists {
                CreatePolicy::NoOpIfExists
            } else {
                CreatePolicy::FailIfExists
            },
        };
        let request = ConnectorCatalogMutationRequest {
            operation_id: ConnectorMutationOperationId::new(),
            target: ConnectorProviderBindingKey {
                instance_id: binding.mutation().descriptor().instance_id.clone(),
                incarnation: binding.planning().binding().incarnation(),
            },
            operation,
            context: command.connector_context().clone(),
        };
        // This run method is the real planning CPU case. The necessary owned
        // execute-request copy is made here, not in the later Action poll.
        PreparedTableMutationInput::try_new(binding, request)
            .map_err(super::product_command_service_error)
    }
}

#[derive(Default)]
struct TableColumnsPort {
    prepared: Option<Vec<TableColumnDef>>,
}
impl LegacyCreateTableColumnsPort for TableColumnsPort {
    fn prepare_columns(
        &mut self,
        columns: &[TableColumnSqlCommand],
    ) -> Result<(), QueryServiceError> {
        if self.prepared.is_some() {
            return Err(internal(
                "ordinary table columns were prepared more than once",
            ));
        }
        self.prepared = Some(
            crate::catalog_application::statement::lower_semantic_create_table_columns(columns)
                .map_err(legacy_column_error)?,
        );
        Ok(())
    }
}

pub(crate) struct LikeColumnsCpuInput {
    pub(crate) metadata: ConnectorTableMetadata,
    pub(crate) control: SqlCompileControl,
}
impl LikeColumnsCpuInput {
    pub(crate) fn run(self) -> Result<Vec<TableColumnDef>, QueryServiceError> {
        self.control
            .checkpoint(CompilePhase::Validate, 1)
            .map_err(QueryServiceError::from_compile_control)?;
        let source = crate::connector::sql_columns_from_connector_metadata(&self.metadata)
            .map_err(legacy_column_error)?;
        let mut columns = Vec::with_capacity(source.len());
        for column in &source {
            self.control
                .checkpoint(CompilePhase::Validate, 1)
                .map_err(QueryServiceError::from_compile_control)?;
            columns.push(
                crate::catalog_application::command::create_like_column(column)
                    .map_err(legacy_column_error)?,
            );
        }
        Ok(columns)
    }
}
fn internal(message: &'static str) -> QueryServiceError {
    QueryServiceError::new(QueryServiceErrorKind::Internal, message)
}
fn legacy_column_error(error: String) -> QueryServiceError {
    // Preserve the old column/default author's actual command presentation.
    super::product_command_service_error(error.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_sql::semantic::{DefaultLiteral, command::CommandLiteral};
    use novarocks_types::schema::SqlType;

    #[test]
    fn ordinary_port_keeps_real_default_type_value_and_declared_nullability() {
        let mut port = TableColumnsPort::default();
        assert!(port.prepared.is_none());
        let columns = [
            TableColumnSqlCommand {
                name: "id".into(),
                data_type: SqlType::BigInt,
                nullable: Some(false),
                default: Some(CommandLiteral::Number("7".into())),
                comment: None,
            },
            TableColumnSqlCommand {
                name: "amount".into(),
                data_type: SqlType::Decimal {
                    precision: 10,
                    scale: 2,
                },
                nullable: None,
                default: Some(CommandLiteral::String("12.30".into())),
                comment: None,
            },
        ];
        port.prepare_columns(&columns).unwrap();
        assert_eq!(
            port.prepared.as_ref().unwrap(),
            &vec![
                TableColumnDef {
                    name: "id".into(),
                    data_type: SqlType::BigInt,
                    nullable: false,
                    default: Some(DefaultLiteral::Int(7))
                },
                TableColumnDef {
                    name: "amount".into(),
                    data_type: SqlType::Decimal {
                        precision: 10,
                        scale: 2
                    },
                    nullable: true,
                    default: Some(DefaultLiteral::Decimal {
                        unscaled: 1230,
                        scale: 2
                    })
                },
            ]
        );
        assert!(port.prepare_columns(&columns).is_err());
    }
    #[test]
    fn failed_original_default_does_not_publish_a_partial_column_vector() {
        let mut port = TableColumnsPort::default();
        let columns = [
            TableColumnSqlCommand {
                name: "valid".into(),
                data_type: SqlType::BigInt,
                nullable: Some(false),
                default: Some(CommandLiteral::Number("3".into())),
                comment: None,
            },
            TableColumnSqlCommand {
                name: "bad".into(),
                data_type: SqlType::Int,
                nullable: Some(false),
                default: Some(CommandLiteral::String("bad".into())),
                comment: None,
            },
        ];
        let actual = port.prepare_columns(&columns).unwrap_err();
        assert!(port.prepared.is_none());
        assert_eq!(actual.kind(), QueryServiceErrorKind::Internal);
        assert!(
            actual
                .message()
                .starts_with("invalid integer DEFAULT `bad`:")
        );
    }
}
