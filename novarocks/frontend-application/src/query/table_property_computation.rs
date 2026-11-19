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
use novarocks_parser::ast::{AstVec, StatementProperty, TrinoCreateTable};
use novarocks_query_application::{
    api::{CatalogTablePropertyBinding, CommandContext},
    session_error::{QueryServiceError, QueryServiceErrorKind},
    sql::property_evaluation::table::{
        prepare_trino_create_table, prepare_trino_set_table_properties,
    },
};
use novarocks_spi::connector::{
    ConnectorCatalogMutationOperation, ConnectorCatalogMutationRequest,
    ConnectorMutationOperationId, ConnectorProviderBindingKey, ConnectorTableMetadata,
    CreatePolicy,
};
use novarocks_sql::{
    compiler::SqlCompileControl,
    semantic::{TableColumnDef, command::CreateTableSqlCommand},
};
use novarocks_type_contract::{CompilePhase, PureCompileControl};
use std::sync::Arc;

pub(crate) enum TrinoTablePropertySyntax {
    Create(TrinoCreateTable),
    Set(AstVec<StatementProperty>),
}

pub(crate) struct TrinoTablePropertyCpuInput {
    pub(crate) source: Arc<str>,
    pub(crate) syntax: TrinoTablePropertySyntax,
    pub(crate) binding: CatalogTablePropertyBinding,
    pub(crate) command: CommandContext,
    pub(crate) context: PropertyCpuContext,
}

pub(crate) enum TrinoTablePropertyOutput {
    Mutation(PreparedTableMutationInput),
    Like(Box<PreparedTrinoTableLike>),
}

pub(crate) struct PreparedTrinoTableLike {
    pub(crate) table: CreateTableSqlCommand,
    binding: CatalogTablePropertyBinding,
    command: CommandContext,
}

impl PreparedTrinoTableLike {
    pub(crate) fn finish(
        self,
        columns: Vec<TableColumnDef>,
    ) -> Result<PreparedTableMutationInput, QueryServiceError> {
        freeze_create(self.binding, self.command, self.table, columns)
    }
}

impl TrinoTablePropertyCpuInput {
    pub(crate) fn run(self) -> Result<TrinoTablePropertyOutput, QueryServiceError> {
        let Self {
            source,
            syntax,
            binding,
            command,
            context,
        } = self;
        verify_original_scope(&context, &command)?;
        let mut constants = context.constant_port();
        match syntax {
            TrinoTablePropertySyntax::Create(statement) => {
                let table = prepare_trino_create_table(
                    &source,
                    &statement,
                    binding.mutation(),
                    &mut constants,
                    &context.control,
                )?;
                if table.like.is_some() {
                    return Ok(TrinoTablePropertyOutput::Like(Box::new(
                        PreparedTrinoTableLike {
                            table,
                            binding,
                            command,
                        },
                    )));
                }
                let columns =
                    crate::catalog_application::statement::lower_semantic_create_table_columns(
                        &table.columns,
                    )
                    .map_err(column_lowering_error)?;
                freeze_create(binding, command, table, columns)
                    .map(TrinoTablePropertyOutput::Mutation)
            }
            TrinoTablePropertySyntax::Set(properties) => {
                let operation = prepare_trino_set_table_properties(
                    &source,
                    &properties,
                    &binding,
                    &mut constants,
                    &context.control,
                )?;
                freeze_mutation(binding, command, operation).map(TrinoTablePropertyOutput::Mutation)
            }
        }
    }
}

fn verify_original_scope(
    context: &PropertyCpuContext,
    command: &CommandContext,
) -> Result<(), QueryServiceError> {
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
    Ok(())
}

fn freeze_create(
    binding: CatalogTablePropertyBinding,
    command: CommandContext,
    prepared: CreateTableSqlCommand,
    columns: Vec<TableColumnDef>,
) -> Result<PreparedTableMutationInput, QueryServiceError> {
    let columns = columns
        .iter()
        .map(crate::catalog_application::statement::connector_column)
        .collect::<Result<Vec<_>, _>>()
        .map_err(column_lowering_error)?;
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
    freeze_mutation(binding, command, operation)
}

fn freeze_mutation(
    binding: CatalogTablePropertyBinding,
    command: CommandContext,
    operation: ConnectorCatalogMutationOperation,
) -> Result<PreparedTableMutationInput, QueryServiceError> {
    let request = ConnectorCatalogMutationRequest {
        operation_id: ConnectorMutationOperationId::new(),
        target: ConnectorProviderBindingKey {
            instance_id: binding.mutation().descriptor().instance_id.clone(),
            incarnation: binding.planning().binding().incarnation(),
        },
        operation,
        context: command.connector_context().clone(),
    };
    // Keep the execution copy on the actual planning CPU.
    PreparedTableMutationInput::try_new(binding, request)
        .map_err(super::product_command_service_error)
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
            .map_err(column_lowering_error)?;
        let mut columns = Vec::with_capacity(source.len());
        for column in &source {
            self.control
                .checkpoint(CompilePhase::Validate, 1)
                .map_err(QueryServiceError::from_compile_control)?;
            columns.push(
                crate::catalog_application::command::create_like_column(column)
                    .map_err(column_lowering_error)?,
            );
        }
        Ok(columns)
    }
}
fn internal(message: &'static str) -> QueryServiceError {
    QueryServiceError::new(QueryServiceErrorKind::Internal, message)
}
fn column_lowering_error(error: String) -> QueryServiceError {
    // Preserve the old column/default author's actual command presentation.
    super::product_command_service_error(error.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_sql::semantic::command::TableColumnSqlCommand;
    use novarocks_sql::semantic::{DefaultLiteral, command::CommandLiteral};
    use novarocks_types::schema::SqlType;

    #[test]
    fn ordinary_port_keeps_real_default_type_value_and_declared_nullability() {
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
        let prepared =
            crate::catalog_application::statement::lower_semantic_create_table_columns(&columns)
                .unwrap();
        assert_eq!(
            prepared,
            vec![
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
    }
    #[test]
    fn failed_original_default_does_not_publish_a_partial_column_vector() {
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
        let actual =
            crate::catalog_application::statement::lower_semantic_create_table_columns(&columns)
                .map_err(column_lowering_error)
                .unwrap_err();
        assert_eq!(actual.kind(), QueryServiceErrorKind::Internal);
        assert!(
            actual
                .message()
                .starts_with("invalid integer DEFAULT `bad`:")
        );
    }
}
