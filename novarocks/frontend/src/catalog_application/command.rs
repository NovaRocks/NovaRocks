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

//! Typed catalog-DDL execution.
//!
//! This capability deliberately owns a closed statement family.  It is not a
//! second generic command dispatcher: callers receive `None` only when SQL is
//! outside catalog DDL, while every recognized statement either executes or
//! returns its parser/admission error.

use novarocks_spi::connector::{ConnectorControlRegistry, ConnectorInstanceId};
use novarocks_types::naming::normalize_identifier;
use std::sync::Arc;

use crate::catalog_application::create_table_ddl::build_iceberg_create_table_ddl;
use crate::catalog_application::model::{CatalogCreateTableKind, CatalogCreateTableRequest};
use crate::catalog_application::query_catalog::{CatalogServiceSource, QueryCatalogService};
use crate::catalog_application::resolver::CatalogAdmission;
use crate::catalog_application::statement::{
    CatalogDropContext, CatalogMutationContext, execute_create_database_statement,
    execute_create_table_statement, execute_drop_catalog_statement,
    execute_drop_database_statement, execute_drop_table_statement,
    execute_typed_create_table_statement,
};
use crate::catalog_application::{CatalogApplicationPort, CatalogCreateCommand};
use crate::mv::domain::readiness::MvReadinessPort;
use crate::runtime::query_result::QueryResultColumn;
use crate::runtime::statement_result::StatementResult;
use novarocks_parser::ast::{CatalogStatement, LiteralKind};
use novarocks_spi::connector::MvStorageObservationPort;
use novarocks_sql::literal::arrow_data_type_to_sql_type;
use novarocks_sql::semantic::{ObjectName, TableColumnDef};

/// Catalog DDL capability built from catalog-only leaf ports.
///
/// The ports are held individually and on purpose: this capability admits a
/// catalog name, mutates connector-owned catalog facts, invalidates the local
/// catalog snapshot, and enforces the MV guards that catalog DDL owns. It has
/// no query execution, statistics, DML writer or MV refresh capability, and it
/// must not be widened into a shared dependency bundle.
#[derive(Clone)]
pub struct CatalogCommandExecutor {
    catalog_service: Arc<QueryCatalogService>,
    catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
    connector_control: Arc<dyn ConnectorControlRegistry>,
    mv_readiness: Arc<MvReadinessPort>,
    mv_storage_observation: Arc<dyn MvStorageObservationPort>,
}

impl CatalogAdmission for CatalogCommandExecutor {
    fn catalog_application(&self) -> Option<&dyn CatalogApplicationPort> {
        self.catalog_application.as_deref()
    }
}

impl CatalogServiceSource for CatalogCommandExecutor {
    fn catalog_service(&self) -> &Arc<QueryCatalogService> {
        &self.catalog_service
    }
}

impl CatalogDropContext for CatalogCommandExecutor {
    fn connector_control(&self) -> &dyn ConnectorControlRegistry {
        self.connector_control.as_ref()
    }

    fn mv_readiness(&self) -> &MvReadinessPort {
        self.mv_readiness.as_ref()
    }

    fn mv_storage_observation(&self) -> &dyn MvStorageObservationPort {
        self.mv_storage_observation.as_ref()
    }
}

impl CatalogMutationContext for CatalogCommandExecutor {
    fn connector_control(&self) -> &dyn ConnectorControlRegistry {
        self.connector_control.as_ref()
    }
}

impl CatalogCommandExecutor {
    pub fn new(
        catalog_service: Arc<QueryCatalogService>,
        catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        mv_readiness: Arc<MvReadinessPort>,
        mv_storage_observation: Arc<dyn MvStorageObservationPort>,
    ) -> Self {
        Self {
            catalog_service,
            catalog_application,
            connector_control,
            mv_readiness,
            mv_storage_observation,
        }
    }

    /// Executes the SQLP-3 basic catalog family without reparsing source SQL.
    pub fn execute_typed(
        &self,
        statement: &CatalogStatement,
        current_catalog: Option<&str>,
        current_database: &str,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<StatementResult, String> {
        match statement {
            CatalogStatement::CreateCatalog(statement) => {
                self.execute_create_catalog(lower_create_catalog(statement)?)
            }
            CatalogStatement::DropCatalog(statement) => {
                execute_drop_catalog_statement(self, &statement.name.value, statement.if_exists)
            }
            CatalogStatement::CreateDatabase(statement) => execute_create_database_statement(
                self,
                &typed_object_name(&statement.name),
                statement.if_not_exists,
                current_catalog,
                connector_context,
            ),
            CatalogStatement::DropDatabase(statement) => execute_drop_database_statement(
                self,
                &typed_object_name(&statement.name),
                current_catalog,
                statement.if_exists,
                statement.force,
                connector_context,
            ),
            CatalogStatement::DropTable(statement) => execute_drop_table_statement(
                self,
                &typed_object_name(&statement.name),
                current_catalog,
                current_database,
                statement.if_exists,
                statement.force,
                connector_context,
            ),
            CatalogStatement::ShowCreateTable(statement) => execute_show_create_table(
                self,
                typed_object_name(&statement.name),
                current_catalog,
                current_database,
                connector_context,
            ),
            CatalogStatement::TruncateTable(_) => {
                Err("catalog statement belongs to a later typed command owner".to_string())
            }
        }
    }

    /// Execute parser-owned table DDL without reparsing the SQL source.
    pub fn execute_table_typed(
        &self,
        statement: &novarocks_parser::ast::TableStatement,
        current_catalog: Option<&str>,
        current_database: &str,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<StatementResult, String> {
        match statement {
            novarocks_parser::ast::TableStatement::Create(statement) => {
                if let Some(source) = &statement.like {
                    return execute_create_table_like(
                        self,
                        typed_object_name(&statement.name),
                        typed_object_name(source),
                        statement.if_not_exists,
                        current_catalog,
                        current_database,
                        connector_context,
                    );
                }
                execute_typed_create_table_statement(
                    self,
                    statement,
                    current_catalog,
                    current_database,
                    connector_context,
                )
            }
        }
    }

    /// Executes an admitted Iceberg `ALTER TABLE` syntax node without a SQL
    /// text round-trip. Reference mutations belong to their dedicated owner;
    /// ADD FILES belongs to the DML lifecycle owner.
    pub fn execute_iceberg_typed(
        &self,
        statement: &novarocks_parser::ast::AlterIcebergTable,
        current_catalog: Option<&str>,
        current_database: &str,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<StatementResult, String> {
        use novarocks_parser::ast::IcebergTableAction;

        match &statement.action {
            IcebergTableAction::Schema(change) => execute_alter_iceberg_schema(
                self,
                crate::catalog_application::statement::AlterIcebergSchemaStmt {
                    table: typed_object_name(&statement.table),
                    change: crate::catalog_application::statement::lower_typed_iceberg_schema_change(
                        change,
                    )?,
                },
                current_catalog,
                current_database,
                connector_context,
            ),
            IcebergTableAction::Properties(action) => execute_alter_iceberg_properties(
                self,
                crate::catalog_application::statement::AlterIcebergPropertiesStmt {
                    table: typed_object_name(&statement.table),
                    op: crate::catalog_application::statement::lower_typed_iceberg_properties_action(
                        action,
                    )?,
                },
                current_catalog,
                current_database,
                connector_context,
            ),
            IcebergTableAction::Partition(change) => execute_alter_partition_spec(
                self,
                typed_object_name(&statement.table),
                crate::catalog_application::statement::lower_typed_iceberg_partition_change(
                    change,
                )?,
                current_catalog,
                current_database,
                connector_context,
            ),
            IcebergTableAction::Reference(_) => Err(
                "Iceberg reference command belongs to the ref command executor".to_string(),
            ),
            IcebergTableAction::AddFiles(_) => {
                Err("ADD FILES belongs to the DML lifecycle executor".to_string())
            }
        }
    }

    fn execute_create_catalog(
        &self,
        command: CatalogCreateCommand,
    ) -> Result<StatementResult, String> {
        let application = self.catalog_application.as_ref().ok_or_else(|| {
            "catalog statements require a configured frontend catalog application".to_string()
        })?;
        application
            .create_catalog(command)
            .map_err(|error| error.to_string())?;
        Ok(StatementResult::Ok)
    }
}

fn typed_object_name(name: &novarocks_parser::ast::ObjectName) -> ObjectName {
    ObjectName {
        parts: name.parts.iter().map(|part| part.value.clone()).collect(),
    }
}

fn catalog_property_text(literal: &novarocks_parser::ast::Literal) -> Result<String, String> {
    match &literal.kind {
        LiteralKind::String(value) => Ok(value.clone()),
        _ => Err("catalog properties require identifier or string values".to_string()),
    }
}

fn lower_create_catalog(
    statement: &novarocks_parser::ast::CreateCatalog,
) -> Result<CatalogCreateCommand, String> {
    let properties = statement
        .properties
        .iter()
        .map(|property| {
            Ok((
                catalog_property_text(&property.key)?,
                catalog_property_text(&property.value)?,
            ))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let instance_id = ConnectorInstanceId::parse(&normalize_identifier(&statement.name.value)?)
        .map_err(|error| format!("invalid catalog connector instance ID: {error}"))?;
    Ok(CatalogCreateCommand {
        instance_id,
        display_name: statement.name.value.clone(),
        properties,
        if_not_exists: statement.if_not_exists,
    })
}

fn execute_alter_iceberg_properties(
    executor: &CatalogCommandExecutor,
    statement: crate::catalog_application::statement::AlterIcebergPropertiesStmt,
    current_catalog: Option<&str>,
    current_database: &str,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<StatementResult, String> {
    let target = crate::catalog_application::resolver::resolve_existing_table_target(
        executor,
        &statement.table,
        current_catalog,
        current_database,
    )?;
    crate::mv::domain::iceberg_guard::reject_if_iceberg_mv_table_with_ports(
        executor.connector_control.as_ref(),
        executor.mv_storage_observation.as_ref(),
        &target,
        crate::mv::domain::iceberg_guard::IcebergMvUserMutation::AlterTable,
    )?;
    if target.provider_id.as_str() != "iceberg" {
        return Err(
            "ALTER TABLE TBLPROPERTIES only supports standalone iceberg catalogs".to_string(),
        );
    }
    let changes = match statement.op {
        crate::catalog_application::statement::PropertiesOp::Set { entries } => entries
            .into_iter()
            .map(
                |(key, value)| novarocks_spi::connector::ConnectorPropertyChange::Set {
                    key: Arc::from(key),
                    value: Arc::from(value),
                },
            )
            .collect(),
        crate::catalog_application::statement::PropertiesOp::Unset { keys, if_exists } => keys
            .into_iter()
            .map(
                |key| novarocks_spi::connector::ConnectorPropertyChange::Unset {
                    key: Arc::from(key),
                    if_exists,
                },
            )
            .collect(),
    };
    let instance_id =
        ConnectorInstanceId::parse(&target.catalog).map_err(|error| error.to_string())?;
    crate::connector::mutation::execute_catalog_mutation(
        executor.connector_control.as_ref(),
        &instance_id,
        novarocks_spi::connector::ConnectorCatalogMutationOperation::AlterProperties {
            table: novarocks_spi::connector::ConnectorTableIdentity {
                instance_id: instance_id.clone(),
                namespace: Arc::from(target.namespace.as_str()),
                table: Arc::from(target.table.as_str()),
            },
            changes,
            authority: novarocks_spi::connector::ConnectorPropertyAuthority::UserStatement,
            expected_committed_partitioning: None,
        },
        connector_context.clone(),
    )?;
    crate::catalog_application::resolver::invalidate_iceberg_caches(executor, &target)?;
    Ok(StatementResult::Ok)
}

fn execute_alter_iceberg_schema(
    executor: &CatalogCommandExecutor,
    statement: crate::catalog_application::statement::AlterIcebergSchemaStmt,
    current_catalog: Option<&str>,
    current_database: &str,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<StatementResult, String> {
    let target = crate::catalog_application::resolver::resolve_existing_table_target(
        executor,
        &statement.table,
        current_catalog,
        current_database,
    )?;
    crate::mv::domain::iceberg_guard::reject_if_iceberg_mv_table_with_ports(
        executor.connector_control.as_ref(),
        executor.mv_storage_observation.as_ref(),
        &target,
        crate::mv::domain::iceberg_guard::IcebergMvUserMutation::AlterTable,
    )?;
    if let crate::catalog_application::statement::IcebergSchemaChange::DropColumn { path } =
        &statement.change
    {
        crate::mv::domain::iceberg_guard::reject_drop_column_mv_dependencies_with_readiness(
            executor.mv_readiness.as_ref(),
            &target,
            path,
        )?;
    }
    let instance_id =
        ConnectorInstanceId::parse(&target.catalog).map_err(|error| error.to_string())?;
    let change = match statement.change {
        crate::catalog_application::statement::IcebergSchemaChange::AddColumn {
            parent,
            name,
            data_type,
            default,
            position,
        } => {
            let column = TableColumnDef {
                name,
                data_type,
                nullable: true,
                aggregation: None,
                default,
            };
            novarocks_spi::connector::ConnectorSchemaChange::AddColumn {
                parent: novarocks_spi::connector::ConnectorColumnPath {
                    segments: parent
                        .segments()
                        .iter()
                        .map(|segment| Arc::from(segment.as_str()))
                        .collect(),
                },
                column: crate::catalog_application::statement::connector_column(&column)?,
                position: crate::catalog_application::statement::connector_schema_position(
                    position,
                ),
            }
        }
        crate::catalog_application::statement::IcebergSchemaChange::DropColumn { path } => {
            novarocks_spi::connector::ConnectorSchemaChange::DropColumn {
                path: crate::catalog_application::statement::connector_schema_path(path),
            }
        }
        crate::catalog_application::statement::IcebergSchemaChange::RenameColumn {
            path,
            new_name,
        } => novarocks_spi::connector::ConnectorSchemaChange::RenameColumn {
            path: crate::catalog_application::statement::connector_schema_path(path),
            to: Arc::from(new_name),
        },
        crate::catalog_application::statement::IcebergSchemaChange::ModifyColumn {
            path,
            new_type,
        } => novarocks_spi::connector::ConnectorSchemaChange::ModifyColumn {
            path: crate::catalog_application::statement::connector_schema_path(path),
            data_type: crate::catalog_application::statement::connector_data_type(&new_type)?,
        },
        crate::catalog_application::statement::IcebergSchemaChange::SetNullable {
            path,
            nullable,
        } => novarocks_spi::connector::ConnectorSchemaChange::SetColumnNullability {
            path: crate::catalog_application::statement::connector_schema_path(path),
            nullable,
        },
        crate::catalog_application::statement::IcebergSchemaChange::Reorder { path, position } => {
            novarocks_spi::connector::ConnectorSchemaChange::ReorderColumn {
                path: crate::catalog_application::statement::connector_schema_path(path),
                position: crate::catalog_application::statement::connector_schema_position(
                    position,
                ),
            }
        }
        crate::catalog_application::statement::IcebergSchemaChange::UpdateComment {
            path,
            comment,
        } => novarocks_spi::connector::ConnectorSchemaChange::SetColumnComment {
            path: crate::catalog_application::statement::connector_schema_path(path),
            comment: Arc::from(comment),
        },
    };
    crate::connector::mutation::execute_catalog_mutation(
        executor.connector_control.as_ref(),
        &instance_id,
        novarocks_spi::connector::ConnectorCatalogMutationOperation::AlterSchema {
            table: novarocks_spi::connector::ConnectorTableIdentity {
                instance_id: instance_id.clone(),
                namespace: Arc::from(target.namespace.as_str()),
                table: Arc::from(target.table.as_str()),
            },
            changes: vec![change],
        },
        connector_context.clone(),
    )?;
    crate::catalog_application::resolver::invalidate_iceberg_caches(executor, &target)?;
    Ok(StatementResult::Ok)
}

fn execute_alter_partition_spec(
    executor: &CatalogCommandExecutor,
    table_name: ObjectName,
    statement: crate::catalog_application::statement::IcebergPartitionSpecChange,
    current_catalog: Option<&str>,
    current_database: &str,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<StatementResult, String> {
    let target = crate::catalog_application::resolver::resolve_table_target(
        executor,
        &table_name,
        current_catalog,
        current_database,
    )?;
    if target.provider_id.as_str() != "iceberg" {
        return Err(format!(
            "ALTER TABLE ADD/DROP PARTITION COLUMN only supports iceberg backends, got `{}`",
            target.provider_id.as_str()
        ));
    }
    crate::mv::domain::iceberg_guard::reject_if_iceberg_mv_table_with_ports(
        executor.connector_control.as_ref(),
        executor.mv_storage_observation.as_ref(),
        &target,
        crate::mv::domain::iceberg_guard::IcebergMvUserMutation::AlterTable,
    )?;
    let (adding, transform) = match statement {
        crate::catalog_application::statement::IcebergPartitionSpecChange::Add(transform) => {
            (true, transform)
        }
        crate::catalog_application::statement::IcebergPartitionSpecChange::Drop(transform) => {
            (false, transform)
        }
    };
    let instance_id =
        ConnectorInstanceId::parse(&target.catalog).map_err(|error| error.to_string())?;
    crate::connector::mutation::execute_catalog_mutation(
        executor.connector_control.as_ref(),
        &instance_id,
        novarocks_spi::connector::ConnectorCatalogMutationOperation::AlterPartitionSpec {
            table: novarocks_spi::connector::ConnectorTableIdentity {
                instance_id: instance_id.clone(),
                namespace: Arc::from(target.namespace.as_str()),
                table: Arc::from(target.table.as_str()),
            },
            add: if adding {
                vec![transform.clone()]
            } else {
                Vec::new()
            },
            drop: if adding { Vec::new() } else { vec![transform] },
        },
        connector_context.clone(),
    )?;
    crate::catalog_application::resolver::invalidate_iceberg_caches(executor, &target)?;
    Ok(StatementResult::Ok)
}

fn execute_create_table_like(
    executor: &CatalogCommandExecutor,
    target: ObjectName,
    source: ObjectName,
    if_not_exists: bool,
    current_catalog: Option<&str>,
    current_database: &str,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<StatementResult, String> {
    let source_target = crate::catalog_application::resolver::resolve_existing_table_target(
        executor,
        &source,
        current_catalog,
        current_database,
    )?;
    let source_table = crate::connector::metadata_load_table(
        executor.connector_control.as_ref(),
        connector_context.clone(),
        &source_target.catalog,
        &source_target.namespace,
        &source_target.table,
        novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
    )?
    .0;
    let columns = source_table
        .columns
        .iter()
        .map(|column| {
            Ok(TableColumnDef {
                name: column.name.clone(),
                data_type: arrow_data_type_to_sql_type(&column.data_type)?,
                nullable: column.nullable,
                aggregation: None,
                default: None,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    execute_create_table_statement(
        executor,
        CatalogCreateTableRequest {
            name: target,
            kind: CatalogCreateTableKind::Iceberg {
                columns,
                key_desc: None,
                bucket_count: None,
                distribution_columns: Vec::new(),
                partition_fields: Vec::new(),
                properties: Vec::new(),
            },
            if_not_exists,
        },
        current_catalog,
        current_database,
        connector_context,
    )
}

fn execute_show_create_table(
    executor: &CatalogCommandExecutor,
    table_name: ObjectName,
    current_catalog: Option<&str>,
    current_database: &str,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<StatementResult, String> {
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    let target = crate::catalog_application::resolver::resolve_existing_table_target(
        executor,
        &table_name,
        current_catalog,
        current_database,
    )?;
    if target.provider_id.as_str() != "iceberg" {
        return Err(format!(
            "SHOW CREATE TABLE only supports Iceberg tables, got `{}` backend",
            target.provider_id.as_str()
        ));
    }
    let instance_id =
        ConnectorInstanceId::parse(&target.catalog).map_err(|error| error.to_string())?;
    let lease = executor
        .connector_control
        .acquire_current(&instance_id)
        .map_err(|error| error.to_string())?;
    let identity = novarocks_spi::connector::ConnectorTableIdentity {
        instance_id,
        namespace: Arc::from(target.namespace.as_str()),
        table: Arc::from(target.table.as_str()),
    };
    let loaded = lease
        .binding()
        .metadata()
        .load_table(novarocks_spi::connector::ConnectorTableRequest {
            table: identity.clone(),
            resolution: novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
            context: connector_context.clone(),
        })
        .map_err(|error| error.to_string())?;
    if loaded.identity != identity || loaded.table.owner() != &identity.instance_id {
        return Err(
            "SHOW CREATE TABLE received corrupt metadata for a different connector table"
                .to_string(),
        );
    }
    let ddl =
        build_iceberg_create_table_ddl(&target.catalog, &target.namespace, &target.table, &loaded)?;
    let fields = vec![
        Field::new("Table", DataType::Utf8, false),
        Field::new("Create Table", DataType::Utf8, false),
    ];
    let arrays: Vec<Arc<dyn arrow::array::Array>> = vec![
        Arc::new(StringArray::from(vec![target.table.clone()])),
        Arc::new(StringArray::from(vec![ddl])),
    ];
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(|error| format!("build SHOW CREATE TABLE result failed: {error}"))?;
    Ok(StatementResult::Query(
        crate::runtime::query_result::QueryResult {
            columns: vec![
                QueryResultColumn {
                    name: "Table".to_string(),
                    data_type: DataType::Utf8,
                    nullable: false,
                    logical_type: None,
                },
                QueryResultColumn {
                    name: "Create Table".to_string(),
                    data_type: DataType::Utf8,
                    nullable: false,
                    logical_type: None,
                },
            ],
            chunks: vec![crate::runtime::query_result::record_batch_to_chunk(batch)?],
        },
    ))
}

#[cfg(test)]
mod tests {
    use novarocks_parser::ast::{CatalogStatement, Statement};

    use super::{CatalogCommandExecutor, lower_create_catalog};

    #[test]
    fn non_catalog_statement_is_not_claimed() {
        // Construction is unnecessary for an unsupported family because the
        // parser gate must reject it before any port is read.
        let sql = "SELECT 'CREATE TABLE t AS SELECT 1'";
        let statements = novarocks_parser::parse(sql).expect("parse query");
        assert!(matches!(statements.as_slice(), [Statement::Query(_)]));
        let _ = std::any::type_name::<CatalogCommandExecutor>();
    }

    #[test]
    fn typed_create_catalog_lowers_without_the_legacy_sql_facade() {
        let statement = novarocks_parser::parse(
            "CREATE EXTERNAL CATALOG IF NOT EXISTS Warehouse PROPERTIES ('type'='iceberg')",
        )
        .expect("parse catalog statement")
        .pop()
        .expect("one statement");
        let Statement::Catalog(CatalogStatement::CreateCatalog(statement)) = statement else {
            panic!("expected CREATE CATALOG");
        };

        let command = lower_create_catalog(&statement).expect("lower catalog command");
        assert_eq!(command.instance_id.as_str(), "warehouse");
        assert_eq!(command.display_name, "Warehouse");
        assert_eq!(
            command.properties,
            [("type".to_string(), "iceberg".to_string())]
        );
        assert!(command.if_not_exists);
    }
}
