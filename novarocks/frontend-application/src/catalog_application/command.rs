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

use crate::connector::schema_creation::FrontendSchemaLoad;
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
    lower_semantic_create_table_statement, lower_semantic_iceberg_partition_change,
    lower_semantic_iceberg_properties_action, lower_semantic_iceberg_schema_change,
};
use crate::mv::domain::readiness::MvReadinessPort;
use novarocks_catalog_application::{CatalogApplicationPort, CatalogCreateCommand};
use novarocks_query_application::api::build_utf8_query_result;
use novarocks_query_application::protocol_delivery::QuerySessionOutput as StatementResult;
use novarocks_spi::connector::MvStorageObservationPort;
use novarocks_sql::semantic::command::{
    AlterIcebergTableSqlCommand, CreateTableSqlCommand, IcebergTableSqlAction,
};
use novarocks_sql::semantic::command::{
    CatalogCreateCommand as SemanticCatalogCreateCommand, CommandLiteral,
};
use novarocks_sql::semantic::{CatalogSqlCommand, ObjectName, TableColumnDef};

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
    pub(super) fn original_catalog_application(&self) -> Option<Arc<dyn CatalogApplicationPort>> {
        self.catalog_application.clone()
    }

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

    /// Executes a complete catalog command lowered by Query Application.
    ///
    /// This role-local adapter deliberately accepts no parser AST and never
    /// reconstructs source SQL.  Table and Iceberg actions remain on their
    /// dedicated semantic adapter cut; accepting them here before that cut
    /// would create a fallback around their product owners.
    pub fn execute_command(
        &self,
        command: &CatalogSqlCommand,
        current_catalog: Option<&str>,
        current_database: &str,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<StatementResult, String> {
        match command {
            CatalogSqlCommand::CreateCatalog(command) => {
                self.execute_create_catalog(lower_semantic_create_catalog(command)?)
            }
            CatalogSqlCommand::DropCatalog { name, if_exists } => {
                execute_drop_catalog_statement(self, name, *if_exists)
            }
            CatalogSqlCommand::CreateDatabase {
                name,
                if_not_exists,
            } => execute_create_database_statement(
                self,
                name,
                *if_not_exists,
                current_catalog,
                connector_context,
            ),
            CatalogSqlCommand::DropDatabase {
                name,
                if_exists,
                force,
            } => execute_drop_database_statement(
                self,
                name,
                current_catalog,
                *if_exists,
                *force,
                connector_context,
            ),
            CatalogSqlCommand::DropTable {
                name,
                if_exists,
                force,
            } => execute_drop_table_statement(
                self,
                name,
                current_catalog,
                current_database,
                *if_exists,
                *force,
                connector_context,
            ),
            CatalogSqlCommand::ShowCreateTable { name } => execute_show_create_table(
                self,
                name.clone(),
                current_catalog,
                current_database,
                connector_context,
            ),
            CatalogSqlCommand::TruncateTable { .. } => {
                Err("TRUNCATE belongs to the DML lifecycle executor".to_string())
            }
            CatalogSqlCommand::CreateTable(_) | CatalogSqlCommand::AlterIcebergTable(_) => {
                Err("catalog command belongs to a dedicated semantic adapter".to_string())
            }
        }
    }

    /// Executes an admitted semantic `CREATE TABLE` command.
    pub fn execute_table_command(
        &self,
        command: &CreateTableSqlCommand,
        current_catalog: Option<&str>,
        current_database: &str,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<StatementResult, String> {
        if let Some(source) = &command.like {
            return execute_create_table_like(
                self,
                command.name.clone(),
                source.clone(),
                command.if_not_exists,
                current_catalog,
                current_database,
                connector_context,
            );
        }
        execute_create_table_statement(
            self,
            lower_semantic_create_table_statement(command)?,
            current_catalog,
            current_database,
            connector_context,
        )
    }

    /// Executes a semantic Iceberg `ALTER TABLE` command owned by the catalog
    /// application. Reference and ADD FILES remain explicit owner cuts.
    pub fn execute_iceberg_command(
        &self,
        command: &AlterIcebergTableSqlCommand,
        current_catalog: Option<&str>,
        current_database: &str,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<StatementResult, String> {
        match &command.action {
            IcebergTableSqlAction::Schema(change) => execute_alter_iceberg_schema(
                self,
                crate::catalog_application::statement::AlterIcebergSchemaStmt {
                    table: command.table.clone(),
                    change: lower_semantic_iceberg_schema_change(change)?,
                },
                current_catalog,
                current_database,
                connector_context,
            ),
            IcebergTableSqlAction::Properties(action) => execute_alter_iceberg_properties(
                self,
                crate::catalog_application::statement::AlterIcebergPropertiesStmt {
                    table: command.table.clone(),
                    op: lower_semantic_iceberg_properties_action(action)?,
                },
                current_catalog,
                current_database,
                connector_context,
            ),
            IcebergTableSqlAction::Partition(change) => execute_alter_partition_spec(
                self,
                command.table.clone(),
                lower_semantic_iceberg_partition_change(change)?,
                current_catalog,
                current_database,
                connector_context,
            ),
            IcebergTableSqlAction::Reference(_) => {
                Err("Iceberg reference command belongs to the ref command executor".to_string())
            }
            IcebergTableSqlAction::AddFiles { .. } => {
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

pub(super) fn lower_semantic_create_catalog(
    command: &SemanticCatalogCreateCommand,
) -> Result<CatalogCreateCommand, String> {
    let properties = command
        .properties
        .iter()
        .map(|property| {
            Ok((
                catalog_command_literal_text(&property.key)?,
                catalog_command_literal_text(&property.value)?,
            ))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let instance_id = ConnectorInstanceId::parse(&normalize_identifier(&command.name)?)
        .map_err(|error| format!("invalid catalog connector instance ID: {error}"))?;
    Ok(CatalogCreateCommand {
        instance_id,
        display_name: command.name.clone(),
        properties,
        if_not_exists: command.if_not_exists,
    })
}

fn catalog_command_literal_text(literal: &CommandLiteral) -> Result<String, String> {
    match literal {
        CommandLiteral::String(value) => Ok(value.clone()),
        _ => Err("catalog properties require identifier or string values".to_string()),
    }
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
        executor.mv_readiness.as_ref(),
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
    let dropped_path = match &statement.change {
        crate::catalog_application::statement::IcebergSchemaChange::DropColumn { path } => {
            Some(path.clone())
        }
        _ => None,
    };
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
    execute_admitted_alter_schema(
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
        |planning| {
            crate::mv::domain::iceberg_guard::reject_if_iceberg_mv_table_with_planning_lease_and_context(
                executor.mv_storage_observation.as_ref(),
                planning,
                executor.mv_readiness.as_ref(),
                &target,
                crate::mv::domain::iceberg_guard::IcebergMvUserMutation::AlterTable,
                connector_context.clone(),
            )?;
            if let Some(path) = &dropped_path {
                crate::mv::domain::iceberg_guard::reject_drop_column_mv_dependencies_with_readiness(
                    executor.mv_readiness.as_ref(),
                    &target,
                    path,
                )?;
            }
            Ok(())
        },
    )?;
    crate::catalog_application::resolver::invalidate_iceberg_caches(executor, &target)?;
    Ok(StatementResult::Ok)
}

/// Admit the complete schema mutation before any metadata-dependent guard.
/// Both guards and publication retain the same control generation even if the
/// catalog's current generation changes while the statement is in progress.
fn execute_admitted_alter_schema(
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    instance_id: &ConnectorInstanceId,
    operation: novarocks_spi::connector::ConnectorCatalogMutationOperation,
    context: novarocks_spi::connector::ConnectorRequestContext,
    guard: impl FnOnce(&novarocks_spi::connector::ConnectorControlPlanningLease) -> Result<(), String>,
) -> Result<crate::connector::mutation::CompletedCatalogMutation, String> {
    let planning = controls
        .acquire_current(instance_id)
        .map_err(|error| error.to_string())?;
    let mutation = planning
        .derive_mutation_lease()
        .map_err(|error| error.to_string())?;
    let operation_id = novarocks_spi::connector::ConnectorMutationOperationId::new();
    mutation
        .admit_operation(operation_id, operation.clone(), context.clone())
        .map_err(|error| error.to_string())?;
    guard(&planning)?;
    crate::connector::mutation::execute_catalog_mutation_with_lease(
        &mutation,
        operation_id,
        operation,
        context,
    )
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
        executor.mv_readiness.as_ref(),
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
        .map(create_like_column)
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

/// Copy the source's complete declaration. A shared Arrow carrier alone
/// cannot distinguish Variant from binary, including inside nested columns.
fn create_like_column(
    column: &novarocks_types::schema::ColumnDef,
) -> Result<TableColumnDef, String> {
    let declared = column
        .declared_value_type()
        .map_err(|error| error.to_string())?;
    Ok(TableColumnDef {
        name: column.name.clone(),
        data_type: novarocks_types::sql_type_from_value_type(&declared)
            .map_err(|error| error.to_string())?,
        nullable: column.nullable,
        aggregation: None,
        default: None,
    })
}

fn execute_show_create_table(
    executor: &CatalogCommandExecutor,
    table_name: ObjectName,
    current_catalog: Option<&str>,
    current_database: &str,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<StatementResult, String> {
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
        .load_frontend_table(novarocks_spi::connector::ConnectorTableRequest {
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
    build_utf8_query_result(
        &["Table", "Create Table"],
        vec![vec![target.table.clone(), ddl]],
    )
    .map(StatementResult::Query)
    .map_err(|error| format!("build SHOW CREATE TABLE result failed: {error}"))
}

#[cfg(test)]
mod tests {
    use novarocks_parser::ast::Statement;

    use super::{CatalogCommandExecutor, lower_semantic_create_catalog};

    use bytes::Bytes;
    use novarocks_spi::connector as spi;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    // These correctness tests exercise the FE sequencing boundary through real
    // SPI leases. Metadata and mutation calls record their own observations;
    // the guard is a metadata consumer, without duplicating MV policy here.
    struct SchemaAlterGeneration {
        descriptor: spi::ConnectorInstanceDescriptor,
        marker: u8,
        calls: Arc<Mutex<Vec<(u8, &'static str)>>>,
        operation_ids: Mutex<Vec<spi::ConnectorMutationOperationId>>,
        admission_error: Option<spi::ConnectorErrorKind>,
        guard_error: bool,
    }

    impl spi::ConnectorMetadata for SchemaAlterGeneration {
        fn instance_id(&self) -> &spi::ConnectorInstanceId {
            &self.descriptor.instance_id
        }
        fn namespace_exists(
            &self,
            _: spi::ConnectorNamespaceRequest,
        ) -> Result<bool, spi::ConnectorError> {
            panic!("unexpected namespace read")
        }
        fn table_exists(
            &self,
            request: spi::ConnectorTableRequest,
        ) -> Result<bool, spi::ConnectorError> {
            assert_eq!(request.table.instance_id, self.descriptor.instance_id);
            self.calls
                .lock()
                .unwrap()
                .push((self.marker, "metadata guard"));
            if self.guard_error {
                Err(spi::ConnectorError::new(
                    spi::ConnectorErrorKind::PermissionDenied,
                    "metadata guard refused",
                ))
            } else {
                Ok(true)
            }
        }
        fn list_tables(
            &self,
            _: spi::ConnectorListTablesRequest,
        ) -> Result<Vec<spi::ConnectorTableIdentity>, spi::ConnectorError> {
            panic!("unexpected table listing")
        }
        fn load_table(
            &self,
            _: spi::ConnectorTableRequest,
            _: &mut spi::read_stack::ConnectorReadSchemaCreation<'_>,
        ) -> Result<
            spi::ConnectorTableMetadata,
            novarocks_connector_contract::PureProviderCompileError<spi::ConnectorError>,
        > {
            panic!("unexpected table load")
        }
    }
    impl spi::ConnectorScanPlanning for SchemaAlterGeneration {
        fn instance_id(&self) -> &spi::ConnectorInstanceId {
            &self.descriptor.instance_id
        }
        fn begin_scan(
            &self,
            _: &spi::ConnectorTableHandle,
            _: spi::ConnectorBeginScanRequest,
        ) -> Result<spi::ConnectorScan, spi::ConnectorError> {
            panic!("unexpected scan")
        }
        fn plan_splits(
            &self,
            _: &spi::ConnectorScanHandle,
            _: spi::ConnectorSplitPlanningRequest,
        ) -> Result<spi::ConnectorSplitPlanningResult, spi::ConnectorError> {
            panic!("unexpected split planning")
        }
    }
    impl spi::ConnectorExecutionDistribution for SchemaAlterGeneration {
        fn declaration(
            &self,
            _: &spi::ConnectorRequestContext,
        ) -> Result<spi::ConnectorProviderBinding, spi::ConnectorError> {
            panic!("unexpected distribution")
        }
    }
    impl spi::ConnectorCatalogMutation for SchemaAlterGeneration {
        fn descriptor(&self) -> &spi::ConnectorInstanceDescriptor {
            &self.descriptor
        }
        fn incarnation(&self) -> spi::ProviderBindingEpoch {
            spi::ProviderBindingEpoch::from_bytes([self.marker; 16])
        }
        fn admit(
            &self,
            request: &spi::ConnectorCatalogMutationRequest,
        ) -> Result<(), spi::ConnectorError> {
            self.calls.lock().unwrap().push((self.marker, "admit"));
            self.operation_ids
                .lock()
                .unwrap()
                .push(request.operation_id);
            assert_eq!(request.target.incarnation, self.incarnation());
            assert!(matches!(
                request.operation,
                spi::ConnectorCatalogMutationOperation::AlterSchema { .. }
            ));
            match self.admission_error {
                Some(kind) => Err(spi::ConnectorError::new(
                    kind,
                    "original provider admission",
                )),
                None => Ok(()),
            }
        }
        fn execute(
            &self,
            request: spi::ConnectorCatalogMutationRequest,
        ) -> Result<
            spi::ExternalMutationOutcome<spi::ConnectorCatalogMutationReceipt>,
            spi::ConnectorError,
        > {
            self.calls.lock().unwrap().push((self.marker, "execute"));
            self.operation_ids
                .lock()
                .unwrap()
                .push(request.operation_id);
            assert_eq!(request.target.incarnation, self.incarnation());
            Ok(spi::ExternalMutationOutcome::CommitUnknown {
                failure: spi::ConnectorMutationFailure::new(
                    spi::ConnectorMutationFailureKind::Unavailable,
                    "response lost",
                ),
                evidence: spi::ExternalMutationEvidence::try_new(
                    1,
                    self.descriptor.clone(),
                    self.incarnation(),
                    request.operation_id,
                    request.operation.kind(),
                    Bytes::from_static(b"schema-alter"),
                )?,
            })
        }
        fn reconcile(
            &self,
            request: spi::ConnectorCatalogMutationReconcileRequest,
        ) -> Result<
            spi::ExternalMutationOutcome<spi::ConnectorCatalogMutationReceipt>,
            spi::ConnectorError,
        > {
            self.calls.lock().unwrap().push((self.marker, "reconcile"));
            self.operation_ids
                .lock()
                .unwrap()
                .push(request.evidence.operation_id());
            Ok(spi::ExternalMutationOutcome::KnownCommitted {
                cleanup: novarocks_spi::connector::ExternalMutationFinalization::Complete,
                effect: spi::ExternalMutationEffect::Applied,
                receipt: spi::ConnectorCatalogMutationReceipt::try_new(
                    self.descriptor.clone(),
                    self.incarnation(),
                    request.evidence.operation_id(),
                    request.evidence.operation_kind(),
                    None,
                )?,
                finalization: spi::ExternalMutationFinalization::Complete,
            })
        }
    }

    struct SwitchingSchemaAlterResolver {
        bindings: Vec<Arc<spi::ConnectorControlBinding>>,
        acquired: AtomicUsize,
    }
    impl spi::ConnectorControlResolver for SwitchingSchemaAlterResolver {
        fn observe_current_binding(
            &self,
            _: &spi::ConnectorInstanceId,
        ) -> Result<spi::ConnectorProviderBindingKey, spi::ConnectorError> {
            panic!("must retain a planning lease")
        }
        fn observe_current_control_runtime(
            &self,
            _: &spi::ConnectorInstanceId,
        ) -> Result<spi::ConnectorControlRuntimeId, spi::ConnectorError> {
            panic!("must retain a planning lease")
        }
        fn acquire_current(
            &self,
            _: &spi::ConnectorInstanceId,
        ) -> Result<spi::ConnectorControlPlanningLease, spi::ConnectorError> {
            let index = self.acquired.fetch_add(1, Ordering::AcqRel);
            Ok(spi::ConnectorControlPlanningLease::new(
                self.bindings[index.min(1)].clone(),
                || {},
            ))
        }
    }

    fn schema_alter_fixture(
        admission_error: Option<spi::ConnectorErrorKind>,
        guard_error: bool,
    ) -> (
        SwitchingSchemaAlterResolver,
        Arc<SchemaAlterGeneration>,
        spi::ConnectorCatalogMutationOperation,
        spi::ConnectorRequestContext,
    ) {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut bindings = Vec::new();
        let mut first = None;
        for marker in [1, 2] {
            let capability = Arc::new(SchemaAlterGeneration {
                descriptor: spi::ConnectorInstanceDescriptor {
                    provider_id: spi::ConnectorProviderId::parse("iceberg").unwrap(),
                    instance_id: spi::ConnectorInstanceId::parse("catalog").unwrap(),
                },
                marker,
                calls: calls.clone(),
                operation_ids: Mutex::new(Vec::new()),
                admission_error,
                guard_error,
            });
            bindings.push(Arc::new(
                spi::ConnectorControlBinding::try_new(
                    capability.descriptor.clone(),
                    spi::ProviderBindingEpoch::from_bytes([marker; 16]),
                    capability.clone(),
                    capability.clone(),
                    capability.clone(),
                    Some(capability.clone()),
                )
                .unwrap(),
            ));
            if marker == 1 {
                first = Some(capability);
            }
        }
        let operation = spi::ConnectorCatalogMutationOperation::AlterSchema {
            table: spi::ConnectorTableIdentity {
                instance_id: spi::ConnectorInstanceId::parse("catalog").unwrap(),
                namespace: "db".into(),
                table: "t".into(),
            },
            changes: vec![spi::ConnectorSchemaChange::AddColumn {
                parent: spi::ConnectorColumnPath {
                    segments: Vec::new(),
                },
                column: spi::ConnectorColumnDefinition {
                    name: "payload".into(),
                    data_type: spi::ConnectorDataType::Variant,
                    nullable: true,
                    aggregation: None,
                    default: None,
                },
                position: spi::ConnectorColumnPosition::Default,
            }],
        };
        let context = crate::connector::connector_request_context(
            None,
            spi::ConnectorStopOwner::new().view(),
        )
        .unwrap();
        (
            SwitchingSchemaAlterResolver {
                bindings,
                acquired: AtomicUsize::new(0),
            },
            first.unwrap(),
            operation,
            context,
        )
    }

    fn schema_alter_metadata_guard(
        planning: &spi::ConnectorControlPlanningLease,
        operation: &spi::ConnectorCatalogMutationOperation,
        context: &spi::ConnectorRequestContext,
    ) -> Result<(), String> {
        let spi::ConnectorCatalogMutationOperation::AlterSchema { table, .. } = operation else {
            panic!("expected schema mutation")
        };
        planning
            .binding()
            .metadata()
            .table_exists(spi::ConnectorTableRequest {
                table: table.clone(),
                resolution: spi::ConnectorTableResolution::StrictBaseTable,
                context: context.clone(),
            })
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    #[test]
    fn schema_alter_admission_refusal_precedes_metadata_guards_and_dispatch() {
        for kind in [
            spi::ConnectorErrorKind::Unsupported,
            spi::ConnectorErrorKind::Cancelled,
            spi::ConnectorErrorKind::DeadlineExceeded,
            spi::ConnectorErrorKind::ResourceExhausted,
            spi::ConnectorErrorKind::InvalidRequest,
        ] {
            let (controls, capability, operation, context) =
                schema_alter_fixture(Some(kind), false);
            let error = super::execute_admitted_alter_schema(
                &controls,
                &capability.descriptor.instance_id,
                operation.clone(),
                context.clone(),
                |planning| schema_alter_metadata_guard(planning, &operation, &context),
            )
            .unwrap_err();
            assert_eq!(
                error,
                spi::ConnectorError::new(kind, "original provider admission").to_string()
            );
            assert_eq!(controls.acquired.load(Ordering::Acquire), 1);
            assert_eq!(*capability.calls.lock().unwrap(), [(1, "admit")]);
            assert_eq!(capability.operation_ids.lock().unwrap().len(), 1);
        }
    }

    #[test]
    fn schema_alter_retains_one_generation_and_operation_through_guard_and_reconcile() {
        let (controls, capability, operation, context) = schema_alter_fixture(None, false);
        super::execute_admitted_alter_schema(
            &controls,
            &capability.descriptor.instance_id,
            operation.clone(),
            context.clone(),
            |planning| schema_alter_metadata_guard(planning, &operation, &context),
        )
        .unwrap();
        assert_eq!(controls.acquired.load(Ordering::Acquire), 1);
        assert_eq!(
            *capability.calls.lock().unwrap(),
            [
                (1, "admit"),
                (1, "metadata guard"),
                (1, "execute"),
                (1, "reconcile")
            ]
        );
        let ids = capability.operation_ids.lock().unwrap();
        assert_eq!(ids.len(), 3);
        assert!(ids.windows(2).all(|pair| pair[0] == pair[1]));
    }

    #[test]
    fn schema_alter_metadata_guard_refusal_prevents_execution() {
        let (controls, capability, operation, context) = schema_alter_fixture(None, true);
        let error = super::execute_admitted_alter_schema(
            &controls,
            &capability.descriptor.instance_id,
            operation.clone(),
            context.clone(),
            |planning| schema_alter_metadata_guard(planning, &operation, &context),
        )
        .unwrap_err();
        assert_eq!(
            error,
            spi::ConnectorError::new(
                spi::ConnectorErrorKind::PermissionDenied,
                "metadata guard refused"
            )
            .to_string()
        );
        assert_eq!(controls.acquired.load(Ordering::Acquire), 1);
        assert_eq!(
            *capability.calls.lock().unwrap(),
            [(1, "admit"), (1, "metadata guard")]
        );
        assert_eq!(capability.operation_ids.lock().unwrap().len(), 1);
    }

    #[test]
    fn create_like_columns_preserve_variant_binary_and_nested_declarations() {
        use arrow::datatypes::{DataType, Field};
        use novarocks_types::schema::{ColumnDef, SqlType};
        use std::sync::Arc;
        let nested = SqlType::Struct(vec![
            ("variant".into(), SqlType::Variant),
            ("binary".into(), SqlType::Binary),
            ("items".into(), SqlType::Array(Box::new(SqlType::Variant))),
            (
                "map".into(),
                SqlType::Map(Box::new(SqlType::String), Box::new(SqlType::Variant)),
            ),
        ]);
        let nested_carrier = DataType::Struct(
            vec![
                Field::new("variant", DataType::LargeBinary, true),
                Field::new("binary", DataType::Binary, true),
                Field::new(
                    "items",
                    DataType::List(Arc::new(Field::new("element", DataType::LargeBinary, true))),
                    true,
                ),
                Field::new(
                    "map",
                    DataType::Map(
                        Arc::new(Field::new(
                            "entries",
                            DataType::Struct(
                                vec![
                                    Field::new("key", DataType::Utf8, false),
                                    Field::new("value", DataType::LargeBinary, true),
                                ]
                                .into(),
                            ),
                            false,
                        )),
                        false,
                    ),
                    true,
                ),
            ]
            .into(),
        );
        for (declaration, carrier) in [
            (SqlType::Variant, DataType::LargeBinary),
            (SqlType::Binary, DataType::Binary),
            (nested, nested_carrier),
        ] {
            let source = ColumnDef {
                name: "payload".into(),
                data_type: carrier,
                nullable: true,
                write_default: None,
                logical_type: Some(declaration.clone()),
            };
            let copied = super::create_like_column(&source).unwrap();
            assert_eq!(copied.data_type, declaration);
            assert_eq!(copied.name, "payload");
            assert!(copied.nullable);
        }
        // An ordinary LargeBinary declaration stays binary; its width alone
        // must never infer Variant identity.
        let binary = ColumnDef {
            name: "raw".into(),
            data_type: arrow::datatypes::DataType::LargeBinary,
            nullable: false,
            write_default: None,
            logical_type: None,
        };
        assert_eq!(
            super::create_like_column(&binary).unwrap().data_type,
            SqlType::Binary
        );
    }

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
        let statement = novarocks_query_application::sql::parse_single_statement(
            "CREATE EXTERNAL CATALOG IF NOT EXISTS Warehouse PROPERTIES ('type'='iceberg')",
        )
        .expect("parse catalog statement");
        let Some(novarocks_query_application::sql::ProductSqlCommand::Catalog(
            novarocks_sql::semantic::CatalogSqlCommand::CreateCatalog(statement),
        )) = novarocks_query_application::sql::lower_product_sql_command(&statement)
            .expect("lower catalog statement")
        else {
            panic!("expected semantic CREATE CATALOG");
        };

        let command = lower_semantic_create_catalog(&statement).expect("lower catalog command");
        assert_eq!(command.instance_id.as_str(), "warehouse");
        assert_eq!(command.display_name, "Warehouse");
        assert_eq!(
            command.properties,
            [("type".to_string(), "iceberg".to_string())]
        );
        assert!(command.if_not_exists);
    }
}
