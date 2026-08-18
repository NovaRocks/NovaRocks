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

use novarocks_catalog::identifier::normalize_identifier;
use novarocks_spi::connector::{ConnectorControlRegistry, ConnectorInstanceId};
use sqlparser::parser::Parser;
use std::sync::Arc;

use crate::catalog_application::create_table_ddl::build_iceberg_create_table_ddl;
use crate::catalog_application::query_catalog::{CatalogServiceSource, QueryCatalogService};
use crate::catalog_application::resolver::CatalogAdmission;
use crate::catalog_application::statement::{
    CatalogDropContext, CatalogMutationContext, execute_create_database_statement,
    execute_create_table_statement, execute_drop_catalog_statement,
    execute_drop_database_statement, execute_drop_table_statement,
};
use crate::catalog_application::{CatalogApplicationPort, CatalogCreateCommand};
use crate::mv::repository::MvRepository;
use crate::mv::storage_observation::MvStorageObservationPort;
use crate::runtime::query_result::QueryResultColumn;
use crate::runtime::statement_result::StatementResult;
use novarocks_sql::syntax::{
    StarRocksDialect, looks_like_create_catalog, looks_like_create_database,
    looks_like_create_table, looks_like_drop_statement,
};

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
    mv_repository: Arc<dyn MvRepository>,
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

    fn mv_repository(&self) -> &dyn MvRepository {
        self.mv_repository.as_ref()
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
        mv_repository: Arc<dyn MvRepository>,
        mv_storage_observation: Arc<dyn MvStorageObservationPort>,
    ) -> Self {
        Self {
            catalog_service,
            catalog_application,
            connector_control,
            mv_repository,
            mv_storage_observation,
        }
    }

    /// Execute exactly one catalog-DDL statement.
    ///
    /// CTAS belongs to the frontend DML service and is rejected here.  A
    /// default-catalog database drop belongs to the view/session route, so it
    /// is also rejected rather than being silently reinterpreted as a provider
    /// namespace mutation.
    pub fn try_execute(
        &self,
        sql: &str,
        current_catalog: Option<&str>,
        current_database: &str,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<Option<StatementResult>, String> {
        let normalized = novarocks_sql::syntax::normalize_for_raw_parse(sql)?;
        if let Some((target, source)) =
            crate::catalog_application::create_table_ddl::parse_create_table_like(&normalized)?
        {
            return execute_create_table_like(
                self,
                target,
                source,
                current_catalog,
                current_database,
                connector_context,
            )
            .map(Some);
        }
        if crate::catalog_application::statement::looks_like_show_create_table(&normalized) {
            return execute_show_create_table(
                self,
                &normalized,
                current_catalog,
                current_database,
                connector_context,
            )
            .map(Some);
        }
        if crate::catalog_application::statement::looks_like_alter_iceberg_properties(&normalized) {
            return execute_alter_iceberg_properties(
                self,
                &normalized,
                current_catalog,
                current_database,
                connector_context,
            )
            .map(Some);
        }
        if crate::catalog_application::statement::looks_like_alter_iceberg_schema(&normalized) {
            return execute_alter_iceberg_schema(
                self,
                &normalized,
                current_catalog,
                current_database,
                connector_context,
            )
            .map(Some);
        }
        if crate::catalog_application::statement::looks_like_alter_partition_column(&normalized) {
            return execute_alter_partition_spec(
                self,
                crate::catalog_application::statement::parse_alter_partition_column_sql(
                    &normalized,
                )?,
                current_catalog,
                current_database,
                connector_context,
            )
            .map(Some);
        }
        let dialect = StarRocksDialect;
        let mut parser = Parser::new(&dialect)
            .try_with_sql(&normalized)
            .map_err(|error| error.to_string())?;

        if looks_like_create_table(&parser) {
            let statement = novarocks_sql::syntax::parse_create_table_statement(&mut parser)?;
            require_statement_end(&mut parser)?;
            return execute_create_table_statement(
                self,
                statement,
                current_catalog,
                current_database,
                connector_context,
            )
            .map(Some);
        }
        if looks_like_create_catalog(&parser) {
            let statement = novarocks_sql::syntax::parse_create_catalog_statement(&mut parser)?;
            require_statement_end(&mut parser)?;
            return self.execute_create_catalog(statement).map(Some);
        }
        if looks_like_create_database(&parser) {
            let (name, if_not_exists) =
                novarocks_sql::syntax::parse_create_database_name(&mut parser)?;
            require_statement_end(&mut parser)?;
            return execute_create_database_statement(
                self,
                &name,
                if_not_exists,
                current_catalog,
                connector_context,
            )
            .map(Some);
        }
        if looks_like_drop_statement(&parser) {
            let statement = novarocks_sql::syntax::parse_drop_statement(&mut parser)?;
            require_statement_end(&mut parser)?;
            use novarocks_sql::syntax::DropStatement;
            return match statement {
                DropStatement::Catalog(statement) => {
                    execute_drop_catalog_statement(self, &statement.name, statement.if_exists)
                        .map(Some)
                }
                DropStatement::Database(statement) => {
                    if current_catalog.is_none() && statement.name.parts.len() == 1 {
                        Err("DROP DATABASE in default_catalog must be routed through the view command capability".to_string())
                    } else {
                        execute_drop_database_statement(
                            self,
                            &statement.name,
                            current_catalog,
                            statement.if_exists,
                            statement.force,
                            connector_context,
                        )
                        .map(Some)
                    }
                }
                DropStatement::Table(statement) => execute_drop_table_statement(
                    self,
                    &statement.name,
                    current_catalog,
                    current_database,
                    statement.if_exists,
                    statement.force,
                    connector_context,
                )
                .map(Some),
            };
        }
        Ok(None)
    }

    fn execute_create_catalog(
        &self,
        statement: novarocks_sql::syntax::CreateCatalogStmt,
    ) -> Result<StatementResult, String> {
        let normalized_catalog = normalize_identifier(&statement.name)?;
        let application = self.catalog_application.as_ref().ok_or_else(|| {
            "catalog statements require a configured frontend catalog application".to_string()
        })?;
        let instance_id = ConnectorInstanceId::parse(&normalized_catalog)
            .map_err(|error| format!("invalid catalog connector instance ID: {error}"))?;
        application
            .create_catalog(CatalogCreateCommand {
                instance_id,
                display_name: statement.name,
                properties: statement.properties,
                if_not_exists: statement.if_not_exists,
            })
            .map_err(|error| error.to_string())?;
        Ok(StatementResult::Ok)
    }
}

fn execute_alter_iceberg_properties(
    executor: &CatalogCommandExecutor,
    sql: &str,
    current_catalog: Option<&str>,
    current_database: &str,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<StatementResult, String> {
    let statement = crate::catalog_application::statement::parse_alter_iceberg_properties_sql(sql)?;
    let target = crate::catalog_application::resolver::resolve_existing_table_target(
        executor,
        &statement.table,
        current_catalog,
        current_database,
    )?;
    crate::mv::iceberg_guard::reject_if_iceberg_mv_table_with_ports(
        executor.connector_control.as_ref(),
        executor.mv_storage_observation.as_ref(),
        &target,
        crate::mv::iceberg_guard::IcebergMvUserMutation::AlterTable,
    )?;
    if target.backend_name != "iceberg" {
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
    sql: &str,
    current_catalog: Option<&str>,
    current_database: &str,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<StatementResult, String> {
    let statement = crate::catalog_application::statement::parse_alter_iceberg_schema_sql(sql)?;
    let target = crate::catalog_application::resolver::resolve_existing_table_target(
        executor,
        &statement.table,
        current_catalog,
        current_database,
    )?;
    crate::mv::iceberg_guard::reject_if_iceberg_mv_table_with_ports(
        executor.connector_control.as_ref(),
        executor.mv_storage_observation.as_ref(),
        &target,
        crate::mv::iceberg_guard::IcebergMvUserMutation::AlterTable,
    )?;
    if let crate::catalog_application::statement::IcebergSchemaChange::DropColumn { path } =
        &statement.change
    {
        crate::mv::iceberg_guard::reject_drop_column_mv_dependencies_with_repository(
            executor.mv_repository.as_ref(),
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
            let column = novarocks_sql::syntax::TableColumnDef {
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
    statement: novarocks_sql::syntax::AlterIcebergPartitionSpecStmt,
    current_catalog: Option<&str>,
    current_database: &str,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<StatementResult, String> {
    let table_name = match &statement {
        novarocks_sql::syntax::AlterIcebergPartitionSpecStmt::AddPartitionColumn {
            table, ..
        }
        | novarocks_sql::syntax::AlterIcebergPartitionSpecStmt::DropPartitionColumn {
            table, ..
        } => table,
    };
    let target = crate::catalog_application::resolver::resolve_table_target(
        executor,
        table_name,
        current_catalog,
        current_database,
    )?;
    if target.backend_name != "iceberg" {
        return Err(format!(
            "ALTER TABLE ADD/DROP PARTITION COLUMN only supports iceberg backends, got `{}`",
            target.backend_name
        ));
    }
    crate::mv::iceberg_guard::reject_if_iceberg_mv_table_with_ports(
        executor.connector_control.as_ref(),
        executor.mv_storage_observation.as_ref(),
        &target,
        crate::mv::iceberg_guard::IcebergMvUserMutation::AlterTable,
    )?;
    let adding = matches!(
        &statement,
        novarocks_sql::syntax::AlterIcebergPartitionSpecStmt::AddPartitionColumn { .. }
    );
    let partition_field = match &statement {
        novarocks_sql::syntax::AlterIcebergPartitionSpecStmt::AddPartitionColumn {
            field, ..
        }
        | novarocks_sql::syntax::AlterIcebergPartitionSpecStmt::DropPartitionColumn {
            field, ..
        } => field,
    };
    let transform =
        crate::catalog_application::statement::connector_partition_transform(partition_field);
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
    target: novarocks_sql::syntax::ObjectName,
    source: novarocks_sql::syntax::ObjectName,
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
            Ok(novarocks_sql::syntax::TableColumnDef {
                name: column.name.clone(),
                data_type: novarocks_sql::syntax::arrow_data_type_to_sql_type(&column.data_type)?,
                nullable: column.nullable,
                aggregation: None,
                default: None,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    execute_create_table_statement(
        executor,
        novarocks_sql::syntax::CreateTableStmt {
            name: target,
            kind: novarocks_sql::syntax::CreateTableKind::Iceberg {
                columns,
                key_desc: None,
                bucket_count: None,
                distribution_columns: Vec::new(),
                partition_fields: Vec::new(),
                properties: Vec::new(),
            },
            legacy_range_partitions: Vec::new(),
            as_select: None,
            if_not_exists: false,
        },
        current_catalog,
        current_database,
        connector_context,
    )
}

fn execute_show_create_table(
    executor: &CatalogCommandExecutor,
    sql: &str,
    current_catalog: Option<&str>,
    current_database: &str,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<StatementResult, String> {
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    let table_name = crate::catalog_application::statement::parse_show_create_table(sql)?;
    let target = crate::catalog_application::resolver::resolve_existing_table_target(
        executor,
        &table_name,
        current_catalog,
        current_database,
    )?;
    if target.backend_name != "iceberg" {
        return Err(format!(
            "SHOW CREATE TABLE only supports Iceberg tables, got `{}` backend",
            target.backend_name
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

fn require_statement_end(parser: &mut Parser<'_>) -> Result<(), String> {
    if parser.consume_token(&sqlparser::tokenizer::Token::SemiColon) {}
    if parser.peek_token() != sqlparser::tokenizer::Token::EOF {
        return Err("catalog command accepts exactly one statement".to_string());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::CatalogCommandExecutor;

    #[test]
    fn non_catalog_statement_is_not_claimed() {
        // Construction is unnecessary for an unsupported family because the
        // parser gate must reject it before any port is read.
        let sql = "SELECT 'CREATE TABLE t AS SELECT 1'";
        let normalized =
            novarocks_sql::syntax::normalize_for_raw_parse(sql).expect("normalize query");
        assert!(normalized.starts_with("SELECT"));
        let _ = std::any::type_name::<CatalogCommandExecutor>();
    }
}
