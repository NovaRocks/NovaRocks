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

//! Query-application ownership of parser-admitted SQL statement shape.

use std::{fmt, sync::Arc};

use novarocks_parser::{ParserError, ast::Statement};
use novarocks_sql::semantic::command::{
    AlterIcebergTableSqlCommand, CatalogCreateCommand, ColumnPositionSql, CommandLiteral,
    CommandProperty, CreateTableSqlCommand, ExpireSnapshotsOptionSql, IcebergColumnSqlAction,
    IcebergPartitionSqlChange, IcebergPropertiesSqlAction, IcebergReferenceKindSql,
    IcebergReferenceSqlAction, IcebergSchemaSqlChange, IcebergTableSqlAction, MaintenanceValueSql,
    ProcedureArgumentModeSql, ProcedureArgumentSql, ReferenceAnchorSql, TableColumnSqlCommand,
    TableDistributionSqlCommand, TablePartitionSqlCommand,
};
use novarocks_sql::semantic::{
    CatalogSqlCommand, ColumnAggregation, IcebergPartitionFieldExpr, MaintenanceSqlCommand,
    ObjectName, StatisticsSqlCommand, TableKeyDesc, TableKeyKind,
};
use novarocks_types::schema::SqlType;

use crate::admitted_query_context::RequestContext;
use crate::api::{
    CatalogCommandConsumer, CommandContext, CommandFuture, MaintenanceCommandConsumer,
    OptionalCommandFuture, StatisticsCommandConsumer,
};
use crate::session_error::{QueryServiceError, QueryServiceErrorKind};

/// SQL batch admission and test-only stable error injection.
pub mod admission;

/// Connection-local execution settings after SQL/session validation and before
/// a role adapter projects them into a particular wire contract.
pub mod session;

/// Typed session-admission errors and their stable user-error descriptors.
pub mod session_admit;

/// Typed DML statement-shape admission errors and their stable user-error descriptors.
pub mod dml_admission;

/// Read-only Catalog facts consumed by session SQL admission.
pub mod catalog;

/// Role-local route for the statement families intentionally outside the
/// closed product-command vocabulary.
///
/// Product statements use [`ProductCommandRouter`] after semantic lowering.
/// This route admits only the explicit MV, View, and
/// test-only maintenance-call shapes; it is not a generic parser-AST route.
pub trait SpecializedStatementRoute: Send + Sync {
    /// Executes the test-only stateless-rebuild procedure when it owns this
    /// exact CALL. `None` leaves the already-lowered maintenance command to
    /// its normal product consumer.
    fn execute_maintenance_call(
        &self,
        _statement: &novarocks_parser::ast::CallStatement,
        _context: &RequestContext,
        _command_context: &CommandContext,
    ) -> OptionalCommandFuture {
        Box::pin(async { Ok(None) })
    }

    /// Build an MV action under the already admitted statement.
    fn prepare_materialized_view_statement_action(
        &self,
        _statement: novarocks_parser::ast::MaterializedViewStatement,
        _context: RequestContext,
        _command_context: CommandContext,
    ) -> Result<Box<dyn crate::session_control::GovernedStatementAction>, crate::api::CommandError>
    {
        Err(crate::api::CommandError::new(
            crate::api::CommandErrorKind::Unsupported,
            "materialized view action route is unavailable",
        ))
    }

    /// Executes the query-owned View family, which is outside the product
    /// command vocabulary.
    fn execute_view(
        &self,
        _statement: &novarocks_parser::ast::ViewStatement,
        _context: &RequestContext,
        _command_context: &CommandContext,
    ) -> CommandFuture {
        Box::pin(async {
            Err(crate::api::CommandError::new(
                crate::api::CommandErrorKind::Unsupported,
                "view command route is unavailable",
            ))
        })
    }
}

/// Query Application's only dispatcher for closed product command values.
///
/// Each consumer is injected by role composition and receives the complete
/// immutable request and governed command contexts. Parser ASTs cannot cross
/// this boundary.
#[derive(Clone)]
pub struct ProductCommandRouter {
    catalog: Arc<dyn CatalogCommandConsumer>,
    statistics: Arc<dyn StatisticsCommandConsumer>,
    maintenance: Arc<dyn MaintenanceCommandConsumer>,
}

impl ProductCommandRouter {
    pub fn new(
        catalog: Arc<dyn CatalogCommandConsumer>,
        statistics: Arc<dyn StatisticsCommandConsumer>,
        maintenance: Arc<dyn MaintenanceCommandConsumer>,
    ) -> Self {
        Self {
            catalog,
            statistics,
            maintenance,
        }
    }

    /// The closed product dispatcher selects the statistics author under the
    /// admitted caller's original statement source.
    pub fn prepare_statistics_statement_action(
        &self,
        command: StatisticsSqlCommand,
        request_context: RequestContext,
        command_context: CommandContext,
    ) -> Result<Box<dyn crate::session_control::GovernedStatementAction>, crate::api::CommandError>
    {
        self.statistics
            .prepare_statement_action(command, request_context, command_context)
    }

    pub fn prepare_catalog_statement_action(
        &self,
        command: CatalogSqlCommand,
        request_context: RequestContext,
        command_context: CommandContext,
    ) -> Result<Box<dyn crate::session_control::GovernedStatementAction>, crate::api::CommandError>
    {
        self.catalog
            .prepare_statement_action(command, request_context, command_context)
    }

    pub fn prepare_maintenance_statement_action(
        &self,
        command: MaintenanceSqlCommand,
        request_context: RequestContext,
        command_context: CommandContext,
    ) -> Result<Box<dyn crate::session_control::GovernedStatementAction>, crate::api::CommandError>
    {
        self.maintenance
            .prepare_statement_action(command, request_context, command_context)
    }

    pub fn execute(
        &self,
        command: ProductSqlCommand,
        request_context: RequestContext,
        command_context: CommandContext,
    ) -> CommandFuture {
        match command {
            ProductSqlCommand::Catalog(command) => {
                self.catalog
                    .execute(*command, request_context, command_context)
            }
            ProductSqlCommand::Statistics(command) => {
                self.statistics
                    .execute(command, request_context, command_context)
            }
            ProductSqlCommand::Maintenance(command) => {
                self.maintenance
                    .execute(command, request_context, command_context)
            }
        }
    }
}

/// A fully lowered product command selected by SQL admission.
///
/// The variants intentionally exclude query, session, DML, view, backend and
/// materialized-view statements: those have separate ownership seams.  In
/// particular, this is not a generic wrapper around parser `Statement`.
#[derive(Clone, Debug, PartialEq)]
pub enum ProductSqlCommand {
    Catalog(Box<CatalogSqlCommand>),
    Statistics(StatisticsSqlCommand),
    Maintenance(MaintenanceSqlCommand),
}

/// Lowers one parser-admitted command family to its closed semantic value.
///
/// The `None` result means that the statement belongs to another explicit
/// owner, not that a caller may pass its parser AST through a generic route.
pub fn lower_product_sql_command(
    statement: &Statement,
) -> Result<Option<ProductSqlCommand>, String> {
    use novarocks_parser::ast::Statement as ParsedStatement;

    match statement {
        ParsedStatement::Catalog(statement) => Ok(Some(ProductSqlCommand::Catalog(Box::new(
            lower_catalog_command(statement)?,
        )))),
        ParsedStatement::Statistics(statement) => Ok(Some(ProductSqlCommand::Statistics(
            lower_statistics_command(statement)?,
        ))),
        ParsedStatement::Maintenance(statement) => Ok(Some(ProductSqlCommand::Maintenance(
            lower_maintenance_command(statement)?,
        ))),
        ParsedStatement::Table(statement) => Ok(Some(ProductSqlCommand::Catalog(Box::new(
            lower_table_command(statement)?,
        )))),
        ParsedStatement::Iceberg(statement) => Ok(Some(ProductSqlCommand::Catalog(Box::new(
            lower_iceberg_command(statement)?,
        )))),
        ParsedStatement::Dml(_)
        | ParsedStatement::MaterializedView(_)
        | ParsedStatement::View(_)
        | ParsedStatement::ShowProcessList(_)
        | ParsedStatement::Session(_)
        | ParsedStatement::Query(_)
        | ParsedStatement::ExplainQuery(_) => Ok(None),
    }
}

fn lower_catalog_command(
    statement: &novarocks_parser::ast::CatalogStatement,
) -> Result<CatalogSqlCommand, String> {
    use novarocks_parser::ast::CatalogStatement;

    Ok(match statement {
        CatalogStatement::TruncateTable(statement) => CatalogSqlCommand::TruncateTable {
            table: lower_object_name(&statement.name),
            target_ref: statement.target_ref.clone(),
        },
        CatalogStatement::CreateCatalog(statement) => {
            CatalogSqlCommand::CreateCatalog(CatalogCreateCommand {
                external: statement.external,
                if_not_exists: statement.if_not_exists,
                name: statement.name.value.clone(),
                comment: statement
                    .comment
                    .as_ref()
                    .map(lower_command_literal)
                    .transpose()?,
                properties: statement
                    .properties
                    .iter()
                    .map(|property| {
                        Ok(CommandProperty {
                            key: lower_command_literal(&property.key)?,
                            value: lower_command_literal(&property.value)?,
                        })
                    })
                    .collect::<Result<_, String>>()?,
            })
        }
        CatalogStatement::DropCatalog(statement) => CatalogSqlCommand::DropCatalog {
            name: statement.name.value.clone(),
            if_exists: statement.if_exists,
        },
        CatalogStatement::CreateDatabase(statement) => CatalogSqlCommand::CreateDatabase {
            name: lower_object_name(&statement.name),
            if_not_exists: statement.if_not_exists,
        },
        CatalogStatement::DropDatabase(statement) => CatalogSqlCommand::DropDatabase {
            name: lower_object_name(&statement.name),
            if_exists: statement.if_exists,
            force: statement.force,
        },
        CatalogStatement::DropTable(statement) => CatalogSqlCommand::DropTable {
            name: lower_object_name(&statement.name),
            if_exists: statement.if_exists,
            force: statement.force,
        },
        CatalogStatement::ShowCreateTable(statement) => CatalogSqlCommand::ShowCreateTable {
            name: lower_object_name(&statement.name),
        },
    })
}

fn lower_table_command(
    statement: &novarocks_parser::ast::TableStatement,
) -> Result<CatalogSqlCommand, String> {
    use novarocks_parser::ast::{TablePartition, TableStatement};

    let TableStatement::Create(statement) = statement;
    let partition = match &statement.partition {
        None => None,
        Some(TablePartition::Transform(partition)) => Some(TablePartitionSqlCommand::Transform(
            partition
                .expressions
                .iter()
                .map(lower_table_partition_transform)
                .collect::<Result<_, _>>()?,
        )),
        Some(TablePartition::LegacyRange(partition)) => {
            Some(TablePartitionSqlCommand::UnsupportedLegacyRange {
                columns: partition
                    .columns
                    .iter()
                    .map(|column| column.value.clone())
                    .collect(),
                definition_count: partition.definitions.len(),
            })
        }
    };
    Ok(CatalogSqlCommand::CreateTable(CreateTableSqlCommand {
        temporary: statement.temporary,
        external: statement.external,
        if_not_exists: statement.if_not_exists,
        name: lower_object_name(&statement.name),
        engine: statement.engine.as_ref().map(|engine| engine.value.clone()),
        like: statement.like.as_ref().map(lower_object_name),
        columns: statement
            .columns
            .iter()
            .map(lower_table_column)
            .collect::<Result<_, _>>()?,
        key: statement.key.as_ref().map(lower_table_key).transpose()?,
        distribution: statement.distribution.as_ref().map(|distribution| {
            TableDistributionSqlCommand {
                columns: distribution
                    .columns
                    .iter()
                    .map(|column| column.value.clone())
                    .collect(),
                random: distribution.random,
                buckets: distribution.buckets,
            }
        }),
        partition,
        order_by: statement
            .order_by
            .iter()
            .map(|column| column.value.clone())
            .collect(),
        properties: statement
            .properties
            .iter()
            .map(|property| {
                Ok(CommandProperty {
                    key: lower_command_literal(&property.key)?,
                    value: lower_command_literal(&property.value)?,
                })
            })
            .collect::<Result<_, String>>()?,
        comment: statement
            .comment
            .as_ref()
            .map(lower_command_literal)
            .transpose()?,
    }))
}

fn lower_table_column(
    column: &novarocks_parser::ast::ColumnDefinition,
) -> Result<TableColumnSqlCommand, String> {
    Ok(TableColumnSqlCommand {
        name: column.name.value.clone(),
        data_type: lower_sql_type(&column.data_type)?,
        nullable: column.nullable,
        aggregation: column
            .aggregation
            .as_ref()
            .map(|value| match value.value.to_ascii_lowercase().as_str() {
                "sum" => Ok(ColumnAggregation::Sum),
                "min" => Ok(ColumnAggregation::Min),
                "max" => Ok(ColumnAggregation::Max),
                "replace" => Ok(ColumnAggregation::Replace),
                "replace_if_not_null" => Ok(ColumnAggregation::ReplaceIfNotNull),
                "bitmap_union" => Ok(ColumnAggregation::BitmapUnion),
                "hll_union" => Ok(ColumnAggregation::HllUnion),
                other => Err(format!("unsupported column aggregation `{other}`")),
            })
            .transpose()?,
        default: column
            .default
            .as_ref()
            .map(lower_command_literal)
            .transpose()?,
        comment: column
            .comment
            .as_ref()
            .map(lower_command_literal)
            .transpose()?,
    })
}

fn lower_table_key(key: &novarocks_parser::ast::TableKey) -> Result<TableKeyDesc, String> {
    use novarocks_parser::ast::TableKeyKind as ParsedKind;

    Ok(TableKeyDesc {
        kind: match key.kind {
            ParsedKind::Duplicate => TableKeyKind::Duplicate,
            ParsedKind::Unique => TableKeyKind::Unique,
            ParsedKind::Aggregate => TableKeyKind::Aggregate,
            ParsedKind::Primary => TableKeyKind::Primary,
        },
        columns: key
            .columns
            .iter()
            .map(|column| column.value.clone())
            .collect(),
    })
}

fn lower_table_partition_transform(
    transform: &novarocks_parser::ast::PartitionTransform,
) -> Result<IcebergPartitionFieldExpr, String> {
    use novarocks_parser::ast::PartitionTransform;

    let column = |value: &novarocks_parser::ast::Ident| value.value.clone();
    Ok(match transform {
        PartitionTransform::Identity { column: value, .. } => IcebergPartitionFieldExpr::Identity {
            column: column(value),
        },
        PartitionTransform::Year { column: value, .. } => IcebergPartitionFieldExpr::Year {
            column: column(value),
        },
        PartitionTransform::Month { column: value, .. } => IcebergPartitionFieldExpr::Month {
            column: column(value),
        },
        PartitionTransform::Day { column: value, .. } => IcebergPartitionFieldExpr::Day {
            column: column(value),
        },
        PartitionTransform::Hour { column: value, .. } => IcebergPartitionFieldExpr::Hour {
            column: column(value),
        },
        PartitionTransform::Void { column: value, .. } => IcebergPartitionFieldExpr::Void {
            column: column(value),
        },
        PartitionTransform::Bucket {
            buckets,
            column: value,
            ..
        } => IcebergPartitionFieldExpr::Bucket {
            column: column(value),
            num_buckets: u32::try_from(*buckets)
                .map_err(|_| "partition bucket count exceeds u32".to_string())?,
        },
        PartitionTransform::Truncate {
            width,
            column: value,
            ..
        } => IcebergPartitionFieldExpr::Truncate {
            column: column(value),
            width: u32::try_from(*width)
                .map_err(|_| "partition truncate width exceeds u32".to_string())?,
        },
    })
}

fn lower_iceberg_command(
    statement: &novarocks_parser::ast::IcebergStatement,
) -> Result<CatalogSqlCommand, String> {
    let novarocks_parser::ast::IcebergStatement::AlterTable(statement) = statement;
    Ok(CatalogSqlCommand::AlterIcebergTable(
        AlterIcebergTableSqlCommand {
            table: lower_object_name(&statement.table),
            action: lower_iceberg_action(&statement.action)?,
        },
    ))
}

fn lower_iceberg_action(
    action: &novarocks_parser::ast::IcebergTableAction,
) -> Result<IcebergTableSqlAction, String> {
    use novarocks_parser::ast::{
        IcebergColumnAction, IcebergPropertiesAction, IcebergReferenceAction, IcebergReferenceKind,
        IcebergSchemaChange, IcebergTableAction, ReferenceAnchor,
    };

    Ok(match action {
        IcebergTableAction::Schema(change) => IcebergTableSqlAction::Schema(match change {
            IcebergSchemaChange::AddColumn {
                path,
                data_type,
                nullable,
                default,
                position,
            } => IcebergSchemaSqlChange::AddColumn {
                path: lower_column_path(path),
                data_type: lower_sql_type(data_type)?,
                nullable: *nullable,
                default: default.as_ref().map(lower_command_literal).transpose()?,
                position: lower_column_position(position),
            },
            IcebergSchemaChange::DropColumn { path } => IcebergSchemaSqlChange::DropColumn {
                path: lower_column_path(path),
            },
            IcebergSchemaChange::RenameColumn { from, to } => {
                IcebergSchemaSqlChange::RenameColumn {
                    from: lower_column_path(from),
                    to: lower_column_path(to),
                }
            }
            IcebergSchemaChange::ModifyColumn { path, data_type } => {
                IcebergSchemaSqlChange::ModifyColumn {
                    path: lower_column_path(path),
                    data_type: lower_sql_type(data_type)?,
                }
            }
            IcebergSchemaChange::AlterColumn { path, action } => {
                IcebergSchemaSqlChange::AlterColumn {
                    path: lower_column_path(path),
                    action: match action {
                        IcebergColumnAction::Reorder(position) => {
                            IcebergColumnSqlAction::Reorder(lower_column_position(position))
                        }
                        IcebergColumnAction::SetNullable(nullable) => {
                            IcebergColumnSqlAction::SetNullable(*nullable)
                        }
                        IcebergColumnAction::Comment(value) => {
                            IcebergColumnSqlAction::Comment(lower_command_literal(value)?)
                        }
                    },
                }
            }
        }),
        IcebergTableAction::Properties(action) => IcebergTableSqlAction::Properties(match action {
            IcebergPropertiesAction::Set { entries } => IcebergPropertiesSqlAction::Set {
                entries: entries
                    .iter()
                    .map(|entry| {
                        Ok((
                            entry.key.value.clone(),
                            lower_command_literal(&entry.value)?,
                        ))
                    })
                    .collect::<Result<_, String>>()?,
            },
            IcebergPropertiesAction::Unset { keys, if_exists } => {
                IcebergPropertiesSqlAction::Unset {
                    keys: keys.iter().map(|key| key.key.value.clone()).collect(),
                    if_exists: *if_exists,
                }
            }
            IcebergPropertiesAction::Comment { value } => IcebergPropertiesSqlAction::Comment {
                value: lower_command_literal(value)?,
            },
        }),
        IcebergTableAction::Partition(change) => IcebergTableSqlAction::Partition(match change {
            novarocks_parser::ast::IcebergPartitionChange::Add { field } => {
                IcebergPartitionSqlChange::Add(lower_iceberg_partition_field(field)?)
            }
            novarocks_parser::ast::IcebergPartitionChange::Drop { field } => {
                IcebergPartitionSqlChange::Drop(lower_iceberg_partition_field(field)?)
            }
        }),
        IcebergTableAction::Reference(action) => IcebergTableSqlAction::Reference(match action {
            IcebergReferenceAction::Create {
                kind,
                name,
                if_not_exists,
                or_replace,
                anchor,
                options,
            } => {
                if options.is_some() {
                    return Err("Iceberg reference retention options are not supported".to_string());
                }
                IcebergReferenceSqlAction::Create {
                    kind: match kind {
                        IcebergReferenceKind::Branch => IcebergReferenceKindSql::Branch,
                        IcebergReferenceKind::Tag => IcebergReferenceKindSql::Tag,
                    },
                    name: name.value.clone(),
                    if_not_exists: *if_not_exists,
                    or_replace: *or_replace,
                    anchor: match anchor {
                        ReferenceAnchor::CurrentMain => ReferenceAnchorSql::CurrentMain,
                        ReferenceAnchor::Version(value) => {
                            ReferenceAnchorSql::Version(lower_command_literal(value)?)
                        }
                    },
                    has_uninterpreted_provider_options: false,
                }
            }
            IcebergReferenceAction::Drop {
                kind,
                name,
                if_exists,
            } => IcebergReferenceSqlAction::Drop {
                kind: match kind {
                    IcebergReferenceKind::Branch => IcebergReferenceKindSql::Branch,
                    IcebergReferenceKind::Tag => IcebergReferenceKindSql::Tag,
                },
                name: name.value.clone(),
                if_exists: *if_exists,
            },
        }),
        IcebergTableAction::AddFiles(command) => IcebergTableSqlAction::AddFiles {
            location: lower_command_literal(&command.location)?,
        },
    })
}

fn lower_column_path(path: &novarocks_parser::ast::ColumnPath) -> Vec<String> {
    path.parts.iter().map(|part| part.value.clone()).collect()
}

fn lower_column_position(position: &novarocks_parser::ast::ColumnPosition) -> ColumnPositionSql {
    use novarocks_parser::ast::ColumnPosition;

    match position {
        ColumnPosition::Default => ColumnPositionSql::Default,
        ColumnPosition::First => ColumnPositionSql::First,
        ColumnPosition::After(path) => ColumnPositionSql::After(lower_column_path(path)),
        ColumnPosition::Before(path) => ColumnPositionSql::Before(lower_column_path(path)),
    }
}

fn lower_iceberg_partition_field(
    field: &novarocks_parser::ast::IcebergPartitionField,
) -> Result<IcebergPartitionFieldExpr, String> {
    use novarocks_parser::ast::{IcebergPartitionField, LiteralKind};

    let column = |path: &novarocks_parser::ast::ColumnPath| -> Result<String, String> {
        let parts = lower_column_path(path);
        if parts.len() != 1 {
            return Err("Iceberg partition transform requires one column identifier".to_string());
        }
        Ok(parts.into_iter().next().expect("checked one path segment"))
    };
    let positive_u32 = |literal: &novarocks_parser::ast::Literal, label: &str| {
        let LiteralKind::Long {
            parsed_value: value,
            ..
        } = &literal.kind
        else {
            return Err(format!("Iceberg {label} requires a numeric literal"));
        };
        u32::try_from(*value).map_err(|_| format!("Iceberg {label} must fit u32"))
    };
    Ok(match field {
        IcebergPartitionField::Identity { column: value, .. } => {
            IcebergPartitionFieldExpr::Identity {
                column: column(value)?,
            }
        }
        IcebergPartitionField::Year { column: value, .. } => IcebergPartitionFieldExpr::Year {
            column: column(value)?,
        },
        IcebergPartitionField::Month { column: value, .. } => IcebergPartitionFieldExpr::Month {
            column: column(value)?,
        },
        IcebergPartitionField::Day { column: value, .. } => IcebergPartitionFieldExpr::Day {
            column: column(value)?,
        },
        IcebergPartitionField::Hour { column: value, .. } => IcebergPartitionFieldExpr::Hour {
            column: column(value)?,
        },
        IcebergPartitionField::Void { column: value, .. } => IcebergPartitionFieldExpr::Void {
            column: column(value)?,
        },
        IcebergPartitionField::Bucket {
            column: value,
            buckets,
            ..
        } => IcebergPartitionFieldExpr::Bucket {
            column: column(value)?,
            num_buckets: positive_u32(buckets, "partition bucket count")?,
        },
        IcebergPartitionField::Truncate {
            column: value,
            width,
            ..
        } => IcebergPartitionFieldExpr::Truncate {
            column: column(value)?,
            width: positive_u32(width, "partition truncate width")?,
        },
    })
}

fn lower_sql_type(type_name: &novarocks_parser::ast::TypeName) -> Result<SqlType, String> {
    use novarocks_parser::ast::{DataTypeParameter, DateTimeType, TypeKind};

    let generic = match &type_name.kind {
        TypeKind::Generic(generic) => generic,
        TypeKind::Row(row) => {
            return Ok(SqlType::Struct(
                row.fields
                    .iter()
                    .map(|field| {
                        let name = field
                            .name
                            .as_ref()
                            .ok_or("ROW schema type requires named fields")?;
                        Ok((name.value.clone(), lower_sql_type(&field.data_type)?))
                    })
                    .collect::<Result<_, String>>()?,
            ));
        }
        TypeKind::DateTime(value) => {
            let supported_precision = value.precision.as_ref().is_none_or(|parameter| {
                matches!(parameter.as_ref(), DataTypeParameter::Numeric(number) if number.parsed_value == 6)
            });
            if value.with_time_zone || !supported_precision {
                return Err("Iceberg schema supports only TIME/TIMESTAMP without time zone at microsecond precision".into());
            }
            return Ok(match value.kind {
                DateTimeType::Time => SqlType::Time,
                DateTimeType::Timestamp => SqlType::DateTime,
            });
        }
        TypeKind::Interval(_) => return Err("INTERVAL schema types are not supported".into()),
        TypeKind::ExplicitArray(_) => {
            return Err("explicit ARRAY schema lengths are not supported".into());
        }
    };
    let name = generic.name.value.to_ascii_lowercase();
    match name.as_str() {
        "array" => {
            let [DataTypeParameter::Type(element)] = generic.arguments.as_slice() else {
                return Err("ARRAY type requires one element type".to_string());
            };
            Ok(SqlType::Array(Box::new(lower_sql_type(
                &element.data_type,
            )?)))
        }
        "map" => {
            let [DataTypeParameter::Type(key), DataTypeParameter::Type(value)] =
                generic.arguments.as_slice()
            else {
                return Err("MAP type requires key and value types".to_string());
            };
            Ok(SqlType::Map(
                Box::new(lower_sql_type(&key.data_type)?),
                Box::new(lower_sql_type(&value.data_type)?),
            ))
        }
        "decimal" | "dec" | "numeric" | "decimal32" | "decimal64" | "decimal128" => {
            let values = generic
                .arguments
                .iter()
                .map(|argument| match argument {
                    DataTypeParameter::Numeric(value) => Ok(value.parsed_value),
                    _ => Err("DECIMAL precision and scale must be numeric literals".to_string()),
                })
                .collect::<Result<Vec<_>, _>>()?;
            if values.len() > 2 {
                return Err("DECIMAL type accepts at most precision and scale".to_string());
            }
            let precision = values
                .first()
                .map(|value| {
                    u8::try_from(*value).map_err(|_| "DECIMAL precision must fit u8".to_string())
                })
                .transpose()?
                .unwrap_or(38);
            let scale = values
                .get(1)
                .map(|value| {
                    i8::try_from(*value).map_err(|_| "DECIMAL scale must fit i8".to_string())
                })
                .transpose()?
                .unwrap_or(0);
            Ok(SqlType::Decimal { precision, scale })
        }
        "tinyint" | "int8" => Ok(SqlType::TinyInt),
        "smallint" | "int16" => Ok(SqlType::SmallInt),
        "int" | "integer" | "int32" => Ok(SqlType::Int),
        "bigint" | "int64" => Ok(SqlType::BigInt),
        "largeint" | "int128" => Ok(SqlType::LargeInt),
        "float" | "float32" => Ok(SqlType::Float),
        "double" | "float64" | "double precision" => Ok(SqlType::Double),
        "boolean" | "bool" => Ok(SqlType::Boolean),
        "string" | "varchar" | "char" | "character" | "text" => Ok(SqlType::String),
        "date" => Ok(SqlType::Date),
        "datetime" | "timestamp" => Ok(SqlType::DateTime),
        "timestamp_ns" | "timestamptz_ns" | "datetime_ns" => Ok(SqlType::DateTimeNs),
        "time" => Ok(SqlType::Time),
        "binary" | "varbinary" => Ok(SqlType::Binary),
        "json" | "jsonb" => Ok(SqlType::Json),
        "bitmap" => Ok(SqlType::Bitmap),
        "hll" => Ok(SqlType::Hll),
        "variant" => Ok(SqlType::Variant),
        _ => Err(format!("unsupported Iceberg schema type `{name}`")),
    }
}

fn lower_statistics_command(
    statement: &novarocks_parser::ast::StatisticsStatement,
) -> Result<StatisticsSqlCommand, String> {
    use novarocks_parser::ast::StatisticsStatement;
    Ok(match statement {
        StatisticsStatement::AnalyzeTable(statement) => StatisticsSqlCommand::AnalyzeTable {
            table: lower_object_name(&statement.name),
            columns: statement
                .columns
                .iter()
                .map(|column| column.value.clone())
                .collect(),
        },
        StatisticsStatement::ShowStats(statement) => StatisticsSqlCommand::ShowStats {
            table: lower_object_name(&statement.name),
        },
    })
}

fn lower_maintenance_command(
    statement: &novarocks_parser::ast::MaintenanceStatement,
) -> Result<MaintenanceSqlCommand, String> {
    use novarocks_parser::ast::{
        ExpireSnapshotsOption, MaintenanceStatement, ProcedureArgumentMode,
    };

    Ok(match statement {
        MaintenanceStatement::Call(statement) => MaintenanceSqlCommand::Call {
            procedure: lower_object_name(&statement.procedure),
            arguments: statement
                .arguments
                .iter()
                .map(|argument| {
                    Ok(ProcedureArgumentSql {
                        name: argument.name.as_ref().map(|name| name.value.clone()),
                        value: lower_maintenance_value(&argument.value)?,
                    })
                })
                .collect::<Result<_, String>>()?,
            argument_mode: match statement.argument_mode {
                ProcedureArgumentMode::Empty => ProcedureArgumentModeSql::Empty,
                ProcedureArgumentMode::Positional => ProcedureArgumentModeSql::Positional,
                ProcedureArgumentMode::Named => ProcedureArgumentModeSql::Named,
            },
        },
        MaintenanceStatement::Optimize(statement) => MaintenanceSqlCommand::Optimize {
            table: lower_object_name(&statement.table),
        },
        MaintenanceStatement::RewriteManifests(statement) => {
            MaintenanceSqlCommand::RewriteManifests {
                table: lower_object_name(&statement.table),
            }
        }
        MaintenanceStatement::ExpireSnapshots(statement) => {
            MaintenanceSqlCommand::ExpireSnapshots {
                table: lower_object_name(&statement.table),
                options: statement
                    .options
                    .iter()
                    .map(|option| match option {
                        ExpireSnapshotsOption::OlderThan { value, .. } => Ok(
                            ExpireSnapshotsOptionSql::OlderThan(lower_maintenance_value(value)?),
                        ),
                        ExpireSnapshotsOption::RetainLast { value, .. } => Ok(
                            ExpireSnapshotsOptionSql::RetainLast(lower_maintenance_value(value)?),
                        ),
                    })
                    .collect::<Result<_, String>>()?,
            }
        }
        MaintenanceStatement::RemoveOrphanFiles(statement) => {
            MaintenanceSqlCommand::RemoveOrphanFiles {
                table: lower_object_name(&statement.table),
                older_than: lower_maintenance_value(&statement.older_than)?,
            }
        }
    })
}

fn lower_maintenance_value(
    value: &novarocks_parser::ast::MaintenanceValue,
) -> Result<MaintenanceValueSql, String> {
    use novarocks_parser::ast::MaintenanceValue;

    Ok(match value {
        MaintenanceValue::Literal(value) => {
            MaintenanceValueSql::Literal(lower_command_literal(value)?)
        }
        MaintenanceValue::Timestamp { value, .. } => {
            MaintenanceValueSql::Timestamp(lower_command_literal(value)?)
        }
        MaintenanceValue::Map(map) => MaintenanceValueSql::Map(
            map.entries
                .iter()
                .map(|entry| {
                    Ok((
                        lower_command_literal(&entry.key)?,
                        lower_command_literal(&entry.value)?,
                    ))
                })
                .collect::<Result<_, String>>()?,
        ),
    })
}

fn lower_command_literal(
    literal: &novarocks_parser::ast::Literal,
) -> Result<CommandLiteral, String> {
    use novarocks_parser::ast::LiteralKind;

    match &literal.kind {
        LiteralKind::Null => Ok(CommandLiteral::Null),
        LiteralKind::Boolean(value) => Ok(CommandLiteral::Bool(*value)),
        LiteralKind::Long { parsed_value, .. } => {
            Ok(CommandLiteral::Number(parsed_value.to_string()))
        }
        LiteralKind::Double(value) => Ok(CommandLiteral::Number(format!("{:?}", value.get()))),
        LiteralKind::Decimal(value) => Ok(CommandLiteral::Number(value.clone())),
        LiteralKind::String(value) => Ok(CommandLiteral::String(value.clone())),
        LiteralKind::Binary(value) => Ok(CommandLiteral::Binary(value.clone())),
        LiteralKind::Generic(_) | LiteralKind::Interval(_) => {
            Err("typed literals are not supported in command arguments".into())
        }
    }
}

fn lower_object_name(name: &novarocks_parser::ast::ObjectName) -> ObjectName {
    ObjectName {
        parts: name.parts.iter().map(|part| part.value.clone()).collect(),
    }
}

/// The application boundary accepts one framed SQL statement.
///
/// Query Application owns SQL batch framing and parser admission. Protocol
/// adapters negotiate multi-result capability and ask the application to
/// execute each admitted fragment in order; this function rejects a fragment
/// that contains more than one statement.
pub fn parse_single_statement(source: &str) -> Result<Statement, SqlStatementParseError> {
    parse_optional_single_statement(source)?
        .ok_or(SqlStatementParseError::ExpectedExactlyOne { actual: 0 })
}

/// Parses one protocol-framed SQL fragment, preserving a comment-only fragment
/// as the absence of a statement.
pub fn parse_optional_single_statement(
    source: &str,
) -> Result<Option<Statement>, SqlStatementParseError> {
    let statements = novarocks_parser::parse(source).map_err(SqlStatementParseError::Parser)?;
    match statements.as_slice() {
        [] => Ok(None),
        [statement] => Ok(Some(statement.clone())),
        _ => Err(SqlStatementParseError::ExpectedExactlyOne {
            actual: statements.len(),
        }),
    }
}

/// Query-application parser-admission failure.
#[derive(Debug)]
pub enum SqlStatementParseError {
    Parser(ParserError),
    ExpectedExactlyOne { actual: usize },
}

impl fmt::Display for SqlStatementParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Parser(error) => error.fmt(formatter),
            Self::ExpectedExactlyOne { actual } => {
                write!(formatter, "expected exactly one statement, found {actual}")
            }
        }
    }
}

impl std::error::Error for SqlStatementParseError {}

/// Projects parser admission failures into the protocol-neutral query-service
/// error vocabulary before any product route or protocol adapter is selected.
pub fn query_service_parse_error(error: SqlStatementParseError, source: &str) -> QueryServiceError {
    match error {
        SqlStatementParseError::Parser(error) => {
            QueryServiceError::from_user_error(error.to_user_error(source))
        }
        SqlStatementParseError::ExpectedExactlyOne { .. } => QueryServiceError::new(
            QueryServiceErrorKind::Parse,
            "command admission requires exactly one statement",
        ),
    }
}

/// Cursor over semicolon-delimited SQL protocol fragments.
///
/// This is protocol-neutral framing: it preserves quote and comment state but
/// does not decide whether a fragment is an executable product statement.
#[derive(Clone)]
pub struct SqlBatchCursor<'a> {
    sql: &'a str,
    offset: usize,
}

impl<'a> SqlBatchCursor<'a> {
    pub fn new(sql: &'a str) -> Self {
        Self { sql, offset: 0 }
    }

    pub fn has_remaining(&self) -> bool {
        self.offset < self.sql.len()
    }

    /// Returns one raw semicolon-delimited fragment.
    pub fn next_fragment(&mut self) -> Result<Option<&'a str>, QueryServiceError> {
        if !self.has_remaining() {
            return Ok(None);
        }
        #[derive(Clone, Copy)]
        enum State {
            Normal,
            SingleQuote,
            DoubleQuote,
            Backtick,
            LineComment,
            BlockComment,
        }

        let start = self.offset;
        let bytes = self.sql.as_bytes();
        let mut index = start;
        let mut state = State::Normal;
        while index < bytes.len() {
            match state {
                State::Normal => match bytes[index] {
                    b'\'' => state = State::SingleQuote,
                    b'"' => state = State::DoubleQuote,
                    b'`' => state = State::Backtick,
                    b'-' if bytes.get(index + 1) == Some(&b'-') => {
                        state = State::LineComment;
                        index += 1;
                    }
                    b'#' => state = State::LineComment,
                    b'/' if bytes.get(index + 1) == Some(&b'*') => {
                        state = State::BlockComment;
                        index += 1;
                    }
                    b';' => {
                        self.offset = index + 1;
                        return Ok(Some(&self.sql[start..index]));
                    }
                    _ => {}
                },
                State::SingleQuote if bytes[index] == b'\'' => state = State::Normal,
                State::DoubleQuote if bytes[index] == b'"' => state = State::Normal,
                State::Backtick if bytes[index] == b'`' => state = State::Normal,
                State::LineComment if bytes[index] == b'\n' => state = State::Normal,
                State::BlockComment
                    if bytes[index] == b'*' && bytes.get(index + 1) == Some(&b'/') =>
                {
                    state = State::Normal;
                    index += 1;
                }
                _ => {}
            }
            index += 1;
        }
        if matches!(
            state,
            State::SingleQuote | State::DoubleQuote | State::Backtick
        ) {
            self.offset = self.sql.len();
            return Err(QueryServiceError::new(
                QueryServiceErrorKind::Parse,
                "unterminated quoted string in SQL batch",
            ));
        }
        self.offset = self.sql.len();
        Ok(Some(&self.sql[start..]))
    }
}

/// Splits a protocol SQL batch without interpreting product-specific commands.
pub fn split_sql_statements(sql: &str) -> Result<Vec<String>, QueryServiceError> {
    let mut cursor = SqlBatchCursor::new(sql);
    let mut statements = Vec::new();
    while let Some(fragment) = cursor.next_fragment()? {
        let statement = fragment.trim();
        if !statement.is_empty() {
            statements.push(statement.to_string());
        }
    }
    Ok(statements)
}

/// Removes leading whole-line comments while retaining the first SQL token.
pub fn strip_leading_line_comments(sql: &str) -> &str {
    let mut remaining = sql.trim();
    loop {
        let Some(newline) = remaining.find('\n') else {
            return if remaining.starts_with("--") || remaining.starts_with('#') {
                ""
            } else {
                remaining
            };
        };
        let line = remaining[..newline].trim();
        if line.is_empty() || line.starts_with("--") || line.starts_with('#') {
            remaining = remaining[newline + 1..].trim_start();
            continue;
        }
        return remaining;
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ProductSqlCommand, SqlBatchCursor, SqlStatementParseError, lower_product_sql_command,
        parse_optional_single_statement, parse_single_statement, query_service_parse_error,
        split_sql_statements, strip_leading_line_comments,
    };
    use crate::session_error::QueryServiceErrorKind;
    use novarocks_sql::semantic::command::{
        CommandLiteral, ExpireSnapshotsOptionSql, MaintenanceValueSql,
    };
    use novarocks_sql::semantic::{CatalogSqlCommand, MaintenanceSqlCommand, StatisticsSqlCommand};

    #[test]
    fn schema_type_lowering_uses_structured_children_and_named_rows() {
        use novarocks_parser::{SYNTHETIC, ast, build};
        use novarocks_types::schema::SqlType;
        let row = build::row_type(vec![ast::RowDataTypeField {
            name: Some(build::ident("payload", None)),
            data_type: build::type_name(
                build::ident("ARRAY", None),
                vec![build::type_parameter(build::simple_type("INT"))],
            ),
            span: SYNTHETIC,
        }]);
        assert_eq!(
            super::lower_sql_type(&row).unwrap(),
            SqlType::Struct(vec![(
                "payload".into(),
                SqlType::Array(Box::new(SqlType::Int))
            )])
        );
        let anonymous = build::row_type(vec![ast::RowDataTypeField {
            name: None,
            data_type: build::simple_type("INT"),
            span: SYNTHETIC,
        }]);
        assert!(
            super::lower_sql_type(&anonymous)
                .unwrap_err()
                .contains("named fields")
        );
        let decimal = build::type_name(
            build::ident("DECIMAL", None),
            vec![build::numeric_parameter(38), build::numeric_parameter(9)],
        );
        assert_eq!(
            super::lower_sql_type(&decimal).unwrap(),
            SqlType::Decimal {
                precision: 38,
                scale: 9
            }
        );
        let overflow = build::type_name(
            build::ident("DECIMAL", None),
            vec![build::numeric_parameter(256)],
        );
        assert!(
            super::lower_sql_type(&overflow)
                .unwrap_err()
                .contains("fit u8")
        );
    }

    #[test]
    fn schema_types_refuse_unrepresentable_temporal_and_explicit_array_facts() {
        use novarocks_parser::{SYNTHETIC, ast, build};
        for kind in [ast::DateTimeType::Time, ast::DateTimeType::Timestamp] {
            for precision in [None, Some(6), Some(3)] {
                for with_time_zone in [false, true] {
                    let ty = ast::TypeName {
                        kind: ast::TypeKind::DateTime(ast::DateTimeDataType {
                            kind,
                            with_time_zone,
                            precision: precision
                                .map(|value| build::numeric_parameter(value).into()),
                        }),
                        span: SYNTHETIC,
                    };
                    assert_eq!(
                        super::lower_sql_type(&ty).is_ok(),
                        !with_time_zone && precision.is_none_or(|value| value == 6)
                    );
                }
            }
        }
        let ty = ast::TypeName {
            kind: ast::TypeKind::ExplicitArray(ast::ExplicitArrayDataType {
                element: build::simple_type("INT").into(),
                length: ast::ArrayLength {
                    spelling: "99999999999999999999999999999".into(),
                    span: SYNTHETIC,
                },
            }),
            span: SYNTHETIC,
        };
        assert!(
            super::lower_sql_type(&ty)
                .unwrap_err()
                .contains("explicit ARRAY")
        );
    }

    #[test]
    fn command_literals_preserve_numeric_category_and_binary_payload() {
        use novarocks_parser::{SYNTHETIC, ast};
        let lower = |kind| {
            super::lower_command_literal(&ast::Literal {
                kind,
                span: SYNTHETIC,
            })
            .unwrap()
        };
        assert_eq!(
            lower(ast::LiteralKind::Long {
                spelling: "0x10".into(),
                parsed_value: 16
            }),
            CommandLiteral::Number("16".into())
        );
        assert_eq!(
            lower(ast::LiteralKind::Double(ast::DoubleValue::from_f64(1.0))),
            CommandLiteral::Number("1.0".into())
        );
        assert_eq!(
            lower(ast::LiteralKind::Decimal(
                "12345678901234567890.00100".into()
            )),
            CommandLiteral::Number("12345678901234567890.00100".into())
        );
        assert_eq!(
            lower(ast::LiteralKind::Binary(vec![0, 255, 39, 92])),
            CommandLiteral::Binary(vec![0, 255, 39, 92])
        );
        assert!(
            super::lower_command_literal(&ast::Literal {
                kind: ast::LiteralKind::Generic(ast::GenericLiteral {
                    type_name: "DATE".into(),
                    value: "2020-01-01".into()
                }),
                span: SYNTHETIC
            })
            .is_err()
        );
    }

    #[test]
    fn accepts_one_parser_statement() {
        let statement = parse_single_statement("SELECT 1").expect("one statement");
        assert!(matches!(
            statement,
            novarocks_parser::ast::Statement::Query(_)
        ));
    }

    #[test]
    fn rejects_multiple_parser_statements() {
        assert!(matches!(
            parse_single_statement("SELECT 1; SELECT 2"),
            Err(SqlStatementParseError::ExpectedExactlyOne { actual: 2 })
        ));
    }

    #[test]
    fn parser_admission_projection_keeps_the_parse_error_vocabulary() {
        let error = query_service_parse_error(
            SqlStatementParseError::ExpectedExactlyOne { actual: 2 },
            "SELECT 1; SELECT 2",
        );
        assert_eq!(error.kind(), QueryServiceErrorKind::Parse);
        assert_eq!(
            error.message(),
            "command admission requires exactly one statement"
        );
    }

    #[test]
    fn retains_comment_only_fragment_as_absent() {
        assert_eq!(
            parse_optional_single_statement("/* comment */").expect("comment parses"),
            None
        );
    }

    #[test]
    fn batch_framing_preserves_quoted_semicolons_and_statement_order() {
        assert_eq!(
            split_sql_statements("SET query_timeout = 1; SELECT ';'; SELECT 3")
                .expect("split SQL batch"),
            vec![
                "SET query_timeout = 1".to_string(),
                "SELECT ';'".to_string(),
                "SELECT 3".to_string(),
            ]
        );
    }

    #[test]
    fn batch_cursor_reports_an_unterminated_quote_before_a_later_fragment() {
        let mut cursor = SqlBatchCursor::new("SELECT 1; SELECT 'unterminated");
        assert_eq!(
            cursor.next_fragment().expect("first fragment"),
            Some("SELECT 1")
        );
        let error = cursor
            .next_fragment()
            .expect_err("must reject unterminated quote");
        assert_eq!(
            error.kind(),
            crate::session_error::QueryServiceErrorKind::Parse
        );
    }

    #[test]
    fn leading_line_comments_preserve_the_following_statement() {
        assert_eq!(
            strip_leading_line_comments("-- generated header\n# another line\nCREATE CATALOG c"),
            "CREATE CATALOG c"
        );
        assert_eq!(strip_leading_line_comments("-- comment only"), "");
    }

    #[test]
    fn catalog_lowering_preserves_force_and_creation_policy_without_a_span() {
        let statement = parse_single_statement("DROP DATABASE IF EXISTS analytics FORCE")
            .expect("parse catalog command");

        assert_eq!(
            lower_product_sql_command(&statement).expect("lower command"),
            Some(ProductSqlCommand::Catalog(Box::new(
                CatalogSqlCommand::DropDatabase {
                    name: novarocks_sql::semantic::ObjectName {
                        parts: vec!["analytics".to_string()],
                    },
                    if_exists: true,
                    force: true,
                }
            )))
        );
    }

    #[test]
    fn statistics_lowering_preserves_table_and_explicit_columns() {
        let statement = parse_single_statement("ANALYZE TABLE ice.db.events (id, kind)")
            .expect("parse statistics command");

        assert_eq!(
            lower_product_sql_command(&statement).expect("lower command"),
            Some(ProductSqlCommand::Statistics(
                StatisticsSqlCommand::AnalyzeTable {
                    table: novarocks_sql::semantic::ObjectName {
                        parts: vec!["ice".to_string(), "db".to_string(), "events".to_string()],
                    },
                    columns: vec!["id".to_string(), "kind".to_string()],
                }
            ))
        );
    }

    #[test]
    fn maintenance_lowering_preserves_every_expire_option_as_values() {
        let statement = parse_single_statement(
            "ALTER TABLE ice.db.events EXPIRE SNAPSHOTS OLDER THAN 1700000000000 RETAIN LAST 3",
        )
        .expect("parse maintenance command");

        assert_eq!(
            lower_product_sql_command(&statement).expect("lower command"),
            Some(ProductSqlCommand::Maintenance(
                MaintenanceSqlCommand::ExpireSnapshots {
                    table: novarocks_sql::semantic::ObjectName {
                        parts: vec!["ice".to_string(), "db".to_string(), "events".to_string()],
                    },
                    options: vec![
                        ExpireSnapshotsOptionSql::OlderThan(MaintenanceValueSql::Literal(
                            CommandLiteral::Number("1700000000000".to_string()),
                        )),
                        ExpireSnapshotsOptionSql::RetainLast(MaintenanceValueSql::Literal(
                            CommandLiteral::Number("3".to_string()),
                        )),
                    ],
                }
            ))
        );
    }

    #[test]
    fn table_lowering_retains_complete_creation_facts_without_parser_values() {
        let statement = parse_single_statement(
            "CREATE TABLE IF NOT EXISTS ice.db.orders (id INT NOT NULL DEFAULT 7 COMMENT 'key') \
             PRIMARY KEY (id) PARTITION BY day(id) DISTRIBUTED BY HASH (id) BUCKETS 4 \
             ORDER BY (id) COMMENT 'orders' PROPERTIES ('format-version' = '3')",
        )
        .expect("parse create table");

        let Some(ProductSqlCommand::Catalog(boxed_command)) =
            lower_product_sql_command(&statement).expect("lower command")
        else {
            panic!("expected semantic CREATE TABLE command");
        };
        let CatalogSqlCommand::CreateTable(command) = *boxed_command else {
            panic!("expected semantic CREATE TABLE command");
        };
        assert!(command.if_not_exists);
        assert_eq!(command.name.parts, ["ice", "db", "orders"]);
        assert_eq!(command.columns.len(), 1);
        assert_eq!(
            command.columns[0].default,
            Some(CommandLiteral::Number("7".to_string()))
        );
        assert_eq!(
            command.columns[0].comment,
            Some(CommandLiteral::String("key".to_string()))
        );
        assert_eq!(command.distribution.expect("distribution").buckets, Some(4));
        assert_eq!(command.order_by, ["id"]);
        assert_eq!(command.properties.len(), 1);
    }

    #[test]
    fn iceberg_lowering_retains_schema_and_property_actions() {
        let statement = parse_single_statement(
            "ALTER TABLE ice.db.orders ADD COLUMN profile STRUCT<name STRING, attributes MAP<STRING, ARRAY<DECIMAL(10, 2)>>> FIRST",
        )
        .expect("parse schema action");
        let Some(ProductSqlCommand::Catalog(boxed_command)) =
            lower_product_sql_command(&statement).expect("lower schema action")
        else {
            panic!("expected semantic Iceberg ALTER command");
        };
        let CatalogSqlCommand::AlterIcebergTable(command) = *boxed_command else {
            panic!("expected semantic Iceberg ALTER command");
        };
        assert_eq!(command.table.parts, ["ice", "db", "orders"]);
        assert!(matches!(
            command.action,
            novarocks_sql::semantic::command::IcebergTableSqlAction::Schema(
                novarocks_sql::semantic::command::IcebergSchemaSqlChange::AddColumn {
                    position: novarocks_sql::semantic::command::ColumnPositionSql::First,
                    ..
                }
            )
        ));

        let statement = parse_single_statement(
            "ALTER TABLE ice.db.orders SET TBLPROPERTIES ('format' = 'parquet', 'owner' = 'ops')",
        )
        .expect("parse property action");
        let Some(ProductSqlCommand::Catalog(boxed_command)) =
            lower_product_sql_command(&statement).expect("lower property action")
        else {
            panic!("expected semantic Iceberg ALTER command");
        };
        let CatalogSqlCommand::AlterIcebergTable(command) = *boxed_command else {
            panic!("expected semantic Iceberg ALTER command");
        };
        assert!(matches!(
            command.action,
            novarocks_sql::semantic::command::IcebergTableSqlAction::Properties(
                novarocks_sql::semantic::command::IcebergPropertiesSqlAction::Set { ref entries }
            ) if entries.len() == 2
        ));
    }

    #[test]
    fn catalog_lowering_selects_every_parser_command_variant() {
        let cases = [
            ("TRUNCATE TABLE ice.db.events", "TRUNCATE TABLE"),
            (
                "CREATE EXTERNAL CATALOG IF NOT EXISTS lake COMMENT 'catalog' PROPERTIES ('type' = 'iceberg')",
                "CREATE CATALOG",
            ),
            ("DROP CATALOG IF EXISTS lake", "DROP CATALOG"),
            (
                "CREATE DATABASE IF NOT EXISTS lake.analytics",
                "CREATE DATABASE",
            ),
            (
                "DROP DATABASE IF EXISTS lake.analytics FORCE",
                "DROP DATABASE",
            ),
            (
                "DROP TABLE IF EXISTS lake.analytics.events FORCE",
                "DROP TABLE",
            ),
            (
                "SHOW CREATE TABLE lake.analytics.events",
                "SHOW CREATE TABLE",
            ),
        ];

        for (sql, expected) in cases {
            let statement = parse_single_statement(sql).expect("parse catalog command");
            let Some(ProductSqlCommand::Catalog(command)) =
                lower_product_sql_command(&statement).expect("lower catalog command")
            else {
                panic!("{expected} must lower to a catalog command");
            };
            let actual = match *command {
                CatalogSqlCommand::TruncateTable { .. } => "TRUNCATE TABLE",
                CatalogSqlCommand::CreateCatalog(_) => "CREATE CATALOG",
                CatalogSqlCommand::DropCatalog { .. } => "DROP CATALOG",
                CatalogSqlCommand::CreateDatabase { .. } => "CREATE DATABASE",
                CatalogSqlCommand::DropDatabase { .. } => "DROP DATABASE",
                CatalogSqlCommand::DropTable { .. } => "DROP TABLE",
                CatalogSqlCommand::ShowCreateTable { .. } => "SHOW CREATE TABLE",
                CatalogSqlCommand::CreateTable(_) | CatalogSqlCommand::AlterIcebergTable(_) => {
                    panic!("{expected} selected an unrelated catalog variant")
                }
            };
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn statistics_lowering_selects_statement_owned_commands() {
        for (sql, expected) in [
            ("ANALYZE TABLE lake.db.events", "ANALYZE"),
            ("SHOW STATS FOR lake.db.events", "SHOW STATS FOR"),
        ] {
            let statement = parse_single_statement(sql).unwrap();
            let Some(ProductSqlCommand::Statistics(command)) =
                lower_product_sql_command(&statement).unwrap()
            else {
                panic!("statistics command");
            };
            let actual = match command {
                StatisticsSqlCommand::AnalyzeTable { .. } => "ANALYZE",
                StatisticsSqlCommand::ShowStats { .. } => "SHOW STATS FOR",
            };
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn maintenance_lowering_selects_every_parser_command_variant() {
        let cases = [
            ("CALL system.rebuild()", "CALL"),
            ("ALTER TABLE lake.db.events OPTIMIZE", "OPTIMIZE"),
            (
                "ALTER TABLE lake.db.events REWRITE MANIFESTS",
                "REWRITE MANIFESTS",
            ),
            (
                "ALTER TABLE lake.db.events EXPIRE SNAPSHOTS OLDER THAN 1700000000000",
                "EXPIRE SNAPSHOTS",
            ),
            (
                "ALTER TABLE lake.db.events REMOVE ORPHAN FILES OLDER THAN 1700000000000",
                "REMOVE ORPHAN FILES",
            ),
        ];

        for (sql, expected) in cases {
            let statement = parse_single_statement(sql).expect("parse maintenance command");
            let Some(ProductSqlCommand::Maintenance(command)) =
                lower_product_sql_command(&statement).expect("lower maintenance command")
            else {
                panic!("{expected} must lower to a maintenance command");
            };
            let actual = match command {
                MaintenanceSqlCommand::Call { .. } => "CALL",
                MaintenanceSqlCommand::Optimize { .. } => "OPTIMIZE",
                MaintenanceSqlCommand::RewriteManifests { .. } => "REWRITE MANIFESTS",
                MaintenanceSqlCommand::ExpireSnapshots { .. } => "EXPIRE SNAPSHOTS",
                MaintenanceSqlCommand::RemoveOrphanFiles { .. } => "REMOVE ORPHAN FILES",
            };
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn iceberg_lowering_selects_every_parser_action_family() {
        let cases = [
            (
                "ALTER TABLE lake.db.events ADD COLUMN added INT",
                "schema add",
            ),
            (
                "ALTER TABLE lake.db.events DROP COLUMN old_column",
                "schema drop",
            ),
            (
                "ALTER TABLE lake.db.events RENAME COLUMN old_column TO new_column",
                "schema rename",
            ),
            (
                "ALTER TABLE lake.db.events MODIFY COLUMN old_column BIGINT",
                "schema modify",
            ),
            (
                "ALTER TABLE lake.db.events ALTER COLUMN old_column SET NOT NULL",
                "schema alter",
            ),
            (
                "ALTER TABLE lake.db.events SET TBLPROPERTIES ('owner' = 'analytics')",
                "properties set",
            ),
            (
                "ALTER TABLE lake.db.events UNSET TBLPROPERTIES IF EXISTS ('owner')",
                "properties unset",
            ),
            (
                "ALTER TABLE lake.db.events COMMENT 'analytics events'",
                "properties comment",
            ),
            (
                "ALTER TABLE lake.db.events ADD PARTITION COLUMN day(event_time)",
                "partition add",
            ),
            (
                "ALTER TABLE lake.db.events DROP PARTITION COLUMN day(event_time)",
                "partition drop",
            ),
            (
                "ALTER TABLE lake.db.events CREATE BRANCH IF NOT EXISTS dev AS OF VERSION 7",
                "reference create",
            ),
            (
                "ALTER TABLE lake.db.events DROP BRANCH IF EXISTS dev",
                "reference drop",
            ),
            (
                "ALTER TABLE lake.db.events ADD FILES FROM 's3://warehouse/events'",
                "add files",
            ),
        ];

        for (sql, expected) in cases {
            let statement = parse_single_statement(sql).expect("parse Iceberg command");
            let Some(ProductSqlCommand::Catalog(boxed_command)) =
                lower_product_sql_command(&statement).expect("lower Iceberg command")
            else {
                panic!("{expected} must lower to an Iceberg catalog command");
            };
            let CatalogSqlCommand::AlterIcebergTable(command) = *boxed_command else {
                panic!("{expected} must lower to an Iceberg catalog command");
            };
            let actual = match command.action {
                novarocks_sql::semantic::command::IcebergTableSqlAction::Schema(
                    novarocks_sql::semantic::command::IcebergSchemaSqlChange::AddColumn { .. },
                ) => "schema add",
                novarocks_sql::semantic::command::IcebergTableSqlAction::Schema(
                    novarocks_sql::semantic::command::IcebergSchemaSqlChange::DropColumn { .. },
                ) => "schema drop",
                novarocks_sql::semantic::command::IcebergTableSqlAction::Schema(
                    novarocks_sql::semantic::command::IcebergSchemaSqlChange::RenameColumn {
                        ..
                    },
                ) => "schema rename",
                novarocks_sql::semantic::command::IcebergTableSqlAction::Schema(
                    novarocks_sql::semantic::command::IcebergSchemaSqlChange::ModifyColumn {
                        ..
                    },
                ) => "schema modify",
                novarocks_sql::semantic::command::IcebergTableSqlAction::Schema(
                    novarocks_sql::semantic::command::IcebergSchemaSqlChange::AlterColumn {
                        ..
                    },
                ) => "schema alter",
                novarocks_sql::semantic::command::IcebergTableSqlAction::Properties(
                    novarocks_sql::semantic::command::IcebergPropertiesSqlAction::Set { .. },
                ) => "properties set",
                novarocks_sql::semantic::command::IcebergTableSqlAction::Properties(
                    novarocks_sql::semantic::command::IcebergPropertiesSqlAction::Unset { .. },
                ) => "properties unset",
                novarocks_sql::semantic::command::IcebergTableSqlAction::Properties(
                    novarocks_sql::semantic::command::IcebergPropertiesSqlAction::Comment {
                        ..
                    },
                ) => "properties comment",
                novarocks_sql::semantic::command::IcebergTableSqlAction::Partition(
                    novarocks_sql::semantic::command::IcebergPartitionSqlChange::Add(_),
                ) => "partition add",
                novarocks_sql::semantic::command::IcebergTableSqlAction::Partition(
                    novarocks_sql::semantic::command::IcebergPartitionSqlChange::Drop(_),
                ) => "partition drop",
                novarocks_sql::semantic::command::IcebergTableSqlAction::Reference(
                    novarocks_sql::semantic::command::IcebergReferenceSqlAction::Create { .. },
                ) => "reference create",
                novarocks_sql::semantic::command::IcebergTableSqlAction::Reference(
                    novarocks_sql::semantic::command::IcebergReferenceSqlAction::Drop { .. },
                ) => "reference drop",
                novarocks_sql::semantic::command::IcebergTableSqlAction::AddFiles { .. } => {
                    "add files"
                }
            };
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn iceberg_lowering_rejects_unimplemented_reference_retention_options() {
        for sql in [
            "ALTER TABLE lake.db.events CREATE BRANCH dev RETAIN 7 DAYS",
            "ALTER TABLE lake.db.events CREATE TAG release AS OF VERSION 7 RETAIN 3 DAYS",
        ] {
            let statement = parse_single_statement(sql).expect("parse retained reference");
            assert!(
                lower_product_sql_command(&statement)
                    .unwrap_err()
                    .contains("retention options")
            );
        }
    }
}
