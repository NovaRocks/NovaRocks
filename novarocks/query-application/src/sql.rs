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
    AlterIcebergTableSqlCommand, AnalyzeModeSql, CatalogCreateCommand, ColumnPositionSql,
    CommandLiteral, CommandProperty, CreateTableSqlCommand, ExpireSnapshotsOptionSql,
    IcebergColumnSqlAction, IcebergPartitionSqlChange, IcebergPropertiesSqlAction,
    IcebergReferenceKindSql, IcebergReferenceSqlAction, IcebergSchemaSqlChange,
    IcebergTableSqlAction, MaintenanceValueSql, ProcedureArgumentModeSql, ProcedureArgumentSql,
    ReferenceAnchorSql, ShowOptimizeFilterSql, ShowOptimizeOrderSql, ShowOptimizeSqlCommand,
    SortDirectionSql, TableColumnSqlCommand, TableDistributionSqlCommand, TablePartitionSqlCommand,
};
use novarocks_sql::semantic::{
    CatalogSqlCommand, ColumnAggregation, IcebergPartitionFieldExpr, MaintenanceSqlCommand,
    ObjectName, StatisticsSqlCommand, TableKeyDesc, TableKeyKind,
};
use novarocks_types::schema::SqlType;

use crate::admitted_query_context::RequestContext;
use crate::api::{
    CatalogCommandConsumer, CommandContext, CommandFuture, MaintenanceCommandConsumer,
    StatisticsCommandConsumer,
};
use crate::protocol_delivery::QuerySessionOutput;
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

/// Parser-admitted KILL statement execution over query and connection ports.
pub mod kill;

/// Query-result scalar conversion used by SQL session user variables.
pub mod user_variable;

/// Product-command port consumed after SQL admission has selected a typed
/// statement. Implementations remain role-local adapters: the port transfers
/// only immutable admitted context and the governed command context.
pub trait CoreCommandRoute: Send + Sync {
    /// Executes the role-gated `SHOW BACKENDS` command.
    fn execute_show_backends(
        &self,
        _context: &RequestContext,
        _command_context: &CommandContext,
    ) -> Result<QuerySessionOutput, String> {
        Err("SHOW BACKENDS command route is unavailable".to_string())
    }

    /// Executes the test-only stateless-rebuild procedure when it owns this
    /// exact CALL. `None` leaves the already-lowered maintenance command to
    /// its normal product consumer.
    fn execute_maintenance_call(
        &self,
        _statement: &novarocks_parser::ast::CallStatement,
        _context: &RequestContext,
        _command_context: &CommandContext,
    ) -> Result<Option<QuerySessionOutput>, String> {
        Ok(None)
    }

    /// Executes the deliberately specialized, complete MV parser input.
    /// MV must not acquire a second semantic command mirror.
    fn execute_materialized_view(
        &self,
        _statement: &novarocks_parser::ast::MaterializedViewStatement,
        _context: &RequestContext,
        _command_context: &CommandContext,
    ) -> Result<QuerySessionOutput, String> {
        Err("materialized view command route is unavailable".to_string())
    }

    /// Executes the query-owned View family, which is outside the product
    /// command vocabulary.
    fn execute_view(
        &self,
        _statement: &novarocks_parser::ast::ViewStatement,
        _context: &RequestContext,
        _command_context: &CommandContext,
    ) -> Result<QuerySessionOutput, String> {
        Err("view command route is unavailable".to_string())
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

    pub fn execute(
        &self,
        command: ProductSqlCommand,
        request_context: RequestContext,
        command_context: CommandContext,
    ) -> CommandFuture {
        match command {
            ProductSqlCommand::Catalog(command) => {
                self.catalog
                    .execute(command, request_context, command_context)
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
    Catalog(CatalogSqlCommand),
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
        ParsedStatement::Catalog(statement) => Ok(Some(ProductSqlCommand::Catalog(
            lower_catalog_command(statement)?,
        ))),
        ParsedStatement::Statistics(statement) => Ok(Some(ProductSqlCommand::Statistics(
            lower_statistics_command(statement)?,
        ))),
        ParsedStatement::Maintenance(statement) => Ok(Some(ProductSqlCommand::Maintenance(
            lower_maintenance_command(statement)?,
        ))),
        ParsedStatement::Table(statement) => Ok(Some(ProductSqlCommand::Catalog(
            lower_table_command(statement)?,
        ))),
        ParsedStatement::Iceberg(statement) => Ok(Some(ProductSqlCommand::Catalog(
            lower_iceberg_command(statement)?,
        ))),
        ParsedStatement::Dml(_)
        | ParsedStatement::MaterializedView(_)
        | ParsedStatement::View(_)
        | ParsedStatement::ShowBackends(_)
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
            } => IcebergReferenceSqlAction::Create {
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
                has_uninterpreted_provider_options: options.is_some(),
            },
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
        let LiteralKind::Number(value) = &literal.kind else {
            return Err(format!("Iceberg {label} requires a numeric literal"));
        };
        value
            .parse::<u32>()
            .map_err(|_| format!("Iceberg {label} must fit u32"))
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
    use novarocks_parser::ast::TypeNameArgument;

    let name = type_name
        .name
        .parts
        .last()
        .ok_or_else(|| "type name is empty".to_string())?
        .value
        .to_ascii_lowercase();
    if type_name.name.parts.len() != 1 {
        return Err(format!("qualified type name `{name}` is not supported"));
    }
    match name.as_str() {
        "array" => {
            let [TypeNameArgument::Type(element)] = type_name.arguments.as_slice() else {
                return Err("ARRAY type requires one element type".to_string());
            };
            Ok(SqlType::Array(Box::new(lower_sql_type(element)?)))
        }
        "map" => {
            let [TypeNameArgument::Type(key), TypeNameArgument::Type(value)] =
                type_name.arguments.as_slice()
            else {
                return Err("MAP type requires key and value types".to_string());
            };
            Ok(SqlType::Map(
                Box::new(lower_sql_type(key)?),
                Box::new(lower_sql_type(value)?),
            ))
        }
        "struct" => Ok(SqlType::Struct(
            type_name
                .arguments
                .iter()
                .map(|argument| match argument {
                    TypeNameArgument::Field(field) => {
                        Ok((field.name.value.clone(), lower_sql_type(&field.data_type)?))
                    }
                    _ => Err("STRUCT type requires named fields".to_string()),
                })
                .collect::<Result<_, String>>()?,
        )),
        "decimal" | "dec" | "numeric" | "decimal32" | "decimal64" | "decimal128" => {
            let values = type_name
                .arguments
                .iter()
                .map(|argument| match argument {
                    TypeNameArgument::Literal(literal) => {
                        if let novarocks_parser::ast::LiteralKind::Number(value) = &literal.kind {
                            Ok(value.as_str())
                        } else {
                            Err("DECIMAL precision and scale must be numeric literals".to_string())
                        }
                    }
                    _ => Err("DECIMAL precision and scale must be numeric literals".to_string()),
                })
                .collect::<Result<Vec<_>, _>>()?;
            if values.len() > 2 {
                return Err("DECIMAL type accepts at most precision and scale".to_string());
            }
            let precision = values
                .first()
                .map(|value| {
                    value
                        .parse::<u8>()
                        .map_err(|_| "DECIMAL precision must fit u8".to_string())
                })
                .transpose()?
                .unwrap_or(38);
            let scale = values
                .get(1)
                .map(|value| {
                    value
                        .parse::<i8>()
                        .map_err(|_| "DECIMAL scale must fit i8".to_string())
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
    use novarocks_parser::ast::{AnalyzeMode, StatisticsStatement};

    Ok(match statement {
        StatisticsStatement::AnalyzeTable(statement) => StatisticsSqlCommand::AnalyzeTable {
            mode: match statement.mode {
                AnalyzeMode::Default => AnalyzeModeSql::Default,
                AnalyzeMode::Full => AnalyzeModeSql::Full,
                AnalyzeMode::Sample => AnalyzeModeSql::Sample,
            },
            table: lower_object_name(&statement.name),
            columns: statement
                .columns
                .iter()
                .map(|column| column.value.clone())
                .collect(),
            with_sync_mode: statement.with_sync_mode,
        },
        StatisticsStatement::ShowAnalyzeJobs(_) => StatisticsSqlCommand::ShowAnalyzeJobs,
        StatisticsStatement::CancelAnalyze(statement) => StatisticsSqlCommand::CancelAnalyze {
            job_id: statement.job_id.clone(),
        },
        StatisticsStatement::ShowTableStats(statement) => StatisticsSqlCommand::ShowTableStats {
            table: lower_object_name(&statement.name),
        },
        StatisticsStatement::ShowBasicStatsMeta(_) => StatisticsSqlCommand::ShowBasicStatsMeta,
        StatisticsStatement::ShowHistogramStatsMeta(_) => {
            StatisticsSqlCommand::ShowHistogramStatsMeta
        }
        StatisticsStatement::DropStats(statement) => StatisticsSqlCommand::DropStats {
            table: lower_object_name(&statement.name),
        },
        StatisticsStatement::DropHistogram(statement) => StatisticsSqlCommand::DropHistogram {
            table: lower_object_name(&statement.name),
            columns: statement
                .columns
                .iter()
                .map(|column| column.value.clone())
                .collect(),
        },
        StatisticsStatement::DropMultipleColumnsStats(statement) => {
            StatisticsSqlCommand::DropMultipleColumnsStats {
                table: lower_object_name(&statement.name),
            }
        }
    })
}

fn lower_maintenance_command(
    statement: &novarocks_parser::ast::MaintenanceStatement,
) -> Result<MaintenanceSqlCommand, String> {
    use novarocks_parser::ast::{
        ExpireSnapshotsOption, MaintenanceStatement, ProcedureArgumentMode, SortDirection,
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
        MaintenanceStatement::ShowOptimize(statement) => {
            MaintenanceSqlCommand::ShowOptimize(ShowOptimizeSqlCommand {
                from: statement.from.as_ref().map(lower_object_name),
                filter: statement
                    .filter
                    .as_ref()
                    .map(|filter| {
                        Ok::<_, String>(ShowOptimizeFilterSql {
                            column: filter.column.value.clone(),
                            value: lower_command_literal(&filter.value)?,
                        })
                    })
                    .transpose()?,
                order_by: statement
                    .order_by
                    .as_ref()
                    .map(|order| ShowOptimizeOrderSql {
                        column: order.column.value.clone(),
                        direction: order.direction.map(|direction| match direction {
                            SortDirection::Asc => SortDirectionSql::Asc,
                            SortDirection::Desc => SortDirectionSql::Desc,
                        }),
                    }),
                limit: statement
                    .limit
                    .as_ref()
                    .map(lower_command_literal)
                    .transpose()?,
            })
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
        LiteralKind::Number(value) => Ok(CommandLiteral::Number(value.clone())),
        LiteralKind::String(value) => Ok(CommandLiteral::String(value.clone())),
        LiteralKind::HexString(value) => {
            let digits = value
                .strip_prefix("0x")
                .or_else(|| value.strip_prefix("0X"))
                .unwrap_or(value);
            decode_hex_literal(digits)
                .map(CommandLiteral::Binary)
                .map_err(|error| format!("invalid hex literal X'{value}': {error}"))
        }
    }
}

fn decode_hex_literal(value: &str) -> Result<Vec<u8>, &'static str> {
    if !value.len().is_multiple_of(2) {
        return Err("hex literal has an odd number of digits");
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let digit = |value: u8| match value {
                b'0'..=b'9' => Ok(value - b'0'),
                b'a'..=b'f' => Ok(value - b'a' + 10),
                b'A'..=b'F' => Ok(value - b'A' + 10),
                _ => Err("hex literal contains a non-hex digit"),
            };
            Ok((digit(pair[0])? << 4) | digit(pair[1])?)
        })
        .collect()
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
        AnalyzeModeSql, CommandLiteral, ExpireSnapshotsOptionSql, MaintenanceValueSql,
    };
    use novarocks_sql::semantic::{CatalogSqlCommand, MaintenanceSqlCommand, StatisticsSqlCommand};

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
            Some(ProductSqlCommand::Catalog(
                CatalogSqlCommand::DropDatabase {
                    name: novarocks_sql::semantic::ObjectName {
                        parts: vec!["analytics".to_string()],
                    },
                    if_exists: true,
                    force: true,
                }
            ))
        );
    }

    #[test]
    fn statistics_lowering_preserves_mode_columns_and_sync_option() {
        let statement =
            parse_single_statement("ANALYZE FULL TABLE ice.db.events (id, kind) WITH SYNC MODE")
                .expect("parse statistics command");

        assert_eq!(
            lower_product_sql_command(&statement).expect("lower command"),
            Some(ProductSqlCommand::Statistics(
                StatisticsSqlCommand::AnalyzeTable {
                    mode: AnalyzeModeSql::Full,
                    table: novarocks_sql::semantic::ObjectName {
                        parts: vec!["ice".to_string(), "db".to_string(), "events".to_string()],
                    },
                    columns: vec!["id".to_string(), "kind".to_string()],
                    with_sync_mode: true,
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

        let Some(ProductSqlCommand::Catalog(CatalogSqlCommand::CreateTable(command))) =
            lower_product_sql_command(&statement).expect("lower command")
        else {
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
        let Some(ProductSqlCommand::Catalog(CatalogSqlCommand::AlterIcebergTable(command))) =
            lower_product_sql_command(&statement).expect("lower schema action")
        else {
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
        let Some(ProductSqlCommand::Catalog(CatalogSqlCommand::AlterIcebergTable(command))) =
            lower_product_sql_command(&statement).expect("lower property action")
        else {
            panic!("expected semantic Iceberg ALTER command");
        };
        assert!(matches!(
            command.action,
            novarocks_sql::semantic::command::IcebergTableSqlAction::Properties(
                novarocks_sql::semantic::command::IcebergPropertiesSqlAction::Set { ref entries }
            ) if entries.len() == 2
        ));
    }
}
