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

use std::sync::Arc;

use novarocks_parser::{ParserError, ast::TrinoStatement};
use novarocks_spi::connector::ConnectorPartitionTransform;
use novarocks_sql::semantic::command::CommandLiteral;
use novarocks_sql::semantic::{
    CatalogSqlCommand, IcebergPartitionFieldExpr, MaintenanceSqlCommand, ObjectName,
    StatisticsSqlCommand,
};
use novarocks_types::schema::SqlType;

use crate::admitted_query_context::RequestContext;
use crate::api::{
    CatalogCommandConsumer, CommandContext, CommandFuture, MaintenanceCommandConsumer,
    StatisticsCommandConsumer,
};

/// Connection-local execution settings after SQL and session validation.
pub mod session;

/// Typed session-admission errors and their stable user-error descriptors.
pub mod session_admit;

/// Typed Trino statement admission shared by syntax-specific callers.
pub mod trino_admission;

/// Typed user-data errors for DDL property scopes and stable descriptors.
pub mod property_admit;

mod maintenance_call;
/// Declared DDL-property evaluation through the original required-constant port.
pub mod property_evaluation;
pub mod table_procedure;

/// Typed table and statistics command lowering after Trino parsing.
pub mod trino_table_command;
pub use maintenance_call::{
    PreparedMaintenanceArgument, PreparedMaintenanceCall, PreparedMaintenanceValue,
};

/// Typed DML statement-shape admission errors and their stable user-error descriptors.
pub mod dml_admission;

/// Read-only Catalog facts consumed by session SQL admission.
pub mod catalog;

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

    pub fn acquire_namespace_property_binding(
        &self,
        name: &novarocks_sql::semantic::ObjectName,
        request_context: &RequestContext,
    ) -> Result<crate::api::CatalogNamespacePropertyBinding, crate::api::CommandError> {
        self.catalog
            .acquire_namespace_property_binding(name, request_context)
    }

    pub fn acquire_table_read_binding(
        &self,
        name: &novarocks_sql::semantic::ObjectName,
        request_context: &RequestContext,
    ) -> Result<crate::api::CatalogTableReadBinding, crate::api::CommandError> {
        self.catalog
            .acquire_table_read_binding(name, request_context)
    }

    pub fn acquire_table_property_binding(
        &self,
        name: &novarocks_sql::semantic::ObjectName,
        request_context: &RequestContext,
    ) -> Result<crate::api::CatalogTablePropertyBinding, crate::api::CommandError> {
        self.catalog
            .acquire_table_property_binding(name, request_context)
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
pub fn connector_partition_transform(
    field: &IcebergPartitionFieldExpr,
) -> ConnectorPartitionTransform {
    match field {
        IcebergPartitionFieldExpr::Identity { column } => ConnectorPartitionTransform::Identity {
            column: Arc::from(column.as_str()),
        },
        IcebergPartitionFieldExpr::Year { column } => ConnectorPartitionTransform::Year {
            column: Arc::from(column.as_str()),
        },
        IcebergPartitionFieldExpr::Month { column } => ConnectorPartitionTransform::Month {
            column: Arc::from(column.as_str()),
        },
        IcebergPartitionFieldExpr::Day { column } => ConnectorPartitionTransform::Day {
            column: Arc::from(column.as_str()),
        },
        IcebergPartitionFieldExpr::Hour { column } => ConnectorPartitionTransform::Hour {
            column: Arc::from(column.as_str()),
        },
        IcebergPartitionFieldExpr::Bucket {
            column,
            num_buckets,
        } => ConnectorPartitionTransform::Bucket {
            column: Arc::from(column.as_str()),
            num_buckets: *num_buckets,
        },
        IcebergPartitionFieldExpr::Truncate { column, width } => {
            ConnectorPartitionTransform::Truncate {
                column: Arc::from(column.as_str()),
                width: *width,
            }
        }
        IcebergPartitionFieldExpr::Void { column } => ConnectorPartitionTransform::Void {
            column: Arc::from(column.as_str()),
        },
    }
}

fn lower_sql_type(type_name: &novarocks_parser::ast::TypeName) -> Result<SqlType, String> {
    use novarocks_parser::ast::{DataTypeParameter, TypeKind};

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
        TypeKind::DateTime(_) | TypeKind::Interval(_) => {
            return novarocks_sql::declared_temporal_type(type_name)
                .map_err(|error| error.to_string())?
                .map(SqlType::Temporal)
                .ok_or_else(|| "temporal syntax lost its type declaration".into());
        }
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

/// Lower an admitted object name into the semantic command vocabulary.
pub fn lower_object_name(name: &novarocks_parser::ast::ObjectName) -> ObjectName {
    ObjectName {
        parts: name.parts.iter().map(|part| part.value.clone()).collect(),
    }
}

/// Admit one complete framed statement and check root structural invariants.
pub fn parse_single_statement(source: &str) -> Result<TrinoStatement, ParserError> {
    let statement = novarocks_parser::parser::parse_statement(source)?;
    novarocks_parser::ast::validate_statement(&statement)?;
    Ok(statement)
}

#[cfg(test)]
mod tests {
    use super::{CommandLiteral, ParserError, TrinoStatement, parse_single_statement};

    #[test]
    fn schema_type_lowering_uses_structured_children_and_named_rows() {
        use novarocks_parser::{SYNTHETIC, ast, build};
        use novarocks_types::schema::SqlType;
        let row = build::row_type(vec![ast::RowDataTypeField {
            name: Some(build::ident("payload", false)),
            data_type: build::type_name(
                build::ident("ARRAY", false),
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
            build::ident("DECIMAL", false),
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
            build::ident("DECIMAL", false),
            vec![build::numeric_parameter(256)],
        );
        assert!(
            super::lower_sql_type(&overflow)
                .unwrap_err()
                .contains("fit u8")
        );
    }

    #[test]
    fn schema_types_keep_exact_temporal_and_refuse_explicit_array_facts() {
        use novarocks_parser::{SYNTHETIC, ast, build};
        use novarocks_type_contract::{TemporalPrecision, TemporalType};
        for kind in [ast::DateTimeType::Time, ast::DateTimeType::Timestamp] {
            for digits in [None, Some(0), Some(6), Some(9), Some(12), Some(13)] {
                for with_time_zone in [false, true] {
                    let ty = ast::TypeName {
                        kind: ast::TypeKind::DateTime(ast::DateTimeDataType {
                            kind,
                            with_time_zone,
                            precision: digits.map(|value| build::numeric_parameter(value).into()),
                        }),
                        span: SYNTHETIC,
                    };
                    let actual = super::lower_sql_type(&ty);
                    let Ok(precision) = TemporalPrecision::new(digits.unwrap_or(3) as u8) else {
                        assert!(actual.is_err());
                        continue;
                    };
                    let declaration = match (kind, with_time_zone) {
                        (ast::DateTimeType::Time, false) => TemporalType::Time(precision),
                        (ast::DateTimeType::Time, true) => {
                            TemporalType::TimeWithTimeZone(precision)
                        }
                        (ast::DateTimeType::Timestamp, false) => TemporalType::Timestamp(precision),
                        (ast::DateTimeType::Timestamp, true) => {
                            TemporalType::TimestampWithTimeZone(precision)
                        }
                    };
                    assert_eq!(
                        actual.unwrap(),
                        novarocks_types::schema::SqlType::Temporal(declaration)
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
    fn accepts_one_framed_statement_with_quoted_semicolons_and_trivia() {
        let statement = parse_single_statement(
            "-- leading ; comment
SELECT 'a;b' AS \"label;part\" /* trailing ; comment */",
        )
        .unwrap();
        assert!(matches!(statement, TrinoStatement::Query(_)));
    }

    #[test]
    fn rejects_multiple_and_trivia_only_requests_at_the_parser() {
        for source in [
            "SELECT 1; SELECT 2",
            "",
            "-- comment only",
            "/* comment only */",
        ] {
            let error = parse_single_statement(source).unwrap_err();
            assert_eq!(
                error.to_user_error(source).code().as_str(),
                "sql.parse.unexpected_token"
            );
        }
    }

    #[test]
    fn applies_the_structural_validation_owner_after_parsing() {
        let source = "WITH a AS (SELECT 1), a AS (SELECT 2) SELECT * FROM a";
        assert!(novarocks_parser::parser::parse_statement(source).is_ok());
        let error = parse_single_statement(source).unwrap_err();
        assert!(matches!(&error, ParserError::Validate(_)));
        assert_eq!(
            error.to_user_error(source).code().as_str(),
            "sql.validate.duplicate_cte_name"
        );
    }
}
