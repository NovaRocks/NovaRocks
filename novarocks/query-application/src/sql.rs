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
    CatalogSqlCommand, IcebergPartitionFieldExpr, ObjectName, StatisticsSqlCommand,
};
use novarocks_types::schema::SqlType;

use crate::admitted_query_context::RequestContext;
use crate::api::{
    CatalogCommandConsumer, CommandContext, CommandFuture, StatisticsCommandConsumer,
};
use crate::session_error::QueryServiceError;

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
}

impl ProductCommandRouter {
    pub fn new(
        catalog: Arc<dyn CatalogCommandConsumer>,
        statistics: Arc<dyn StatisticsCommandConsumer>,
    ) -> Self {
        Self {
            catalog,
            statistics,
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

fn lower_sql_type(
    type_name: &novarocks_parser::ast::TypeName,
    source: &str,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<SqlType, QueryServiceError> {
    use novarocks_sql::{TypeDeclarationProblem, resolve_trino_type};

    let value =
        resolve_trino_type(type_name, true, control).map_err(|error| match error.problem {
            TypeDeclarationProblem::Control => QueryServiceError::from_compile_control(
                error.control.expect("original type declaration control"),
            ),
            TypeDeclarationProblem::InvalidDecimalParameters => QueryServiceError::from_user_error(
                novarocks_sql::analyze_error::AnalyzeError::invalid_type_argument(
                    error.to_string(),
                    error.span,
                )
                .to_user_error(Some(source)),
            ),
            TypeDeclarationProblem::Unsupported => {
                trino_admission::unsupported_statement_detail(source, error.span, error.to_string())
            }
            TypeDeclarationProblem::UnknownType | TypeDeclarationProblem::InvalidParameters => {
                let written = novarocks_parser::printer::print_type(type_name)
                    .map(|name| format!("Unknown type '{name}'"))
                    .unwrap_or_else(|_| error.to_string());
                trino_admission::type_not_found(source, error.span, written)
            }
        })?;
    check_named_schema_rows(type_name, source, control)?;
    novarocks_types::sql_type_from_execution_value(value.typed_binding()).map_err(|error| {
        trino_admission::unsupported_statement_detail(source, type_name.span, error.to_string())
    })
}

fn check_named_schema_rows(
    type_name: &novarocks_parser::ast::TypeName,
    source: &str,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<(), QueryServiceError> {
    use novarocks_parser::ast::{DataTypeParameter, TypeKind};
    use novarocks_type_contract::CompilePhase;

    control
        .checkpoint(CompilePhase::Validate, 1)
        .map_err(QueryServiceError::from_compile_control)?;
    match &type_name.kind {
        TypeKind::Row(row) => {
            for field in &row.fields {
                if field.name.is_none() {
                    return Err(trino_admission::unsupported_statement_detail(
                        source,
                        field.span,
                        "ROW schema type requires named fields".into(),
                    ));
                }
                check_named_schema_rows(&field.data_type, source, control)?;
            }
        }
        TypeKind::Generic(generic) => {
            for parameter in &generic.arguments {
                if let DataTypeParameter::Type(nested) = parameter {
                    check_named_schema_rows(&nested.data_type, source, control)?;
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn validate_bounded_varchar_default(
    source: &str,
    literal: &novarocks_parser::ast::Literal,
    data_type: &SqlType,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<(), QueryServiceError> {
    use dml_admission::DmlAdmissionError;
    use novarocks_parser::ast::LiteralKind;
    use novarocks_type_contract::{CharacterKind, CompileCheckpoints, CompilePhase};

    let SqlType::BoundedCharacter(character) = data_type else {
        return Ok(());
    };
    if character.kind() != CharacterKind::Varchar {
        return Ok(());
    }
    let width = character.length();
    match &literal.kind {
        LiteralKind::Null => return Ok(()),
        LiteralKind::String(value) => {
            let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)
                .map_err(QueryServiceError::from_compile_control)?;
            let mut fits = true;
            for (index, _) in value.chars().enumerate() {
                work.step()
                    .map_err(QueryServiceError::from_compile_control)?;
                if index == width as usize {
                    fits = false;
                    break;
                }
            }
            work.finish()
                .map_err(QueryServiceError::from_compile_control)?;
            if fits {
                return Ok(());
            }
        }
        _ => {}
    }
    Err(QueryServiceError::from_user_error(
        DmlAdmissionError::InvalidDefaultColumnValue.to_user_error(
            source,
            literal.span,
            format!("DEFAULT value is not a valid VARCHAR({width}) literal"),
        ),
    ))
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

    fn schema_type(type_name: &novarocks_parser::ast::TypeName) -> Result<super::SqlType, String> {
        super::lower_sql_type(
            type_name,
            "",
            &novarocks_sql::compiler::SqlCompileControl::unbounded(),
        )
        .map_err(|error| error.to_string())
    }

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
            schema_type(&row).unwrap(),
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
            schema_type(&anonymous)
                .unwrap_err()
                .contains("named fields")
        );
        let decimal = build::type_name(
            build::ident("DECIMAL", false),
            vec![build::numeric_parameter(38), build::numeric_parameter(9)],
        );
        assert_eq!(
            schema_type(&decimal).unwrap(),
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
            schema_type(&overflow)
                .unwrap_err()
                .contains("DECIMAL precision must be in range [1, 38]: 256")
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
                    let actual = schema_type(&ty);
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
        assert!(schema_type(&ty).unwrap_err().contains("explicit ARRAY"));
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
