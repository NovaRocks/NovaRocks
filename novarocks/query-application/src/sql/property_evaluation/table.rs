// SPDX-License-Identifier: Apache-2.0

//! Property and column preparation for the typed table statements.

use std::{collections::BTreeSet, sync::Arc};

use novarocks_failure_contract::SharedFailureCause;
use novarocks_parser::{
    Span,
    ast::{
        Expr, LikePropertiesOption, Query, StatementProperty, StatementSaveMode,
        TrinoColumnDefinition, TrinoCreateTable, TrinoCreateTableAsSelect, TrinoLikeClause,
        TrinoTableElement,
    },
};
use novarocks_spi::connector::{
    ConnectorCatalogMutationOperation, ConnectorCatalogMutationSession, ConnectorError,
    DdlPropertyDeclaration, DdlPropertyValue,
};
use novarocks_sql::{
    analyze_error::AnalyzeError,
    semantic::command::{CommandLiteral, CreateTableSqlCommand, TableColumnSqlCommand},
};
use novarocks_type_contract::PureCompileControl;

use super::{
    DdlPropertyChange, DdlPropertyConstantPort, DdlPropertyEvaluationMode, DdlPropertyNamePolicy,
    DdlPropertyScope, EvaluatedDdlProperties, checkpoint, copy_string,
    evaluate_ddl_properties_with_name_policy, invariant, observe_bytes,
    statement_property_assignments,
};
use crate::{
    api::CatalogTablePropertyBinding,
    session_error::QueryServiceError,
    sql::{
        dml_admission::DmlAdmissionError, lower_command_literal, lower_object_name, lower_sql_type,
        validate_bounded_varchar_default,
    },
};

fn unsupported(source: &str, span: Span, message: impl Into<String>) -> QueryServiceError {
    QueryServiceError::from_user_error(
        DmlAdmissionError::CreateTableUnsupportedForm.to_user_error(source, span, message),
    )
}

fn provider_error(error: ConnectorError) -> QueryServiceError {
    QueryServiceError::from_failure_cause(SharedFailureCause::new(error))
}

struct TableNames<'a>(&'a ConnectorCatalogMutationSession);
impl DdlPropertyNamePolicy for TableNames<'_> {
    fn check_unknown_property(&mut self, name: &str) -> Result<(), QueryServiceError> {
        self.0
            .check_table_property_name(name)
            .map_err(provider_error)
    }
}

fn evaluate_table_properties(
    source: &str,
    properties: &[StatementProperty],
    declarations: &[DdlPropertyDeclaration],
    mode: DdlPropertyEvaluationMode,
    names: &mut dyn DdlPropertyNamePolicy,
    port: &mut dyn DdlPropertyConstantPort,
    control: &dyn PureCompileControl,
) -> Result<EvaluatedDdlProperties, QueryServiceError> {
    let assignments = statement_property_assignments(properties, control)?;
    evaluate_ddl_properties_with_name_policy(
        DdlPropertyScope::Table,
        mode,
        source,
        declarations,
        &assignments,
        port,
        control,
        names,
    )
}

fn into_create_values(
    evaluated: EvaluatedDdlProperties,
    control: &dyn PureCompileControl,
) -> Result<Vec<(String, DdlPropertyValue)>, QueryServiceError> {
    let mut properties = Vec::with_capacity(evaluated.properties().len());
    for property in evaluated.into_properties() {
        checkpoint(control, 1)?;
        let value = match property.change {
            DdlPropertyChange::Set(value) | DdlPropertyChange::Reset(value) => value,
        };
        properties.push((property.name, value));
    }
    Ok(properties)
}

fn table_like<'a>(
    source: &str,
    statement: &'a TrinoCreateTable,
    control: &dyn PureCompileControl,
) -> Result<Option<&'a TrinoLikeClause>, QueryServiceError> {
    checkpoint(control, 1)?;
    if statement.save_mode == StatementSaveMode::Replace {
        return Err(unsupported(
            source,
            statement.span,
            "CREATE OR REPLACE TABLE is not supported",
        ));
    }
    if let [TrinoTableElement::Like(like)] = statement.elements.as_slice() {
        if like.properties_option == Some(LikePropertiesOption::Including) {
            return Err(unsupported(
                source,
                like.span,
                "CREATE TABLE LIKE INCLUDING PROPERTIES is not supported",
            ));
        }
        return Ok(Some(like));
    }
    if statement.elements.is_empty() {
        return Err(invariant("Prepared CREATE TABLE has no table elements"));
    }
    for element in &statement.elements {
        checkpoint(control, 1)?;
        match element {
            TrinoTableElement::Column(column) => check_column_shape(source, column)?,
            TrinoTableElement::Like(like) => {
                return Err(unsupported(
                    source,
                    like.span,
                    "CREATE TABLE LIKE must be the only table element",
                ));
            }
        }
    }
    Ok(None)
}

fn check_column_shape(
    source: &str,
    column: &TrinoColumnDefinition,
) -> Result<(), QueryServiceError> {
    if column.name.parts.len() != 1 {
        return Err(unsupported(
            source,
            column.name.span,
            "CREATE TABLE requires simple column names",
        ));
    }
    if !column.properties.is_empty() {
        return Err(unsupported(
            source,
            column.properties[0].span,
            "CREATE TABLE column properties are not supported",
        ));
    }
    Ok(())
}

fn lower_table_column(
    source: &str,
    column: &TrinoColumnDefinition,
    names: &mut BTreeSet<String>,
    control: &dyn PureCompileControl,
) -> Result<TableColumnSqlCommand, QueryServiceError> {
    let [name] = column.name.parts.as_slice() else {
        return Err(invariant(
            "Admitted CREATE TABLE column has a qualified name",
        ));
    };
    let data_type = lower_sql_type(&column.data_type, source, control)?;
    observe_bytes(&name.value, control)?;
    if !names.insert(name.value.to_lowercase()) {
        let written_name = if name.quoted {
            format!("\"{}\"", name.value.replace('"', "\"\""))
        } else {
            name.value.clone()
        };
        let error = AnalyzeError::duplicate_column_name(
            format!("Column name '{written_name}' specified more than once"),
            column.span,
        );
        return Err(QueryServiceError::from_user_error(
            error.to_user_error(Some(source)),
        ));
    }
    let default = match &column.default_value {
        None => None,
        Some(Expr::Literal(literal)) => {
            validate_bounded_varchar_default(source, literal, &data_type, control)?;
            Some(
                lower_command_literal(literal)
                    .map_err(|message| unsupported(source, literal.span, message))?,
            )
        }
        Some(_) => {
            return Err(invariant(
                "Prepared CREATE TABLE column default is not a literal",
            ));
        }
    };
    Ok(TableColumnSqlCommand {
        name: copy_string(&name.value, control)?,
        data_type,
        nullable: Some(column.nullable),
        default,
        comment: column
            .comment
            .as_deref()
            .map(|value| copy_string(value, control).map(CommandLiteral::String))
            .transpose()?,
    })
}

fn lower_create_table(
    source: &str,
    statement: &TrinoCreateTable,
    like: Option<&TrinoLikeClause>,
    properties: Vec<(String, DdlPropertyValue)>,
    control: &dyn PureCompileControl,
) -> Result<CreateTableSqlCommand, QueryServiceError> {
    let mut columns = Vec::with_capacity(statement.elements.len());
    let mut names = BTreeSet::new();
    for element in &statement.elements {
        checkpoint(control, 1)?;
        if let TrinoTableElement::Column(column) = element {
            columns.push(lower_table_column(source, column, &mut names, control)?);
        }
    }
    let comment = statement
        .comment
        .as_deref()
        .map(|comment| copy_string(comment, control))
        .transpose()?;
    for name in std::iter::once(&statement.name).chain(like.map(|like| &like.table_name)) {
        for part in &name.parts {
            checkpoint(control, 1)?;
            observe_bytes(&part.value, control)?;
        }
    }
    let name = lower_object_name(&statement.name);
    let like = like.map(|like| lower_object_name(&like.table_name));
    checkpoint(control, 0)?;
    Ok(CreateTableSqlCommand {
        if_not_exists: statement.save_mode == StatementSaveMode::Ignore,
        name,
        like,
        columns,
        comment,
        properties,
    })
}

/// Known capabilities only. The caller decides target existence from the
/// original scoped metadata before invoking property preparation.
pub fn validate_trino_create_table(
    source: &str,
    statement: &TrinoCreateTable,
    control: &dyn PureCompileControl,
) -> Result<(), QueryServiceError> {
    table_like(source, statement, control).map(|_| ())
}

/// Called after the target exists/IFNE decision on the original Root. An
/// accepted request retains this exact mutation session for publication.
/// The caller only opens a LIKE source after this property preparation.
pub fn prepare_trino_create_table(
    source: &str,
    statement: &TrinoCreateTable,
    mutation: &ConnectorCatalogMutationSession,
    port: &mut dyn DdlPropertyConstantPort,
    control: &dyn PureCompileControl,
) -> Result<CreateTableSqlCommand, QueryServiceError> {
    let like = table_like(source, statement, control)?;
    let declarations = mutation
        .table_property_declarations()
        .map_err(provider_error)?;
    let evaluated = evaluate_table_properties(
        source,
        &statement.properties,
        &declarations,
        DdlPropertyEvaluationMode::Create,
        &mut TableNames(mutation),
        port,
        control,
    )?;
    let properties = into_create_values(evaluated, control)?;
    lower_create_table(source, statement, like, properties, control)
}

/// Facts consumed before analysing the original CTAS source query. The
/// surrounding statement and query are still owned by their real caller.
#[derive(Debug)]
pub struct PreparedTrinoCtas<'a> {
    pub table: CreateTableSqlCommand,
    pub query: &'a Query,
}

pub fn validate_trino_ctas(
    source: &str,
    statement: &TrinoCreateTableAsSelect,
    control: &dyn PureCompileControl,
) -> Result<(), QueryServiceError> {
    checkpoint(control, 1)?;
    let refusal = if statement.save_mode == StatementSaveMode::Replace {
        "CREATE OR REPLACE TABLE AS is not supported"
    } else if statement.column_aliases.is_some() {
        "CREATE TABLE AS column aliases are not supported"
    } else if !statement.with_data {
        "CREATE TABLE AS WITH NO DATA is not supported"
    } else {
        return Ok(());
    };
    Err(unsupported(source, statement.span, refusal))
}

/// Caller inputs must be actual CTAS declarations and their same-generation
/// name policy. Existing ordinary CREATE (v2) and MV declarations cannot
/// supply the original staged CTAS v3 default; the CTAS provider issues it.
/// Apply only after the existing-target/IFNE decision, before query analysis.
pub fn prepare_trino_ctas<'a>(
    source: &str,
    statement: &'a TrinoCreateTableAsSelect,
    declarations: &[DdlPropertyDeclaration],
    names: &mut dyn DdlPropertyNamePolicy,
    port: &mut dyn DdlPropertyConstantPort,
    control: &dyn PureCompileControl,
) -> Result<PreparedTrinoCtas<'a>, QueryServiceError> {
    validate_trino_ctas(source, statement, control)?;
    if !declarations.iter().any(|property| {
        property.name() == "format_version"
            && *property.default_value() == DdlPropertyValue::Integer(3)
    }) {
        return Err(invariant(
            "Staged CTAS requires the original v3 format_version declaration",
        ));
    }
    let evaluated = evaluate_table_properties(
        source,
        &statement.properties,
        declarations,
        DdlPropertyEvaluationMode::Create,
        names,
        port,
        control,
    )?;
    let properties = into_create_values(evaluated, control)?;
    // Preserve the evaluated properties exactly. The real staging provider
    // must validate its v3 profile before source analysis or staging work.
    let comment = statement
        .comment
        .as_deref()
        .map(|value| copy_string(value, control))
        .transpose()?;
    for part in &statement.name.parts {
        checkpoint(control, 1)?;
        observe_bytes(&part.value, control)?;
    }
    let name = lower_object_name(&statement.name);
    checkpoint(control, 0)?;
    Ok(PreparedTrinoCtas {
        table: CreateTableSqlCommand {
            if_not_exists: statement.save_mode == StatementSaveMode::Ignore,
            name,
            like: None,
            columns: Vec::new(),
            comment,
            properties,
        },
        query: &statement.query,
    })
}

/// Freeze one real Table property operation from the original binding. No
/// provider publish is started here. The caller retains the same pin/lease.
pub fn prepare_trino_set_table_properties(
    source: &str,
    properties: &[StatementProperty],
    binding: &CatalogTablePropertyBinding,
    port: &mut dyn DdlPropertyConstantPort,
    control: &dyn PureCompileControl,
) -> Result<ConnectorCatalogMutationOperation, QueryServiceError> {
    checkpoint(control, 1)?;
    let mutation = binding.mutation();
    let declarations = mutation
        .table_property_declarations()
        .map_err(provider_error)?;
    let evaluated = evaluate_table_properties(
        source,
        properties,
        &declarations,
        DdlPropertyEvaluationMode::Alter,
        &mut TableNames(mutation),
        port,
        control,
    )?;
    let properties = into_table_changes(evaluated, control)?;
    Ok(ConnectorCatalogMutationOperation::SetTableProperties {
        table: binding.table().clone(),
        properties,
    })
}

type TablePropertyChanges = Arc<[(Arc<str>, DdlPropertyChange)]>;

fn into_table_changes(
    evaluated: EvaluatedDdlProperties,
    control: &dyn PureCompileControl,
) -> Result<TablePropertyChanges, QueryServiceError> {
    let mut properties = Vec::with_capacity(evaluated.properties().len());
    for property in evaluated.into_properties() {
        checkpoint(control, 1)?;
        properties.push((Arc::from(property.name), property.change));
    }
    checkpoint(control, 0)?;
    Ok(properties.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_constant_contract::ConstantValue;
    use novarocks_parser::{ast::TrinoStatement, parser::parse_statement};
    use novarocks_spi::connector::DdlPropertyType;
    use novarocks_sql::{
        compiler::{RequiredConstantTarget, SqlCompileControl},
        semantic::command::CommandLiteral,
    };
    use novarocks_types::schema::SqlType;

    use crate::sql::property_evaluation::{ClosedNames, DdlPropertyConstantError};

    struct DefaultOnly;
    impl DdlPropertyConstantPort for DefaultOnly {
        fn evaluate(
            &mut self,
            _: &Expr,
            _: RequiredConstantTarget,
        ) -> Result<ConstantValue, DdlPropertyConstantError> {
            panic!("DEFAULT and absence must not enter constant evaluation");
        }
    }

    fn declaration(
        name: &str,
        ty: DdlPropertyType,
        value: DdlPropertyValue,
    ) -> DdlPropertyDeclaration {
        DdlPropertyDeclaration::new(name, ty, value, false).unwrap()
    }

    #[test]
    fn bounded_character_default_rejections_point_to_the_literal() {
        let control = SqlCompileControl::unbounded();
        let create = "CREATE TABLE t (v VARCHAR(1) DEFAULT '𐐷é')";
        let alter = "ALTER TABLE t ADD COLUMN v VARCHAR(4) DEFAULT 3";
        let TrinoStatement::CreateTable(create_node) = parse_statement(create).unwrap() else {
            panic!("expected CREATE TABLE");
        };
        let TrinoStatement::AddColumn(alter_node) = parse_statement(alter).unwrap() else {
            panic!("expected ALTER TABLE ADD COLUMN");
        };
        let create_error = lower_create_table(create, &create_node, None, vec![], &control)
            .expect_err("two code points exceed VARCHAR(1)");
        let alter_error =
            crate::sql::trino_table_command::lower_trino_add_column(alter, &alter_node, &control)
                .expect_err("a numeric literal is invalid for bounded VARCHAR");

        for (error, source, literal) in [(create_error, create, "'𐐷é'"), (alter_error, alter, "3")]
        {
            let error = error.user_error().unwrap();
            assert_eq!(
                error.code().as_str(),
                "sql.admit.invalid_default_column_value"
            );
            assert_eq!(
                error.location().unwrap().column() as usize,
                source.rfind(literal).unwrap() + 1,
            );
        }
    }

    #[test]
    fn ordinary_create_keeps_column_defaults_and_like_separate_from_target_properties() {
        let control = SqlCompileControl::unbounded();
        let declarations = [declaration(
            "format_version",
            DdlPropertyType::Integer,
            DdlPropertyValue::Integer(2),
        )];
        let source = "CREATE TABLE IF NOT EXISTS lake.ns.t (\
                      \"Id\" bigint DEFAULT 7 NOT NULL COMMENT 'key', \
                      optional bigint DEFAULT NULL COMMENT 'id', bare bigint) \
                      COMMENT 'note' WITH (format_version = DEFAULT)";
        let TrinoStatement::CreateTable(statement) = parse_statement(source).unwrap() else {
            panic!("real ordinary CREATE");
        };
        let like = table_like(source, &statement, &control).unwrap();
        let evaluated = evaluate_table_properties(
            source,
            &statement.properties,
            &declarations,
            DdlPropertyEvaluationMode::Create,
            &mut ClosedNames,
            &mut DefaultOnly,
            &control,
        )
        .unwrap();
        let command = lower_create_table(
            source,
            &statement,
            like,
            into_create_values(evaluated, &control).unwrap(),
            &control,
        )
        .unwrap();
        assert!(command.if_not_exists);
        assert_eq!(command.name.parts, ["lake", "ns", "t"]);
        assert_eq!(command.comment.as_deref(), Some("note"));
        assert_eq!(
            command.properties,
            vec![("format_version".into(), DdlPropertyValue::Integer(2))]
        );
        assert!(command.like.is_none());
        assert_eq!(
            command.columns,
            vec![
                TableColumnSqlCommand {
                    name: "Id".into(),
                    data_type: SqlType::BigInt,
                    nullable: Some(false),
                    default: Some(CommandLiteral::Number("7".into())),
                    comment: Some(CommandLiteral::String("key".into())),
                },
                TableColumnSqlCommand {
                    name: "optional".into(),
                    data_type: SqlType::BigInt,
                    nullable: Some(true),
                    default: Some(CommandLiteral::Null),
                    comment: Some(CommandLiteral::String("id".into())),
                },
                TableColumnSqlCommand {
                    name: "bare".into(),
                    data_type: SqlType::BigInt,
                    nullable: Some(true),
                    default: None,
                    comment: None,
                },
            ]
        );

        let source = "CREATE TABLE lake.ns.t (LIKE lake.ns.src EXCLUDING PROPERTIES)";
        let TrinoStatement::CreateTable(statement) = parse_statement(source).unwrap() else {
            panic!("real ordinary CREATE LIKE");
        };
        let like = table_like(source, &statement, &control).unwrap();
        let evaluated = evaluate_table_properties(
            source,
            &statement.properties,
            &declarations,
            DdlPropertyEvaluationMode::Create,
            &mut ClosedNames,
            &mut DefaultOnly,
            &control,
        )
        .unwrap();
        let command = lower_create_table(
            source,
            &statement,
            like,
            into_create_values(evaluated, &control).unwrap(),
            &control,
        )
        .unwrap();
        assert_eq!(command.like.unwrap().parts, ["lake", "ns", "src"]);
        assert!(command.columns.is_empty());
        assert_eq!(
            command.properties,
            vec![("format_version".into(), DdlPropertyValue::Integer(2))]
        );
    }

    #[test]
    fn ordinary_create_reports_the_second_case_insensitive_column_definition() {
        for (source, duplicate, value) in [
            (
                "CREATE TABLE sqlp5_duplicate_column (id INT, id BIGINT)",
                "id BIGINT",
                "id",
            ),
            (
                r#"CREATE TABLE lake.ns.t ("Id" INT, "iD" BIGINT)"#,
                r#""iD" BIGINT"#,
                r#""iD""#,
            ),
        ] {
            let TrinoStatement::CreateTable(statement) = parse_statement(source).unwrap() else {
                panic!("real ordinary CREATE");
            };
            let control = SqlCompileControl::unbounded();
            let like = table_like(source, &statement, &control).unwrap();
            let error = lower_create_table(source, &statement, like, Vec::new(), &control)
                .expect_err("duplicate explicit column name must fail before connector creation");
            let error = error.user_error().unwrap();
            assert_eq!(error.code().as_str(), "sql.analyze.duplicate_column_name");
            assert_eq!(
                error.location().unwrap().column(),
                source.find(duplicate).unwrap() as u32 + 1
            );
            assert_eq!(
                error.message(),
                format!("Column name '{value}' specified more than once")
            );
        }
    }

    #[test]
    fn ctas_preparation_uses_staged_declarations_and_borrows_the_actual_query() {
        let source = "CREATE TABLE IF NOT EXISTS lake.ns.t COMMENT 'ctas'                       WITH (format_version = DEFAULT) AS (SELECT missing_column) WITH DATA";
        let TrinoStatement::CreateTableAsSelect(statement) = parse_statement(source).unwrap()
        else {
            panic!("real CTAS");
        };
        let declarations = [declaration(
            "format_version",
            DdlPropertyType::Integer,
            DdlPropertyValue::Integer(3),
        )];
        let control = SqlCompileControl::unbounded();
        let prepared = prepare_trino_ctas(
            source,
            &statement,
            &declarations,
            &mut ClosedNames,
            &mut DefaultOnly,
            &control,
        )
        .unwrap();
        assert!(std::ptr::eq(prepared.query, statement.query.as_ref()));
        assert!(prepared.table.if_not_exists);
        assert!(prepared.table.columns.is_empty());
        assert_eq!(prepared.table.name.parts, ["lake", "ns", "t"]);
        assert_eq!(prepared.table.comment.as_deref(), Some("ctas"));
        assert_eq!(
            prepared.table.properties,
            vec![("format_version".into(), DdlPropertyValue::Integer(3))]
        );
    }

    #[test]
    fn alter_named_default_freezes_reset_and_omits_unassigned_declarations() {
        let source = "ALTER TABLE lake.ns.t SET PROPERTIES gc_enabled = DEFAULT";
        let TrinoStatement::SetProperties(statement) = parse_statement(source).unwrap() else {
            panic!("real ALTER TABLE SET PROPERTIES");
        };
        let declarations = [
            declaration(
                "format_version",
                DdlPropertyType::Integer,
                DdlPropertyValue::Integer(2),
            ),
            declaration(
                "gc_enabled",
                DdlPropertyType::Boolean,
                DdlPropertyValue::Null,
            ),
        ];
        let control = SqlCompileControl::unbounded();
        let evaluated = evaluate_table_properties(
            source,
            &statement.properties,
            &declarations,
            DdlPropertyEvaluationMode::Alter,
            &mut ClosedNames,
            &mut DefaultOnly,
            &control,
        )
        .unwrap();
        let properties = into_table_changes(evaluated, &control).unwrap();
        assert_eq!(
            properties.as_ref(),
            &[(
                Arc::from("gc_enabled"),
                DdlPropertyChange::Reset(DdlPropertyValue::Null)
            )]
        );
    }

    #[test]
    fn unsupported_table_shapes_are_named_at_the_actual_node() {
        let control = SqlCompileControl::unbounded();
        for (source, message, anchor) in [
            (
                "CREATE TABLE lake.ns.t (LIKE lake.ns.src INCLUDING PROPERTIES)",
                "CREATE TABLE LIKE INCLUDING PROPERTIES is not supported",
                "LIKE",
            ),
            (
                "CREATE TABLE lake.ns.t (id bigint, LIKE lake.ns.src)",
                "CREATE TABLE LIKE must be the only table element",
                "LIKE",
            ),
            (
                "CREATE TABLE lake.ns.t (id bigint WITH (unsupported = true))",
                "CREATE TABLE column properties are not supported",
                "unsupported",
            ),
            (
                "CREATE TABLE lake.ns.t AS SELECT 1 WITH NO DATA",
                "CREATE TABLE AS WITH NO DATA is not supported",
                "CREATE",
            ),
        ] {
            let statement = parse_statement(source).unwrap();
            let (error, span) = match &statement {
                TrinoStatement::CreateTable(table) => (
                    validate_trino_create_table(source, table, &control).unwrap_err(),
                    match &table.elements[table.elements.len() - 1] {
                        TrinoTableElement::Like(like) => like.span,
                        TrinoTableElement::Column(column) => column.properties[0].span,
                    },
                ),
                TrinoStatement::CreateTableAsSelect(ctas) => (
                    validate_trino_ctas(source, ctas, &control).unwrap_err(),
                    ctas.span,
                ),
                _ => panic!("real CREATE"),
            };
            assert!(source[span.start()..span.end()].starts_with(anchor));
            assert_eq!(
                error.user_error(),
                Some(
                    &DmlAdmissionError::CreateTableUnsupportedForm
                        .to_user_error(source, span, message)
                )
            );
        }
    }
}
