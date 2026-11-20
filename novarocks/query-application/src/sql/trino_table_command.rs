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

//! Lower typed table statements to the existing command and binding owners.

use novarocks_parser::{Span, ast};
use novarocks_spi::connector::{
    ConnectorProviderId, DdlPropertyChange, DdlPropertyDeclaration, DdlPropertyValue,
};
use novarocks_sql::semantic::{
    CatalogSqlCommand, ObjectName, StatisticsSqlCommand,
    command::{
        AlterIcebergTableSqlCommand, ColumnPositionSql, CommandLiteral, IcebergColumnSqlAction,
        IcebergReferenceKindSql, IcebergReferenceSqlAction, IcebergSchemaSqlChange,
        IcebergTableSqlAction,
    },
};
use novarocks_type_contract::{CompilePhase, PureCompileControl};

use super::property_admit::DdlPropertyScope;
use super::property_evaluation::{
    DdlPropertyConstantPort, DdlPropertyEvaluationMode, EvaluatedDdlProperties,
    evaluate_ddl_properties, statement_property_assignments,
};
use super::table_procedure::{
    BoundTableProcedure, TableProcedureInputArgument, bind_arguments, resolve_table_procedure,
};
use super::trino_admission::unsupported_statement;
use super::{lower_command_literal, lower_object_name, lower_sql_type};
use crate::session_error::{QueryServiceError, QueryServiceErrorKind};

pub fn lower_trino_rename_table(
    source: &str,
    statement: &ast::TrinoRenameTable,
) -> Result<CatalogSqlCommand, QueryServiceError> {
    Err(unsupported_statement(
        source,
        statement.span,
        "ALTER TABLE RENAME TO",
    ))
}

pub fn lower_trino_add_column(
    source: &str,
    statement: &ast::TrinoAddColumn,
) -> Result<CatalogSqlCommand, QueryServiceError> {
    reject_existence_clause(
        source,
        statement.span,
        statement.table_exists || statement.column_not_exists,
        "ADD COLUMN with an existence condition",
    )?;
    let column = &statement.column;
    if !column.nullable {
        return Err(unsupported_statement(
            source,
            column.span,
            "ADD COLUMN NOT NULL",
        ));
    }
    if column.comment.is_some() || !column.properties.is_empty() {
        return Err(unsupported_statement(
            source,
            column.span,
            "ADD COLUMN with COMMENT or WITH",
        ));
    }
    let data_type = lower_sql_type(&column.data_type).map_err(invalid_value)?;
    let default = match &column.default_value {
        None => None,
        Some(ast::Expr::Literal(literal)) => {
            Some(lower_command_literal(literal).map_err(invalid_value)?)
        }
        Some(expression) => {
            return Err(unsupported_statement(
                source,
                expression.span(),
                "ADD COLUMN with an expression default",
            ));
        }
    };
    let position = match &statement.position {
        None | Some(ast::TrinoColumnPosition::Last) => ColumnPositionSql::Default,
        Some(ast::TrinoColumnPosition::First) => ColumnPositionSql::First,
        Some(ast::TrinoColumnPosition::After(after)) => {
            ColumnPositionSql::After(vec![after.value.clone()])
        }
    };
    Ok(schema_command(
        &statement.name,
        IcebergSchemaSqlChange::AddColumn {
            path: lower_object_name(&column.name).parts,
            data_type,
            nullable: Some(column.nullable),
            default,
            position,
        },
    ))
}

pub fn lower_trino_rename_column(
    source: &str,
    statement: &ast::TrinoRenameColumn,
) -> Result<CatalogSqlCommand, QueryServiceError> {
    reject_existence_clause(
        source,
        statement.span,
        statement.table_exists || statement.column_exists,
        "RENAME COLUMN with an existence condition",
    )?;
    Ok(schema_command(
        &statement.table,
        IcebergSchemaSqlChange::RenameColumn {
            from: lower_object_name(&statement.source).parts,
            to: vec![statement.target.value.clone()],
        },
    ))
}

pub fn lower_trino_drop_column(
    source: &str,
    statement: &ast::TrinoDropColumn,
) -> Result<CatalogSqlCommand, QueryServiceError> {
    reject_existence_clause(
        source,
        statement.span,
        statement.table_exists || statement.column_exists,
        "DROP COLUMN with an existence condition",
    )?;
    Ok(schema_command(
        &statement.table,
        IcebergSchemaSqlChange::DropColumn {
            path: lower_object_name(&statement.field).parts,
        },
    ))
}

pub fn lower_trino_set_column_type(
    source: &str,
    statement: &ast::TrinoSetColumnType,
) -> Result<CatalogSqlCommand, QueryServiceError> {
    reject_existence_clause(
        source,
        statement.span,
        statement.table_exists,
        "SET DATA TYPE with a table existence condition",
    )?;
    Ok(schema_command(
        &statement.table_name,
        IcebergSchemaSqlChange::ModifyColumn {
            path: lower_object_name(&statement.column_name).parts,
            data_type: lower_sql_type(&statement.data_type).map_err(invalid_value)?,
        },
    ))
}

pub fn lower_trino_drop_not_null(
    source: &str,
    statement: &ast::TrinoDropNotNullConstraint,
) -> Result<CatalogSqlCommand, QueryServiceError> {
    reject_existence_clause(
        source,
        statement.span,
        statement.table_exists,
        "DROP NOT NULL with a table existence condition",
    )?;
    Ok(schema_command(
        &statement.table,
        IcebergSchemaSqlChange::AlterColumn {
            path: vec![statement.column.value.clone()],
            action: IcebergColumnSqlAction::SetNullable(true),
        },
    ))
}

pub fn lower_trino_table_comment(
    source: &str,
    statement: &ast::TrinoComment,
) -> Result<CatalogSqlCommand, QueryServiceError> {
    match statement.kind {
        ast::CommentKind::Table | ast::CommentKind::Column => {}
        ast::CommentKind::View => {
            return Err(unsupported_statement(
                source,
                statement.span,
                "COMMENT ON VIEW",
            ));
        }
        ast::CommentKind::MaterializedView => {
            return Err(unsupported_statement(
                source,
                statement.span,
                "COMMENT ON MATERIALIZED VIEW",
            ));
        }
    }
    let Some(comment) = &statement.comment else {
        return Err(unsupported_statement(
            source,
            statement.span,
            if statement.kind == ast::CommentKind::Table {
                "COMMENT ON TABLE IS NULL"
            } else {
                "COMMENT ON COLUMN IS NULL"
            },
        ));
    };
    let mut table = lower_object_name(&statement.name);
    let action = if statement.kind == ast::CommentKind::Table {
        IcebergTableSqlAction::Comment(comment.clone())
    } else {
        let column = table
            .parts
            .pop()
            .filter(|_| !table.parts.is_empty())
            .ok_or_else(|| invalid_value("COMMENT ON COLUMN requires a table and a column"))?;
        IcebergTableSqlAction::Schema(IcebergSchemaSqlChange::AlterColumn {
            path: vec![column],
            action: IcebergColumnSqlAction::Comment(CommandLiteral::String(comment.clone())),
        })
    };
    Ok(table_command(table, action))
}

/// The caller takes declarations from the retained statistics session used by
/// its real ANALYZE action. Values come from the original constant evaluator.
pub fn prepare_trino_analyze(
    source: &str,
    statement: &ast::TrinoAnalyze,
    declarations: &[DdlPropertyDeclaration],
    constants: &mut dyn DdlPropertyConstantPort,
    control: &dyn PureCompileControl,
) -> Result<StatisticsSqlCommand, QueryServiceError> {
    let properties = evaluate_statement_properties(
        DdlPropertyScope::Analyze,
        source,
        &statement.properties,
        declarations,
        constants,
        control,
    )?;
    let columns = match optional_property(properties, "columns")? {
        None => None,
        Some(DdlPropertyValue::ArrayVarchar(columns)) => {
            let mut explicit = Vec::with_capacity(columns.len());
            for column in columns {
                check(control)?;
                explicit.push(column.ok_or_else(|| {
                    let Some(expression) = statement
                        .properties
                        .iter()
                        .find(|property| property.name.value.eq_ignore_ascii_case("columns"))
                        .and_then(|property| property.value.as_ref())
                    else {
                        return invariant("ANALYZE columns provider default contains NULL");
                    };
                    QueryServiceError::from_user_error(DdlPropertyScope::Analyze.to_user_error(
                        source,
                        expression.span(),
                        "Invalid null value in analyze columns property",
                    ))
                })?);
            }
            Some(explicit)
        }
        Some(_) => {
            return Err(invariant(
                "ANALYZE columns declaration returned a different type",
            ));
        }
    };
    Ok(StatisticsSqlCommand::AnalyzeTable {
        table: lower_object_name(&statement.table_name),
        columns,
    })
}

pub fn validate_trino_create_branch(
    source: &str,
    statement: &ast::TrinoCreateBranch,
) -> Result<(), QueryServiceError> {
    if let Some(from) = &statement.from_branch {
        return Err(unsupported_statement(
            source,
            from.span,
            "CREATE BRANCH FROM",
        ));
    }
    Ok(())
}

/// Use declarations from the same mutation generation selected for the ref action.
pub fn prepare_trino_create_branch(
    source: &str,
    statement: &ast::TrinoCreateBranch,
    declarations: &[DdlPropertyDeclaration],
    constants: &mut dyn DdlPropertyConstantPort,
    control: &dyn PureCompileControl,
) -> Result<CatalogSqlCommand, QueryServiceError> {
    validate_trino_create_branch(source, statement)?;
    let properties = evaluate_statement_properties(
        DdlPropertyScope::Branch,
        source,
        &statement.properties,
        declarations,
        constants,
        control,
    )?;
    let snapshot_id = match optional_property(properties, "snapshot_id")? {
        None => None,
        Some(DdlPropertyValue::Bigint(snapshot_id)) => Some(snapshot_id),
        Some(_) => {
            return Err(invariant(
                "branch snapshot_id declaration returned a different type",
            ));
        }
    };
    Ok(table_command(
        lower_object_name(&statement.table_name),
        IcebergTableSqlAction::Reference(IcebergReferenceSqlAction::Create {
            kind: IcebergReferenceKindSql::Branch,
            name: statement.branch_name.value.clone(),
            if_not_exists: statement.save_mode == ast::StatementSaveMode::Ignore,
            or_replace: statement.save_mode == ast::StatementSaveMode::Replace,
            snapshot_id,
        }),
    ))
}

pub fn lower_trino_drop_branch(statement: &ast::TrinoDropBranch) -> CatalogSqlCommand {
    table_command(
        lower_object_name(&statement.table_name),
        IcebergTableSqlAction::Reference(IcebergReferenceSqlAction::Drop {
            kind: IcebergReferenceKindSql::Branch,
            name: statement.branch_name.value.clone(),
            if_exists: statement.exists,
        }),
    )
}

pub fn lower_trino_fast_forward_branch(
    source: &str,
    statement: &ast::TrinoFastForwardBranch,
) -> Result<CatalogSqlCommand, QueryServiceError> {
    Err(unsupported_statement(
        source,
        statement.span,
        "FAST FORWARD BRANCH",
    ))
}

pub fn lower_trino_show_branches(
    source: &str,
    statement: &ast::TrinoShowBranches,
) -> Result<CatalogSqlCommand, QueryServiceError> {
    Err(unsupported_statement(
        source,
        statement.span,
        "SHOW BRANCHES",
    ))
}

/// The original target owner supplies the exact selected provider. The result
/// retains the procedure's typed binding and any borrowed write input. The
/// procedure executor still owns defaults, time policy and the actual effect.
pub fn bind_trino_table_execute<'a>(
    source: &str,
    statement: &'a ast::TrinoTableExecute,
    provider: &ConnectorProviderId,
    constants: &mut dyn DdlPropertyConstantPort,
    catalog: &str,
    control: &dyn PureCompileControl,
) -> Result<BoundTableProcedure<'a>, QueryServiceError> {
    if let Some(branch) = &statement.table.branch {
        return Err(unsupported_statement(
            source,
            branch.span,
            "ALTER TABLE EXECUTE on a branch",
        ));
    }
    if let Some(period) = &statement.table.query_period {
        return Err(unsupported_statement(
            source,
            period.span,
            "ALTER TABLE EXECUTE with a query period",
        ));
    }
    let procedure = resolve_table_procedure(provider, &statement.procedure_name)?;
    if let Some(selection) = &statement.selection {
        use super::table_procedure::TableProcedureKind;
        if matches!(
            procedure.kind(),
            TableProcedureKind::OptimizeManifests
                | TableProcedureKind::ExpireSnapshots
                | TableProcedureKind::RemoveOrphanFiles
        ) {
            return Err(super::trino_admission::unsupported_statement_detail(
                source,
                statement.span,
                format!("WHERE not supported for procedure {}", procedure.name()),
            ));
        }
        return Err(unsupported_statement(
            source,
            selection.span(),
            "ALTER TABLE EXECUTE with WHERE",
        ));
    }
    let mut arguments = Vec::with_capacity(statement.arguments.len());
    for argument in &statement.arguments {
        check(control)?;
        arguments.push(TableProcedureInputArgument::from(argument));
    }
    bind_arguments(
        procedure,
        &arguments,
        source,
        statement.span,
        constants,
        catalog,
        control,
    )
}

fn evaluate_statement_properties(
    scope: DdlPropertyScope,
    source: &str,
    properties: &[ast::StatementProperty],
    declarations: &[DdlPropertyDeclaration],
    constants: &mut dyn DdlPropertyConstantPort,
    control: &dyn PureCompileControl,
) -> Result<EvaluatedDdlProperties, QueryServiceError> {
    let assignments = statement_property_assignments(properties, control)?;
    evaluate_ddl_properties(
        scope,
        DdlPropertyEvaluationMode::Create,
        source,
        declarations,
        &assignments,
        constants,
        control,
    )
}

fn optional_property(
    properties: EvaluatedDdlProperties,
    name: &str,
) -> Result<Option<DdlPropertyValue>, QueryServiceError> {
    let mut properties = properties.into_properties().into_iter();
    let Some(property) = properties.next() else {
        return Ok(None);
    };
    if property.name != name || properties.next().is_some() {
        return Err(invariant(
            "statement properties have no corresponding command field",
        ));
    }
    Ok(Some(match property.change {
        DdlPropertyChange::Set(value) | DdlPropertyChange::Reset(value) => value,
    }))
}

fn reject_existence_clause(
    source: &str,
    span: Span,
    conditional: bool,
    subject: &str,
) -> Result<(), QueryServiceError> {
    if conditional {
        return Err(unsupported_statement(source, span, subject));
    }
    Ok(())
}

fn schema_command(name: &ast::ObjectName, schema: IcebergSchemaSqlChange) -> CatalogSqlCommand {
    table_command(
        lower_object_name(name),
        IcebergTableSqlAction::Schema(schema),
    )
}

fn table_command(table: ObjectName, action: IcebergTableSqlAction) -> CatalogSqlCommand {
    CatalogSqlCommand::AlterIcebergTable(AlterIcebergTableSqlCommand { table, action })
}

fn check(control: &dyn PureCompileControl) -> Result<(), QueryServiceError> {
    control
        .checkpoint(CompilePhase::Validate, 1)
        .map_err(QueryServiceError::from_compile_control)
}

fn invariant(message: &str) -> QueryServiceError {
    QueryServiceError::new(QueryServiceErrorKind::Internal, message)
}

fn invalid_value(message: impl Into<String>) -> QueryServiceError {
    QueryServiceError::new(QueryServiceErrorKind::InvalidValue, message)
}
