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

use std::collections::{BTreeMap, BTreeSet};

use crate::query_execution::maintenance::MaintenanceRequestContext;
use chrono::{DateTime, NaiveDateTime, Utc};
use novarocks_sql::semantic::command::{
    CommandLiteral, ExpireSnapshotsOptionSql, MaintenanceSqlCommand, MaintenanceValueSql,
    ProcedureArgumentModeSql, SortDirectionSql,
};
use novarocks_types::naming::normalize_identifier;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ParsedMaintenanceStatement {
    Execute {
        name_parts: Vec<String>,
        action: ParsedMaintenanceAction,
    },
    SubmitOptimize {
        name_parts: Vec<String>,
    },
    ShowOptimize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ParsedMaintenanceAction {
    RewriteDataFiles {
        options: BTreeMap<String, String>,
        branch: Option<String>,
        where_clause: Option<String>,
    },
    RewriteManifests {
        use_caching: Option<bool>,
        spec_id: Option<i32>,
    },
    ExpireSnapshots {
        older_than_ms: Option<i64>,
        retain_last: Option<u32>,
    },
    RemoveOrphanFiles {
        older_than_ms: i64,
    },
    RewritePositionDeleteFiles {
        options: BTreeMap<String, String>,
        where_clause: Option<String>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ParsedShowOptimize {
    pub(super) catalog: Option<String>,
    pub(super) database: Option<String>,
    pub(super) table_name: Option<String>,
    pub(super) order_by_create_time_desc: bool,
    pub(super) limit: Option<usize>,
}

/// Lowers a syntax-only maintenance statement into the existing maintenance
/// admission DTOs without reparsing SQL text.
pub(crate) fn lower_typed_maintenance_statement(
    statement: &novarocks_parser::ast::MaintenanceStatement,
    context: MaintenanceRequestContext<'_>,
) -> Result<ParsedMaintenanceStatement, String> {
    use novarocks_parser::ast::MaintenanceStatement;

    match statement {
        MaintenanceStatement::Call(call) => lower_typed_call_action(call, context),
        MaintenanceStatement::Optimize(optimize) => {
            Ok(ParsedMaintenanceStatement::SubmitOptimize {
                name_parts: normalize_typed_object_name(&optimize.table)?,
            })
        }
        MaintenanceStatement::RewriteManifests(rewrite) => {
            let name_parts = normalize_typed_object_name(&rewrite.table)?;
            reject_branch_suffix(&name_parts, "REWRITE MANIFESTS")?;
            Ok(ParsedMaintenanceStatement::Execute {
                name_parts,
                action: ParsedMaintenanceAction::RewriteManifests {
                    use_caching: None,
                    spec_id: None,
                },
            })
        }
        MaintenanceStatement::ExpireSnapshots(expire) => lower_typed_expire_snapshots(expire),
        MaintenanceStatement::RemoveOrphanFiles(remove) => lower_typed_remove_orphan_files(remove),
        MaintenanceStatement::ShowOptimize(show) => {
            let _ = lower_typed_show_optimize(show)?;
            Ok(ParsedMaintenanceStatement::ShowOptimize)
        }
    }
}

/// Lowers the Query-Application semantic command into the maintenance
/// product's admitted DTOs. This is deliberately a value-to-value mapping;
/// no parser AST is reconstructed or accepted at this boundary.
pub(crate) fn lower_semantic_maintenance_statement(
    statement: &MaintenanceSqlCommand,
    context: MaintenanceRequestContext<'_>,
) -> Result<ParsedMaintenanceStatement, String> {
    match statement {
        MaintenanceSqlCommand::Call {
            procedure,
            arguments,
            argument_mode,
        } => lower_semantic_call(procedure, arguments, *argument_mode, context),
        MaintenanceSqlCommand::Optimize { table } => {
            Ok(ParsedMaintenanceStatement::SubmitOptimize {
                name_parts: normalize_semantic_object_name(table)?,
            })
        }
        MaintenanceSqlCommand::RewriteManifests { table } => {
            let name_parts = normalize_semantic_object_name(table)?;
            reject_branch_suffix(&name_parts, "REWRITE MANIFESTS")?;
            Ok(ParsedMaintenanceStatement::Execute {
                name_parts,
                action: ParsedMaintenanceAction::RewriteManifests {
                    use_caching: None,
                    spec_id: None,
                },
            })
        }
        MaintenanceSqlCommand::ExpireSnapshots { table, options } => {
            let name_parts = normalize_semantic_object_name(table)?;
            reject_branch_suffix(&name_parts, "EXPIRE SNAPSHOTS")?;
            let mut older_than_ms = None;
            let mut retain_last = None;
            for option in options {
                match option {
                    ExpireSnapshotsOptionSql::OlderThan(value) => {
                        if older_than_ms.is_some() {
                            return Err("EXPIRE SNAPSHOTS: duplicate OLDER THAN clause".to_string());
                        }
                        older_than_ms = Some(lower_semantic_expire_timestamp_ms(value)?);
                    }
                    ExpireSnapshotsOptionSql::RetainLast(value) => {
                        if retain_last.is_some() {
                            return Err(
                                "EXPIRE SNAPSHOTS: duplicate RETAIN LAST clause".to_string()
                            );
                        }
                        retain_last = Some(lower_semantic_expire_retain_last(value)?);
                    }
                }
            }
            if older_than_ms.is_none() && retain_last.is_none() {
                return Err(
                    "EXPIRE SNAPSHOTS requires at least OLDER THAN or RETAIN LAST clause"
                        .to_string(),
                );
            }
            Ok(ParsedMaintenanceStatement::Execute {
                name_parts,
                action: ParsedMaintenanceAction::ExpireSnapshots {
                    older_than_ms,
                    retain_last,
                },
            })
        }
        MaintenanceSqlCommand::RemoveOrphanFiles { table, older_than } => {
            let name_parts = normalize_semantic_object_name(table)?;
            reject_branch_suffix(&name_parts, "REMOVE ORPHAN FILES")?;
            Ok(ParsedMaintenanceStatement::Execute {
                name_parts,
                action: ParsedMaintenanceAction::RemoveOrphanFiles {
                    older_than_ms: lower_semantic_expire_timestamp_ms(older_than)?,
                },
            })
        }
        MaintenanceSqlCommand::ShowOptimize(statement) => {
            let _ = lower_semantic_show_optimize(statement)?;
            Ok(ParsedMaintenanceStatement::ShowOptimize)
        }
    }
}

pub(crate) fn lower_semantic_show_optimize(
    statement: &novarocks_sql::semantic::command::ShowOptimizeSqlCommand,
) -> Result<ParsedShowOptimize, String> {
    let (catalog, database) = match statement.from.as_ref() {
        Some(name) => match normalize_semantic_object_name(name)?.as_slice() {
            [database] => (None, Some(database.clone())),
            [catalog, database] => (Some(catalog.clone()), Some(database.clone())),
            _ => {
                return Err(
                    "SHOW ALTER TABLE OPTIMIZE FROM only supports db or catalog.db".to_string(),
                );
            }
        },
        None => (None, None),
    };
    let table_name = match statement.filter.as_ref() {
        Some(filter) => {
            if !filter.column.eq_ignore_ascii_case("TableName") {
                return Err(
                    "SHOW ALTER TABLE OPTIMIZE only supports WHERE TableName = '...'".to_string(),
                );
            }
            let CommandLiteral::String(value) = &filter.value else {
                return Err(
                    "SHOW ALTER TABLE OPTIMIZE only supports WHERE TableName = '...'".to_string(),
                );
            };
            Some(normalize_identifier(value)?)
        }
        None => None,
    };
    let order_by_create_time_desc = match statement.order_by.as_ref() {
        Some(order_by) => {
            if !order_by.column.eq_ignore_ascii_case("CreateTime") {
                return Err(
                    "SHOW ALTER TABLE OPTIMIZE only supports ORDER BY CreateTime".to_string(),
                );
            }
            matches!(order_by.direction, Some(SortDirectionSql::Desc))
        }
        None => false,
    };
    let limit = match statement.limit.as_ref() {
        Some(CommandLiteral::Number(value)) => Some(
            value
                .parse::<usize>()
                .map_err(|error| format!("parse SHOW ALTER TABLE OPTIMIZE LIMIT: {error}"))?,
        ),
        Some(_) => return Err("SHOW ALTER TABLE OPTIMIZE LIMIT expects number".to_string()),
        None => None,
    };
    Ok(ParsedShowOptimize {
        catalog,
        database,
        table_name,
        order_by_create_time_desc,
        limit,
    })
}

fn lower_semantic_call(
    procedure: &novarocks_sql::semantic::ObjectName,
    arguments: &[novarocks_sql::semantic::command::ProcedureArgumentSql],
    declared_mode: ProcedureArgumentModeSql,
    context: MaintenanceRequestContext<'_>,
) -> Result<ParsedMaintenanceStatement, String> {
    let name_parts = normalize_semantic_object_name(procedure)?;
    let [catalog, namespace, procedure] = name_parts.as_slice() else {
        return Err("CALL procedure name must be catalog.system.procedure".to_string());
    };
    if namespace != "system" {
        return Err("Iceberg procedures must use system namespace".to_string());
    }
    let args = arguments
        .iter()
        .map(|argument| {
            Ok(ProcedureArg {
                name: argument
                    .name
                    .as_ref()
                    .map(|name| normalize_identifier(name))
                    .transpose()?,
                value: lower_semantic_procedure_value(&argument.value)?,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let mode = classify_arg_mode(&args)?;
    let declared_mode = match declared_mode {
        ProcedureArgumentModeSql::Empty => ProcedureArgMode::Empty,
        ProcedureArgumentModeSql::Named => ProcedureArgMode::Named,
        ProcedureArgumentModeSql::Positional => ProcedureArgMode::Positional,
    };
    if mode != declared_mode {
        return Err("CALL procedure argument mode does not match arguments".to_string());
    }
    ensure_no_duplicate_named_args(&args)?;
    lower_call_procedure(
        &CallProcedure {
            catalog: catalog.clone(),
            procedure: procedure.clone(),
            args,
            mode,
        },
        context,
    )
}

fn lower_semantic_procedure_value(
    value: &MaintenanceValueSql,
) -> Result<ProcedureArgValue, String> {
    match value {
        MaintenanceValueSql::Literal(value) => lower_semantic_procedure_literal(value),
        MaintenanceValueSql::Timestamp(CommandLiteral::String(value)) => {
            parse_timestamp_millis(value)
                .map(ProcedureArgValue::TimestampMillis)
                .map_err(|error| format!("CALL procedure invalid TIMESTAMP literal: {error}"))
        }
        MaintenanceValueSql::Timestamp(_) => {
            Err("CALL procedure TIMESTAMP expects a single quoted string".to_string())
        }
        MaintenanceValueSql::Map(entries) => {
            let mut values = BTreeMap::new();
            for (key, value) in entries {
                let (CommandLiteral::String(key), CommandLiteral::String(value)) = (key, value)
                else {
                    return Err(
                        "CALL procedure map key/value expects a single quoted string".to_string(),
                    );
                };
                if values.insert(key.clone(), value.clone()).is_some() {
                    return Err(format!("duplicate CALL procedure map key '{key}'"));
                }
            }
            Ok(ProcedureArgValue::StringMap(values))
        }
    }
}

fn lower_semantic_procedure_literal(value: &CommandLiteral) -> Result<ProcedureArgValue, String> {
    match value {
        CommandLiteral::String(value) => Ok(ProcedureArgValue::String(value.clone())),
        CommandLiteral::Number(value) => value
            .parse::<i64>()
            .map(ProcedureArgValue::Integer)
            .map_err(|error| format!("CALL procedure invalid integer argument '{value}': {error}")),
        CommandLiteral::Bool(value) => Ok(ProcedureArgValue::Boolean(*value)),
        CommandLiteral::Null => Ok(ProcedureArgValue::Null),
        CommandLiteral::Binary(_) => Err("CALL procedure unsupported argument value".to_string()),
    }
}

fn lower_semantic_expire_timestamp_ms(value: &MaintenanceValueSql) -> Result<i64, String> {
    let MaintenanceValueSql::Literal(value) = value else {
        return Err(
            "EXPIRE SNAPSHOTS: expected timestamp literal (quoted string or integer)".to_string(),
        );
    };
    match value {
        CommandLiteral::String(value) => {
            if let Ok(value) = DateTime::parse_from_rfc3339(value) {
                return Ok(value.with_timezone(&Utc).timestamp_millis());
            }
            if let Ok(value) = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S") {
                return Ok(value.and_utc().timestamp_millis());
            }
            Err(format!(
                "EXPIRE SNAPSHOTS: cannot parse timestamp '{value}'; expected RFC 3339 (e.g. '2026-04-01T00:00:00Z') or 'YYYY-MM-DD HH:MM:SS'"
            ))
        }
        CommandLiteral::Number(value) => value.parse::<i64>().map_err(|error| {
            format!("EXPIRE SNAPSHOTS: invalid epoch-ms integer '{value}': {error}")
        }),
        _ => Err(
            "EXPIRE SNAPSHOTS: expected timestamp literal (quoted string or integer)".to_string(),
        ),
    }
}

fn lower_semantic_expire_retain_last(value: &MaintenanceValueSql) -> Result<u32, String> {
    let MaintenanceValueSql::Literal(CommandLiteral::Number(value)) = value else {
        return Err("EXPIRE SNAPSHOTS: expected integer for RETAIN LAST".to_string());
    };
    let value = value.parse::<u64>().map_err(|error| {
        format!("EXPIRE SNAPSHOTS: invalid RETAIN LAST value '{value}': {error}")
    })?;
    if value == 0 {
        return Err("EXPIRE SNAPSHOTS: RETAIN LAST must be >= 1".to_string());
    }
    value
        .try_into()
        .map_err(|_| "EXPIRE SNAPSHOTS: RETAIN LAST value too large".to_string())
}

fn normalize_semantic_object_name(
    name: &novarocks_sql::semantic::ObjectName,
) -> Result<Vec<String>, String> {
    name.parts
        .iter()
        .map(|part| normalize_identifier(part))
        .collect()
}

/// Lowers typed `SHOW ALTER TABLE OPTIMIZE` presentation clauses.
pub(crate) fn lower_typed_show_optimize(
    statement: &novarocks_parser::ast::ShowAlterTableOptimize,
) -> Result<ParsedShowOptimize, String> {
    use novarocks_parser::ast::{LiteralKind, SortDirection};

    let (catalog, database) = match statement.from.as_ref() {
        Some(name) => match normalize_typed_object_name(name)?.as_slice() {
            [database] => (None, Some(database.clone())),
            [catalog, database] => (Some(catalog.clone()), Some(database.clone())),
            _ => {
                return Err(
                    "SHOW ALTER TABLE OPTIMIZE FROM only supports db or catalog.db".to_string(),
                );
            }
        },
        None => (None, None),
    };

    let table_name = match statement.filter.as_ref() {
        Some(filter) => {
            if !filter.column.value.eq_ignore_ascii_case("TableName") {
                return Err(
                    "SHOW ALTER TABLE OPTIMIZE only supports WHERE TableName = '...'".to_string(),
                );
            }
            let LiteralKind::String(value) = &filter.value.kind else {
                return Err(
                    "SHOW ALTER TABLE OPTIMIZE only supports WHERE TableName = '...'".to_string(),
                );
            };
            Some(normalize_identifier(value)?)
        }
        None => None,
    };

    let order_by_create_time_desc = match statement.order_by.as_ref() {
        Some(order_by) => {
            if !order_by.column.value.eq_ignore_ascii_case("CreateTime") {
                return Err(
                    "SHOW ALTER TABLE OPTIMIZE only supports ORDER BY CreateTime".to_string(),
                );
            }
            matches!(order_by.direction, Some(SortDirection::Desc))
        }
        None => false,
    };

    let limit = match statement.limit.as_ref() {
        Some(limit) => match &limit.kind {
            LiteralKind::Number(value) => Some(
                value
                    .parse::<usize>()
                    .map_err(|error| format!("parse SHOW ALTER TABLE OPTIMIZE LIMIT: {error}"))?,
            ),
            _ => {
                return Err("SHOW ALTER TABLE OPTIMIZE LIMIT expects number".to_string());
            }
        },
        None => None,
    };

    Ok(ParsedShowOptimize {
        catalog,
        database,
        table_name,
        order_by_create_time_desc,
        limit,
    })
}

/// Whether a typed maintenance statement originated from the Spark procedure
/// spelling used by the existing execution path.
pub(crate) const fn is_typed_spark_maintenance_call(
    statement: &novarocks_parser::ast::MaintenanceStatement,
) -> bool {
    matches!(
        statement,
        novarocks_parser::ast::MaintenanceStatement::Call(_)
    )
}

fn lower_typed_expire_snapshots(
    statement: &novarocks_parser::ast::ExpireSnapshots,
) -> Result<ParsedMaintenanceStatement, String> {
    let name_parts = normalize_typed_object_name(&statement.table)?;
    reject_branch_suffix(&name_parts, "EXPIRE SNAPSHOTS")?;

    let mut older_than_ms = None;
    let mut retain_last = None;
    for option in &statement.options {
        match option {
            novarocks_parser::ast::ExpireSnapshotsOption::OlderThan { value, .. } => {
                if older_than_ms.is_some() {
                    return Err("EXPIRE SNAPSHOTS: duplicate OLDER THAN clause".to_string());
                }
                older_than_ms = Some(lower_typed_expire_timestamp_ms(value)?);
            }
            novarocks_parser::ast::ExpireSnapshotsOption::RetainLast { value, .. } => {
                if retain_last.is_some() {
                    return Err("EXPIRE SNAPSHOTS: duplicate RETAIN LAST clause".to_string());
                }
                retain_last = Some(lower_typed_expire_retain_last(value)?);
            }
        }
    }
    if older_than_ms.is_none() && retain_last.is_none() {
        return Err(
            "EXPIRE SNAPSHOTS requires at least OLDER THAN or RETAIN LAST clause".to_string(),
        );
    }
    Ok(ParsedMaintenanceStatement::Execute {
        name_parts,
        action: ParsedMaintenanceAction::ExpireSnapshots {
            older_than_ms,
            retain_last,
        },
    })
}

fn lower_typed_remove_orphan_files(
    statement: &novarocks_parser::ast::RemoveOrphanFiles,
) -> Result<ParsedMaintenanceStatement, String> {
    let name_parts = normalize_typed_object_name(&statement.table)?;
    reject_branch_suffix(&name_parts, "REMOVE ORPHAN FILES")?;
    Ok(ParsedMaintenanceStatement::Execute {
        name_parts,
        action: ParsedMaintenanceAction::RemoveOrphanFiles {
            older_than_ms: lower_typed_expire_timestamp_ms(&statement.older_than)?,
        },
    })
}

fn lower_typed_expire_timestamp_ms(
    value: &novarocks_parser::ast::MaintenanceValue,
) -> Result<i64, String> {
    use novarocks_parser::ast::{LiteralKind, MaintenanceValue};

    match value {
        MaintenanceValue::Literal(literal) => match &literal.kind {
            LiteralKind::String(value) => {
                if let Ok(value) = DateTime::parse_from_rfc3339(value) {
                    return Ok(value.with_timezone(&Utc).timestamp_millis());
                }
                if let Ok(value) = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S") {
                    return Ok(value.and_utc().timestamp_millis());
                }
                Err(format!(
                    "EXPIRE SNAPSHOTS: cannot parse timestamp '{value}'; expected RFC 3339 (e.g. '2026-04-01T00:00:00Z') or 'YYYY-MM-DD HH:MM:SS'"
                ))
            }
            LiteralKind::Number(value) => value.parse::<i64>().map_err(|error| {
                format!("EXPIRE SNAPSHOTS: invalid epoch-ms integer '{value}': {error}")
            }),
            _ => Err(
                "EXPIRE SNAPSHOTS: expected timestamp literal (quoted string or integer)"
                    .to_string(),
            ),
        },
        _ => Err(
            "EXPIRE SNAPSHOTS: expected timestamp literal (quoted string or integer)".to_string(),
        ),
    }
}

fn lower_typed_expire_retain_last(
    value: &novarocks_parser::ast::MaintenanceValue,
) -> Result<u32, String> {
    use novarocks_parser::ast::{LiteralKind, MaintenanceValue};

    let MaintenanceValue::Literal(literal) = value else {
        return Err("EXPIRE SNAPSHOTS: expected integer for RETAIN LAST".to_string());
    };
    let LiteralKind::Number(value) = &literal.kind else {
        return Err("EXPIRE SNAPSHOTS: expected integer for RETAIN LAST".to_string());
    };
    let value = value.parse::<u64>().map_err(|error| {
        format!("EXPIRE SNAPSHOTS: invalid RETAIN LAST value '{value}': {error}")
    })?;
    if value == 0 {
        return Err("EXPIRE SNAPSHOTS: RETAIN LAST must be >= 1".to_string());
    }
    value
        .try_into()
        .map_err(|_| "EXPIRE SNAPSHOTS: RETAIN LAST value too large".to_string())
}

fn lower_typed_call_action(
    call: &novarocks_parser::ast::CallStatement,
    context: MaintenanceRequestContext<'_>,
) -> Result<ParsedMaintenanceStatement, String> {
    let name_parts = normalize_typed_object_name(&call.procedure)?;
    let [catalog, namespace, procedure] = name_parts.as_slice() else {
        return Err("CALL procedure name must be catalog.system.procedure".to_string());
    };
    if namespace != "system" {
        return Err("Iceberg procedures must use system namespace".to_string());
    }

    let args = call
        .arguments
        .iter()
        .map(lower_typed_procedure_arg)
        .collect::<Result<Vec<_>, _>>()?;
    let mode = classify_arg_mode(&args)?;
    let declared_mode = match call.argument_mode {
        novarocks_parser::ast::ProcedureArgumentMode::Empty => ProcedureArgMode::Empty,
        novarocks_parser::ast::ProcedureArgumentMode::Named => ProcedureArgMode::Named,
        novarocks_parser::ast::ProcedureArgumentMode::Positional => ProcedureArgMode::Positional,
    };
    if mode != declared_mode {
        return Err("CALL procedure argument mode does not match arguments".to_string());
    }
    ensure_no_duplicate_named_args(&args)?;
    lower_call_procedure(
        &CallProcedure {
            catalog: catalog.clone(),
            procedure: procedure.clone(),
            args,
            mode,
        },
        context,
    )
}

fn lower_typed_procedure_arg(
    argument: &novarocks_parser::ast::ProcedureArgument,
) -> Result<ProcedureArg, String> {
    Ok(ProcedureArg {
        name: argument
            .name
            .as_ref()
            .map(|name| normalize_identifier(&name.value))
            .transpose()?,
        value: lower_typed_procedure_value(&argument.value)?,
    })
}

fn lower_typed_procedure_value(
    value: &novarocks_parser::ast::MaintenanceValue,
) -> Result<ProcedureArgValue, String> {
    use novarocks_parser::ast::{LiteralKind, MaintenanceValue};

    match value {
        MaintenanceValue::Literal(literal) => match &literal.kind {
            LiteralKind::String(value) => Ok(ProcedureArgValue::String(value.clone())),
            LiteralKind::Number(value) => value
                .parse::<i64>()
                .map(ProcedureArgValue::Integer)
                .map_err(|error| {
                    format!("CALL procedure invalid integer argument '{value}': {error}")
                }),
            LiteralKind::Boolean(value) => Ok(ProcedureArgValue::Boolean(*value)),
            LiteralKind::Null => Ok(ProcedureArgValue::Null),
            LiteralKind::HexString(_) => {
                Err("CALL procedure unsupported argument value".to_string())
            }
        },
        MaintenanceValue::Timestamp { value, .. } => {
            let LiteralKind::String(value) = &value.kind else {
                return Err("CALL procedure TIMESTAMP expects a single quoted string".to_string());
            };
            parse_timestamp_millis(value)
                .map(ProcedureArgValue::TimestampMillis)
                .map_err(|error| format!("CALL procedure invalid TIMESTAMP literal: {error}"))
        }
        MaintenanceValue::Map(map) => {
            let mut values = BTreeMap::new();
            for entry in &map.entries {
                let (LiteralKind::String(key), LiteralKind::String(value)) =
                    (&entry.key.kind, &entry.value.kind)
                else {
                    return Err(
                        "CALL procedure map key/value expects a single quoted string".to_string(),
                    );
                };
                if values.insert(key.clone(), value.clone()).is_some() {
                    return Err(format!("duplicate CALL procedure map key '{key}'"));
                }
            }
            Ok(ProcedureArgValue::StringMap(values))
        }
    }
}

fn normalize_typed_object_name(
    name: &novarocks_parser::ast::ObjectName,
) -> Result<Vec<String>, String> {
    name.parts
        .iter()
        .map(|part| normalize_identifier(&part.value))
        .collect()
}

fn reject_branch_suffix(name_parts: &[String], action: &str) -> Result<(), String> {
    if name_parts.len() >= 2
        && name_parts
            .last()
            .is_some_and(|part| part.starts_with("branch_") || part.starts_with("tag_"))
    {
        return Err(format!(
            "{action} does not support branch/tag suffix on table name: {}",
            name_parts.join(".")
        ));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProcedureArgMode {
    Named,
    Positional,
    Empty,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ProcedureArgValue {
    String(String),
    Boolean(bool),
    Integer(i64),
    TimestampMillis(i64),
    StringMap(BTreeMap<String, String>),
    Null,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ProcedureArg {
    name: Option<String>,
    value: ProcedureArgValue,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CallProcedure {
    catalog: String,
    procedure: String,
    args: Vec<ProcedureArg>,
    mode: ProcedureArgMode,
}

fn lower_call_procedure(
    statement: &CallProcedure,
    context: MaintenanceRequestContext<'_>,
) -> Result<ParsedMaintenanceStatement, String> {
    let named = normalize_procedure_args(statement)?;
    let table = required_string_arg(&named, "table")?;
    let name_parts =
        resolve_procedure_table_name(&statement.catalog, context.current_database, &table)?;

    let older_than_ms = optional_timestamp_arg(&named, "older_than")?;
    let retain_last = optional_u32_arg(&named, "retain_last")?;
    let use_caching = optional_bool_arg(&named, "use_caching")?;
    let spec_id = optional_i32_arg(&named, "spec_id")?;
    let branch = optional_string_arg(&named, "branch")?;
    let where_clause = optional_string_arg(&named, "where")?;
    let options = optional_string_map_arg(&named, "options")?.unwrap_or_default();

    validate_supported_args(&statement.procedure, named.keys())?;
    validate_current_task_args(&statement.procedure, named.keys())?;

    let action = match statement.procedure.as_str() {
        "rewrite_data_files" => {
            validate_rewrite_data_files(&options, branch.as_ref(), where_clause.as_ref())?;
            ParsedMaintenanceAction::RewriteDataFiles {
                options,
                branch,
                where_clause,
            }
        }
        "rewrite_manifests" => ParsedMaintenanceAction::RewriteManifests {
            use_caching,
            spec_id,
        },
        "expire_snapshots" => {
            if older_than_ms.is_none() && retain_last.is_none() {
                return Err("expire_snapshots requires `older_than` or `retain_last`".to_string());
            }
            ParsedMaintenanceAction::ExpireSnapshots {
                older_than_ms,
                retain_last,
            }
        }
        "remove_orphan_files" => ParsedMaintenanceAction::RemoveOrphanFiles {
            older_than_ms: older_than_ms.ok_or_else(|| {
                "remove_orphan_files requires `older_than` TIMESTAMP argument".to_string()
            })?,
        },
        "rewrite_position_delete_files" => ParsedMaintenanceAction::RewritePositionDeleteFiles {
            options,
            where_clause,
        },
        other => return Err(format!("unsupported Iceberg system procedure `{other}`")),
    };
    Ok(ParsedMaintenanceStatement::Execute { name_parts, action })
}

fn parse_timestamp_millis(value: &str) -> Result<i64, String> {
    if let Ok(value) = DateTime::parse_from_rfc3339(value) {
        return Ok(value.with_timezone(&Utc).timestamp_millis());
    }
    if let Ok(value) = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S") {
        return Ok(value.and_utc().timestamp_millis());
    }
    if !value.is_empty() && value.chars().all(|character| character.is_ascii_digit()) {
        return value
            .parse::<i64>()
            .map_err(|error| format!("invalid epoch-ms timestamp '{value}': {error}"));
    }
    Err(format!(
        "cannot parse timestamp '{value}'; expected RFC3339, epoch-ms, or YYYY-MM-DD HH:MM:SS"
    ))
}

fn classify_arg_mode(args: &[ProcedureArg]) -> Result<ProcedureArgMode, String> {
    let has_named = args.iter().any(|arg| arg.name.is_some());
    let has_positional = args.iter().any(|arg| arg.name.is_none());
    match (has_named, has_positional) {
        (false, false) => Ok(ProcedureArgMode::Empty),
        (true, false) => Ok(ProcedureArgMode::Named),
        (false, true) => Ok(ProcedureArgMode::Positional),
        (true, true) => Err("CALL procedure cannot mix positional and named arguments".to_string()),
    }
}

fn ensure_no_duplicate_named_args(args: &[ProcedureArg]) -> Result<(), String> {
    let mut seen = BTreeSet::new();
    for arg in args {
        let Some(name) = &arg.name else {
            continue;
        };
        if !seen.insert(name) {
            return Err(format!("duplicate CALL procedure argument '{name}'"));
        }
    }
    Ok(())
}

fn normalize_procedure_args(
    statement: &CallProcedure,
) -> Result<BTreeMap<String, ProcedureArgValue>, String> {
    let mut named = BTreeMap::new();
    match statement.mode {
        ProcedureArgMode::Empty => {}
        ProcedureArgMode::Named => {
            for arg in &statement.args {
                let Some(name) = &arg.name else {
                    return Err(
                        "CALL procedure cannot mix positional and named arguments".to_string()
                    );
                };
                insert_procedure_arg(&mut named, name, arg.value.clone())?;
            }
        }
        ProcedureArgMode::Positional => {
            let names = positional_names(&statement.procedure)?;
            if statement.args.len() > names.len() {
                return Err(format!(
                    "Iceberg system procedure `{}` accepts at most {} positional arguments, got {}",
                    statement.procedure,
                    names.len(),
                    statement.args.len()
                ));
            }
            for (arg, name) in statement.args.iter().zip(names.iter()) {
                insert_procedure_arg(&mut named, name, arg.value.clone())?;
            }
        }
    }
    Ok(named)
}

fn insert_procedure_arg(
    named: &mut BTreeMap<String, ProcedureArgValue>,
    name: &str,
    value: ProcedureArgValue,
) -> Result<(), String> {
    if named.insert(name.to_string(), value).is_some() {
        return Err(format!("duplicate CALL procedure argument `{name}`"));
    }
    Ok(())
}

fn positional_names(procedure: &str) -> Result<&'static [&'static str], String> {
    match procedure {
        "rewrite_data_files" => Ok(&[
            "table",
            "strategy",
            "sort_order",
            "options",
            "where",
            "branch",
        ]),
        "rewrite_manifests" => Ok(&["table", "use_caching", "spec_id"]),
        "expire_snapshots" => Ok(&[
            "table",
            "older_than",
            "retain_last",
            "max_concurrent_deletes",
            "stream_results",
            "snapshot_ids",
            "clean_expired_metadata",
        ]),
        "remove_orphan_files" => Ok(&[
            "table",
            "older_than",
            "location",
            "dry_run",
            "max_concurrent_deletes",
            "file_list_view",
            "equal_schemes",
            "equal_authorities",
            "prefix_mismatch_mode",
            "prefix_listing",
            "stream_results",
        ]),
        "rewrite_position_delete_files" => Ok(&["table", "options", "where"]),
        other => Err(format!("unsupported Iceberg system procedure `{other}`")),
    }
}

fn validate_supported_args<'a>(
    procedure: &str,
    keys: impl IntoIterator<Item = &'a String>,
) -> Result<(), String> {
    let allowed = positional_names(procedure)?;
    for key in keys {
        if !allowed.contains(&key.as_str()) {
            return Err(format!(
                "unsupported argument `{key}` for Iceberg system procedure `{procedure}`"
            ));
        }
    }
    Ok(())
}

fn validate_current_task_args<'a>(
    procedure: &str,
    keys: impl IntoIterator<Item = &'a String>,
) -> Result<(), String> {
    let implemented = match procedure {
        "rewrite_data_files" => &["table", "options", "where", "branch"][..],
        "rewrite_manifests" => &["table", "use_caching", "spec_id"],
        "expire_snapshots" => &["table", "older_than", "retain_last"],
        "remove_orphan_files" => &["table", "older_than"],
        "rewrite_position_delete_files" => &["table", "options", "where"],
        other => return Err(format!("unsupported Iceberg system procedure `{other}`")),
    };
    for key in keys {
        if !implemented.contains(&key.as_str()) {
            return Err(format!(
                "argument `{key}` for Iceberg system procedure `{procedure}` is not implemented in NovaRocks yet"
            ));
        }
    }
    Ok(())
}

fn validate_rewrite_data_files(
    options: &BTreeMap<String, String>,
    branch: Option<&String>,
    where_clause: Option<&String>,
) -> Result<(), String> {
    if where_clause.is_some() {
        return Err("rewrite_data_files where is not supported in NovaRocks yet".to_string());
    }
    if branch.is_some() {
        return Err("rewrite_data_files branch is not supported in NovaRocks yet".to_string());
    }
    for (key, value) in options {
        match key.as_str() {
            "rewrite-all" if value.eq_ignore_ascii_case("true") => {}
            "rewrite-all" => {
                return Err("rewrite_data_files option `rewrite-all` must be `true`".to_string());
            }
            other => return Err(format!("unsupported rewrite_data_files option `{other}`")),
        }
    }
    Ok(())
}

fn required_string_arg(
    named: &BTreeMap<String, ProcedureArgValue>,
    name: &str,
) -> Result<String, String> {
    match named.get(name) {
        Some(ProcedureArgValue::String(value)) => Ok(value.clone()),
        Some(value) => Err(format!(
            "CALL procedure argument `{name}` must be a string, got {}",
            procedure_arg_type(value)
        )),
        None => Err(format!("CALL procedure requires `{name}` argument")),
    }
}

fn optional_string_arg(
    named: &BTreeMap<String, ProcedureArgValue>,
    name: &str,
) -> Result<Option<String>, String> {
    match named.get(name) {
        Some(ProcedureArgValue::String(value)) => Ok(Some(value.clone())),
        Some(ProcedureArgValue::Null) | None => Ok(None),
        Some(value) => Err(format!(
            "CALL procedure argument `{name}` must be a string, got {}",
            procedure_arg_type(value)
        )),
    }
}

fn optional_bool_arg(
    named: &BTreeMap<String, ProcedureArgValue>,
    name: &str,
) -> Result<Option<bool>, String> {
    match named.get(name) {
        Some(ProcedureArgValue::Boolean(value)) => Ok(Some(*value)),
        Some(ProcedureArgValue::Null) | None => Ok(None),
        Some(value) => Err(format!(
            "CALL procedure argument `{name}` must be a boolean, got {}",
            procedure_arg_type(value)
        )),
    }
}

fn optional_timestamp_arg(
    named: &BTreeMap<String, ProcedureArgValue>,
    name: &str,
) -> Result<Option<i64>, String> {
    match named.get(name) {
        Some(ProcedureArgValue::TimestampMillis(value)) => Ok(Some(*value)),
        Some(ProcedureArgValue::Null) | None => Ok(None),
        Some(value) => Err(format!(
            "CALL procedure argument `{name}` must be a TIMESTAMP literal, got {}",
            procedure_arg_type(value)
        )),
    }
}

fn optional_u32_arg(
    named: &BTreeMap<String, ProcedureArgValue>,
    name: &str,
) -> Result<Option<u32>, String> {
    match named.get(name) {
        Some(ProcedureArgValue::Integer(value)) => {
            if *value <= 0 {
                return Err(format!("CALL procedure argument `{name}` must be >= 1"));
            }
            u32::try_from(*value)
                .map(Some)
                .map_err(|_| format!("CALL procedure argument `{name}` is too large"))
        }
        Some(ProcedureArgValue::Null) | None => Ok(None),
        Some(value) => Err(format!(
            "CALL procedure argument `{name}` must be an integer, got {}",
            procedure_arg_type(value)
        )),
    }
}

fn optional_i32_arg(
    named: &BTreeMap<String, ProcedureArgValue>,
    name: &str,
) -> Result<Option<i32>, String> {
    match named.get(name) {
        Some(ProcedureArgValue::Integer(value)) => i32::try_from(*value)
            .map(Some)
            .map_err(|_| format!("CALL procedure argument `{name}` does not fit i32")),
        Some(ProcedureArgValue::Null) | None => Ok(None),
        Some(value) => Err(format!(
            "CALL procedure argument `{name}` must be an integer, got {}",
            procedure_arg_type(value)
        )),
    }
}

fn optional_string_map_arg(
    named: &BTreeMap<String, ProcedureArgValue>,
    name: &str,
) -> Result<Option<BTreeMap<String, String>>, String> {
    match named.get(name) {
        Some(ProcedureArgValue::StringMap(value)) => Ok(Some(value.clone())),
        Some(ProcedureArgValue::Null) | None => Ok(None),
        Some(value) => Err(format!(
            "CALL procedure argument `{name}` must be a string map, got {}",
            procedure_arg_type(value)
        )),
    }
}

fn procedure_arg_type(value: &ProcedureArgValue) -> &'static str {
    match value {
        ProcedureArgValue::String(_) => "string",
        ProcedureArgValue::Boolean(_) => "boolean",
        ProcedureArgValue::Integer(_) => "integer",
        ProcedureArgValue::TimestampMillis(_) => "timestamp",
        ProcedureArgValue::StringMap(_) => "string map",
        ProcedureArgValue::Null => "null",
    }
}

fn resolve_procedure_table_name(
    call_catalog: &str,
    current_database: &str,
    raw_table: &str,
) -> Result<Vec<String>, String> {
    let parts = raw_table
        .split('.')
        .map(normalize_identifier)
        .collect::<Result<Vec<_>, _>>()?;
    let call_catalog = normalize_identifier(call_catalog)?;
    match parts.as_slice() {
        [table] => Ok(vec![
            call_catalog,
            normalize_identifier(current_database)?,
            table.clone(),
        ]),
        [namespace, table] => Ok(vec![call_catalog, namespace.clone(), table.clone()]),
        [catalog, namespace, table] => {
            if catalog != &call_catalog {
                return Err(format!(
                    "CALL procedure table catalog `{catalog}` does not match procedure catalog `{call_catalog}`"
                ));
            }
            Ok(vec![call_catalog, namespace.clone(), table.clone()])
        }
        _ => Err(format!(
            "CALL procedure table must be `table`, `namespace.table`, or `catalog.namespace.table`, got `{raw_table}`"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_parser::{
        ast::{MaintenanceStatement, Statement},
        parse,
    };

    fn typed_maintenance(source: &str) -> MaintenanceStatement {
        let statements = parse(source).expect("typed maintenance statement should parse");
        let [Statement::Maintenance(statement)] = statements.as_slice() else {
            panic!("expected one typed maintenance statement for {source}");
        };
        statement.clone()
    }

    fn context() -> MaintenanceRequestContext<'static> {
        MaintenanceRequestContext {
            current_catalog: Some("ice"),
            current_database: "default_db",
        }
    }

    #[test]
    fn lowers_typed_spark_call_without_sql_reparse() {
        let statement = typed_maintenance(
            "CALL `ice`.system.rewrite_manifests(\
                `table` => 'analytics.orders', use_caching => TRUE, spec_id => 3)",
        );

        assert!(is_typed_spark_maintenance_call(&statement));
        assert_eq!(
            lower_typed_maintenance_statement(&statement, context()),
            Ok(ParsedMaintenanceStatement::Execute {
                name_parts: vec![
                    "ice".to_string(),
                    "analytics".to_string(),
                    "orders".to_string()
                ],
                action: ParsedMaintenanceAction::RewriteManifests {
                    use_caching: Some(true),
                    spec_id: Some(3),
                },
            })
        );
    }

    #[test]
    fn lowers_typed_alter_maintenance_with_existing_semantics() {
        let statement = typed_maintenance(
            "ALTER TABLE `ice`.`analytics`.`orders` EXPIRE SNAPSHOTS \
             OLDER THAN 1700000000000 RETAIN LAST 3",
        );

        assert_eq!(
            lower_typed_maintenance_statement(&statement, context()),
            Ok(ParsedMaintenanceStatement::Execute {
                name_parts: vec![
                    "ice".to_string(),
                    "analytics".to_string(),
                    "orders".to_string()
                ],
                action: ParsedMaintenanceAction::ExpireSnapshots {
                    older_than_ms: Some(1_700_000_000_000),
                    retain_last: Some(3),
                },
            })
        );
    }

    #[test]
    fn typed_lowering_reapplies_legacy_admission_limits() {
        for (source, expected) in [
            (
                "ALTER TABLE ice.db.branch_main REWRITE MANIFESTS",
                "does not support branch/tag suffix",
            ),
            (
                "ALTER TABLE ice.db.orders EXPIRE SNAPSHOTS OLDER THAN TIMESTAMP '2026-01-01 00:00:00'",
                "expected timestamp literal",
            ),
            (
                "CALL ice.system.rewrite_data_files(table => 'db.orders', options => MAP('rewrite-all', 1))",
                "map key/value expects a single quoted string",
            ),
        ] {
            let statement = typed_maintenance(source);
            let error = lower_typed_maintenance_statement(&statement, context())
                .expect_err("typed syntax outside legacy maintenance admission must fail");
            assert!(error.contains(expected), "{source}: {error}");
        }
    }

    #[test]
    fn semantic_lowering_matches_typed_expire_snapshot_admission() {
        let source = "ALTER TABLE ice.analytics.orders EXPIRE SNAPSHOTS OLDER THAN 1700000000000 RETAIN LAST 3";
        let typed = typed_maintenance(source);
        let statement = novarocks_query_application::sql::parse_single_statement(source)
            .expect("parse semantic maintenance command");
        let Some(novarocks_query_application::sql::ProductSqlCommand::Maintenance(command)) =
            novarocks_query_application::sql::lower_product_sql_command(&statement)
                .expect("lower semantic maintenance command")
        else {
            panic!("expected semantic maintenance command");
        };

        assert_eq!(
            lower_semantic_maintenance_statement(&command, context()),
            lower_typed_maintenance_statement(&typed, context()),
        );
    }

    #[test]
    fn semantic_lowering_matches_typed_procedure_admission() {
        let source = "CALL ice.system.rewrite_manifests(table => 'analytics.orders', use_caching => TRUE, spec_id => 3)";
        let typed = typed_maintenance(source);
        let statement = novarocks_query_application::sql::parse_single_statement(source)
            .expect("parse semantic maintenance command");
        let Some(novarocks_query_application::sql::ProductSqlCommand::Maintenance(command)) =
            novarocks_query_application::sql::lower_product_sql_command(&statement)
                .expect("lower semantic maintenance command")
        else {
            panic!("expected semantic maintenance command");
        };

        assert_eq!(
            lower_semantic_maintenance_statement(&command, context()),
            lower_typed_maintenance_statement(&typed, context()),
        );
    }

    #[test]
    fn lowers_typed_show_optimize_presentation() {
        let statement = typed_maintenance(
            "SHOW ALTER TABLE OPTIMIZE FROM `ice`.`analytics` \
             WHERE `TableName` = 'orders' ORDER BY `CreateTime` DESC LIMIT 20",
        );
        let MaintenanceStatement::ShowOptimize(show) = statement else {
            panic!("expected typed SHOW ALTER TABLE OPTIMIZE");
        };

        assert_eq!(
            lower_typed_show_optimize(&show),
            Ok(ParsedShowOptimize {
                catalog: Some("ice".to_string()),
                database: Some("analytics".to_string()),
                table_name: Some("orders".to_string()),
                order_by_create_time_desc: true,
                limit: Some(20),
            })
        );
    }

    #[test]
    fn typed_show_lowering_rejects_non_legacy_filter_literals() {
        let statement = typed_maintenance("SHOW ALTER TABLE OPTIMIZE WHERE TableName = 1");
        let MaintenanceStatement::ShowOptimize(show) = statement else {
            panic!("expected typed SHOW ALTER TABLE OPTIMIZE");
        };

        assert!(
            lower_typed_show_optimize(&show)
                .expect_err("numeric TableName filter is not part of legacy grammar")
                .contains("WHERE TableName")
        );
    }
}
