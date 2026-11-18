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

use std::collections::BTreeSet;

use novarocks_parser::{
    Span,
    ast::{
        CallStatement, Expr, FunctionCall, Ident, Literal, LiteralKind, MaintenanceValue,
        ObjectName, ProcedureArgumentMode, TrinoCall,
    },
};
use novarocks_sql::semantic::{
    MaintenanceSqlCommand,
    command::{
        CommandLiteral, MaintenanceValueSql, ProcedureArgumentModeSql, ProcedureArgumentSql,
    },
};

use crate::api::{CommandError, CommandErrorKind};

/// The procedure input consumed by both specialized and ordinary CALL routes.
/// Literal spelling remains available to domain checks that require it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedMaintenanceCall {
    pub procedure: ObjectName,
    pub arguments: Vec<PreparedMaintenanceArgument>,
    pub argument_mode: ProcedureArgumentModeSql,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedMaintenanceArgument {
    pub name: Option<Ident>,
    pub value: PreparedMaintenanceValue,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PreparedMaintenanceValue {
    Literal(Literal),
    Timestamp(Literal),
    Map(Vec<(Literal, Literal)>),
}

impl PreparedMaintenanceValue {
    pub fn literal(&self) -> Option<&Literal> {
        match self {
            Self::Literal(value) => Some(value),
            Self::Timestamp(_) | Self::Map(_) => None,
        }
    }

    fn maintenance_value(&self) -> Result<MaintenanceValueSql, String> {
        match self {
            Self::Literal(value) => {
                super::lower_command_literal(value).map(MaintenanceValueSql::Literal)
            }
            Self::Timestamp(value) => {
                let literal = match &value.kind {
                    LiteralKind::Generic(generic)
                        if generic.type_name.eq_ignore_ascii_case("TIMESTAMP") =>
                    {
                        CommandLiteral::String(generic.value.clone())
                    }
                    _ => super::lower_command_literal(value)?,
                };
                Ok(MaintenanceValueSql::Timestamp(literal))
            }
            Self::Map(entries) => entries
                .iter()
                .map(|(key, value)| {
                    Ok((
                        super::lower_command_literal(key)?,
                        super::lower_command_literal(value)?,
                    ))
                })
                .collect::<Result<_, String>>()
                .map(MaintenanceValueSql::Map),
        }
    }
}

impl PreparedMaintenanceCall {
    pub fn from_legacy(statement: &CallStatement) -> Self {
        let arguments = statement
            .arguments
            .iter()
            .map(|argument| {
                let value = match &argument.value {
                    MaintenanceValue::Literal(value) => {
                        PreparedMaintenanceValue::Literal(value.clone())
                    }
                    MaintenanceValue::Timestamp { value, .. } => {
                        PreparedMaintenanceValue::Timestamp(value.clone())
                    }
                    MaintenanceValue::Map(map) => PreparedMaintenanceValue::Map(
                        map.entries
                            .iter()
                            .map(|entry| (entry.key.clone(), entry.value.clone()))
                            .collect(),
                    ),
                };
                PreparedMaintenanceArgument {
                    name: argument.name.clone(),
                    value,
                    span: argument.span,
                }
            })
            .collect();
        let argument_mode = match statement.argument_mode {
            ProcedureArgumentMode::Empty => ProcedureArgumentModeSql::Empty,
            ProcedureArgumentMode::Positional => ProcedureArgumentModeSql::Positional,
            ProcedureArgumentMode::Named => ProcedureArgumentModeSql::Named,
        };
        Self {
            procedure: statement.procedure.clone(),
            arguments,
            argument_mode,
            span: statement.span,
        }
    }

    pub fn from_trino(statement: &TrinoCall) -> Result<Self, CommandError> {
        let has_named = statement
            .arguments
            .iter()
            .any(|argument| argument.name.is_some());
        let has_positional = statement
            .arguments
            .iter()
            .any(|argument| argument.name.is_none());
        let argument_mode = match (has_named, has_positional) {
            (false, false) => ProcedureArgumentModeSql::Empty,
            (true, false) => ProcedureArgumentModeSql::Named,
            (false, true) => ProcedureArgumentModeSql::Positional,
            (true, true) => {
                return Err(CommandError::new(
                    CommandErrorKind::Invalid,
                    "CALL requires only named or only positional procedure arguments",
                ));
            }
        };
        let mut names = BTreeSet::new();
        for name in statement
            .arguments
            .iter()
            .filter_map(|argument| argument.name.as_ref())
        {
            if !names.insert(name.value.to_ascii_lowercase()) {
                return Err(CommandError::new(
                    CommandErrorKind::Invalid,
                    format!(
                        "CALL requires a unique procedure argument name: `{}`",
                        name.value
                    ),
                ));
            }
        }
        let arguments = statement
            .arguments
            .iter()
            .map(|argument| {
                Ok(PreparedMaintenanceArgument {
                    name: argument.name.clone(),
                    value: prepare_trino_argument(&argument.value).map_err(|message| {
                        CommandError::new(CommandErrorKind::Unsupported, message)
                    })?,
                    span: argument.span,
                })
            })
            .collect::<Result<_, CommandError>>()?;
        Ok(Self {
            procedure: statement.name.clone(),
            arguments,
            argument_mode,
            span: statement.span,
        })
    }

    pub fn maintenance_command(&self) -> Result<MaintenanceSqlCommand, String> {
        let arguments = self
            .arguments
            .iter()
            .map(|argument| {
                Ok(ProcedureArgumentSql {
                    name: argument.name.as_ref().map(|name| name.value.clone()),
                    value: argument.value.maintenance_value()?,
                })
            })
            .collect::<Result<_, String>>()?;
        Ok(MaintenanceSqlCommand::Call {
            procedure: super::lower_object_name(&self.procedure),
            arguments,
            argument_mode: self.argument_mode,
        })
    }
}

fn prepare_trino_argument(value: &Expr) -> Result<PreparedMaintenanceValue, &'static str> {
    match value {
        Expr::Literal(literal) => {
            if let LiteralKind::Generic(generic) = &literal.kind
                && generic.type_name.eq_ignore_ascii_case("TIMESTAMP")
            {
                return Ok(PreparedMaintenanceValue::Timestamp(literal.clone()));
            }
            prepare_plain_literal(literal)
                .cloned()
                .map(PreparedMaintenanceValue::Literal)
        }
        Expr::FunctionCall(call) if is_procedure_map(call) => {
            let pairs = call.arguments.chunks_exact(2);
            if !pairs.remainder().is_empty() {
                return Err("CALL MAP expects alternating literal key and value arguments");
            }
            pairs
                .map(|pair| {
                    let (Expr::Literal(key), Expr::Literal(value)) =
                        (&pair[0].value, &pair[1].value)
                    else {
                        return Err("CALL MAP expects alternating literal key and value arguments");
                    };
                    Ok((
                        prepare_plain_literal(key)?.clone(),
                        prepare_plain_literal(value)?.clone(),
                    ))
                })
                .collect::<Result<_, _>>()
                .map(PreparedMaintenanceValue::Map)
        }
        _ => Err("CALL supports literal, TIMESTAMP and flat MAP arguments"),
    }
}

fn prepare_plain_literal(literal: &Literal) -> Result<&Literal, &'static str> {
    match literal.kind {
        LiteralKind::Generic(_) | LiteralKind::Interval(_) => {
            Err("typed literals are not supported in command arguments")
        }
        _ => Ok(literal),
    }
}

fn is_procedure_map(call: &FunctionCall) -> bool {
    matches!(call.name.parts.as_slice(), [name] if !name.quoted && name.value.eq_ignore_ascii_case("MAP"))
        && !call.distinct
        && call.order_by.is_none()
        && call.filter.is_none()
        && call.null_treatment.is_none()
        && call.window.is_none()
        && call.processing_mode.is_none()
        && call
            .arguments
            .iter()
            .all(|argument| argument.name.is_none())
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_parser::{
        ast::{Statement, TrinoStatement},
        trino_parser,
    };
    use novarocks_sql::semantic::ObjectName;

    fn prepare(sql: &str) -> Result<MaintenanceSqlCommand, CommandError> {
        let TrinoStatement::Call(call) = trino_parser::parse_statement(sql).unwrap() else {
            panic!("expected CALL");
        };
        PreparedMaintenanceCall::from_trino(&call)?
            .maintenance_command()
            .map_err(|message| CommandError::new(CommandErrorKind::Unsupported, message))
    }

    #[test]
    fn typed_call_moves_to_the_original_maintenance_value_shapes() {
        let legacy_sql = "CALL lake.system.proc(table => 's.t', before => TIMESTAMP '2020-01-01 00:00:00', options => map('keep', 'true', 'drop', 'false'))";
        let sql = r#"CALL lake.system.proc("table" => 's.t', before => TIMESTAMP '2020-01-01 00:00:00', options => map('keep', 'true', 'drop', 'false'))"#;
        let expected = MaintenanceSqlCommand::Call {
            procedure: ObjectName {
                parts: vec!["lake".into(), "system".into(), "proc".into()],
            },
            argument_mode: ProcedureArgumentModeSql::Named,
            arguments: vec![
                ProcedureArgumentSql {
                    name: Some("table".into()),
                    value: MaintenanceValueSql::Literal(CommandLiteral::String("s.t".into())),
                },
                ProcedureArgumentSql {
                    name: Some("before".into()),
                    value: MaintenanceValueSql::Timestamp(CommandLiteral::String(
                        "2020-01-01 00:00:00".into(),
                    )),
                },
                ProcedureArgumentSql {
                    name: Some("options".into()),
                    value: MaintenanceValueSql::Map(vec![
                        (
                            CommandLiteral::String("keep".into()),
                            CommandLiteral::String("true".into()),
                        ),
                        (
                            CommandLiteral::String("drop".into()),
                            CommandLiteral::String("false".into()),
                        ),
                    ]),
                },
            ],
        };
        let legacy = novarocks_parser::parse(legacy_sql).unwrap().pop().unwrap();
        let Statement::Maintenance(legacy) = legacy else {
            panic!("expected legacy CALL")
        };
        assert_eq!(
            super::super::lower_maintenance_command(&legacy).unwrap(),
            expected
        );
        assert_eq!(prepare(sql).unwrap(), expected);
    }

    #[test]
    fn unsupported_call_arguments_and_invalid_argument_modes_remain_distinct() {
        for sql in [
            "CALL lake.system.proc(1 + 2)",
            "CALL lake.system.proc(map('key'))",
            "CALL lake.system.proc(map(ARRAY['key'], ARRAY['value']))",
            "CALL lake.system.proc(lake.map('key', 'value'))",
        ] {
            assert_eq!(
                prepare(sql).unwrap_err().kind(),
                CommandErrorKind::Unsupported,
                "{sql}"
            );
        }
        for sql in [
            "CALL lake.system.proc(x => 1, 2)",
            "CALL lake.system.proc(x => 1, X => 2)",
        ] {
            assert_eq!(
                prepare(sql).unwrap_err().kind(),
                CommandErrorKind::Invalid,
                "{sql}"
            );
        }
    }
}
