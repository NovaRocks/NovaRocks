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
    ast::{Expr, FunctionCall, Ident, Literal, LiteralKind, ObjectName, TrinoCall},
};
use novarocks_sql::semantic::command::ProcedureArgumentModeSql;

use crate::api::{CommandError, CommandErrorKind};

/// Arguments retained for registered management and development CALL routes.
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
}

impl PreparedMaintenanceCall {
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
            let (pairs, remainder) = call.arguments.as_slice().as_chunks::<2>();
            if !remainder.is_empty() {
                return Err("CALL MAP expects alternating literal key and value arguments");
            }
            pairs
                .iter()
                .map(|[key, value]| {
                    let (Expr::Literal(key), Expr::Literal(value)) = (&key.value, &value.value)
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
    use novarocks_parser::{ast::TrinoStatement, parser};

    fn prepare(sql: &str) -> Result<PreparedMaintenanceCall, CommandError> {
        let TrinoStatement::Call(call) = parser::parse_statement(sql).unwrap() else {
            panic!("expected CALL");
        };
        PreparedMaintenanceCall::from_trino(&call)
    }

    #[test]
    fn typed_call_moves_to_the_original_maintenance_value_shapes() {
        let sql = r#"CALL lake.system.proc("table" => 's.t', before => TIMESTAMP '2020-01-01 00:00:00', options => map('keep', 'true', 'drop', 'false'))"#;
        let call = prepare(sql).unwrap();
        assert_eq!(
            call.procedure
                .parts
                .iter()
                .map(|part| part.value.as_str())
                .collect::<Vec<_>>(),
            ["lake", "system", "proc"]
        );
        assert_eq!(call.argument_mode, ProcedureArgumentModeSql::Named);
        let [table, before, options] = call.arguments.as_slice() else {
            panic!("expected three named arguments");
        };
        assert_eq!(table.name.as_ref().unwrap().value, "table");
        assert_eq!(before.name.as_ref().unwrap().value, "before");
        assert_eq!(options.name.as_ref().unwrap().value, "options");
        assert!(matches!(&table.value,
            PreparedMaintenanceValue::Literal(Literal{kind: LiteralKind::String(value),..})
                if value == "s.t"));
        assert!(matches!(&before.value,
            PreparedMaintenanceValue::Timestamp(Literal{kind: LiteralKind::Generic(value),..})
                if value.value == "2020-01-01 00:00:00"));
        let PreparedMaintenanceValue::Map(entries) = &options.value else {
            panic!("expected ordered literal pairs");
        };
        assert_eq!(entries.len(), 2);
        for ((key, value), (expected_key, expected_value)) in
            entries.iter().zip([("keep", "true"), ("drop", "false")])
        {
            assert!(matches!(&key.kind, LiteralKind::String(k) if k == expected_key));
            assert!(matches!(&value.kind, LiteralKind::String(v) if v == expected_value));
        }
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
