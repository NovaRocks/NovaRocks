// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Temporary real legacy slots, evaluated through the original declaration loop.

use super::*;
use novarocks_parser::ast::legacy::CreateTable;
use novarocks_spi::connector::{
    ConnectorCatalogMutationSession, ConnectorError, ConnectorErrorKind, ConnectorOperationControl,
    ConnectorPartitionTransform, LegacyPreparedTablePropertyValue,
};
use std::sync::Arc;

/// Both controls belong to the same admitted statement. The real caller runs
/// this pure preparation on its original planning CPU and retains the binding.
/// This entry and its Provider inverse are removed with the old parser at P12.
pub(in crate::sql) fn prepare_legacy_create_table_properties(
    source: &str,
    statement: &CreateTable,
    partitioning: Option<(&[ConnectorPartitionTransform], Span)>,
    mutation: &ConnectorCatalogMutationSession,
    connector_control: &dyn ConnectorOperationControl,
    port: &mut dyn DdlPropertyConstantPort,
    control: &dyn PureCompileControl,
    before_raw: impl FnOnce() -> Result<(), QueryServiceError>,
) -> Result<(Vec<(String, DdlPropertyValue)>, Option<String>), QueryServiceError> {
    checkpoint(control, 0)?;
    let properties = &statement.properties;
    let mut raw = Vec::with_capacity(properties.len());
    for property in properties {
        checkpoint(control, 1)?;
        raw.push((
            Arc::from(legacy_property_text(&property.key, control)?),
            Arc::from(legacy_property_text(&property.value, control)?),
        ));
    }
    // The original COMMENT renderer followed every property literal but
    // preceded the Provider's raw gate and its typed decoding. Keep it here
    // under the same author; COMMENT remains a separate final command fact.
    let comment = statement
        .comment
        .as_ref()
        .map(|literal| legacy_property_text(literal, control))
        .transpose()?;
    if let Some(comment) = &comment {
        // The old renderer appended this real clause slot before the complete
        // raw gate. Keep that ordering, including duplicate-comment failures.
        // This preparation-only slot is consumed below, never sent twice to
        // the final typed command or its metadata composer.
        raw.push((Arc::from("comment"), Arc::from(comment.as_str())));
    }
    // Invoke the original FE column/default stage only after both literal
    // renderers, before any Provider raw gate or typed-property decoding.
    checkpoint(control, 0)?;
    before_raw()?;
    checkpoint(control, 0)?;
    let prepared = mutation
        .prepare_legacy_create_table_property_inputs(&raw, connector_control)
        .map_err(|failure| {
            if failure.input_index.is_some_and(|index| index >= raw.len()) {
                return invariant("Legacy property failure issued a foreign input ordinal");
            }
            let span = failure.input_index.map(|index| {
                properties
                    .get(index)
                    .map_or(statement.span, |property| property.span)
            });
            property_failure(source, span, failure.error)
        })?;
    if prepared.len() != raw.len() {
        return Err(invariant(
            "Legacy property preparation omitted an original input",
        ));
    }
    let declarations = mutation
        .table_property_declarations()
        .map_err(|error| property_failure(source, None, error))?;
    // These are local neutral values, not parser nodes or a final command's
    // origin. Each issued ordinal must identify its actual original slot.
    let mut values = Vec::new();
    let mut extras = Vec::new();
    let mut first_extra = None;
    for (index, property) in prepared.into_iter().enumerate() {
        checkpoint(control, 1)?;
        if property.input_index != index {
            return Err(invariant(
                "Legacy property preparation reordered original input ordinals",
            ));
        }
        if index == properties.len() && comment.is_some() {
            match property.value {
                LegacyPreparedTablePropertyValue::Extra { key, value }
                    if key == "comment" && Some(&value) == comment.as_ref() => {}
                _ => {
                    return Err(invariant(
                        "Legacy COMMENT preparation changed its original slot",
                    ));
                }
            }
            continue;
        }
        let original = properties.get(property.input_index).ok_or_else(|| {
            invariant("Legacy property preparation issued a foreign input ordinal")
        })?;
        match property.value {
            LegacyPreparedTablePropertyValue::Declared { name, value } => {
                values.push((
                    property.input_index,
                    name.to_owned(),
                    value,
                    original.key.span,
                    original.value.span,
                    original.span,
                ));
            }
            LegacyPreparedTablePropertyValue::Extra { key, value } => {
                first_extra.get_or_insert((property.input_index, original));
                extras.push((key, Some(value)));
            }
        }
    }
    if let Some((index, original)) = first_extra {
        values.push((
            index,
            "extra_properties".to_owned(),
            DdlPropertyValue::MapVarcharVarchar(extras),
            original.key.span,
            original.value.span,
            original.span,
        ));
    }
    values.sort_by_key(|value| value.0);
    if let Some((fields, span)) = partitioning {
        let value = mutation
            .prepare_legacy_create_table_partitioning_property(fields, connector_control)
            .map_err(|error| property_failure(source, Some(span), error))?;
        values.push((
            properties.len(),
            "partitioning".to_owned(),
            value,
            span,
            span,
            span,
        ));
    }
    let assignments = values
        .iter()
        .map(
            |(_, name, value, name_span, span, full_span)| DdlPropertyAssignment {
                name,
                name_span: *name_span,
                span: *full_span,
                value: DdlPropertyAssignmentValue::Authored { value, span: *span },
            },
        )
        .collect::<Vec<_>>();
    struct OriginalNames<'a>(&'a ConnectorCatalogMutationSession);
    impl DdlPropertyNamePolicy for OriginalNames<'_> {
        fn check_unknown_property(&mut self, name: &str) -> Result<(), QueryServiceError> {
            self.0.check_table_property_name(name).map_err(|error| {
                QueryServiceError::from_failure_cause(SharedFailureCause::new(error))
            })
        }
    }
    let evaluated = evaluate_ddl_properties_with_name_policy(
        DdlPropertyScope::Table,
        DdlPropertyEvaluationMode::Create,
        source,
        &declarations,
        &assignments,
        port,
        control,
        &mut OriginalNames(mutation),
    )?;
    let properties = evaluated
        .into_properties()
        .into_iter()
        .map(|property| {
            let DdlPropertyChange::Set(value) = property.change else {
                return Err(invariant(
                    "Legacy CREATE properties unexpectedly contain Reset",
                ));
            };
            Ok((property.name, value))
        })
        .collect::<Result<Vec<_>, QueryServiceError>>()?;
    Ok((properties, comment))
}

fn property_failure(source: &str, span: Option<Span>, error: ConnectorError) -> QueryServiceError {
    if error.kind() == ConnectorErrorKind::InvalidRequest
        && let Some(span) = span
    {
        QueryServiceError::from_user_error_with_cause(
            DdlPropertyScope::Table.to_user_error(source, span, error.message()),
            SharedFailureCause::new(error),
        )
    } else {
        QueryServiceError::from_failure_cause(SharedFailureCause::new(error))
    }
}

/// Preserve the old literal renderer's exact domain. This does not evaluate a
/// SQL expression, print/reparse SQL, or infer a numeric constant from text.
fn legacy_property_text(
    literal: &novarocks_parser::ast::Literal,
    control: &dyn PureCompileControl,
) -> Result<String, QueryServiceError> {
    use novarocks_parser::ast::LiteralKind;
    use novarocks_sql::semantic::command::CommandLiteral;
    match &literal.kind {
        LiteralKind::String(value) | LiteralKind::Decimal(value) => observe_bytes(value, control)?,
        LiteralKind::Long { spelling, .. } => observe_bytes(spelling, control)?,
        LiteralKind::Binary(value) => {
            for chunk in value.chunks(256) {
                checkpoint(control, chunk.len() as u32)?;
            }
        }
        _ => {}
    }
    let literal = super::super::lower_command_literal(literal)
        .map_err(|message| QueryServiceError::new(QueryServiceErrorKind::Internal, message))?;
    match literal {
        CommandLiteral::Null => Err(QueryServiceError::new(
            QueryServiceErrorKind::Internal,
            "table properties do not support NULL values",
        )),
        CommandLiteral::Bool(value) => Ok(value.to_string()),
        CommandLiteral::Number(value) | CommandLiteral::String(value) => {
            observe_bytes(&value, control)?;
            Ok(value)
        }
        CommandLiteral::Binary(value) => {
            use std::fmt::Write;
            let mut result = String::from("0x");
            for chunk in value.chunks(256) {
                checkpoint(control, chunk.len() as u32)?;
                for byte in chunk {
                    write!(result, "{byte:02x}").expect("String formatting cannot fail");
                }
            }
            checkpoint(control, 0)?;
            Ok(result)
        }
    }
}
