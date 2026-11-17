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

//! DDL property preparation, separate from expression execution and provider policy.

use std::collections::HashSet;

use novarocks_constant_contract::{ConstantError, ConstantValue};
use novarocks_failure_contract::SharedFailureCause;
use novarocks_parser::{Span, ast::Expr};
use novarocks_spi::connector::{DdlPropertyDeclaration, DdlPropertyType, DdlPropertyValue};
use novarocks_sql::compiler::RequiredConstantTarget;
use novarocks_type_contract::{CompilePhase, PureCompileControl, ValueKind, ValueLogicalType};

use super::property_admit::DdlPropertyScope;
use crate::session_error::{QueryServiceError, QueryServiceErrorKind};

#[derive(Clone, Copy, Debug)]
pub struct DdlPropertyAssignment<'a> {
    pub name: &'a str,
    pub name_span: Span,
    pub span: Span,
    pub value: DdlPropertyAssignmentValue<'a>,
}

#[derive(Clone, Copy, Debug)]
pub enum DdlPropertyAssignmentValue<'a> {
    Expression(&'a Expr),
    Default { span: Span },
}

/// The original computation owner alone classifies a failed expression as
/// property user data. All other typed failures cross this boundary unchanged.
#[derive(Debug)]
pub enum DdlPropertyConstantError {
    InvalidValue {
        message: String,
        cause: Option<SharedFailureCause>,
    },
    Failure(QueryServiceError),
}

impl DdlPropertyConstantError {
    pub fn invalid_value(message: impl Into<String>) -> Self {
        Self::InvalidValue {
            message: message.into(),
            cause: None,
        }
    }

    pub fn invalid_value_with_cause(message: impl Into<String>, cause: SharedFailureCause) -> Self {
        Self::InvalidValue {
            message: message.into(),
            cause: Some(cause),
        }
    }
}

impl From<QueryServiceError> for DdlPropertyConstantError {
    fn from(error: QueryServiceError) -> Self {
        Self::Failure(error)
    }
}

pub trait DdlPropertyConstantPort {
    fn evaluate(
        &mut self,
        expression: &Expr,
        target: RequiredConstantTarget,
    ) -> Result<ConstantValue, DdlPropertyConstantError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DdlPropertyEvaluationMode {
    Create,
    Alter,
}

/// Reset retains its explicit default, including a NULL default for ALTER.
/// Neither variant denotes absence; providers still validate non-NULL children.
#[derive(Clone, Debug, PartialEq)]
pub enum DdlPropertyChange {
    Set(DdlPropertyValue),
    Reset(DdlPropertyValue),
}

#[derive(Clone, Debug, PartialEq)]
pub struct EvaluatedDdlProperty {
    pub name: String,
    pub change: DdlPropertyChange,
}

#[derive(Clone, Debug, PartialEq)]
pub struct EvaluatedDdlProperties {
    properties: Vec<EvaluatedDdlProperty>,
}

impl EvaluatedDdlProperties {
    pub fn properties(&self) -> &[EvaluatedDdlProperty] {
        &self.properties
    }
    pub fn into_properties(self) -> Vec<EvaluatedDdlProperty> {
        self.properties
    }
}

/// Declaration order determines ordinary frozen output order. Catalog has no
/// declaration lookup: its original case-sensitive names retain input order.
/// No published Set value is whole SQL NULL. Reset(NULL) is an ALTER intention.
pub fn evaluate_ddl_properties(
    scope: DdlPropertyScope,
    mode: DdlPropertyEvaluationMode,
    source: &str,
    declarations: &[DdlPropertyDeclaration],
    assignments: &[DdlPropertyAssignment<'_>],
    port: &mut dyn DdlPropertyConstantPort,
    control: &dyn PureCompileControl,
) -> Result<EvaluatedDdlProperties, QueryServiceError> {
    checkpoint(control, 0)?;
    let catalog = scope == DdlPropertyScope::Catalog;
    if catalog && !declarations.is_empty() {
        return Err(invariant(
            "Catalog properties do not use declaration lookup",
        ));
    }
    let mut declaration_names = HashSet::new();
    for declaration in declarations {
        observe_bytes(declaration.name(), control)?;
        if declaration.name() != declaration.name().to_lowercase()
            || !declaration_names.insert(declaration.name())
        {
            return Err(invariant(
                "DDL property declarations must have unique lowercase names",
            ));
        }
    }
    let mut seen = HashSet::new();
    let mut changes = vec![None; declarations.len()];
    let mut catalog_properties = Vec::new();
    for assignment in assignments {
        checkpoint(control, 1)?;
        observe_bytes(assignment.name, control)?;
        let name = if catalog {
            assignment.name.to_owned()
        } else {
            assignment.name.to_lowercase()
        };
        checkpoint(control, 0)?;
        let declaration = if catalog {
            None
        } else {
            Some(
                declarations
                    .iter()
                    .position(|declaration| declaration.name() == name)
                    .ok_or_else(|| {
                        invalid(
                            scope,
                            source,
                            assignment.name_span,
                            format!("Unknown property '{name}'"),
                        )
                    })?,
            )
        };
        if !seen.insert(copy_string(&name, control)?) {
            return Err(invalid(
                scope,
                source,
                assignment.name_span,
                format!("Duplicate property '{name}'"),
            ));
        }
        let value_type = declaration.map_or(DdlPropertyType::Varchar, |index| {
            declarations[index].value_type()
        });
        let change = match assignment.value {
            DdlPropertyAssignmentValue::Default { span } => {
                let Some(index) = declaration else {
                    return Err(invalid(
                        scope,
                        source,
                        span,
                        "Catalog property DEFAULT is not supported",
                    ));
                };
                let value = copy_value(declarations[index].default_value(), control)?;
                if mode == DdlPropertyEvaluationMode::Create && value == DdlPropertyValue::Null {
                    continue;
                }
                DdlPropertyChange::Reset(value)
            }
            DdlPropertyAssignmentValue::Expression(expression) => {
                let value = if value_type == DdlPropertyType::MapVarcharVarchar {
                    evaluate_map(scope, source, expression, port, control)?
                } else {
                    evaluate_value(scope, source, expression, value_type, port, control)?
                };
                if value == DdlPropertyValue::Null {
                    return Err(invalid(
                        scope,
                        source,
                        expression.span(),
                        format!("Property '{name}' cannot be NULL"),
                    ));
                }
                DdlPropertyChange::Set(value)
            }
        };
        if let Some(index) = declaration {
            changes[index] = Some(change);
        } else {
            catalog_properties.push(EvaluatedDdlProperty { name, change });
        }
    }
    let properties = if catalog {
        catalog_properties
    } else {
        let mut properties = Vec::new();
        for (declaration, change) in declarations.iter().zip(changes) {
            checkpoint(control, 1)?;
            let change = match change {
                Some(change) => Some(change),
                None if mode == DdlPropertyEvaluationMode::Create
                    && declaration.default_value() != &DdlPropertyValue::Null =>
                {
                    Some(DdlPropertyChange::Set(copy_value(
                        declaration.default_value(),
                        control,
                    )?))
                }
                None => None,
            };
            if let Some(change) = change {
                properties.push(EvaluatedDdlProperty {
                    name: copy_string(declaration.name(), control)?,
                    change,
                });
            }
        }
        properties
    };
    checkpoint(control, 0)?;
    Ok(EvaluatedDdlProperties { properties })
}

fn evaluate_value(
    scope: DdlPropertyScope,
    source: &str,
    expression: &Expr,
    value_type: DdlPropertyType,
    port: &mut dyn DdlPropertyConstantPort,
    control: &dyn PureCompileControl,
) -> Result<DdlPropertyValue, QueryServiceError> {
    let target = match value_type {
        DdlPropertyType::Boolean => RequiredConstantTarget::Boolean,
        DdlPropertyType::Integer => RequiredConstantTarget::Integer,
        DdlPropertyType::Bigint => RequiredConstantTarget::Bigint,
        DdlPropertyType::Double => RequiredConstantTarget::Double,
        DdlPropertyType::Varchar => RequiredConstantTarget::Varchar,
        DdlPropertyType::ArrayVarchar => RequiredConstantTarget::ArrayVarchar,
        DdlPropertyType::MapVarcharVarchar => {
            return Err(invariant("MAP uses its structural owner"));
        }
    };
    checkpoint(control, 0)?;
    let constant = match port.evaluate(expression, target) {
        Ok(constant) => constant,
        Err(DdlPropertyConstantError::Failure(error)) => return Err(error),
        Err(DdlPropertyConstantError::InvalidValue { message, cause }) => {
            let error = scope.to_user_error(source, expression.span(), message);
            return Err(match cause {
                Some(cause) => QueryServiceError::from_user_error_with_cause(error, cause),
                None => QueryServiceError::from_user_error(error),
            });
        }
    };
    checkpoint(control, 0)?;
    // The required-constant port must return the complete declared target,
    // not merely an Arrow carrier that happens to be readable as that target.
    let binding = constant.value_type();
    let matches = binding.logical_type() == ValueLogicalType::Physical
        && match (value_type, binding.value_type().kind()) {
            (DdlPropertyType::Boolean, ValueKind::Scalar(arrow::datatypes::DataType::Boolean))
            | (DdlPropertyType::Integer, ValueKind::Scalar(arrow::datatypes::DataType::Int32))
            | (DdlPropertyType::Bigint, ValueKind::Scalar(arrow::datatypes::DataType::Int64))
            | (DdlPropertyType::Double, ValueKind::Scalar(arrow::datatypes::DataType::Float64))
            | (DdlPropertyType::Varchar, ValueKind::String) => true,
            (DdlPropertyType::ArrayVarchar, ValueKind::List(child)) => {
                matches!(child.kind(), ValueKind::String)
            }
            _ => false,
        };
    if !matches {
        return Err(invariant(
            "Required property constant has a different complete target type",
        ));
    }
    let phase = CompilePhase::Validate;
    if constant
        .is_null_observed(phase, control)
        .map_err(constant_failure)?
    {
        return Ok(DdlPropertyValue::Null);
    }
    let missing =
        || invariant("Required property constant cannot be projected by its original reader");
    let value = match value_type {
        DdlPropertyType::Boolean => DdlPropertyValue::Boolean(
            constant
                .try_boolean()
                .map_err(constant_failure)?
                .ok_or_else(missing)?,
        ),
        DdlPropertyType::Integer => DdlPropertyValue::Integer(
            i32::try_from(
                constant
                    .signed_integer_observed(phase, control)
                    .map_err(constant_failure)?
                    .ok_or_else(missing)?,
            )
            .map_err(|_| missing())?,
        ),
        DdlPropertyType::Bigint => DdlPropertyValue::Bigint(
            constant
                .int64_observed(phase, control)
                .map_err(constant_failure)?
                .ok_or_else(missing)?,
        ),
        DdlPropertyType::Double => DdlPropertyValue::Double(f64::from_bits(
            constant
                .try_f64_bits()
                .map_err(constant_failure)?
                .ok_or_else(missing)?,
        )),
        DdlPropertyType::Varchar => DdlPropertyValue::Varchar(copy_string(
            constant
                .utf8_observed(phase, control)
                .map_err(constant_failure)?
                .ok_or_else(missing)?,
            control,
        )?),
        DdlPropertyType::ArrayVarchar => {
            let list = constant
                .utf8_list_observed(phase, control)
                .map_err(constant_failure)?
                .ok_or_else(missing)?;
            let mut items = Vec::with_capacity(list.len());
            for index in 0..list.len() {
                checkpoint(control, 1)?;
                items.push(
                    list.item(index, phase, control)
                        .map_err(constant_failure)?
                        .map(|item| copy_string(item, control))
                        .transpose()?,
                );
            }
            DdlPropertyValue::ArrayVarchar(items)
        }
        DdlPropertyType::MapVarcharVarchar => {
            return Err(invariant("MAP uses its structural owner"));
        }
    };
    checkpoint(control, 0)?;
    Ok(value)
}

fn evaluate_map(
    scope: DdlPropertyScope,
    source: &str,
    expression: &Expr,
    port: &mut dyn DdlPropertyConstantPort,
    control: &dyn PureCompileControl,
) -> Result<DdlPropertyValue, QueryServiceError> {
    let shape = || {
        invalid(
            scope,
            source,
            expression.span(),
            "Map property requires MAP with two equal-length ARRAY constructors",
        )
    };
    let Expr::FunctionCall(call) = expression else {
        return Err(shape());
    };
    // QualifiedName's pinned suffix identity lowercases even delimited names.
    // No catalog lookup is guessed for a qualified function name.
    let [name] = call.name.parts.as_slice() else {
        return Err(shape());
    };
    if !name.value.eq_ignore_ascii_case("map") {
        return Err(shape());
    }
    let [keys, values] = call.arguments.as_slice() else {
        return Err(shape());
    };
    if call.distinct
        || call.order_by.is_some()
        || call.filter.is_some()
        || call.window.is_some()
        || call.null_treatment.is_some()
        || call.processing_mode.is_some()
        || call
            .arguments
            .iter()
            .any(|argument| argument.name.is_some())
    {
        return Err(shape());
    }
    let (Expr::Array(keys), Expr::Array(values)) = (&keys.value, &values.value) else {
        return Err(shape());
    };
    if keys.values.len() != values.values.len() {
        return Err(shape());
    }
    let mut evaluated_keys = Vec::with_capacity(keys.values.len());
    // Evaluate every original key, then every original value. Key validation
    // follows both arrays, so a NULL/duplicate key never hides a value failure.
    for key in &keys.values {
        evaluated_keys.push(
            match evaluate_value(scope, source, key, DdlPropertyType::Varchar, port, control)? {
                DdlPropertyValue::Null => None,
                DdlPropertyValue::Varchar(key) => Some(key),
                _ => return Err(invariant("Map key lost its required VARCHAR target")),
            },
        );
    }
    let mut evaluated_values = Vec::with_capacity(values.values.len());
    for value in &values.values {
        evaluated_values.push(
            match evaluate_value(
                scope,
                source,
                value,
                DdlPropertyType::Varchar,
                port,
                control,
            )? {
                DdlPropertyValue::Null => None,
                DdlPropertyValue::Varchar(value) => Some(value),
                _ => return Err(invariant("Map value lost its required VARCHAR target")),
            },
        );
    }
    let mut seen = HashSet::new();
    let mut entries = Vec::with_capacity(evaluated_keys.len());
    for (key, value) in evaluated_keys.into_iter().zip(evaluated_values) {
        checkpoint(control, 1)?;
        let Some(key) = key else {
            return Err(invalid(
                scope,
                source,
                expression.span(),
                "Map property key cannot be NULL",
            ));
        };
        if !seen.insert(copy_string(&key, control)?) {
            return Err(invalid(
                scope,
                source,
                expression.span(),
                "Duplicate map property key",
            ));
        }
        entries.push((key, value));
    }
    Ok(DdlPropertyValue::MapVarcharVarchar(entries))
}

fn invariant(message: &str) -> QueryServiceError {
    QueryServiceError::new(QueryServiceErrorKind::Internal, message)
}
fn invalid(
    scope: DdlPropertyScope,
    source: &str,
    span: Span,
    message: impl Into<String>,
) -> QueryServiceError {
    QueryServiceError::from_user_error(scope.to_user_error(source, span, message))
}
fn constant_failure(error: ConstantError) -> QueryServiceError {
    match error {
        ConstantError::Control(error) => QueryServiceError::from_compile_control(error),
        ConstantError::Source(cause) => QueryServiceError::from_failure_cause(cause),
        error => QueryServiceError::from_failure_cause(SharedFailureCause::new(error)),
    }
}
fn checkpoint(control: &dyn PureCompileControl, units: u32) -> Result<(), QueryServiceError> {
    control
        .checkpoint(CompilePhase::Validate, units)
        .map_err(QueryServiceError::from_compile_control)
}
fn observe_bytes(value: &str, control: &dyn PureCompileControl) -> Result<(), QueryServiceError> {
    for chunk in value.as_bytes().chunks(256) {
        checkpoint(control, chunk.len() as u32)?;
    }
    Ok(())
}
fn copy_string(value: &str, control: &dyn PureCompileControl) -> Result<String, QueryServiceError> {
    observe_bytes(value, control)?;
    let value = value.to_owned();
    checkpoint(control, 0)?;
    Ok(value)
}
fn copy_value(
    value: &DdlPropertyValue,
    control: &dyn PureCompileControl,
) -> Result<DdlPropertyValue, QueryServiceError> {
    checkpoint(control, 1)?;
    Ok(match value {
        DdlPropertyValue::Varchar(value) => DdlPropertyValue::Varchar(copy_string(value, control)?),
        DdlPropertyValue::ArrayVarchar(values) => DdlPropertyValue::ArrayVarchar(
            values
                .iter()
                .map(|value| {
                    checkpoint(control, 1)?;
                    value
                        .as_ref()
                        .map(|value| copy_string(value, control))
                        .transpose()
                })
                .collect::<Result<_, _>>()?,
        ),
        DdlPropertyValue::MapVarcharVarchar(entries) => {
            let mut seen = HashSet::new();
            let mut copied = Vec::with_capacity(entries.len());
            for (key, value) in entries {
                checkpoint(control, 1)?;
                if !seen.insert(key.as_str()) {
                    return Err(invariant(
                        "DDL property default contains duplicate map keys",
                    ));
                }
                copied.push((
                    copy_string(key, control)?,
                    value
                        .as_ref()
                        .map(|value| copy_string(value, control))
                        .transpose()?,
                ));
            }
            DdlPropertyValue::MapVarcharVarchar(copied)
        }
        value => value.clone(),
    })
}
