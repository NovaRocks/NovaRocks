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

// Keep the application public path while SPI owns the neutral instruction.
pub use novarocks_spi::connector::DdlPropertyChange;
use novarocks_sql::compiler::RequiredConstantTarget;
use novarocks_type_contract::{CompilePhase, PureCompileControl, ValueKind, ValueLogicalType};

pub mod table;

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

/// A scope owner's capability check at the original unknown-name position.
/// Returning Ok does not declare support: the evaluator still rejects Unknown.
/// Errors retain their original source, control or capability classification.
pub trait DdlPropertyNamePolicy {
    fn check_unknown_property(&mut self, canonical: &str) -> Result<(), QueryServiceError>;
}

struct ClosedNames;
impl DdlPropertyNamePolicy for ClosedNames {
    fn check_unknown_property(&mut self, _canonical: &str) -> Result<(), QueryServiceError> {
        Ok(())
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

pub(super) fn validate_create_authorization(
    source: &str,
    principal: Option<&novarocks_parser::ast::StatementPrincipal>,
    subject: &str,
) -> Result<(), QueryServiceError> {
    if let Some(principal) = principal {
        return Err(super::trino_admission::unsupported_statement(
            source,
            principal.name.span,
            subject,
        ));
    }
    Ok(())
}

pub fn prepare_trino_create_catalog(
    source: &str,
    statement: &novarocks_parser::ast::TrinoCreateCatalog,
    port: &mut dyn DdlPropertyConstantPort,
    control: &dyn PureCompileControl,
) -> Result<novarocks_sql::semantic::command::CatalogCreateCommand, QueryServiceError> {
    checkpoint(control, 0)?;
    validate_create_authorization(
        source,
        statement.principal.as_ref(),
        "CREATE CATALOG AUTHORIZATION",
    )?;
    let assignments = statement_property_assignments(&statement.properties, control)?;
    let evaluated = evaluate_properties(
        DdlPropertyScope::Catalog,
        DdlPropertyEvaluationMode::Create,
        source,
        &[],
        &assignments,
        port,
        control,
        PropertyNameChecks::CatalogUsing,
    )?;
    let properties = catalog_strings(evaluated, control)?;
    let name = copy_string(&statement.catalog_name.value, control)?;
    if statement.connector_name.quoted {
        // The pinned task passes the formatted identifier to ConnectorName.
        // Its connector-name pattern cannot accept the delimited spelling.
        return Err(QueryServiceError::new(
            QueryServiceErrorKind::InvalidValue,
            "CREATE CATALOG USING requires an undelimited connector name",
        ));
    }
    let connector_name = Some(copy_string(&statement.connector_name.value, control)?);
    // COMMENT is a syntax fact. The catalog creation owner consumes no comment.
    Ok(novarocks_sql::semantic::command::CatalogCreateCommand {
        if_not_exists: statement.not_exists,
        name,
        connector_name,
        properties,
    })
}

/// The caller obtains these declarations from its actual namespace mutation
/// session and retains that session through publication.
pub fn prepare_trino_create_schema(
    source: &str,
    statement: &novarocks_parser::ast::TrinoCreateSchema,
    declarations: &[DdlPropertyDeclaration],
    port: &mut dyn DdlPropertyConstantPort,
    control: &dyn PureCompileControl,
) -> Result<novarocks_sql::semantic::CatalogSqlCommand, QueryServiceError> {
    checkpoint(control, 0)?;
    validate_create_authorization(
        source,
        statement.principal.as_ref(),
        "CREATE SCHEMA AUTHORIZATION",
    )?;
    let assignments = statement_property_assignments(&statement.properties, control)?;
    let evaluated = evaluate_ddl_properties(
        DdlPropertyScope::Schema,
        DdlPropertyEvaluationMode::Create,
        source,
        declarations,
        &assignments,
        port,
        control,
    )?;
    let mut properties = Vec::with_capacity(evaluated.properties().len());
    for property in evaluated.into_properties() {
        checkpoint(control, 1)?;
        let value = match property.change {
            DdlPropertyChange::Set(value) | DdlPropertyChange::Reset(value) => value,
        };
        properties.push((property.name, value));
    }
    for part in &statement.schema_name.parts {
        checkpoint(control, 1)?;
        observe_bytes(&part.value, control)?;
    }
    let name = super::lower_object_name(&statement.schema_name);
    checkpoint(control, 0)?;
    Ok(novarocks_sql::semantic::CatalogSqlCommand::CreateDatabase {
        name,
        if_not_exists: statement.not_exists,
        properties,
    })
}

pub(super) fn statement_property_assignments<'a>(
    properties: &'a [novarocks_parser::ast::StatementProperty],
    control: &dyn PureCompileControl,
) -> Result<Vec<DdlPropertyAssignment<'a>>, QueryServiceError> {
    let mut assignments = Vec::with_capacity(properties.len());
    for property in properties {
        checkpoint(control, 1)?;
        assignments.push(DdlPropertyAssignment {
            name: &property.name.value,
            name_span: property.name.span,
            span: property.span,
            value: match &property.value {
                Some(expression) => DdlPropertyAssignmentValue::Expression(expression),
                None => DdlPropertyAssignmentValue::Default {
                    span: property.span,
                },
            },
        });
    }
    Ok(assignments)
}

fn catalog_strings(
    evaluated: EvaluatedDdlProperties,
    control: &dyn PureCompileControl,
) -> Result<Vec<(String, String)>, QueryServiceError> {
    let mut properties = Vec::with_capacity(evaluated.properties().len());
    for property in evaluated.into_properties() {
        checkpoint(control, 1)?;
        let DdlPropertyChange::Set(DdlPropertyValue::Varchar(value)) = property.change else {
            return Err(invariant(
                "Prepared catalog property lost its non-NULL VARCHAR value",
            ));
        };
        properties.push((property.name, value));
    }
    Ok(properties)
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
    let mut policy = ClosedNames;
    evaluate_ddl_properties_with_name_policy(
        scope,
        mode,
        source,
        declarations,
        assignments,
        port,
        control,
        &mut policy,
    )
}

enum PropertyNameChecks<'a> {
    Existing(&'a mut dyn DdlPropertyNamePolicy),
    CatalogUsing,
}

/// Evaluate in original assignment order. Only a missing declaration invokes
/// the name policy, at that assignment, before the original typed Unknown error.
/// Known names, duplicate known names and dynamic Catalog keys bypass it.
pub fn evaluate_ddl_properties_with_name_policy(
    scope: DdlPropertyScope,
    mode: DdlPropertyEvaluationMode,
    source: &str,
    declarations: &[DdlPropertyDeclaration],
    assignments: &[DdlPropertyAssignment<'_>],
    port: &mut dyn DdlPropertyConstantPort,
    control: &dyn PureCompileControl,
    policy: &mut dyn DdlPropertyNamePolicy,
) -> Result<EvaluatedDdlProperties, QueryServiceError> {
    evaluate_properties(
        scope,
        mode,
        source,
        declarations,
        assignments,
        port,
        control,
        PropertyNameChecks::Existing(policy),
    )
}

fn evaluate_properties(
    scope: DdlPropertyScope,
    mode: DdlPropertyEvaluationMode,
    source: &str,
    declarations: &[DdlPropertyDeclaration],
    assignments: &[DdlPropertyAssignment<'_>],
    port: &mut dyn DdlPropertyConstantPort,
    control: &dyn PureCompileControl,
    mut name_checks: PropertyNameChecks<'_>,
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
        if matches!(name_checks, PropertyNameChecks::CatalogUsing)
            && assignment.name.eq_ignore_ascii_case("type")
        {
            return Err(invalid(
                DdlPropertyScope::Catalog,
                source,
                assignment.name_span,
                "Catalog property 'type' cannot override the USING connector",
            ));
        }
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
                    .map_or_else(
                        || {
                            if let PropertyNameChecks::Existing(policy) = &mut name_checks {
                                policy.check_unknown_property(&name)?;
                            }
                            Err(invalid(
                                scope,
                                source,
                                assignment.name_span,
                                format!("Unknown property '{name}'"),
                            ))
                        },
                        Ok,
                    )?,
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
                let value = evaluate_value(scope, source, expression, value_type, port, control)?;
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
    evaluate_required_property_value(expression, value_type, port, control, |message, cause| {
        let error = scope.to_user_error(source, expression.span(), message);
        match cause {
            Some(cause) => QueryServiceError::from_user_error_with_cause(error, cause),
            None => QueryServiceError::from_user_error(error),
        }
    })
}

/// Run the original required-constant port and project its complete result.
/// The caller authors only its own domain's data-error identity.
pub(super) fn evaluate_required_property_value(
    expression: &Expr,
    value_type: DdlPropertyType,
    port: &mut dyn DdlPropertyConstantPort,
    control: &dyn PureCompileControl,
    invalid_value: impl FnOnce(String, Option<SharedFailureCause>) -> QueryServiceError,
) -> Result<DdlPropertyValue, QueryServiceError> {
    let target = match value_type {
        DdlPropertyType::Boolean => RequiredConstantTarget::Boolean,
        DdlPropertyType::Integer => RequiredConstantTarget::Integer,
        DdlPropertyType::Bigint => RequiredConstantTarget::Bigint,
        DdlPropertyType::Double => RequiredConstantTarget::Double,
        DdlPropertyType::Varchar => RequiredConstantTarget::Varchar,
        DdlPropertyType::ArrayVarchar => RequiredConstantTarget::ArrayVarchar,
        DdlPropertyType::MapVarcharVarchar => RequiredConstantTarget::MapVarcharVarchar,
    };
    let constant = evaluate_required_constant(expression, target, port, control, invalid_value)?;
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
            (DdlPropertyType::MapVarcharVarchar, _) => target.admits_map_source(binding),
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
            let ValueKind::Map { key, value } = binding.value_type().kind() else {
                return Err(missing());
            };
            if matches!(
                key.kind(),
                ValueKind::Scalar(arrow::datatypes::DataType::Null)
            ) {
                let empty = constant
                    .unknown_map_is_empty_observed(phase, control)
                    .map_err(constant_failure)?
                    .ok_or_else(missing)?;
                if !empty {
                    return Err(invariant(
                        "Required UNKNOWN-key map contains a resident entry",
                    ));
                }
                DdlPropertyValue::MapVarcharVarchar(Vec::new())
            } else {
                let map = if matches!(
                    value.kind(),
                    ValueKind::Scalar(arrow::datatypes::DataType::Null)
                ) {
                    constant.utf8_map_with_unknown_values_observed(phase, control)
                } else {
                    constant.utf8_map_observed(phase, control)
                }
                .map_err(constant_failure)?
                .ok_or_else(missing)?;
                let mut entries = Vec::with_capacity(map.len());
                for index in 0..map.len() {
                    checkpoint(control, 1)?;
                    let (key, value) = map.item(index, phase, control).map_err(constant_failure)?;
                    let key = copy_string(key.ok_or_else(missing)?, control)?;
                    let value = value.map(|text| copy_string(text, control)).transpose()?;
                    entries.push((key, value));
                }
                DdlPropertyValue::MapVarcharVarchar(entries)
            }
        }
    };
    checkpoint(control, 0)?;
    Ok(value)
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
pub(super) fn evaluate_required_constant(
    expression: &Expr,
    target: RequiredConstantTarget,
    port: &mut dyn DdlPropertyConstantPort,
    control: &dyn PureCompileControl,
    invalid_value: impl FnOnce(String, Option<SharedFailureCause>) -> QueryServiceError,
) -> Result<ConstantValue, QueryServiceError> {
    checkpoint(control, 0)?;
    let constant = match port.evaluate(expression, target) {
        Ok(constant) => constant,
        Err(DdlPropertyConstantError::Failure(error)) => return Err(error),
        Err(DdlPropertyConstantError::InvalidValue { message, cause }) => {
            return Err(invalid_value(message, cause));
        }
    };
    checkpoint(control, 0)?;
    Ok(constant)
}

pub(super) fn constant_failure(error: ConstantError) -> QueryServiceError {
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
pub(super) fn copy_string(
    value: &str,
    control: &dyn PureCompileControl,
) -> Result<String, QueryServiceError> {
    observe_bytes(value, control)?;
    let value = value.to_owned();
    checkpoint(control, 0)?;
    Ok(value)
}
pub(super) fn copy_value(
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
