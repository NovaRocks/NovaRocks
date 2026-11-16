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

//! Checked borrowing of a SQL-prepared typed constant. This performs no cast,
//! inference, formatting/reparse, or kernel execution.
use super::*;
use novarocks_constant_contract::{ConstantError, ConstantValue};
use novarocks_type_contract::{CompilePhase, PureCompileControl, ValueKind, ValueLogicalType};

pub struct SqlPropertyDeclaration {
    declaration: Arc<PropertyDeclaration>,
    scope: SessionPropertyScope,
}

impl SqlPropertyDeclaration {
    pub fn value_type(&self) -> SessionPropertyType {
        self.declaration.value_type()
    }

    pub fn qualified_name(&self) -> String {
        self.scope.qualified_name(self.declaration.name())
    }

    pub fn validate_value(
        self,
        constant: &ConstantValue,
        control: &dyn PureCompileControl,
    ) -> Result<ValidatedSessionProperty, SessionPropertyError> {
        self.declaration
            .validate_scoped_sql_value(constant, control, self.scope)
    }
}

impl PropertyDeclaration {
    /// SQL has already performed conversion and evaluation. Keep the selected
    /// source binding through the handoff to the session owner.
    fn validate_scoped_sql_value(
        self: &Arc<Self>,
        constant: &ConstantValue,
        control: &dyn PureCompileControl,
        scope: SessionPropertyScope,
    ) -> Result<ValidatedSessionProperty, SessionPropertyError> {
        let phase = CompilePhase::Validate;
        control
            .checkpoint(phase, 0)
            .map_err(ConstantError::from)
            .map_err(SessionPropertyError::Constant)?;
        let qualify = || scope.qualified_name(self.name());
        let invalid_type = || SessionPropertyError::InvalidValue {
            name: qualify(),
            detail: format!(
                "Expected {} after SQL conversion",
                self.value_type.sql_name()
            ),
        };
        if self.project.is_some() != matches!(scope, SessionPropertyScope::System) {
            return Err(SessionPropertyError::ProjectionInvariant);
        }
        let binding = constant.value_type();
        if binding.logical_type() != ValueLogicalType::Physical {
            return Err(invalid_type());
        }
        let expected = self.value_type.data_type();
        let exact_kind = match (self.value_type, binding.value_type().kind()) {
            (SessionPropertyType::Varchar, ValueKind::String) => true,
            (_, ValueKind::Scalar(data_type)) => data_type == &expected,
            _ => false,
        };
        if !exact_kind {
            return Err(invalid_type());
        }
        if constant
            .is_null_observed(phase, control)
            .map_err(SessionPropertyError::Constant)?
        {
            return Err(SessionPropertyError::NullValue { name: qualify() });
        }
        let missing = || SessionPropertyError::ProjectionInvariant;
        let value = match self.value_type {
            SessionPropertyType::Boolean => SessionPropertyValue::Boolean(
                constant
                    .try_boolean()
                    .map_err(SessionPropertyError::Constant)?
                    .ok_or_else(missing)?,
            ),
            SessionPropertyType::Integer => {
                let value = constant
                    .signed_integer_observed(phase, control)
                    .map_err(SessionPropertyError::Constant)?
                    .ok_or_else(missing)?;
                SessionPropertyValue::Integer(
                    i32::try_from(value).map_err(|_| SessionPropertyError::ProjectionInvariant)?,
                )
            }
            SessionPropertyType::Bigint => SessionPropertyValue::Bigint(
                constant
                    .int64_observed(phase, control)
                    .map_err(SessionPropertyError::Constant)?
                    .ok_or_else(missing)?,
            ),
            SessionPropertyType::Double => SessionPropertyValue::Double(f64::from_bits(
                constant
                    .try_f64_bits()
                    .map_err(SessionPropertyError::Constant)?
                    .ok_or_else(missing)?,
            )),
            SessionPropertyType::Varchar => SessionPropertyValue::Varchar(
                constant
                    .try_utf8_borrowed_observed(phase, control)
                    .map_err(SessionPropertyError::Constant)?
                    .ok_or_else(missing)?
                    .to_owned(),
            ),
        };
        self.validate_value(&value).map_err(|error| match error {
            SessionPropertyError::InvalidValue { detail, .. } => {
                SessionPropertyError::InvalidValue {
                    name: qualify(),
                    detail,
                }
            }
            original => original,
        })?;
        let raw = value.render()?;
        control
            .checkpoint(phase, 0)
            .map_err(ConstantError::from)
            .map_err(SessionPropertyError::Constant)?;
        Ok(ValidatedSessionProperty {
            declaration: self.clone(),
            value,
            raw,
            scope,
            constant_source: Some(constant.clone()),
        })
    }
}
impl CatalogPropertyMetadata {
    pub fn lookup_sql(&self, name: &str) -> Result<SqlPropertyDeclaration, SessionPropertyError> {
        Ok(SqlPropertyDeclaration {
            declaration: self.lookup(&name.to_ascii_lowercase())?,
            scope: SessionPropertyScope::Catalog {
                catalog: self.catalog().to_owned(),
                identity: self.identity().cloned(),
            },
        })
    }
}

impl SessionPropertyRegistry {
    pub fn lookup_sql_system(
        &self,
        name: &str,
    ) -> Result<SqlPropertyDeclaration, SessionPropertyError> {
        Ok(SqlPropertyDeclaration {
            declaration: self.lookup_system(name, SessionPropertyOrigin::Sql)?,
            scope: SessionPropertyScope::System,
        })
    }

    pub fn prepare_sql_constant(
        &self,
        name: &str,
        value: &ConstantValue,
        control: &dyn PureCompileControl,
    ) -> Result<ValidatedSessionProperty, SessionPropertyError> {
        self.lookup_sql_system(name)?.validate_value(value, control)
    }
}
