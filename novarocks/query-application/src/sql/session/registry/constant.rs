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

impl PropertyDeclaration {
    /// SQL has already performed the permitted conversion and evaluation.
    /// Keep the complete source binding and selected ordinal through handoff.
    pub fn validate_sql_value(
        self: &Arc<Self>,
        constant: &ConstantValue,
        control: &dyn PureCompileControl,
    ) -> Result<ValidatedSessionProperty, SessionPropertyError> {
        let phase = CompilePhase::Validate;
        control
            .checkpoint(phase, 0)
            .map_err(ConstantError::from)
            .map_err(SessionPropertyError::Constant)?;
        let invalid_type = || SessionPropertyError::InvalidValue {
            name: self.name.clone(),
            detail: format!(
                "Expected {} after SQL conversion",
                self.value_type.sql_name()
            ),
        };
        if self.project.is_none() {
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
            return Err(SessionPropertyError::InvalidValue {
                name: self.name.clone(),
                detail: "Session property value must not be null".into(),
            });
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
        self.validate_value(&value)?;
        let raw = value.render()?;
        control
            .checkpoint(phase, 0)
            .map_err(ConstantError::from)
            .map_err(SessionPropertyError::Constant)?;
        Ok(ValidatedSessionProperty {
            declaration: self.clone(),
            value,
            raw,
            scope: SessionPropertyScope::System,
            constant_source: Some(constant.clone()),
        })
    }
}
impl SessionPropertyRegistry {
    pub fn prepare_sql_constant(
        &self,
        name: &str,
        value: &ConstantValue,
        control: &dyn PureCompileControl,
    ) -> Result<ValidatedSessionProperty, SessionPropertyError> {
        self.lookup_system(name, SessionPropertyOrigin::Sql)?
            .validate_sql_value(value, control)
    }
}
