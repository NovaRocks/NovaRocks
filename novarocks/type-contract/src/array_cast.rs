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

//! Explicit array CAST conversion boundaries. These facts grant no kernel capability.
use crate::{FunctionValueType, ValueLogicalType};
use arrow_schema::DataType;

/// A present mode selects the admitted explicit array conversion semantics.
/// Absence on the containing CAST retains its existing scalar/implicit policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum ArrayCastFailureMode {
    ReportError,
    OutputNull,
}

/// Validate only the boundary's type and successful-NULL obligations. Full
/// nested domains, layouts, Fields and conversion capability remain with the
/// original typed declaration, validation and prepared recipe authors.
pub fn array_cast_mode_types_valid(
    source: &FunctionValueType,
    result: &FunctionValueType,
    mode: ArrayCastFailureMode,
) -> bool {
    source.logical_type() == ValueLogicalType::Physical
        && result.logical_type() == ValueLogicalType::Physical
        && matches!(source.data_type(), DataType::List(_) | DataType::Null)
        && matches!(result.data_type(), DataType::List(_))
        && (!source.nullable() || result.nullable())
        && (mode != ArrayCastFailureMode::OutputNull || result.nullable())
}

/// Observe the original semantic nodes while checking nested NULL and nominal
/// obligations. Unknown values may acquire a declared target; a non-NULL
/// shared carrier cannot establish a nominal domain. This grants no recipe.
pub fn array_cast_mode_types_valid_observed<E>(
    source: &FunctionValueType,
    result: &FunctionValueType,
    mode: ArrayCastFailureMode,
    mut observe: impl FnMut() -> Result<(), E>,
) -> Result<bool, E> {
    observe()?;
    if !array_cast_mode_types_valid(source, result, mode) {
        return Ok(false);
    }
    fn visit<E>(
        source: &crate::ValueTypeRef,
        result: &crate::ValueTypeRef,
        observe: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<bool, E> {
        observe()?;
        if source.nullable() && !result.nullable() {
            return Ok(false);
        }
        if matches!(source.kind(), crate::ValueKind::Scalar(DataType::Null)) {
            return Ok(result.nullable());
        }
        // Only this explicit array boundary admits the actual JSON text
        // conversion author. A common UTF8 carrier alone still grants nothing.
        match (source.kind(), result.kind()) {
            (crate::ValueKind::Nominal(ValueLogicalType::Json), crate::ValueKind::String) => {
                return Ok(result.nullable()); // JSON null is a successful SQL NULL.
            }
            (crate::ValueKind::String, crate::ValueKind::Nominal(ValueLogicalType::Json)) => {
                return Ok(true); // The admitted converter encodes a JSON string.
            }
            _ => {}
        }
        if !crate::preserves_value_domains_observed(source, result, |_| observe())? {
            // Lists may contain an explicitly authored unknown-to-nominal
            // conversion. Check every paired child instead of granting a
            // general shared-carrier exception.
            if !matches!(
                (source.kind(), result.kind()),
                (crate::ValueKind::List(_), crate::ValueKind::List(_))
            ) {
                return Ok(false);
            }
        }
        if source.child_count() == result.child_count() {
            for index in 0..source.child_count() {
                if !visit(
                    source.child_type_at(index).expect("actual child"),
                    result.child_type_at(index).expect("paired child"),
                    observe,
                )? {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }
    visit(source.value_type(), result.value_type(), &mut observe)
}

#[cfg(test)]
#[path = "array_cast_guard_tests.rs"]
mod guard_tests;
