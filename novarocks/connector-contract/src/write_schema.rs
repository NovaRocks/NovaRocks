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

//! Pure Arrow field contracts shared by writer recipes and write
//! result relations. Runtime Arrow arrays and provider capabilities stay out.
use crate::{ConnectorError, ConnectorErrorKind};
use arrow_schema::{DataType, Field};
use novarocks_type_contract::{
    ArrowTypeProfile, MAX_VALUE_TYPE_DEPTH, ValueTypeError, ValueTypeVisit,
    visit_arrow_layout_events_observed,
};

pub const WRITE_FIELD_ALLOCATION_CHARGE: usize = 128;
const TYPE_ALLOCATION_CHARGE: usize = 64;

pub use novarocks_type_contract::{
    arrow_data_types_exact, arrow_fields_exact, arrow_schemas_exact,
};

fn resource_exhausted(message: &'static str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::ResourceExhausted, message)
}

pub fn charge_write_schema(decoded_bytes: &mut usize, amount: usize) -> Result<(), ConnectorError> {
    *decoded_bytes = decoded_bytes
        .checked_add(amount)
        .ok_or_else(|| resource_exhausted("write relation schema allocation charge overflowed"))?;
    Ok(())
}

fn invalid_type(error: ValueTypeError) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, error.to_string())
}

/// The writer owner's original per-field charge. This reads only borrowed
/// attributes, so the flat type graph can admit the same domain before building
/// an Arrow Field. It does not validate logical metadata or grant allocation.
fn charge_write_field_header(name: &str, decoded_bytes: &mut usize) -> Result<(), ConnectorError> {
    charge_write_schema(decoded_bytes, WRITE_FIELD_ALLOCATION_CHARGE)?;
    charge_write_schema(decoded_bytes, name.len())
}

fn charge_write_metadata_entry(
    key: &str,
    value: &str,
    decoded_bytes: &mut usize,
) -> Result<(), ConnectorError> {
    charge_write_schema(decoded_bytes, 2 * size_of::<String>())?;
    charge_write_schema(decoded_bytes, key.len())?;
    charge_write_schema(decoded_bytes, value.len())
}

/// The writer depth and original type-node charge, including a timestamp's
/// optional timezone bytes. This does not reinterpret the carrier, parse a
/// timezone or impose the Value owner's unrelated unfolded-node bound.
pub fn charge_write_type_header(
    depth: usize,
    timestamp_timezone: Option<&str>,
    decoded_bytes: &mut usize,
) -> Result<(), ConnectorError> {
    if depth > MAX_VALUE_TYPE_DEPTH {
        return Err(invalid_type(ValueTypeError::TooDeep));
    }
    charge_write_schema(decoded_bytes, TYPE_ALLOCATION_CHARGE)?;
    if let Some(timezone) = timestamp_timezone {
        charge_write_schema(decoded_bytes, timezone.len())?;
    }
    Ok(())
}

/// Events at the original writer-law traversal. Before events permit the
/// caller to admit opaque source lookup/iteration before it starts. Completed
/// events retain the existing observer trace and partial charge behavior.
#[derive(Clone, Copy, Debug)]
pub enum WriteSchemaVisit<'a> {
    BeforeField(&'a Field),
    BeforeType(&'a DataType),
    Completed,
}

/// Borrow an existing caller context; this function owns no entry, footer,
/// resource budget or new control scope. It uses the same validation grammar
/// as the plain and completed-only observed entry points below.
pub fn validate_write_field_schema_events<E: From<ConnectorError>>(
    field: &Field,
    depth: usize,
    decoded_bytes: &mut usize,
    mut observe: impl FnMut(WriteSchemaVisit<'_>) -> Result<(), E>,
) -> Result<(), E> {
    validate_field_core(field, depth, decoded_bytes, true, &mut observe)
}

/// Validate original provider attributes and structural/resource laws beside
/// an independently supplied complete typed binding. Source attributes confer
/// no execution semantics; the caller checks the binding/layout correspondence.
/// Every original name, metadata byte and child occurrence is still observed.
pub fn validate_write_source_field_schema_events<E: From<ConnectorError>>(
    field: &Field,
    depth: usize,
    decoded_bytes: &mut usize,
    mut observe: impl FnMut(WriteSchemaVisit<'_>) -> Result<(), E>,
) -> Result<(), E> {
    validate_field_core(field, depth, decoded_bytes, false, &mut observe)
}

pub(crate) fn validate_write_bound_field_schema_events<E: From<ConnectorError>>(
    binding: crate::ConnectorWriteFieldRef<'_>,
    depth: usize,
    decoded_bytes: &mut usize,
    mut observe: impl FnMut(WriteSchemaVisit<'_>) -> Result<(), E>,
) -> Result<(), E> {
    validate_field_core(
        binding.field(),
        depth,
        decoded_bytes,
        binding.execution_type().is_none(),
        &mut observe,
    )
}

pub fn validate_write_field_schema(
    field: &Field,
    depth: usize,
    decoded_bytes: &mut usize,
) -> Result<(), ConnectorError> {
    // The existing unobserved contract delegates the same traversal. It does
    // not manufacture a PureCompileControl or certify a preparation budget.
    validate_field_core(field, depth, decoded_bytes, true, &mut |_| Ok(()))
}

/// Validate the same writer domain on the caller's existing observer. The
/// caller owns entry, ordinary/success tails and resource admission; this
/// traversal neither creates a scope nor copies fields. HashMap iteration and
/// logical metadata lookup remain opaque library work, not a claimed internal
/// cooperative quantum or an allocator grant.
pub fn validate_write_field_schema_observed<E: From<ConnectorError>>(
    field: &Field,
    depth: usize,
    decoded_bytes: &mut usize,
    mut observe: impl FnMut() -> Result<(), E>,
) -> Result<(), E> {
    validate_field_core(field, depth, decoded_bytes, true, &mut |event| {
        if matches!(event, WriteSchemaVisit::Completed) {
            observe()?;
        }
        Ok(())
    })
}

fn validate_field_core<E: From<ConnectorError>>(
    field: &Field,
    depth: usize,
    decoded_bytes: &mut usize,
    validate_annotations: bool,
    observe: &mut impl FnMut(WriteSchemaVisit<'_>) -> Result<(), E>,
) -> Result<(), E> {
    observe_field(field, decoded_bytes, validate_annotations, observe)?;
    validate_type_core(
        field.data_type(),
        depth,
        decoded_bytes,
        validate_annotations,
        observe,
    )
}

fn observe_field<E: From<ConnectorError>>(
    field: &Field,
    decoded_bytes: &mut usize,
    validate_annotations: bool,
    observe: &mut impl FnMut(WriteSchemaVisit<'_>) -> Result<(), E>,
) -> Result<(), E> {
    observe(WriteSchemaVisit::BeforeField(field))?;
    if validate_annotations {
        novarocks_type_contract::field_logical_type(field).map_err(invalid_type)?;
    }
    charge_write_field_header(field.name(), decoded_bytes)?;
    observe(WriteSchemaVisit::Completed)?;
    for (key, value) in field.metadata() {
        charge_write_metadata_entry(key, value, decoded_bytes)?;
        observe(WriteSchemaVisit::Completed)?;
    }
    Ok(())
}

pub fn validate_write_data_type(
    data_type: &DataType,
    depth: usize,
    decoded_bytes: &mut usize,
) -> Result<(), ConnectorError> {
    validate_type_core(data_type, depth, decoded_bytes, true, &mut |_| Ok(()))
}

/// Borrow the caller's observer for the original type traversal. All child
/// occurrences remain charged, including shared Arrow Field backing.
pub fn validate_write_data_type_observed<E: From<ConnectorError>>(
    data_type: &DataType,
    depth: usize,
    decoded_bytes: &mut usize,
    mut observe: impl FnMut() -> Result<(), E>,
) -> Result<(), E> {
    validate_type_core(data_type, depth, decoded_bytes, true, &mut |event| {
        if matches!(event, WriteSchemaVisit::Completed) {
            observe()?;
        }
        Ok(())
    })
}

fn validate_type_core<E: From<ConnectorError>>(
    data_type: &DataType,
    depth: usize,
    decoded_bytes: &mut usize,
    validate_annotations: bool,
    observe: &mut impl FnMut(WriteSchemaVisit<'_>) -> Result<(), E>,
) -> Result<(), E> {
    enum WalkError<E> {
        Writer(E),
        Shape(ValueTypeError),
    }
    impl<E> From<ValueTypeError> for WalkError<E> {
        fn from(error: ValueTypeError) -> Self {
            Self::Shape(error)
        }
    }
    visit_arrow_layout_events_observed(
        data_type,
        ArrowTypeProfile::Physical,
        |visit, coordinate| {
            let result = match visit {
                ValueTypeVisit::Field(field) => {
                    observe_field(field, decoded_bytes, validate_annotations, observe)
                }
                ValueTypeVisit::TypeNode(data_type) => (|| {
                    observe(WriteSchemaVisit::BeforeType(data_type))?;
                    let timezone = match data_type {
                        DataType::Timestamp(_, timezone) => timezone.as_deref(),
                        _ => None,
                    };
                    let logical_depth =
                        coordinate.logical().saturating_add(depth.saturating_sub(1));
                    charge_write_type_header(logical_depth, timezone, decoded_bytes)?;
                    observe(WriteSchemaVisit::Completed)
                })(),
                ValueTypeVisit::ChildEdge(_) => Ok(()),
            };
            result.map_err(WalkError::Writer)
        },
    )
    .map_err(|error| match error {
        WalkError::Writer(error) => error,
        WalkError::Shape(error) => E::from(invalid_type(error)),
    })
}

#[cfg(test)]
mod tests;
