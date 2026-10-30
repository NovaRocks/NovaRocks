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

use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields};

use crate::{FunctionValueType, ValueLogicalType, wider_type};

#[derive(Debug)]
pub enum ComparisonCoercionFailure {
    Operands(novarocks_failure_contract::BoundedDetail),
    InvalidPrecision { precision: i16, scale: i8 },
    PrecisionExtent { precision: i16, scale: i8 },
    SemanticRule(novarocks_failure_contract::BoundedDetail),
    ValueType(crate::ExecutionValueTypeError),
    Layout(crate::owned_resources::layout::LayoutResourceError),
    Vec(crate::ControlResourceError),
    HashMap(crate::owned_resources::hashmap::HashMapResourceError),
    BTree(crate::owned_resources::btree::BTreeResourceError),
}
impl ComparisonCoercionFailure {
    fn operands(detail: impl std::fmt::Display) -> Self {
        Self::Operands(novarocks_failure_contract::BoundedDetail::truncating_display(detail))
    }
    fn from_construction(cause: crate::ValueConstructionFailure<std::convert::Infallible>) -> Self {
        match cause {
            crate::ValueConstructionFailure::ValueType(cause) => Self::ValueType(cause),
            // A construction Rule is this domain's rejected semantic unification,
            // distinct from its typed physical metadata and allocation failures.
            crate::ValueConstructionFailure::Rule(detail) => Self::SemanticRule(
                novarocks_failure_contract::BoundedDetail::truncating(&detail),
            ),
            crate::ValueConstructionFailure::Observer(never) => match never {},
            crate::ValueConstructionFailure::Layout(cause) => Self::Layout(cause),
            crate::ValueConstructionFailure::Vec(cause) => Self::Vec(cause),
            crate::ValueConstructionFailure::HashMap(cause) => Self::HashMap(cause),
            crate::ValueConstructionFailure::BTree(cause) => Self::BTree(cause),
        }
    }
}
impl std::fmt::Display for ComparisonCoercionFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Operands(detail) | Self::SemanticRule(detail) => f.write_str(detail.as_str()),
            Self::InvalidPrecision { precision, scale } => write!(
                f,
                "decimal comparison invalid precision {precision}, scale {scale}"
            ),
            Self::PrecisionExtent { precision, scale } => write!(
                f,
                "decimal comparison precision overflow: requires Decimal256({precision}, {scale}), maximum 76 digits"
            ),
            Self::ValueType(cause) => cause.fmt(f),
            Self::Layout(cause) => cause.fmt(f),
            Self::Vec(cause) => write!(f, "comparison type construction: {cause:?}"),
            Self::HashMap(cause) => cause.fmt(f),
            Self::BTree(cause) => cause.fmt(f),
        }
    }
}
impl std::error::Error for ComparisonCoercionFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::ValueType(cause) => Some(cause),
            Self::Layout(cause) => Some(cause),
            Self::HashMap(cause) => Some(cause),
            Self::BTree(cause) => Some(cause),
            _ => None,
        }
    }
}
impl novarocks_failure_contract::FailureProjection for ComparisonCoercionFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{
            FailureClass, FailureFact, FailureReason, RepresentationBoundary, RepresentationFacts,
            RepresentationLayout, domain,
        };
        let (class, code) = match self {
            Self::Operands(_) => (FailureClass::Invariant, 66),
            Self::InvalidPrecision { .. } => (FailureClass::Invariant, 67),
            Self::PrecisionExtent { .. } => (FailureClass::Representation, 68),
            Self::SemanticRule(_) => (FailureClass::Data, 69),
            Self::ValueType(cause) => return cause.failure_fact(),
            Self::Layout(cause) => return cause.failure_fact(),
            Self::HashMap(cause) => return cause.failure_fact(),
            Self::BTree(cause) => return cause.failure_fact(),
            Self::Vec(crate::ControlResourceError::Control(cause)) => return cause.failure_fact(),
            Self::Vec(crate::ControlResourceError::SourceModel(_)) => (FailureClass::Invariant, 70),
        };
        let fact = FailureFact::new(
            class,
            FailureReason::new(
                domain::TYPE_CONTRACT,
                std::num::NonZeroU16::new(code).unwrap(),
            ),
        );
        if let Self::PrecisionExtent { precision, .. } = self {
            fact.with_representation(RepresentationFacts {
                boundary: RepresentationBoundary::MinimumUnit,
                layout: RepresentationLayout {
                    domain: domain::TYPE_CONTRACT,
                    code: std::num::NonZeroU16::new(3).unwrap(),
                },
                required: u64::try_from(*precision).ok(),
                extent_limit: Some(76),
                representable_prefix: None,
            })
        } else {
            fact
        }
    }
}
impl novarocks_failure_contract::OuterFailureProjection for ComparisonCoercionFailure {}

/// Common decimal type for a decimal-vs-decimal comparison / equi-join key.
/// scale = max(s1,s2); precision = max(p1-s1, p2-s2) + scale. Promotes to
/// Decimal256 when the precision exceeds 38 or either side is already 256;
/// errors when it would exceed 76 (Decimal256 max). This is the single source
/// shared by `comparison_common_type`, lower binary_pred, and lower join-key.
pub fn decimal_compare_type(
    left: &DataType,
    right: &DataType,
) -> Result<DataType, ComparisonCoercionFailure> {
    let (lp, ls, left_is_256) = match left {
        DataType::Decimal128(p, s) => (*p, *s, false),
        DataType::Decimal256(p, s) => (*p, *s, true),
        _ => {
            return Err(ComparisonCoercionFailure::operands(format_args!(
                "decimal comparison requires decimal operands (left={left:?}, right={right:?})"
            )));
        }
    };
    let (rp, rs, right_is_256) = match right {
        DataType::Decimal128(p, s) => (*p, *s, false),
        DataType::Decimal256(p, s) => (*p, *s, true),
        _ => {
            return Err(ComparisonCoercionFailure::operands(format_args!(
                "decimal comparison requires decimal operands (left={left:?}, right={right:?})"
            )));
        }
    };

    let target_scale: i8 = ls.max(rs);
    let lhs_int_digits: i16 = (lp as i16) - (ls as i16);
    let rhs_int_digits: i16 = (rp as i16) - (rs as i16);
    let int_digits: i16 = lhs_int_digits.max(rhs_int_digits).max(0);
    let target_precision: i16 = int_digits + (target_scale as i16);
    if target_precision <= 0 {
        return Err(ComparisonCoercionFailure::InvalidPrecision {
            precision: target_precision,
            scale: target_scale,
        });
    }
    let need_decimal256 = left_is_256 || right_is_256 || target_precision > 38;
    if need_decimal256 {
        if target_precision > 76 {
            return Err(ComparisonCoercionFailure::PrecisionExtent {
                precision: target_precision,
                scale: target_scale,
            });
        }
        let target_precision_u8 = target_precision as u8;
        return Ok(DataType::Decimal256(target_precision_u8, target_scale));
    }
    let target_precision_u8 = target_precision as u8;
    Ok(DataType::Decimal128(target_precision_u8, target_scale))
}

/// Comparison operand common type. Single authority shared by analyzer,
/// execution `normalize_comparison_types`, and lower binary_pred / join-key.
/// `Ok(None)`: operands already equal, OR pair is out of scope (temporal,
/// largeint-decimal, cross-family) and is left to the caller.
/// `Ok(Some(t))`: nullable / numeric / decimal / string-numeric pair, including
/// same-shape complex containers whose nested scalar fields have a common type,
/// -> cast BOTH operands to `t`.
/// `Err`: decimal-compatible pair whose common precision exceeds Decimal256
/// (> 76).
pub fn comparison_common_type(
    left: &DataType,
    right: &DataType,
) -> Result<Option<DataType>, ComparisonCoercionFailure> {
    if left == right {
        return Ok(None);
    }
    if left == &DataType::Null {
        return Ok(Some(right.clone()));
    }
    if right == &DataType::Null {
        return Ok(Some(left.clone()));
    }
    if let Some(common) = comparison_common_complex_type(left, right)? {
        return Ok(Some(common));
    }
    let is_int = |dt: &DataType| {
        matches!(
            dt,
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
        )
    };
    let is_bool = |dt: &DataType| matches!(dt, DataType::Boolean);
    let is_float = |dt: &DataType| matches!(dt, DataType::Float32 | DataType::Float64);
    let int_as_zero_scale_decimal = |dt: &DataType| -> Option<DataType> {
        match dt {
            DataType::Boolean => Some(DataType::Decimal128(1, 0)),
            DataType::Int8 => Some(DataType::Decimal128(3, 0)),
            DataType::Int16 => Some(DataType::Decimal128(5, 0)),
            DataType::Int32 => Some(DataType::Decimal128(10, 0)),
            DataType::Int64 => Some(DataType::Decimal128(19, 0)),
            _ => None,
        }
    };
    let is_decimal =
        |dt: &DataType| matches!(dt, DataType::Decimal128(_, _) | DataType::Decimal256(_, _));

    if (crate::is_largeint_data_type(left) && (is_int(right) || is_bool(right)))
        || ((is_int(left) || is_bool(left)) && crate::is_largeint_data_type(right))
    {
        return Ok(Some(DataType::FixedSizeBinary(crate::LARGEINT_BYTE_WIDTH)));
    }
    if (is_bool(left) && is_int(right)) || (is_int(left) && is_bool(right)) {
        return Ok(Some(DataType::Int64));
    }
    if (is_bool(left) && is_float(right)) || (is_float(left) && is_bool(right)) {
        return Ok(Some(DataType::Float64));
    }
    if is_int(left) && is_int(right) {
        return Ok(Some(DataType::Int64));
    }
    if (is_int(left) && is_float(right)) || (is_float(left) && is_int(right)) {
        return Ok(Some(DataType::Float64));
    }
    if is_float(left) && is_float(right) {
        return Ok(Some(DataType::Float64));
    }
    if is_decimal(left) && is_decimal(right) {
        return Ok(Some(decimal_compare_type(left, right)?));
    }
    if let Some(left_decimal) = int_as_zero_scale_decimal(left)
        && is_decimal(right)
    {
        return Ok(Some(decimal_compare_type(&left_decimal, right)?));
    }
    if let Some(right_decimal) = int_as_zero_scale_decimal(right)
        && is_decimal(left)
    {
        return Ok(Some(decimal_compare_type(left, &right_decimal)?));
    }
    if (is_float(left) && is_decimal(right)) || (is_decimal(left) && is_float(right)) {
        return Ok(Some(DataType::Float64));
    }
    let is_string = |dt: &DataType| matches!(dt, DataType::Utf8 | DataType::LargeUtf8);
    let is_numeric = |dt: &DataType| is_int(dt) || is_float(dt) || is_decimal(dt);
    if is_string(left) && is_numeric(right) {
        return Ok(Some(right.clone()));
    }
    if is_numeric(left) && is_string(right) {
        return Ok(Some(left.clone()));
    }

    Ok(None)
}

fn comparison_common_complex_type(
    left: &DataType,
    right: &DataType,
) -> Result<Option<DataType>, ComparisonCoercionFailure> {
    match (left, right) {
        (DataType::List(left_item), DataType::List(right_item)) => {
            let Some((item, changed)) = comparison_common_field(left_item, right_item)? else {
                return Ok(None);
            };
            Ok(changed.then_some(DataType::List(item)))
        }
        (DataType::LargeList(left_item), DataType::LargeList(right_item)) => {
            let Some((item, changed)) = comparison_common_field(left_item, right_item)? else {
                return Ok(None);
            };
            Ok(changed.then_some(DataType::LargeList(item)))
        }
        (
            DataType::FixedSizeList(left_item, left_size),
            DataType::FixedSizeList(right_item, right_size),
        ) if left_size == right_size => {
            let Some((item, changed)) = comparison_common_field(left_item, right_item)? else {
                return Ok(None);
            };
            Ok(changed.then_some(DataType::FixedSizeList(item, *left_size)))
        }
        (DataType::Struct(left_fields), DataType::Struct(right_fields))
            if left_fields.len() == right_fields.len() =>
        {
            comparison_common_struct_type(left_fields, right_fields)
        }
        (
            DataType::Map(left_entries, left_ordered),
            DataType::Map(right_entries, right_ordered),
        ) if left_ordered == right_ordered => {
            let Some((entries, changed)) = comparison_common_field(left_entries, right_entries)?
            else {
                return Ok(None);
            };
            Ok(changed.then_some(DataType::Map(entries, *left_ordered)))
        }
        _ => Ok(None),
    }
}

fn comparison_common_struct_type(
    left_fields: &Fields,
    right_fields: &Fields,
) -> Result<Option<DataType>, ComparisonCoercionFailure> {
    if let Some(fields) = comparison_common_struct_fields_by_name(left_fields, right_fields)? {
        return Ok(Some(DataType::Struct(fields)));
    }

    let mut fields = Vec::with_capacity(left_fields.len());
    let mut changed_any = left_fields
        .iter()
        .zip(right_fields.iter())
        .any(|(left_field, right_field)| left_field.name() != right_field.name());
    for (left_field, right_field) in left_fields.iter().zip(right_fields.iter()) {
        let Some((field, changed)) = comparison_common_field(left_field, right_field)? else {
            return Ok(None);
        };
        changed_any |= changed;
        fields.push(field);
    }
    Ok(changed_any.then(|| DataType::Struct(Fields::from(fields))))
}

fn comparison_common_struct_fields_by_name(
    left_fields: &Fields,
    right_fields: &Fields,
) -> Result<Option<Fields>, ComparisonCoercionFailure> {
    let right_by_name = right_fields
        .iter()
        .map(|field| (field.name().as_str(), field))
        .collect::<std::collections::HashMap<_, _>>();
    if left_fields
        .iter()
        .any(|field| !right_by_name.contains_key(field.name().as_str()))
    {
        return Ok(None);
    }

    let mut fields = Vec::with_capacity(left_fields.len());
    let mut changed_any = left_fields
        .iter()
        .zip(right_fields.iter())
        .any(|(left_field, right_field)| left_field.name() != right_field.name());
    for left_field in left_fields {
        let right_field = right_by_name
            .get(left_field.name().as_str())
            .expect("right field exists by name");
        let Some((field, changed)) = comparison_common_field(left_field, right_field)? else {
            return Ok(None);
        };
        changed_any |= changed;
        fields.push(field);
    }
    Ok(changed_any.then(|| Fields::from(fields)))
}

fn comparison_common_field(
    left: &Field,
    right: &Field,
) -> Result<Option<(Arc<Field>, bool)>, ComparisonCoercionFailure> {
    let data_type = if left.data_type() == right.data_type() {
        left.data_type().clone()
    } else if let Some(common) =
        comparison_common_nested_field_type(left.data_type(), right.data_type())?
    {
        common
    } else {
        return Ok(None);
    };
    let nullable = left.is_nullable() || right.is_nullable();
    let changed = left.name() != right.name()
        || left.is_nullable() != nullable
        || right.is_nullable() != nullable
        || left.data_type() != &data_type
        || right.data_type() != &data_type;
    Ok(Some((
        Arc::new(Field::new(left.name(), data_type, nullable)),
        changed,
    )))
}

fn comparison_common_nested_field_type(
    left: &DataType,
    right: &DataType,
) -> Result<Option<DataType>, ComparisonCoercionFailure> {
    let common = comparison_common_type(left, right)?;
    if common.is_some() && (is_string_type(left) || is_string_type(right)) {
        return Ok(Some(wider_type(left, right)));
    }
    Ok(common)
}

fn is_string_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Utf8 | DataType::LargeUtf8)
}

/// Complete comparison operand type using the existing carrier comparison rule.
///
/// `None` retains the carrier API's out-of-scope meaning. Incompatible logical
/// identities are errors, not an invitation to erase an identity with a carrier
/// fallback. The result is a type calculation only: a caller must materialize
/// any semantic conversion through its actual function owner.
///
/// Recursive inputs must first have been admitted within the caller's type
/// resource bounds. This API does not claim cooperative compilation accounting.
pub fn comparison_common_value_type(
    left: &FunctionValueType,
    right: &FunctionValueType,
) -> Result<Option<FunctionValueType>, ComparisonCoercionFailure> {
    validate_operands(left, right)?;
    if left.data_type() == &DataType::Null || right.data_type() == &DataType::Null {
        let value = null_common_value(left, right);
        return Ok((value != *left || value != *right).then_some(value));
    }
    let carrier = comparison_common_type(left.data_type(), right.data_type())?;
    let common_exists = carrier.is_some()
        || crate::arrow_type_equals_ignoring_metadata(left.data_type(), right.data_type());
    let candidate = carrier.unwrap_or_else(|| left.data_type().clone());
    let value = value_type_for_common_layout(candidate, left, right)?;
    if !common_exists {
        return Ok(None);
    }
    Ok((value != *left || value != *right).then_some(value))
}

/// Complete type for the existing comparison carrier fallback.
///
/// This shares `wider_type` rather than defining another carrier promotion
/// algorithm. It preserves the selected source's field facts and permits only
/// the explicitly declared JSON/text and LARGEINT/signed-integer domain pairs.
/// It does not authorize runtime casts or supply decimal conversion policy.
pub fn wider_comparison_value_type(
    left: &FunctionValueType,
    right: &FunctionValueType,
) -> Result<FunctionValueType, ComparisonCoercionFailure> {
    validate_operands(left, right)?;
    if left.data_type() == &DataType::Null || right.data_type() == &DataType::Null {
        return Ok(null_common_value(left, right));
    }
    value_type_for_common_layout(wider_type(left.data_type(), right.data_type()), left, right)
}

fn validate_operands(
    left: &FunctionValueType,
    right: &FunctionValueType,
) -> Result<(), ComparisonCoercionFailure> {
    left.validate()
        .and_then(|()| right.validate())
        .map_err(|error| ComparisonCoercionFailure::ValueType(error.into()))
}

fn null_common_value(left: &FunctionValueType, right: &FunctionValueType) -> FunctionValueType {
    let mut result = if left.data_type() == &DataType::Null {
        right.clone()
    } else {
        left.clone()
    };
    result = result
        .with_nullable(left.nullable() || right.nullable())
        .expect("valid function nullability declaration");
    result
}

fn signed_integer(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    )
}

/// Bind the complete semantic result to a layout selected by the calling
/// owner's existing common-type policy. This restores child domains without
/// treating format metadata or encoding identifiers as semantic authority.
/// It authorizes no cast, installed implementation, or allocation capacity.
pub fn value_type_for_common_layout(
    carrier: DataType,
    left: &FunctionValueType,
    right: &FunctionValueType,
) -> Result<FunctionValueType, ComparisonCoercionFailure> {
    value_type_for_common_layout_with_construction::<std::convert::Infallible>(
        carrier,
        left,
        right,
        crate::ValueTypeStorageLifetime::pure(),
        None,
    )
    .map_err(ComparisonCoercionFailure::from_construction)
}
pub fn value_type_for_common_layout_with_construction<E>(
    carrier: DataType,
    left: &FunctionValueType,
    right: &FunctionValueType,
    storage: crate::ValueTypeStorageLifetime,
    observer: Option<&mut dyn FnMut(crate::ValueConstructionRequest) -> Result<(), E>>,
) -> Result<FunctionValueType, crate::ValueConstructionFailure<E>> {
    common_value_core(
        carrier,
        left,
        right,
        storage,
        &mut crate::value_construction::ValueConstruction::new(observer),
    )
}
fn common_value_core<E>(
    mut carrier: DataType,
    left: &FunctionValueType,
    right: &FunctionValueType,
    storage: crate::ValueTypeStorageLifetime,
    creation: &mut crate::value_construction::ValueConstruction<'_, E>,
) -> Result<FunctionValueType, crate::ValueConstructionFailure<E>> {
    if left.data_type() == &DataType::Null || right.data_type() == &DataType::Null {
        let source = if left.data_type() == &DataType::Null {
            right
        } else {
            left
        };
        return source.with_nullable_with_construction(
            left.nullable() || right.nullable(),
            storage,
            creation.observer(),
        );
    }
    let logical_type = if left.logical_type() == right.logical_type() {
        if left.logical_type() == ValueLogicalType::Physical
            && ((crate::is_largeint_data_type(left.data_type())
                && (crate::is_numeric_value_type(right)
                    || right.data_type() == &DataType::Boolean))
                || (crate::is_largeint_data_type(right.data_type())
                    && (crate::is_numeric_value_type(left)
                        || left.data_type() == &DataType::Boolean)))
        {
            return Err(crate::ValueConstructionFailure::Rule(
                "fixed binary has no declared numeric comparison identity".into(),
            ));
        }
        left.logical_type()
    } else {
        match (left.logical_type(), right.logical_type()) {
            (ValueLogicalType::LargeInt, ValueLogicalType::Physical)
                if signed_integer(right.data_type()) =>
            {
                carrier = left.data_type().clone();
                ValueLogicalType::LargeInt
            }
            (ValueLogicalType::Physical, ValueLogicalType::LargeInt)
                if signed_integer(left.data_type()) =>
            {
                carrier = right.data_type().clone();
                ValueLogicalType::LargeInt
            }
            (ValueLogicalType::Json, ValueLogicalType::Physical)
                if right.data_type() == &DataType::Utf8 =>
            {
                carrier = DataType::Utf8;
                ValueLogicalType::Physical
            }
            (ValueLogicalType::Physical, ValueLogicalType::Json)
                if left.data_type() == &DataType::Utf8 =>
            {
                carrier = DataType::Utf8;
                ValueLogicalType::Physical
            }
            _ => {
                return Err(crate::ValueConstructionFailure::Rule(format!(
                    "comparison has no declared conversion between {:?} and {:?}",
                    left.logical_type(),
                    right.logical_type()
                )));
            }
        }
    };
    restore_typed_value(carrier, left, right, logical_type, storage, creation)
}

fn restore_typed_value<E>(
    carrier: DataType,
    left: &FunctionValueType,
    right: &FunctionValueType,
    logical: ValueLogicalType,
    storage: crate::ValueTypeStorageLifetime,
    creation: &mut crate::value_construction::ValueConstruction<'_, E>,
) -> Result<FunctionValueType, crate::ValueConstructionFailure<E>> {
    use crate::{ValueField, ValueKind, ValueLayout, ValueTypeRef, ValueUnionField};
    let fail = || {
        crate::ValueConstructionFailure::Rule(
            "comparison cannot erase an unsupported typed semantic structure".to_string(),
        )
    };
    let layout =
        ValueLayout::try_new_with_construction(&carrier, storage.clone(), creation.observer())?;
    let child = |index: usize,
                 target: &DataType,
                 creation: &mut crate::value_construction::ValueConstruction<'_, E>|
     -> Result<ValueTypeRef, crate::ValueConstructionFailure<E>> {
        let a = left
            .child_at_with_construction(index, storage.clone(), creation.observer())?
            .ok_or_else(fail)?;
        let b = right
            .child_at_with_construction(index, storage.clone(), creation.observer())?
            .ok_or_else(fail)?;
        Ok(
            common_value_core(target.clone(), &a, &b, storage.clone(), creation)?
                .value_type()
                .clone(),
        )
    };
    let kind = if logical != ValueLogicalType::Physical {
        ValueKind::Nominal(logical)
    } else {
        match (
            layout.data_type(),
            left.value_type().kind(),
            right.value_type().kind(),
        ) {
            (
                DataType::List(field)
                | DataType::LargeList(field)
                | DataType::ListView(field)
                | DataType::LargeListView(field),
                ValueKind::List(_),
                ValueKind::List(_),
            ) => ValueKind::List(child(0, field.data_type(), creation)?),
            (
                DataType::FixedSizeList(field, length),
                ValueKind::FixedSizeList { length: a, .. },
                ValueKind::FixedSizeList { length: b, .. },
            ) if length == a && a == b => ValueKind::FixedSizeList {
                item: child(0, field.data_type(), creation)?,
                length: *length,
            },
            (DataType::Struct(target), ValueKind::Struct(a), ValueKind::Struct(b))
                if target.len() == a.len() && a.len() == b.len() =>
            {
                let by_name = a
                    .iter()
                    .all(|field| b.iter().any(|other| other.name() == field.name()));
                creation.collected_box_slice::<ValueField>(target.len())?;
                let fields = target
                    .iter()
                    .enumerate()
                    .map(|(index, target)| {
                        let right_index = if by_name {
                            b.iter()
                                .position(|other| other.name() == a[index].name())
                                .ok_or_else(fail)?
                        } else {
                            index
                        };
                        let a = left
                            .child_at_with_construction(
                                index,
                                storage.clone(),
                                creation.observer(),
                            )?
                            .ok_or_else(fail)?;
                        let b = right
                            .child_at_with_construction(
                                right_index,
                                storage.clone(),
                                creation.observer(),
                            )?
                            .ok_or_else(fail)?;
                        let value = common_value_core(
                            target.data_type().clone(),
                            &a,
                            &b,
                            storage.clone(),
                            creation,
                        )?;
                        ValueField::from_borrowed_name_with_construction(
                            target.name().as_str(),
                            value.value_type().clone(),
                            storage.clone(),
                            creation.observer(),
                        )
                    })
                    .collect::<Result<Vec<_>, crate::ValueConstructionFailure<E>>>()?;
                ValueKind::Struct(fields.into())
            }
            (DataType::Map(entries, _), ValueKind::Map { .. }, ValueKind::Map { .. }) => {
                let DataType::Struct(fields) = entries.data_type() else {
                    return Err(fail());
                };
                ValueKind::Map {
                    key: child(0, fields[0].data_type(), creation)?,
                    value: child(1, fields[1].data_type(), creation)?,
                }
            }
            (DataType::Union(target, _), ValueKind::Union(a), ValueKind::Union(b))
                if target.len() == a.len() && a.len() == b.len() =>
            {
                creation.collected_box_slice::<ValueUnionField>(target.len())?;
                let fields = target
                    .iter()
                    .enumerate()
                    .map(|(index, (id, target))| {
                        if a[index].type_id != id {
                            return Err(fail());
                        }
                        let right_index = b
                            .iter()
                            .position(|other| other.type_id == id)
                            .ok_or_else(fail)?;
                        let a = left
                            .child_at_with_construction(
                                index,
                                storage.clone(),
                                creation.observer(),
                            )?
                            .ok_or_else(fail)?;
                        let b = right
                            .child_at_with_construction(
                                right_index,
                                storage.clone(),
                                creation.observer(),
                            )?
                            .ok_or_else(fail)?;
                        let value = common_value_core(
                            target.data_type().clone(),
                            &a,
                            &b,
                            storage.clone(),
                            creation,
                        )?;
                        Ok(ValueUnionField {
                            type_id: id,
                            field: ValueField::from_borrowed_name_with_construction(
                                target.name().as_str(),
                                value.value_type().clone(),
                                storage.clone(),
                                creation.observer(),
                            )?,
                        })
                    })
                    .collect::<Result<Vec<_>, crate::ValueConstructionFailure<E>>>()?;
                ValueKind::Union(fields.into())
            }
            (target, _, _) => {
                // An existing whole-carrier fallback may choose scalar shapes;
                // it has no authority to discard already-decided child domains.
                if ((left.value_type().child_count() > 0 && has_nominal_child(left.value_type()))
                    || (right.value_type().child_count() > 0
                        && has_nominal_child(right.value_type())))
                    && !left.same_value_domain(right)
                {
                    return Err(fail());
                }
                let declared = ValueTypeRef::try_from_declared_arrow_with_construction(
                    target,
                    left.nullable() || right.nullable(),
                    logical,
                    storage.clone(),
                    creation.observer(),
                )?;
                match declared.kind() {
                    ValueKind::Struct(fields) => creation.array::<ValueField>(fields.len())?,
                    ValueKind::Union(fields) => creation.array::<ValueUnionField>(fields.len())?,
                    _ => {}
                }
                declared.kind().clone()
            }
        }
    };
    let value = ValueTypeRef::try_new_with_construction(
        kind,
        left.nullable() || right.nullable(),
        storage,
        creation.observer(),
    )?;
    FunctionValueType::try_from_value(value, layout).map_err(Into::into)
}

fn has_nominal_child(value: &crate::ValueTypeRef) -> bool {
    if matches!(value.kind(), crate::ValueKind::Nominal(_)) {
        return true;
    }
    (0..value.child_count()).any(|index| value.child_type_at(index).is_some_and(has_nominal_child))
}

#[cfg(test)]
mod value_tests {
    #[test]
    fn decimal_common_precision_boundary_is_exact_and_does_not_retain_operand_fields() {
        use novarocks_failure_contract::{FailureClass, FailureProjection, RepresentationBoundary};
        let error =
            decimal_compare_type(&DataType::Decimal256(76, 0), &DataType::Decimal256(76, 38))
                .unwrap_err();
        let fact = error.failure_fact();
        assert_eq!(fact.class(), FailureClass::Representation);
        let representation = fact.representation().unwrap();
        assert_eq!(representation.boundary, RepresentationBoundary::MinimumUnit);
        assert_eq!(representation.required, Some(114));
        assert_eq!(representation.extent_limit, Some(76));
        assert_eq!(
            decimal_compare_type(&DataType::Decimal256(76, 0), &DataType::Decimal256(76, 0))
                .unwrap(),
            DataType::Decimal256(76, 0)
        );
        let field = Arc::new(Field::new("item", DataType::Int32, true));
        let weak = Arc::downgrade(&field);
        let invalid =
            decimal_compare_type(&DataType::List(field), &DataType::Decimal128(3, 0)).unwrap_err();
        assert!(weak.upgrade().is_none());
        assert_eq!(invalid.failure_fact().class(), FailureClass::Invariant);
    }

    use super::*;
    use arrow_schema::TimeUnit;

    fn value(carrier: DataType, logical: ValueLogicalType, nullable: bool) -> FunctionValueType {
        FunctionValueType::try_from_legacy_declared_projection(carrier, nullable, logical).unwrap()
    }
    fn physical(carrier: DataType, nullable: bool) -> FunctionValueType {
        value(carrier, ValueLogicalType::Physical, nullable)
    }
    fn field(
        name: &str,
        carrier: DataType,
        logical: ValueLogicalType,
        nullable: bool,
    ) -> Arc<Field> {
        let mut metadata = [("provider.field-id".into(), "73".into())]
            .into_iter()
            .collect::<std::collections::HashMap<_, _>>();
        if let Some(domain) = logical.metadata_value() {
            metadata.insert(crate::NR_LOGICAL_TYPE_KEY.into(), domain.into());
        }
        Arc::new(Field::new(name, carrier, nullable).with_metadata(metadata))
    }

    #[test]
    fn scalar_common_types_preserve_existing_decimal_and_numeric_rules() {
        let left = physical(DataType::Int32, false);
        let right = physical(DataType::Decimal128(10, 2), true);
        assert_eq!(
            comparison_common_value_type(&left, &right).unwrap(),
            Some(physical(DataType::Decimal128(12, 2), true))
        );
        assert!(
            comparison_common_value_type(
                &physical(DataType::Decimal256(76, 0), false),
                &physical(DataType::Decimal256(76, 38), false)
            )
            .is_err()
        );
        assert_eq!(comparison_common_value_type(&left, &left).unwrap(), None);
    }

    #[test]
    fn only_declared_root_pairs_get_semantic_common_types() {
        let large = value(
            DataType::FixedSizeBinary(16),
            ValueLogicalType::LargeInt,
            false,
        );
        let integer = physical(DataType::Int64, true);
        let expected = value(
            DataType::FixedSizeBinary(16),
            ValueLogicalType::LargeInt,
            true,
        );
        for (left, right) in [(&large, &integer), (&integer, &large)] {
            assert_eq!(
                comparison_common_value_type(left, right).unwrap(),
                Some(expected.clone())
            );
            assert_eq!(wider_comparison_value_type(left, right).unwrap(), expected);
        }
        let json = value(DataType::Utf8, ValueLogicalType::Json, false);
        let text = physical(DataType::Utf8, true);
        assert_eq!(
            comparison_common_value_type(&json, &text).unwrap(),
            Some(text.clone())
        );
        assert_eq!(wider_comparison_value_type(&text, &json).unwrap(), text);
        assert!(
            wider_comparison_value_type(&large, &physical(DataType::Decimal128(20, 0), true))
                .is_err()
        );
        assert!(wider_comparison_value_type(&large, &physical(DataType::Utf8, true)).is_err());
        assert!(wider_comparison_value_type(&json, &integer).is_err());
    }

    #[test]
    fn fixed_binary_and_uuid_are_never_inferred_numeric() {
        let raw = physical(DataType::FixedSizeBinary(16), false);
        let uuid = value(DataType::FixedSizeBinary(16), ValueLogicalType::Uuid, false);
        for numeric in [
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Boolean,
            DataType::Float64,
            DataType::Decimal32(7, 2),
            DataType::Decimal64(15, 2),
            DataType::Decimal128(20, 0),
            DataType::Decimal256(55, 2),
        ] {
            let numeric = physical(numeric, true);
            assert!(comparison_common_value_type(&raw, &numeric).is_err());
            assert!(wider_comparison_value_type(&raw, &numeric).is_err());
            assert!(comparison_common_value_type(&numeric, &raw).is_err());
            assert!(wider_comparison_value_type(&numeric, &raw).is_err());
            assert!(comparison_common_value_type(&uuid, &numeric).is_err());
            assert!(wider_comparison_value_type(&uuid, &numeric).is_err());
        }
        assert_eq!(comparison_common_value_type(&uuid, &uuid).unwrap(), None);
        assert_eq!(wider_comparison_value_type(&uuid, &uuid).unwrap(), uuid);
    }

    #[test]
    fn null_copies_full_decided_source_and_only_joins_nullability() {
        let source = value(DataType::Utf8, ValueLogicalType::Json, false);
        let null = physical(DataType::Null, true);
        let mut expected = source.clone();
        expected = expected
            .with_nullable(true)
            .expect("valid function nullability declaration");
        assert_eq!(
            comparison_common_value_type(&null, &source).unwrap(),
            Some(expected.clone())
        );
        assert_eq!(
            wider_comparison_value_type(&source, &null).unwrap(),
            expected
        );
        let list = physical(
            DataType::List(field(
                "actual",
                DataType::Utf8,
                ValueLogicalType::Json,
                false,
            )),
            false,
        );
        let mut expected = list.clone();
        expected = expected
            .with_nullable(true)
            .expect("valid function nullability declaration");
        assert_eq!(wider_comparison_value_type(&null, &list).unwrap(), expected);
    }

    #[test]
    fn nested_common_uses_canonical_layout_and_complete_declared_domain() {
        let left_item = field("source_item", DataType::Utf8, ValueLogicalType::Json, false);
        let right_item = field(
            "other_item",
            DataType::Utf8,
            ValueLogicalType::Physical,
            true,
        );
        let left = physical(DataType::List(left_item.clone()), false);
        let right = physical(DataType::List(right_item), true);
        for result in [
            comparison_common_value_type(&left, &right)
                .unwrap()
                .unwrap(),
            wider_comparison_value_type(&left, &right).unwrap(),
        ] {
            let DataType::List(item) = result.data_type() else {
                panic!("expected List")
            };
            assert_eq!(item.name(), "item");
            assert!(item.is_nullable());
            assert!(item.metadata().is_empty());
            assert_eq!(
                result.child_at(0).unwrap().logical_type(),
                ValueLogicalType::Physical
            );
            assert!(!item.metadata().contains_key(crate::NR_LOGICAL_TYPE_KEY));
            assert!(result.nullable());
        }
        assert_eq!(
            crate::field_logical_type(&left_item).unwrap(),
            ValueLogicalType::Json
        );
    }

    #[test]
    fn nested_signed_to_largeint_retains_authoritative_semantic_domain() {
        let left = physical(
            DataType::Struct(
                vec![field(
                    "n",
                    DataType::FixedSizeBinary(16),
                    ValueLogicalType::LargeInt,
                    false,
                )]
                .into(),
            ),
            false,
        );
        let right = physical(
            DataType::Struct(
                vec![field(
                    "n",
                    DataType::Int32,
                    ValueLogicalType::Physical,
                    true,
                )]
                .into(),
            ),
            false,
        );
        let result = comparison_common_value_type(&left, &right)
            .unwrap()
            .unwrap();
        let DataType::Struct(fields) = result.data_type() else {
            panic!("expected Struct")
        };
        assert_eq!(
            result.child_at(0).unwrap().logical_type(),
            ValueLogicalType::LargeInt
        );
        assert!(fields[0].metadata().is_empty());
        assert!(fields[0].is_nullable());
    }

    #[test]
    fn dictionary_encoding_attributes_do_not_decide_value_identity() {
        #[allow(deprecated)]
        let make = |id, nullable| {
            physical(
                DataType::Struct(
                    vec![
                        Field::new_dict(
                            "dict",
                            DataType::Dictionary(
                                Box::new(DataType::Int8),
                                Box::new(DataType::Utf8),
                            ),
                            nullable,
                            id,
                            true,
                        )
                        .with_metadata(std::collections::HashMap::<
                            String,
                            String,
                        >::from([
                            ("provider.field-id".into(), "91".into()),
                        ])),
                    ]
                    .into(),
                ),
                false,
            )
        };
        let left = make(73, false);
        let right = make(73, true);
        let expected = make(73, true);
        assert_eq!(
            wider_comparison_value_type(&left, &right).unwrap(),
            expected
        );
        assert_eq!(
            comparison_common_value_type(&left, &make(74, true)).unwrap(),
            Some(expected.clone())
        );
        assert_eq!(
            wider_comparison_value_type(&left, &make(74, true)).unwrap(),
            expected
        );
    }

    #[test]
    fn ordinary_physical_fallback_keeps_temporal_and_text_behavior() {
        let date = physical(DataType::Date32, false);
        let time = physical(
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        );
        assert_eq!(comparison_common_value_type(&date, &time).unwrap(), None);
        assert_eq!(wider_comparison_value_type(&date, &time).unwrap(), time);
        let text = physical(DataType::Utf8, false);
        assert_eq!(
            comparison_common_value_type(&text, &physical(DataType::Int32, true)).unwrap(),
            Some(physical(DataType::Int32, true))
        );
        assert_eq!(
            wider_comparison_value_type(&text, &physical(DataType::Int32, true)).unwrap(),
            physical(DataType::Utf8, true)
        );
    }

    #[test]
    fn map_fallback_uses_canonical_roles_and_complete_semantics() {
        let entries = field(
            "actual_entries",
            DataType::Struct(
                vec![
                    field(
                        "actual_key",
                        DataType::Int32,
                        ValueLogicalType::Physical,
                        false,
                    ),
                    field("actual_value", DataType::Utf8, ValueLogicalType::Json, true),
                ]
                .into(),
            ),
            ValueLogicalType::Physical,
            false,
        );
        let left = physical(DataType::Map(entries.clone(), true), false);
        let result = wider_comparison_value_type(&left, &left).unwrap();
        assert_eq!(result, left);
        let right = physical(
            DataType::Map(
                field(
                    "other_entries",
                    DataType::Struct(
                        vec![
                            field(
                                "actual_key",
                                DataType::Int64,
                                ValueLogicalType::Physical,
                                false,
                            ),
                            field(
                                "actual_value",
                                DataType::Utf8,
                                ValueLogicalType::Physical,
                                true,
                            ),
                        ]
                        .into(),
                    ),
                    ValueLogicalType::Physical,
                    false,
                ),
                true,
            ),
            false,
        );
        let result = wider_comparison_value_type(&left, &right).unwrap();
        let DataType::Map(fields, sorted) = result.data_type() else {
            panic!("expected Map")
        };
        assert!(!sorted); // Existing wider_type carrier behavior.
        assert_eq!(fields.name(), "entries");
        assert!(fields.metadata().is_empty());
        let DataType::Struct(children) = fields.data_type() else {
            panic!("expected entries")
        };
        assert_eq!(children[1].name(), "value");
        assert!(children[1].metadata().is_empty());
        assert_eq!(
            result.child_at(1).unwrap().logical_type(),
            ValueLogicalType::Physical
        );
    }
}
