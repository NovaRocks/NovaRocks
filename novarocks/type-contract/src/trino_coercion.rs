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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use arrow_schema::{DataType, Field, FieldRef};
use novarocks_result_contract::{CharacterKind, CharacterType};
use std::{alloc::Layout, sync::Arc};

use crate::{
    CompileCheckpoints, CompileControlError, FunctionValueType, TemporalType,
    ValueConstructionFailure, ValueConstructionRequest, ValueField, ValueKind, ValueLayout,
    ValueTypeRef, ValueTypeStorageLifetime, owned_resources::layout::arc_layout,
    value_construction::ValueConstruction,
};

/// The authored context of a standard SQL value conversion.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TrinoCastMode {
    Explicit,
    Try,
    Implicit,
    StoreAssignment,
}

/// Trino's common type for supported scalar declarations. Collection and row
/// binding must recurse through their semantic children and use this relation
/// for the leaves, irrespective of their physical Arrow representation.
pub fn trino_common_scalar_kind(left: &ValueKind, right: &ValueKind) -> Option<ValueKind> {
    use DataType::Null;
    use ValueKind::{BoundedCharacter, Scalar, String, Temporal};

    match (left, right) {
        (Scalar(Null), leaf) | (leaf, Scalar(Null))
            if !matches!(
                leaf,
                ValueKind::List(_)
                    | ValueKind::FixedSizeList { .. }
                    | ValueKind::Struct(_)
                    | ValueKind::Map { .. }
                    | ValueKind::Union(_)
            ) =>
        {
            Some(leaf.clone())
        }
        (BoundedCharacter(left), BoundedCharacter(right)) => {
            let kind = if left.kind() == right.kind() {
                left.kind()
            } else {
                CharacterKind::Varchar
            };
            Some(BoundedCharacter(CharacterType::new(
                kind,
                left.length().max(right.length()),
            )?))
        }
        (String, BoundedCharacter(_)) | (BoundedCharacter(_), String) => Some(String),
        (Scalar(left), Scalar(right)) if left == right => Some(Scalar(left.clone())),
        (Scalar(DataType::Date32), Scalar(timestamp @ DataType::Timestamp(_, None)))
        | (Scalar(timestamp @ DataType::Timestamp(_, None)), Scalar(DataType::Date32)) => {
            Some(Scalar(timestamp.clone()))
        }
        (Scalar(DataType::Timestamp(left, None)), Scalar(DataType::Timestamp(right, None))) => {
            Some(Scalar(DataType::Timestamp((*left).max(*right), None)))
        }
        (Temporal(left), Temporal(right)) => left.common_type(*right).map(Temporal),
        (
            Temporal(kind @ (TemporalType::Timestamp(_) | TemporalType::TimestampWithTimeZone(_))),
            Scalar(DataType::Date32),
        )
        | (
            Scalar(DataType::Date32),
            Temporal(kind @ (TemporalType::Timestamp(_) | TemporalType::TimestampWithTimeZone(_))),
        ) => Some(Temporal(*kind)),
        (Scalar(left), Scalar(right)) => common_numeric(left, right).map(Scalar),
        (String, String) => Some(String),
        (ValueKind::Binary, ValueKind::Binary) => Some(ValueKind::Binary),
        (ValueKind::Nominal(left), ValueKind::Nominal(right)) if left == right => {
            Some(ValueKind::Nominal(*left))
        }
        _ => None,
    }
}

/// Directional SQL coercion. In the default character mode CHAR can coerce
/// to VARCHAR, and a declared target width must fit the common declaration.
pub fn trino_can_coerce_scalar(from: &ValueKind, to: &ValueKind) -> bool {
    if matches!(
        (from, to),
        (ValueKind::BoundedCharacter(from), ValueKind::BoundedCharacter(to))
            if from.kind() == CharacterKind::Varchar && to.kind() == CharacterKind::Char
    ) {
        return false;
    }
    trino_common_scalar_kind(from, to).is_some_and(|common| &common == to)
}

/// Type identity used in overload matching. Nullability and physical Arrow
/// encoding are carried separately by the exact selected argument.
pub fn trino_same_type(
    left: &ValueTypeRef,
    right: &ValueTypeRef,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, CompileControlError> {
    matches_type(left, right, TypeRelation::Exact, work)
}

/// Directional expression coercion, including covariant arrays/maps and
/// positionally matched row fields. Write assignments have separate rules.
pub fn trino_can_coerce(
    source: &ValueTypeRef,
    target: &ValueTypeRef,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, CompileControlError> {
    matches_type(source, target, TypeRelation::Implicit, work)
}

/// Store assignment checks compatible declarations before the value
/// conversion checks narrowing and character width at the selected row.
pub fn trino_can_store_assign(
    source: &ValueTypeRef,
    target: &ValueTypeRef,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, CompileControlError> {
    matches_type(source, target, TypeRelation::StoreAssignment, work)
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum TypeRelation {
    Exact,
    Implicit,
    StoreAssignment,
}

fn matches_type(
    from: &ValueTypeRef,
    to: &ValueTypeRef,
    relation: TypeRelation,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, CompileControlError> {
    use ValueKind::{List, Map, Scalar, Struct};

    work.step()?;
    Ok(match (from.kind(), to.kind()) {
        (Scalar(DataType::Null), _) if relation != TypeRelation::Exact => true,
        (List(left), List(right)) => matches_type(left, right, relation, work)?,
        (Map { key: lk, value: lv }, Map { key: rk, value: rv }) => {
            matches_type(lk, rk, relation, work)? && matches_type(lv, rv, relation, work)?
        }
        (Struct(left), Struct(right)) => {
            if left.len() != right.len() {
                return Ok(false);
            }
            for (left_field, right_field) in left.iter().zip(right.iter()) {
                if relation == TypeRelation::Exact {
                    let left_name = left_field.name();
                    let right_name = right_field.name();
                    if left_name.len() != right_name.len() {
                        return Ok(false);
                    }
                    for (a, b) in left_name.bytes().zip(right_name.bytes()) {
                        work.step()?;
                        if a != b {
                            return Ok(false);
                        }
                    }
                }
                if !matches_type(
                    left_field.value_type(),
                    right_field.value_type(),
                    relation,
                    work,
                )? {
                    return Ok(false);
                }
            }
            true
        }
        (
            ValueKind::FixedSizeList {
                item: left,
                length: left_length,
            },
            ValueKind::FixedSizeList {
                item: right,
                length: right_length,
            },
        ) => left_length == right_length && matches_type(left, right, TypeRelation::Exact, work)?,
        (left, right) if left == right => true,
        (left, right) => match relation {
            TypeRelation::Exact => false,
            TypeRelation::Implicit => trino_can_coerce_scalar(left, right),
            TypeRelation::StoreAssignment => trino_common_scalar_kind(left, right).is_some(),
        },
    })
}

/// The common SQL declaration paired with a canonical frozen value layout.
/// None represents an incompatible pair, independently of conversion support.
pub fn trino_common_type_with_construction<E>(
    left: &FunctionValueType,
    right: &FunctionValueType,
    storage: ValueTypeStorageLifetime,
    observer: Option<&mut dyn FnMut(ValueConstructionRequest) -> Result<(), E>>,
) -> Result<Option<FunctionValueType>, ValueConstructionFailure<E>> {
    let mut creation = ValueConstruction::new(observer);
    let Some((value, carrier)) = common_value(
        left.value_type(),
        left.data_type(),
        right.value_type(),
        right.data_type(),
        &storage,
        &mut creation,
    )?
    else {
        return Ok(None);
    };
    let layout = ValueLayout::try_new_with_construction(&carrier, storage, creation.observer())?;
    Ok(Some(FunctionValueType::try_from_value(value, layout)?))
}

fn common_value<E>(
    left: &ValueTypeRef,
    left_carrier: &DataType,
    right: &ValueTypeRef,
    right_carrier: &DataType,
    storage: &ValueTypeStorageLifetime,
    creation: &mut ValueConstruction<'_, E>,
) -> Result<Option<(ValueTypeRef, DataType)>, ValueConstructionFailure<E>> {
    use ValueKind::{BoundedCharacter, List, Map, Scalar, String, Struct};

    if let Some(observe) = creation.observer() {
        observe(ValueConstructionRequest::Work(1)).map_err(ValueConstructionFailure::Observer)?;
    }
    let nullable = left.nullable() || right.nullable();
    let retained = match (left.kind(), right.kind()) {
        (_, Scalar(DataType::Null)) => Some((left, left_carrier)),
        (Scalar(DataType::Null), _) => Some((right, right_carrier)),
        _ if left.shares_storage(right) => Some((left, left_carrier)),
        _ => None,
    };
    if let Some((value, carrier)) = retained {
        return Ok(Some((
            value.with_nullable_with_construction(
                nullable,
                storage.clone(),
                creation.observer(),
            )?,
            creation.cloned_data_type(carrier)?,
        )));
    }

    let (kind, carrier) = match (left.kind(), right.kind(), left_carrier, right_carrier) {
        (
            List(left),
            List(right),
            DataType::List(l) | DataType::LargeList(l),
            DataType::List(r) | DataType::LargeList(r),
        ) => {
            let Some((item, carrier)) =
                common_value(left, l.data_type(), right, r.data_type(), storage, creation)?
            else {
                return Ok(None);
            };
            let field = common_field("item", carrier, item.nullable(), creation)?;
            (List(item), DataType::List(field))
        }
        (
            Map { key: lk, value: lv },
            Map { key: rk, value: rv },
            DataType::Map(l, _),
            DataType::Map(r, _),
        ) => {
            let (DataType::Struct(l), DataType::Struct(r)) = (l.data_type(), r.data_type()) else {
                return Ok(None);
            };
            let Some((key, kc)) = common_value(
                lk,
                l[0].data_type(),
                rk,
                r[0].data_type(),
                storage,
                creation,
            )?
            else {
                return Ok(None);
            };
            let Some((value, vc)) = common_value(
                lv,
                l[1].data_type(),
                rv,
                r[1].data_type(),
                storage,
                creation,
            )?
            else {
                return Ok(None);
            };
            creation.arc_slice::<FieldRef>(2)?;
            let entries = DataType::Struct(
                [
                    common_field("key", kc, false, creation)?,
                    common_field("value", vc, value.nullable(), creation)?,
                ]
                .into(),
            );
            let entries = common_field("entries", entries, false, creation)?;
            (Map { key, value }, DataType::Map(entries, false))
        }
        (Struct(left), Struct(right), DataType::Struct(l), DataType::Struct(r))
            if left.len() == right.len() =>
        {
            creation.array::<ValueField>(left.len())?;
            creation.array::<FieldRef>(left.len())?;
            let mut semantic = Vec::with_capacity(left.len());
            let mut fields = Vec::with_capacity(left.len());
            for (index, (left, right)) in left.iter().zip(right.iter()).enumerate() {
                let Some((child, carrier)) = common_value(
                    left.value_type(),
                    l[index].data_type(),
                    right.value_type(),
                    r[index].data_type(),
                    storage,
                    creation,
                )?
                else {
                    return Ok(None);
                };
                // SQL's anonymous ROW fields have empty semantic names.
                let left_name = left.name();
                let right_name = right.name();
                let equal = if left_name.len() == right_name.len() {
                    let mut equal = true;
                    for (a, b) in left_name.bytes().zip(right_name.bytes()) {
                        if let Some(observe) = creation.observer() {
                            observe(ValueConstructionRequest::Work(1))
                                .map_err(ValueConstructionFailure::Observer)?;
                        }
                        if a != b {
                            equal = false;
                            break;
                        }
                    }
                    equal
                } else {
                    false
                };
                let name = if equal { left_name } else { "" };
                fields.push(common_field(name, carrier, child.nullable(), creation)?);
                semantic.push(ValueField::from_borrowed_name_with_construction(
                    name,
                    child,
                    storage.clone(),
                    creation.observer(),
                )?);
            }
            creation.arc_slice::<FieldRef>(fields.len())?;
            (
                Struct(semantic.into_boxed_slice()),
                DataType::Struct(fields.into()),
            )
        }
        (left, right, _, _) => {
            let Some(kind) = trino_common_scalar_kind(left, right) else {
                return Ok(None);
            };
            let carrier = match &kind {
                Scalar(ty) => ty.clone(),
                ValueKind::Temporal(ty) => ty.carrier(),
                String | BoundedCharacter(_) => DataType::Utf8,
                ValueKind::Binary => DataType::Binary,
                ValueKind::Nominal(domain) => {
                    debug_assert!(domain.validate_carrier(left_carrier).is_ok());
                    creation.cloned_data_type(left_carrier)?
                }
                _ => unreachable!("a scalar common type cannot contain children"),
            };
            (kind, carrier)
        }
    };
    let value = ValueTypeRef::try_new_with_construction(
        kind,
        nullable,
        storage.clone(),
        creation.observer(),
    )?;
    Ok(Some((value, carrier)))
}

fn common_field<E>(
    name: &str,
    carrier: DataType,
    nullable: bool,
    creation: &mut ValueConstruction<'_, E>,
) -> Result<FieldRef, ValueConstructionFailure<E>> {
    creation.name(name.len())?;
    creation.exact(|| arc_layout(Layout::new::<Field>()))?;
    Ok(Arc::new(Field::new(name, carrier, nullable)))
}

fn common_numeric(left: &DataType, right: &DataType) -> Option<DataType> {
    use DataType::{Decimal128, Float32, Float64};

    let numeric =
        |value: &DataType| matches!(value, Float32 | Float64) || decimal_shape(value).is_some();
    if !numeric(left) || !numeric(right) {
        return None;
    }
    if left == &Float64 || right == &Float64 {
        return Some(Float64);
    }
    if left == &Float32 || right == &Float32 {
        return Some(Float32);
    }
    if let (Some(left_rank), Some(right_rank)) = (integer_rank(left), integer_rank(right)) {
        return Some(if left_rank >= right_rank {
            left.clone()
        } else {
            right.clone()
        });
    }
    let (left_precision, left_scale) = decimal_shape(left)?;
    let (right_precision, right_scale) = decimal_shape(right)?;
    let scale = left_scale.max(right_scale);
    let whole_digits = (left_precision - left_scale).max(right_precision - right_scale);
    Some(Decimal128((whole_digits + scale).min(38), scale as i8))
}

fn integer_rank(kind: &DataType) -> Option<u8> {
    match kind {
        DataType::Int8 => Some(0),
        DataType::Int16 => Some(1),
        DataType::Int32 => Some(2),
        DataType::Int64 => Some(3),
        _ => None,
    }
}

/// Whether a Decimal128 precision and scale expansion covers every value of
/// the declared standard numeric source. A capped common type need not have
/// enough integer digits even when Trino allows the implicit coercion.
pub fn trino_decimal_widening_is_total(source: &DataType, target: &DataType) -> bool {
    if !matches!(target, DataType::Decimal128(..)) {
        return false;
    }
    let (Some((source_precision, source_scale)), Some((target_precision, target_scale))) =
        (decimal_shape(source), decimal_shape(target))
    else {
        return false;
    };
    target_scale >= source_scale
        && target_precision - target_scale >= source_precision - source_scale
}

fn decimal_shape(kind: &DataType) -> Option<(u8, u8)> {
    match kind {
        DataType::Decimal128(precision, scale)
            if (1..=38).contains(precision) && *scale >= 0 && (*scale as u8) <= *precision =>
        {
            Some((*precision, *scale as u8))
        }
        _ => integer_rank(kind).map(|rank| ([3, 5, 10, 19][usize::from(rank)], 0)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use DataType::{Boolean, Decimal128, Float32, Int8, Int16, Int64};
    use ValueKind::{BoundedCharacter, Scalar, String, Temporal};

    #[test]
    fn common_numeric_type_uses_the_reference_width_and_scale() {
        for (left, right, expected) in [
            (Int8, Int16, Int16),
            (Int64, Float32, Float32),
            (Int64, Decimal128(1, 1), Decimal128(20, 1)),
            (Decimal128(38, 0), Decimal128(38, 38), Decimal128(38, 38)),
        ] {
            let (left, right, expected) = (Scalar(left), Scalar(right), Scalar(expected));
            assert_eq!(
                trino_common_scalar_kind(&left, &right),
                Some(expected.clone())
            );
            assert_eq!(trino_common_scalar_kind(&right, &left), Some(expected));
        }
        assert_eq!(
            trino_common_scalar_kind(&Scalar(Boolean), &Scalar(Int8)),
            None
        );
        assert_eq!(trino_common_scalar_kind(&String, &Scalar(Int8)), None);
        assert!(!trino_can_coerce_scalar(&Scalar(Int64), &Scalar(Int16)));
    }

    #[test]
    fn character_coercion_uses_declared_lengths_and_direction() {
        let char_four = BoundedCharacter(CharacterType::new(CharacterKind::Char, 4).unwrap());
        let varchar_four = BoundedCharacter(CharacterType::new(CharacterKind::Varchar, 4).unwrap());
        let varchar_two = BoundedCharacter(CharacterType::new(CharacterKind::Varchar, 2).unwrap());
        assert_eq!(
            trino_common_scalar_kind(&char_four, &varchar_two),
            Some(varchar_four.clone())
        );
        assert!(trino_can_coerce_scalar(&char_four, &varchar_four));
        assert!(trino_can_coerce_scalar(&char_four, &String));
        assert!(!trino_can_coerce_scalar(&varchar_four, &char_four));
        assert!(!trino_can_coerce_scalar(&char_four, &varchar_two));
    }

    #[test]
    fn temporal_coercion_uses_declarations_instead_of_integer_carriers() {
        use crate::TemporalPrecision;
        let time = Temporal(TemporalType::Time(TemporalPrecision::DEFAULT));
        let zoned_time = Temporal(TemporalType::TimeWithTimeZone(
            TemporalPrecision::PICOSECONDS,
        ));
        assert!(trino_can_coerce_scalar(&time, &zoned_time));
        assert!(!trino_can_coerce_scalar(&zoned_time, &time));
        assert_eq!(trino_common_scalar_kind(&time, &Scalar(Int64)), None);

        let date = Scalar(DataType::Date32);
        let timestamp = Temporal(TemporalType::Timestamp(TemporalPrecision::PICOSECONDS));
        assert!(trino_can_coerce_scalar(&date, &timestamp));
        assert!(!trino_can_coerce_scalar(&timestamp, &date));
    }
}
