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

//! Conversion between Iceberg's physical units and the canonical SQL values.

mod predicate;
mod read;
pub(crate) use predicate::project_domain;
pub(crate) use read::{TemporalReadProjection, contains_temporal};

use crate::iceberg::spec::{PrimitiveType, Type};
use arrow::datatypes::{DataType, Field, TimeUnit};
use novarocks_type_contract::{
    TemporalMicros, TemporalPrecision, TemporalType, TemporalValue, TemporalValueError,
    TimeZoneKey, ValueKind, ValueTypeError, ValueTypeRef,
};

pub(crate) fn declaration(primitive: &PrimitiveType) -> Option<TemporalType> {
    let micros = TemporalPrecision::MICROSECONDS;
    let nanos = TemporalPrecision::NANOSECONDS;
    Some(match primitive {
        PrimitiveType::Time => TemporalType::Time(micros),
        PrimitiveType::Timestamp => TemporalType::Timestamp(micros),
        PrimitiveType::Timestamptz => TemporalType::TimestampWithTimeZone(micros),
        PrimitiveType::TimestampNs => TemporalType::Timestamp(nanos),
        PrimitiveType::TimestamptzNs => TemporalType::TimestampWithTimeZone(nanos),
        _ => return None,
    })
}

/// The same Iceberg instant is used by the Parquet-facing layout with no
/// Arrow zone and by the metadata writer's declared UTC Arrow layout.
pub(crate) fn matches_read_storage_layout(declaration: TemporalType, layout: &DataType) -> bool {
    match (declaration, layout) {
        (TemporalType::Time(_), DataType::Time64(TimeUnit::Microsecond)) => true,
        (TemporalType::Timestamp(precision), DataType::Timestamp(unit, None)) => {
            *unit == timestamp_unit(precision)
        }
        (TemporalType::TimestampWithTimeZone(precision), DataType::Timestamp(unit, zone)) => {
            *unit == timestamp_unit(precision)
                && zone
                    .as_deref()
                    .is_none_or(|z| matches!(z, "UTC" | "+00:00"))
        }
        _ => false,
    }
}

fn timestamp_unit(precision: TemporalPrecision) -> TimeUnit {
    if precision.fits_microseconds() {
        TimeUnit::Microsecond
    } else {
        TimeUnit::Nanosecond
    }
}

/// Match changed leaves against the explicit format declaration while
/// applying the original normalized equality to unchanged physical leaves.
pub(crate) fn matches_projection<E: From<ValueTypeError>>(
    declared: &Type,
    source: &DataType,
    target: &DataType,
    value: &ValueTypeRef,
    work: &mut impl FnMut(usize) -> Result<(), E>,
) -> Result<bool, E> {
    work(1)?;
    if let Type::Primitive(primitive) = declared
        && let Some(temporal) = declaration(primitive)
    {
        return Ok(value.temporal() == Some(temporal)
            && matches_read_storage_layout(temporal, source)
            && target == &temporal.carrier());
    }
    let field_matches = |field: &crate::iceberg::spec::NestedField,
                         source: &Field,
                         target: &Field,
                         value: &ValueTypeRef,
                         work: &mut _| {
        let nullable = !field.required;
        if source.is_nullable() != nullable
            || target.is_nullable() != nullable
            || value.nullable() != nullable
        {
            return Ok(false);
        }
        matches_projection(
            &field.field_type,
            source.data_type(),
            target.data_type(),
            value,
            work,
        )
    };
    match (declared, source, target, value.kind()) {
        (
            Type::Struct(declared),
            DataType::Struct(source),
            DataType::Struct(target),
            ValueKind::Struct(values),
        ) if declared.fields().len() == source.len()
            && source.len() == target.len()
            && target.len() == values.len() =>
        {
            for (((declared, source), target), value) in
                declared.fields().iter().zip(source).zip(target).zip(values)
            {
                work(declared.name.len() + 1)?;
                if declared.name != *source.name()
                    || declared.name != *target.name()
                    || declared.name != value.name()
                    || !field_matches(declared, source, target, value.value_type(), work)?
                {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        (
            Type::List(declared),
            DataType::List(source),
            DataType::List(target),
            ValueKind::List(value),
        ) => field_matches(&declared.element_field, source, target, value, work),
        (
            Type::Map(declared),
            DataType::Map(source, source_sorted),
            DataType::Map(target, target_sorted),
            ValueKind::Map { key, value },
        ) if source_sorted == target_sorted && !source.is_nullable() && !target.is_nullable() => {
            let (DataType::Struct(source), DataType::Struct(target)) =
                (source.data_type(), target.data_type())
            else {
                return Ok(false);
            };
            Ok(source.len() == 2
                && target.len() == 2
                && field_matches(&declared.key_field, &source[0], &target[0], key, work)?
                && field_matches(&declared.value_field, &source[1], &target[1], value, work)?)
        }
        (Type::Primitive(_) | Type::Variant(_), _, _, _) => {
            novarocks_type_contract::data_type_layout_matches_carrier_observed(target, source, work)
        }
        _ => Ok(false),
    }
}

pub(crate) fn from_storage(
    declaration: TemporalType,
    storage: i64,
) -> Result<TemporalValue, TemporalValueError> {
    let error = TemporalValueError::OutOfRange;
    let picos = i128::from(storage) * i128::from(declaration.precision().picosecond_step());
    match declaration {
        TemporalType::Time(_) => TemporalValue::Time(i64::try_from(picos).map_err(|_| error)?),
        TemporalType::Timestamp(_) => {
            TemporalValue::Timestamp(TemporalMicros::from_picos(picos).ok_or(error)?)
        }
        TemporalType::TimestampWithTimeZone(_) => TemporalValue::TimestampWithTimeZone {
            instant: TemporalMicros::from_picos(picos).ok_or(error)?,
            zone: TimeZoneKey::UTC,
        },
        _ => return Err(error),
    }
    .validate(declaration)
}

pub(crate) fn to_storage(
    declaration: TemporalType,
    value: TemporalValue,
) -> Result<i64, TemporalValueError> {
    let picos = observable_picos(declaration, value)?;
    let step = i128::from(declaration.precision().picosecond_step());
    i64::try_from(picos / step).map_err(|_| TemporalValueError::OutOfRange)
}

pub(crate) fn observable_picos(
    declaration: TemporalType,
    value: TemporalValue,
) -> Result<i128, TemporalValueError> {
    match value.validate(declaration)? {
        TemporalValue::Time(picos) => Ok(i128::from(picos)),
        TemporalValue::Timestamp(time) => Ok(time.as_picos()),
        TemporalValue::TimestampWithTimeZone { instant, .. } => Ok(instant.as_picos()),
        _ => Err(TemporalValueError::OutOfRange),
    }
}

pub(crate) fn compatible(storage: TemporalType, input: TemporalType) -> bool {
    matches!(
        (storage, input),
        (TemporalType::Time(_), TemporalType::Time(_))
            | (TemporalType::Timestamp(_), TemporalType::Timestamp(_))
            | (
                TemporalType::TimestampWithTimeZone(_),
                TemporalType::TimestampWithTimeZone(_)
            )
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_type_contract::PICOS_PER_MICROSECOND;

    #[test]
    fn negative_nanos_and_utc_instants_round_trip() {
        let timestamp = declaration(&PrimitiveType::TimestampNs).unwrap();
        let TemporalValue::Timestamp(value) = from_storage(timestamp, -1).unwrap() else {
            panic!("expected a timestamp");
        };
        assert_eq!(value.micros(), -1);
        assert_eq!(
            value.picoseconds_of_microsecond(),
            PICOS_PER_MICROSECOND as u32 - 1_000
        );
        assert_eq!(
            to_storage(timestamp, TemporalValue::Timestamp(value)),
            Ok(-1)
        );

        let zoned = declaration(&PrimitiveType::Timestamptz).unwrap();
        assert_eq!(
            from_storage(zoned, -1),
            Ok(TemporalValue::TimestampWithTimeZone {
                instant: TemporalMicros::whole_micros(-1),
                zone: TimeZoneKey::UTC,
            })
        );
        assert!(from_storage(zoned, (1_i64 << 51) * 1_000).is_err());
    }

    #[test]
    fn physical_write_requires_already_aligned_and_in_range_values() {
        let us = declaration(&PrimitiveType::Timestamp).unwrap();
        let ns = declaration(&PrimitiveType::TimestampNs).unwrap();
        let sub_micros = TemporalValue::Timestamp(TemporalMicros::new(-1, 900_000).unwrap());
        assert!(to_storage(us, sub_micros).is_err());
        assert_eq!(to_storage(ns, sub_micros), Ok(-100));
        assert!(
            to_storage(
                ns,
                TemporalValue::Timestamp(TemporalMicros::whole_micros(i64::MAX))
            )
            .is_err()
        );
    }
}
