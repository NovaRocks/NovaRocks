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

//! Prove that a source domain is a union of entire partitions.

use arrow::datatypes::Date32Type;
use novarocks_spi::connector::read_stack::{Bound, ConnectorValue, Domain};

use crate::iceberg::spec::{Datum, PrimitiveType, Transform, Type};

pub(super) fn transform_absorbs(
    primitive: &PrimitiveType,
    transform: &Transform,
    domain: &Domain,
) -> bool {
    if matches!(
        transform,
        Transform::Void | Transform::Unknown | Transform::Bucket(0) | Transform::Truncate(0)
    ) || transform
        .result_type(&Type::Primitive(primitive.clone()))
        .is_err()
    {
        return false;
    }
    if *transform == Transform::Identity
        || (*primitive == PrimitiveType::Date && *transform == Transform::Day)
        || domain.values().is_none()
        || domain.is_all()
    {
        return true;
    }
    if matches!(transform, Transform::Bucket(_)) {
        return false;
    }
    if domain.values().is_all() {
        return true;
    }

    // Exact points are only supported for identity and day(date). The incoming
    // domain normalizes points as unit ranges, including points in a union.
    domain.values().ranges().iter().all(|range| {
        range.single_value().is_none()
            && boundary_aligns(primitive, transform, range.low(), true)
            && boundary_aligns(primitive, transform, range.high(), false)
    })
}

fn boundary_aligns(
    primitive: &PrimitiveType,
    transform: &Transform,
    bound: &Bound,
    lower: bool,
) -> bool {
    let (value, inclusive) = match bound {
        Bound::Unbounded => return true,
        Bound::Inclusive(value) => (value, true),
        Bound::Exclusive(value) => (value, false),
    };
    // Lower inclusive and upper exclusive need the previous representable
    // source value. The other two bounds need the next one.
    let step = if lower == inclusive { -1 } else { 1 };
    match transform {
        Transform::Truncate(width) => match value {
            ConnectorValue::Varchar(value) if *primitive == PrimitiveType::String => {
                // Trino's unbounded VARCHAR has a next value (append NUL),
                // but no previous value. The appended codepoint must survive.
                step > 0 && value.chars().take(*width as usize).count() < *width as usize
            }
            _ => truncate_boundary(*width, value, step),
        },
        Transform::Day | Transform::Hour => temporal_bucket_boundary(transform, value, step),
        Transform::Year | Transform::Month => calendar_boundary(transform, value, step),
        _ => false,
    }
}

fn truncate_boundary(width: u32, value: &ConnectorValue, step: i64) -> bool {
    let (value, minimum, maximum) = match value {
        ConnectorValue::TinyInt(value) => {
            (i128::from(*value), i128::from(i8::MIN), i128::from(i8::MAX))
        }
        ConnectorValue::SmallInt(value) => (
            i128::from(*value),
            i128::from(i16::MIN),
            i128::from(i16::MAX),
        ),
        ConnectorValue::Integer(value) => (
            i128::from(*value),
            i128::from(i32::MIN),
            i128::from(i32::MAX),
        ),
        ConnectorValue::BigInt(value) => (
            i128::from(*value),
            i128::from(i64::MIN),
            i128::from(i64::MAX),
        ),
        ConnectorValue::Decimal {
            unscaled,
            precision,
            ..
        } => {
            let Some(max) = 10_i128
                .checked_pow(u32::from(*precision))
                .and_then(|value| value.checked_sub(1))
            else {
                return false;
            };
            (*unscaled, -max, max)
        }
        _ => return false,
    };
    let Some(adjacent) = value
        .checked_add(i128::from(step))
        .filter(|value| (minimum..=maximum).contains(value))
    else {
        return false;
    };
    // Compare mathematical buckets in a wide representation. Subtraction
    // back to an i32/i64 physical partition value can overflow at the minimum.
    let width = i128::from(width);
    value.div_euclid(width) != adjacent.div_euclid(width)
}

fn temporal_bucket_boundary(transform: &Transform, value: &ConnectorValue, step: i64) -> bool {
    let (timestamp, ticks_per_second) = match value {
        ConnectorValue::TimestampMicros(value) | ConnectorValue::TimestampTzMicros(value) => {
            (*value, 1_000_000)
        }
        ConnectorValue::TimestampNanos(value) | ConnectorValue::TimestampTzNanos(value) => {
            (*value, 1_000_000_000)
        }
        _ => return false,
    };
    let seconds = if *transform == Transform::Hour {
        3_600
    } else {
        86_400
    };
    let ticks = seconds * ticks_per_second;
    timestamp
        .checked_add(step)
        .is_some_and(|adjacent| timestamp.div_euclid(ticks) != adjacent.div_euclid(ticks))
}

fn calendar_boundary(transform: &Transform, value: &ConnectorValue, step: i64) -> bool {
    let Some((value, adjacent)) = calendar_datums(value, step) else {
        return false;
    };
    let Ok(transform) = crate::iceberg::transform::create_transform_function(transform) else {
        return false;
    };
    match (
        transform.transform_literal_result(&value),
        transform.transform_literal_result(&adjacent),
    ) {
        (Ok(left), Ok(right)) => left != right,
        _ => false,
    }
}

fn calendar_datums(value: &ConnectorValue, step: i64) -> Option<(Datum, Datum)> {
    let pair =
        |value: i64, datum: fn(i64) -> Datum| Some((datum(value), datum(value.checked_add(step)?)));
    match value {
        ConnectorValue::Date(value) => {
            let adjacent = value.checked_add(i32::try_from(step).ok()?)?;
            // The vendor's DATE literal transforms require representable
            // calendar dates. Outside that range no alignment is proven.
            Date32Type::to_naive_date_opt(*value)?;
            Date32Type::to_naive_date_opt(adjacent)?;
            Some((Datum::date(*value), Datum::date(adjacent)))
        }
        ConnectorValue::TimestampMicros(value) => pair(*value, Datum::timestamp_micros),
        ConnectorValue::TimestampTzMicros(value) => pair(*value, Datum::timestamptz_micros),
        ConnectorValue::TimestampNanos(value) => pair(*value, Datum::timestamp_nanos),
        ConnectorValue::TimestampTzNanos(value) => pair(*value, Datum::timestamptz_nanos),
        _ => None,
    }
}
