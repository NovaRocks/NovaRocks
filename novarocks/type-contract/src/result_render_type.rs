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

//! Semantic render facts are independent from Arrow's runtime carrier. This
//! preparation check compares the frozen type with its canonical storage
//! schema; dictionary/view hydration remains an execution capability.

use crate::{
    FunctionValueType, IntervalField, IntervalQualifier, TemporalPrecision, TemporalType,
    TemporalTypeError, ValueKind, ValueLogicalType, ValueTypeRef,
};
use novarocks_result_contract::{
    NativeRenderType, RenderField, RenderIntervalField, RenderTemporalType,
};

impl From<TemporalType> for RenderTemporalType {
    fn from(value: TemporalType) -> Self {
        match value {
            TemporalType::Time(p) => Self::Time(p.digits()),
            TemporalType::TimeWithTimeZone(p) => Self::TimeWithTimeZone(p.digits()),
            TemporalType::Timestamp(p) => Self::Timestamp(p.digits()),
            TemporalType::TimestampWithTimeZone(p) => Self::TimestampWithTimeZone(p.digits()),
            TemporalType::Interval(q) => Self::Interval {
                start: q.start().into(),
                end: q.end().into(),
                leading_precision: q.leading_precision(),
                fractional_precision: q.fractional_precision().digits(),
            },
        }
    }
}

impl TryFrom<RenderTemporalType> for TemporalType {
    type Error = TemporalTypeError;

    fn try_from(value: RenderTemporalType) -> Result<Self, Self::Error> {
        use RenderTemporalType as R;
        Ok(match value {
            R::Time(p) => Self::Time(TemporalPrecision::new(p)?),
            R::TimeWithTimeZone(p) => Self::TimeWithTimeZone(TemporalPrecision::new(p)?),
            R::Timestamp(p) => Self::Timestamp(TemporalPrecision::new(p)?),
            R::TimestampWithTimeZone(p) => Self::TimestampWithTimeZone(TemporalPrecision::new(p)?),
            R::Interval {
                start,
                end,
                leading_precision,
                fractional_precision,
            } => Self::Interval(IntervalQualifier::new(
                start.into(),
                end.into(),
                leading_precision,
                TemporalPrecision::new(fractional_precision)?,
            )?),
        })
    }
}

impl From<IntervalField> for RenderIntervalField {
    fn from(value: IntervalField) -> Self {
        match value {
            IntervalField::Year => Self::Year,
            IntervalField::Month => Self::Month,
            IntervalField::Day => Self::Day,
            IntervalField::Hour => Self::Hour,
            IntervalField::Minute => Self::Minute,
            IntervalField::Second => Self::Second,
        }
    }
}

impl From<RenderIntervalField> for IntervalField {
    fn from(value: RenderIntervalField) -> Self {
        match value {
            RenderIntervalField::Year => Self::Year,
            RenderIntervalField::Month => Self::Month,
            RenderIntervalField::Day => Self::Day,
            RenderIntervalField::Hour => Self::Hour,
            RenderIntervalField::Minute => Self::Minute,
            RenderIntervalField::Second => Self::Second,
        }
    }
}

pub fn render_field_matches_declaration(
    render: &RenderField,
    declaration: &FunctionValueType,
) -> bool {
    render_field_matches_storage(render, declaration.data_type(), declaration.nullable())
        && render_exact_declarations_match(render, declaration.value_type())
}

fn render_exact_declarations_match(render: &RenderField, value: &ValueTypeRef) -> bool {
    use NativeRenderType as R;
    match (&render.native_type, value.kind()) {
        (R::Json, ValueKind::Nominal(ValueLogicalType::Json)) => true,
        (R::Json, _) | (_, ValueKind::Nominal(ValueLogicalType::Json)) => false,
        (R::Uuid, ValueKind::Nominal(ValueLogicalType::Uuid)) => true,
        (R::Uuid, _) | (_, ValueKind::Nominal(ValueLogicalType::Uuid)) => false,
        (R::BoundedCharacter(render), ValueKind::BoundedCharacter(declared)) => render == declared,
        (R::BoundedCharacter(_), _) | (_, ValueKind::BoundedCharacter(_)) => false,
        (R::Temporal(render), ValueKind::Temporal(declared)) => *render == (*declared).into(),
        (R::Temporal(_), _) | (_, ValueKind::Temporal(_)) => false,
        (R::List(render), ValueKind::List(item)) => render_exact_declarations_match(render, item),
        (R::Map { key, value }, ValueKind::Map { key: kt, value: vt }) => {
            render_exact_declarations_match(key, kt) && render_exact_declarations_match(value, vt)
        }
        (R::Struct(render), ValueKind::Struct(fields)) => {
            render.iter().zip(fields).all(|(render, field)| {
                render_exact_declarations_match(&render.field, field.value_type())
            })
        }
        _ => true,
    }
}

pub fn render_field_matches_storage(
    render: &novarocks_result_contract::RenderField,
    data_type: &arrow_schema::DataType,
    nullable: bool,
) -> bool {
    render.nullable == nullable && render_carrier_matches(&render.native_type, data_type)
}

fn render_carrier_matches(
    render: &novarocks_result_contract::NativeRenderType,
    carrier: &arrow_schema::DataType,
) -> bool {
    use arrow_schema::{DataType as D, TimeUnit as U};
    use novarocks_result_contract::{NativeRenderType as R, RenderTimeUnit as T};
    let time_matches = |render: &T, carrier: &U| {
        matches!(
            (render, carrier),
            (T::Second, U::Second)
                | (T::Millisecond, U::Millisecond)
                | (T::Microsecond, U::Microsecond)
                | (T::Nanosecond, U::Nanosecond)
        )
    };
    let field_matches = |render: &novarocks_result_contract::RenderField,
                         carrier: &arrow_schema::Field| {
        render.nullable == carrier.is_nullable()
            && render_carrier_matches(&render.native_type, carrier.data_type())
    };
    match (render, carrier) {
        (R::Null, D::Null)
        | (R::Boolean, D::Boolean)
        | (R::SignedInteger(8), D::Int8)
        | (R::SignedInteger(16), D::Int16)
        | (R::SignedInteger(32), D::Int32)
        | (R::SignedInteger(64), D::Int64)
        | (R::UnsignedInteger(8), D::UInt8)
        | (R::UnsignedInteger(16), D::UInt16)
        | (R::UnsignedInteger(32), D::UInt32)
        | (R::UnsignedInteger(64), D::UInt64)
        | (R::LargeInt | R::Uuid, D::FixedSizeBinary(16))
        | (R::Float32, D::Float32)
        | (R::Float64, D::Float64)
        | (R::String | R::BoundedCharacter(_), D::Utf8 | D::LargeUtf8)
        | (R::Binary | R::Opaque(_), D::Binary)
        | (R::Json | R::Variant, D::LargeBinary)
        | (R::Date, D::Date32) => true,
        (
            R::Decimal {
                bits: 128,
                precision,
                scale,
            },
            D::Decimal128(p, s),
        )
        | (
            R::Decimal {
                bits: 256,
                precision,
                scale,
            },
            D::Decimal256(p, s),
        ) => precision == p && scale == s,
        (R::Time { unit }, D::Time32(u) | D::Time64(u)) => time_matches(unit, u),
        (R::Timestamp { unit, timezone }, D::Timestamp(u, zone)) => {
            time_matches(unit, u) && timezone.as_deref() == zone.as_deref()
        }
        (R::Temporal(ty), _) => {
            TemporalType::try_from(*ty).is_ok_and(|ty| ty.carrier() == *carrier)
        }
        (R::List(child), D::List(field)) => field_matches(child, field),
        (R::Map { key, value }, D::Map(entries, _)) => match entries.data_type() {
            D::Struct(fields) if fields.len() == 2 => {
                field_matches(key, &fields[0]) && field_matches(value, &fields[1])
            }
            _ => false,
        },
        (R::Struct(render), D::Struct(fields)) => {
            render.len() == fields.len()
                && render
                    .iter()
                    .zip(fields)
                    .all(|(r, f)| r.name == *f.name() && field_matches(&r.field, f))
        }
        _ => false,
    }
}
