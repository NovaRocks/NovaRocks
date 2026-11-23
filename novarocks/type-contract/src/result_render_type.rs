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

use crate::{FunctionValueType, ValueKind, ValueTypeRef};
use novarocks_result_contract::{NativeRenderType, RenderField};

pub fn render_field_matches_declaration(
    render: &RenderField,
    declaration: &FunctionValueType,
) -> bool {
    render_field_matches_storage(render, declaration.data_type(), declaration.nullable())
        && render_character_declarations_match(render, declaration.value_type())
}

fn render_character_declarations_match(render: &RenderField, value: &ValueTypeRef) -> bool {
    use NativeRenderType as R;
    match (&render.native_type, value.kind()) {
        (R::BoundedCharacter(render), ValueKind::BoundedCharacter(declared)) => render == declared,
        (R::BoundedCharacter(_), _) | (_, ValueKind::BoundedCharacter(_)) => false,
        (R::List(render), ValueKind::List(item)) => {
            render_character_declarations_match(render, item)
        }
        (R::Map { key, value }, ValueKind::Map { key: kt, value: vt }) => {
            render_character_declarations_match(key, kt)
                && render_character_declarations_match(value, vt)
        }
        (R::Struct(render), ValueKind::Struct(fields)) => {
            render.iter().zip(fields).all(|(render, field)| {
                render_character_declarations_match(&render.field, field.value_type())
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
        | (R::LargeInt, D::FixedSizeBinary(16))
        | (R::Float32, D::Float32)
        | (R::Float64, D::Float64)
        | (R::String | R::BoundedCharacter(_), D::Utf8 | D::LargeUtf8)
        | (R::Json, D::Utf8)
        | (R::Binary | R::Opaque(_), D::Binary)
        | (R::Variant, D::LargeBinary)
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
