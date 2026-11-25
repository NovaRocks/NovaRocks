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

//! Frozen scalar semantic facts compared with canonical Native storage.
//! This check does not infer logical Json/Variant/opaque identity from Arrow.
//! The frozen schema supplies that authority; runtime owners separately verify
//! actual encoded carriers and their original backing policy separately.

use arrow_schema::{DataType as D, Field, TimeUnit as U};
use novarocks_result_contract::{ScalarField, ScalarTimestampUnit as T, ScalarValueType as S};

/// Complete frozen scalar identity and accurate canonical value layout.
/// Source metadata is not an execution semantic author. This borrowed check
/// constructs no projected fields, schemas, strings, or allocation grants.
pub fn scalar_field_matches_binding(
    field: &ScalarField,
    binding: &crate::TypedValueBinding,
) -> bool {
    scalar_field_matches_binding_observed(field, binding, &mut || {
        Ok::<(), std::convert::Infallible>(())
    })
    .unwrap_or(false)
}

/// Preserve the original comparison observer's first refusal without tail work.
pub fn scalar_field_matches_binding_observed<E>(
    field: &ScalarField,
    binding: &crate::TypedValueBinding,
    observe: &mut impl FnMut() -> Result<(), E>,
) -> Result<bool, E> {
    if !semantics_match(field, binding.value_type(), observe)? {
        return Ok(false);
    }
    // The unchanged storage grammar is a bounded borrowed operation. Control
    // surrounds it; its internal comparisons do not claim cooperative work.
    observe()?;
    let matches = scalar_field_matches_storage(field, binding.data_type(), binding.nullable());
    observe()?;
    Ok(matches)
}

fn semantics_match<E>(
    field: &ScalarField,
    value: &crate::ValueTypeRef,
    observe: &mut impl FnMut() -> Result<(), E>,
) -> Result<bool, E> {
    use crate::{ValueKind, ValueLogicalType as V};
    use novarocks_result_contract::ScalarOpaqueType as O;
    observe()?;
    if field.nullable != value.nullable() {
        return Ok(false);
    }
    Ok(match (&field.value_type, value.kind()) {
        (S::List(expected), ValueKind::List(actual)) => semantics_match(expected, actual, observe)?,
        (
            S::Map { key, value },
            ValueKind::Map {
                key: actual_key,
                value: actual_value,
            },
        ) => {
            semantics_match(key, actual_key, observe)?
                && semantics_match(value, actual_value, observe)?
        }
        (S::Struct(expected), ValueKind::Struct(actual)) if expected.len() == actual.len() => {
            for (expected, actual) in expected.iter().zip(actual) {
                observe()?;
                if expected.name.len() != actual.name().len() {
                    return Ok(false);
                }
                for (left, right) in expected.name.bytes().zip(actual.name().bytes()) {
                    observe()?;
                    if left != right {
                        return Ok(false);
                    }
                }
                if !semantics_match(&expected.field, actual.value_type(), observe)? {
                    return Ok(false);
                }
            }
            true
        }
        (S::Json, ValueKind::Nominal(V::Json))
        | (S::Variant, ValueKind::Nominal(V::Variant))
        | (S::LargeInt, ValueKind::Nominal(V::LargeInt))
        | (S::Opaque(O::Hll), ValueKind::Nominal(V::Hll))
        | (S::Opaque(O::Bitmap), ValueKind::Nominal(V::Bitmap))
        | (S::Opaque(O::Object), ValueKind::Nominal(V::Object))
        | (S::Opaque(O::Percentile), ValueKind::Nominal(V::Percentile))
        | (S::String, ValueKind::String)
        | (S::Binary, ValueKind::Binary) => true,
        (
            S::Null
            | S::Boolean
            | S::SignedInteger(_)
            | S::Float32
            | S::Float64
            | S::Date
            | S::TimeMicros
            | S::Decimal { .. }
            | S::Timestamp { .. },
            ValueKind::Scalar(_),
        ) => true,
        _ => false,
    })
}

pub fn scalar_field_matches_storage(field: &ScalarField, carrier: &D, nullable: bool) -> bool {
    field.nullable == nullable && matches_storage(&field.value_type, carrier)
}

fn matches_field(field: &ScalarField, carrier: &Field) -> bool {
    scalar_field_matches_storage(field, carrier.data_type(), carrier.is_nullable())
}
fn matches_storage(field: &S, carrier: &D) -> bool {
    match (field, carrier) {
        (S::Null, D::Null)
        | (S::Boolean, D::Boolean)
        | (S::SignedInteger(8), D::Int8)
        | (S::SignedInteger(16), D::Int16)
        | (S::SignedInteger(32), D::Int32)
        | (S::SignedInteger(64), D::Int64)
        | (S::LargeInt, D::FixedSizeBinary(16))
        | (S::Float32, D::Float32)
        | (S::Float64, D::Float64)
        | (S::String, D::Utf8)
        | (S::Binary | S::Opaque(_), D::Binary)
        | (S::Json | S::Variant, D::LargeBinary)
        | (S::Date, D::Date32)
        | (S::TimeMicros, D::Time64(U::Microsecond)) => true,
        (
            S::Decimal {
                bits: 128,
                precision,
                scale,
            },
            D::Decimal128(p, s),
        )
        | (
            S::Decimal {
                bits: 256,
                precision,
                scale,
            },
            D::Decimal256(p, s),
        ) => precision == p && i16::from(*scale) == i16::from(*s),
        (S::Timestamp { unit, timezone }, D::Timestamp(u, zone)) => {
            matches!(
                (unit, u),
                (T::Microsecond, U::Microsecond) | (T::Nanosecond, U::Nanosecond)
            ) && timezone.as_deref() == zone.as_deref()
        }
        (S::List(child), D::List(field)) => field.name() == "item" && matches_field(child, field),
        (S::Map { key, value }, D::Map(entries, false))
            if entries.name() == "entries" && !entries.is_nullable() =>
        {
            match entries.data_type() {
                D::Struct(fields)
                    if fields.len() == 2
                        && fields[0].name() == "key"
                        && fields[1].name() == "value" =>
                {
                    matches_field(key, &fields[0]) && matches_field(value, &fields[1])
                }
                _ => false,
            }
        }
        (S::Struct(expected), D::Struct(actual)) => {
            expected.len() == actual.len()
                && expected.iter().zip(actual).all(|(expected, actual)| {
                    expected.name == *actual.name() && matches_field(&expected.field, actual)
                })
        }
        _ => false,
    }
}

#[cfg(test)]
mod binding_tests {
    use super::*;
    use crate::{
        FunctionValueType, ValueKind, ValueLayout, ValueLogicalType, ValueTypeRef,
        ValueTypeStorageLifetime as Hold,
    };
    use std::sync::Arc;

    fn list(json: bool, child_nullable: bool) -> FunctionValueType {
        let child = ValueTypeRef::try_new(
            if json {
                ValueKind::Nominal(ValueLogicalType::Json)
            } else {
                ValueKind::Binary
            },
            child_nullable,
            Hold::pure(),
        )
        .unwrap();
        let value = ValueTypeRef::try_new(ValueKind::List(child), false, Hold::pure()).unwrap();
        let layout = D::List(Arc::new(Field::new("item", D::LargeBinary, child_nullable)));
        FunctionValueType::try_from_value(
            value,
            ValueLayout::try_new(&layout, Hold::pure()).unwrap(),
        )
        .unwrap()
    }
    fn scalar() -> ScalarField {
        ScalarField {
            nullable: false,
            value_type: S::List(Box::new(ScalarField {
                nullable: true,
                value_type: S::Json,
            })),
        }
    }

    #[test]
    fn scalar_binding_rejects_same_carrier_nested_domain_drift_and_accepts_independent_equal_authors()
     {
        let json = list(true, true);
        let binary = list(false, true);
        assert_eq!(json.data_type(), binary.data_type());
        assert!(
            scalar_field_matches_storage(&scalar(), binary.data_type(), binary.nullable()),
            "carrier alone cannot establish child identity"
        );
        assert!(!scalar_field_matches_binding(
            &scalar(),
            binary.typed_binding()
        ));
        let independent = list(true, true);
        assert!(!json.value_type().shares_storage(independent.value_type()));
        assert!(
            !json
                .typed_binding()
                .layout()
                .shares_storage(independent.typed_binding().layout())
        );
        assert!(scalar_field_matches_binding(
            &scalar(),
            json.typed_binding()
        ));
        assert!(scalar_field_matches_binding(
            &scalar(),
            independent.typed_binding()
        ));
        assert!(
            !scalar_field_matches_binding(&scalar(), list(true, false).typed_binding()),
            "child NULL promises stay exact"
        );
    }

    #[test]
    fn borrowed_scalar_binding_observer_preserves_first_original_refusal_without_tail() {
        #[derive(Debug, PartialEq)]
        enum Stop {
            Original(u32),
        }
        let value = list(true, true);
        let mut visits = 0;
        let result =
            scalar_field_matches_binding_observed(&scalar(), value.typed_binding(), &mut || {
                visits += 1;
                Err(Stop::Original(73))
            });
        assert_eq!(result, Err(Stop::Original(73)));
        assert_eq!(visits, 1);
    }
}
