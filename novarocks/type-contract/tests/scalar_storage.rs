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

//! Actual Arrow schema comparisons only. Expected semantic facts are declared
//! independently; these tests do not infer logical metadata, inspect array
//! values, fund source growth, or enable the Native scalar producer.

use std::sync::Arc;

use arrow_schema::{DataType as D, Field, Schema, TimeUnit as U};
use novarocks_result_contract::{
    NamedScalarField, ScalarField, ScalarOpaqueType, ScalarSchema, ScalarTimestampUnit as T,
    ScalarValueType as S,
};
use novarocks_type_contract::result_scalar_type::scalar_field_matches_storage;

fn scalar(value_type: S, nullable: bool) -> ScalarField {
    ScalarField {
        nullable,
        value_type,
    }
}

fn matches(expected: &ScalarField, actual: D, nullable: bool) -> bool {
    let expected = ScalarSchema::try_new(expected.clone()).expect("valid declared scalar schema");
    let actual = Schema::new(vec![Field::new("root", actual, nullable)]);
    let actual = &actual.fields()[0];
    scalar_field_matches_storage(expected.field(), actual.data_type(), actual.is_nullable())
}

fn named(name: &str, value_type: S, nullable: bool) -> NamedScalarField {
    NamedScalarField {
        name: name.to_owned(),
        field: scalar(value_type, nullable),
    }
}

fn structure(fields: Vec<Field>) -> D {
    D::Struct(fields.into())
}

fn map(fields: Vec<Field>) -> D {
    D::Map(
        Arc::new(Field::new("entries", structure(fields), false)),
        false,
    )
}

#[test]
fn canonical_scalar_types_match_exact_storage_and_nullability() {
    let cases = [
        (S::Null, D::Null),
        (S::Boolean, D::Boolean),
        (S::SignedInteger(8), D::Int8),
        (S::SignedInteger(16), D::Int16),
        (S::SignedInteger(32), D::Int32),
        (S::SignedInteger(64), D::Int64),
        (S::LargeInt, D::FixedSizeBinary(16)),
        (S::Float32, D::Float32),
        (S::Float64, D::Float64),
        (S::String, D::Utf8),
        (S::Binary, D::Binary),
        (S::Date, D::Date32),
        (S::TimeMicros, D::Time64(U::Microsecond)),
    ];
    for (semantic, physical) in cases {
        for nullable in [false, true] {
            let expected = scalar(semantic.clone(), nullable);
            assert!(matches(&expected, physical.clone(), nullable));
            assert!(!matches(&expected, physical.clone(), !nullable));
        }
    }
}

#[test]
fn integer_storage_neither_narrows_widens_nor_accepts_unsigned() {
    for (bits, physical) in [(8, D::Int8), (16, D::Int16), (32, D::Int32), (64, D::Int64)] {
        for expected_bits in [8, 16, 32, 64] {
            assert_eq!(
                matches(
                    &scalar(S::SignedInteger(expected_bits), false),
                    physical.clone(),
                    false
                ),
                bits == expected_bits,
            );
        }
    }
    for unsigned in [D::UInt8, D::UInt16, D::UInt32, D::UInt64] {
        for bits in [8, 16, 32, 64] {
            assert!(!matches(
                &scalar(S::SignedInteger(bits), false),
                unsigned.clone(),
                false
            ));
        }
    }
}

#[test]
fn largeint_requires_its_exact_sixteen_byte_carrier() {
    let expected = scalar(S::LargeInt, true);
    for wrong in [
        D::FixedSizeBinary(8),
        D::FixedSizeBinary(15),
        D::FixedSizeBinary(17),
        D::Binary,
        D::Int64,
        D::Decimal128(38, 0),
    ] {
        assert!(!matches(&expected, wrong, true));
    }
    assert!(matches(&expected, D::FixedSizeBinary(16), true));
}

#[test]
fn floats_preserve_width_without_numeric_fallback() {
    assert!(!matches(&scalar(S::Float32, false), D::Float64, false));
    assert!(!matches(&scalar(S::Float64, false), D::Float32, false));
    for semantic in [S::Float32, S::Float64] {
        assert!(!matches(
            &scalar(semantic.clone(), false),
            D::Float16,
            false
        ));
        assert!(!matches(&scalar(semantic, false), D::Int64, false));
    }
}

#[test]
fn decimal_width_precision_and_scale_are_exact() {
    for (bits, precision, scale, physical) in [
        (128, 38, 0, D::Decimal128(38, 0)),
        (128, 17, 9, D::Decimal128(17, 9)),
        (256, 76, 38, D::Decimal256(76, 38)),
    ] {
        let expected = scalar(
            S::Decimal {
                bits,
                precision,
                scale,
            },
            true,
        );
        assert!(matches(&expected, physical, true));
        for wrong in [
            D::Decimal128(precision, -1),
            D::Decimal256(precision, -1),
            D::Utf8,
            D::Int64,
        ] {
            assert!(!matches(&expected, wrong, true));
        }
    }
    let expected = scalar(
        S::Decimal {
            bits: 128,
            precision: 17,
            scale: 9,
        },
        false,
    );
    for wrong in [
        D::Decimal128(18, 9),
        D::Decimal128(17, 8),
        D::Decimal256(17, 9),
    ] {
        assert!(!matches(&expected, wrong, false));
    }
    let expected = scalar(
        S::Decimal {
            bits: 256,
            precision: 38,
            scale: 0,
        },
        false,
    );
    assert!(!matches(&expected, D::Decimal128(38, 0), false));
}

#[test]
fn temporal_units_and_timezone_facts_are_not_normalized() {
    assert!(!matches(&scalar(S::Date, false), D::Date64, false));
    for wrong in [
        D::Time32(U::Second),
        D::Time32(U::Millisecond),
        D::Time64(U::Nanosecond),
        D::Int64,
    ] {
        assert!(!matches(&scalar(S::TimeMicros, false), wrong, false));
    }
    for (unit, physical_unit) in [
        (T::Microsecond, U::Microsecond),
        (T::Nanosecond, U::Nanosecond),
    ] {
        for zone in [None, Some("UTC"), Some("+00:00")] {
            let expected = scalar(
                S::Timestamp {
                    unit,
                    timezone: zone.map(str::to_owned),
                },
                true,
            );
            assert!(matches(
                &expected,
                D::Timestamp(physical_unit, zone.map(Into::into)),
                true
            ));
            for wrong_unit in [U::Second, U::Millisecond] {
                assert!(!matches(
                    &expected,
                    D::Timestamp(wrong_unit, zone.map(Into::into)),
                    true
                ));
            }
            let other_unit = if unit == T::Microsecond {
                U::Nanosecond
            } else {
                U::Microsecond
            };
            assert!(!matches(
                &expected,
                D::Timestamp(other_unit, zone.map(Into::into)),
                true
            ));
            let other_zone = if zone == Some("UTC") {
                Some("+00:00")
            } else {
                Some("UTC")
            };
            assert!(!matches(
                &expected,
                D::Timestamp(physical_unit, other_zone.map(Into::into)),
                true
            ));
        }
    }
    assert!(!matches(
        &scalar(
            S::Timestamp {
                unit: T::Microsecond,
                timezone: Some("UTC".to_owned())
            },
            false
        ),
        D::Timestamp(U::Microsecond, None),
        false
    ));
}

#[test]
fn semantic_identity_is_declared_even_when_physical_storage_is_shared() {
    // Physical compatibility is deliberately not a proof of Json/opaque
    // logical identity. The runtime must compare its original logical facts.
    let variant = ScalarSchema::try_new(scalar(S::Variant, true)).unwrap();
    let json = ScalarSchema::try_new(scalar(S::Json, true)).unwrap();
    assert_ne!(variant, json);
    assert!(matches(variant.field(), D::LargeBinary, true));
    assert!(matches(json.field(), D::LargeBinary, true));
    assert!(!matches(json.field(), D::Utf8, true));
    for opaque in [
        ScalarOpaqueType::Hll,
        ScalarOpaqueType::Bitmap,
        ScalarOpaqueType::Object,
        ScalarOpaqueType::Percentile,
    ] {
        let expected = ScalarSchema::try_new(scalar(S::Opaque(opaque), false)).unwrap();
        assert_ne!(
            expected,
            ScalarSchema::try_new(scalar(S::Binary, false)).unwrap()
        );
        assert!(matches(expected.field(), D::Binary, false));
        assert!(!matches(expected.field(), D::LargeBinary, false));
    }
    assert!(matches(&scalar(S::Variant, true), D::LargeBinary, true));
    assert!(!matches(&scalar(S::Variant, true), D::Binary, true));
}

#[test]
fn view_large_and_dictionary_carriers_do_not_become_static_fallbacks() {
    for wrong in [
        D::LargeUtf8,
        D::Utf8View,
        D::Dictionary(Box::new(D::Int32), Box::new(D::Utf8)),
    ] {
        assert!(!matches(&scalar(S::String, true), wrong.clone(), true));
        assert!(!matches(&scalar(S::Json, true), wrong, true));
    }
    for wrong in [
        D::LargeBinary,
        D::BinaryView,
        D::Dictionary(Box::new(D::Int32), Box::new(D::Binary)),
    ] {
        assert!(!matches(&scalar(S::Binary, true), wrong, true));
    }
    assert!(!matches(
        &scalar(S::SignedInteger(32), false),
        D::Dictionary(Box::new(D::Int32), Box::new(D::Int32)),
        false
    ));
}

#[test]
fn list_child_type_and_nullability_remain_exact() {
    let expected = scalar(S::List(Box::new(scalar(S::SignedInteger(32), true))), false);
    assert!(matches(
        &expected,
        D::List(Arc::new(Field::new("item", D::Int32, true))),
        false
    ));
    for wrong in [
        D::List(Arc::new(Field::new("item", D::Int64, true))),
        D::List(Arc::new(Field::new("item", D::Int32, false))),
        D::LargeList(Arc::new(Field::new("item", D::Int32, true))),
        D::FixedSizeList(Arc::new(Field::new("item", D::Int32, true)), 1),
    ] {
        assert!(!matches(&expected, wrong, false));
    }
}

#[test]
fn nested_struct_names_ordinals_and_nullability_are_authoritative() {
    let expected = scalar(
        S::Struct(vec![
            named("left", S::SignedInteger(32), false),
            named(
                "nested",
                S::Struct(vec![named("text", S::String, true)]),
                true,
            ),
        ]),
        false,
    );
    let nested = structure(vec![Field::new("text", D::Utf8, true)]);
    let left = Field::new("left", D::Int32, false);
    let right = Field::new("nested", nested.clone(), true);
    assert!(matches(
        &expected,
        structure(vec![left.clone(), right.clone()]),
        false
    ));
    for wrong in [
        structure(vec![right.clone(), left.clone()]),
        structure(vec![Field::new("renamed", D::Int32, false), right.clone()]),
        structure(vec![left.clone(), Field::new("nested", nested, false)]),
        structure(vec![
            left.clone(),
            Field::new(
                "nested",
                structure(vec![Field::new("renamed", D::Utf8, true)]),
                true,
            ),
        ]),
        structure(vec![
            left.clone(),
            Field::new(
                "nested",
                structure(vec![Field::new("text", D::Utf8, false)]),
                true,
            ),
        ]),
        structure(vec![left]),
    ] {
        assert!(!matches(&expected, wrong, false));
    }
}

#[test]
fn map_children_have_exact_roles_types_and_nullability() {
    let expected = scalar(
        S::Map {
            key: Box::new(scalar(S::String, false)),
            value: Box::new(scalar(S::SignedInteger(64), true)),
        },
        true,
    );
    let key = Field::new("key", D::Utf8, false);
    let value = Field::new("value", D::Int64, true);
    assert!(matches(
        &expected,
        map(vec![key.clone(), value.clone()]),
        true
    ));
    for wrong in [
        map(vec![value.clone(), key.clone()]),
        map(vec![Field::new("key", D::Utf8, true), value.clone()]),
        map(vec![key.clone(), Field::new("value", D::Int64, false)]),
        map(vec![key.clone(), Field::new("value", D::Int32, true)]),
        map(vec![key.clone()]),
        map(vec![
            key.clone(),
            value.clone(),
            Field::new("extra", D::Int64, true),
        ]),
        D::Map(Arc::new(Field::new("entries", D::Int64, false)), false),
        structure(vec![key, value]),
    ] {
        assert!(!matches(&expected, wrong, true));
    }
}

#[test]
fn scalar_native_binding_requires_the_sole_exact_slot() {
    let expected = ScalarSchema::try_new(scalar(S::Boolean, false)).unwrap();
    assert!(expected.validate_native_slots(&[7]).is_err());
    assert!(expected.clone().bind_native_slots(&[]).is_err());
    assert!(expected.clone().bind_native_slots(&[7, 8]).is_err());
    let bound = expected.bind_native_slots(&[7]).unwrap();
    assert_eq!(bound.source_ordinal(), 0);
    assert_eq!(bound.source_slot(), Some(7));
    assert!(bound.validate_native_slots(&[7]).is_ok());
    assert!(bound.validate_native_slots(&[8]).is_err());
    assert!(bound.validate_native_slots(&[]).is_err());
    assert!(bound.validate_native_slots(&[7, 7]).is_err());
    assert!(bound.bind_native_slots(&[8]).is_err());
}

#[test]
fn canonical_list_and_map_carrier_names_flags_and_entry_nullability_are_exact() {
    let item = scalar(S::SignedInteger(64), true);
    assert!(!matches(
        &scalar(S::List(Box::new(item)), true),
        D::List(Arc::new(Field::new("not_item", D::Int64, true))),
        true,
    ));
    let expected = scalar(
        S::Map {
            key: Box::new(scalar(S::SignedInteger(64), false)),
            value: Box::new(scalar(S::SignedInteger(64), false)),
        },
        true,
    );
    let entries = structure(vec![
        Field::new("key", D::Int64, false),
        Field::new("value", D::Int64, false),
    ]);
    for wrong in [
        D::Map(
            Arc::new(Field::new("entries", entries.clone(), false)),
            true,
        ),
        D::Map(
            Arc::new(Field::new("entries", entries.clone(), true)),
            false,
        ),
        D::Map(
            Arc::new(Field::new("renamed", entries.clone(), false)),
            false,
        ),
        map(vec![
            Field::new("renamed", D::Int64, false),
            Field::new("value", D::Int64, false),
        ]),
        map(vec![
            Field::new("key", D::Int64, false),
            Field::new("renamed", D::Int64, false),
        ]),
        map(vec![
            Field::new("value", D::Int64, false),
            Field::new("key", D::Int64, false),
        ]),
    ] {
        assert!(!matches(&expected, wrong, true));
    }
}
