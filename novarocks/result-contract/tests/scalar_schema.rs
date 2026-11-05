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

//! Pure schema boundaries only. Fixture allocations are ordinary allocations;
//! these tests do not prove source pregrant, scalar encoding, or Host installation.

use novarocks_result_contract::{
    NamedScalarField, RootContractError, RootProfileV1, ScalarField, ScalarOpaqueType,
    ScalarProfileV1, ScalarSchema, ScalarTimestampUnit, ScalarValueType,
};

fn field(value_type: ScalarValueType, nullable: bool) -> ScalarField {
    ScalarField {
        nullable,
        value_type,
    }
}

fn named(name: impl Into<String>, value_type: ScalarValueType, nullable: bool) -> NamedScalarField {
    NamedScalarField {
        name: name.into(),
        field: field(value_type, nullable),
    }
}

fn schema(value_type: ScalarValueType) -> ScalarSchema {
    ScalarSchema::try_new(field(value_type, true)).unwrap()
}

fn rejects(field: ScalarField, error: RootContractError) {
    assert_eq!(ScalarSchema::try_new(field).unwrap_err(), error);
}

#[test]
fn closed_primitive_identities_and_nullability_survive_without_presentation() {
    let types = [
        ScalarValueType::Null,
        ScalarValueType::Boolean,
        ScalarValueType::SignedInteger(8),
        ScalarValueType::SignedInteger(16),
        ScalarValueType::SignedInteger(32),
        ScalarValueType::SignedInteger(64),
        ScalarValueType::LargeInt,
        ScalarValueType::Float32,
        ScalarValueType::Float64,
        ScalarValueType::String,
        ScalarValueType::Binary,
        ScalarValueType::Date,
        ScalarValueType::TimeMicros,
        ScalarValueType::Json,
        ScalarValueType::Variant,
        ScalarValueType::Opaque(ScalarOpaqueType::Hll),
        ScalarValueType::Opaque(ScalarOpaqueType::Bitmap),
        ScalarValueType::Opaque(ScalarOpaqueType::Object),
        ScalarValueType::Opaque(ScalarOpaqueType::Percentile),
    ];
    let mut schemas = Vec::new();
    for value_type in types {
        for nullable in [false, true] {
            let expected = field(value_type.clone(), nullable);
            let actual = ScalarSchema::try_new(expected.clone()).unwrap();
            assert_eq!(actual.field(), &expected);
            assert_eq!(actual.source_ordinal(), 0);
            assert_eq!(actual.source_slot(), None);
            for other in &schemas {
                assert_ne!(&actual, other);
            }
            schemas.push(actual);
        }
    }
    for bits in [0, 1, 7, 24, 128, u16::MAX] {
        rejects(
            field(ScalarValueType::SignedInteger(bits), false),
            RootContractError::InvalidSchema,
        );
    }
}

#[test]
fn decimal_coefficients_precision_and_scale_are_exact_native_facts() {
    for (bits, max) in [(128, 38), (256, 76)] {
        for (precision, scale) in [(1, 0), (1, 1), (max, 0), (max, max)] {
            let value_type = ScalarValueType::Decimal {
                bits,
                precision,
                scale,
            };
            assert_eq!(schema(value_type.clone()).field().value_type, value_type);
        }
        for (precision, scale) in [(0, 0), (max + 1, 0), (1, 2), (max, max + 1)] {
            rejects(
                field(
                    ScalarValueType::Decimal {
                        bits,
                        precision,
                        scale,
                    },
                    true,
                ),
                RootContractError::InvalidSchema,
            );
        }
    }
    for bits in [0, 32, 64, 129, u16::MAX] {
        rejects(
            field(
                ScalarValueType::Decimal {
                    bits,
                    precision: 1,
                    scale: 0,
                },
                true,
            ),
            RootContractError::InvalidSchema,
        );
    }
    assert_ne!(
        schema(ScalarValueType::Decimal {
            bits: 128,
            precision: 38,
            scale: 0
        }),
        schema(ScalarValueType::Decimal {
            bits: 256,
            precision: 38,
            scale: 0
        })
    );
}

#[test]
fn timestamp_unit_and_optional_timezone_never_guess_a_default() {
    let mut schemas = Vec::new();
    for unit in [
        ScalarTimestampUnit::Microsecond,
        ScalarTimestampUnit::Nanosecond,
    ] {
        for timezone in [
            None,
            Some("UTC".to_owned()),
            Some("Asia/Shanghai".to_owned()),
            Some("utc".to_owned()),
        ] {
            let expected = ScalarValueType::Timestamp { unit, timezone };
            let actual = schema(expected.clone());
            assert_eq!(actual.field().value_type, expected);
            for other in &schemas {
                assert_ne!(&actual, other);
            }
            schemas.push(actual);
        }
        rejects(
            field(
                ScalarValueType::Timestamp {
                    unit,
                    timezone: Some(String::new()),
                },
                false,
            ),
            RootContractError::InvalidSchema,
        );
    }
}

#[test]
fn nested_schema_preserves_order_names_nullable_map_keys_and_logical_identities() {
    let expected = field(
        ScalarValueType::Struct(vec![
            named(
                "same",
                ScalarValueType::List(Box::new(field(ScalarValueType::Binary, false))),
                true,
            ),
            named(
                "same",
                ScalarValueType::Map {
                    key: Box::new(field(ScalarValueType::String, true)),
                    value: Box::new(field(
                        ScalarValueType::Struct(vec![
                            named("", ScalarValueType::Json, false),
                            named("雪", ScalarValueType::Variant, true),
                            named("hll", ScalarValueType::Opaque(ScalarOpaqueType::Hll), false),
                        ]),
                        true,
                    )),
                },
                false,
            ),
        ]),
        false,
    );
    let actual = ScalarSchema::try_new(expected.clone()).unwrap();
    assert_eq!(actual.field(), &expected);
    let mut reordered = expected.clone();
    let ScalarValueType::Struct(fields) = &mut reordered.value_type else {
        unreachable!()
    };
    fields.reverse();
    assert_ne!(actual, ScalarSchema::try_new(reordered).unwrap());
    let mut renamed = expected;
    let ScalarValueType::Struct(fields) = &mut renamed.value_type else {
        unreachable!()
    };
    fields[0].name.push('!');
    assert_ne!(actual, ScalarSchema::try_new(renamed).unwrap());
}

#[test]
fn native_binding_requires_one_exact_slot_and_is_idempotent_only_for_that_slot() {
    let local = schema(ScalarValueType::Binary);
    for slots in [vec![], vec![0], vec![17], vec![17, 18]] {
        assert_eq!(
            local.validate_native_slots(&slots),
            Err(RootContractError::InvalidSchema)
        );
    }
    for slots in [vec![], vec![17, 18]] {
        assert_eq!(
            local.clone().bind_native_slots(&slots).unwrap_err(),
            RootContractError::InvalidSchema
        );
    }
    for slot in [0, 17, u32::MAX] {
        let bound = local.clone().bind_native_slots(&[slot]).unwrap();
        assert_eq!(bound.source_ordinal(), 0);
        assert_eq!(bound.source_slot(), Some(slot));
        assert_eq!(bound.validate_native_slots(&[slot]), Ok(()));
        assert_eq!(bound.clone().bind_native_slots(&[slot]).unwrap(), bound);
        assert_eq!(
            bound.clone().bind_native_slots(&[slot ^ 1]).unwrap_err(),
            RootContractError::InvalidSchema
        );
        assert_eq!(
            bound.validate_native_slots(&[]),
            Err(RootContractError::InvalidSchema)
        );
        assert_eq!(
            bound.validate_native_slots(&[slot, slot]),
            Err(RootContractError::InvalidSchema)
        );
        assert_ne!(bound, local);
    }
}

#[test]
fn spare_capacity_is_billed_while_semantic_equality_and_clone_binding_are_stable() {
    let compact = schema(ScalarValueType::Struct(vec![named(
        "zone",
        ScalarValueType::String,
        true,
    )]));
    let mut name = String::with_capacity(4096);
    name.push_str("zone");
    let mut fields = Vec::with_capacity(128);
    fields.push(named(name, ScalarValueType::String, true));
    let spare = schema(ScalarValueType::Struct(fields));
    assert_eq!(spare, compact);
    assert_eq!(spare.wire_capacity_bytes(), compact.wire_capacity_bytes());
    assert!(spare.backing_bytes() > compact.backing_bytes());
    let bound = spare.bind_native_slots(&[42]).unwrap();
    let clone = bound.clone();
    assert_eq!(clone, compact.bind_native_slots(&[42]).unwrap());
    assert_eq!(clone.backing_bytes(), bound.backing_bytes());
    assert_eq!(clone.wire_capacity_bytes(), bound.wire_capacity_bytes());
    assert_eq!(clone.validate_native_slots(&[42]), Ok(()));
}

#[test]
fn backing_cap_refuses_large_spare_vec_and_string_even_with_tiny_semantic_content() {
    let mut fields =
        Vec::with_capacity(RootProfileV1::SCHEMA_BACKING_BYTES / size_of::<NamedScalarField>() + 1);
    fields.push(named("a", ScalarValueType::Boolean, false));
    rejects(
        field(ScalarValueType::Struct(fields), true),
        RootContractError::SchemaLimit,
    );
    assert!(
        schema(ScalarValueType::Struct(vec![named(
            "a",
            ScalarValueType::Boolean,
            false
        )]))
        .backing_bytes()
            < RootProfileV1::SCHEMA_BACKING_BYTES
    );

    let mut zone = String::with_capacity(RootProfileV1::SCHEMA_BACKING_BYTES);
    zone.push_str("UTC");
    rejects(
        field(
            ScalarValueType::Timestamp {
                unit: ScalarTimestampUnit::Nanosecond,
                timezone: Some(zone),
            },
            true,
        ),
        RootContractError::SchemaLimit,
    );
    assert!(
        schema(ScalarValueType::Timestamp {
            unit: ScalarTimestampUnit::Nanosecond,
            timezone: Some("UTC".to_owned())
        })
        .backing_bytes()
            < RootProfileV1::SCHEMA_BACKING_BYTES
    );
}

#[test]
fn single_name_cap_applies_to_struct_names_and_timestamp_zones() {
    for len in [
        RootProfileV1::MAX_NAME_BYTES,
        RootProfileV1::MAX_NAME_BYTES + 1,
    ] {
        for value_type in [
            ScalarValueType::Struct(vec![named("a".repeat(len), ScalarValueType::Null, true)]),
            ScalarValueType::Timestamp {
                unit: ScalarTimestampUnit::Microsecond,
                timezone: Some("z".repeat(len)),
            },
        ] {
            let actual = ScalarSchema::try_new(field(value_type, true));
            if len == RootProfileV1::MAX_NAME_BYTES {
                assert!(actual.is_ok());
            } else {
                assert_eq!(actual.unwrap_err(), RootContractError::SchemaLimit);
            }
        }
    }
}

#[test]
fn schema_wire_limit_is_inclusive_and_independent_of_backing_limit() {
    let base = schema(ScalarValueType::Struct(
        (0..4)
            .map(|_| named("", ScalarValueType::Null, true))
            .collect(),
    ));
    let mut remaining = RootProfileV1::SCHEMA_WIRE_BYTES - base.wire_capacity_bytes();
    let mut lengths = [0; 4];
    for len in &mut lengths {
        *len = remaining.min(RootProfileV1::MAX_NAME_BYTES);
        remaining -= *len;
    }
    assert_eq!(remaining, 0);
    let make = |lengths: [usize; 4]| {
        field(
            ScalarValueType::Struct(
                lengths
                    .into_iter()
                    .map(|len| named("a".repeat(len), ScalarValueType::Null, true))
                    .collect(),
            ),
            true,
        )
    };
    let exact = ScalarSchema::try_new(make(lengths)).unwrap();
    assert_eq!(
        exact.wire_capacity_bytes(),
        RootProfileV1::SCHEMA_WIRE_BYTES
    );
    assert!(exact.backing_bytes() < RootProfileV1::SCHEMA_BACKING_BYTES);
    lengths[3] += 1;
    assert!(lengths[3] <= RootProfileV1::MAX_NAME_BYTES);
    rejects(make(lengths), RootContractError::SchemaLimit);
}

#[test]
fn semantic_depth_counts_all_container_edges_including_map_values() {
    let make = |containers: usize| {
        let mut current = field(ScalarValueType::SignedInteger(64), true);
        for index in 0..containers {
            current = field(
                if index % 2 == 0 {
                    ScalarValueType::List(Box::new(current))
                } else {
                    ScalarValueType::Map {
                        key: Box::new(field(ScalarValueType::String, true)),
                        value: Box::new(current),
                    }
                },
                false,
            );
        }
        current
    };
    assert!(ScalarSchema::try_new(make(RootProfileV1::MAX_DEPTH - 1)).is_ok());
    rejects(
        make(RootProfileV1::MAX_DEPTH),
        RootContractError::SchemaLimit,
    );
}

#[test]
fn struct_field_count_is_bounded_without_silently_deduplicating_names() {
    let make = |count| {
        field(
            ScalarValueType::Struct(
                (0..count)
                    .map(|_| named("", ScalarValueType::Boolean, true))
                    .collect(),
            ),
            false,
        )
    };
    let exact = ScalarSchema::try_new(make(RootProfileV1::MAX_COLUMNS)).unwrap();
    let ScalarValueType::Struct(fields) = &exact.field().value_type else {
        unreachable!()
    };
    assert_eq!(fields.len(), RootProfileV1::MAX_COLUMNS);
    rejects(
        make(RootProfileV1::MAX_COLUMNS + 1),
        RootContractError::SchemaLimit,
    );
}

#[test]
fn scalar_ceiling_constants_match_the_frozen_p00_profile() {
    // The historical mysql_input object also freezes the still-owned Internal
    // Scalar/session limits. Retain those exact facts without reviving a client
    // input owner, changing the historical profile, or granting capacity.
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/testing/mem-1-m07/profile-v1.json");
    let json = std::fs::read_to_string(path).unwrap();
    let (_, rest) = json.split_once("\"mysql_input\"").unwrap();
    let (object, _) = rest.split_once('}').unwrap();
    for (key, expected) in [
        ("single_value_bytes", ScalarProfileV1::SINGLE_VALUE_BYTES),
        ("scalar_record_bytes", ScalarProfileV1::RECORD_BYTES),
        (
            "scalar_record_header_bytes",
            novarocks_result_contract::SCALAR_LEAF_HEADER_BYTES,
        ),
        ("scalar_child_bytes", ScalarProfileV1::CHILD_BYTES),
        (
            "assignment_scratch_bytes",
            ScalarProfileV1::ASSIGNMENT_SCRATCH_BYTES,
        ),
        ("session_live_bytes", ScalarProfileV1::SESSION_LIVE_BYTES),
        (
            "session_staged_bytes",
            ScalarProfileV1::SESSION_STAGED_BYTES,
        ),
        ("variables", ScalarProfileV1::VARIABLES),
    ] {
        let needle = format!("\"{key}\"");
        let (_, value) = object.split_once(&needle).unwrap();
        assert!(
            !value.contains(&needle),
            "duplicate frozen scalar key: {key}"
        );
        let (_, value) = value.split_once(':').unwrap();
        let digits: String = value
            .trim_start()
            .chars()
            .take_while(char::is_ascii_digit)
            .collect();
        assert_eq!(
            digits.parse::<usize>().unwrap(),
            expected,
            "frozen scalar key: {key}"
        );
    }
}

#[test]
fn scalar_domain_requires_typed_schema_before_native_binding() {
    use novarocks_result_contract::{
        FrozenRootOutput, InternalResultDomain, RootOutputContract, RootProfileId,
    };
    let bare = RootOutputContract::new(
        RootProfileId::V1,
        FrozenRootOutput::InternalFacts(InternalResultDomain::ScalarValueV1),
    );
    assert_eq!(
        bare.validate_purpose(),
        Err(RootContractError::InvalidSchema)
    );
    assert_eq!(
        bare.bind_native_slots(&[7]),
        Err(RootContractError::InvalidSchema)
    );
    let typed = RootOutputContract::new(
        RootProfileId::V1,
        FrozenRootOutput::ScalarValue(schema(ScalarValueType::Binary)),
    );
    assert!(typed.validate_purpose().is_ok());
    assert!(typed.bind_native_slots(&[7]).is_ok());
}
