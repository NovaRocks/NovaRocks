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

//! Pure type-owner correctness tests. Physical profiles are compared to the
//! explicit semantic model; no runtime encoder capability or budget is granted.

use crate::*;
use arrow_schema::{DataType, Field};
use std::sync::Arc;

fn map_layers(mut value: DataType, layers: usize) -> DataType {
    for _ in 0..layers {
        value = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Arc::new(Field::new("key", DataType::Int32, false)),
                        Arc::new(Field::new("value", value, false)),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
    }
    value
}
fn dictionaries(mut value: DataType, layers: usize) -> DataType {
    for _ in 0..layers {
        value = DataType::Dictionary(Box::new(DataType::Int32), Box::new(value));
    }
    value
}
fn profile(value: &DataType, profile: ArrowTypeProfile) -> Result<(), ValueTypeError> {
    validate_arrow_layout_structure_with_profile_observed(value, profile, |_| Ok(()))
}
fn event(value: ValueTypeVisit<'_>) -> (u8, usize) {
    match value {
        ValueTypeVisit::TypeNode(ty) => (0, std::ptr::from_ref(ty) as usize),
        ValueTypeVisit::ChildEdge(ty) => (1, std::ptr::from_ref(ty) as usize),
        ValueTypeVisit::Field(field) => (2, std::ptr::from_ref(field) as usize),
    }
}

#[test]
fn map_scaffolding_has_physical_visits_without_spending_logical_depth() {
    // n Maps plus a scalar is semantic depth n+1 and physical depth 2n+1.
    for (maps, physical_pass, value_pass) in [
        (31, true, true),
        (32, false, true),
        (63, false, true),
        (64, false, false),
    ] {
        let ty = map_layers(DataType::Int64, maps);
        assert_eq!(
            profile(&ty, ArrowTypeProfile::Physical).is_ok(),
            physical_pass
        );
        assert_eq!(profile(&ty, ArrowTypeProfile::Value).is_ok(), value_pass);
        assert_eq!(
            profile(&ty, ArrowTypeProfile::CanonicalValue).is_ok(),
            value_pass
        );
        let layout = ValueLayout::try_new(&ty, ValueTypeStorageLifetime::pure());
        assert_eq!(layout.is_ok(), value_pass);
        let semantic = ValueTypeRef::try_from_declared_arrow(
            &ty,
            false,
            ValueLogicalType::Physical,
            ValueTypeStorageLifetime::pure(),
        );
        assert_eq!(semantic.is_ok(), value_pass);
        if let Ok(semantic) = semantic {
            assert_eq!(semantic.depth(), maps + 1);
        }
    }
    let ty = map_layers(DataType::Int64, 63);
    let mut types = 0;
    let mut fields = 0;
    validate_arrow_layout_structure_with_profile_observed::<ValueTypeError>(
        &ty,
        ArrowTypeProfile::Value,
        |visit| {
            match visit {
                ValueTypeVisit::TypeNode(_) => types += 1,
                ValueTypeVisit::Field(_) => fields += 1,
                _ => {}
            }
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(types, 3 * 63 + 1);
    assert_eq!(fields, 3 * 63);
}

#[test]
fn complete_function_value_and_layout_match_accept_semantic_map64() {
    let plain = map_layers(DataType::Utf8, 63);
    let actual = map_layers(dictionaries(DataType::Utf8, 1), 63);
    let value = FunctionValueType::new(plain.clone(), false);
    assert_eq!(value.value_type().depth(), MAX_VALUE_TYPE_DEPTH);
    assert_eq!(value.value_type().expanded_nodes(), 2 * 63 + 1);
    assert!(
        value_layout_matches_carrier_observed::<ValueTypeError>(
            value.typed_binding().layout(),
            &actual,
            |_| Ok(())
        )
        .unwrap()
    );
    assert!(
        arrow_data_types_exact_with_profile_observed::<ValueTypeError>(
            &plain,
            &plain,
            ArrowTypeProfile::CanonicalValue,
            || Ok(())
        )
        .unwrap()
    );
    assert!(matches!(
        arrow_data_types_exact_observed::<ValueTypeError>(&plain, &plain, || Ok(())),
        Err(ValueTypeError::TooDeep)
    ));
    assert!(
        arrow_data_types_exact_borrowed_with_profile_observed::<ValueTypeError>(
            &plain,
            &plain,
            ArrowTypeProfile::CanonicalValue,
            || Ok(())
        )
        .unwrap()
    );
    let mut scratch = [None; MAX_VALUE_TYPE_NODES];
    value
        .validate_with_scratch_observed::<ValueTypeError>(&mut scratch, |_| Ok(()))
        .unwrap();
}

#[test]
fn value_profile_accepts_more_than_63_wrappers_within_the_existing_type_contract() {
    assert_eq!(ArrowTypeProfile::Physical.physical_depth_limit(), Some(64));
    assert_eq!(ArrowTypeProfile::Value.physical_depth_limit(), None);
    assert_eq!(
        ArrowTypeProfile::CanonicalValue.physical_depth_limit(),
        None
    );
    let maps = map_layers(DataType::Int64, 63);
    for wrappers in [64, 96] {
        let encoded = dictionaries(maps.clone(), wrappers);
        // 96 wrappers plus this Map chain has physical depth 223. Those
        // coordinates are observed facts, not independent rejection limits.
        assert!(profile(&encoded, ArrowTypeProfile::Value).is_ok());
        assert!(matches!(
            profile(&encoded, ArrowTypeProfile::Physical),
            Err(ValueTypeError::TooDeep)
        ));
        let layout = ValueLayout::try_new(&encoded, ValueTypeStorageLifetime::pure()).unwrap();
        assert_eq!(layout.data_type(), &maps);
        let semantic = ValueTypeRef::try_from_declared_arrow(
            &encoded,
            false,
            ValueLogicalType::Physical,
            ValueTypeStorageLifetime::pure(),
        )
        .unwrap();
        assert_eq!(semantic.depth(), MAX_VALUE_TYPE_DEPTH);
        let complete = FunctionValueType::new(encoded, false);
        assert_eq!(complete.value_type().depth(), MAX_VALUE_TYPE_DEPTH);
        assert_eq!(complete.data_type(), &maps);
    }
    // Canonical is a representation constraint, regardless of depth.
    assert!(matches!(
        profile(
            &dictionaries(DataType::Int64, 1),
            ArrowTypeProfile::CanonicalValue
        ),
        Err(ValueTypeError::InvalidType(_))
    ));
    assert!(ArrowTypeProfile::Value.check_physical_depth(223).is_ok());
    assert!(
        ArrowTypeProfile::CanonicalValue
            .check_physical_depth(128)
            .is_ok()
    );
}

#[test]
fn run_end_and_dictionary_wrappers_keep_observed_coordinates_without_a_new_quota() {
    let mut encoded = DataType::Int64;
    let mut coordinate = ArrowTypeDepth::ROOT;
    for index in 0..96 {
        coordinate = coordinate.encoding_child().unwrap();
        encoded = if index % 2 == 0 {
            dictionaries(encoded, 1)
        } else {
            DataType::RunEndEncoded(
                Arc::new(Field::new("run_ends", DataType::Int32, false)),
                Arc::new(Field::new("values", encoded, false)),
            )
        };
    }
    assert_eq!(coordinate.logical(), 1);
    assert_eq!(coordinate.physical(), 97);
    assert_eq!(coordinate.encoding_wrappers(), 96);
    coordinate.check(ArrowTypeProfile::Value).unwrap();
    assert!(profile(&encoded, ArrowTypeProfile::Value).is_ok());
    assert_eq!(
        ValueLayout::try_new(&encoded, ValueTypeStorageLifetime::pure())
            .unwrap()
            .data_type(),
        &DataType::Int64
    );
    let extra = dictionaries(encoded, 1);
    assert!(profile(&extra, ArrowTypeProfile::Value).is_ok());
    assert!(ValueLayout::try_new(&extra, ValueTypeStorageLifetime::pure()).is_ok());
}

#[test]
fn encoding_occurrences_still_spend_the_inherited_expanded_node_budget() {
    let field = Arc::new(Field::new(
        "encoded",
        dictionaries(DataType::Int64, 96),
        false,
    ));
    let within = DataType::Struct(vec![field.clone(); 20].into());
    assert!(profile(&within, ArrowTypeProfile::Value).is_ok());
    let over = DataType::Struct(vec![field; 24].into());
    assert_eq!(
        profile(&over, ArrowTypeProfile::Value),
        Err(ValueTypeError::TooManyNodes)
    );
}

#[test]
fn all_map_entry_nodes_still_count_against_the_original_physical_node_budget() {
    for (maps, pass) in [(1023, true), (1024, false)] {
        let field = Arc::new(Field::new("value", map_layers(DataType::Int32, 1), false));
        let ty = DataType::Struct(vec![field; maps].into());
        let result = profile(&ty, ArrowTypeProfile::Value);
        if pass {
            assert!(result.is_ok());
        } else {
            assert_eq!(result, Err(ValueTypeError::TooManyNodes));
        }
    }
}

#[test]
fn scratch_and_heap_keep_identical_physical_events_for_each_explicit_profile() {
    for ty in [
        map_layers(DataType::Int32, 63),
        map_layers(DataType::Int32, 64),
        dictionaries(DataType::Utf8, 63),
        dictionaries(DataType::Utf8, 64),
        dictionaries(DataType::Utf8, 96),
    ] {
        for profile in [
            ArrowTypeProfile::Physical,
            ArrowTypeProfile::Value,
            ArrowTypeProfile::CanonicalValue,
        ] {
            let mut first = Vec::new();
            let a = validate_arrow_layout_structure_with_profile_observed::<ValueTypeError>(
                &ty,
                profile,
                |visit| {
                    first.push(event(visit));
                    Ok(())
                },
            );
            let mut scratch = [None; MAX_VALUE_TYPE_NODES];
            let mut second = Vec::new();
            let b = validate_arrow_layout_structure_with_profile_and_scratch_observed::<
                ValueTypeError,
            >(&ty, profile, &mut scratch, |visit| {
                second.push(event(visit));
                Ok(())
            });
            assert_eq!(a, b);
            assert_eq!(first, second);
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Stop {
    Type(ValueTypeError),
    Caller(usize),
}
impl From<ValueTypeError> for Stop {
    fn from(error: ValueTypeError) -> Self {
        Self::Type(error)
    }
}
#[test]
fn explicit_profile_refusal_is_original_and_stops_before_later_work() {
    let ty = map_layers(dictionaries(DataType::Utf8, 1), 8);
    let mut events = 0;
    validate_arrow_layout_structure_with_profile_observed::<Stop>(
        &ty,
        ArrowTypeProfile::Value,
        |_| {
            events += 1;
            Ok(())
        },
    )
    .unwrap();
    for stop in 0..events {
        let mut at = 0;
        let result = validate_arrow_layout_structure_with_profile_observed::<Stop>(
            &ty,
            ArrowTypeProfile::Value,
            |_| {
                assert!(at <= stop, "callback after the primary refusal");
                let current = at;
                at += 1;
                if current == stop {
                    Err(Stop::Caller(stop))
                } else {
                    Ok(())
                }
            },
        );
        assert_eq!(result, Err(Stop::Caller(stop)));
        assert_eq!(at, stop + 1);
    }
}

#[test]
fn explicit_schema_profile_reuses_one_comparison_without_pending_preflights() {
    let ty = map_layers(DataType::Int64, 63);
    let mut validated_visits = 0;
    assert!(
        crate::schema::arrow_validated_data_types_exact_observed::<ValueTypeError>(
            &ty,
            &ty,
            || {
                validated_visits += 1;
                Ok(())
            }
        )
        .unwrap()
    );
    let mut profile_visits = 0;
    assert!(
        arrow_data_types_exact_borrowed_with_profile_observed::<ValueTypeError>(
            &ty,
            &ty,
            ArrowTypeProfile::CanonicalValue,
            || {
                profile_visits += 1;
                Ok(())
            }
        )
        .unwrap()
    );
    assert_eq!(profile_visits, validated_visits);
    let mut invalid = DataType::Int64;
    for _ in 1..190 {
        invalid = DataType::Struct(vec![Arc::new(Field::new("child", invalid, false))].into());
    }
    assert!(matches!(
        arrow_data_types_exact_borrowed_with_profile_observed::<ValueTypeError>(
            &invalid,
            &invalid,
            ArrowTypeProfile::Value,
            || Ok(())
        ),
        Err(ValueTypeError::TooDeep)
    ));
    let wrapped = dictionaries(ty, 96);
    assert!(
        arrow_data_types_exact_borrowed_with_profile_observed::<ValueTypeError>(
            &wrapped,
            &wrapped,
            ArrowTypeProfile::Value,
            || Ok(())
        )
        .unwrap()
    );
    let over = dictionaries(wrapped, 1);
    assert!(
        arrow_data_types_exact_borrowed_with_profile_observed::<ValueTypeError>(
            &over,
            &over,
            ArrowTypeProfile::Value,
            || Ok(())
        )
        .unwrap()
    );
}

#[test]
fn immutable_complete_map64_exact_comparison_keeps_layout_and_observer_identity() {
    let left = FunctionValueType::new(map_layers(DataType::Utf8, 63), false);
    let equal = FunctionValueType::new(map_layers(DataType::Utf8, 63), false);
    let different_layout = FunctionValueType::new(map_layers(DataType::LargeUtf8, 63), false);
    assert_eq!(left.value_type(), different_layout.value_type());
    assert!(
        left.exactly_equals_observed::<ValueTypeError>(&equal, || Ok(()))
            .unwrap()
    );
    assert!(
        !left
            .exactly_equals_observed::<ValueTypeError>(&different_layout, || Ok(()))
            .unwrap()
    );
    // Raw Arrow callers retain their original explicit physical policy.
    assert!(matches!(
        arrow_data_types_exact_observed::<ValueTypeError>(
            left.data_type(),
            equal.data_type(),
            || Ok(())
        ),
        Err(ValueTypeError::TooDeep)
    ));
    let mut observed = 0;
    assert!(
        left.exactly_equals_weighted_observed::<Stop>(&equal, |_| {
            observed += 1;
            Ok(())
        })
        .unwrap()
    );
    for stop in [0, observed / 2, observed - 1] {
        let mut at = 0;
        let result = left.exactly_equals_weighted_observed::<Stop>(&equal, |_| {
            assert!(at <= stop, "callback after original comparison refusal");
            let current = at;
            at += 1;
            if current == stop {
                Err(Stop::Caller(stop))
            } else {
                Ok(())
            }
        });
        assert_eq!(result, Err(Stop::Caller(stop)));
        assert_eq!(at, stop + 1);
    }
}
