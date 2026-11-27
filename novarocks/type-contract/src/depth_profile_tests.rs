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
        (32, true, true),
        (63, true, true),
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
        Ok(true)
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
    let mut scratch = [None; crate::MAX_ARROW_TYPE_PATH_DEPTH];
    value
        .validate_with_scratch_observed::<ValueTypeError>(&mut scratch, |_| Ok(()))
        .unwrap();
}

fn run_end(value: DataType) -> DataType {
    DataType::RunEndEncoded(
        Arc::new(Field::new("run_ends", DataType::Int32, false)),
        Arc::new(Field::new("values", value, false)),
    )
}

#[test]
fn rejects_each_pair_of_consecutive_encodings() {
    let dict = |value| dictionaries(value, 1);
    for (outer, inner) in [
        (
            dict as fn(DataType) -> DataType,
            dict as fn(DataType) -> DataType,
        ),
        (dict, run_end),
        (run_end, dict),
        (run_end, run_end),
    ] {
        let ty = outer(inner(DataType::Int64));
        assert!(matches!(
            profile(&ty, ArrowTypeProfile::Value),
            Err(ValueTypeError::InvalidType(_))
        ));
        assert!(ValueLayout::try_new(&ty, ValueTypeStorageLifetime::pure()).is_err());
        assert!(
            ValueTypeRef::try_from_declared_arrow(
                &ty,
                false,
                ValueLogicalType::Physical,
                ValueTypeStorageLifetime::pure(),
            )
            .is_err()
        );
    }
}

#[test]
fn encoding_at_distinct_logical_nodes_fits_the_derived_path() {
    let wrapped_map_path = |levels: usize| {
        let mut value = run_end(DataType::Int64);
        for _ in 1..levels {
            value = run_end(map_layers(value, 1));
        }
        value
    };
    assert_eq!(
        profile(
            &wrapped_map_path(MAX_VALUE_TYPE_DEPTH),
            ArrowTypeProfile::Value
        ),
        Ok(())
    );
    assert_eq!(
        profile(
            &wrapped_map_path(MAX_VALUE_TYPE_DEPTH + 1),
            ArrowTypeProfile::Value
        ),
        Err(ValueTypeError::TooDeep)
    );
    let dict_leaf = dictionaries(DataType::Int64, 1);
    let dict_struct = dictionaries(
        DataType::Struct(vec![Arc::new(Field::new("child", dict_leaf, true))].into()),
        1,
    );
    assert_eq!(profile(&dict_struct, ArrowTypeProfile::Value), Ok(()));
    assert!(matches!(
        profile(&dict_struct, ArrowTypeProfile::CanonicalValue),
        Err(ValueTypeError::InvalidType(_))
    ));
}

#[test]
fn wide_siblings_visit_every_map_scaffold() {
    let field = Arc::new(Field::new("value", map_layers(DataType::Int32, 1), false));
    let ty = DataType::Struct(vec![field; 1025].into());
    let mut nodes = 0;
    validate_arrow_layout_structure_with_profile_observed::<ValueTypeError>(
        &ty,
        ArrowTypeProfile::Value,
        |visit| {
            if matches!(visit, ValueTypeVisit::TypeNode(_)) {
                nodes += 1;
            }
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(nodes, 1 + 4 * 1025);
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
            let mut scratch = [None; crate::MAX_ARROW_TYPE_PATH_DEPTH];
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
    let wrapped = dictionaries(ty, 1);
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
    assert!(matches!(
        arrow_data_types_exact_borrowed_with_profile_observed::<ValueTypeError>(
            &over,
            &over,
            ArrowTypeProfile::Value,
            || Ok(())
        ),
        Err(ValueTypeError::InvalidType(_))
    ));
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
    // Both comparison ports preserve the same logical depth.
    assert!(matches!(
        arrow_data_types_exact_observed::<ValueTypeError>(
            left.data_type(),
            equal.data_type(),
            || Ok(())
        ),
        Ok(true)
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
