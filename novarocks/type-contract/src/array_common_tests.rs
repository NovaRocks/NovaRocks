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

use super::*;
use std::convert::Infallible;

fn physical(ty: DataType, nullable: bool) -> FunctionValueType {
    FunctionValueType::new(ty, nullable)
}
fn json(nullable: bool) -> FunctionValueType {
    FunctionValueType::try_with_logical_type(DataType::Utf8, nullable, ValueLogicalType::Json)
        .unwrap()
}
fn infer(
    args: &[&FunctionValueType],
) -> Result<FunctionValueType, ArrayCommonTypeFailure<Infallible>> {
    array_constructor_result_with_construction(args, ValueTypeStorageLifetime::pure(), None)
}
fn item_of(result: &FunctionValueType) -> FunctionValueType {
    assert!(!result.nullable());
    let item = result.child_at(0).unwrap();
    assert!(item.nullable());
    item
}
fn nested(item: &FunctionValueType, nullable: bool) -> FunctionValueType {
    let storage = ValueTypeStorageLifetime::pure();
    let value = ValueTypeRef::try_new(
        ValueKind::List(item.value_type().clone()),
        nullable,
        storage.clone(),
    )
    .unwrap();
    let layout = ValueLayout::try_new(
        &DataType::List(Arc::new(Field::new(
            "source_item",
            item.data_type().clone(),
            item.nullable(),
        ))),
        storage,
    )
    .unwrap();
    FunctionValueType::try_from_value(value, layout).unwrap()
}

#[test]
fn numeric_rules_are_array_only_full_fvt_and_both_operand_orders() {
    for (a, b, expected) in [
        (DataType::Int8, DataType::Int16, DataType::Int16),
        (DataType::Int32, DataType::Float32, DataType::Float32),
        (DataType::Int64, DataType::Float32, DataType::Float32),
        (
            DataType::Decimal128(20, 1),
            DataType::Float32,
            DataType::Float32,
        ),
        (
            DataType::Decimal128(20, 1),
            DataType::Float64,
            DataType::Float64,
        ),
        (
            DataType::Int64,
            DataType::Decimal128(2, 1),
            DataType::Decimal128(20, 1),
        ),
        (
            DataType::Decimal128(2, 0),
            DataType::Decimal128(2, 1),
            DataType::Decimal128(3, 1),
        ),
        (
            DataType::Decimal128(38, 0),
            DataType::Decimal128(38, 20),
            DataType::Decimal128(38, 20),
        ),
    ] {
        let a = physical(a, false);
        let b = physical(b, true);
        let old_a = a.clone();
        let old_b = b.clone();
        for args in [[&a, &b], [&b, &a]] {
            let result = infer(&args).unwrap();
            assert_eq!(item_of(&result).data_type(), &expected);
        }
        assert_eq!(a, old_a);
        assert_eq!(b, old_b);
    }
    assert_eq!(
        crate::wider_type(&DataType::Decimal128(38, 0), &DataType::Decimal128(38, 20)),
        DataType::Decimal256(58, 20)
    );
}

#[test]
fn empty_unknown_original_precision_and_duplicate_occurrences_keep_their_facts() {
    let empty = infer(&[]).unwrap();
    assert_eq!(item_of(&empty).data_type(), &DataType::Null);
    let null = physical(DataType::Null, true);
    assert_eq!(
        item_of(&infer(&[&null, &null]).unwrap()).data_type(),
        &DataType::Null
    );
    let original = physical(DataType::Decimal128(2, 1), false);
    for args in [
        vec![&original],
        vec![&original, &original],
        vec![&null, &original],
    ] {
        assert_eq!(
            item_of(&infer(&args).unwrap()).data_type(),
            &DataType::Decimal128(2, 1)
        );
    }
    let malformed_unknown = physical(DataType::Null, false);
    assert!(matches!(
        infer(&[&malformed_unknown]),
        Err(ArrayCommonTypeFailure::Rule(
            ArrayCommonRuleFailure::NonnullableUnknown
        ))
    ));
    let integer = physical(DataType::Int32, false);
    let text = physical(DataType::Utf8, false);
    for args in [[&integer, &text], [&text, &integer]] {
        assert!(matches!(
            infer(&args),
            Err(ArrayCommonTypeFailure::Rule(
                ArrayCommonRuleFailure::UnreviewedHeterogeneousFamily
            ))
        ));
    }
}

#[test]
fn json_identity_unknown_and_string_conflict_use_actual_semantics() {
    let json = json(false);
    let text = physical(DataType::Utf8, false);
    let null = physical(DataType::Null, true);
    assert_eq!(json.data_type(), text.data_type());
    for args in [
        vec![&json],
        vec![&json, &json],
        vec![&null, &json],
        vec![&json, &null],
    ] {
        assert_eq!(
            item_of(&infer(&args).unwrap()).logical_type(),
            ValueLogicalType::Json
        );
    }
    for args in [[&json, &text], [&text, &json]] {
        assert!(matches!(
            infer(&args),
            Err(ArrayCommonTypeFailure::Rule(
                ArrayCommonRuleFailure::IncompatibleDomains
            ))
        ));
    }
    let a = nested(&json, false);
    let b = nested(&text, false);
    assert_eq!(a.data_type(), b.data_type());
    assert!(matches!(
        infer(&[&a, &b]),
        Err(ArrayCommonTypeFailure::Rule(
            ArrayCommonRuleFailure::IncompatibleDomains
        ))
    ));
}

#[test]
fn nested_list_full_nullability_and_exact_struct_layouts_are_retained() {
    let i32 = physical(DataType::Int32, false);
    let i64 = physical(DataType::Int64, true);
    let a = nested(&i32, false);
    let b = nested(&i64, true);
    let original_a = a.clone();
    let original_b = b.clone();
    let result = infer(&[&a, &b]).unwrap();
    let inner = item_of(&result);
    let leaf = inner.child_at(0).unwrap();
    assert!(leaf.nullable());
    assert_eq!(leaf.data_type(), &DataType::Int64);
    assert_eq!(a, original_a);
    assert_eq!(b, original_b);
    let named = physical(
        DataType::Struct(vec![Field::new("QuotedField", DataType::Int32, false)].into()),
        false,
    );
    let result = infer(&[&named, &named]).unwrap();
    assert!(
        item_of(&result)
            .same_physical_binding_ignoring_root_nullability_observed::<Infallible>(&named, || Ok(
                ()
            ))
            .unwrap()
    );
    let other = physical(
        DataType::Struct(vec![Field::new("other", DataType::Int32, false)].into()),
        false,
    );
    assert!(matches!(
        infer(&[&named, &other]),
        Err(ArrayCommonTypeFailure::Rule(
            ArrayCommonRuleFailure::UnreviewedHeterogeneousFamily
        ))
    ));
}

#[test]
fn homogeneous_existing_types_do_not_create_unreviewed_heterogeneous_promotions() {
    let unsigned = physical(DataType::UInt64, false);
    let signed = physical(DataType::Int64, false);
    assert_eq!(
        item_of(&infer(&[&unsigned, &unsigned]).unwrap()).data_type(),
        &DataType::UInt64
    );
    assert!(matches!(
        infer(&[&unsigned, &signed]),
        Err(ArrayCommonTypeFailure::Rule(
            ArrayCommonRuleFailure::UnreviewedHeterogeneousFamily
        ))
    ));
    let wide = physical(DataType::Decimal256(40, 0), false);
    assert_eq!(
        item_of(&infer(&[&wide, &wide]).unwrap()).data_type(),
        &DataType::Decimal256(40, 0)
    );
    assert!(matches!(
        infer(&[&wide, &signed]),
        Err(ArrayCommonTypeFailure::Rule(
            ArrayCommonRuleFailure::UnreviewedHeterogeneousFamily
        ))
    ));
}

#[test]
fn actual_construction_and_work_requests_preserve_the_first_original_refusal() {
    let left = nested(&physical(DataType::Decimal128(2, 0), false), false);
    let right = nested(&physical(DataType::Decimal128(2, 1), true), true);
    let mut events = Vec::new();
    let mut observe = |event| {
        events.push(event);
        Ok::<_, Infallible>(())
    };
    array_constructor_result_with_construction(
        &[&left, &right],
        ValueTypeStorageLifetime::pure(),
        Some(&mut observe),
    )
    .unwrap();
    assert!(
        events
            .iter()
            .any(|event| matches!(event, ValueConstructionRequest::Exact(_)))
    );
    assert!(
        events
            .iter()
            .any(|event| matches!(event, ValueConstructionRequest::Work(_)))
    );
    #[derive(Debug)]
    struct Refusal {
        token: u64,
    }
    let original = Arc::new(Refusal { token: 731 });
    for stop in 0..events.len() {
        let mut seen = 0;
        let mut observe = |_| {
            let at = seen;
            seen += 1;
            assert!(at <= stop, "callback after first refusal");
            if at == stop {
                Err(Arc::clone(&original))
            } else {
                Ok(())
            }
        };
        let error = array_constructor_result_with_construction(
            &[&left, &right],
            ValueTypeStorageLifetime::pure(),
            Some(&mut observe),
        )
        .unwrap_err();
        let ArrayCommonTypeFailure::Construction(ValueConstructionFailure::Observer(cause)) = error
        else {
            panic!("original observer failure")
        };
        assert!(Arc::ptr_eq(&cause, &original));
        assert_eq!(cause.token, 731);
        assert_eq!(seen, stop + 1);
    }
}

#[test]
fn common_coercion_cannot_borrow_explicit_cast_or_narrowing_profiles() {
    for (source, target, allowed) in [
        (DataType::Int16, DataType::Int32, true),
        (DataType::Int64, DataType::Float32, true),
        (DataType::Float32, DataType::Float64, true),
        (DataType::Decimal128(2, 0), DataType::Decimal128(3, 1), true),
        (
            DataType::Decimal128(38, 0),
            DataType::Decimal128(38, 38),
            true,
        ),
        (DataType::Float64, DataType::Decimal128(20, 0), false),
        (DataType::Int64, DataType::Int32, false),
        (DataType::Utf8, DataType::Int32, false),
        (
            DataType::Decimal128(4, 1),
            DataType::Decimal128(3, 0),
            false,
        ),
    ] {
        assert_eq!(array_common_numeric_coercion(&source, &target), allowed);
        let source = FunctionValueType::new(source, false);
        let target = FunctionValueType::new(target, false);
        assert_eq!(
            array_common_coercion_types_valid_observed::<std::convert::Infallible>(
                &source,
                &target,
                || Ok(())
            )
            .unwrap(),
            allowed
        );
    }
    let json =
        FunctionValueType::try_with_logical_type(DataType::Utf8, false, ValueLogicalType::Json)
            .unwrap();
    let text = FunctionValueType::new(DataType::Utf8, true);
    for (source, target) in [(&json, &text), (&text, &json)] {
        assert!(
            !array_common_coercion_types_valid_observed::<std::convert::Infallible>(
                source,
                target,
                || Ok(())
            )
            .unwrap()
        );
    }
}

#[test]
fn array_common_does_not_promote_temporal_integer_carriers() {
    use crate::{TemporalPrecision, TemporalType};

    let declaration = TemporalType::Time(TemporalPrecision::DEFAULT);
    let carrier = declaration.carrier();
    let storage = ValueTypeStorageLifetime::pure();
    let time = FunctionValueType::try_from_value(
        ValueTypeRef::try_new(ValueKind::Temporal(declaration), false, storage.clone()).unwrap(),
        ValueLayout::try_new(&carrier, storage).unwrap(),
    )
    .unwrap();
    let bigint = physical(DataType::Int64, false);
    assert_eq!(time.data_type(), bigint.data_type());
    for args in [[&time, &bigint], [&bigint, &time]] {
        assert!(matches!(
            infer(&args),
            Err(ArrayCommonTypeFailure::Rule(
                ArrayCommonRuleFailure::UnreviewedHeterogeneousFamily
            ))
        ));
    }
    let item = item_of(&infer(&[&time, &time]).unwrap());
    assert_eq!(item.value_type().temporal(), Some(declaration));
    assert_eq!(item.data_type(), &carrier);
}
