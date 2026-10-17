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

//! Independent checked-pool comparison regressions, separate from real SQL shape.
use super::*;
use arrow::array::{Array, ArrayRef, Date32Array, Int64Array, UInt64Array};
use arrow::datatypes::Field;
use novarocks_constant_contract::{ConstantPool, ConstantValue};
use novarocks_type_contract::{CompilePhase, FunctionValueType, ValueLogicalType};
use std::sync::Arc;

fn selected(array: ArrayRef) -> ConstantValue {
    let nullable = array.logical_null_count() > 0;
    ConstantPool::try_new(
        Arc::new(Field::new(
            "independent.original",
            array.data_type().clone(),
            nullable,
        )),
        FunctionValueType::new(array.data_type().clone(), nullable),
        array.to_data(),
        crate::constant::test_constant_policy(),
        CompilePhase::Validate,
        crate::optimizer::rewrite::context::unbounded_rewrite_test_control(),
    )
    .unwrap()
    .value(1)
    .unwrap()
}
fn add(arena: &mut ScalarArena, value: ConstantValue) -> ScalarId {
    let ty = value.value_type().clone();
    arena
        .intern_observed(
            ScalarNode::Constant(value),
            ty,
            crate::optimizer::rewrite::context::unbounded_rewrite_test_control(),
        )
        .unwrap()
}
fn literal(arena: &mut ScalarArena, value: i64) -> ScalarId {
    arena
        .intern_observed(
            ScalarNode::Literal(HashableLiteral(LiteralValue::Int(value))),
            FunctionValueType::new(DataType::Int64, false),
            crate::optimizer::rewrite::context::unbounded_rewrite_test_control(),
        )
        .unwrap()
}
#[test]
fn checked_signed_comparison_uses_actual_nonzero_ordinal_without_float_rounding() {
    let mut arena = ScalarArena::new();
    let left = add(
        &mut arena,
        selected(Arc::new(Int64Array::from(vec![
            i64::MIN,
            9_007_199_254_740_993,
        ]))),
    );
    let right = add(
        &mut arena,
        selected(Arc::new(Int64Array::from(vec![
            i64::MAX,
            9_007_199_254_740_992,
        ]))),
    );
    assert_eq!(
        compare_literals(&arena, left, right),
        Some(Ordering::Greater)
    );
    assert_eq!(compare_literals(&arena, right, left), Some(Ordering::Less));
    let boundary = literal(&mut arena, 9_007_199_254_740_992);
    assert_eq!(
        compare_literals(&arena, left, boundary),
        Some(Ordering::Greater)
    );
    assert_eq!(
        compare_literals(&arena, boundary, right),
        Some(Ordering::Equal)
    );
}
#[test]
fn checked_signed_comparison_retains_full_i64_extremes() {
    let mut arena = ScalarArena::new();
    let min = add(
        &mut arena,
        selected(Arc::new(Int64Array::from(vec![7, i64::MIN]))),
    );
    let max = add(
        &mut arena,
        selected(Arc::new(Int64Array::from(vec![-7, i64::MAX]))),
    );
    assert_eq!(compare_literals(&arena, min, max), Some(Ordering::Less));
    let exact_max = literal(&mut arena, i64::MAX);
    assert_eq!(
        compare_literals(&arena, max, exact_max),
        Some(Ordering::Equal)
    );
}
#[test]
fn checked_null_temporal_unsigned_and_nominal_values_do_not_enter_signed_comparison() {
    let mut arena = ScalarArena::new();
    let valid = literal(&mut arena, 10);
    let null = selected(Arc::new(Int64Array::from(vec![Some(10), None])));
    let date = selected(Arc::new(Date32Array::from(vec![0, 10])));
    let unsigned = selected(Arc::new(UInt64Array::from(vec![0, u64::MAX])));
    let largeint_type = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        false,
        ValueLogicalType::LargeInt,
    )
    .unwrap();
    let nominal = ConstantValue::from_largeint(
        Arc::new(Field::new("nominal", DataType::FixedSizeBinary(16), false)),
        largeint_type,
        10,
        crate::constant::test_constant_policy(),
        CompilePhase::Validate,
        crate::optimizer::rewrite::context::unbounded_rewrite_test_control(),
    )
    .unwrap();
    for value in [null, date, unsigned, nominal] {
        let id = add(&mut arena, value);
        assert_eq!(compare_literals(&arena, id, valid), None);
        assert_eq!(compare_literals(&arena, valid, id), None);
    }
}
