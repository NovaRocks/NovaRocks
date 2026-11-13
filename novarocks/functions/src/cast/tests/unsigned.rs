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

use super::super::*;
use crate::{ConstantPolicy, ConstantPool, SelectedValues, Selection};
use arrow_array::{ArrayRef, BooleanArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array};
use arrow_schema::Field;
use novarocks_type_contract::{EvaluationDemand, EvaluationDomainId, ExpressionUseId};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Default)]
struct CompileControl {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for CompileControl {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        trace.push(units);
        if let Some((at, cause)) = self.refusal.clone()
            && trace.len() == at + 1
        {
            Err(cause)
        } else {
            Ok(())
        }
    }
}
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        trace.push(units);
        if let Some((at, cause)) = &self.refusal
            && trace.len() == *at + 1
        {
            Err(cause.clone())
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("pure cast must not wait")
    }
}
fn ty(carrier: DataType, nullable: bool) -> FunctionValueType {
    FunctionValueType::from_legacy_declared_projection(carrier, nullable)
}
fn policies() -> [DecimalOverflowPolicy; 2] {
    [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ]
}
fn unsigned_width(carrier: &DataType) -> Option<u32> {
    match carrier {
        DataType::UInt8 => Some(8),
        DataType::UInt16 => Some(16),
        DataType::UInt32 => Some(32),
        DataType::UInt64 => Some(64),
        _ => None,
    }
}
fn signed_width(carrier: &DataType) -> Option<u32> {
    match carrier {
        DataType::Int8 => Some(8),
        DataType::Int16 => Some(16),
        DataType::Int32 => Some(32),
        DataType::Int64 => Some(64),
        _ => None,
    }
}
// Test cases enumerate every ordered profile involving UInt; this is not a
// production capability table or an alternate cast author.
fn profiles() -> Vec<(DataType, DataType)> {
    let carriers = [
        DataType::Boolean,
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
    ];
    carriers
        .iter()
        .flat_map(|source| {
            carriers.iter().filter_map(move |target| {
                (unsigned_width(source).is_some() || unsigned_width(target).is_some())
                    .then_some((source.clone(), target.clone()))
            })
        })
        .collect()
}
fn expected_successful_null(source: &DataType, target: &DataType, allow: bool) -> bool {
    if let Some(source) = unsigned_width(source) {
        if let Some(target) = unsigned_width(target) {
            return target < source;
        }
        if let Some(target) = signed_width(target) {
            return target <= source;
        }
    }
    if unsigned_width(target).is_some() {
        if signed_width(source).is_some() {
            return true;
        }
        if matches!(source, DataType::Float32 | DataType::Float64) {
            return !allow;
        }
    }
    false
}
fn context() -> ExpressionEffectContext {
    ExpressionEffectContext {
        use_id: ExpressionUseId::new(71),
        domain: EvaluationDomainId::new(91),
        demand: EvaluationDemand::Value,
    }
}
fn prepare(
    source: &DataType,
    target: &DataType,
    nullable: bool,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> PreparedCastRecipe {
    PreparedCastRecipe::try_new(
        CastOperation::Carrier,
        &ty(source.clone(), nullable),
        &ty(
            target.clone(),
            nullable || expected_successful_null(source, target, allow),
        ),
        policy,
        allow,
        &CompileControl::default(),
    )
    .unwrap()
}
fn input(source: &DataType) -> ArrayRef {
    match source {
        DataType::Boolean => Arc::new(BooleanArray::from(vec![Some(false), Some(true), None])),
        DataType::Int8 => Arc::new(Int8Array::from(vec![
            Some(0),
            Some(1),
            Some(-1),
            Some(i8::MIN),
            Some(i8::MAX),
            None,
        ])),
        DataType::Int16 => Arc::new(Int16Array::from(vec![
            Some(0),
            Some(1),
            Some(-1),
            Some(i16::MIN),
            Some(i16::MAX),
            None,
        ])),
        DataType::Int32 => Arc::new(Int32Array::from(vec![
            Some(0),
            Some(1),
            Some(-1),
            Some(i32::MIN),
            Some(i32::MAX),
            None,
        ])),
        DataType::Int64 => Arc::new(Int64Array::from(vec![
            Some(0),
            Some(1),
            Some(-1),
            Some(i64::MIN),
            Some(i64::MAX),
            None,
        ])),
        DataType::UInt8 => Arc::new(UInt8Array::from(vec![
            Some(0),
            Some(1),
            Some(u8::MAX),
            None,
        ])),
        DataType::UInt16 => Arc::new(UInt16Array::from(vec![
            Some(0),
            Some(1),
            Some(u16::MAX),
            None,
        ])),
        DataType::UInt32 => Arc::new(UInt32Array::from(vec![
            Some(0),
            Some(1),
            Some(u32::MAX),
            None,
        ])),
        DataType::UInt64 => Arc::new(UInt64Array::from(vec![
            Some(0),
            Some(1),
            Some(u64::MAX),
            None,
        ])),
        DataType::Float32 => Arc::new(Float32Array::from(vec![
            Some(0.0),
            Some(-0.5),
            Some(-1.0),
            Some(f32::NAN),
            Some(f32::INFINITY),
            Some(f32::NEG_INFINITY),
            None,
        ])),
        DataType::Float64 => Arc::new(Float64Array::from(vec![
            Some(0.0),
            Some(-0.5),
            Some(-1.0),
            Some(f64::NAN),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
            None,
        ])),
        _ => panic!("explicit primitive fixture"),
    }
}
// The independent complete matrix oracle uses the locked public scalar API.
// Hand-authored boundary tests below do not use this oracle.
fn arrow_expected(array: &ArrayRef, target: &DataType, row: usize) -> Option<CastRowResult> {
    if array.is_null(row) {
        return Some(CastRowResult::Null);
    }
    macro_rules! convert {
        ($array:ty, $native:ty) => {{
            let value = array.as_any().downcast_ref::<$array>().unwrap().value(row);
            match target {
                DataType::UInt8 => arrow_cast::cast::num_cast::<$native, u8>(value)
                    .map(|v| CastRowResult::Unsigned(u64::from(v))),
                DataType::UInt16 => arrow_cast::cast::num_cast::<$native, u16>(value)
                    .map(|v| CastRowResult::Unsigned(u64::from(v))),
                DataType::UInt32 => arrow_cast::cast::num_cast::<$native, u32>(value)
                    .map(|v| CastRowResult::Unsigned(u64::from(v))),
                DataType::UInt64 => {
                    arrow_cast::cast::num_cast::<$native, u64>(value).map(CastRowResult::Unsigned)
                }
                DataType::Int8 => arrow_cast::cast::num_cast::<$native, i8>(value)
                    .map(|v| CastRowResult::Signed(i64::from(v))),
                DataType::Int16 => arrow_cast::cast::num_cast::<$native, i16>(value)
                    .map(|v| CastRowResult::Signed(i64::from(v))),
                DataType::Int32 => arrow_cast::cast::num_cast::<$native, i32>(value)
                    .map(|v| CastRowResult::Signed(i64::from(v))),
                DataType::Int64 => {
                    arrow_cast::cast::num_cast::<$native, i64>(value).map(CastRowResult::Signed)
                }
                DataType::Float32 => {
                    arrow_cast::cast::num_cast::<$native, f32>(value).map(CastRowResult::Float32)
                }
                DataType::Float64 => {
                    arrow_cast::cast::num_cast::<$native, f64>(value).map(CastRowResult::Float64)
                }
                DataType::Boolean => Some(CastRowResult::Boolean(
                    arrow_cast::cast::cast_num_to_bool(value),
                )),
                _ => panic!("test target outside the matrix"),
            }
        }};
    }
    match array.data_type() {
        DataType::UInt8 => convert!(UInt8Array, u8),
        DataType::UInt16 => convert!(UInt16Array, u16),
        DataType::UInt32 => convert!(UInt32Array, u32),
        DataType::UInt64 => convert!(UInt64Array, u64),
        DataType::Int8 => convert!(Int8Array, i8),
        DataType::Int16 => convert!(Int16Array, i16),
        DataType::Int32 => convert!(Int32Array, i32),
        DataType::Int64 => convert!(Int64Array, i64),
        DataType::Float32 => convert!(Float32Array, f32),
        DataType::Float64 => convert!(Float64Array, f64),
        DataType::Boolean => {
            let value = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(row);
            let value = if value { 1_u64 } else { 0 };
            Some(CastRowResult::Unsigned(value))
        }
        _ => panic!("test source outside the matrix"),
    }
}
fn at(recipe: &PreparedCastRecipe, input: &ArrayRef, row: usize) -> CastRowResult {
    recipe
        .evaluate_row(
            EvaluatedArgument::Column(input),
            row,
            row,
            &Control::default(),
        )
        .unwrap()
}
fn assert_exact(actual: CastRowResult, expected: CastRowResult) {
    match (actual, expected) {
        (CastRowResult::Float32(actual), CastRowResult::Float32(expected)) => {
            assert_eq!(actual.to_bits(), expected.to_bits())
        }
        (CastRowResult::Float64(actual), CastRowResult::Float64(expected)) => {
            assert_eq!(actual.to_bits(), expected.to_bits())
        }
        (actual, expected) => assert_eq!(actual, expected),
    }
}
fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 8,
        max_array_nodes: 8,
        max_logical_elements: 64,
        max_retained_buffer_bytes: 4096,
        max_type_depth: 8,
        max_type_nodes: 64,
        max_dictionary_depth: 4,
        max_metadata_bytes: 1024,
        max_library_validation_work: 4096,
        max_library_validation_bytes: 8192,
    }
}
fn constant(array: ArrayRef, nullable: bool, ordinal: u32) -> crate::ConstantValue {
    let source = ty(array.data_type().clone(), nullable);
    ConstantPool::try_new(
        Arc::new(source.try_to_field("actual-uint-pool").unwrap()),
        source,
        array.to_data(),
        policy(),
        CompilePhase::Validate,
        &CompileControl::default(),
    )
    .unwrap()
    .value(ordinal)
    .unwrap()
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        invalid("control-owned invalid program"),
        internal("control-owned internal failure"),
        KernelFailure::Operational(crate::KernelDiagnostic::new("control-owned operation")),
        KernelFailure::InstanceFailed,
    ]
}

#[test]
fn all_seventy_two_unsigned_profiles_preserve_full_types_nullable_policy_and_exact_effects() {
    let pairs = profiles();
    assert_eq!(pairs.len(), 72);
    let mut total = 0;
    for (source, target) in pairs {
        if !expected_successful_null(&source, &target, false) {
            total += 1;
        }
        for policy in policies() {
            for allow in [false, true] {
                let can_null = expected_successful_null(&source, &target, allow);
                assert_eq!(
                    carrier_cast_can_produce_null(&source, &target, allow),
                    can_null
                );
                for nullable in [false, true] {
                    let recipe = prepare(&source, &target, nullable, policy, allow);
                    assert_eq!(recipe.operation(), CastOperation::Carrier);
                    assert_eq!(recipe.source_type(), &ty(source.clone(), nullable));
                    assert_eq!(
                        recipe.result_type(),
                        &ty(target.clone(), nullable || can_null)
                    );
                    assert_eq!(recipe.policy(), policy);
                    assert_eq!(recipe.allow_throw_exception(), allow);
                    assert_eq!(
                        recipe.own_effects(context()).for_use(context()).unwrap(),
                        ExpressionEffects {
                            may_raise_row_error: matches!(
                                source,
                                DataType::Float32 | DataType::Float64
                            ) && allow,
                            ..ExpressionEffects::PURE_VALUE
                        }
                    );
                    let narrow = PreparedCastRecipe::try_new(
                        CastOperation::Carrier,
                        &ty(source.clone(), nullable),
                        &ty(target.clone(), false),
                        policy,
                        allow,
                        &CompileControl::default(),
                    );
                    if nullable || can_null {
                        assert_eq!(narrow, Err(CastPrepareError::TypeMismatch));
                    } else {
                        narrow.unwrap();
                    }
                    // Conservative result nullability remains legal for every profile.
                    PreparedCastRecipe::try_new(
                        CastOperation::Carrier,
                        &ty(source.clone(), nullable),
                        &ty(target.clone(), true),
                        policy,
                        allow,
                        &CompileControl::default(),
                    )
                    .unwrap();
                }
                let recipe = prepare(&source, &target, true, policy, allow);
                let input = input(&source);
                for row in 0..input.len() {
                    let actual = at(&recipe, &input, row);
                    match arrow_expected(&input, &target, row) {
                        Some(expected) => assert_exact(actual, expected),
                        None if allow
                            && matches!(source, DataType::Float32 | DataType::Float64) =>
                        {
                            let CastRowResult::RowError(error) = actual else {
                                panic!("missing own float range error")
                            };
                            assert_eq!(error.selected_ordinal(), row);
                        }
                        None => assert_eq!(actual, CastRowResult::Null),
                    }
                }
            }
        }
    }
    assert_eq!(total, 32);
}

#[test]
fn signed_unsigned_integer_bounds_are_successful_null_even_under_allow_and_report_error() {
    let fixtures: Vec<(ArrayRef, DataType, Vec<CastRowResult>)> = vec![
        (
            Arc::new(Int64Array::from(vec![
                Some(-1),
                Some(i64::MIN),
                Some(0),
                Some(i64::MAX),
                None,
            ])),
            DataType::UInt64,
            vec![
                CastRowResult::Null,
                CastRowResult::Null,
                CastRowResult::Unsigned(0),
                CastRowResult::Unsigned(9223372036854775807),
                CastRowResult::Null,
            ],
        ),
        (
            Arc::new(UInt64Array::from(vec![
                Some(9223372036854775807),
                Some(9223372036854775808),
                Some(u64::MAX),
                None,
            ])),
            DataType::Int64,
            vec![
                CastRowResult::Signed(i64::MAX),
                CastRowResult::Null,
                CastRowResult::Null,
                CastRowResult::Null,
            ],
        ),
        (
            Arc::new(UInt16Array::from(vec![
                Some(255),
                Some(256),
                Some(u16::MAX),
                None,
            ])),
            DataType::UInt8,
            vec![
                CastRowResult::Unsigned(255),
                CastRowResult::Null,
                CastRowResult::Null,
                CastRowResult::Null,
            ],
        ),
        (
            Arc::new(UInt8Array::from(vec![
                Some(127),
                Some(128),
                Some(255),
                None,
            ])),
            DataType::Int8,
            vec![
                CastRowResult::Signed(127),
                CastRowResult::Null,
                CastRowResult::Null,
                CastRowResult::Null,
            ],
        ),
        (
            Arc::new(UInt8Array::from(vec![
                Some(127),
                Some(128),
                Some(255),
                None,
            ])),
            DataType::Int16,
            vec![
                CastRowResult::Signed(127),
                CastRowResult::Signed(128),
                CastRowResult::Signed(255),
                CastRowResult::Null,
            ],
        ),
    ];
    for (input, target, expected) in fixtures {
        for policy in policies() {
            for allow in [false, true] {
                let recipe = prepare(input.data_type(), &target, true, policy, allow);
                for (row, expected) in expected.iter().enumerate() {
                    assert_exact(at(&recipe, &input, row), expected.clone());
                }
            }
        }
    }
}

#[test]
fn float_unsigned_exclusive_bounds_allow_negative_fraction_zero_and_keep_original_range_errors() {
    for source in [DataType::Float32, DataType::Float64] {
        for target in [
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
        ] {
            let (inside, expected, outside) = match (&source, &target) {
                (_, DataType::UInt8) => (255.5, 255, 256.0),
                (_, DataType::UInt16) => (65535.5, 65535, 65536.0),
                (DataType::Float32, DataType::UInt32) => (
                    f64::from(f32::from_bits(0x4f7fffff)),
                    4294967040,
                    4294967296.0,
                ),
                (DataType::Float64, DataType::UInt32) => (4294967295.5, 4294967295, 4294967296.0),
                (DataType::Float32, DataType::UInt64) => (
                    f64::from(f32::from_bits(0x5f7fffff)),
                    18446742974197923840,
                    18446744073709551616.0,
                ),
                (DataType::Float64, DataType::UInt64) => (
                    f64::from_bits(0x43efffffffffffff),
                    18446744073709549568,
                    18446744073709551616.0,
                ),
                _ => unreachable!("eight exact unsigned profiles"),
            };
            let values = vec![
                Some(-0.5),
                Some(-0.0),
                Some(inside),
                Some(-1.0),
                Some(outside),
                Some(f64::NAN),
                Some(f64::INFINITY),
                Some(f64::NEG_INFINITY),
                None,
            ];
            let input: ArrayRef = if source == DataType::Float32 {
                Arc::new(Float32Array::from(
                    values
                        .iter()
                        .map(|v| v.map(|v| v as f32))
                        .collect::<Vec<_>>(),
                ))
            } else {
                Arc::new(Float64Array::from(values))
            };
            for policy in policies() {
                for allow in [false, true] {
                    let recipe = prepare(&source, &target, true, policy, allow);
                    for (row, expected) in [0, 0, expected].into_iter().enumerate() {
                        assert_eq!(at(&recipe, &input, row), CastRowResult::Unsigned(expected));
                    }
                    for row in 3..8 {
                        let actual = at(&recipe, &input, row);
                        if allow {
                            let CastRowResult::RowError(error) = actual else {
                                panic!("expected checked unsigned own row error")
                            };
                            assert_eq!(error.selected_ordinal(), row);
                            let name = match target {
                                DataType::UInt8 => "TINYINT UNSIGNED",
                                DataType::UInt16 => "SMALLINT UNSIGNED",
                                DataType::UInt32 => "INT UNSIGNED",
                                DataType::UInt64 => "BIGINT UNSIGNED",
                                _ => unreachable!(),
                            };
                            let value = if source == DataType::Float32 {
                                f64::from(
                                    input
                                        .as_any()
                                        .downcast_ref::<Float32Array>()
                                        .unwrap()
                                        .value(row),
                                )
                            } else {
                                input
                                    .as_any()
                                    .downcast_ref::<Float64Array>()
                                    .unwrap()
                                    .value(row)
                            };
                            assert_eq!(
                                error.message(),
                                format!(
                                    "Expr evaluate meet error: CAST failed: from {source:?} to {target:?}: {value} conflict with range of {name}"
                                )
                            );
                        } else {
                            assert_eq!(actual, CastRowResult::Null);
                        }
                    }
                    assert_eq!(at(&recipe, &input, 8), CastRowResult::Null);
                }
            }
        }
    }
}

#[test]
fn unsigned_float_and_boolean_casts_keep_direct_bits_and_do_not_promise_roundtrip_identity() {
    let input: ArrayRef = Arc::new(UInt64Array::from(vec![
        Some(0),
        Some(1),
        Some(u64::MAX),
        Some((1_u64 << 62) + (1_u64 << 38) + 1),
        Some((1_u64 << 24) + 1),
        Some((1_u64 << 53) + 1),
    ]));
    let f32_bits = [
        0_u32, 0x3f800000, 0x5f800000, 0x5e800001, 0x4b800000, 0x5a000000,
    ];
    let f64_bits = [
        0_u64,
        0x3ff0000000000000,
        0x43f0000000000000,
        0x43d0000010000000,
        0x4170000010000000,
        0x4340000000000000,
    ];
    for policy in policies() {
        for allow in [false, true] {
            for target in [DataType::Float32, DataType::Float64, DataType::Boolean] {
                let recipe = prepare(&DataType::UInt64, &target, false, policy, allow);
                for row in 0..input.len() {
                    match at(&recipe, &input, row) {
                        CastRowResult::Float32(value) => assert_eq!(value.to_bits(), f32_bits[row]),
                        CastRowResult::Float64(value) => assert_eq!(value.to_bits(), f64_bits[row]),
                        CastRowResult::Boolean(value) => assert_eq!(value, row != 0),
                        other => panic!("unexpected total UInt conversion {other:?}"),
                    }
                }
            }
            for target in [
                DataType::UInt8,
                DataType::UInt16,
                DataType::UInt32,
                DataType::UInt64,
            ] {
                let recipe = prepare(&DataType::Boolean, &target, true, policy, allow);
                let bools: ArrayRef =
                    Arc::new(BooleanArray::from(vec![Some(false), Some(true), None]));
                assert_eq!(at(&recipe, &bools, 0), CastRowResult::Unsigned(0));
                assert_eq!(at(&recipe, &bools, 1), CastRowResult::Unsigned(1));
                assert_eq!(at(&recipe, &bools, 2), CastRowResult::Null);
            }
            for rounded in [
                Arc::new(Float32Array::from(vec![f32::from_bits(0x5f800000)])) as ArrayRef,
                Arc::new(Float64Array::from(vec![f64::from_bits(0x43f0000000000000)])) as ArrayRef,
            ] {
                let recipe = prepare(rounded.data_type(), &DataType::UInt64, false, policy, allow);
                let actual = at(&recipe, &rounded, 0);
                if allow {
                    assert!(matches!(actual, CastRowResult::RowError(_)));
                } else {
                    assert_eq!(actual, CastRowResult::Null);
                }
            }
        }
    }
}

#[test]
fn unsigned_cast_uses_actual_constant_ordinal_scalar_slice_and_compact_source_addresses() {
    let recipe = prepare(
        &DataType::UInt64,
        &DataType::UInt64,
        true,
        DecimalOverflowPolicy::ReportError,
        true,
    );
    let pool: ArrayRef = Arc::new(UInt64Array::from(vec![Some(0), None, Some(u64::MAX)]));
    let value = constant(pool.clone(), true, 2);
    assert_eq!(value.ordinal(), 2);
    assert_eq!(
        recipe
            .evaluate_row(
                EvaluatedArgument::Constant(&value),
                81,
                999,
                &Control::default()
            )
            .unwrap(),
        CastRowResult::Unsigned(u64::MAX)
    );
    let null = constant(pool.clone(), true, 1);
    assert_eq!(
        recipe
            .evaluate_row(
                EvaluatedArgument::Constant(&null),
                81,
                999,
                &Control::default()
            )
            .unwrap(),
        CastRowResult::Null
    );
    let scalar = pool.slice(2, 1);
    assert_eq!(
        recipe
            .evaluate_row(
                EvaluatedArgument::Scalar(&scalar),
                81,
                999,
                &Control::default()
            )
            .unwrap(),
        CastRowResult::Unsigned(u64::MAX)
    );
    let dense: ArrayRef = Arc::new(UInt64Array::from(vec![None, Some(u64::MAX), None, Some(0)]));
    let dense = dense.slice(1, 3);
    let selection = Selection::try_sparse(3, &[0, 2]).unwrap();
    let compact = SelectedValues::try_new(
        selection,
        &DataType::UInt64,
        Arc::new(UInt64Array::from(vec![Some(u64::MAX), Some(0)])),
        Box::default(),
    )
    .unwrap();
    for (ordinal, row) in selection.iter().enumerate() {
        for argument in [
            EvaluatedArgument::Column(&dense),
            EvaluatedArgument::SelectedColumn(&compact),
        ] {
            assert_eq!(
                recipe
                    .evaluate_row(argument, ordinal, row, &Control::default())
                    .unwrap(),
                CastRowResult::Unsigned(if ordinal == 0 { u64::MAX } else { 0 })
            );
        }
    }
}

#[derive(Debug)]
struct ForeignUInt64(UInt64Array);
// SAFETY: The immutable canonical UInt64Array owns all buffers and supplies
// every layout/lifetime method. Only Any identity differs for the class gate.
unsafe impl Array for ForeignUInt64 {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn to_data(&self) -> arrow_data::ArrayData {
        self.0.to_data()
    }
    fn into_data(self) -> arrow_data::ArrayData {
        self.0.into_data()
    }
    fn data_type(&self) -> &DataType {
        self.0.data_type()
    }
    fn slice(&self, offset: usize, length: usize) -> ArrayRef {
        Arc::new(self.0.slice(offset, length))
    }
    fn len(&self) -> usize {
        self.0.len()
    }
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    fn offset(&self) -> usize {
        self.0.offset()
    }
    fn nulls(&self) -> Option<&arrow_buffer::NullBuffer> {
        self.0.nulls()
    }
    fn get_buffer_memory_size(&self) -> usize {
        self.0.get_buffer_memory_size()
    }
    fn get_array_memory_size(&self) -> usize {
        self.0.get_array_memory_size()
    }
}

#[test]
fn unsigned_cast_checks_class_full_constant_type_address_and_journal_before_null() {
    for target in [
        DataType::UInt8,
        DataType::UInt64,
        DataType::Int64,
        DataType::Float32,
        DataType::Boolean,
    ] {
        let recipe = prepare(
            &DataType::UInt64,
            &target,
            true,
            DecimalOverflowPolicy::OutputNull,
            false,
        );
        let foreign: ArrayRef = Arc::new(ForeignUInt64(UInt64Array::from(vec![None])));
        assert!(matches!(
            recipe.evaluate_row(
                EvaluatedArgument::Column(&foreign),
                0,
                0,
                &Control::default()
            ),
            Err(KernelFailure::Internal(_))
        ));
        let null: ArrayRef = Arc::new(UInt64Array::from(vec![None]));
        let wrong: ArrayRef = Arc::new(Int64Array::from(vec![None]));
        let many: ArrayRef = Arc::new(UInt64Array::from(vec![None, Some(1)]));
        for (argument, row) in [
            (EvaluatedArgument::Column(&wrong), 0),
            (EvaluatedArgument::Scalar(&many), 0),
            (EvaluatedArgument::Column(&many), 2),
        ] {
            assert!(matches!(
                recipe.evaluate_row(argument, 0, row, &Control::default()),
                Err(KernelFailure::InvalidProgram(_))
            ));
        }
        let selected = SelectedValues::try_new(
            Selection::try_sparse(2, &[1]).unwrap(),
            &DataType::UInt64,
            null.clone(),
            vec![RowDataError::new(0, "actual required child failure")].into_boxed_slice(),
        )
        .unwrap();
        for row in [0, 1] {
            assert!(matches!(
                recipe.evaluate_row(
                    EvaluatedArgument::SelectedColumn(&selected),
                    0,
                    row,
                    &Control::default()
                ),
                Err(KernelFailure::InvalidProgram(_))
            ));
        }
        let nonnull = prepare(
            &DataType::UInt64,
            &target,
            false,
            DecimalOverflowPolicy::ReportError,
            true,
        );
        assert!(matches!(
            nonnull.evaluate_row(EvaluatedArgument::Column(&null), 0, 0, &Control::default()),
            Err(KernelFailure::InvalidProgram(_))
        ));
        let nullable_constant = constant(Arc::new(UInt64Array::from(vec![Some(1)])), true, 0);
        assert!(matches!(
            nonnull.evaluate_row(
                EvaluatedArgument::Constant(&nullable_constant),
                0,
                0,
                &Control::default()
            ),
            Err(KernelFailure::InvalidProgram(_))
        ));
        let exact = constant(Arc::new(UInt64Array::from(vec![Some(1)])), false, 0);
        assert_exact(
            nonnull
                .evaluate_row(
                    EvaluatedArgument::Constant(&exact),
                    0,
                    0,
                    &Control::default(),
                )
                .unwrap(),
            arrow_expected(EvaluatedArgument::Constant(&exact).array(), &target, 0).unwrap(),
        );
    }
    super::super::tests::assert_f3_admitted_preparation(
        DataType::Boolean,
        DataType::Decimal128(10, 0),
    );
    super::super::tests::assert_f3_selected_literal_case(
        Arc::new(
            arrow_array::Decimal128Array::from(vec![Some(1200), Some(0), None])
                .with_precision_and_scale(10, 2)
                .unwrap(),
        ),
        Arc::new(arrow_array::UInt64Array::from(vec![
            Some(12_u64),
            Some(0),
            None,
        ])),
    );
    let boolean = ty(DataType::Boolean, true);
    let unsigned = ty(DataType::UInt64, true);
    for foreign in [
        ty(DataType::Float16, true),
        ty(DataType::FixedSizeBinary(16), true),
        FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            true,
            ValueLogicalType::LargeInt,
        )
        .unwrap(),
        FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            true,
            ValueLogicalType::Uuid,
        )
        .unwrap(),
    ] {
        for (source, target) in [(&foreign, &unsigned), (&boolean, &foreign)] {
            if source == &boolean && target == &ty(DataType::FixedSizeBinary(16), true) {
                // Exact Physical target only; nominal Fixed16 targets below still refuse.
                super::super::tests::assert_f3_admitted_preparation(
                    DataType::Boolean,
                    DataType::FixedSizeBinary(16),
                );
                continue;
            }
            assert_eq!(
                PreparedCastRecipe::try_new(
                    CastOperation::Carrier,
                    source,
                    target,
                    DecimalOverflowPolicy::OutputNull,
                    true,
                    &CompileControl::default()
                ),
                Err(CastPrepareError::Unsupported)
            );
        }
    }
}

#[test]
fn unsigned_cast_compile_three_causes_keep_actual_metadata_quantum_and_every_exact_prefix() {
    let wide = ty(
        DataType::Struct(
            (0..320)
                .map(|i| {
                    Arc::new(
                        Field::new(format!("actual-{i}"), DataType::UInt64, true).with_metadata(
                            HashMap::from([("provider".to_owned(), "actual-source".to_owned())]),
                        ),
                    )
                })
                .collect::<Vec<_>>()
                .into(),
        ),
        true,
    );
    let mut pairs = profiles()
        .into_iter()
        .map(|(source, target)| (ty(source, true), ty(target, true)))
        .collect::<Vec<_>>();
    pairs.push((wide, ty(DataType::UInt64, true)));
    for (source, target) in pairs {
        let baseline = CompileControl::default();
        let _ = PreparedCastRecipe::try_new(
            CastOperation::Carrier,
            &source,
            &target,
            DecimalOverflowPolicy::ReportError,
            true,
            &baseline,
        );
        let trace = baseline.trace.lock().unwrap().clone();
        assert_eq!(trace[0], 0);
        assert!(trace.last().copied().unwrap() > 0);
        if matches!(source.data_type(), DataType::Struct(_)) {
            assert!(trace.contains(&256));
        }
        for at in 0..trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = CompileControl {
                    refusal: Some((at, cause.clone())),
                    ..Default::default()
                };
                let error = PreparedCastRecipe::try_new(
                    CastOperation::Carrier,
                    &source,
                    &target,
                    DecimalOverflowPolicy::ReportError,
                    true,
                    &control,
                )
                .unwrap_err();
                assert_eq!(error.control_error(), Some(cause));
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
}

#[test]
fn unsigned_cast_runtime_seven_causes_keep_all_original_success_null_rowerror_and_outer_error_prefixes()
 {
    for (source, target) in profiles() {
        for allow in [false, true] {
            let recipe = prepare(
                &source,
                &target,
                true,
                DecimalOverflowPolicy::ReportError,
                allow,
            );
            let input = input(&source);
            for row in 0..=input.len() {
                let baseline = Control::default();
                let result =
                    recipe.evaluate_row(EvaluatedArgument::Column(&input), row, row, &baseline);
                let trace = baseline.trace.lock().unwrap().clone();
                assert_eq!(trace[0], 0);
                assert!(trace.iter().any(|units| *units > 0));
                if matches!(result, Ok(CastRowResult::RowError(_))) {
                    // Formatting and publication each flush the same original work.
                    assert!(trace.len() >= 4);
                    assert_eq!(trace.last(), Some(&0));
                } else {
                    assert!(trace.last().copied().unwrap() > 0);
                }
                for at in 0..trace.len() {
                    for cause in causes() {
                        let control = Control {
                            refusal: Some((at, cause.clone())),
                            ..Default::default()
                        };
                        assert_eq!(
                            recipe.evaluate_row(
                                EvaluatedArgument::Column(&input),
                                row,
                                row,
                                &control
                            ),
                            Err(cause)
                        );
                        assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
                    }
                }
            }
        }
    }
}
