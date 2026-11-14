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
use arrow_array::{ArrayRef, BooleanArray};
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
// This enumerates test cases, not the production capability or binding author.
fn profiles() -> Vec<(DataType, DataType)> {
    let mut pairs = vec![(DataType::Boolean, DataType::Boolean)];
    for numeric in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
    ] {
        pairs.push((DataType::Boolean, numeric.clone()));
        pairs.push((numeric, DataType::Boolean));
    }
    pairs
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
        &ty(target.clone(), nullable),
        policy,
        allow,
        &CompileControl::default(),
    )
    .unwrap()
}
fn context() -> ExpressionEffectContext {
    ExpressionEffectContext {
        use_id: ExpressionUseId::new(71),
        domain: EvaluationDomainId::new(91),
        demand: EvaluationDemand::Value,
    }
}
fn bool_array(values: Vec<Option<bool>>) -> ArrayRef {
    Arc::new(BooleanArray::from(values))
}
fn numbers(carrier: &DataType) -> ArrayRef {
    match carrier {
        DataType::Int8 => Arc::new(Int8Array::from(vec![
            Some(0),
            Some(-1),
            Some(i8::MIN),
            Some(i8::MAX),
            None,
        ])),
        DataType::Int16 => Arc::new(Int16Array::from(vec![
            Some(0),
            Some(-1),
            Some(i16::MIN),
            Some(i16::MAX),
            None,
        ])),
        DataType::Int32 => Arc::new(Int32Array::from(vec![
            Some(0),
            Some(-1),
            Some(i32::MIN),
            Some(i32::MAX),
            None,
        ])),
        DataType::Int64 => Arc::new(Int64Array::from(vec![
            Some(0),
            Some(-1),
            Some(i64::MIN),
            Some(i64::MAX),
            None,
        ])),
        DataType::Float32 => Arc::new(Float32Array::from(vec![
            Some(0.0),
            Some(-1.0),
            Some(f32::MIN),
            Some(f32::MAX),
            None,
        ])),
        DataType::Float64 => Arc::new(Float64Array::from(vec![
            Some(0.0),
            Some(-1.0),
            Some(f64::MIN),
            Some(f64::MAX),
            None,
        ])),
        _ => panic!("explicit numeric fixture"),
    }
}
fn eval(recipe: &PreparedCastRecipe, input: &ArrayRef, row: usize) -> CastRowResult {
    recipe
        .evaluate_row(
            EvaluatedArgument::Column(input),
            row,
            row,
            &Control::default(),
        )
        .unwrap()
}
fn expect_bool_numeric(value: CastRowResult, target: &DataType, expected: Option<bool>) {
    match (value, target, expected) {
        (CastRowResult::Null, _, None) => (),
        (CastRowResult::Boolean(value), DataType::Boolean, Some(expected)) => {
            assert_eq!(value, expected)
        }
        (
            CastRowResult::Signed(value),
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64,
            Some(expected),
        ) => assert_eq!(value, if expected { 1 } else { 0 }),
        (CastRowResult::Float32(value), DataType::Float32, Some(expected)) => {
            assert_eq!(value.to_bits(), if expected { 0x3f800000 } else { 0 })
        }
        (CastRowResult::Float64(value), DataType::Float64, Some(expected)) => assert_eq!(
            value.to_bits(),
            if expected { 0x3ff0000000000000 } else { 0 }
        ),
        (value, _, expected) => {
            panic!("unexpected {target:?} output {value:?}, expected {expected:?}")
        }
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
    let t = ty(array.data_type().clone(), nullable);
    ConstantPool::try_new(
        Arc::new(t.try_to_field("actual-bool-pool").unwrap()),
        t,
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
fn all_thirteen_bool_profiles_freeze_full_types_inert_policies_and_source_only_nullability() {
    let pairs = profiles();
    assert_eq!(pairs.len(), 13);
    for (source, target) in pairs {
        for nullable in [false, true] {
            for policy in policies() {
                for allow in [false, true] {
                    let recipe = prepare(&source, &target, nullable, policy, allow);
                    assert_eq!(recipe.operation(), CastOperation::Carrier);
                    assert_eq!(recipe.source_type(), &ty(source.clone(), nullable));
                    assert_eq!(recipe.result_type(), &ty(target.clone(), nullable));
                    assert_eq!(recipe.decimal_overflow_policy(), policy);
                    assert_eq!(recipe.allow_throw_exception(), allow);
                    assert_eq!(
                        recipe.own_effects(context()).for_use(context()).unwrap(),
                        ExpressionEffects::PURE_VALUE
                    );
                    // A conservative nullable result is legal for a nonnullable source.
                    PreparedCastRecipe::try_new(
                        CastOperation::Carrier,
                        &ty(source.clone(), false),
                        &ty(target.clone(), true),
                        policy,
                        allow,
                        &CompileControl::default(),
                    )
                    .unwrap();
                    assert_eq!(
                        PreparedCastRecipe::try_new(
                            CastOperation::Carrier,
                            &ty(source.clone(), true),
                            &ty(target.clone(), false),
                            policy,
                            allow,
                            &CompileControl::default()
                        ),
                        Err(CastPrepareError::TypeMismatch)
                    );
                }
            }
        }
        for policy in policies() {
            for allow in [false, true] {
                let recipe = prepare(&source, &target, true, policy, allow);
                if source == DataType::Boolean {
                    let input = bool_array(vec![Some(false), Some(true), None]);
                    for (row, expected) in [Some(false), Some(true), None].into_iter().enumerate() {
                        expect_bool_numeric(eval(&recipe, &input, row), &target, expected);
                    }
                } else {
                    let input = numbers(&source);
                    for (row, expected) in [Some(false), Some(true), Some(true), Some(true), None]
                        .into_iter()
                        .enumerate()
                    {
                        expect_bool_numeric(
                            eval(&recipe, &input, row),
                            &DataType::Boolean,
                            expected,
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn floating_bool_cast_uses_ieee_nonzero_for_nan_payloads_infinities_signed_zero_and_subnormals() {
    let f32_bits = [
        0, 0x80000000, 1, 0x80000001, 0x7f800000, 0xff800000, 0x7fc00123, 0xffc00456, 0x7f800001,
        0xff800002, 0x7f7fffff, 0xff7fffff,
    ];
    let f64_bits = [
        0,
        0x8000000000000000,
        1,
        0x8000000000000001,
        0x7ff0000000000000,
        0xfff0000000000000,
        0x7ff8000000000123,
        0xfff8000000000456,
        0x7ff0000000000001,
        0xfff0000000000002,
        0x7fefffffffffffff,
        0xffefffffffffffff,
    ];
    let f32: ArrayRef = Arc::new(Float32Array::from(f32_bits.map(f32::from_bits).to_vec()));
    let f64: ArrayRef = Arc::new(Float64Array::from(f64_bits.map(f64::from_bits).to_vec()));
    for input in [f32, f64] {
        for policy in policies() {
            for allow in [false, true] {
                let recipe = prepare(input.data_type(), &DataType::Boolean, false, policy, allow);
                let old = arrow_cast::cast(input.as_ref(), &DataType::Boolean).unwrap();
                let old = old.as_any().downcast_ref::<BooleanArray>().unwrap();
                for row in 0..input.len() {
                    let expected = row >= 2; // Independent truth table, not a numeric conversion oracle.
                    assert_eq!(eval(&recipe, &input, row), CastRowResult::Boolean(expected));
                    assert_eq!(old.value(row), expected);
                }
            }
        }
    }
}

#[test]
fn bool_cast_preserves_nonzero_constant_pool_ordinal_scalar_slice_and_selected_compact_addresses() {
    for (source, target) in [
        (DataType::Boolean, DataType::Int64),
        (DataType::Int64, DataType::Boolean),
    ] {
        let recipe = prepare(
            &source,
            &target,
            true,
            DecimalOverflowPolicy::ReportError,
            true,
        );
        let whole: ArrayRef = if source == DataType::Boolean {
            bool_array(vec![Some(false), None, Some(true)])
        } else {
            Arc::new(Int64Array::from(vec![Some(0), None, Some(-7)]))
        };
        let value = constant(whole.clone(), true, 2);
        assert_eq!(value.ordinal(), 2);
        expect_bool_numeric(
            recipe
                .evaluate_row(
                    EvaluatedArgument::Constant(&value),
                    97,
                    999,
                    &Control::default(),
                )
                .unwrap(),
            &target,
            Some(true),
        );
        let null = constant(whole.clone(), true, 1);
        assert_eq!(
            recipe
                .evaluate_row(
                    EvaluatedArgument::Constant(&null),
                    97,
                    999,
                    &Control::default()
                )
                .unwrap(),
            CastRowResult::Null
        );
        let scalar = whole.slice(2, 1);
        expect_bool_numeric(
            recipe
                .evaluate_row(
                    EvaluatedArgument::Scalar(&scalar),
                    97,
                    999,
                    &Control::default(),
                )
                .unwrap(),
            &target,
            Some(true),
        );
        let dense: ArrayRef = if source == DataType::Boolean {
            bool_array(vec![None, Some(true), None, Some(false)]).slice(1, 3)
        } else {
            (Arc::new(Int64Array::from(vec![None, Some(-7), None, Some(0)])) as ArrayRef)
                .slice(1, 3)
        };
        let compact: ArrayRef = if source == DataType::Boolean {
            bool_array(vec![Some(true), Some(false)])
        } else {
            Arc::new(Int64Array::from(vec![Some(-7), Some(0)]))
        };
        let selection = Selection::try_sparse(3, &[0, 2]).unwrap();
        let selected =
            SelectedValues::try_new(selection, &source, compact, Box::default()).unwrap();
        for (ordinal, row) in selection.iter().enumerate() {
            for argument in [
                EvaluatedArgument::Column(&dense),
                EvaluatedArgument::SelectedColumn(&selected),
            ] {
                expect_bool_numeric(
                    recipe
                        .evaluate_row(argument, ordinal, row, &Control::default())
                        .unwrap(),
                    &target,
                    Some(ordinal == 0),
                );
            }
        }
    }
}

#[derive(Debug)]
struct ForeignBoolean(BooleanArray);
// SAFETY: Buffer, layout and lifetime methods delegate to the immutable canonical
// BooleanArray. Only Any identity differs to test the required concrete-class gate.
unsafe impl Array for ForeignBoolean {
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
fn bool_cast_checks_concrete_classes_addresses_required_journals_and_nullable_promises_before_null()
{
    for target in [
        DataType::Boolean,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
    ] {
        let recipe = prepare(
            &DataType::Boolean,
            &target,
            true,
            DecimalOverflowPolicy::OutputNull,
            false,
        );
        let foreign: ArrayRef = Arc::new(ForeignBoolean(BooleanArray::from(vec![None])));
        assert!(matches!(
            recipe.evaluate_row(
                EvaluatedArgument::Column(&foreign),
                0,
                0,
                &Control::default()
            ),
            Err(KernelFailure::Internal(_))
        ));
        let wrong: ArrayRef = Arc::new(Int64Array::from(vec![None]));
        let many = bool_array(vec![None, Some(true)]);
        let empty = bool_array(vec![]);
        for (argument, ordinal, row) in [
            (EvaluatedArgument::Column(&wrong), 0, 0),
            (EvaluatedArgument::Scalar(&many), 0, 0),
            (EvaluatedArgument::Column(&many), 0, 2),
            (EvaluatedArgument::Column(&empty), 0, 0),
        ] {
            assert!(matches!(
                recipe.evaluate_row(argument, ordinal, row, &Control::default()),
                Err(KernelFailure::InvalidProgram(_))
            ));
        }
        let selected = SelectedValues::try_new(
            Selection::try_sparse(2, &[1]).unwrap(),
            &DataType::Boolean,
            bool_array(vec![None]),
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
        let strict = prepare(
            &DataType::Boolean,
            &target,
            false,
            DecimalOverflowPolicy::ReportError,
            true,
        );
        assert!(matches!(
            strict.evaluate_row(EvaluatedArgument::Column(&many), 0, 0, &Control::default()),
            Err(KernelFailure::InvalidProgram(_))
        ));
        let nullable_nonnull = constant(bool_array(vec![Some(true)]), true, 0);
        assert!(matches!(
            strict.evaluate_row(
                EvaluatedArgument::Constant(&nullable_nonnull),
                0,
                0,
                &Control::default()
            ),
            Err(KernelFailure::InvalidProgram(_))
        ));
        let nonnull = constant(bool_array(vec![Some(true)]), false, 0);
        expect_bool_numeric(
            strict
                .evaluate_row(
                    EvaluatedArgument::Constant(&nonnull),
                    0,
                    0,
                    &Control::default(),
                )
                .unwrap(),
            &target,
            Some(true),
        );
    }
}

#[test]
fn bool_cast_does_not_author_nominal_fixed_bytes_encoded_or_unsupported_operation_capabilities() {
    let boolean = ty(DataType::Boolean, true);
    for source in [
        ty(DataType::Binary, true),
        ty(DataType::Float16, true),
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
        FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
            .unwrap(),
    ] {
        for (source, result) in [(&source, &boolean), (&boolean, &source)] {
            assert_eq!(
                PreparedCastRecipe::try_new(
                    CastOperation::Carrier,
                    source,
                    result,
                    DecimalOverflowPolicy::OutputNull,
                    true,
                    &CompileControl::default()
                ),
                Err(CastPrepareError::Unsupported)
            );
        }
    }
    // Boolean->decimal is approved P05; the reverse D128->Boolean still refuses.
    super::super::tests::assert_f3_admitted_preparation(
        DataType::Boolean,
        DataType::Decimal128(10, 0),
    );
    assert_eq!(
        PreparedCastRecipe::try_new(
            CastOperation::Carrier,
            &ty(DataType::Decimal128(10, 0), true),
            &boolean,
            DecimalOverflowPolicy::OutputNull,
            true,
            &CompileControl::default(),
        ),
        Err(CastPrepareError::Unsupported)
    );
    let fixed = Arc::new(
        arrow_array::FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            [Some(1_i128.to_be_bytes()), Some(0_i128.to_be_bytes()), None].into_iter(),
            16,
        )
        .unwrap(),
    ) as ArrayRef;
    let bools = Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])) as ArrayRef;
    super::super::tests::assert_f3_selected_literal_case(Arc::clone(&bools), Arc::clone(&fixed));
    super::super::tests::assert_f3_selected_literal_case(fixed, bools);
    for operation in [CastOperation::Time, CastOperation::TimeFromDatetime] {
        assert_eq!(
            PreparedCastRecipe::try_new(
                operation,
                &boolean,
                &boolean,
                DecimalOverflowPolicy::ReportError,
                true,
                &CompileControl::default()
            ),
            Err(CastPrepareError::Unsupported)
        );
    }
}

#[test]
fn bool_cast_compile_three_causes_keep_entry_metadata_quantum_and_exact_primary_prefix() {
    let wide = ty(
        DataType::Struct(
            (0..320)
                .map(|i| {
                    Arc::new(
                        Field::new(format!("actual-{i}"), DataType::Boolean, true).with_metadata(
                            HashMap::from([("provider".to_owned(), "actual-source".to_owned())]),
                        ),
                    )
                })
                .collect::<Vec<_>>()
                .into(),
        ),
        true,
    );
    let mut sources = profiles()
        .into_iter()
        .map(|(source, target)| (ty(source, false), ty(target, false)))
        .collect::<Vec<_>>();
    sources.push((wide, ty(DataType::Boolean, true)));
    for (source, target) in sources {
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
fn bool_cast_runtime_seven_causes_keep_success_null_and_ordinary_error_exact_refusing_prefixes() {
    for (source, target) in profiles() {
        let recipe = prepare(
            &source,
            &target,
            true,
            DecimalOverflowPolicy::ReportError,
            true,
        );
        let input = if source == DataType::Boolean {
            bool_array(vec![Some(true), None])
        } else {
            numbers(&source)
        };
        let null_row = input.len() - 1;
        for row in [0, null_row, input.len()] {
            let baseline = Control::default();
            let _ = recipe.evaluate_row(EvaluatedArgument::Column(&input), row, row, &baseline);
            let trace = baseline.trace.lock().unwrap().clone();
            assert_eq!(trace[0], 0);
            assert!(trace.last().copied().unwrap() > 0);
            for at in 0..trace.len() {
                for cause in causes() {
                    let control = Control {
                        refusal: Some((at, cause.clone())),
                        ..Default::default()
                    };
                    assert_eq!(
                        recipe.evaluate_row(EvaluatedArgument::Column(&input), row, row, &control),
                        Err(cause)
                    );
                    assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
                }
            }
        }
    }
}
