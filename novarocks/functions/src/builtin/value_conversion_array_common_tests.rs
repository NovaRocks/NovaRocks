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

use super::*;
use crate::{
    EvaluatedArgument, FunctionArgument, FunctionArgumentType, FunctionResultType,
    FunctionSpecializationFailure, FunctionValueType, ScalarEvaluationInstance,
    ScopedExpressionEffects, Selection, specialize_frozen_scalar, specialize_scalar,
};
use arrow_array::{Array, ArrayRef, Decimal128Array, Float64Array};
use arrow_schema::DataType;
use novarocks_type_contract::{
    CompileControlError, EvaluationDemand, EvaluationRegionId, ExpressionUseId, SemanticParameters,
    ValueLogicalType,
};
use std::{sync::Mutex, time::Duration};

#[derive(Default)]
struct CompileControl {
    calls: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for CompileControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut calls = self.calls.lock().unwrap();
        let at = calls.len();
        if let Some((stop, _)) = &self.refusal {
            assert!(at <= *stop, "callback after original refusal");
        }
        calls.push((phase, units));
        match &self.refusal {
            Some((stop, cause)) if at == *stop => Err(cause.clone()),
            _ => Ok(()),
        }
    }
}
#[derive(Default)]
struct Control {
    calls: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        let mut calls = self.calls.lock().unwrap();
        let at = calls.len();
        if let Some((stop, _)) = &self.refusal {
            assert!(at <= *stop, "callback after original refusal");
        }
        calls.push(units);
        match &self.refusal {
            Some((stop, cause)) if at == *stop => Err(cause.clone()),
            _ => Ok(()),
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("pure common conversion never waits")
    }
}
fn owner() -> ValueConversionOwner {
    let (declaration, resolver) = value_conversion::definition_parts().unwrap();
    ValueConversionOwner::new(declaration, resolver).unwrap()
}
fn context() -> novarocks_type_contract::ExpressionEffectContext {
    novarocks_type_contract::ExpressionEffectContext {
        use_id: ExpressionUseId::new(811),
        region: EvaluationRegionId::new(812),
        demand: EvaluationDemand::Value,
    }
}
fn argument(source: FunctionValueType) -> FunctionArgument {
    FunctionArgument::Value {
        value_type: source,
        constant: None,
    }
}
fn request<'a>(
    args: &'a [FunctionArgument],
    target: &'a FunctionValueType,
) -> FunctionBindingRequest<'a> {
    FunctionBindingRequest {
        arguments: args,
        logical_argument_count: 1,
        expected_result_type: Some(target),
    }
}
fn prepare(
    source: FunctionValueType,
    target: &FunctionValueType,
    policy: DecimalOverflowPolicy,
    control: &dyn PureCompileControl,
) -> Result<Arc<dyn PreparedScalarKernel>, FunctionSpecializationFailure> {
    let owner = owner();
    let args = [argument(source)];
    let selected = Arc::new(owner.select_at_overload_observed(
        &FunctionOverloadId::try_new(value_conversion::ARRAY_COMMON_COERCION).unwrap(),
        request(&args, target),
        control,
    )?);
    let parameters = SemanticParameters::try_new([]).unwrap();
    let uses = [Some(ExpressionUseId::new(813))];
    let input = CallEffectInput {
        context: context().into(),
        argument_uses: crate::CallArgumentUses::SelectedChannels(&uses),
        function_id: owner.declaration.function_id(),
        kind: FunctionKind::Scalar,
        selected: &selected,
        request: request(&args, target),
        environment: &[],
        parameters: &parameters,
        decimal_overflow_policy: policy,
        proof_scope: CallProofScope::Region(context().region),
    };
    let fresh = specialize_scalar(
        &owner,
        input,
        Arc::clone(&selected),
        ScopedExpressionEffects::pure_value(context()),
        control,
    )?
    .into_prepared();
    // Replay authenticates the chosen family, exact full types and real effects;
    // it must not rerun the automatic nine-family resolver.
    let replay = specialize_frozen_scalar(
        &owner,
        input,
        Arc::clone(&selected),
        fresh.contract().effects(),
        ScopedExpressionEffects::pure_value(context()),
        control,
    )?
    .into_prepared();
    assert_eq!(replay.contract(), fresh.contract());
    Ok(replay)
}
#[test]
fn common_family_is_exact_only_hidden_and_replayable_without_auto_resolution() {
    assert_eq!(
        value_conversion::value_conversion_definition()
            .unwrap()
            .visibility(),
        FunctionVisibility::Hidden
    );
    let owner = owner();
    let source = FunctionValueType::new(DataType::Decimal128(20, 1), false);
    let target = FunctionValueType::new(DataType::Float64, false);
    let args = [argument(source.clone())];
    let control = CompileControl::default();
    assert!(matches!(
        owner.resolve(request(&args, &target), &control),
        Err(FunctionBindingError::NoMatchingOverload)
    ));
    assert!(matches!(
        value_conversion::recipe_family(&source, &target),
        Err(FunctionBindingError::NoMatchingOverload)
    ));
    let overload = FunctionOverloadId::try_new(value_conversion::ARRAY_COMMON_COERCION).unwrap();
    let selected = owner
        .select_at_overload_observed(&overload, request(&args, &target), &control)
        .unwrap();
    assert_eq!(
        selected.argument_types.as_ref(),
        &[FunctionArgumentType::Value(source)]
    );
    assert_eq!(
        selected.result_type,
        FunctionResultType::Scalar(target.clone())
    );
    owner
        .validate_selected(&selected, request(&args, &target), &control)
        .unwrap();
    let wrong = FunctionValueType::new(DataType::Int32, false);
    assert!(
        owner
            .validate_selected(&selected, request(&args, &wrong), &control)
            .is_err()
    );
    let json = FunctionValueType::try_with_logical_type(
        DataType::LargeBinary,
        false,
        ValueLogicalType::Json,
    )
    .unwrap();
    let text = FunctionValueType::new(DataType::Utf8, true);
    let json_args = [argument(json)];
    assert!(
        owner
            .select_at_overload_observed(&overload, request(&json_args, &text), &control)
            .is_err()
    );
    let unknown = FunctionOverloadId::try_new("test/common/not-installed").unwrap();
    assert!(
        owner
            .select_at_overload_observed(&unknown, request(&args, &target), &control)
            .is_err()
    );
}
#[test]
fn common_family_effects_are_real_may_raise_under_every_frozen_decimal_policy() {
    for policy in [
        DecimalOverflowPolicy::ReportError,
        DecimalOverflowPolicy::OutputNull,
    ] {
        let prepared = prepare(
            FunctionValueType::new(DataType::Decimal128(38, 0), false),
            &FunctionValueType::new(DataType::Decimal128(38, 1), false),
            policy,
            &CompileControl::default(),
        )
        .unwrap();
        assert_eq!(
            prepared.contract().effects().own_row_error,
            FunctionIntrinsicRowError::MayRaise
        );
        assert_eq!(
            prepared.contract().effects().failure_behavior,
            crate::FunctionFailureBehavior::Propagate
        );
        assert_eq!(
            prepared.contract().effects().null_behavior,
            novarocks_type_contract::FunctionNullBehavior::Strict
        );
        let source: ArrayRef = Arc::new(
            Decimal128Array::from(vec![10_i128.pow(37), 1])
                .with_precision_and_scale(38, 0)
                .unwrap(),
        );
        let args = [EvaluatedArgument::Column(&source)];
        let mut instance = ScalarEvaluationInstance::instantiate(prepared).unwrap();
        let out = instance
            .evaluate(Selection::all(2), &args, &Control::default())
            .unwrap();
        assert_eq!(out.errors().len(), 1);
        assert_eq!(out.errors()[0].selected_ordinal(), 0);
        assert_eq!(
            out.errors()[0].reason(),
            crate::DataFailureReason::Domain(
                novarocks_function_contract::kernel_failure::NUMERIC_OUT_OF_RANGE
            )
        );
        assert!(out.values().is_null(0));
        assert_eq!(
            out.values()
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value(1),
            10
        );
        assert_eq!(
            instance.retained_bytes().unwrap(),
            instance.retained_upper_bound()
        );
    }
}
#[test]
fn common_decimal_double_uses_independent_reference_bits_on_original_selected_addresses() {
    let source = FunctionValueType::new(DataType::Decimal128(20, 1), false);
    let target = FunctionValueType::new(DataType::Float64, false);
    let prepared = prepare(
        source,
        &target,
        DecimalOverflowPolicy::OutputNull,
        &CompileControl::default(),
    )
    .unwrap();
    let values: ArrayRef = Arc::new(
        Decimal128Array::from(vec![0_i128, 90071992547409930, 0, 45035996273704965])
            .with_precision_and_scale(20, 1)
            .unwrap(),
    );
    let rows = [1, 3];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let arguments = [EvaluatedArgument::Column(&values)];
    let mut instance = ScalarEvaluationInstance::instantiate(Arc::clone(&prepared)).unwrap();
    let out = instance
        .evaluate(selection, &arguments, &Control::default())
        .unwrap();
    assert_eq!(out.selection().iter().collect::<Vec<_>>(), rows);
    assert!(out.errors().is_empty());
    let actual = out
        .values()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(
        actual
            .values()
            .iter()
            .map(|x| x.to_bits())
            .collect::<Vec<_>>(),
        [0x4340000000000000_u64, 0x4330000000000000_u64]
    );
    let compact: ArrayRef = Arc::new(
        Decimal128Array::from(vec![90071992547409930_i128, 45035996273704965])
            .with_precision_and_scale(20, 1)
            .unwrap(),
    );
    let selected = crate::SelectedValues::try_new(
        selection,
        &DataType::Decimal128(20, 1),
        compact,
        Box::new([]),
    )
    .unwrap();
    let arguments = [EvaluatedArgument::SelectedColumn(&selected)];
    let mut instance = ScalarEvaluationInstance::instantiate(prepared).unwrap();
    let from_selected = instance
        .evaluate(selection, &arguments, &Control::default())
        .unwrap();
    assert!(from_selected.errors().is_empty());
    assert_eq!(
        from_selected
            .values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values()
            .iter()
            .map(|v| v.to_bits())
            .collect::<Vec<_>>(),
        [0x4340000000000000_u64, 0x4330000000000000_u64]
    );
}
#[test]
fn common_cv_keeps_the_original_nonzero_pool_ordinal() {
    use crate::{ConstantPolicy, ConstantPool};
    let source = FunctionValueType::new(DataType::Decimal128(20, 1), false);
    let values = Decimal128Array::from(vec![7_i128, 90071992547409930, 9])
        .with_precision_and_scale(20, 1)
        .unwrap();
    let pool = ConstantPool::try_new(
        Arc::new(source.try_to_field("original-common-cv").unwrap()),
        source.clone(),
        values.to_data(),
        ConstantPolicy {
            max_rows: 16,
            max_array_nodes: 64,
            max_logical_elements: 4096,
            max_retained_buffer_bytes: 65536,
            max_type_depth: 8,
            max_type_nodes: 64,
            max_dictionary_depth: 4,
            max_metadata_bytes: 4096,
            max_library_validation_work: 65536,
            max_library_validation_bytes: 131072,
        },
        CompilePhase::Validate,
        crate::binding_test_control(),
    )
    .unwrap();
    let value = pool.value(1).unwrap();
    let original = Arc::clone(value.pool().array());
    let prepared = prepare(
        source,
        &FunctionValueType::new(DataType::Float64, false),
        DecimalOverflowPolicy::ReportError,
        &CompileControl::default(),
    )
    .unwrap();
    let args = [EvaluatedArgument::Constant(&value)];
    let rows = [1, 3];
    let mut instance = ScalarEvaluationInstance::instantiate(prepared).unwrap();
    let out = instance
        .evaluate(
            Selection::try_sparse(4, &rows).unwrap(),
            &args,
            &Control::default(),
        )
        .unwrap();
    assert_eq!(value.ordinal(), 1);
    assert!(Arc::ptr_eq(&original, value.pool().array()));
    assert_eq!(
        out.values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values()
            .iter()
            .map(|v| v.to_bits())
            .collect::<Vec<_>>(),
        [0x4340000000000000_u64; 2]
    );
}
#[test]
fn common_evaluation_refusals_keep_outer_identity_without_a_later_footer() {
    let prepared = prepare(
        FunctionValueType::new(DataType::Decimal128(20, 1), false),
        &FunctionValueType::new(DataType::Float64, false),
        DecimalOverflowPolicy::ReportError,
        &CompileControl::default(),
    )
    .unwrap();
    let value: ArrayRef = Arc::new(
        Decimal128Array::from(vec![90071992547409930_i128; 300])
            .with_precision_and_scale(20, 1)
            .unwrap(),
    );
    let args = [EvaluatedArgument::Column(&value)];
    let observed = Control::default();
    ScalarEvaluationInstance::instantiate(Arc::clone(&prepared))
        .unwrap()
        .evaluate(Selection::all(300), &args, &observed)
        .unwrap();
    let count = observed.calls.lock().unwrap().len();
    for cause in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(crate::KernelDiagnostic::new("original shape refusal")),
        KernelFailure::Internal(crate::KernelDiagnostic::new("original internal refusal")),
    ] {
        for at in 0..count {
            let control = Control {
                calls: Mutex::new(vec![]),
                refusal: Some((at, cause.clone())),
            };
            let mut instance =
                ScalarEvaluationInstance::instantiate(Arc::clone(&prepared)).unwrap();
            assert_eq!(
                instance
                    .evaluate(Selection::all(300), &args, &control)
                    .unwrap_err(),
                cause
            );
            assert_eq!(control.calls.lock().unwrap().len(), at + 1);
        }
    }
}
#[test]
fn common_exact_selection_and_preparation_observe_original_compile_refusals() {
    let run = |control: &CompileControl| {
        prepare(
            FunctionValueType::new(DataType::Decimal128(20, 1), false),
            &FunctionValueType::new(DataType::Float64, false),
            DecimalOverflowPolicy::ReportError,
            control,
        )
    };
    let observed = CompileControl::default();
    run(&observed).unwrap();
    let count = observed.calls.lock().unwrap().len();
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in 0..count {
            let control = CompileControl {
                calls: Mutex::new(vec![]),
                refusal: Some((at, cause.clone())),
            };
            let error = run(&control).unwrap_err();
            assert!(
                matches!(error, FunctionSpecializationFailure::Control(ref actual) if actual == &cause)
            );
            assert_eq!(control.calls.lock().unwrap().len(), at + 1);
        }
    }
}

fn common_list_type(leaf: DataType, leaf_nullable: bool, root_nullable: bool) -> FunctionValueType {
    use arrow_schema::Field;
    FunctionValueType::new(
        DataType::List(Arc::new(Field::new(
            "item",
            DataType::List(Arc::new(Field::new("item", leaf, leaf_nullable))),
            true,
        ))),
        root_nullable,
    )
}

#[test]
fn common_recursive_lists_keep_required_roots_parent_null_empty_and_null_child() {
    use arrow_array::ListArray;
    use arrow_buffer::{NullBuffer, OffsetBuffer};
    for nullable_root in [false, true] {
        let from = common_list_type(DataType::Decimal128(20, 1), true, nullable_root);
        let to = common_list_type(DataType::Float64, true, nullable_root);
        let coefficients: ArrayRef = Arc::new(
            Decimal128Array::from(vec![
                Some(90071992547409930_i128),
                None,
                Some(45035996273704965),
            ])
            .with_precision_and_scale(20, 1)
            .unwrap(),
        );
        let DataType::List(inner_field) = from.data_type() else {
            unreachable!()
        };
        let DataType::List(leaf_field) = inner_field.data_type() else {
            unreachable!()
        };
        let inner: ArrayRef = Arc::new(
            ListArray::try_new(
                Arc::clone(leaf_field),
                OffsetBuffer::new(vec![0_i32, 2, 2, 2, 3].into()),
                coefficients,
                Some(NullBuffer::from(vec![true, true, false, true])),
            )
            .unwrap(),
        );
        let source: ArrayRef = Arc::new(
            ListArray::try_new(
                Arc::clone(inner_field),
                OffsetBuffer::new(vec![0_i32, 2, 3, 4, 4].into()),
                inner,
                nullable_root.then(|| NullBuffer::from(vec![true, false, true, true])),
            )
            .unwrap(),
        );
        let original = Arc::clone(&source);
        let prepared = prepare(
            from.clone(),
            &to,
            DecimalOverflowPolicy::OutputNull,
            &CompileControl::default(),
        )
        .unwrap();
        let mut instance = ScalarEvaluationInstance::instantiate(prepared).unwrap();
        let evaluated_arguments = [EvaluatedArgument::Column(&source)];
        let output = instance
            .evaluate(Selection::all(4), &evaluated_arguments, &Control::default())
            .unwrap();
        assert!(Arc::ptr_eq(&source, &original));
        assert_eq!(source.data_type(), from.data_type());
        assert_eq!(output.values().data_type(), to.data_type());
        assert_eq!(output.selection().iter().collect::<Vec<_>>(), [0, 1, 2, 3]);
        assert!(output.errors().is_empty());
        let outer = output
            .values()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(outer.is_null(1), nullable_root);
        assert_eq!(outer.value_length(3), 0);
        let first = outer.value(0);
        let first = first.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(first.value_length(1), 0);
        let values = first.value(0);
        let values = values.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(values.value(0).to_bits(), 0x4340000000000000_u64);
        assert!(values.is_null(1));
        if !nullable_root {
            let row = outer.value(1);
            assert!(row.as_any().downcast_ref::<ListArray>().unwrap().is_null(0));
        }
        let last = outer.value(2);
        let values = last.as_any().downcast_ref::<ListArray>().unwrap().value(0);
        assert_eq!(
            values
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0)
                .to_bits(),
            0x4330000000000000_u64
        );
    }
}

#[test]
fn common_nested_timestamp_precision_widening_keeps_values_and_nulls() {
    use arrow_array::{ListArray, TimestampMicrosecondArray};
    use arrow_buffer::{NullBuffer, OffsetBuffer};
    use novarocks_type_contract::{
        TemporalPrecision, TemporalType, TemporalValue, ValueKind, ValueLayout, ValueTypeRef,
        ValueTypeStorageLifetime,
    };
    use novarocks_value_arrays::decode::PreparedTemporalArray;

    let timestamp_array = |declaration: TemporalType| {
        let storage = ValueTypeStorageLifetime::pure();
        let item = FunctionValueType::try_from_value(
            ValueTypeRef::try_new(ValueKind::Temporal(declaration), true, storage.clone()).unwrap(),
            ValueLayout::try_new(&declaration.carrier(), storage).unwrap(),
        )
        .unwrap();
        let inner = crate::builtin::signature::declared_list(&item, true).unwrap();
        crate::builtin::signature::declared_list(&inner, false).unwrap()
    };
    for (source_precision, target_precision, micros) in [
        (
            TemporalPrecision::DEFAULT,
            TemporalPrecision::MICROSECONDS,
            1_000,
        ),
        (
            TemporalPrecision::MICROSECONDS,
            TemporalPrecision::new(7).unwrap(),
            1_001,
        ),
    ] {
        let source_decl = TemporalType::Timestamp(source_precision);
        let target_decl = TemporalType::Timestamp(target_precision);
        let source_type = timestamp_array(source_decl);
        let target_type = timestamp_array(target_decl);
        let DataType::List(inner_field) = source_type.data_type() else {
            unreachable!()
        };
        let DataType::List(item_field) = inner_field.data_type() else {
            unreachable!()
        };
        let inner: ArrayRef = Arc::new(
            ListArray::try_new(
                Arc::clone(item_field),
                OffsetBuffer::new(vec![0_i32, 2, 2, 3, 3].into()),
                Arc::new(TimestampMicrosecondArray::from(vec![
                    Some(-micros),
                    None,
                    Some(micros),
                ])),
                Some(NullBuffer::from(vec![true, true, true, false])),
            )
            .unwrap(),
        );
        let source: ArrayRef = Arc::new(
            ListArray::try_new(
                Arc::clone(inner_field),
                OffsetBuffer::new(vec![0_i32, 3, 4].into()),
                inner,
                None,
            )
            .unwrap(),
        );
        let prepared = prepare(
            source_type,
            &target_type,
            DecimalOverflowPolicy::ReportError,
            &CompileControl::default(),
        )
        .unwrap();
        let mut instance = ScalarEvaluationInstance::instantiate(prepared).unwrap();
        let evaluated_arguments = [EvaluatedArgument::Column(&source)];
        let output = instance
            .evaluate(Selection::all(2), &evaluated_arguments, &Control::default())
            .unwrap();
        assert!(output.errors().is_empty());
        assert_eq!(output.values().data_type(), target_type.data_type());
        let outer = output
            .values()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(outer.value_offsets(), &[0, 3, 4]);
        let inner = outer.values().as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(inner.value_offsets(), &[0, 2, 2, 3, 3]);
        assert!(inner.is_null(3));
        let timestamps =
            PreparedTemporalArray::bind(target_decl, inner.values().as_ref(), true).unwrap();
        let expected = [
            Some(TemporalValue::from_i64_carrier(source_decl, -micros).unwrap()),
            None,
            Some(TemporalValue::from_i64_carrier(source_decl, micros).unwrap()),
        ];
        for (row, expected) in expected.into_iter().enumerate() {
            assert_eq!(timestamps.value(row).unwrap(), expected);
        }
    }
}

#[test]
fn common_recursive_cap38_data_is_owned_and_null_parent_hides_unreachable_child() {
    use arrow_array::ListArray;
    use arrow_buffer::{NullBuffer, OffsetBuffer};
    let from = common_list_type(DataType::Decimal128(38, 0), false, true);
    let to = common_list_type(DataType::Decimal128(38, 1), false, true);
    let DataType::List(inner_field) = from.data_type() else {
        unreachable!()
    };
    let DataType::List(leaf_field) = inner_field.data_type() else {
        unreachable!()
    };
    let coefficients: ArrayRef = Arc::new(
        Decimal128Array::from(vec![1_i128, 10_i128.pow(37), 10_i128.pow(37)])
            .with_precision_and_scale(38, 0)
            .unwrap(),
    );
    let inner: ArrayRef = Arc::new(
        ListArray::try_new(
            Arc::clone(leaf_field),
            OffsetBuffer::new(vec![0_i32, 1, 2, 3].into()),
            coefficients,
            None,
        )
        .unwrap(),
    );
    let source: ArrayRef = Arc::new(
        ListArray::try_new(
            Arc::clone(inner_field),
            OffsetBuffer::new(vec![0_i32, 1, 2, 3].into()),
            inner,
            Some(NullBuffer::from(vec![true, false, true])),
        )
        .unwrap(),
    );
    let prepared = prepare(
        from,
        &to,
        DecimalOverflowPolicy::OutputNull,
        &CompileControl::default(),
    )
    .unwrap();
    let mut instance = ScalarEvaluationInstance::instantiate(prepared).unwrap();
    let evaluated_arguments = [EvaluatedArgument::Column(&source)];
    let output = instance
        .evaluate(Selection::all(3), &evaluated_arguments, &Control::default())
        .unwrap();
    assert_eq!(output.errors().len(), 1);
    assert_eq!(output.errors()[0].selected_ordinal(), 2);
    assert_eq!(
        output.errors()[0].reason(),
        crate::DataFailureReason::Domain(
            novarocks_function_contract::kernel_failure::NUMERIC_OUT_OF_RANGE
        )
    );
    let outer = output
        .values()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert!(outer.is_null(1));
    assert!(outer.is_null(2));
    let first = outer.value(0);
    let values = first.as_any().downcast_ref::<ListArray>().unwrap().value(0);
    assert_eq!(
        values
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .value(0),
        10
    );
    assert_eq!(output.values().data_type(), to.data_type());
}

#[test]
fn common_unknown_lift_and_list_unknown_identity_do_not_grant_scalar_unknown_identity() {
    use arrow_array::NullArray;
    let unknown = FunctionValueType::new(DataType::Null, true);
    let target = FunctionValueType::new(DataType::Float64, true);
    let source: ArrayRef = Arc::new(NullArray::new(3));
    let prepared = prepare(
        unknown.clone(),
        &target,
        DecimalOverflowPolicy::ReportError,
        &CompileControl::default(),
    )
    .unwrap();
    let mut instance = ScalarEvaluationInstance::instantiate(prepared).unwrap();
    let evaluated_arguments = [EvaluatedArgument::Column(&source)];
    let output = instance
        .evaluate(Selection::all(3), &evaluated_arguments, &Control::default())
        .unwrap();
    assert_eq!(source.logical_null_count(), 3);
    assert_eq!(output.values().data_type(), &DataType::Float64);
    assert_eq!(output.values().logical_null_count(), 3);
    assert!(output.errors().is_empty());
    assert!(matches!(
        crate::PreparedCastRecipe::prepare_array_common_coercion(
            &unknown,
            &unknown,
            &CompileControl::default()
        ),
        Err(crate::CastPrepareError::Unsupported)
    ));
    assert_eq!(
        novarocks_failure_contract::FailureProjection::failure_fact(
            &prepare(
                unknown.clone(),
                &unknown,
                DecimalOverflowPolicy::ReportError,
                &CompileControl::default()
            )
            .unwrap_err()
        )
        .class(),
        novarocks_failure_contract::FailureClass::Unsupported
    );
    let required = FunctionValueType::new(DataType::Float64, false);
    assert!(matches!(
        prepare(
            unknown,
            &required,
            DecimalOverflowPolicy::ReportError,
            &CompileControl::default()
        ),
        Err(FunctionSpecializationFailure::Binding(
            FunctionBindingError::InvalidBinding(_)
        ))
    ));
    let list = FunctionValueType::new(
        DataType::List(Arc::new(arrow_schema::Field::new(
            "item",
            DataType::Null,
            true,
        ))),
        false,
    );
    let widened = FunctionValueType::new(list.data_type().clone(), true);
    let identity = crate::PreparedCastRecipe::prepare_array_common_coercion(
        &list,
        &widened,
        &CompileControl::default(),
    )
    .unwrap();
    assert!(identity.is_identity());
    assert_eq!(identity.retained_heap_bytes_observed(|| Ok(())).unwrap(), 0);
    // The real ARRAY constructor's all-UNKNOWN body must stay with its original
    // owner; this test does not manufacture a scalar NULL-only recipe.
}

fn common_materialized_layout(ty: &FunctionValueType) -> FunctionValueType {
    use novarocks_type_contract::{ExecutionValueTypeError, ValueLayout, ValueTypeStorageLifetime};
    fn build(data: &DataType) -> ValueLayout {
        let storage = ValueTypeStorageLifetime::pure();
        if let DataType::List(field) = data {
            ValueLayout::try_from_normalized_arrow_with_children_observed::<ExecutionValueTypeError>(data.clone(), vec![build(field.data_type())].into_boxed_slice(), storage.clone(), storage, |_| Ok(())).unwrap()
        } else {
            ValueLayout::try_from_normalized_arrow(data.clone(), storage).unwrap()
        }
    }
    FunctionValueType::try_from_value(ty.value_type().clone(), build(ty.data_type())).unwrap()
}

#[test]
fn common_retained_projection_requests_match_actual_creator_in_both_cache_shapes() {
    use novarocks_type_contract::{ValueConstructionRequest, ValueTypeStorageLifetime};
    use std::{alloc::Layout, convert::Infallible};
    // Raw provider annotations are not part of canonical FVT. Keep this
    // source declaration independent instead of asserting raw tags on FVT.
    let raw_leaf = Arc::new(
        arrow_schema::Field::new("provider-name", DataType::Decimal128(20, 1), true).with_metadata(
            std::collections::HashMap::from([("provider".into(), "original".into())]),
        ),
    );
    let raw = DataType::List(Arc::new(arrow_schema::Field::new(
        "outer-provider",
        DataType::List(Arc::clone(&raw_leaf)),
        true,
    )));
    let canonical = FunctionValueType::new(raw, false);
    let result = common_list_type(DataType::Float64, true, false);
    assert_eq!(raw_leaf.name(), "provider-name");
    assert_eq!(
        raw_leaf.metadata().get("provider").map(String::as_str),
        Some("original")
    );
    for materialized in [false, true] {
        let source = if materialized {
            common_materialized_layout(&canonical)
        } else {
            canonical.clone()
        };
        let target = if materialized {
            common_materialized_layout(&result)
        } else {
            result.clone()
        };
        // Observe the real ValueLayout creator, not the method under test.
        // Keep created holders live, as the real prepared recipe does.
        let mut holders = Vec::new();
        let mut requests = Vec::new();
        for value in [&source, &target] {
            let mut layout = value.typed_binding().layout().clone();
            while matches!(layout.data_type(), DataType::List(_)) {
                let next = layout
                    .child_projection_at_with_construction::<Infallible>(
                        0,
                        ValueTypeStorageLifetime::pure(),
                        Some(&mut |request| {
                            if let ValueConstructionRequest::Exact(layout) = request {
                                requests.push(layout);
                            }
                            Ok(())
                        }),
                    )
                    .unwrap()
                    .unwrap();
                holders.push(next.clone());
                layout = next;
            }
        }
        assert_eq!(requests.len(), if materialized { 0 } else { 4 });
        let creator_bytes: usize = requests.iter().map(Layout::size).sum();
        // This fixed fixture has exactly two List edges and therefore two
        // owned child recipe Boxes; the root recipe is embedded in its owner.
        let expected = creator_bytes + 2 * Layout::new::<crate::PreparedCastRecipe>().size();
        let recipe = crate::PreparedCastRecipe::prepare_array_common_coercion(
            &source,
            &target,
            &CompileControl::default(),
        )
        .unwrap();
        assert_eq!(
            recipe.retained_heap_bytes_observed(|| Ok(())).unwrap(),
            expected
        );
        assert_eq!(source, canonical);
        assert_eq!(target, result);
        assert_eq!(holders.len(), 4);
    }
}

// The Execution Frame resolves child journals before the ordinary kernel
// boundary; this entry must refuse an unresolved source journal.
#[test]
fn common_ordinary_kernel_refuses_unresolved_source_data_without_reclassifying_input() {
    use crate::{RowDataError, SelectedValues};
    use std::error::Error;
    let from = FunctionValueType::new(DataType::Decimal128(20, 1), true);
    let to = FunctionValueType::new(DataType::Float64, true);
    let rows = [1_usize, 4, 7];
    let selection = Selection::try_sparse(10, &rows).unwrap();
    // Use an existing domain's genuine upstream Data cause, as if a map
    // lookup of Decimal values failed. No adapter invents a cast failure.
    let source_error = RowDataError::from_domain(
        1,
        crate::builtin::map_lookup_core::LookupRowFailure::MissingKey,
    )
    .unwrap();
    let expected_error = source_error.clone();
    let carrier: ArrayRef = Arc::new(
        Decimal128Array::from(vec![Some(170_i128), None, Some(-30)])
            .with_precision_and_scale(20, 1)
            .unwrap(),
    );
    let original_carrier = Arc::clone(&carrier);
    let source = SelectedValues::try_new(
        selection,
        from.data_type(),
        carrier,
        Box::from([source_error]),
    )
    .unwrap();
    let prepared = prepare(
        from,
        &to,
        DecimalOverflowPolicy::OutputNull,
        &CompileControl::default(),
    )
    .unwrap();
    let mut instance = ScalarEvaluationInstance::instantiate(prepared).unwrap();
    let evaluated_arguments = [EvaluatedArgument::SelectedColumn(&source)];
    let error = instance
        .evaluate(selection, &evaluated_arguments, &Control::default())
        .unwrap_err();
    assert_eq!(
        novarocks_failure_contract::FailureProjection::failure_fact(&error),
        novarocks_failure_contract::FailureProjection::failure_fact(
            &crate::EvaluationContractError::ArgumentRowErrors
        )
    );
    assert_eq!(source.selection().iter().collect::<Vec<_>>(), rows);
    assert!(Arc::ptr_eq(source.values(), &original_carrier));
    assert_eq!(source.errors().len(), 1);
    let actual = &source.errors()[0];
    assert_eq!(actual.selected_ordinal(), 1);
    assert_eq!(actual, &expected_error);
    let original_cause = expected_error.source().unwrap().source().unwrap();
    let retained_cause = actual.source().unwrap().source().unwrap();
    assert!(std::ptr::eq(original_cause, retained_cause));
    let values = source
        .values()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(values.value(0), 170);
    assert!(values.is_null(1));
    assert_eq!(values.value(2), -30);
}

// This actual port assertion is intentionally stronger than is_identity().
#[test]
fn common_list_unknown_root_widen_reaches_the_original_identity_port() {
    use arrow_array::{ListArray, NullArray};
    use arrow_buffer::OffsetBuffer;
    let source_type = FunctionValueType::new(
        DataType::List(Arc::new(arrow_schema::Field::new(
            "item",
            DataType::Null,
            true,
        ))),
        false,
    );
    let target_type = FunctionValueType::new(source_type.data_type().clone(), true);
    let DataType::List(field) = source_type.data_type() else {
        unreachable!()
    };
    let values: ArrayRef = Arc::new(NullArray::new(3));
    let source: ArrayRef = Arc::new(
        ListArray::try_new(
            Arc::clone(field),
            OffsetBuffer::new(vec![0_i32, 2, 3].into()),
            values,
            None,
        )
        .unwrap(),
    );
    let original = Arc::clone(&source);
    let prepared = prepare(
        source_type.clone(),
        &target_type,
        DecimalOverflowPolicy::ReportError,
        &CompileControl::default(),
    )
    .unwrap();
    let mut instance = ScalarEvaluationInstance::instantiate(prepared).unwrap();
    let evaluated_arguments = [EvaluatedArgument::Column(&source)];
    let output = instance
        .evaluate(Selection::all(2), &evaluated_arguments, &Control::default())
        .unwrap();
    assert!(Arc::ptr_eq(&source, &original));
    assert_eq!(source.data_type(), source_type.data_type());
    assert_eq!(output.values().data_type(), target_type.data_type());
    assert!(output.errors().is_empty());
    let list = output
        .values()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(list.null_count(), 0);
    assert_eq!(list.value_length(0), 2);
    assert_eq!(list.value_length(1), 1);
    assert_eq!(list.values().logical_null_count(), 3);
}
