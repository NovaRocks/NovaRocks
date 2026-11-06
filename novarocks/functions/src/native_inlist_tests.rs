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
//! Shared original signed IN/state/control witnesses. Not formal MEM admission.
use super::{InError, InObservation, InRows, signed_equality_observed, signed_equality_required};
use crate::{KernelDiagnostic, KernelEvaluationControl, KernelFailure};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Int8Array, Int16Array, Int32Array, Int64Array, NullArray,
};
use arrow_schema::DataType;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
fn values(array: &BooleanArray) -> Vec<Option<bool>> {
    array.iter().collect()
}
fn source(width: usize) -> ArrayRef {
    match width {
        8 => Arc::new(Int8Array::from(vec![Some(-1), None, Some(7)])),
        16 => Arc::new(Int16Array::from(vec![Some(-1), None, Some(7)])),
        32 => Arc::new(Int32Array::from(vec![Some(-1), None, Some(7)])),
        64 => Arc::new(Int64Array::from(vec![Some(-1), None, Some(7)])),
        _ => unreachable!(),
    }
}
#[test]
fn inlist_core_signed_full_pair_matrix_original_nulls_and_scalar() {
    for left in [8, 16, 32, 64] {
        for right in [8, 16, 32, 64] {
            let left = source(left);
            let column = source(right);
            assert_eq!(
                values(&signed_equality_required(&left, &column).unwrap().unwrap()),
                vec![Some(true), None, Some(true)]
            );
            let scalar = column.slice(0, 1);
            assert_eq!(
                values(&signed_equality_required(&left, &scalar).unwrap().unwrap()),
                vec![Some(true), None, Some(false)]
            );
        }
    }
}
#[test]
fn inlist_core_original_match_dominates_null_and_root_null() {
    let input: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), Some(2), None]));
    let candidate: ArrayRef = Arc::new(Int32Array::from(vec![Some(1); 3]));
    for negated in [false, true] {
        let mut state = InRows::begin_required(&input).unwrap();
        state.candidate_nulls_required(&candidate).unwrap();
        state
            .equalities_required(
                &signed_equality_required(&input, &candidate)
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
        let null: ArrayRef = Arc::new(Int32Array::from(vec![None; 3]));
        state.candidate_nulls_required(&null).unwrap();
        state
            .equalities_required(&signed_equality_required(&input, &null).unwrap().unwrap())
            .unwrap();
        assert_eq!(
            values(&state.finish_required(&input, negated).unwrap()),
            vec![Some(!negated), None, None]
        );
        let bare: ArrayRef = Arc::new(NullArray::new(3));
        assert_eq!(
            values(
                &InRows::begin_required(&bare)
                    .unwrap()
                    .finish_required(&bare, negated)
                    .unwrap()
            ),
            vec![None; 3]
        );
    }
}
#[test]
fn inlist_core_original_length_message_is_full_and_empty_assembly_exact() {
    let input = source(32);
    let mut state = InRows::begin_required(&input).unwrap();
    let empty = input.slice(0, 0);
    assert_eq!(
        state
            .candidate_nulls_required(&empty)
            .unwrap_err()
            .to_string(),
        "IN predicate value length mismatch: input has 3, value has 0"
    );
    assert_eq!(
        InRows::begin_required(&empty)
            .unwrap()
            .finish_required(&empty, false)
            .unwrap()
            .to_data(),
        BooleanArray::from(Vec::<bool>::new()).to_data()
    );
}
struct Control {
    trace: Mutex<Vec<u32>>,
    fail: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        trace.push(units);
        if let Some((at, cause)) = &self.fail
            && trace.len() == *at
        {
            return Err(cause.clone());
        }
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("IN never waits")
    }
}
fn invoke(control: &dyn KernelEvaluationControl) -> Result<BooleanArray, InError<KernelFailure>> {
    let mut units = 0u32;
    let mut observe = |event| {
        match event {
            InObservation::Step => {
                units += 1;
                if units < 256 {
                    return Ok(());
                }
            }
            InObservation::OpaqueBoundary => {}
        }
        let due = units;
        units = 0;
        control.checkpoint(due)
    };
    let input: ArrayRef = Arc::new(Int64Array::from(vec![Some(7); 513]));
    let candidate: ArrayRef = Arc::new(Int8Array::from(vec![Some(7)]));
    let mut state = InRows::begin_observed(&input, &mut observe)?;
    state.candidate_nulls_observed(&candidate, &mut observe)?;
    let eq = signed_equality_observed(&input, &candidate, &mut observe)?.unwrap();
    state.equalities_observed(&eq, &mut observe)?;
    state.finish_observed(&input, false, &mut observe)
}
#[test]
fn inlist_core_every_real_callback_preserves_all_seven_causes_without_footer() {
    let success = Control {
        trace: Mutex::new(Vec::new()),
        fail: None,
    };
    assert!(
        values(&invoke(&success).unwrap())
            .into_iter()
            .all(|v| v == Some(true))
    );
    let trace = success.trace.into_inner().unwrap();
    assert!(trace.contains(&256));
    let causes = [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(KernelDiagnostic::new("original invalid refusal")),
        KernelFailure::Internal(KernelDiagnostic::new("original internal refusal")),
        KernelFailure::Operational(KernelDiagnostic::new("original operational refusal")),
        KernelFailure::InstanceFailed,
    ];
    for at in 1..=trace.len() {
        for cause in &causes {
            let control = Control {
                trace: Mutex::new(Vec::new()),
                fail: Some((at, cause.clone())),
            };
            assert!(matches!(invoke(&control),Err(InError::Host(found)) if found==*cause));
            assert_eq!(
                *control.trace.lock().unwrap(),
                trace[..at],
                "no callback after originating refusal"
            );
        }
    }
}

#[test]
fn inlist_recipe_exact_signed_widths_nullable_result_and_foreign_carriers() {
    use crate::{ComparisonPrepareError, PreparedNativeInListRecipe};
    use arrow_schema::DataType;
    use novarocks_type_contract::{
        CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
    };
    struct Compile;
    impl PureCompileControl for Compile {
        fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            assert!(units <= 256);
            Ok(())
        }
    }
    for data_type in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Utf8,
    ] {
        let input = FunctionValueType::from_legacy_declared_projection(data_type.clone(), false);
        let nullable = FunctionValueType::from_legacy_declared_projection(data_type, true);
        let result = FunctionValueType::new(DataType::Boolean, true);
        for negated in [false, true] {
            let recipe = PreparedNativeInListRecipe::try_new(
                negated,
                &input,
                &[&input, &nullable],
                &result,
                &Compile,
            )
            .unwrap();
            assert_eq!(recipe.source_type(), &input);
            assert_eq!(recipe.candidate_types(), &[input.clone(), nullable.clone()]);
            assert_eq!(recipe.result_type(), &result);
            assert_eq!(recipe.negated(), negated);
            assert_eq!(
                PreparedNativeInListRecipe::try_new(
                    negated,
                    &input,
                    &[&nullable],
                    &FunctionValueType::new(DataType::Boolean, false),
                    &Compile
                )
                .unwrap_err(),
                ComparisonPrepareError::TypeMismatch
            );
        }
    }
    for ty in [DataType::UInt64, DataType::Null, DataType::Decimal128(4, 0)] {
        let input = FunctionValueType::from_legacy_declared_projection(ty, true);
        assert_eq!(
            PreparedNativeInListRecipe::try_new(
                false,
                &input,
                &[],
                &FunctionValueType::new(DataType::Boolean, true),
                &Compile
            )
            .unwrap_err(),
            ComparisonPrepareError::Unsupported
        );
    }
}
#[test]
fn inlist_shared_selected_parent_mapping_truthonly_and_error_placeholder_are_distinct() {
    let input: ArrayRef = Arc::new(Int32Array::from(vec![Some(7), None, Some(9), Some(11)]));
    let candidate: ArrayRef = Arc::new(Int32Array::from(vec![None, Some(9)]));
    for truth_only in [false, true] {
        let mut state = InRows::begin_required(&input).unwrap();
        state
            .candidate_nulls_selected_observed(&candidate, &[0, 2], &mut |_| Ok::<_, ()>(()))
            .unwrap();
        state
            .equalities_selected_observed(
                &BooleanArray::from(vec![None, Some(true)]),
                &[0, 2],
                &mut |_| Ok::<_, ()>(()),
            )
            .unwrap();
        let out = state
            .finish_selected_observed(&input, false, truth_only, &[3], &mut |_| Ok::<_, ()>(()))
            .unwrap();
        assert_eq!(
            values(&out),
            if truth_only {
                vec![Some(false), Some(false), Some(true), None]
            } else {
                vec![None, None, Some(true), None]
            }
        );
    }
}

#[test]
fn inlist_recipe_actual_compile_callbacks_preserve_three_causes_and_no_footer() {
    use crate::{ComparisonPrepareError, PreparedNativeInListRecipe};
    use novarocks_type_contract::{
        CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
    };
    struct Compile {
        trace: Mutex<Vec<(CompilePhase, u32)>>,
        stop: usize,
        cause: CompileControlError,
    }
    impl PureCompileControl for Compile {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            assert!(units <= 256);
            let mut trace = self.trace.lock().unwrap();
            assert!(trace.len() < self.stop, "no compile footer after refusal");
            trace.push((phase, units));
            if trace.len() == self.stop {
                Err(self.cause.clone())
            } else {
                Ok(())
            }
        }
    }
    let source =
        FunctionValueType::from_legacy_declared_projection(arrow_schema::DataType::Int64, true);
    let result =
        FunctionValueType::from_legacy_declared_projection(arrow_schema::DataType::Boolean, true);
    let success = Compile {
        trace: Mutex::new(vec![]),
        stop: usize::MAX,
        cause: CompileControlError::Cancelled,
    };
    PreparedNativeInListRecipe::try_new(false, &source, &[&source, &source], &result, &success)
        .unwrap();
    let trace = success.trace.into_inner().unwrap();
    for stop in 1..=trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Compile {
                trace: Mutex::new(vec![]),
                stop,
                cause: cause.clone(),
            };
            let error = PreparedNativeInListRecipe::try_new(
                false,
                &source,
                &[&source, &source],
                &result,
                &control,
            )
            .unwrap_err();
            assert_eq!(error.control_error(), Some(cause));
            assert!(matches!(
                error,
                ComparisonPrepareError::Control(_) | ComparisonPrepareError::Kernel(_)
            ));
            assert_eq!(*control.trace.lock().unwrap(), trace[..stop]);
        }
    }
}

#[test]
fn inlist_recipe_created_candidate_owner_preserves_first_host_and_last_reference() {
    use crate::{NativeInListCreationFailure, PreparedNativeInListRecipe};
    use novarocks_type_contract::{
        CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
        ValueSchemaCreation, ValueTypeStorageHold, ValueTypeStorageLifetime,
    };
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };
    struct Compile(Mutex<Vec<(CompilePhase, u32)>>);
    impl PureCompileControl for Compile {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            self.0.lock().unwrap().push((phase, units));
            Ok(())
        }
    }
    struct Hold;
    impl ValueTypeStorageHold for Hold {}
    let input = FunctionValueType::new(DataType::Int64, false);
    let nullable = FunctionValueType::new(DataType::Int64, true);
    let output = FunctionValueType::new(DataType::Boolean, true);
    let run = |fail_at: Option<usize>| {
        let control = Compile(Mutex::new(Vec::new()));
        let hold = Arc::new(Hold);
        let weak = Arc::downgrade(&hold);
        let next = AtomicUsize::new(0);
        let refused = AtomicUsize::new(usize::MAX);
        let cause = Arc::new(());
        let mut request = |_: std::alloc::Layout| {
            let at = next.fetch_add(1, Ordering::Relaxed);
            if Some(at) == fail_at {
                refused.store(control.0.lock().unwrap().len(), Ordering::Relaxed);
                Err(cause.clone())
            } else {
                Ok(())
            }
        };
        let mut opaque = |_: usize| panic!("this recipe has only actual public Vec/Arc requests");
        let mut creation = ValueSchemaCreation::new(
            ValueTypeStorageLifetime::held(hold.clone()),
            &mut request,
            &mut opaque,
        );
        let result = PreparedNativeInListRecipe::try_new_with_creator_in(
            false,
            &input,
            &[&input, &nullable],
            &output,
            &mut creation,
            &control,
        );
        if let Some(at) = fail_at {
            assert!(
                matches!(result, Err(NativeInListCreationFailure::Creator(actual))
                if Arc::ptr_eq(&actual, &cause))
            );
            assert_eq!(next.load(Ordering::Relaxed), at + 1);
            assert_eq!(
                control.0.lock().unwrap().len(),
                refused.load(Ordering::Relaxed)
            );
            drop(creation);
            drop(hold);
            assert!(weak.upgrade().is_none());
        } else {
            let recipe = result.unwrap();
            assert_eq!(recipe.candidate_types(), &[input.clone(), nullable.clone()]);
            let late = recipe.clone();
            drop(recipe);
            drop(creation);
            drop(hold);
            assert!(weak.upgrade().is_some());
            drop(late);
            assert!(weak.upgrade().is_none());
        }
        next.load(Ordering::Relaxed)
    };
    let total = run(None);
    assert!(
        total >= 2,
        "actual collection and its immutable holder are distinct requests"
    );
    for at in 0..total {
        run(Some(at));
    }
}

#[test]
fn inlist_recipe_created_callbacks_keep_original_control_and_no_request_tail() {
    use crate::{NativeInListCreationFailure, PreparedNativeInListRecipe};
    use novarocks_type_contract::{
        CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
        ValueSchemaCreation, ValueTypeStorageLifetime,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Compile<'a> {
        next: AtomicUsize,
        fail: Option<(usize, CompileControlError)>,
        requests: &'a AtomicUsize,
        refused_requests: AtomicUsize,
    }
    impl PureCompileControl for Compile<'_> {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            let at = self.next.fetch_add(1, Ordering::Relaxed);
            if let Some((boundary, error)) = self.fail.clone()
                && at == boundary
            {
                self.refused_requests
                    .store(self.requests.load(Ordering::Relaxed), Ordering::Relaxed);
                Err(error)
            } else {
                Ok(())
            }
        }
    }
    let input = FunctionValueType::new(DataType::Int64, false);
    let output = FunctionValueType::new(DataType::Boolean, false);
    let run = |fail: Option<(usize, CompileControlError)>| {
        let requests = AtomicUsize::new(0);
        let control = Compile {
            next: AtomicUsize::new(0),
            fail: fail.clone(),
            requests: &requests,
            refused_requests: AtomicUsize::new(usize::MAX),
        };
        let mut request = |_: std::alloc::Layout| {
            requests.fetch_add(1, Ordering::Relaxed);
            Ok::<_, ()>(())
        };
        let mut opaque = |_: usize| panic!("no opaque recipe request");
        let mut creation =
            ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut request, &mut opaque);
        let result = PreparedNativeInListRecipe::try_new_with_creator_in(
            false,
            &input,
            &[&input],
            &output,
            &mut creation,
            &control,
        );
        if let Some((at, cause)) = fail {
            assert!(
                matches!(result, Err(NativeInListCreationFailure::Control(actual)) if actual == cause)
            );
            assert_eq!(control.next.load(Ordering::Relaxed), at + 1);
            assert_eq!(
                requests.load(Ordering::Relaxed),
                control.refused_requests.load(Ordering::Relaxed)
            );
        } else {
            assert!(result.is_ok());
        }
        control.next.load(Ordering::Relaxed)
    };
    let total = run(None);
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in 0..total {
            run(Some((at, cause.clone())));
        }
    }
}

#[test]
fn inlist_borrowed_refinement_matches_original_profile_effects_and_control_prefix() {
    use crate::PreparedNativeInListRecipe;
    use novarocks_type_contract::{
        CompileControlError, CompilePhase, EvaluationDemand, EvaluationDomainId,
        ExpressionEffectContext, ExpressionUseId, FunctionValueType, PureCompileControl,
    };
    struct Compile {
        trace: Mutex<Vec<(CompilePhase, u32)>>,
        refusal: Option<(usize, CompileControlError)>,
    }
    impl PureCompileControl for Compile {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            let mut trace = self.trace.lock().unwrap();
            let at = trace.len();
            if let Some((stop, _)) = self.refusal {
                assert!(at <= stop, "callback after original IN refusal");
            }
            trace.push((phase, units));
            match self.refusal.clone() {
                Some((stop, cause)) if at == stop => Err(cause),
                _ => Ok(()),
            }
        }
    }
    let context = ExpressionEffectContext {
        use_id: ExpressionUseId::new(601),
        domain: EvaluationDomainId::new(41),
        demand: EvaluationDemand::Value,
    };
    let source = FunctionValueType::new(DataType::Int64, true);
    let target = FunctionValueType::new(DataType::Boolean, true);
    let candidates = vec![&source; 320];
    let original = Compile {
        trace: Mutex::new(Vec::new()),
        refusal: None,
    };
    let prepared =
        PreparedNativeInListRecipe::try_new(false, &source, &candidates, &target, &original)
            .unwrap();
    let borrowed = Compile {
        trace: Mutex::new(Vec::new()),
        refusal: None,
    };
    let effects = PreparedNativeInListRecipe::refine_borrowed_profile(
        &source,
        candidates.iter().copied(),
        &target,
        context,
        &borrowed,
    )
    .unwrap();
    assert_eq!(effects, prepared.own_effects(context));
    let trace = original.trace.into_inner().unwrap();
    assert_eq!(*borrowed.trace.lock().unwrap(), trace);
    assert!(trace.iter().all(|(_, units)| *units <= 256));
    assert!(
        trace
            .iter()
            .map(|(_, units)| u64::from(*units))
            .sum::<u64>()
            >= 320
    );
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for stop in 0..trace.len() {
            let control = Compile {
                trace: Mutex::new(Vec::new()),
                refusal: Some((stop, cause.clone())),
            };
            let error = PreparedNativeInListRecipe::refine_borrowed_profile(
                &source,
                candidates.iter().copied(),
                &target,
                context,
                &control,
            )
            .unwrap_err();
            assert_eq!(error.control_error(), Some(cause.clone()));
            assert_eq!(*control.trace.lock().unwrap(), trace[..=stop]);
        }
    }
    let required_target = FunctionValueType::new(DataType::Boolean, false);
    let foreign_candidate = FunctionValueType::new(DataType::Int32, true);
    let unsupported_source = FunctionValueType::new(DataType::Float64, true);
    for (input, candidate, output) in [
        (&source, &source, &required_target),
        (&source, &foreign_candidate, &target),
        (&unsupported_source, &unsupported_source, &target),
    ] {
        let original = Compile {
            trace: Mutex::new(Vec::new()),
            refusal: None,
        };
        let borrowed = Compile {
            trace: Mutex::new(Vec::new()),
            refusal: None,
        };
        let error =
            PreparedNativeInListRecipe::try_new(false, input, &[candidate], output, &original)
                .unwrap_err();
        let borrowed_error = PreparedNativeInListRecipe::refine_borrowed_profile(
            input,
            std::iter::once(candidate),
            output,
            context,
            &borrowed,
        )
        .unwrap_err();
        assert_eq!(error, borrowed_error);
        assert_eq!(
            *original.trace.lock().unwrap(),
            *borrowed.trace.lock().unwrap()
        );
    }
}

#[test]
fn inlist_operation_guards_are_unmaskable_and_do_not_mutate_the_match_state() {
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    let input = source(32);
    let mut state = InRows::begin_required(&input).unwrap();
    let failure = state
        .equalities_required(&BooleanArray::from(vec![true]))
        .unwrap_err();
    assert_eq!(failure.failure_fact().class(), FailureClass::Invariant);
    assert_eq!(failure.failure_fact().reason().code(), 381);
    assert_eq!(
        values(&state.finish_required(&input, false).unwrap()),
        vec![Some(false), None, Some(false)]
    );
    let mut state = InRows::begin_required(&input).unwrap();
    let candidate = input.slice(0, 1);
    let failure = state
        .candidate_nulls_selected_observed(&candidate, &[3], &mut |_| Ok::<_, ()>(()))
        .unwrap_err();
    assert!(
        matches!(failure, InError::Operation(ref cause) if cause.failure_fact().class() == FailureClass::Invariant)
    );
}
#[test]
fn inlist_actual_arrow_equality_error_retains_sdk_source_without_its_inputs() {
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    use std::error::Error;
    let left: ArrayRef = Arc::new(Int32Array::from(vec![1]));
    let right: ArrayRef = Arc::new(arrow_array::StringArray::from(vec!["one"]));
    let weak = Arc::downgrade(&right);
    let source = arrow_ord::cmp::eq(
        &left.as_ref() as &dyn arrow_array::Datum,
        &right.as_ref() as &dyn arrow_array::Datum,
    )
    .unwrap_err();
    let original = source.to_string();
    let failure = super::arrow_failure(source);
    drop(left);
    drop(right);
    assert!(weak.upgrade().is_none());
    assert_eq!(failure.failure_fact().class(), FailureClass::Invariant);
    let source = failure
        .source()
        .unwrap()
        .source()
        .unwrap()
        .source()
        .unwrap();
    assert!(source.downcast_ref::<arrow_schema::ArrowError>().is_some());
    assert_eq!(source.to_string(), original);
}

#[test]
fn inlist_json_profile_preserves_nominal_evidence_and_rejects_plain_text_mixing() {
    use novarocks_type_contract::{
        CompileControlError, CompilePhase, FunctionValueType, PureCompileControl, ValueLogicalType,
    };
    struct Compile;
    impl PureCompileControl for Compile {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            Ok(())
        }
    }
    let json =
        FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
            .unwrap();
    let plain = FunctionValueType::new(DataType::Utf8, true);
    let boolean = FunctionValueType::new(DataType::Boolean, true);
    let recipe =
        super::PreparedNativeInListRecipe::try_new(false, &json, &[&json], &boolean, &Compile)
            .unwrap();
    assert_eq!(recipe.source_type(), &json);
    assert_eq!(recipe.candidate_types(), std::slice::from_ref(&json));
    assert!(
        super::PreparedNativeInListRecipe::try_new(false, &json, &[&plain], &boolean, &Compile)
            .is_err()
    );
    assert!(
        super::PreparedNativeInListRecipe::try_new(false, &plain, &[&json], &boolean, &Compile)
            .is_err()
    );
}

#[test]
fn json_inlist_required_data_effects_are_shared_by_owned_and_borrowed_profiles() {
    use novarocks_type_contract::{
        EvaluationDemand, EvaluationDomainId, ExpressionEffectContext, ExpressionUseId,
        FunctionValueType, ValueLogicalType,
    };
    let context = ExpressionEffectContext {
        use_id: ExpressionUseId::new(760),
        domain: EvaluationDomainId::new(14),
        demand: EvaluationDemand::Value,
    };
    let result = FunctionValueType::new(DataType::Boolean, true);
    for (carrier, logical, required) in [
        (DataType::Utf8, ValueLogicalType::Json, true),
        (DataType::Utf8, ValueLogicalType::Physical, false),
        (DataType::Int64, ValueLogicalType::Physical, false),
    ] {
        let source = FunctionValueType::try_with_logical_type(carrier, true, logical).unwrap();
        for negated in [false, true] {
            let prepared = super::PreparedNativeInListRecipe::try_new(
                negated,
                &source,
                &[&source],
                &result,
                crate::binding_test_control(),
            )
            .unwrap();
            let own = prepared.own_effects(context);
            let borrowed = super::PreparedNativeInListRecipe::refine_borrowed_profile(
                &source,
                [&source].into_iter(),
                &result,
                context,
                crate::binding_test_control(),
            )
            .unwrap();
            assert_eq!(own, borrowed);
            let effects = own.for_use(context).unwrap();
            assert_eq!(effects.may_raise_row_error, required);
            assert_eq!(effects.may_raise_invocation_data, required);
            assert_eq!(effects.permits_boolean_reordering(), !required);
            assert_eq!(effects.permits_earlier_evaluation(), !required);
        }
    }
}
