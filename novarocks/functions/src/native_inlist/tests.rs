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
//! Private IN state, construction, and control witnesses.
use super::{InError, InObservation, InRows, MembershipComparison};
use crate::{KernelDiagnostic, KernelEvaluationControl, KernelFailure};
use arrow_array::{Array, ArrayRef, BooleanArray, Int64Array};
use arrow_schema::DataType;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
fn values(array: &BooleanArray) -> Vec<Option<bool>> {
    array.iter().collect()
}

#[test]
fn group_reduction_uses_ieee_equality_and_preserves_unknowns() {
    use super::MembershipEquality;
    use crate::EvaluatedArgument;
    use arrow_array::Float64Array;
    use novarocks_type_contract::FunctionValueType;
    let ty = FunctionValueType::new(DataType::Float64, true);
    let MembershipEquality::Comparison(equality) =
        MembershipEquality::prepare_group(&ty, &ty, crate::binding_test_control()).unwrap()
    else {
        panic!("DOUBLE equality must borrow its two selected rows");
    };
    let left: ArrayRef = Arc::new(Float64Array::from(vec![Some(f64::NAN), Some(-0.0), None]));
    let right: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(f64::NAN),
        Some(0.0),
        Some(0.0),
    ]));
    let control = Control {
        trace: Mutex::new(Vec::new()),
        fail: None,
    };
    let mut observer = |_| Ok::<_, std::convert::Infallible>(());
    let mut state = InRows::begin_in_observed(3, None, &mut observer).unwrap();
    for row in 0..3 {
        let result = equality
            .compare_rows(
                EvaluatedArgument::Column(&left),
                row,
                row,
                EvaluatedArgument::Column(&right),
                row,
                row,
                &control,
            )
            .unwrap();
        state.record_equality(row, result).unwrap();
    }
    assert!(state.needs_comparison(0).unwrap());
    assert!(!state.needs_comparison(1).unwrap());
    assert_eq!(
        values(
            &state
                .finish_group_observed(false, &[], &mut observer)
                .unwrap()
        ),
        vec![Some(false), Some(true), None],
    );
}
#[test]
fn membership_match_dominates_candidate_null_and_preserves_probe_null() {
    let mut state = InRows::begin_in_observed(3, None, &mut accept).unwrap();
    state.record_equality(2, None).unwrap();
    state
        .record_selected_equalities(
            &BooleanArray::from(vec![Some(true), Some(false)]),
            &[0, 1],
            &mut accept,
        )
        .unwrap();
    state
        .record_selected_equalities(&BooleanArray::from(vec![None, None]), &[0, 1], &mut accept)
        .unwrap();
    assert_eq!(
        values(
            &state
                .finish_group_observed(false, &[], &mut accept)
                .unwrap()
        ),
        vec![Some(true), None, None],
    );
    let empty = InRows::begin_in_observed(0, None, &mut accept).unwrap();
    assert!(
        empty
            .finish_group_observed(false, &[], &mut accept)
            .unwrap()
            .is_empty()
    );
}

fn accept(_: InObservation) -> Result<(), std::convert::Infallible> {
    Ok(())
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

#[test]
fn json_membership_prepares_only_demanded_probes_and_reuses_literal_backing() {
    use super::{JsonConstantKeyCache, JsonProbeKeys, json_array, json_host, key_allocator};
    use arrow_array::LargeBinaryArray;
    use arrow_schema::Field;
    use novarocks_constant_contract::{ConstantPolicy, ConstantValue};
    use novarocks_type_contract::{CompilePhase, FunctionValueType, ValueLogicalType};

    let json_null = b"\x01\x20".as_slice();
    let invalid_unused = b"null".as_slice();
    let input: ArrayRef = Arc::new(LargeBinaryArray::from(vec![json_null, invalid_unused]));
    let control = Control {
        trace: Mutex::new(Vec::new()),
        fail: None,
    };
    let mut probe = JsonProbeKeys::prepare(&input, None, &control).unwrap();
    let before = control.trace.lock().unwrap().len();
    assert!(probe.prepare_at(0, &control).unwrap().is_some());
    let after = control.trace.lock().unwrap().len();
    assert!(after > before);
    assert!(probe.prepare_at(0, &control).unwrap().is_some());
    assert_eq!(control.trace.lock().unwrap().len(), after);
    assert!(probe.keys[1].is_none());

    let fvt = FunctionValueType::try_with_logical_type(
        DataType::LargeBinary,
        false,
        ValueLogicalType::Json,
    )
    .unwrap();
    let original = ConstantValue::from_binary(
        Arc::new(Field::new("candidate", DataType::LargeBinary, false)),
        fvt,
        json_null,
        ConstantPolicy {
            max_rows: 4,
            max_array_nodes: 4,
            max_logical_elements: 4,
            max_retained_buffer_bytes: 1024,
            max_type_depth: 4,
            max_type_nodes: 4,
            max_dictionary_depth: 1,
            max_metadata_bytes: 1024,
            max_library_validation_work: 16384,
            max_library_validation_bytes: 16384,
        },
        CompilePhase::Validate,
        crate::binding_test_control(),
    )
    .unwrap();
    let allocator = key_allocator(json_host(None)).unwrap();
    let mut cache = JsonConstantKeyCache::default();
    assert!(
        cache
            .get_or_prepare(&original, json_array(&input).unwrap(), &allocator, &control)
            .unwrap()
            .is_some()
    );
    let after_first = control.trace.lock().unwrap().len();
    assert!(
        cache
            .get_or_prepare(
                &original.clone(),
                json_array(&input).unwrap(),
                &allocator,
                &control,
            )
            .unwrap()
            .is_some()
    );
    assert_eq!(control.trace.lock().unwrap().len(), after_first + 1);
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
    let candidate: ArrayRef = Arc::new(Int64Array::from(vec![Some(7)]));
    let ty = novarocks_type_contract::FunctionValueType::new(DataType::Int64, true);
    let comparison =
        MembershipComparison::try_new(&ty, &ty, crate::binding_test_control()).unwrap();
    let mut state = InRows::begin_in_observed(input.len(), None, &mut observe)?;
    let equality = comparison.membership_equalities_observed(
        &input,
        &candidate,
        None,
        control,
        &mut observe,
    )?;
    let parents = (0..input.len()).collect::<Vec<_>>();
    equality.apply_to_selected_observed(&mut state, &parents, &mut observe)?;
    state.finish_group_observed(false, &[], &mut observe)
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
            assert!(matches!(
                invoke(&control),
                Err(InError::Host(found) | InError::Operation(found)) if found == *cause
            ));
            assert_eq!(
                *control.trace.lock().unwrap(),
                trace[..at],
                "no callback after originating refusal"
            );
        }
    }
}

#[test]
fn membership_comparison_keeps_exact_flat_domains_and_nullable_result() {
    use crate::ComparisonPrepareError;
    use novarocks_type_contract::FunctionValueType;

    for data_type in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Utf8,
        DataType::UInt64,
        DataType::Null,
        DataType::Decimal128(4, 0),
    ] {
        let input = FunctionValueType::from_legacy_declared_projection(data_type.clone(), false);
        let nullable = FunctionValueType::from_legacy_declared_projection(data_type, true);
        let comparison =
            MembershipComparison::try_new(&input, &nullable, crate::binding_test_control())
                .unwrap();
        assert_eq!(comparison.source_type(), &input);
        assert!(comparison.result_nullable(&nullable));
        assert_eq!(
            MembershipComparison::try_new(
                &input,
                &FunctionValueType::new(DataType::Boolean, false),
                crate::binding_test_control(),
            )
            .unwrap_err(),
            ComparisonPrepareError::TypeMismatch
        );
    }
}
#[test]
fn membership_selected_parents_preserve_truth_only_and_error_placeholders() {
    for truth_only in [false, true] {
        let mut state = InRows::begin_in_observed(4, None, &mut accept).unwrap();
        state.record_equality(1, None).unwrap();
        state
            .record_selected_equalities(
                &BooleanArray::from(vec![None, Some(true)]),
                &[0, 2],
                &mut accept,
            )
            .unwrap();
        let out = state
            .finish_group_observed(truth_only, &[3], &mut accept)
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
fn malformed_membership_equality_geometry_is_an_invariant_failure() {
    use novarocks_failure_contract::{FailureClass, FailureProjection};

    let mut state = InRows::begin_in_observed(3, None, &mut accept).unwrap();
    for parents in [&[0, 1][..], &[3][..]] {
        let failure = state
            .record_selected_equalities(&BooleanArray::from(vec![true]), parents, &mut accept)
            .unwrap_err();
        assert!(matches!(failure, InError::Operation(ref cause)
                if cause.failure_fact().class() == FailureClass::Invariant
                    && cause.failure_fact().reason().code() == 381));
    }
    assert_eq!(
        values(
            &state
                .finish_group_observed(false, &[], &mut accept)
                .unwrap()
        ),
        vec![Some(false); 3],
    );
}

#[test]
fn membership_comparison_preserves_the_json_domain_and_its_failure_effects() {
    use novarocks_type_contract::{FunctionValueType, ValueLogicalType};

    let control = crate::binding_test_control();
    let json = FunctionValueType::try_with_logical_type(
        DataType::LargeBinary,
        true,
        ValueLogicalType::Json,
    )
    .unwrap();
    let plain = FunctionValueType::new(DataType::Utf8, true);
    let untyped_binary = FunctionValueType::new(DataType::LargeBinary, true);
    let comparison = MembershipComparison::try_new(&json, &json, control).unwrap();
    assert_eq!(comparison.source_type(), &json);
    for other in [&plain, &untyped_binary] {
        assert!(MembershipComparison::try_new(&json, other, control).is_err());
        assert!(MembershipComparison::try_new(other, &json, control).is_err());
    }
    for (source, required) in [
        (json, true),
        (plain, false),
        (FunctionValueType::new(DataType::Int64, true), false),
    ] {
        let (_, effects) = super::candidate_group_profile(&source, &source, control).unwrap();
        assert_eq!(effects.may_raise_row_error, required);
        assert_eq!(effects.may_raise_invocation_data, required);
        assert_eq!(effects.permits_boolean_reordering(), !required);
        assert_eq!(effects.permits_earlier_evaluation(), !required);
    }
}
