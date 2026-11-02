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
use crate::kernel_control::internal;
use crate::{ConstantPolicy, ConstantPool, EvaluatedArgument, ScalarEvaluationInstance, Selection};
use arrow_array::{Array, ArrayRef, BooleanArray, ListArray, StringArray, new_empty_array};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, DecimalOverflowPolicy, FunctionValueType,
};
use std::{sync::Mutex, time::Duration};
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = &self.refusal {
            assert!(at <= *stop, "callback after first cause");
        }
        trace.push(units);
        match &self.refusal {
            Some((stop, cause)) if *stop == at => Err(cause.clone()),
            _ => Ok(()),
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("array match never waits")
    }
}
#[derive(Default)]
struct CompileControl {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for CompileControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop, "compile callback after first cause");
        }
        trace.push((phase, units));
        match self.refusal.clone() {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}

fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        invalid("original refusal"),
        internal("original refusal"),
        KernelFailure::Operational(crate::KernelDiagnostic::new("original refusal")),
        KernelFailure::InstanceFailed,
    ]
}

fn policy() -> ConstantPolicy {
    // Finite fixture admission of already allocated real Arrow arrays, not a MEM grant.
    ConstantPolicy {
        max_rows: 1024,
        max_array_nodes: 64,
        max_logical_elements: 65536,
        max_retained_buffer_bytes: 1024 * 1024,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 16,
        max_metadata_bytes: 65536,
        max_library_validation_work: 4 * 1024 * 1024,
        max_library_validation_bytes: 4 * 1024 * 1024,
    }
}

fn instance(name: &str, a: &ArrayRef) -> ScalarEvaluationInstance {
    ScalarEvaluationInstance::instantiate(
        prepared_for_test_with_control(
            name,
            &[FunctionValueType::from_legacy_declared_projection(
                a.data_type().clone(),
                true,
            )],
            DecimalOverflowPolicy::OutputNull,
            crate::binding_test_control(),
        )
        .unwrap(),
    )
    .unwrap()
}
fn list(values: ArrayRef, offsets: Vec<i32>, valid: Option<Vec<bool>>) -> ArrayRef {
    Arc::new(ListArray::new(
        Arc::new(Field::new("item", values.data_type().clone(), true)),
        OffsetBuffer::new(offsets.into()),
        values,
        valid.map(NullBuffer::from),
    ))
}
fn bools(a: &ArrayRef) -> Vec<Option<bool>> {
    assert_eq!(a.data_type(), &DataType::Boolean);
    a.as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .iter()
        .collect()
}
#[test]
fn array_match_selected_source_origin_root_null_three_values_slice_empty_and_nonzero_pool() {
    let a = list(
        Arc::new(BooleanArray::from(vec![
            Some(false),
            Some(true),
            None,
            Some(false),
            Some(true),
            Some(true),
        ])),
        vec![0, 1, 3, 4, 6],
        Some(vec![true, true, false, true]),
    );
    let rows = [1usize, 2, 3];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let args = [EvaluatedArgument::Column(&a)];
    for name in ["all_match", "any_match"] {
        let expected = if name == "all_match" {
            vec![None, None, Some(true)]
        } else {
            vec![Some(true), None, Some(true)]
        };
        let result = instance(name, &a)
            .evaluate(selection, &args, &Control::default())
            .unwrap();
        assert!(result.errors().is_empty());
        assert_eq!(bools(result.values()), expected);
        let ty = FunctionValueType::from_legacy_declared_projection(a.data_type().clone(), true);
        let pool = ConstantPool::try_new(
            Arc::new(ty.try_to_field("pool-source").unwrap()),
            ty,
            a.to_data(),
            policy(),
            CompilePhase::Validate,
            crate::binding_test_control(),
        )
        .unwrap();
        let constant = pool.value(1).unwrap();
        let args = [EvaluatedArgument::Constant(&constant)];
        let result = instance(name, &a)
            .evaluate(Selection::all(3), &args, &Control::default())
            .unwrap();
        assert_eq!(
            bools(result.values()),
            if name == "all_match" {
                vec![None; 3]
            } else {
                vec![Some(true); 3]
            }
        );
        let sliced = a.slice(1, 3);
        let args = [EvaluatedArgument::Column(&sliced)];
        let result = instance(name, &sliced)
            .evaluate(Selection::all(3), &args, &Control::default())
            .unwrap();
        assert_eq!(bools(result.values()), expected);
        let empty = a.slice(0, 0);
        let args = [EvaluatedArgument::Column(&empty)];
        assert_eq!(
            instance(name, &empty)
                .evaluate(Selection::all(0), &args, &Control::default())
                .unwrap()
                .values()
                .len(),
            0
        );
    }
}
#[test]
fn array_match_selected_original_string_safe_nulls_and_exact_child_metadata() {
    let a = list(
        Arc::new(StringArray::from(vec![
            Some("true"),
            Some("invalid"),
            Some("false"),
            None,
        ])),
        vec![0, 2, 4],
        None,
    );
    for name in ["all_match", "any_match"] {
        let args = [EvaluatedArgument::Column(&a)];
        let result = instance(name, &a)
            .evaluate(Selection::all(2), &args, &Control::default())
            .unwrap();
        assert!(result.errors().is_empty());
        assert_eq!(
            bools(result.values()),
            if name == "all_match" {
                vec![None, Some(false)]
            } else {
                vec![Some(true), None]
            }
        );
        let prepared = prepared_for_test_with_policy(
            name,
            &[FunctionValueType::from_legacy_declared_projection(
                a.data_type().clone(),
                true,
            )],
            DecimalOverflowPolicy::ReportError,
        )
        .unwrap();
        assert_eq!(
            prepared.contract().selected().argument_types[0],
            FunctionArgumentType::Value(FunctionValueType::from_legacy_declared_projection(
                a.data_type().clone(),
                true
            ))
        );
        // Binding authored this canonical field. A changed already-evaluated
        // field is rejected rather than silently retagged by the kernel.
        let list = a.as_any().downcast_ref::<ListArray>().unwrap();
        let altered: ArrayRef = Arc::new(ListArray::new(
            Arc::new(
                Field::new("authored-item", list.values().data_type().clone(), true).with_metadata(
                    std::collections::HashMap::<String, String>::from([(
                        "opaque-child".into(),
                        "not-erased".into(),
                    )]),
                ),
            ),
            list.offsets().clone(),
            list.values().clone(),
            list.nulls().cloned(),
        ));
        let arguments = [EvaluatedArgument::Column(&altered)];
        let mut instance = ScalarEvaluationInstance::instantiate(prepared).unwrap();
        assert!(matches!(
            instance.evaluate(Selection::all(2), &arguments, &Control::default()),
            Err(KernelFailure::InvalidProgram(_))
        ));
    }
}
#[test]
fn array_match_selected_all_seven_causes_each_observation_and_failed_instance_latch() {
    for name in ["all_match", "any_match"] {
        // Prevent short circuit so the actual original inner boolean loop exceeds one quantum.
        let a = list(
            Arc::new(BooleanArray::from(vec![Some(name == "all_match"); 700])),
            vec![0, 700],
            None,
        );
        let args = [EvaluatedArgument::Column(&a)];
        let good = Control::default();
        instance(name, &a)
            .evaluate(Selection::all(1), &args, &good)
            .unwrap();
        let trace = good.trace.lock().unwrap().clone();
        assert!(trace.contains(&256));
        for cause in causes() {
            for stop in 0..trace.len() {
                let control = Control {
                    trace: Mutex::new(vec![]),
                    refusal: Some((stop, cause.clone())),
                };
                let mut kernel = instance(name, &a);
                assert_eq!(
                    kernel
                        .evaluate(Selection::all(1), &args, &control)
                        .unwrap_err(),
                    cause
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=stop]);
                let after = Control::default();
                assert_eq!(
                    kernel
                        .evaluate(Selection::all(1), &args, &after)
                        .unwrap_err(),
                    KernelFailure::InstanceFailed
                );
                assert!(after.trace.lock().unwrap().is_empty());
            }
        }
        let a = list(new_empty_array(&DataType::Boolean), vec![0; 322], None);
        let args = [EvaluatedArgument::Column(&a)];
        let control = Control::default();
        let result = instance(name, &a)
            .evaluate(Selection::all(321), &args, &control)
            .unwrap();
        assert_eq!(bools(result.values()), vec![Some(name == "all_match"); 321]);
        assert!(control.trace.lock().unwrap().contains(&256));
        let a = list(
            crate::largeint::array_from_i128(&vec![
                Some(if name == "all_match" { 1 } else { 0 });
                700
            ])
            .unwrap(),
            vec![0, 700],
            None,
        );
        let args = [EvaluatedArgument::Column(&a)];
        let control = Control::default();
        instance(name, &a)
            .evaluate(Selection::all(1), &args, &control)
            .unwrap();
        assert!(control.trace.lock().unwrap().contains(&256));
    }
}

#[test]
fn array_match_compile_callbacks_preserve_primary_causes_on_success_and_rejection() {
    for name in ["all_match", "any_match"] {
        for (source, succeeds) in [
            (
                FunctionValueType::from_legacy_declared_projection(
                    DataType::List(Arc::new(Field::new("item", DataType::Boolean, true))),
                    true,
                ),
                true,
            ),
            (FunctionValueType::new(DataType::Binary, true), false),
        ] {
            let good = CompileControl::default();
            assert_eq!(
                prepared_for_test_with_control(
                    name,
                    std::slice::from_ref(&source),
                    DecimalOverflowPolicy::OutputNull,
                    &good,
                )
                .is_ok(),
                succeeds,
            );
            let trace = good.trace.lock().unwrap().clone();
            assert!(!trace.is_empty());
            for stop in 0..trace.len() {
                for cause in [
                    CompileControlError::Cancelled,
                    CompileControlError::DeadlineExceeded,
                    CompileControlError::ResourceExhausted,
                ] {
                    let control = CompileControl {
                        trace: Mutex::new(vec![]),
                        refusal: Some((stop, cause.clone())),
                    };
                    let error = prepared_for_test_with_control(
                        name,
                        std::slice::from_ref(&source),
                        DecimalOverflowPolicy::OutputNull,
                        &control,
                    )
                    .expect_err("refused compilation must not publish a prepared kernel");
                    let actual = match error {
                        crate::FunctionSpecializationFailure::Control(actual) => Some(actual),
                        crate::FunctionSpecializationFailure::Kernel(KernelFailure::Cancelled) => {
                            Some(CompileControlError::Cancelled)
                        }
                        crate::FunctionSpecializationFailure::Kernel(
                            KernelFailure::DeadlineExceeded,
                        ) => Some(CompileControlError::DeadlineExceeded),
                        crate::FunctionSpecializationFailure::Kernel(
                            KernelFailure::ResourceExhausted,
                        ) => Some(CompileControlError::ResourceExhausted),
                        _ => None,
                    };
                    assert_eq!(actual, Some(cause));
                    assert_eq!(*control.trace.lock().unwrap(), trace[..=stop]);
                }
            }
        }
    }
}
