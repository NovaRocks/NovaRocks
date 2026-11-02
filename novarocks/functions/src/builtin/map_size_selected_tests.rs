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

//! Exact array selected addressing and actual original-control refusal points.
use super::super::map_size_owner::tests::prepared_with_control;
use super::*;
use crate::{
    EvaluatedArgument, FunctionSpecializationFailure, KernelDiagnostic, ScalarEvaluationInstance,
    SelectedValues, Selection,
};
use arrow_array::{Array, ArrayRef, Int32Array, MapArray, StringArray, StructArray};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        let mut t = self.trace.lock().unwrap();
        let at = t.len();
        if let Some((stop, _)) = &self.refusal {
            assert!(at <= *stop)
        }
        t.push(units);
        match &self.refusal {
            Some((stop, cause)) if *stop == at => Err(cause.clone()),
            _ => Ok(()),
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("collection kernels never wait")
    }
}
#[derive(Default)]
struct Compile {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Compile {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut t = self.trace.lock().unwrap();
        let at = t.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop)
        }
        t.push((phase, units));
        match self.refusal.clone() {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}

fn fixture(nullable: bool, sorted: bool) -> ArrayRef {
    let entries = StructArray::new(
        vec![
            Arc::new(Field::new("key", DataType::Int32, false)),
            Arc::new(Field::new("value", DataType::Utf8, true).with_metadata(
                std::collections::HashMap::<String, String>::from([(
                    "value-fact".into(),
                    "preserved".into(),
                )]),
            )),
        ]
        .into(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                None,
                Some("value"),
                None,
                None,
                Some("last"),
            ])),
        ],
        None,
    );
    let external: ArrayRef = Arc::new(MapArray::new(
        Arc::new(
            Field::new("entries", entries.data_type().clone(), false).with_metadata(
                std::collections::HashMap::<String, String>::from([(
                    "entry-fact".into(),
                    "preserved".into(),
                )]),
            ),
        ),
        OffsetBuffer::new(vec![0, 2, 2, 4, 5].into()),
        entries,
        nullable.then(|| NullBuffer::from(vec![true, true, false, true])),
        sorted,
    ));
    let binding = FunctionValueType::try_with_logical_type(
        external.data_type().clone(),
        nullable,
        novarocks_type_contract::ValueLogicalType::Physical,
    )
    .unwrap();
    crate::test_value_ingress::array_for_binding(&external, &binding).unwrap()
}
fn source(a: &ArrayRef, nullable: bool) -> FunctionValueType {
    FunctionValueType::from_legacy_declared_projection(a.data_type().clone(), nullable)
}
fn prepared(
    types: &[FunctionValueType],
    c: &dyn PureCompileControl,
) -> Result<Arc<dyn crate::PreparedScalarKernel>, FunctionSpecializationFailure> {
    prepared_with_control("map_size", types, c)
}
fn output(v: &SelectedValues<'_>) -> Vec<Option<i32>> {
    assert!(v.errors().is_empty());
    v.values()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .iter()
        .collect()
}
#[test]
fn map_size_original_offsets_sparse_sliced_scalar_and_compact_mappings() {
    for nullable in [false, true] {
        for sorted in [false, true] {
            let a = fixture(nullable, sorted);
            let p = prepared(&[source(&a, nullable)], crate::binding_test_control()).unwrap();
            let rows = [0, 2, 3];
            let selection = Selection::try_sparse(4, &rows).unwrap();
            let take = arrow_select::take::take(
                a.as_ref(),
                &arrow_array::UInt32Array::from(vec![0, 2, 3]),
                None,
            )
            .unwrap();
            let compact =
                SelectedValues::try_new(selection, a.data_type(), take, Box::default()).unwrap();
            for arg in [
                EvaluatedArgument::Column(&a),
                EvaluatedArgument::SelectedColumn(&compact),
            ] {
                assert_eq!(
                    output(
                        &ScalarEvaluationInstance::instantiate(p.clone())
                            .unwrap()
                            .evaluate(selection, &[arg], &Control::default())
                            .unwrap()
                    ),
                    if nullable {
                        vec![Some(2), None, Some(1)]
                    } else {
                        vec![Some(2), Some(2), Some(1)]
                    }
                );
            }
            let sliced = a.slice(1, 3);
            let sparse = [0, 2];
            let sel = Selection::try_sparse(3, &sparse).unwrap();
            assert_eq!(
                output(
                    &ScalarEvaluationInstance::instantiate(p.clone())
                        .unwrap()
                        .evaluate(
                            sel,
                            &[EvaluatedArgument::Column(&sliced)],
                            &Control::default()
                        )
                        .unwrap()
                ),
                vec![Some(0), Some(1)]
            );
            let scalar = a.slice(3, 1);
            assert_eq!(
                output(
                    &ScalarEvaluationInstance::instantiate(p.clone())
                        .unwrap()
                        .evaluate(
                            selection,
                            &[EvaluatedArgument::Scalar(&scalar)],
                            &Control::default()
                        )
                        .unwrap()
                ),
                vec![Some(1); 3]
            );
            assert!(
                ScalarEvaluationInstance::instantiate(p)
                    .unwrap()
                    .evaluate(
                        Selection::try_sparse(4, &[]).unwrap(),
                        &[EvaluatedArgument::Column(&a)],
                        &Control::default()
                    )
                    .unwrap()
                    .values()
                    .is_empty()
            );
        }
    }
}
#[test]
fn map_size_constant_pool_nonzero_ordinal_keeps_exact_field_and_null_parent() {
    let a = fixture(true, true);
    let source = source(&a, true);
    let policy = crate::ConstantPolicy {
        max_rows: 1024,
        max_array_nodes: 64,
        max_logical_elements: 65536,
        max_retained_buffer_bytes: 1048576,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 16,
        max_metadata_bytes: 65536,
        max_library_validation_work: 4194304,
        max_library_validation_bytes: 4194304,
    };
    let pool = crate::ConstantPool::try_new(
        Arc::new(source.try_to_field("actual").unwrap()),
        source.clone(),
        a.to_data(),
        policy,
        CompilePhase::Validate,
        crate::binding_test_control(),
    )
    .unwrap();
    let p = prepared(&[source], crate::binding_test_control()).unwrap();
    let rows = [1, 256, 512];
    let selection = Selection::try_sparse(513, &rows).unwrap();
    for ordinal in [2, 3] {
        let v = pool.value(ordinal).unwrap();
        assert_eq!(
            output(
                &ScalarEvaluationInstance::instantiate(p.clone())
                    .unwrap()
                    .evaluate(
                        selection,
                        &[EvaluatedArgument::Constant(&v)],
                        &Control::default()
                    )
                    .unwrap()
            ),
            vec![if ordinal == 2 { None } else { Some(1) }; 3]
        );
    }
}
#[test]
fn map_size_actual_large_work_and_seven_failure_causes_keep_prefix_and_latch() {
    let a = fixture(true, true);
    let index =
        arrow_array::UInt32Array::from((0..513).map(|r| (r % 4) as u32).collect::<Vec<_>>());
    let a = arrow_select::take::take(a.as_ref(), &index, None).unwrap();
    let p = prepared(&[source(&a, true)], crate::binding_test_control()).unwrap();
    let args = [EvaluatedArgument::Column(&a)];
    let good = Control::default();
    ScalarEvaluationInstance::instantiate(p.clone())
        .unwrap()
        .evaluate(Selection::all(513), &args, &good)
        .unwrap();
    let trace = good.trace.lock().unwrap().clone();
    assert!(trace.contains(&256));
    for at in 0..trace.len() {
        for cause in [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
            KernelFailure::InvalidProgram(KernelDiagnostic::new("original invalid")),
            KernelFailure::Internal(KernelDiagnostic::new("original internal")),
            KernelFailure::Operational(KernelDiagnostic::new("original operational")),
            KernelFailure::InstanceFailed,
        ] {
            let c = Control {
                trace: Mutex::new(vec![]),
                refusal: Some((at, cause.clone())),
            };
            let mut k = ScalarEvaluationInstance::instantiate(p.clone()).unwrap();
            assert_eq!(
                k.evaluate(Selection::all(513), &args, &c).unwrap_err(),
                cause
            );
            assert_eq!(*c.trace.lock().unwrap(), trace[..=at]);
            let after = Control::default();
            assert_eq!(
                k.evaluate(Selection::all(513), &args, &after).unwrap_err(),
                KernelFailure::InstanceFailed
            );
            assert!(after.trace.lock().unwrap().is_empty());
        }
    }
}
#[test]
fn map_size_every_actual_compile_callback_preserves_three_causes_and_named_refusal_tail() {
    let a = fixture(true, true);
    for types in [
        vec![source(&a, true)],
        vec![source(&a, false)],
        vec![FunctionValueType::new(DataType::Int32, true)],
    ] {
        let good = Compile::default();
        let success = *types[0].data_type() == *a.data_type();
        assert_eq!(prepared(&types, &good).is_ok(), success);
        let trace = good.trace.lock().unwrap().clone();
        assert!(!trace.is_empty());
        for at in 0..trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let c = Compile {
                    trace: Mutex::new(vec![]),
                    refusal: Some((at, cause.clone())),
                };
                let error = prepared(&types, &c).err().unwrap();
                let actual = match error {
                    FunctionSpecializationFailure::Control(c) => Some(c),
                    FunctionSpecializationFailure::Kernel(KernelFailure::Cancelled) => {
                        Some(CompileControlError::Cancelled)
                    }
                    FunctionSpecializationFailure::Kernel(KernelFailure::DeadlineExceeded) => {
                        Some(CompileControlError::DeadlineExceeded)
                    }
                    FunctionSpecializationFailure::Kernel(KernelFailure::ResourceExhausted) => {
                        Some(CompileControlError::ResourceExhausted)
                    }
                    _ => None,
                };
                assert_eq!(actual, Some(cause));
                assert_eq!(*c.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
}
