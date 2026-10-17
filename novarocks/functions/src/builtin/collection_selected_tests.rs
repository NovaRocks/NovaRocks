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

//! Actual selected ABI mapping, required row errors and first-cause controls.
use super::super::{array_literal_owner::tests as literal, map_element_at_owner::tests as access};
use super::*;
use crate::{
    EvaluatedArgument, FunctionSpecializationFailure, KernelDiagnostic, ScalarEvaluationInstance,
    SelectedValues, Selection,
};
use arrow_array::{
    Array, ArrayRef, Int32Array, ListArray, MapArray, StringArray, StructArray, UInt64Array,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field};
use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
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
        match self.refusal {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}
fn prepared(
    op: Operation,
    types: &[FunctionValueType],
    control: &dyn PureCompileControl,
) -> Result<Arc<dyn crate::PreparedScalarKernel>, FunctionSpecializationFailure> {
    match op {
        Operation::ArrayLiteral => {
            literal::prepared_with_control("__array_literal", types, control)
        }
        Operation::MapLookup => access::prepared_with_control("__map_element_at", types, control),
    }
}
fn map(keys: ArrayRef, values: ArrayRef, offsets: Vec<i32>, valid: Option<Vec<bool>>) -> ArrayRef {
    let fs = vec![
        Arc::new(Field::new("key", keys.data_type().clone(), false)),
        Arc::new(Field::new("value", values.data_type().clone(), true)),
    ];
    let e = StructArray::new(fs.into(), vec![keys, values], None);
    Arc::new(MapArray::new(
        Arc::new(Field::new("entries", e.data_type().clone(), false)),
        OffsetBuffer::new(offsets.into()),
        e,
        valid.map(NullBuffer::from),
        false,
    ))
}
fn source(a: &ArrayRef) -> FunctionValueType {
    FunctionValueType::from_legacy_declared_projection(a.data_type().clone(), true)
}
#[test]
fn collection_independent_column_scalar_compact_mappings_preserve_null_key_and_constructor_order() {
    let column: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None, Some(3), Some(4)]));
    let scalar: ArrayRef = Arc::new(Int32Array::from(vec![Some(8)]));
    let rows = [1, 3];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let compact: ArrayRef = Arc::new(Int32Array::from(vec![Some(10), Some(40)]));
    let selected =
        SelectedValues::try_new(selection, &DataType::Int32, compact, Box::new([])).unwrap();
    let p = prepared(
        Operation::ArrayLiteral,
        &[
            source(&column),
            source(&scalar),
            FunctionValueType::new(DataType::Int32, true),
        ],
        crate::binding_test_control(),
    )
    .unwrap();
    let args = [
        EvaluatedArgument::Column(&column),
        EvaluatedArgument::Scalar(&scalar),
        EvaluatedArgument::SelectedColumn(&selected),
    ];
    let mut k = ScalarEvaluationInstance::instantiate(p).unwrap();
    let out = k.evaluate(selection, &args, &Control::default()).unwrap();
    assert!(out.errors().is_empty());
    let list = out.values().as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(list.value_offsets(), &[0, 3, 6]);
    assert_eq!(
        list.values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![None, Some(8), Some(10), Some(4), Some(8), Some(40)]
    );
    let m = map(
        Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
        Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
        vec![0, 1, 3, 4, 5],
        None,
    );
    let key: ArrayRef = Arc::new(Int32Array::from(vec![None]));
    let args = [
        EvaluatedArgument::Column(&m),
        EvaluatedArgument::Scalar(&key),
    ];
    let p = prepared(
        Operation::MapLookup,
        &[source(&m), source(&key)],
        crate::binding_test_control(),
    )
    .unwrap();
    let mut k = ScalarEvaluationInstance::instantiate(p).unwrap();
    let out = k.evaluate(selection, &args, &Control::default()).unwrap();
    assert!(out.errors().is_empty());
    assert_eq!(
        out.values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![None, None]
    );
}
#[test]
fn map_lookup_original_unsupported_domain_is_bounded_rowdata_and_undemanded_errors_eliminate() {
    let m = map(
        Arc::new(UInt64Array::from(vec![1, 2])),
        Arc::new(Int32Array::from(vec![7, 8])),
        vec![0, 1, 2, 2],
        Some(vec![true, true, false]),
    );
    let key: ArrayRef = Arc::new(UInt64Array::from(vec![Some(1), None, Some(2)]));
    let args = [
        EvaluatedArgument::Column(&m),
        EvaluatedArgument::Column(&key),
    ];
    let p = prepared(
        Operation::MapLookup,
        &[source(&m), source(&key)],
        crate::binding_test_control(),
    )
    .unwrap();
    let mut k = ScalarEvaluationInstance::instantiate(p.clone()).unwrap();
    let out = k
        .evaluate(Selection::all(3), &args, &Control::default())
        .unwrap();
    assert_eq!(out.errors().len(), 1);
    assert_eq!(out.errors()[0].selected_ordinal(), 0);
    assert_eq!(
        out.errors()[0].message(),
        "map key compare unsupported type: UInt64"
    );
    assert_eq!(
        out.values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![None, None, None]
    );
    let rows = [1, 2];
    let mut k = ScalarEvaluationInstance::instantiate(p).unwrap();
    let out = k
        .evaluate(
            Selection::try_sparse(3, &rows).unwrap(),
            &args,
            &Control::default(),
        )
        .unwrap();
    assert!(out.errors().is_empty());
}
#[test]
fn collection_every_actual_runtime_checkpoint_preserves_seven_causes_quantum_and_failed_latch() {
    let text: ArrayRef = Arc::new(StringArray::from(
        (0..257)
            .map(|r| {
                if r % 7 == 0 {
                    None
                } else {
                    Some("é\0payload")
                }
            })
            .collect::<Vec<_>>(),
    ));
    let m = map(
        Arc::new(Int32Array::from_iter_values(0..300)),
        Arc::new(Int32Array::from_iter_values(0..300)),
        vec![0, 300],
        None,
    );
    let miss: ArrayRef = Arc::new(Int32Array::from(vec![-1]));
    let unsupported = map(
        Arc::new(UInt64Array::from(vec![1])),
        Arc::new(Int32Array::from(vec![2])),
        vec![0, 1],
        None,
    );
    let uk: ArrayRef = Arc::new(UInt64Array::from(vec![1]));
    for (op, columns, rows) in [
        (Operation::ArrayLiteral, vec![text], 257),
        (Operation::MapLookup, vec![m, miss], 1),
        (Operation::MapLookup, vec![unsupported, uk], 1),
        (
            Operation::MapLookup,
            vec![
                empty_uninhabited_map(&FunctionValueType::new(DataType::Int32, false), 1),
                Arc::new(StringArray::from(vec!["probe"])),
            ],
            1,
        ),
    ] {
        let types = columns.iter().map(source).collect::<Vec<_>>();
        let p = prepared(op, &types, crate::binding_test_control()).unwrap();
        let args = columns
            .iter()
            .map(EvaluatedArgument::Column)
            .collect::<Vec<_>>();
        let good = Control::default();
        ScalarEvaluationInstance::instantiate(p.clone())
            .unwrap()
            .evaluate(Selection::all(rows), &args, &good)
            .unwrap();
        let trace = good.trace.lock().unwrap().clone();
        assert!(!trace.is_empty());
        if rows > 1 {
            assert!(trace.contains(&256));
        }
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
                let control = Control {
                    trace: Mutex::new(vec![]),
                    refusal: Some((at, cause.clone())),
                };
                let mut k = ScalarEvaluationInstance::instantiate(p.clone()).unwrap();
                assert_eq!(
                    k.evaluate(Selection::all(rows), &args, &control)
                        .unwrap_err(),
                    cause
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
                let after = Control::default();
                assert_eq!(
                    k.evaluate(Selection::all(rows), &args, &after).unwrap_err(),
                    KernelFailure::InstanceFailed
                );
                assert!(after.trace.lock().unwrap().is_empty());
            }
        }
    }
}
#[test]
fn collection_every_actual_compile_checkpoint_preserves_three_causes_and_ordinary_refusal_tail() {
    let scalar = FunctionValueType::new(DataType::Int32, true);
    let m = map(
        Arc::new(Int32Array::from(vec![1])),
        Arc::new(Int32Array::from(vec![2])),
        vec![0, 1],
        None,
    );
    for (op, types, success) in [
        (Operation::ArrayLiteral, vec![], true),
        (Operation::ArrayLiteral, vec![scalar.clone(); 3], true),
        (Operation::MapLookup, vec![source(&m), scalar.clone()], true),
        (Operation::MapLookup, vec![scalar.clone(), scalar], false),
        (
            Operation::MapLookup,
            vec![
                access::uninhabited_map(&FunctionValueType::new(DataType::Int32, false), false),
                FunctionValueType::new(DataType::Utf8, false),
            ],
            true,
        ),
    ] {
        let good = Compile::default();
        assert_eq!(prepared(op, &types, &good).is_ok(), success);
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
                    refusal: Some((at, cause)),
                };
                let error = prepared(op, &types, &c).err().unwrap();
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

#[test]
fn map_lookup_null_values_support_original_and_compact_row_mappings() {
    let source_map = map(
        Arc::new(Int32Array::from(vec![1, 1, 1, 1])),
        Arc::new(arrow_array::NullArray::new(4)),
        vec![0, 1, 2, 3, 3, 4],
        Some(vec![true, true, false, true, true]),
    );
    let key: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 1, 1, 1]));
    let scalar: ArrayRef = Arc::new(Int32Array::from(vec![1]));
    let rows = [0, 1, 2, 4];
    for selection in [Selection::all(5), Selection::try_sparse(5, &rows).unwrap()] {
        for key_argument in [
            EvaluatedArgument::Column(&key),
            EvaluatedArgument::Scalar(&scalar),
        ] {
            let prepared = prepared(
                Operation::MapLookup,
                &[source(&source_map), source(&key)],
                crate::binding_test_control(),
            )
            .unwrap();
            let mut kernel = ScalarEvaluationInstance::instantiate(prepared).unwrap();
            let args = [EvaluatedArgument::Column(&source_map), key_argument];
            let result = kernel
                .evaluate(selection, &args, &Control::default())
                .unwrap();
            assert!(result.errors().is_empty());
            assert_eq!(result.values().data_type(), &DataType::Null);
            assert_eq!(result.values().len(), selection.len());
            assert_eq!(result.values().logical_null_count(), selection.len());
            assert!(result.values().to_data().nulls().is_none());
        }
    }
}

fn empty_uninhabited_map(value: &FunctionValueType, rows: usize) -> ArrayRef {
    let ty = access::uninhabited_map(value, false);
    let DataType::Map(entries, sorted) = ty.data_type() else {
        unreachable!()
    };
    let DataType::Struct(fields) = entries.data_type() else {
        unreachable!()
    };
    let values = StructArray::new(
        fields.clone(),
        vec![
            Arc::new(arrow_array::NullArray::new(0)),
            arrow_array::new_empty_array(value.data_type()),
        ],
        None,
    );
    Arc::new(MapArray::new(
        entries.clone(),
        OffsetBuffer::new(vec![0_i32; rows + 1].into()),
        values,
        None,
        *sorted,
    ))
}

#[test]
fn uninhabited_key_map_lookup_returns_typed_null_for_column_scalar_and_compact_probe() {
    use novarocks_type_contract::ValueLogicalType;
    for value in [
        FunctionValueType::new(DataType::Null, true),
        FunctionValueType::new(DataType::Int32, false),
        FunctionValueType::try_with_logical_type(DataType::Utf8, false, ValueLogicalType::Json)
            .unwrap(),
        FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            true,
            ValueLogicalType::LargeInt,
        )
        .unwrap(),
    ] {
        let map = empty_uninhabited_map(&value, 4);
        let probe: ArrayRef = Arc::new(StringArray::from(vec![
            Some("a"),
            None,
            Some("b"),
            Some("c"),
        ]));
        let scalar = probe.slice(0, 1);
        let rows = [1, 3];
        for selection in [Selection::all(4), Selection::try_sparse(4, &rows).unwrap()] {
            let compact_probe: ArrayRef = Arc::new(StringArray::from_iter(
                selection
                    .iter()
                    .map(|row| if row == 1 { None } else { Some("probe") }),
            ));
            let compact =
                SelectedValues::try_new(selection, &DataType::Utf8, compact_probe, Box::default())
                    .unwrap();
            for probe in [
                EvaluatedArgument::Column(&probe),
                EvaluatedArgument::Scalar(&scalar),
                EvaluatedArgument::SelectedColumn(&compact),
            ] {
                let prepared = prepared(
                    Operation::MapLookup,
                    &[
                        access::uninhabited_map(&value, false),
                        FunctionValueType::new(DataType::Utf8, true),
                    ],
                    crate::binding_test_control(),
                )
                .unwrap();
                let mut kernel = ScalarEvaluationInstance::instantiate(prepared).unwrap();
                let args = [EvaluatedArgument::Column(&map), probe];
                let out = kernel
                    .evaluate(selection, &args, &Control::default())
                    .unwrap();
                assert!(out.errors().is_empty());
                assert_eq!(out.values().data_type(), value.data_type());
                assert_eq!(out.values().len(), selection.len());
                assert_eq!(out.values().logical_null_count(), selection.len());
            }
        }
    }
}

#[test]
fn uninhabited_key_map_does_not_erase_probe_errors_or_accept_wrong_frozen_carrier() {
    let value = FunctionValueType::new(DataType::Int32, false);
    let map = empty_uninhabited_map(&value, 1);
    let probe: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>]));
    let failed = SelectedValues::try_new(
        Selection::all(1),
        &DataType::Utf8,
        probe,
        Box::from([crate::RowDataError::new(0, "required probe failed")]),
    )
    .unwrap();
    let wrong_probe: ArrayRef = Arc::new(Int32Array::from(vec![1]));
    let short_probe: ArrayRef = Arc::new(StringArray::from(Vec::<Option<&str>>::new()));
    let wrong_map = empty_uninhabited_map(&FunctionValueType::new(DataType::Utf8, false), 1);
    for args in [
        [
            EvaluatedArgument::Column(&map),
            EvaluatedArgument::SelectedColumn(&failed),
        ],
        [
            EvaluatedArgument::Column(&map),
            EvaluatedArgument::Column(&wrong_probe),
        ],
        [
            EvaluatedArgument::Column(&map),
            EvaluatedArgument::Column(&short_probe),
        ],
        [
            EvaluatedArgument::Column(&wrong_map),
            EvaluatedArgument::SelectedColumn(&failed),
        ],
    ] {
        let prepared = prepared(
            Operation::MapLookup,
            &[
                access::uninhabited_map(&value, false),
                FunctionValueType::new(DataType::Utf8, true),
            ],
            crate::binding_test_control(),
        )
        .unwrap();
        let mut kernel = ScalarEvaluationInstance::instantiate(prepared).unwrap();
        assert!(matches!(
            kernel.evaluate(Selection::all(1), &args, &Control::default()),
            Err(KernelFailure::InvalidProgram(_))
        ));
        let after = Control::default();
        assert_eq!(
            kernel.evaluate(Selection::all(0), &[], &after).unwrap_err(),
            KernelFailure::InstanceFailed
        );
        assert!(after.trace.lock().unwrap().is_empty());
    }
}
