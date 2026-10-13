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

//! The original recursive aggregate scalar reader and output builder, shared
//! by legacy shells and pure owners through their exact state allocator.
//! Explicit control witnesses; this module does not prove output funding.
use super::*;
use crate::kernel_control::{internal, invalid};
use crate::{KernelEvaluationControl, KernelFailure};
use arrow_schema::Field;
use std::{collections::HashMap, sync::Mutex, time::Duration};

#[derive(Default)]
struct Control {
    calls: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        let mut calls = self.calls.lock().unwrap();
        let at = calls.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop, "callback after refusal");
        }
        calls.push(units);
        assert!(units <= 256);
        match &self.refusal {
            Some((stop, cause)) if *stop == at => Err(cause.clone()),
            _ => Ok(()),
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("scalar builder never waits")
    }
}
fn fixed() -> DataType {
    DataType::FixedSizeBinary(16)
}
fn item(name: &str, nullable: bool) -> Field {
    Field::new(name, fixed(), nullable).with_metadata(HashMap::from([(
        "nr_logical_type".into(),
        "largeint".into(),
    )]))
}
fn flat_values() -> Vec<Option<AggScalarValue>> {
    vec![
        Some(AggScalarValue::Decimal128(i128::MIN)),
        None,
        Some(AggScalarValue::Decimal128(i128::MAX)),
    ]
}
fn build(
    ty: &DataType,
    values: Vec<Option<AggScalarValue>>,
    control: &Control,
    observed: bool,
) -> Result<ArrayRef, ScalarStateError> {
    let mut checkpoints = EvaluationCheckpoints::new(control);
    let mut work = ScalarWork::new(Some(&mut checkpoints));
    if observed {
        build_scalar_array_with_largeint_observation(ty, values, &mut work)
    } else {
        build_scalar_array(ty, values, &mut work)
    }
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        invalid("original refusal"),
        internal("original refusal"),
        KernelFailure::Operational(KernelDiagnostic::new("original refusal")),
        KernelFailure::InstanceFailed,
    ]
}
#[test]
fn scalar_largeint_default_trace_stays_unobserved_and_new_body_keeps_bytes_and_nulls() {
    let old = Control::default();
    let new = Control::default();
    let a = build(&fixed(), flat_values(), &old, false).unwrap();
    let b = build(&fixed(), flat_values(), &new, true).unwrap();
    assert!(old.calls.lock().unwrap().is_empty());
    assert_eq!(*new.calls.lock().unwrap(), [3, 0, 3, 0]);
    assert_eq!(a.to_data(), b.to_data());
    let b = b.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
    assert_eq!(b.value(0), i128::MIN.to_be_bytes());
    assert!(b.is_null(1));
    assert_eq!(b.value(2), i128::MAX.to_be_bytes());
    let raw = build_scalar_array(&fixed(), flat_values(), &mut ScalarWork::new(None)).unwrap();
    assert_eq!(a.to_data(), raw.to_data());
}
#[test]
fn scalar_largeint_observation_reaches_list_struct_and_both_map_children() {
    for shape in 0..3 {
        let ty = match shape {
            0 => DataType::List(Arc::new(item("item", true))),
            1 => DataType::Struct(vec![item("field", true)].into()),
            _ => DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(vec![item("key", false), item("value", true)].into()),
                    false,
                )),
                false,
            ),
        };
        let values = || {
            vec![Some(match shape {
                0 => AggScalarValue::List(flat_values()),
                1 => AggScalarValue::Struct(vec![Some(AggScalarValue::Decimal128(-1))]),
                _ => AggScalarValue::Map(vec![(Some(AggScalarValue::Decimal128(-1)), None)]),
            })]
        };
        let old = Control::default();
        let new = Control::default();
        let a = build(&ty, values(), &old, false).unwrap();
        let b = build(&ty, values(), &new, true).unwrap();
        assert!(novarocks_type_contract::arrow_data_types_exact(
            b.data_type(),
            &ty
        ));
        assert_eq!(a.to_data(), b.to_data());
        let old_count = old.calls.lock().unwrap().len();
        let new_count = new.calls.lock().unwrap().len();
        assert_eq!(new_count - old_count, if shape == 2 { 8 } else { 4 });
    }
}
#[test]
fn scalar_largeint_empty_builder_has_four_real_boundaries() {
    for ty in [fixed(), DataType::List(Arc::new(item("item", true)))] {
        let old = Control::default();
        let new = Control::default();
        let a = build(&ty, vec![], &old, false).unwrap();
        let b = build(&ty, vec![], &new, true).unwrap();
        assert_eq!(a.to_data(), b.to_data());
        assert!(old.calls.lock().unwrap().is_empty());
        assert_eq!(*new.calls.lock().unwrap(), [0, 0, 0, 0]);
    }
}
#[test]
fn scalar_largeint_each_builder_boundary_and_quantum_preserves_all_typed_causes() {
    let values = || {
        (0..513)
            .map(|v| Some(AggScalarValue::Decimal128(v)))
            .collect::<Vec<_>>()
    };
    let success = Control::default();
    build(&fixed(), values(), &success, true).unwrap();
    let prefix = success.calls.into_inner().unwrap();
    assert_eq!(prefix, [256, 256, 1, 0, 256, 256, 1, 0]);
    for cause in causes() {
        for stop in 0..prefix.len() {
            let control = Control {
                refusal: Some((stop, cause.clone())),
                ..Control::default()
            };
            let error = build(&fixed(), values(), &control, true).unwrap_err();
            assert!(matches!(error, ScalarStateError::Kernel(actual) if actual == cause));
            assert_eq!(*control.calls.lock().unwrap(), prefix[..=stop]);
        }
    }
}
#[test]
fn scalar_largeint_mismatch_stays_original_data_before_builder_and_control_is_required() {
    let values = || vec![Some(AggScalarValue::Utf8("original".into()))];
    for observed in [false, true] {
        let control = Control::default();
        let error = build(&fixed(), values(), &control, observed).unwrap_err();
        assert!(
            matches!(error, ScalarStateError::Legacy(message) if message == "scalar output type mismatch for LargeInt")
        );
        assert!(control.calls.lock().unwrap().is_empty());
    }
    let error =
        build_scalar_array_with_largeint_observation(&fixed(), vec![], &mut ScalarWork::new(None))
            .unwrap_err();
    assert!(
        matches!(error, ScalarStateError::Kernel(KernelFailure::InvalidProgram(diagnostic))
        if diagnostic.message() == "observed LARGEINT scalar builder requires evaluation control")
    );
}
