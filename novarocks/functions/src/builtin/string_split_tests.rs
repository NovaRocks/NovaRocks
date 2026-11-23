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
use crate::kernel_control::{internal, invalid};
use crate::{
    EvaluatedArgument, FunctionValueType, KernelEvaluationControl, ScalarEvaluationInstance,
};
use std::sync::Mutex;
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
            assert!(at <= *stop, "callback after first refusal");
        }
        trace.push(units);
        match &self.refusal {
            Some((stop, cause)) if *stop == at => Err(cause.clone()),
            _ => Ok(()),
        }
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        panic!("string computation never waits")
    }
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        invalid("original invalid"),
        internal("original internal"),
        KernelFailure::Operational(crate::KernelDiagnostic::new("original operational")),
        KernelFailure::InstanceFailed,
    ]
}

use arrow_array::{Int64Array, ListArray, NullArray};
fn prepared() -> Arc<dyn crate::PreparedScalarKernel> {
    super::super::string_split_owner::tests::prepared_for_test(
        "split",
        &[
            FunctionValueType::new(DataType::Utf8, true),
            FunctionValueType::new(DataType::Utf8, true),
        ],
    )
    .unwrap()
}
#[test]
fn split_actual_instance_seven_causes_every_checkpoint_and_no_failed_replay() {
    let text = "éx:".repeat(80);
    let first = Arc::new(StringArray::from(vec![
        Some(text.as_str()),
        None,
        Some("a:b"),
        Some(""),
    ])) as ArrayRef;
    let second = Arc::new(StringArray::from(vec![
        Some(":"),
        Some(":"),
        Some(":"),
        Some(":"),
    ])) as ArrayRef;
    let args = [
        EvaluatedArgument::Column(&first),
        EvaluatedArgument::Column(&second),
    ];
    let rows = [0, 2, 3];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let prepared = prepared();
    let ok = Control::default();
    ScalarEvaluationInstance::instantiate(prepared.clone())
        .unwrap()
        .evaluate(selection, &args, &ok)
        .unwrap();
    let trace = ok.trace.lock().unwrap().clone();
    assert!(trace.len() > 256);
    for cause in causes() {
        for at in 0..trace.len() {
            let control = Control {
                trace: Mutex::new(vec![]),
                refusal: Some((at, cause.clone())),
            };
            let mut instance = ScalarEvaluationInstance::instantiate(prepared.clone()).unwrap();
            assert_eq!(
                instance.evaluate(selection, &args, &control).unwrap_err(),
                cause
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            assert_eq!(
                instance.evaluate(selection, &args, &control).unwrap_err(),
                KernelFailure::InstanceFailed
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
}
#[test]
fn split_selected_compact_and_scalar_origins_use_demanded_addresses_only() {
    let rows = [1, 3];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let compact = Arc::new(StringArray::from(vec![Some("é中"), None])) as ArrayRef;
    let selected =
        SelectedValues::try_new(selection, &DataType::Utf8, compact, Box::default()).unwrap();
    let scalar = Arc::new(StringArray::from(vec![Some("中")])) as ArrayRef;
    let args = [
        EvaluatedArgument::SelectedColumn(&selected),
        EvaluatedArgument::Scalar(&scalar),
    ];
    let out = ScalarEvaluationInstance::instantiate(prepared())
        .unwrap()
        .evaluate(selection, &args, &Control::default())
        .unwrap();
    let left = Arc::new(StringArray::from(vec![Some("é中"), None])) as ArrayRef;
    let right = Arc::new(StringArray::from(vec![Some("中"), Some("中")])) as ArrayRef;
    let expected = evaluate_required(&left, &right).unwrap();
    assert_eq!(out.values().to_data(), expected.to_data());
    assert!(out.errors().is_empty());
    let source = Arc::new(StringArray::from(vec![
        "inactive", "a:b", "inactive", "c:d",
    ])) as ArrayRef;
    let delimiter = Arc::new(StringArray::from(vec![Some(":"), Some(":")])) as ArrayRef;
    let selected =
        SelectedValues::try_new(selection, &DataType::Utf8, delimiter, Box::default()).unwrap();
    let args = [
        EvaluatedArgument::Column(&source),
        EvaluatedArgument::SelectedColumn(&selected),
    ];
    let out = ScalarEvaluationInstance::instantiate(prepared())
        .unwrap()
        .evaluate(selection, &args, &Control::default())
        .unwrap();
    let list = out.values().as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(list.len(), 2);
    for (i, want) in [
        (0, vec![Some("a"), Some("b")]),
        (1, vec![Some("c"), Some("d")]),
    ] {
        let a = list.value(i);
        assert_eq!(
            a.as_any().downcast_ref::<StringArray>().unwrap(),
            &StringArray::from(want)
        );
    }
}
#[test]
fn split_shared_raw_exact_admission_and_batch_length_errors_remain_full() {
    let left = Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef;
    let right = Arc::new(StringArray::from(vec![","])) as ArrayRef;
    assert_eq!(
        evaluate_required(&left, &right).unwrap_err().to_string(),
        "split: argument length mismatch"
    );
    let invalid = Arc::new(Int64Array::from(vec![None; 2])) as ArrayRef;
    assert_eq!(
        evaluate_required(&invalid, &right).unwrap_err().to_string(),
        "split: first argument must be a string array"
    );
    assert_eq!(
        evaluate_required(&left, &(Arc::new(NullArray::new(2)) as ArrayRef))
            .unwrap_err()
            .to_string(),
        "split: second argument must be a string array"
    );
}

#[test]
fn split_failure_is_nominal_and_extent_refusal_does_not_append_a_partial_piece() {
    use arrow_array::builder::ArrayBuilder;
    use novarocks_failure_contract::{FailureClass, FailureProjection, RepresentationBoundary};
    use std::error::Error;
    let input: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    let weak = Arc::downgrade(&input);
    let delimiter: ArrayRef = Arc::new(StringArray::from(vec![","]));
    let failure = evaluate_required(&input, &delimiter).unwrap_err();
    drop(input);
    assert!(weak.upgrade().is_none());
    assert_eq!(failure.failure_fact().class(), FailureClass::Invariant);
    assert_eq!(failure.failure_fact().reason().code(), 368);
    assert!(
        failure
            .source()
            .unwrap()
            .source()
            .unwrap()
            .is::<SplitContract>()
    );
    for (mut row, mut children, mut bytes, boundary, layout) in [
        (
            i32::MAX as usize,
            0,
            0,
            RepresentationBoundary::MinimumUnit,
            63,
        ),
        (0, i32::MAX, 0, RepresentationBoundary::SingleOutput, 63),
        (0, 0, i32::MAX, RepresentationBoundary::SingleOutput, 60),
    ] {
        let before = (row, children, bytes);
        let mut builder = ListBuilder::new(StringBuilder::new());
        let failure =
            append_piece(&mut builder, "x", &mut row, &mut children, &mut bytes).unwrap_err();
        assert_eq!((row, children, bytes), before);
        assert_eq!(builder.values().len(), 0);
        let fact = failure.failure_fact();
        assert_eq!(fact.class(), FailureClass::Representation);
        assert!(fact.resource().is_none());
        let extent = fact.representation().unwrap();
        assert_eq!(extent.boundary, boundary);
        assert_eq!(extent.layout.code.get(), layout);
        assert_eq!(extent.representable_prefix, None);
    }
}
