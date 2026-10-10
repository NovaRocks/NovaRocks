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

//! This exercises the one comparison caller, including existing consumers;
//! no alternative comparison algorithm or result builder is introduced.
use super::*;
use arrow::array::Int64Array;
use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
use std::sync::Mutex;
struct Compile;
impl PureCompileControl for Compile {
    fn checkpoint(&self, _: CompilePhase, n: u32) -> Result<(), CompileControlError> {
        assert!(n <= 256);
        Ok(())
    }
}
struct Control {
    trace: Mutex<Vec<u32>>,
    stop: usize,
    cause: KernelFailure,
}
impl Control {
    fn new(stop: usize, cause: KernelFailure) -> Self {
        Self {
            trace: Mutex::new(vec![]),
            stop,
            cause,
        }
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, n: u32) -> Result<(), KernelFailure> {
        assert!(n <= 256);
        let mut t = self.trace.lock().unwrap();
        assert!(
            t.len() < self.stop,
            "no callback after first originating refusal"
        );
        t.push(n);
        if t.len() == self.stop {
            Err(self.cause.clone())
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("comparison never waits")
    }
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(KernelDiagnostic::new("comparison-origin-invalid")),
        KernelFailure::Internal(KernelDiagnostic::new("comparison-origin-internal")),
        KernelFailure::Operational(KernelDiagnostic::new("comparison-origin-operational")),
        KernelFailure::InstanceFailed,
    ]
}
fn run<'a>(
    left: &Value<'_>,
    right: &Value<'_>,
    selection: Selection<'a>,
    nullsafe: bool,
    c: &Control,
) -> Result<SelectedValues<'a>, KernelFailure> {
    let ty = novarocks_functions::FunctionValueType::new(DataType::Int64, true);
    let ordinary = novarocks_functions::PreparedComparisonRecipe::try_new(
        novarocks_functions::ComparisonOperator::Le,
        &ty,
        &ty,
        &Compile,
    )
    .unwrap();
    let null =
        novarocks_functions::PreparedNullSafeComparisonRecipe::try_new(&ty, &ty, &Compile).unwrap();
    let observed = ObservedControl {
        original: c,
        refused: AtomicBool::new(false),
        operation_aborted: AtomicBool::new(false),
    };
    let mut work = Work {
        control: &observed,
        pending: 0,
        scalar_scope: None,
    };
    let result = evaluate_comparison(
        if nullsafe {
            ComparisonRecipe::NullSafe(&null)
        } else {
            ComparisonRecipe::Ordinary(&ordinary)
        },
        left,
        right,
        selection,
        &mut work,
    );
    work.finish(result)
}
#[test]
fn between_comparison_control_success_trace_matches_original_author_without_extra_callbacks() {
    let ty = novarocks_functions::FunctionValueType::new(DataType::Int64, true);
    let recipe = novarocks_functions::PreparedComparisonRecipe::try_new(
        novarocks_functions::ComparisonOperator::Le,
        &ty,
        &ty,
        &Compile,
    )
    .unwrap();
    let l: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None, Some(9)]));
    let r: ArrayRef = Arc::new(Int64Array::from(vec![Some(5), Some(5), Some(5)]));
    for row in 0..3 {
        let old = Control::new(usize::MAX, KernelFailure::Cancelled);
        let before = recipe
            .compare_rows(
                EvaluatedArgument::Column(&l),
                row,
                row,
                EvaluatedArgument::Column(&r),
                row,
                row,
                &old,
            )
            .unwrap();
        let new = Control::new(usize::MAX, KernelFailure::Cancelled);
        let observation = novarocks_functions::KernelControlObservation::new(&new);
        let after = recipe
            .compare_rows(
                EvaluatedArgument::Column(&l),
                row,
                row,
                EvaluatedArgument::Column(&r),
                row,
                row,
                &observation,
            )
            .unwrap();
        assert_eq!(before, after);
        assert_eq!(*old.trace.lock().unwrap(), *new.trace.lock().unwrap());
    }
}
#[test]
fn between_comparison_control_every_callback_seven_causes_original_and_nullsafe_consumers() {
    let left = Value::Column(Arc::new(Int64Array::from(vec![Some(1), None, Some(9)])));
    let right = Value::Column(Arc::new(Int64Array::from(vec![Some(5), Some(5), Some(5)])));
    for nullsafe in [false, true] {
        for rows in [vec![0, 1, 2], vec![0, 2], Vec::new()] {
            let selection = Selection::try_sparse(3, &rows).unwrap();
            let record = Control::new(usize::MAX, KernelFailure::Cancelled);
            run(&left, &right, selection, nullsafe, &record).unwrap();
            let trace = record.trace.lock().unwrap().clone();
            assert!(!trace.is_empty());
            for stop in 1..=trace.len() {
                for cause in causes() {
                    let c = Control::new(stop, cause.clone());
                    assert!(
                        matches!(run(&left,&right,selection,nullsafe,&c),Err(actual) if actual==cause)
                    );
                    assert_eq!(*c.trace.lock().unwrap(), trace[..stop]);
                }
            }
        }
    }
}
#[test]
fn between_comparison_control_row_data_and_legal_boolean_mask_remain_original() {
    let rows = vec![0, 2];
    let selection = Selection::try_sparse(3, &rows).unwrap();
    let array: ArrayRef = Arc::new(Int64Array::from(vec![None, Some(9)]));
    let left = Value::Selected(
        SelectedValues::try_new(
            selection,
            &DataType::Int64,
            array,
            vec![RowDataError::new(0, "original comparison child data")].into_boxed_slice(),
        )
        .unwrap(),
    );
    let right = Value::Column(Arc::new(Int64Array::from(vec![Some(5); 3])));
    let c = Control::new(usize::MAX, KernelFailure::Cancelled);
    let output = run(&left, &right, selection, false, &c).unwrap();
    assert_eq!(output.errors().len(), 1);
    assert_eq!(
        output.errors()[0].message(),
        "original comparison child data"
    );
    assert_eq!(
        output
            .values()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![None, Some(false)]
    );
    let observed = ObservedControl {
        original: &c,
        refused: AtomicBool::new(false),
        operation_aborted: AtomicBool::new(false),
    };
    let mut w = Work {
        control: &observed,
        pending: 0,
        scalar_scope: None,
    };
    let mut state = BooleanRows::new(2, &mut w).unwrap();
    let mut errors = BTreeMap::new();
    state
        .consume(
            &output,
            &[0, 1],
            ControlShape::Conjunction,
            EvaluationDemand::Value,
            true,
            &mut errors,
            &mut w,
        )
        .unwrap();
    let dominating = SelectedValues::try_new(
        selection,
        &DataType::Boolean,
        Arc::new(BooleanArray::from(vec![false, false])),
        Box::default(),
    )
    .unwrap();
    state
        .consume(
            &dominating,
            &[0, 1],
            ControlShape::Conjunction,
            EvaluationDemand::Value,
            true,
            &mut errors,
            &mut w,
        )
        .unwrap();
    let (array, errors) = state
        .finish(
            ControlShape::Conjunction,
            EvaluationDemand::Value,
            errors,
            &mut w,
        )
        .unwrap();
    assert!(errors.is_empty());
    assert_eq!(
        array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(false), Some(false)]
    );
}
