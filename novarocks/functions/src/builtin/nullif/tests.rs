// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.
use super::*;
use arrow_array::{Int64Array, TimestampSecondArray, new_null_array};
use arrow_schema::Field;
use std::sync::atomic::{AtomicUsize, Ordering};

fn run(
    left: EvaluatedArgument<'_>,
    right: EvaluatedArgument<'_>,
    selection: Selection<'_>,
    control: &dyn KernelEvaluationControl,
) -> Result<ArrayRef, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = evaluate_values(left, right, selection, &mut work);
    work.finish_result(result)
}
#[test]
fn selected_nullif_sparse_and_scalar_preserve_original_rows_and_right_null() {
    let left: ArrayRef = Arc::new(Int64Array::from(vec![
        Some(99),
        Some(1),
        None,
        Some(3),
        Some(5),
        Some(6),
    ]));
    let right: ArrayRef = Arc::new(Int64Array::from(vec![Some(1)]));
    let selection = Selection::try_sparse(6, &[1, 2, 4, 5]).unwrap();
    let output = run(
        EvaluatedArgument::Column(&left),
        EvaluatedArgument::Scalar(&right),
        selection,
        &LegacyControl,
    )
    .unwrap();
    assert_eq!(
        output.to_data(),
        Int64Array::from(vec![None, None, Some(5), Some(6)]).to_data()
    );
    let right: ArrayRef = Arc::new(Int64Array::from(vec![None::<i64>]));
    let output = run(
        EvaluatedArgument::Column(&left),
        EvaluatedArgument::Scalar(&right),
        selection,
        &LegacyControl,
    )
    .unwrap();
    assert_eq!(
        output.to_data(),
        Int64Array::from(vec![Some(1), None, Some(5), Some(6)]).to_data()
    );
}
#[test]
fn selected_nullif_compact_utf8_and_empty_keep_source_identity() {
    let selection = Selection::try_sparse(9, &[1, 5, 8]).unwrap();
    let left: ArrayRef = Arc::new(
        StringArray::from(vec![Some("prefix"), Some("中"), None, Some("retain")]).slice(1, 3),
    );
    let right: ArrayRef = Arc::new(StringArray::from(vec![Some("中"), Some("different"), None]));
    let compact =
        SelectedValues::try_new(selection, &DataType::Utf8, left.clone(), Box::default()).unwrap();
    let other = SelectedValues::try_new(selection, &DataType::Utf8, right, Box::default()).unwrap();
    let output = run(
        EvaluatedArgument::SelectedColumn(&compact),
        EvaluatedArgument::SelectedColumn(&other),
        selection,
        &LegacyControl,
    )
    .unwrap();
    assert_eq!(
        output.to_data(),
        StringArray::from(vec![None, None, Some("retain")]).to_data()
    );
    let empty = Selection::try_sparse(9, &[]).unwrap();
    let output = run(
        EvaluatedArgument::Column(&left),
        EvaluatedArgument::Scalar(&left),
        empty,
        &LegacyControl,
    )
    .unwrap();
    assert_eq!(output.len(), 0);
    assert_eq!(output.data_type(), &DataType::Utf8);
}
#[test]
fn selected_nullif_bounded_contract_error_and_existing_timezone_values() {
    let ty = DataType::FixedSizeList(
        Arc::new(Field::new("x".repeat(900), DataType::Int32, true)),
        1,
    );
    let input = new_null_array(&ty, 2);
    let actual = evaluate_legacy(input.clone(), input, &ty).unwrap_err();
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    assert_eq!(actual.failure_fact().class(), FailureClass::Invariant);
    assert_eq!(
        actual.to_string(),
        novarocks_failure_contract::BoundedDetail::truncating_display(format_args!(
            "nullif unsupported type: {ty:?}"
        ))
        .as_str()
    );
    assert!(actual.to_string().len() <= 512);
    let left: ArrayRef =
        Arc::new(TimestampSecondArray::from(vec![Some(1), Some(2)]).with_timezone("UTC"));
    let right: ArrayRef =
        Arc::new(TimestampSecondArray::from(vec![Some(1), None]).with_timezone("UTC"));
    let output = evaluate_legacy(left.clone(), right, left.data_type()).unwrap();
    assert_eq!(
        output.to_data(),
        TimestampSecondArray::from(vec![None, Some(2)])
            .with_timezone("UTC")
            .to_data()
    );
}
struct RefusingControl {
    calls: AtomicUsize,
    refuse: usize,
}
impl KernelEvaluationControl for RefusingControl {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        let call = self.calls.fetch_add(1, Ordering::Relaxed) + 1;
        if call == self.refuse {
            Err(invalid("first nullif control refusal"))
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        unreachable!()
    }
}
#[test]
fn selected_nullif_every_checkpoint_refusal_is_primary_and_stops_work() {
    let long = "中".repeat(1200);
    let left: ArrayRef = Arc::new(StringArray::from(vec![
        Some(long.as_str()),
        Some("a"),
        None,
    ]));
    let right: ArrayRef = Arc::new(StringArray::from(vec![
        Some(long.as_str()),
        None,
        Some("b"),
    ]));
    let control = RefusingControl {
        calls: AtomicUsize::new(0),
        refuse: usize::MAX,
    };
    let output = run(
        EvaluatedArgument::Column(&left),
        EvaluatedArgument::Column(&right),
        Selection::all(3),
        &control,
    )
    .unwrap();
    assert_eq!(
        output.to_data(),
        StringArray::from(vec![None, Some("a"), None]).to_data()
    );
    let calls = control.calls.load(Ordering::Relaxed);
    assert!(calls > 10);
    for refuse in 1..=calls {
        let control = RefusingControl {
            calls: AtomicUsize::new(0),
            refuse,
        };
        let error = run(
            EvaluatedArgument::Column(&left),
            EvaluatedArgument::Column(&right),
            Selection::all(3),
            &control,
        )
        .unwrap_err();
        assert_eq!(error, invalid("first nullif control refusal"));
        assert_eq!(control.calls.load(Ordering::Relaxed), refuse);
    }
}

#[test]
fn nullif_frozen_null_result_has_no_guessed_physical_carrier() {
    let nulls: ArrayRef = Arc::new(arrow_array::NullArray::new(3));
    let result = evaluate_legacy(nulls.clone(), nulls, &DataType::Null).unwrap();
    assert_eq!(result.data_type(), &DataType::Null);
    assert_eq!(result.len(), 3);
}
