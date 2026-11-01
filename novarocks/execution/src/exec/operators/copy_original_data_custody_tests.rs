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

//! Actual original Data retains its full diagnostic backing under the same
//! production host until real Drop. These probes do not register public BY.
use super::*;
use arrow::buffer::NullBuffer;

fn bad_run() -> (ArrayRef, Arc<UInt64Array>) {
    let source: ArrayRef = Arc::new(
        RunArray::<Int16Type>::try_new(
            &Int16Array::from(vec![1, 3]),
            &Int32Array::from(vec![10, 20]),
        )
        .unwrap(),
    );
    let indices = Arc::new(UInt64Array::new(
        vec![u64::MAX, 0].into(),
        Some(NullBuffer::from(vec![true, true])),
    ));
    (source, indices)
}
fn error_with(host: Arc<Host>, control: &Control) -> CopyOperationError {
    let (source, indices) = bad_run();
    match take_copy_in(source, CopyIndices::UInt64(indices), (), host, control) {
        Err(error) => error,
        Ok(_) => panic!("original Run valid out-of-bounds index must return whole Data"),
    }
}

#[test]
fn by_copy_run_hidden_max_is_null_and_keeps_real_output_custody() {
    let (source, _) = bad_run();
    let indices = Arc::new(UInt64Array::new(
        vec![u64::MAX, 0].into(),
        Some(NullBuffer::from(vec![false, true])),
    ));
    let original = arrow::compute::take(source.as_ref(), indices.as_ref(), None).unwrap();
    let tr = tracker();
    let host = Host::new(tr.clone(), None);
    let control = Control::new(tr, None);
    let actual = take_copy_in(
        source,
        CopyIndices::UInt64(indices),
        (),
        host.clone(),
        &control,
    )
    .unwrap();
    assert_eq!(actual.values().to_data(), original.to_data());
    let run = actual
        .values()
        .as_any()
        .downcast_ref::<RunArray<Int16Type>>()
        .unwrap();
    assert_eq!(run.len(), 2);
    let values = run.values().as_any().downcast_ref::<Int32Array>().unwrap();
    let first = run.get_physical_index(0);
    let second = run.get_physical_index(1);
    assert!(values.is_null(first));
    assert!(values.is_valid(second));
    assert_eq!(values.value(second), 10);
    drop(actual);
    assert_released(&host);
}

#[test]
fn by_copy_original_data_full_valid_max_text_is_charged_until_real_drop() {
    let (source, indices) = bad_run();
    let original = arrow::compute::take(source.as_ref(), indices.as_ref(), None)
        .unwrap_err()
        .to_string();
    let tr = tracker();
    let host = Host::new(tr.clone(), None);
    let control = Control::new(tr.clone(), None);
    let error = error_with(host.clone(), &control);
    let CopyOperationError::OriginalData(text) = &error else {
        panic!("original Data changed: {error:?}")
    };
    assert_eq!(text.text(), original);
    assert!(text.retained_bytes() >= original.len());
    assert_eq!(tr.current(), i64::try_from(text.retained_bytes()).unwrap());
    assert!(
        host.events()
            .iter()
            .any(|e| matches!(e, Event::OpaqueAttempt(n) if *n >= text.retained_bytes()))
    );
    drop(error);
    assert_released(&host);
}

#[test]
fn by_copy_original_data_each_actual_host_fault_retains_typed_primary_no_retry() {
    let tr = tracker();
    let base = Host::new(tr.clone(), None);
    let control = Control::new(tr, None);
    let error = error_with(base.clone(), &control);
    assert!(matches!(error, CopyOperationError::OriginalData(_)));
    drop(error);
    let attempts = base
        .events()
        .iter()
        .filter(|e| matches!(e, Event::Attempt(_) | Event::OpaqueAttempt(_)))
        .count();
    assert_released(&base);
    for stop in 0..attempts {
        for cause in causes() {
            let tr = tracker();
            let host = Host::new(tr.clone(), Some((stop, cause.clone())));
            let control = Control::new(tr, None);
            let actual = error_with(host.clone(), &control);
            match actual {
                CopyOperationError::Control(actual) => assert_eq!(actual, cause),
                other => panic!("first actual host fault changed: {other:?}"),
            }
            assert_eq!(
                host.events()
                    .iter()
                    .filter(|e| matches!(e, Event::Attempt(_) | Event::OpaqueAttempt(_)))
                    .count(),
                stop + 1
            );
            assert_released(&host);
        }
    }
}

#[test]
fn by_copy_original_data_each_control_cause_has_exact_prefix_and_no_footer() {
    let tr = tracker();
    let base = Host::new(tr.clone(), None);
    let control = Control::new(tr, None);
    let error = error_with(base.clone(), &control);
    assert!(matches!(error, CopyOperationError::OriginalData(_)));
    drop(error);
    let trace = control.trace().iter().map(|(u, _)| *u).collect::<Vec<_>>();
    assert_released(&base);
    for stop in 0..trace.len() {
        for cause in causes() {
            let tr = tracker();
            let host = Host::new(tr.clone(), None);
            let control = Control::new(tr, Some((stop, cause.clone())));
            let error = error_with(host.clone(), &control);
            match error {
                CopyOperationError::Control(actual) => assert_eq!(actual, cause),
                other => panic!("first control cause changed: {other:?}"),
            }
            assert_eq!(
                control.trace().iter().map(|(u, _)| *u).collect::<Vec<_>>(),
                trace[..=stop]
            );
            assert_released(&host);
        }
    }
}

#[test]
fn by_copy_original_data_failure_drops_input_refs_before_their_original_owner() {
    struct Owner {
        source: std::sync::Weak<dyn Array>,
        indices: std::sync::Weak<UInt64Array>,
        drops: Arc<AtomicUsize>,
    }
    impl Drop for Owner {
        fn drop(&mut self) {
            assert!(self.source.upgrade().is_none());
            assert!(self.indices.upgrade().is_none());
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }
    let (source, indices) = bad_run();
    let drops = Arc::new(AtomicUsize::new(0));
    let owner = Owner {
        source: Arc::downgrade(&source),
        indices: Arc::downgrade(&indices),
        drops: drops.clone(),
    };
    let tr = tracker();
    let host = Host::new(tr.clone(), None);
    let control = Control::new(tr, None);
    let error = take_copy_in(
        source,
        CopyIndices::UInt64(indices),
        owner,
        host.clone(),
        &control,
    )
    .err()
    .expect("original Data");
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(
        host.tracker.current() > 0,
        "only actual returned diagnostic remains live"
    );
    drop(error);
    assert_released(&host);
}

#[test]
fn by_copy_original_data_fixed_list_generated_null_zero_keeps_run_child_payload() {
    use arrow::array::FixedSizeListArray;
    let values = StringArray::from(vec![
        "original hidden payload".repeat(101),
        "second".to_owned(),
    ]);
    let run: ArrayRef = Arc::new(
        RunArray::<Int16Type>::try_new(&Int16Array::from(vec![2_i16, 4]), &values).unwrap(),
    );
    let source: ArrayRef = Arc::new(
        FixedSizeListArray::try_new(
            Arc::new(Field::new("real_runs", run.data_type().clone(), true)),
            2,
            run,
            None,
        )
        .unwrap(),
    );
    let indices = Arc::new(UInt32Array::new(
        vec![99_u32, 1].into(),
        Some(NullBuffer::from(vec![false, true])),
    ));
    let original = arrow::compute::take(source.as_ref(), indices.as_ref(), None).unwrap();
    let tr = tracker();
    let host = Host::new(tr.clone(), None);
    let control = Control::new(tr, None);
    let actual = take_copy_in(
        source,
        CopyIndices::UInt32(indices),
        (),
        host.clone(),
        &control,
    )
    .unwrap();
    assert_eq!(actual.values().to_data(), original.to_data());
    drop(actual);
    assert_released(&host);
}

#[test]
fn by_copy_null_empty_run_under_fixed_list_skips_hidden_index_and_keeps_real_custody() {
    use arrow::array::FixedSizeListArray;
    let run: ArrayRef = Arc::new(
        RunArray::<Int16Type>::try_new(
            &Int16Array::from(Vec::<i16>::new()),
            &StringArray::from(Vec::<&str>::new()),
        )
        .unwrap(),
    );
    let source: ArrayRef = Arc::new(
        FixedSizeListArray::try_new(
            Arc::new(Field::new("real_runs", run.data_type().clone(), true)),
            2,
            run,
            None,
        )
        .unwrap(),
    );
    let indices = Arc::new(UInt32Array::new(
        vec![99_u32].into(),
        Some(NullBuffer::from(vec![false])),
    ));
    let original = arrow::compute::take(source.as_ref(), indices.as_ref(), None).unwrap();
    assert_eq!(original.len(), 1);
    assert!(original.is_null(0));
    let tr = tracker();
    let host = Host::new(tr.clone(), None);
    let control = Control::new(tr, None);
    let actual = take_copy_in(
        source,
        CopyIndices::UInt32(indices),
        (),
        host.clone(),
        &control,
    )
    .unwrap();
    assert_eq!(actual.values().to_data(), original.to_data());
    assert_eq!(actual.values().len(), 1);
    assert!(actual.values().is_null(0));
    let list = actual
        .values()
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .unwrap();
    assert_eq!(list.value_length(), 2);
    assert_eq!(list.values().len(), 2);
    let child = list
        .values()
        .as_any()
        .downcast_ref::<RunArray<Int16Type>>()
        .unwrap();
    let strings = child
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    for row in 0..child.len() {
        assert!(strings.is_null(child.get_physical_index(row)));
    }
    drop(actual);
    assert_released(&host);
}
