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

use super::{ObservedControl, Work, gather};
use arrow::{
    array::{
        Array, ArrayRef, DictionaryArray, FixedSizeListArray, Int8Array, ListArray, StringArray,
    },
    datatypes::{Field, Int8Type},
};
use arrow_buffer::OffsetBuffer;
use novarocks_functions::{KernelDiagnostic, KernelEvaluationControl, KernelFailure};
use std::{
    sync::{Arc, Mutex, atomic::AtomicBool},
    time::Duration,
};

struct Control {
    stop_at: usize,
    cause: KernelFailure,
    trace: Mutex<Vec<u32>>,
    stopped: Mutex<bool>,
}
impl Control {
    fn new(stop_at: usize, cause: KernelFailure) -> Self {
        Self {
            stop_at,
            cause,
            trace: Mutex::new(vec![]),
            stopped: Mutex::new(false),
        }
    }
    fn accepting() -> Self {
        Self::new(usize::MAX, KernelFailure::Cancelled)
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        let mut stopped = self.stopped.lock().unwrap();
        assert!(
            !*stopped,
            "originating copy refusal must not trigger a second callback"
        );
        let mut trace = self.trace.lock().unwrap();
        trace.push(units);
        if trace.len() == self.stop_at {
            *stopped = true;
            Err(self.cause.clone())
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("selected copy does not wait")
    }
}
fn copy(
    array: &ArrayRef,
    indices: &[Option<u64>],
    control: &Control,
) -> Result<ArrayRef, KernelFailure> {
    let observed = ObservedControl {
        original: control,
        refused: AtomicBool::new(false),
        operation_aborted: AtomicBool::new(false),
    };
    let mut work = Work {
        control: &observed,
        pending: 0,
        scalar_scope: None,
    };
    let result = observed
        .checkpoint(0)
        .and_then(|()| gather(array, indices, &mut work));
    work.finish(result)
}
fn dictionary(length: usize) -> ArrayRef {
    let values = Arc::new(StringArray::from_iter_values(
        (0..length).map(|i| format!("word_{i}")),
    ));
    Arc::new(DictionaryArray::<Int8Type>::try_new(Int8Array::from(vec![0, 1, 2]), values).unwrap())
}
fn list(length: usize) -> ArrayRef {
    let values = dictionary(length);
    Arc::new(
        ListArray::try_new(
            Arc::new(Field::new("words", values.data_type().clone(), false)),
            OffsetBuffer::new(vec![0_i32, 1, 2, 3].into()),
            values,
            None,
        )
        .unwrap(),
    )
}

#[test]
fn nested_dictionary_128_is_refused_before_arrow_copy_for_values_and_all_null_indices() {
    let array = list(128);
    for indices in [
        vec![Some(2), Some(0)],
        vec![None, None, None],
        vec![None, Some(1)],
    ] {
        let result = copy(&array, &indices, &Control::accepting());
        assert!(matches!(result, Err(KernelFailure::ResourceExhausted)));
    }
}

#[test]
fn nested_dictionary_127_and_sliced_parent_keep_actual_keys_nulls_and_backing() {
    let array = list(127).slice(1, 1);
    let output = copy(&array, &[Some(0), None, Some(0)], &Control::accepting()).unwrap();
    let output = output.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(output.len(), 3);
    assert_eq!(output.null_count(), 1);
    assert!(output.is_null(1));
    assert_eq!(output.value_offsets(), &[0, 1, 1, 2]);
    for row in [0, 2] {
        let value = output.value(row);
        let value = value
            .as_any()
            .downcast_ref::<DictionaryArray<Int8Type>>()
            .unwrap();
        assert_eq!(value.keys().value(0), 1);
        let strings = value
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(strings.len(), 127);
        assert_eq!(strings.value(1), "word_1");
    }
    let nulls = copy(&array, &[None, None], &Control::accepting()).unwrap();
    assert_eq!(nulls.len(), 2);
    assert_eq!(nulls.null_count(), 2);
}

#[test]
fn direct_dictionary_128_retains_values_instead_of_applying_mutable_constructor_limit() {
    let array = dictionary(128);
    let output = copy(&array, &[Some(2), None, Some(0)], &Control::accepting()).unwrap();
    let source = array
        .as_any()
        .downcast_ref::<DictionaryArray<Int8Type>>()
        .unwrap();
    let output = output
        .as_any()
        .downcast_ref::<DictionaryArray<Int8Type>>()
        .unwrap();
    assert_eq!(output.keys().value(0), 2);
    assert!(output.is_null(1));
    assert_eq!(output.keys().value(2), 0);
    assert_eq!(output.values().len(), 128);
    assert!(Arc::ptr_eq(source.values(), output.values()));
    let nulls = copy(&array, &[None, None], &Control::accepting()).unwrap();
    assert_eq!(nulls.null_count(), 2);
}

#[test]
fn fixed_list_null_indices_still_construct_its_nested_list_dictionary() {
    let values = list(128);
    let array = Arc::new(
        FixedSizeListArray::try_new(
            Arc::new(Field::new("lists", values.data_type().clone(), true)),
            1,
            values,
            None,
        )
        .unwrap(),
    ) as ArrayRef;
    assert!(matches!(
        copy(&array, &[None, None], &Control::accepting()),
        Err(KernelFailure::ResourceExhausted)
    ));
    // Zero output bypasses the library copy constructor and remains legal.
    let empty = copy(&array, &[], &Control::accepting()).unwrap();
    assert_eq!(empty.len(), 0);
    assert_eq!(empty.data_type(), array.data_type());
}

fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(KernelDiagnostic::new("copy-origin-invalid")),
        KernelFailure::Internal(KernelDiagnostic::new("copy-origin-internal")),
        KernelFailure::Operational(KernelDiagnostic::new("copy-origin-operational")),
        KernelFailure::InstanceFailed,
    ]
}

#[test]
fn long_copy_preserves_every_control_callback_category_and_never_retries_refusal() {
    let array = list(127);
    let indices = (0..320)
        .map(|i| {
            if i % 3 == 0 {
                None
            } else {
                Some((i % 3) as u64)
            }
        })
        .collect::<Vec<_>>();
    let recorder = Control::accepting();
    let output = copy(&array, &indices, &recorder).unwrap();
    assert_eq!(output.len(), 320);
    let trace = recorder.trace.lock().unwrap().clone();
    assert!(
        trace.contains(&256),
        "actual index/range preflight must reach a positive quantum"
    );
    assert!(trace.iter().all(|units| *units <= 256));
    assert!(trace.iter().any(|units| *units > 0 && *units < 256));
    for stop_at in 1..=trace.len() {
        for cause in causes() {
            let control = Control::new(stop_at, cause.clone());
            let result = copy(&array, &indices, &control);
            assert!(
                matches!(result, Err(actual) if actual == cause),
                "copy callback {stop_at}"
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..stop_at]);
        }
    }
}

#[test]
fn capacity_failure_does_not_replace_an_originating_preflight_control_failure() {
    let array = list(128);
    let recorder = Control::accepting();
    assert!(matches!(
        copy(&array, &[None, None], &recorder),
        Err(KernelFailure::ResourceExhausted)
    ));
    let trace = recorder.trace.lock().unwrap().clone();
    for stop_at in 1..=trace.len() {
        for cause in causes() {
            let control = Control::new(stop_at, cause.clone());
            let result = copy(&array, &[None, None], &control);
            assert!(matches!(result, Err(actual) if actual == cause));
            assert_eq!(*control.trace.lock().unwrap(), trace[..stop_at]);
        }
    }
}
