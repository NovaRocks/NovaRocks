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
use crate::KernelFailure;
use novarocks_failure_contract::{FailureClass, FailureProjection};
use novarocks_type_contract::ValueTypeStorageLifetime;
use std::{
    cell::Cell,
    sync::atomic::{AtomicUsize, Ordering},
};

struct Control {
    calls: AtomicUsize,
    stop_at: Option<usize>,
}
impl Control {
    fn new(stop_at: Option<usize>) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            stop_at,
        }
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= crate::MAX_UNOBSERVED_KERNEL_WORK);
        let calls = self.calls.fetch_add(1, Ordering::Relaxed) + 1;
        if self.stop_at == Some(calls) {
            Err(KernelFailure::DeadlineExceeded)
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        panic!("selected-copy construction must never wait")
    }
}
fn source(width: i32, payload: Vec<u8>, validity: Option<Vec<bool>>) -> ArrayRef {
    Arc::new(
        FixedSizeBinaryArray::try_new(
            width,
            Buffer::from_vec(payload),
            validity.map(NullBuffer::from),
        )
        .unwrap(),
    )
}
fn attempt<H>(
    sources: &[ArrayRef],
    choices: &[(usize, usize)],
    control: &dyn KernelEvaluationControl,
    request: &mut dyn FnMut(Layout) -> Result<(), H>,
    opaque: &mut dyn FnMut(usize) -> Result<(), H>,
) -> Result<ArrayRef, CopyConstructionError<H>> {
    let mut creation = ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), request, opaque);
    // Exercise the public original construction entry rather than a K copy.
    crate::selected_copy::copy_nested_values_construction(sources, choices, &mut creation, control)
}
fn copied(sources: &[ArrayRef], choices: &[(usize, usize)]) -> ArrayRef {
    attempt(
        sources,
        choices,
        &Control::new(None),
        &mut |_| Ok::<_, ()>(()),
        &mut |_| Ok(()),
    )
    .unwrap()
}

#[test]
fn fixed_copy_preserves_selected_order_hidden_null_bytes_and_detaches_source() {
    let original = source(
        3,
        b"aaaxxxcccddd".to_vec(),
        Some(vec![true, false, true, true]),
    );
    let sliced = original.slice(1, 3);
    let other = source(3, b"EEEFFF".to_vec(), None);
    let original_weak = Arc::downgrade(&original);
    let sliced_weak = Arc::downgrade(&sliced);
    let other_weak = Arc::downgrade(&other);
    let pointer = sliced
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap()
        .values()
        .as_ptr();
    let sources = vec![sliced, other];
    let output = copied(&sources, &[(0, 2), (1, 0), (0, 0), (0, 1), (1, 0)]);
    let array = output
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(array.value_length(), 3);
    assert_eq!(array.len(), 5);
    assert_ne!(array.values().as_ptr(), pointer);
    assert_eq!(array.value_data(), b"dddEEExxxcccEEE");
    assert_eq!(
        (0..5).map(|i| array.is_valid(i)).collect::<Vec<_>>(),
        [true, true, false, true, true]
    );
    let alias = output.slice(1, 3);
    drop(output);
    drop(sources);
    drop(original);
    assert!(original_weak.upgrade().is_none());
    assert!(sliced_weak.upgrade().is_none());
    assert!(other_weak.upgrade().is_none());
    let array = alias
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(array.len(), 3);
    assert_eq!(array.value(0), b"EEE");
    assert!(array.is_null(1));
    assert_eq!(array.value(1), b"xxx");
    assert_eq!(array.value(2), b"ccc");
}

#[test]
fn zero_width_copy_keeps_row_count_nulls_repetitions_and_empty_selection() {
    for validity in [vec![true, true, true], vec![true, false, true]] {
        let sources = vec![source(0, vec![], Some(validity.clone()))];
        let output = copied(&sources, &[(0, 2), (0, 1), (0, 2), (0, 0), (0, 1)]);
        let array = output
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(array.len(), 5);
        assert_eq!(array.value_length(), 0);
        assert!(array.values().is_empty());
        assert_eq!(
            (0..5).map(|i| array.is_valid(i)).collect::<Vec<_>>(),
            [
                validity[2],
                validity[1],
                validity[2],
                validity[0],
                validity[1]
            ]
        );
        let empty = copied(&sources, &[]);
        assert_eq!(empty.len(), 0);
        assert_eq!(empty.data_type(), &DataType::FixedSizeBinary(0));
    }
}

#[test]
fn long_fixed_value_is_copied_exactly_and_control_can_stop_inside_that_value() {
    let value: Vec<u8> = (0..COPY_BYTES * 2 + 17).map(|i| (i % 251) as u8).collect();
    let sources = vec![source(value.len() as i32, value.clone(), None)];
    let output = copied(&sources, &[(0, 0), (0, 0)]);
    let array = output
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(array.value(0), value);
    assert_eq!(array.value(1), value);

    let baseline = Control::new(None);
    let mut expected = Vec::with_capacity(value.len());
    append_value(
        &mut expected,
        &value,
        &mut EvaluationCheckpoints::new(&baseline),
    )
    .unwrap();
    assert_eq!(expected, value);
    let mut observed_partial_stop = false;
    // The exact private segment size/count is not a public contract. Visit
    // every actual checkpoint and independently inspect unfinished payload.
    for stop in 1..=baseline.calls.load(Ordering::Relaxed) {
        let control = Control::new(Some(stop));
        let mut partial = Vec::with_capacity(value.len());
        let error = append_value(
            &mut partial,
            &value,
            &mut EvaluationCheckpoints::new(&control),
        )
        .unwrap_err();
        assert!(matches!(
            error,
            CopyError::Control(KernelFailure::DeadlineExceeded)
        ));
        assert_eq!(control.calls.load(Ordering::Relaxed), stop);
        assert_eq!(partial, value[..partial.len()]);
        observed_partial_stop |= !partial.is_empty() && partial.len() < value.len();
    }
    assert!(
        observed_partial_stop,
        "one value must expose a real internal cancellation boundary"
    );
}

#[derive(Debug, Eq, PartialEq)]
struct Refusal(usize);
#[test]
fn fixed_copy_preserves_every_original_creator_refusal_and_never_finishes_after_it() {
    let sources = vec![source(3, b"aaabbb".to_vec(), Some(vec![false, true]))];
    let choices = [(0, 0), (0, 1), (0, 0)];
    let events = Cell::new(0);
    let mut request = |_| {
        events.set(events.get() + 1);
        Ok::<_, Refusal>(())
    };
    let mut opaque = |_| {
        events.set(events.get() + 1);
        Ok::<_, Refusal>(())
    };
    attempt(
        &sources,
        &choices,
        &Control::new(None),
        &mut request,
        &mut opaque,
    )
    .unwrap();
    let total = events.get();
    assert!(total > 0);
    for refuse_at in 1..=total {
        let calls = Cell::new(0);
        let event = || {
            let ordinal = calls.get() + 1;
            calls.set(ordinal);
            if ordinal == refuse_at {
                Err(Refusal(ordinal))
            } else {
                Ok(())
            }
        };
        let result = attempt(
            &sources,
            &choices,
            &Control::new(None),
            &mut |_| event(),
            &mut |_| event(),
        );
        assert!(
            matches!(result, Err(CopyConstructionError::Creator(Refusal(actual))) if actual == refuse_at)
        );
        assert_eq!(calls.get(), refuse_at);
    }
}

#[test]
fn fixed_copy_preserves_control_failure_at_every_actual_boundary() {
    let sources = vec![source(
        (COPY_BYTES + 3) as i32,
        vec![9; COPY_BYTES + 3],
        None,
    )];
    let choices = [(0, 0)];
    let baseline = Control::new(None);
    attempt(
        &sources,
        &choices,
        &baseline,
        &mut |_| Ok::<_, ()>(()),
        &mut |_| Ok(()),
    )
    .unwrap();
    for stop in 1..=baseline.calls.load(Ordering::Relaxed) {
        let control = Control::new(Some(stop));
        let result = attempt(
            &sources,
            &choices,
            &control,
            &mut |_| Ok::<_, ()>(()),
            &mut |_| Ok(()),
        );
        assert!(matches!(
            result,
            Err(CopyConstructionError::Copy(CopyError::Control(
                KernelFailure::DeadlineExceeded
            )))
        ));
        assert_eq!(control.calls.load(Ordering::Relaxed), stop);
    }
}

#[test]
fn fixed_constructor_extent_uses_real_backing_without_allocating_a_large_value() {
    assert!(preflight_fixed_binary_constructor_extent(0, usize::MAX).is_ok());
    assert!(preflight_fixed_binary_constructor_extent(1, isize::MAX as usize).is_ok());
    assert!(matches!(
        preflight_fixed_binary_constructor_extent(-1, 0),
        Err(CopyError::Invalid(_))
    ));
    // Arrow's signed width is not a signed total-payload offset. These
    // extents exceed i32::MAX but fit the actual 64-bit Vec backing layout.
    #[cfg(target_pointer_width = "64")]
    {
        assert!(preflight_fixed_binary_constructor_extent(i32::MAX, 2).is_ok());
        assert!(preflight_fixed_binary_constructor_extent(16, i32::MAX as usize / 16 + 1).is_ok());
    }
    let too_many_bytes = isize::MAX as usize + 1;
    let error = preflight_fixed_binary_constructor_extent(1, too_many_bytes).unwrap_err();
    let fact = error.failure_fact();
    assert_eq!(fact.class(), FailureClass::Representation);
    let representation = fact.representation().unwrap();
    assert_eq!(representation.required, Some(too_many_bytes as u64));
    assert_eq!(representation.extent_limit, Some(isize::MAX as u64));
    assert_eq!(
        representation.layout.code,
        crate::allocation_failure::RUST_ARRAY_BACKING_LAYOUT
    );
    // Even arithmetic outside usize must retain the original backing fact.
    let error = preflight_fixed_binary_constructor_extent(2, usize::MAX).unwrap_err();
    assert_eq!(error.failure_fact().class(), FailureClass::Representation);
}

#[test]
fn fixed_copy_rejects_invalid_selection_before_destination_allocation() {
    let sources = vec![source(3, b"aaa".to_vec(), None)];
    for choices in [vec![(1, 0)], vec![(0, 1)]] {
        let requests = Cell::new(0);
        let result = attempt(
            &sources,
            &choices,
            &Control::new(None),
            &mut |_| {
                requests.set(requests.get() + 1);
                Ok::<_, ()>(())
            },
            &mut |_| {
                requests.set(requests.get() + 1);
                Ok(())
            },
        );
        assert!(matches!(
            result,
            Err(CopyConstructionError::Copy(CopyError::Invalid(_)))
        ));
        assert_eq!(requests.get(), 0);
    }
}
