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
//! Scope/control witnesses only; these test leases grant no production memory.
use super::*;
use crate::kernel_input::EvaluationCheckpoints;
use crate::{KernelDiagnostic, KernelEvaluationControl, KernelFailure};
use allocator_api2::alloc::{AllocError, Allocator, Global};
use std::{
    alloc::Layout,
    ptr::NonNull,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering as AtomicOrdering},
    },
    time::Duration,
};

#[derive(Clone, Debug)]
struct TestAllocator;
unsafe impl Allocator for TestAllocator {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        Global.allocate(layout)
    }
    unsafe fn deallocate(&self, pointer: NonNull<u8>, layout: Layout) {
        unsafe { Global.deallocate(pointer, layout) }
    }
}
impl ScalarStateAllocator for TestAllocator {
    fn scalar_allocation_error(&self, _: &str) -> ScalarStateError {
        KernelFailure::ResourceExhausted.into()
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
enum Event {
    Enter(usize),
    Drop(usize),
    Check(u32, bool),
}
#[derive(Default)]
struct Trace {
    events: Mutex<Vec<Event>>,
    active: AtomicBool,
    refusal: Option<KernelFailure>,
    refuse_only_inside_sort: bool,
}
impl KernelEvaluationControl for Trace {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        self.events.lock().unwrap().push(Event::Check(
            units,
            self.active.load(AtomicOrdering::SeqCst),
        ));
        assert!(units <= 256);
        if self.refuse_only_inside_sort && !self.active.load(AtomicOrdering::SeqCst) {
            return Ok(());
        }
        self.refusal.clone().map_or(Ok(()), Err)
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("N sorting never waits")
    }
}
struct Lease {
    trace: Arc<Trace>,
    len: usize,
}
impl Drop for Lease {
    fn drop(&mut self) {
        assert!(self.trace.active.swap(false, AtomicOrdering::SeqCst));
        self.trace
            .events
            .lock()
            .unwrap()
            .push(Event::Drop(self.len));
    }
}
struct Resources {
    trace: Arc<Trace>,
    refusal: Option<KernelFailure>,
}
impl NSortResources for Resources {
    type Lease = Lease;
    fn before_sort(
        &mut self,
        len: usize,
        _: &mut ScalarWork<'_, '_>,
    ) -> Result<Lease, KernelFailure> {
        self.trace.events.lock().unwrap().push(Event::Enter(len));
        if let Some(cause) = &self.refusal {
            return Err(cause.clone());
        }
        assert!(!self.trace.active.swap(true, AtomicOrdering::SeqCst));
        Ok(Lease {
            trace: self.trace.clone(),
            len,
        })
    }
}
fn state() -> NState<TestAllocator> {
    let mut state = NState::new(TestAllocator);
    state.init_limit_if_needed(512).unwrap();
    // This is the lawful sorted/full state immediately before the next update.
    state
        .values
        .extend((0..512).map(TrackedAggScalarValue::Decimal128));
    state
}
fn values(state: &NState<TestAllocator>) -> Vec<i128> {
    state
        .values
        .iter()
        .map(|value| match value {
            TrackedAggScalarValue::Decimal128(value) => *value,
            _ => panic!("expected original inline i128 value"),
        })
        .collect()
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        crate::kernel_control::invalid("sort source refused"),
        crate::kernel_control::internal("sort source refused"),
        KernelFailure::Operational(KernelDiagnostic::new("sort source refused")),
        KernelFailure::InstanceFailed,
    ]
}
#[test]
fn n_sort_scope_covers_original_comparisons_and_ends_between_both_sorts() {
    for smallest in [false, true] {
        let trace = Arc::new(Trace::default());
        let mut resources = Resources {
            trace: trace.clone(),
            refusal: None,
        };
        let mut checkpoints = EvaluationCheckpoints::new(trace.as_ref());
        let mut state = state();
        state
            .push_value_with_sort_resources(
                TrackedAggScalarValue::Decimal128(-1),
                smallest,
                &mut resources,
                &mut ScalarWork::new(Some(&mut checkpoints)),
            )
            .unwrap();
        let events = trace.events.lock().unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, Event::Enter(_) | Event::Drop(_)))
                .cloned()
                .collect::<Vec<_>>(),
            vec![
                Event::Enter(513),
                Event::Drop(513),
                Event::Enter(512),
                Event::Drop(512)
            ]
        );
        assert!(
            events
                .iter()
                .any(|event| matches!(event, Event::Check(units, true) if *units > 0))
        );
        assert!(!trace.active.load(AtomicOrdering::SeqCst));
        assert_eq!(
            values(&state),
            if smallest {
                (-1i128..511).collect::<Vec<_>>()
            } else {
                (0i128..512).collect::<Vec<_>>()
            }
        );
    }
}
#[test]
fn n_sort_noop_and_owned_scope_preserve_original_control_trace_and_values() {
    for smallest in [false, true] {
        let plain = Trace::default();
        let observed = Arc::new(Trace::default());
        let mut resources = Resources {
            trace: observed.clone(),
            refusal: None,
        };
        let mut a = state();
        let mut b = state();
        let mut ap = EvaluationCheckpoints::new(&plain);
        let mut bp = EvaluationCheckpoints::new(observed.as_ref());
        a.push_value(
            TrackedAggScalarValue::Decimal128(-1),
            smallest,
            &mut ScalarWork::new(Some(&mut ap)),
        )
        .unwrap();
        b.push_value_with_sort_resources(
            TrackedAggScalarValue::Decimal128(-1),
            smallest,
            &mut resources,
            &mut ScalarWork::new(Some(&mut bp)),
        )
        .unwrap();
        let checkpoints = |trace: &Trace| {
            trace
                .events
                .lock()
                .unwrap()
                .iter()
                .filter_map(|event| {
                    if let Event::Check(units, _) = event {
                        Some(*units)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(checkpoints(&plain), checkpoints(&observed));
        assert_eq!(values(&a), values(&b));
    }
}
#[test]
fn n_sort_before_refusal_preserves_every_cause_without_sort_or_footer() {
    for cause in causes() {
        let trace = Arc::new(Trace::default());
        let mut resources = Resources {
            trace: trace.clone(),
            refusal: Some(cause.clone()),
        };
        let mut state = state();
        let error = state
            .push_value_with_sort_resources(
                TrackedAggScalarValue::Decimal128(-1),
                true,
                &mut resources,
                &mut ScalarWork::new(None),
            )
            .unwrap_err();
        assert!(matches!(error, ScalarStateError::Kernel(actual) if actual == cause));
        assert_eq!(*trace.events.lock().unwrap(), [Event::Enter(513)]);
        assert_eq!(state.values.len(), 513);
        assert!(matches!(
            state.values.last(),
            Some(TrackedAggScalarValue::Decimal128(-1))
        ));
        assert!(!trace.active.load(AtomicOrdering::SeqCst));
    }
}
#[test]
fn n_sort_original_leading_flush_refuses_before_scope_birth() {
    for cause in causes() {
        let trace = Arc::new(Trace {
            refusal: Some(cause.clone()),
            ..Trace::default()
        });
        let mut resources = Resources {
            trace: trace.clone(),
            refusal: None,
        };
        let mut checkpoints = EvaluationCheckpoints::new(trace.as_ref());
        let mut work = ScalarWork::new(Some(&mut checkpoints));
        work.step().unwrap();
        let mut state = state();
        let before = values(&state);
        let error = state.sort_values(&mut resources, &mut work).unwrap_err();
        assert!(matches!(error, ScalarStateError::Kernel(actual) if actual == cause));
        assert_eq!(*trace.events.lock().unwrap(), [Event::Check(1, false)]);
        assert_eq!(values(&state), before);
    }
}

#[test]
fn n_sort_update_forwards_independent_value_and_limit_addresses() {
    let trace = Arc::new(Trace::default());
    let mut resources = Resources {
        trace: trace.clone(),
        refusal: None,
    };
    let values = crate::largeint::array_from_i128(&[Some(7), Some(-2)]).unwrap();
    let limits: ArrayRef = Arc::new(arrow_array::Int64Array::from(vec![2]));
    let mut state = NState::new(TestAllocator);
    state
        .update_from_arrays_with_sort_resources(
            &values,
            1,
            &limits,
            0,
            true,
            &mut resources,
            &mut ScalarWork::new(None),
        )
        .unwrap();
    assert_eq!(self::values(&state), [-2]);
    assert_eq!(
        *trace.events.lock().unwrap(),
        [
            Event::Enter(1),
            Event::Drop(1),
            Event::Enter(1),
            Event::Drop(1)
        ]
    );
}
#[test]
fn n_sort_merge_keeps_each_decoded_push_in_sequential_scopes() {
    let mut payload = 2u32.to_le_bytes().to_vec();
    payload.extend_from_slice(&3u32.to_le_bytes());
    for value in [3i128, 1, 2] {
        payload.push(7);
        payload.extend_from_slice(&value.to_le_bytes());
    }
    let input: ArrayRef = Arc::new(BinaryArray::from(vec![Some(payload.as_slice())]));
    let trace = Arc::new(Trace::default());
    let mut resources = Resources {
        trace: trace.clone(),
        refusal: None,
    };
    let mut state = NState::new(TestAllocator);
    state
        .merge_from_array_with_sort_resources(
            &input,
            0,
            true,
            &mut resources,
            &mut ScalarWork::new(None),
        )
        .unwrap();
    assert_eq!(values(&state), [1, 2]);
    let events = trace.events.lock().unwrap();
    let lengths = [1, 1, 2, 2, 3, 2];
    assert_eq!(
        *events,
        lengths
            .into_iter()
            .flat_map(|len| [Event::Enter(len), Event::Drop(len)])
            .collect::<Vec<_>>()
    );
    assert!(!trace.active.load(AtomicOrdering::SeqCst));
}

#[test]
fn n_sort_comparator_refusal_keeps_cause_and_releases_scope_without_footer() {
    for cause in causes() {
        let trace = Arc::new(Trace {
            refusal: Some(cause.clone()),
            refuse_only_inside_sort: true,
            ..Trace::default()
        });
        let mut resources = Resources {
            trace: trace.clone(),
            refusal: None,
        };
        let mut state = state();
        let mut checkpoints = EvaluationCheckpoints::new(trace.as_ref());
        let error = state
            .push_value_with_sort_resources(
                TrackedAggScalarValue::Decimal128(-1),
                true,
                &mut resources,
                &mut ScalarWork::new(Some(&mut checkpoints)),
            )
            .unwrap_err();
        assert!(matches!(error, ScalarStateError::Kernel(actual) if actual == cause));
        let events = trace.events.lock().unwrap();
        let refusal = events
            .iter()
            .position(|event| matches!(event, Event::Check(_, true)))
            .unwrap();
        assert_eq!(
            &events[refusal..],
            [Event::Check(256, true), Event::Drop(513)]
        );
        assert!(!trace.active.load(AtomicOrdering::SeqCst));
    }
}
