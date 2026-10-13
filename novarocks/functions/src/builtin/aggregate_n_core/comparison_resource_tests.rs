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

//! Core-local witnesses for reached comparison scopes and first-cause cleanup.
//! These instance-owned guards grant no production memory or diagnostic custody.
use super::*;
use crate::kernel_input::EvaluationCheckpoints;
use crate::{KernelDiagnostic, KernelEvaluationControl, KernelFailure};
use allocator_api2::alloc::{AllocError, Allocator, Global};
use std::{
    alloc::Layout,
    ptr::NonNull,
    sync::{Arc, Mutex},
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
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Kind {
    Sort,
    Comparison(usize),
}
#[derive(Clone, Debug, Eq, PartialEq)]
enum Event {
    Enter(Kind),
    Drop(Kind),
    Check(u32, bool, bool),
}
#[derive(Default)]
struct TraceState {
    events: Vec<Event>,
    sort: bool,
    comparison: bool,
    comparisons: usize,
}
#[derive(Default)]
struct Trace {
    state: Mutex<TraceState>,
    control_refusal: Option<KernelFailure>,
}
impl KernelEvaluationControl for Trace {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        let mut state = self.state.lock().unwrap();
        let (sort, comparison) = (state.sort, state.comparison);
        state.events.push(Event::Check(units, sort, comparison));
        assert!(units <= 256);
        if comparison {
            assert!(sort);
            if let Some(cause) = &self.control_refusal {
                return Err(cause.clone());
            }
        }
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("N comparisons never wait")
    }
}
struct Lease {
    trace: Arc<Trace>,
    kind: Kind,
}
impl Drop for Lease {
    fn drop(&mut self) {
        let mut state = self.trace.state.lock().unwrap();
        match self.kind {
            Kind::Sort => {
                assert!(state.sort && !state.comparison);
                state.sort = false;
            }
            Kind::Comparison(_) => {
                assert!(state.sort && state.comparison);
                state.comparison = false;
            }
        }
        state.events.push(Event::Drop(self.kind));
    }
}
struct Resources {
    trace: Arc<Trace>,
    comparison_refusal: Option<(usize, KernelFailure)>,
}
impl NSortResources for Resources {
    type Lease = Lease;
    fn before_sort(
        &mut self,
        _: usize,
        _: &mut ScalarWork<'_, '_>,
    ) -> Result<Lease, KernelFailure> {
        let mut state = self.trace.state.lock().unwrap();
        assert!(!state.sort && !state.comparison);
        state.sort = true;
        state.events.push(Event::Enter(Kind::Sort));
        Ok(Lease {
            trace: self.trace.clone(),
            kind: Kind::Sort,
        })
    }
    fn before_comparison(
        &mut self,
        _: &mut ScalarWork<'_, '_>,
    ) -> Result<Option<Lease>, KernelFailure> {
        let mut state = self.trace.state.lock().unwrap();
        assert!(state.sort && !state.comparison);
        let index = state.comparisons;
        state.comparisons += 1;
        state.events.push(Event::Enter(Kind::Comparison(index)));
        if let Some((stop, cause)) = &self.comparison_refusal {
            assert!(index <= *stop, "comparison after first refusal");
            if index == *stop {
                return Err(cause.clone());
            }
        }
        state.comparison = true;
        Ok(Some(Lease {
            trace: self.trace.clone(),
            kind: Kind::Comparison(index),
        }))
    }
}
fn state(values: Vec<TrackedAggScalarValue<TestAllocator>>) -> NState<TestAllocator> {
    let mut state = NState::new(TestAllocator);
    state.values.extend(values);
    state
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        crate::kernel_control::invalid("comparison refusal"),
        crate::kernel_control::internal("comparison refusal"),
        KernelFailure::Operational(KernelDiagnostic::new("comparison refusal")),
        KernelFailure::InstanceFailed,
    ]
}
fn numbers() -> Vec<TrackedAggScalarValue<TestAllocator>> {
    (0..513)
        .rev()
        .map(TrackedAggScalarValue::Decimal128)
        .collect()
}
fn checks(trace: &Trace) -> Vec<u32> {
    trace
        .state
        .lock()
        .unwrap()
        .events
        .iter()
        .filter_map(|event| match event {
            Event::Check(units, _, _) => Some(*units),
            _ => None,
        })
        .collect()
}
#[test]
fn reached_comparison_scopes_keep_original_values_and_default_control_trace() {
    let raw = Trace::default();
    let observed = Arc::new(Trace::default());
    let mut left = state(numbers());
    let mut right = state(numbers());
    let mut raw_checkpoints = EvaluationCheckpoints::new(&raw);
    let mut checkpoints = EvaluationCheckpoints::new(observed.as_ref());
    left.sort_values(
        &mut NoopSortResources,
        &mut ScalarWork::new(Some(&mut raw_checkpoints)),
    )
    .unwrap();
    right
        .sort_values(
            &mut Resources {
                trace: observed.clone(),
                comparison_refusal: None,
            },
            &mut ScalarWork::new(Some(&mut checkpoints)),
        )
        .unwrap();
    let values = |state: &NState<TestAllocator>| {
        state
            .values
            .iter()
            .map(|v| match v {
                TrackedAggScalarValue::Decimal128(v) => *v,
                _ => panic!("expected inline i128"),
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(values(&left), (0..513).collect::<Vec<i128>>());
    assert_eq!(values(&left), values(&right));
    assert_eq!(checks(&raw), checks(&observed));
    let trace = observed.state.lock().unwrap();
    assert!(trace.comparisons >= 256);
    assert!(!trace.sort && !trace.comparison);
    assert_eq!(
        trace
            .events
            .iter()
            .filter(|e| matches!(e, Event::Enter(Kind::Comparison(_))))
            .count(),
        trace
            .events
            .iter()
            .filter(|e| matches!(e, Event::Drop(Kind::Comparison(_))))
            .count()
    );
    assert!(
        trace
            .events
            .iter()
            .any(|e| matches!(e, Event::Check(256, true, true)))
    );
}
#[test]
fn mixed_tags_and_unordered_float_keep_original_equal_under_nested_scope() {
    for values in [
        vec![
            TrackedAggScalarValue::Bool(true),
            TrackedAggScalarValue::Int64(7),
        ],
        vec![
            TrackedAggScalarValue::Float64(f64::NAN),
            TrackedAggScalarValue::Float64(7.0),
        ],
    ] {
        let expected = format!("{values:?}");
        let trace = Arc::new(Trace::default());
        let mut state = state(values);
        let mut checkpoints = EvaluationCheckpoints::new(trace.as_ref());
        state
            .sort_values(
                &mut Resources {
                    trace: trace.clone(),
                    comparison_refusal: None,
                },
                &mut ScalarWork::new(Some(&mut checkpoints)),
            )
            .unwrap();
        assert_eq!(format!("{:?}", state.values.as_slice()), expected);
        assert_eq!(
            trace.state.lock().unwrap().events,
            [
                Event::Check(0, false, false),
                Event::Enter(Kind::Sort),
                Event::Enter(Kind::Comparison(0)),
                Event::Drop(Kind::Comparison(0)),
                Event::Drop(Kind::Sort),
                Event::Check(1, false, false)
            ]
        );
    }
}
#[test]
fn comparison_admission_refusal_preserves_every_first_cause_and_skips_later_work() {
    for cause in causes() {
        let trace = Arc::new(Trace::default());
        let mut state = state(numbers());
        let mut checkpoints = EvaluationCheckpoints::new(trace.as_ref());
        let error = state
            .sort_values(
                &mut Resources {
                    trace: trace.clone(),
                    comparison_refusal: Some((1, cause.clone())),
                },
                &mut ScalarWork::new(Some(&mut checkpoints)),
            )
            .unwrap_err();
        assert!(matches!(error, ScalarStateError::Kernel(actual) if actual == cause));
        assert_eq!(
            trace.state.lock().unwrap().events,
            [
                Event::Check(0, false, false),
                Event::Enter(Kind::Sort),
                Event::Enter(Kind::Comparison(0)),
                Event::Drop(Kind::Comparison(0)),
                Event::Enter(Kind::Comparison(1)),
                Event::Drop(Kind::Sort)
            ]
        );
    }
}
#[test]
fn original_comparator_control_refusal_releases_inner_then_outer_without_footer() {
    for cause in causes() {
        let trace = Arc::new(Trace {
            control_refusal: Some(cause.clone()),
            ..Trace::default()
        });
        let mut state = state(numbers());
        let mut checkpoints = EvaluationCheckpoints::new(trace.as_ref());
        let error = state
            .sort_values(
                &mut Resources {
                    trace: trace.clone(),
                    comparison_refusal: None,
                },
                &mut ScalarWork::new(Some(&mut checkpoints)),
            )
            .unwrap_err();
        assert!(matches!(error, ScalarStateError::Kernel(actual) if actual == cause));
        assert_eq!(checks(&trace), [0, 256]);
        let state = trace.state.lock().unwrap();
        assert!(!state.sort && !state.comparison);
        assert!(matches!(
            state.events.as_slice(),
            [
                ..,
                Event::Check(256, true, true),
                Event::Drop(Kind::Comparison(_)),
                Event::Drop(Kind::Sort)
            ]
        ));
    }
}
