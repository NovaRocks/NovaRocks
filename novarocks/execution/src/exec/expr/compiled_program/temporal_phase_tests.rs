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

//! Single Frame source phase tests; no replacement evaluator or source walker.
use super::*;
use arrow::array::{BinaryArray, Date32Array, TimestampMicrosecondArray};
use novarocks_local_program::ProgramExpressionArena;
use novarocks_type_contract::{ExpressionUseId, TemporalSourceShape as S};
use std::sync::Mutex;
struct Control {
    trace: Mutex<Vec<u32>>,
    fail: usize,
    cause: KernelFailure,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        let mut trace = self.trace.lock().unwrap();
        trace.push(units);
        if trace.len() == self.fail {
            Err(self.cause.clone())
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("source phases must not wait")
    }
}
fn control(fail: usize, cause: KernelFailure) -> Control {
    Control {
        trace: Mutex::new(Vec::new()),
        fail,
        cause,
    }
}
fn frame(work: &mut Work<'_, '_>) -> Frame {
    Frame::new(
        ProgramUseRef {
            arena: ProgramExpressionArena::Main,
            use_id: ExpressionUseId::new(1),
        },
        ControlShape::TemporalSource(S::SecondsCastOther),
        &DataType::Int64,
        vec![1, 3, 5],
        novarocks_functions::ScalarInvocationActivation::Activated,
        vec![],
        work,
    )
    .unwrap()
}
fn child(ordinals: Vec<usize>, array: ArrayRef, errors: Vec<RowDataError>) -> Child {
    Child {
        ordinals,
        value: OwnedValue::Selected(array, errors.into_boxed_slice()),
    }
}
fn normal_with_prior_error(frame: &mut Frame, bad_last: bool, work: &mut Work<'_, '_>) {
    frame.next = 1;
    frame
        .attach_temporal(
            child(
                vec![0, 1, 2],
                Arc::new(Date32Array::from(vec![
                    Some(0),
                    None,
                    Some(if bad_last { i32::MAX } else { 0 }),
                ])),
                vec![RowDataError::new(1, "earlier normal child failure")],
            ),
            S::SecondsCastOther,
            7,
            work,
        )
        .unwrap();
}
#[test]
fn error_placeholders_never_trigger_whole_invocation_null_guard() {
    let ctl = control(usize::MAX, KernelFailure::Cancelled);
    let observed = ObservedControl {
        original: &ctl,
        refused: AtomicBool::new(false),
        operation_aborted: AtomicBool::new(false),
    };
    let mut work = Work {
        control: &observed,
        pending: 0,
        scalar_scope: None,
    };
    let mut frame = frame(&mut work);
    normal_with_prior_error(&mut frame, false, &mut work);
    frame.next = 2;
    assert!(
        frame
            .next_ordinals(
                ControlShape::TemporalSource(S::SecondsCastOther),
                3,
                true,
                7,
                &mut work
            )
            .unwrap()
            .is_none()
    );
    assert_eq!(frame.errors[&1].message(), "earlier normal child failure");
}
#[test]
fn demanded_deepest_data_error_projects_all_successes_preserving_prior_child_error() {
    let ctl = control(usize::MAX, KernelFailure::Cancelled);
    let observed = ObservedControl {
        original: &ctl,
        refused: AtomicBool::new(false),
        operation_aborted: AtomicBool::new(false),
    };
    let mut work = Work {
        control: &observed,
        pending: 0,
        scalar_scope: None,
    };
    let mut frame = frame(&mut work);
    normal_with_prior_error(&mut frame, true, &mut work);
    frame.next = 2;
    assert_eq!(
        frame
            .next_ordinals(
                ControlShape::TemporalSource(S::SecondsCastOther),
                3,
                true,
                7,
                &mut work
            )
            .unwrap(),
        Some(vec![0, 2])
    );
    frame.next = 3;
    frame
        .attach_temporal(
            child(
                vec![0, 2],
                Arc::new(Date32Array::from(vec![0, i32::MAX])),
                vec![],
            ),
            S::SecondsCastOther,
            7,
            &mut work,
        )
        .unwrap();
    let expected = "Cast error: Failed to convert 2147483647 to temporal for Date32";
    assert_eq!(frame.errors[&0].message(), expected);
    assert_eq!(frame.errors[&1].message(), "earlier normal child failure");
    assert_eq!(frame.errors[&2].message(), expected);
}
#[test]
fn binary_safe_null_is_data_not_control_failure_for_exact_sparse_domain() {
    let ctl = control(usize::MAX, KernelFailure::Cancelled);
    let observed = ObservedControl {
        original: &ctl,
        refused: AtomicBool::new(false),
        operation_aborted: AtomicBool::new(false),
    };
    let mut work = Work {
        control: &observed,
        pending: 0,
        scalar_scope: None,
    };
    let mut frame = frame(&mut work);
    frame.next = 3;
    frame
        .attach_temporal(
            child(
                vec![0, 2],
                Arc::new(BinaryArray::from(vec![
                    Some(&b"12:34:56"[..]),
                    Some(&[0xff][..]),
                ])),
                vec![],
            ),
            S::SecondsCastOther,
            7,
            &mut work,
        )
        .unwrap();
    assert!(frame.errors.is_empty());
    assert_eq!(
        frame.temporal.unwrap().seconds,
        vec![Some(45296), None, None]
    );
}
#[test]
fn long_zone_data_error_is_full_until_owner_row_projection() {
    let ctl = control(usize::MAX, KernelFailure::Cancelled);
    let observed = ObservedControl {
        original: &ctl,
        refused: AtomicBool::new(false),
        operation_aborted: AtomicBool::new(false),
    };
    let mut work = Work {
        control: &observed,
        pending: 0,
        scalar_scope: None,
    };
    let zone = "invalid_zone_".repeat(60);
    let array =
        Arc::new(TimestampMicrosecondArray::from(vec![i64::MAX]).with_timezone(zone.clone()))
            as ArrayRef;
    let expected = format!(
        "Parser error: Invalid timezone \"{zone}\": only offset based timezones supported without chrono-tz feature"
    );
    let mut values = TemporalValues::new(3, &mut work).unwrap();
    let rows = [5];
    assert_eq!(
        values
            .consume(
                S::SecondsCastOther,
                2,
                Selection::try_sparse(7, &rows).unwrap(),
                &[2],
                array.clone(),
                &mut work
            )
            .unwrap(),
        Some(expected.clone())
    );
    assert!(expected.len() > 512);
    let mut frame = frame(&mut work);
    frame.next = 3;
    frame
        .attach_temporal(
            child(vec![2], array, vec![]),
            S::SecondsCastOther,
            7,
            &mut work,
        )
        .unwrap();
    assert_eq!(frame.errors[&2], RowDataError::new(2, &expected));
    assert_eq!(frame.errors.len(), 1);
    assert!(!observed.refused.load(Ordering::Relaxed));
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(KernelDiagnostic::new("source-invalid")),
        KernelFailure::Internal(KernelDiagnostic::new("source-internal")),
        KernelFailure::Operational(KernelDiagnostic::new("source-operational")),
        KernelFailure::InstanceFailed,
    ]
}
fn deepest(control: &Control) -> Result<(), KernelFailure> {
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
    let frame = Frame::new(
        ProgramUseRef {
            arena: ProgramExpressionArena::Main,
            use_id: ExpressionUseId::new(1),
        },
        ControlShape::TemporalSource(S::SecondsCastOther),
        &DataType::Int64,
        vec![1, 3, 5],
        novarocks_functions::ScalarInvocationActivation::Activated,
        vec![],
        &mut work,
    );
    let mut frame = frame?;
    frame.next = 3;
    frame.attach_temporal(
        child(
            vec![0, 1, 2],
            Arc::new(Date32Array::from(vec![0, i32::MAX, 0])),
            vec![],
        ),
        S::SecondsCastOther,
        7,
        &mut work,
    )
}
#[test]
fn every_demanded_data_error_phase_checkpoint_preserves_seven_typed_control_causes() {
    let recorder = control(usize::MAX, KernelFailure::Cancelled);
    deepest(&recorder).unwrap();
    let trace = recorder.trace.lock().unwrap().clone();
    assert!(!trace.is_empty());
    assert!(trace.iter().all(|n| *n <= 256));
    for index in 1..=trace.len() {
        for cause in causes() {
            let control = control(index, cause.clone());
            assert_eq!(deepest(&control), Err(cause));
            assert_eq!(*control.trace.lock().unwrap(), trace[..index]);
        }
    }
}
