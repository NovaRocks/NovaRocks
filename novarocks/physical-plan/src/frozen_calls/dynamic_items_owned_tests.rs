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

#[derive(Default)]
struct BorrowedControl {
    fail_at: Option<(usize, CompileControlError)>,
    trace: Mutex<Vec<(CompilePhase, u32)>>,
}
impl PureCompileControl for BorrowedControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut trace = self.trace.lock().unwrap();
        let index = trace.len();
        trace.push((phase, units));
        match self.fail_at.clone() {
            Some((stop, cause)) if stop == index => Err(cause),
            _ => Ok(()),
        }
    }
}
fn count_in(
    calls: &FrozenFragmentCalls,
    control: &BorrowedControl,
) -> Result<usize, FrozenCallError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = calls.dynamic_items_in(&mut work);
    finish_frozen_calls(work, result)
}

#[test]
fn actual_sparse_call_environments_keep_original_count_and_caller_phase_without_nested_scope() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let mut fixture = scalar_fixture(3, u32::MAX, &mut fixture_constants);
    fixture.calls[0].effects.environment = Box::from([
        SemanticParameterRef {
            id: SemanticParameterId::new(0),
            expected_key: SemanticParameterKey::TimeZone,
        },
        SemanticParameterRef {
            id: SemanticParameterId::new(u32::MAX),
            expected_key: SemanticParameterKey::AllowThrowException,
        },
    ]);
    let calls = fixture.checked().unwrap();
    let original = Control::default();
    assert_eq!(calls.dynamic_items_observed(&original).unwrap(), 5);
    assert_eq!(
        *original.observations.lock().unwrap(),
        vec![(CompilePhase::Validate, 0), (CompilePhase::Validate, 3)]
    );
    let caller = BorrowedControl::default();
    let mut work = CompileCheckpoints::try_new(&caller, CompilePhase::Decode).unwrap();
    assert_eq!(calls.dynamic_items_in(&mut work).unwrap(), 5);
    // No child footer observes these three real entry visits.
    assert_eq!(
        *caller.trace.lock().unwrap(),
        vec![(CompilePhase::Decode, 0)]
    );
    work.finish().unwrap();
    assert_eq!(
        *caller.trace.lock().unwrap(),
        vec![(CompilePhase::Decode, 0), (CompilePhase::Decode, 3)]
    );
    assert_eq!(calls.entries().len(), 3);
    assert_eq!(
        calls
            .entries()
            .values()
            .next()
            .unwrap()
            .effects
            .environment
            .len(),
        2
    );
}

#[test]
fn actual_wide_call_count_quantum_and_empty_tail_preserve_every_first_control_cause() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    for count in [0, 3, 320] {
        let fixture = if count == 0 {
            empty_fixture()
        } else {
            scalar_fixture(count, 0, &mut fixture_constants)
        };
        let calls = fixture.checked().unwrap();
        let baseline = BorrowedControl::default();
        assert_eq!(count_in(&calls, &baseline).unwrap(), count);
        let trace = baseline.trace.lock().unwrap().clone();
        let expected = match count {
            0 => vec![(CompilePhase::Decode, 0), (CompilePhase::Decode, 0)],
            3 => vec![(CompilePhase::Decode, 0), (CompilePhase::Decode, 3)],
            320 => vec![
                (CompilePhase::Decode, 0),
                (CompilePhase::Decode, 256),
                (CompilePhase::Decode, 64),
            ],
            _ => unreachable!(),
        };
        assert_eq!(trace, expected);
        for stop in 0..trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let caller = BorrowedControl {
                    fail_at: Some((stop, cause.clone())),
                    ..BorrowedControl::default()
                };
                assert_eq!(
                    count_in(&calls, &caller),
                    Err(FrozenCallError::Control(cause))
                );
                assert_eq!(*caller.trace.lock().unwrap(), trace[..=stop]);
            }
        }
    }
}
