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
struct PrefixControl {
    fail_at: Option<(usize, CompileControlError)>,
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refused: Mutex<bool>,
}
impl PureCompileControl for PrefixControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(
            !*self.refused.lock().unwrap(),
            "callback after primary refusal"
        );
        assert!(units <= novarocks_type_contract::MAX_UNOBSERVED_COMPILE_WORK);
        let mut trace = self.trace.lock().unwrap();
        let index = trace.len();
        trace.push((phase, units));
        if let Some((at, cause)) = self.fail_at.clone()
            && index == at
        {
            *self.refused.lock().unwrap() = true;
            return Err(cause);
        }
        Ok(())
    }
}
fn causes() -> [CompileControlError; 3] {
    [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ]
}
fn assert_prefixes(
    definitions: &Definitions,
    dto: &wire::ExpressionControl,
    trace: &[(CompilePhase, u32)],
) {
    for at in 0..trace.len() {
        for cause in causes() {
            let control = PrefixControl {
                fail_at: Some((at, cause.clone())),
                ..Default::default()
            };
            assert_eq!(
                decode_expression_control(definitions, dto, &control).unwrap_err(),
                ControlCodecError::Control(cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
}

#[test]
fn decode_malformed_projection_observes_completed_tail_before_returning_format_error() {
    let (definitions, flow) = binary_fixture();
    let original = encode_expression_control(&flow, &Control::default()).unwrap();
    let mut malformed = Vec::new();
    let mut add = |change: fn(&mut wire::ExpressionControl)| {
        let mut dto = original.clone();
        change(&mut dto);
        malformed.push(dto);
    };
    add(|dto| {
        dto.regions.push(wire::EvaluationRegion {
            id: 11,
            parent_region_id: Some(u32::MAX),
            guard: Some(wire::RegionGuard {
                owner_use_id: Some(0),
                kind: Some(wire::region_guard::Kind::Simple(i32::MAX)),
            }),
        });
    });
    add(|dto| {
        dto.regions[0].guard = Some(wire::RegionGuard {
            owner_use_id: None,
            kind: Some(wire::region_guard::Kind::Simple(1)),
        });
    });
    add(|dto| dto.uses[0].demand = 0);
    add(|dto| dto.uses[0].demand = i32::MAX);
    add(|dto| dto.uses[0].region_id = None);
    add(|dto| dto.uses[0].definition_id = None);
    add(|dto| dto.uses[0].control = None);
    add(|dto| dto.uses[0].control.as_mut().unwrap().kind = None);
    add(|dto| {
        dto.uses[0].control.as_mut().unwrap().kind =
            Some(wire::control_shape::Kind::Simple(i32::MAX));
    });
    for dto in malformed {
        let recorder = PrefixControl::default();
        assert!(matches!(
            decode_expression_control(&definitions, &dto, &recorder),
            Err(ControlCodecError::InvalidShape(_))
        ));
        let trace = recorder.trace.lock().unwrap().clone();
        // All fixtures finish some real projection work before malformed input.
        // Before the production fix this was only the entry callback.
        assert_eq!(trace.len(), 2);
        assert_eq!(trace[0], (CompilePhase::Decode, 0));
        assert_eq!(trace[1].0, CompilePhase::Decode);
        assert!(trace[1].1 > 0);
        assert_prefixes(&definitions, &dto, &trace);
    }
}

#[test]
fn decode_zero_work_error_still_observes_original_tail_and_keeps_refusal_primary() {
    let (definitions, flow) = binary_fixture();
    let mut dto = encode_expression_control(&flow, &Control::default()).unwrap();
    dto.uses.clear();
    dto.regions[0].guard = Some(wire::RegionGuard {
        owner_use_id: Some(0),
        kind: None,
    });
    let recorder = PrefixControl::default();
    assert_eq!(
        decode_expression_control(&definitions, &dto, &recorder).unwrap_err(),
        ControlCodecError::InvalidShape("guard kind is missing")
    );
    let trace = recorder.trace.lock().unwrap().clone();
    assert_eq!(
        trace,
        [(CompilePhase::Decode, 0), (CompilePhase::Decode, 0)]
    );
    assert_prefixes(&definitions, &dto, &trace);
}

#[test]
fn decode_success_and_quantum_boundary_errors_keep_every_original_callback_prefix() {
    for count in [1, 255, 256, 257, 320] {
        let (definitions, flow) = many_roots_fixture(count);
        let original = encode_expression_control(&flow, &Control::default()).unwrap();
        let recorder = PrefixControl::default();
        assert_eq!(
            decode_expression_control(&definitions, &original, &recorder).unwrap(),
            flow
        );
        let trace = recorder.trace.lock().unwrap().clone();
        assert_eq!(trace[0], (CompilePhase::Decode, 0));
        if count >= 255 {
            assert!(trace.contains(&(CompilePhase::Decode, 256)));
        }
        assert_prefixes(&definitions, &original, &trace);
        let mut malformed = original;
        malformed.uses.last_mut().unwrap().control = None;
        let recorder = PrefixControl::default();
        assert_eq!(
            decode_expression_control(&definitions, &malformed, &recorder).unwrap_err(),
            ControlCodecError::InvalidShape("use control is missing")
        );
        let trace = recorder.trace.lock().unwrap().clone();
        assert!(
            trace
                .iter()
                .all(|(phase, _)| *phase == CompilePhase::Decode)
        );
        // A quantum can leave an exact zero-unit tail; it is still observed.
        assert!(trace.len() >= 2);
        assert_prefixes(&definitions, &malformed, &trace);
    }
}

#[test]
fn flow_error_projection_preserves_typed_control_causes_and_ordinary_flow_category() {
    for cause in causes() {
        assert_eq!(
            ControlCodecError::from(ExpressionControlFlowError::Control(cause.clone())),
            ControlCodecError::Control(cause)
        );
    }
    let ordinary = ExpressionControlFlowError::InvalidReference;
    assert_eq!(
        ControlCodecError::from(ordinary.clone()),
        ControlCodecError::Flow(ordinary)
    );
    let (definitions, flow) = binary_fixture();
    let mut dto = encode_expression_control(&flow, &Control::default()).unwrap();
    dto.uses[0].definition_id = Some(12345);
    let recorder = PrefixControl::default();
    assert_eq!(
        decode_expression_control(&definitions, &dto, &recorder).unwrap_err(),
        ControlCodecError::Flow(ExpressionControlFlowError::InvalidReference)
    );
    assert_prefixes(&definitions, &dto, &recorder.trace.lock().unwrap().clone());
}
