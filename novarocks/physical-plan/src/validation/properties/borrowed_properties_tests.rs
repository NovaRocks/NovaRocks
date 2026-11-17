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
use crate::*;
use novarocks_type_contract::{
    CompileControlError, CompilePhase, EvaluationRegionId, ExpressionControlFlow,
    ExpressionEvaluationRegion, PureCompileControl,
};
use std::sync::Mutex;

const SOURCE: usize = 4 * 1024 * 1024;
const PROJECTION: PropertyProofProjectionLimits = PropertyProofProjectionLimits {
    max_request_bytes: 1024 * 1024,
    max_coexisting_bytes: 8 * 1024 * 1024,
    max_projection_work: 1024 * 1024,
};
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl Control {
    fn trace(&self) -> Vec<u32> {
        self.trace.lock().unwrap().clone()
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Decode);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop, "callback after originating refusal");
        }
        trace.push(units);
        match self.refusal.clone() {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}

#[derive(Default)]
struct SetupControl(Mutex<Vec<(CompilePhase, u32)>>);
impl PureCompileControl for SetupControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Validate);
        assert!(units <= 256);
        self.0.lock().unwrap().push((phase, units));
        Ok(())
    }
}

fn fixture() -> (Fragment, PhysicalRootUses, FrozenFragmentCalls) {
    let mut builder = FragmentBuilder::new(FragmentId::new(0));
    let source = NodeId::new(u32::MAX);
    builder
        .add_values(source, Box::from([Box::default()]), Box::default())
        .unwrap();
    let mut child = source;
    for id in [0, 7, 1] {
        builder
            .add_limit(NodeId::new(id), child, Some(1), 0)
            .unwrap();
        child = NodeId::new(id);
    }
    let fragment = builder
        .finish_definition(
            child,
            FragmentSink::Noop,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let setup = SetupControl::default();
    let flow = ExpressionControlFlow::try_new(
        vec![ExpressionEvaluationRegion {
            id: EvaluationRegionId::new(0),
            parent: None,
            guard: None,
        }],
        vec![],
        fragment.expressions(),
        CompilePhase::Validate,
        &setup,
    )
    .unwrap();
    let uses = PhysicalRootUses::try_new(&fragment, flow, vec![], &setup).unwrap();
    let calls = FrozenFragmentCalls::try_new(&fragment, &uses, vec![], &setup).unwrap();
    (fragment, uses, calls)
}

fn run(
    fixture: &(Fragment, PhysicalRootUses, FrozenFragmentCalls),
    derive: bool,
    control: &Control,
    admit: &mut PropertyResourceAdmission<'_>,
) -> Result<PropertyProofProjectionFacts, FragmentPropertyError> {
    let (fragment, uses, calls) = fixture;
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = if derive {
        derive_fragment_output_properties_in(
            fragment,
            &FragmentCuts::default(),
            uses,
            calls,
            PlanLimits::FROZEN,
            SOURCE,
            PROJECTION,
            admit,
            &mut work,
        )
        .map(|(candidates, facts)| {
            assert_eq!(candidates.len(), 4);
            for (id, properties) in candidates {
                assert_eq!(properties, fragment.nodes()[&id].output_properties);
            }
            facts
        })
    } else {
        validate_fragment_output_properties_in(
            fragment,
            uses,
            calls,
            PlanLimits::FROZEN,
            SOURCE,
            PROJECTION,
            admit,
            &mut work,
        )
    };
    if matches!(&result, Err(FragmentPropertyError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

#[test]
fn caller_graph_composition_matches_original_formulas_and_cumulative_prefix() {
    let fixture = fixture();
    let (fragment, uses, calls) = &fixture;
    let setup = SetupControl::default();
    let original = validate_fragment_output_properties_observed(
        fragment,
        uses,
        calls,
        PlanLimits::FROZEN,
        SOURCE,
        PROJECTION,
        &setup,
    )
    .unwrap();
    let derived = derive_fragment_output_properties_observed(
        fragment,
        &FragmentCuts::default(),
        uses,
        calls,
        PlanLimits::FROZEN,
        SOURCE,
        PROJECTION,
        &setup,
    )
    .unwrap();
    for derive in [false, true] {
        let control = Control::default();
        let mut snapshots = Vec::new();
        let facts = run(&fixture, derive, &control, &mut |next| {
            snapshots.push(*next);
            Ok(())
        })
        .unwrap();
        let fields = |facts: PropertyProofProjectionFacts| {
            (
                facts.request_bytes,
                facts.coexisting_bytes,
                facts.projection_work,
            )
        };
        // Complete property validation also checks guarantees; derivation
        // returns its occurrence proof. Compare each to its own original author.
        assert_eq!(
            fields(facts),
            fields(if derive { derived.1 } else { original })
        );
        assert!(snapshots.len() > 4);
        assert!(
            snapshots.last().unwrap().allocation_requests_upper_bound
                > snapshots.first().unwrap().allocation_requests_upper_bound
        );
        for pair in snapshots.windows(2) {
            assert!(
                pair[0].allocation_requests_upper_bound <= pair[1].allocation_requests_upper_bound
            );
            assert!(
                pair[0].allocation_request_bytes_upper_bound
                    <= pair[1].allocation_request_bytes_upper_bound
            );
            assert!(pair[0].cumulative_work_upper_bound <= pair[1].cumulative_work_upper_bound);
        }
        // Decode is the caller's sole phase, including original structural
        // validation and the second actual Kahn schedule for formulas.
        assert_eq!(control.trace().first(), Some(&0));
    }
}

#[test]
fn caller_graph_composition_each_actual_callback_preserves_three_primary_causes() {
    let fixture = fixture();
    for derive in [false, true] {
        let baseline = Control::default();
        run(&fixture, derive, &baseline, &mut |_| Ok(())).unwrap();
        let trace = baseline.trace();
        for at in 0..trace.len() {
            for cause in CAUSES {
                let control = Control {
                    trace: Mutex::default(),
                    refusal: Some((at, cause.clone())),
                };
                assert!(matches!(
                    run(&fixture, derive, &control, &mut |_| Ok(())),
                    Err(FragmentPropertyError::Control(actual)) if actual == cause
                ));
                assert_eq!(control.trace(), trace[..=at]);
            }
        }
    }
}

#[test]
fn caller_graph_composition_known_request_refuses_before_the_next_real_callback() {
    let fixture = fixture();
    for derive in [false, true] {
        let baseline = Control::default();
        let mut snapshots = Vec::new();
        run(&fixture, derive, &baseline, &mut |next| {
            snapshots.push((*next, baseline.trace().len()));
            Ok(())
        })
        .unwrap();
        let trace = baseline.trace();
        // Each strict allocation-request increment admits a real source-owned
        // known prefix. No synthetic pending-work preload is used.
        for (index, (facts, callbacks)) in snapshots.iter().enumerate() {
            if facts.allocation_requests_upper_bound == 0
                || (index > 0
                    && facts.allocation_requests_upper_bound
                        == snapshots[index - 1].0.allocation_requests_upper_bound)
            {
                continue;
            }
            let limit = facts.allocation_requests_upper_bound - 1;
            for cause in CAUSES {
                let control = Control {
                    trace: Mutex::default(),
                    refusal: Some((*callbacks, cause)),
                };
                let result = run(&fixture, derive, &control, &mut |next| {
                    if next.allocation_requests_upper_bound > limit {
                        Err(CompileControlError::ResourceExhausted)
                    } else {
                        Ok(())
                    }
                });
                assert!(matches!(
                    result,
                    Err(FragmentPropertyError::Control(
                        CompileControlError::ResourceExhausted
                    ))
                ));
                assert_eq!(control.trace(), trace[..*callbacks]);
            }
        }
    }
}
