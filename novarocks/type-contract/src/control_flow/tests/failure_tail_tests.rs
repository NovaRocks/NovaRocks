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
    observations: Mutex<Vec<(CompilePhase, u32)>>,
    refused: Mutex<bool>,
}
impl PureCompileControl for PrefixControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(
            !*self.refused.lock().unwrap(),
            "callback after primary refusal"
        );
        assert!(units <= MAX_UNOBSERVED_COMPILE_WORK);
        let mut trace = self.observations.lock().unwrap();
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
fn phases() -> [CompilePhase; 4] {
    [
        CompilePhase::Encode,
        CompilePhase::Decode,
        CompilePhase::Validate,
        CompilePhase::LowerProgram,
    ]
}
fn causes() -> [CompileControlError; 3] {
    [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ]
}
fn assert_prefixes(
    regions: &[ExpressionEvaluationRegion],
    uses: &[ExpressionInvocation<u32>],
    phase: CompilePhase,
    trace: &[(CompilePhase, u32)],
) {
    for at in 0..trace.len() {
        for cause in causes() {
            let owner = PrefixControl {
                fail_at: Some((at, cause.clone())),
                ..Default::default()
            };
            assert_eq!(
                ExpressionControlFlow::try_new(
                    regions.to_vec(),
                    uses.to_vec(),
                    &definitions(2),
                    phase,
                    &owner,
                )
                .unwrap_err(),
                ExpressionControlFlowError::Control(cause),
            );
            assert_eq!(*owner.observations.lock().unwrap(), trace[..=at]);
        }
    }
}

#[test]
fn malformed_flow_owners_observe_completed_tail_without_reclassifying_graph_errors() {
    use ExpressionControlFlowError as E;
    let mut missing_child = invocation(0, 1000);
    missing_child.arguments = Box::from([ExpressionUseId::new(19)]);
    let mut wrong_demand = invocation(0, 1000);
    wrong_demand.arguments = Box::from([ExpressionUseId::new(1)]);
    let mut child = invocation(1, u32::MAX);
    child.context.demand = EvaluationDemand::TruthOnly;
    let mut wrong_arity = invocation(0, 1000);
    wrong_arity.control = ControlShape::If;
    let mut invalid_guard = root();
    invalid_guard.parent = Some(EvaluationRegionId::new(19));
    let mut cyclic_a = invocation(0, 1000);
    cyclic_a.arguments = Box::from([ExpressionUseId::new(1)]);
    let mut cyclic_b = invocation(1, u32::MAX);
    cyclic_b.arguments = Box::from([ExpressionUseId::new(0)]);
    let mut shared = invocation(0, 1000);
    shared.arguments = Box::from([ExpressionUseId::new(1), ExpressionUseId::new(1)]);
    let cases = [
        (vec![root(), root()], vec![], E::DuplicateIdentity),
        (vec![root()], vec![invocation(0, 19)], E::InvalidReference),
        (
            vec![root()],
            vec![invocation(0, 1000), invocation(0, u32::MAX)],
            E::DuplicateIdentity,
        ),
        (
            vec![invalid_guard],
            vec![invocation(0, 1000)],
            E::InvalidGuard,
        ),
        (vec![root()], vec![missing_child], E::InvalidReference),
        (vec![root()], vec![wrong_demand, child], E::InvalidDemand),
        (vec![root()], vec![wrong_arity], E::InvalidControlShape),
        (vec![root()], vec![cyclic_a, cyclic_b], E::Cycle),
        (
            vec![root()],
            vec![shared, invocation(1, 1000)],
            E::SharedUse,
        ),
    ];
    for phase in phases() {
        for (regions, uses, expected) in &cases {
            let owner = PrefixControl::default();
            assert_eq!(
                ExpressionControlFlow::try_new(
                    regions.clone(),
                    uses.clone(),
                    &definitions(2),
                    phase,
                    &owner,
                )
                .unwrap_err(),
                *expected,
            );
            let trace = owner.observations.lock().unwrap().clone();
            // Each short malformed graph completed real indexing/graph work.
            // The old constructor returned these errors after only entry(0).
            assert_eq!(trace.len(), 2);
            assert_eq!(trace[0], (phase, 0));
            assert!(trace[1].1 > 0);
            assert!(trace.iter().all(|(actual, _)| *actual == phase));
            assert_prefixes(regions, uses, phase, &trace);
        }
    }
}

#[test]
fn duplicate_region_failure_observes_exact_255_256_257_completed_work_boundaries() {
    for phase in phases() {
        for count in [255, 256, 257] {
            let mut regions = (0..count)
                .map(|id| ExpressionEvaluationRegion {
                    id: EvaluationRegionId::new(id),
                    parent: None,
                    guard: None,
                })
                .collect::<Vec<_>>();
            regions.push(regions[0]);
            let owner = PrefixControl::default();
            assert_eq!(
                ExpressionControlFlow::try_new(
                    regions.clone(),
                    vec![],
                    &definitions(2),
                    phase,
                    &owner,
                )
                .unwrap_err(),
                ExpressionControlFlowError::DuplicateIdentity,
            );
            let trace = owner.observations.lock().unwrap().clone();
            let expected = if count == 255 {
                vec![(phase, 0), (phase, 255)]
            } else {
                vec![(phase, 0), (phase, 256), (phase, count - 256)]
            };
            assert_eq!(trace, expected);
            assert_prefixes(&regions, &[], phase, &trace);
        }
    }
}

#[test]
fn checked_flow_success_preserves_sparse_child_order_phase_and_primary_callback_prefixes() {
    let mut parent = invocation(u32::MAX, u32::MAX);
    parent.arguments = Box::from([ExpressionUseId::new(9), ExpressionUseId::new(0)]);
    let regions = vec![root()];
    let uses = vec![parent, invocation(0, 1000), invocation(9, 1000)];
    for phase in phases() {
        let owner = PrefixControl::default();
        let graph = ExpressionControlFlow::try_new(
            regions.clone(),
            uses.clone(),
            &definitions(2),
            phase,
            &owner,
        )
        .unwrap();
        assert_eq!(graph.root_use_ids(), [ExpressionUseId::new(u32::MAX)]);
        assert_eq!(
            graph.uses()[&ExpressionUseId::new(u32::MAX)]
                .arguments
                .as_ref(),
            [ExpressionUseId::new(9), ExpressionUseId::new(0)],
        );
        assert_eq!(graph.use_reference_count(), 5);
        let trace = owner.observations.lock().unwrap().clone();
        assert_eq!(trace.len(), 2);
        assert_eq!(trace[0], (phase, 0));
        assert!(trace[1].1 > 0);
        assert_prefixes(&regions, &uses, phase, &trace);
    }
}

#[test]
fn zero_work_owner_error_and_empty_success_both_observe_the_original_tail() {
    for phase in phases() {
        let members = definitions(MAX_CONTROL_DEFINITIONS + 1);
        let owner = PrefixControl::default();
        assert_eq!(
            ExpressionControlFlow::<u32>::try_new(vec![], vec![], &members, phase, &owner)
                .unwrap_err(),
            ExpressionControlFlowError::TooManyItems,
        );
        assert_eq!(members.lookups.get(), 0);
        let trace = owner.observations.lock().unwrap().clone();
        assert_eq!(trace, [(phase, 0), (phase, 0)]);
        for at in 0..trace.len() {
            for cause in causes() {
                let owner = PrefixControl {
                    fail_at: Some((at, cause.clone())),
                    ..Default::default()
                };
                assert_eq!(
                    ExpressionControlFlow::<u32>::try_new(vec![], vec![], &members, phase, &owner)
                        .unwrap_err(),
                    ExpressionControlFlowError::Control(cause),
                );
                assert_eq!(*owner.observations.lock().unwrap(), trace[..=at]);
            }
        }
        let owner = PrefixControl::default();
        let graph =
            ExpressionControlFlow::<u32>::try_new(vec![], vec![], &definitions(0), phase, &owner)
                .unwrap();
        assert!(graph.root_use_ids().is_empty());
        assert_eq!(
            *owner.observations.lock().unwrap(),
            [(phase, 0), (phase, 0)]
        );
        assert_prefixes(&[], &[], phase, &[(phase, 0), (phase, 0)]);
    }
}
