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
        assert_eq!(phase, CompilePhase::Decode);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let index = trace.len();
        if let Some((stop, _)) = self.fail_at {
            assert!(index <= stop, "callback after original refusal");
        }
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
    let environment = [
        SemanticParameterRef {
            id: SemanticParameterId::new(0),
            expected_key: SemanticParameterKey::TimeZone,
        },
        SemanticParameterRef {
            id: SemanticParameterId::new(u32::MAX),
            expected_key: SemanticParameterKey::StatementStartUtc,
        },
    ];
    let fragment = dynamic_relations(3, &environment);
    assert!(fragment.unit(fragment.root()).is_none());
    let calls = fragment.calls().clone();
    assert_eq!(calls.entries().len(), 3);
    let first = calls.entries().values().next().unwrap();
    assert_eq!(first.context.identity.call.get(), u32::MAX);
    assert_eq!(first.effects.environment.as_ref(), environment.as_ref());
    let source_address = first.effects.environment.as_ptr();
    // The actual source table, including its owned environment Box, survives
    // fragment exit through the retained Arc. Weak observation grants no hold.
    let weak_source = std::sync::Arc::downgrade(&calls.entries);
    let retained = calls.clone();
    drop(fragment);
    let original = Control::default();
    assert_eq!(calls.dynamic_items_observed(&original).unwrap(), 5);
    assert_eq!(
        *original.observations.lock().unwrap(),
        vec![(CompilePhase::Validate, 0), (CompilePhase::Validate, 3)]
    );
    let caller = BorrowedControl::default();
    let mut work = CompileCheckpoints::try_new(&caller, CompilePhase::Decode).unwrap();
    assert_eq!(calls.dynamic_items_in(&mut work).unwrap(), 5);
    assert_eq!(
        *caller.trace.lock().unwrap(),
        vec![(CompilePhase::Decode, 0)]
    );
    work.finish().unwrap();
    assert_eq!(
        *caller.trace.lock().unwrap(),
        vec![(CompilePhase::Decode, 0), (CompilePhase::Decode, 3)]
    );
    assert_eq!(
        retained
            .entries()
            .values()
            .next()
            .unwrap()
            .effects
            .environment
            .as_ptr(),
        source_address
    );
    assert_eq!(
        retained
            .entries()
            .values()
            .next()
            .unwrap()
            .effects
            .environment
            .as_ref(),
        environment.as_ref()
    );
    drop(calls);
    assert!(weak_source.upgrade().is_some());
    assert_eq!(count_in(&retained, &BorrowedControl::default()).unwrap(), 5);
    drop(retained);
    assert!(weak_source.upgrade().is_none());
}

#[test]
fn actual_wide_call_count_quantum_and_empty_tail_preserve_every_first_control_cause() {
    for count in [0, 3, 320] {
        let fragment = dynamic_relations(count, &[]);
        let calls = fragment.calls().clone();
        assert_eq!(calls.entries().len(), count);
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

fn dynamic_relations(count: usize, environment: &[SemanticParameterRef]) -> Fragment {
    if count == 0 {
        assert!(environment.is_empty());
        return empty_fixture().fragment;
    }
    let mut builder = FragmentBuilder::new(FragmentId::new(u32::MAX));
    let input = NodeId::new(0);
    builder
        .add_values(input, Box::from([Box::default()]), Box::default())
        .unwrap();
    let node = NodeId::new(u32::MAX);
    let mut outputs = Vec::new();
    let mut calls = Vec::new();
    let mut claims = Vec::new();
    let mut requests = Vec::new();
    for ordinal in 0..count {
        let ordinal = u32::try_from(ordinal).unwrap();
        let output = builder
            .add_value(
                integer(),
                ValueOrigin::NodeOutput {
                    node,
                    output_ordinal: ordinal,
                },
            )
            .unwrap();
        outputs.push(output);
        calls.push(WindowCallSpec {
            binding: function(FunctionKind::Window, integer()),
            distinct: false,
            arguments: Box::default(),
            order_by: Box::default(),
            frame: None,
            ignore_nulls: false,
            aggregate_binding: None,
            output,
        });
        let site = PhysicalCallSite::Window {
            node,
            call: ordinal,
        };
        let identity = novarocks_type_contract::RelationCallId::new(if ordinal == 0 {
            u32::MAX
        } else {
            ordinal - 1
        });
        let context = novarocks_type_contract::RelationEffectContext::call(identity);
        let mut effects = declared_effects(
            FunctionKind::Window,
            CallProofScope::Relation(context.identity),
        );
        if ordinal == 0 {
            effects.environment = environment.into();
        }
        claims.push(FrozenPhysicalCall {
            site,
            context,
            materialized_writer_input: None,
            effects,
            decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        });
        requests.push((site, zero_argument_request()));
    }
    install_node(
        &mut builder,
        node,
        vec![input],
        outputs,
        NodeKind::Window(WindowSpec {
            partition_by: Box::default(),
            order_by: Box::default(),
            calls: calls.into_boxed_slice(),
        }),
    );
    let fragment = builder
        .finish_structure(
            node,
            FragmentSink::Noop,
            dop(),
            PlanLimits::FROZEN,
            &Control::default(),
            StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let fragment = fragment
        .with_call_requests_observed(requests, &Control::default())
        .unwrap()
        .with_calls_observed(claims, &Control::default())
        .unwrap();
    // The source itself passes complete local laws before the count oracle.
    crate::validate_fragment_definition(&fragment).unwrap();
    fragment
}
