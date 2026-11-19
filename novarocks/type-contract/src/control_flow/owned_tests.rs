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
use std::sync::Mutex;
struct Definitions(Box<[u32]>);
impl ExpressionDefinitionMembership<u32> for Definitions {
    fn definition_count(&self) -> usize {
        self.0.len()
    }
    fn contains_definition(&self, id: u32) -> bool {
        self.0.contains(&id)
    }
}
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    fail: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, n: u32) -> Result<(), CompileControlError> {
        assert!(n <= 256);
        let mut t = self.trace.lock().unwrap();
        let at = t.len();
        t.push(n);
        if let Some((i, c)) = self.fail.clone()
            && i == at
        {
            Err(c)
        } else {
            Ok(())
        }
    }
}
fn region() -> ExpressionEvaluationRegion {
    ExpressionEvaluationRegion {
        id: EvaluationRegionId::new(u32::MAX),
        parent: None,
        guard: None,
    }
}
fn invocation() -> ExpressionInvocation<u32> {
    ExpressionInvocation {
        context: ExpressionEffectContext {
            use_id: ExpressionUseId::new(0),
            region: region().id,
            demand: crate::EvaluationDemand::Value,
        },
        definition: 17,
        control: ControlShape::Eager,
        arguments: Box::default(),
    }
}
fn run(
    duplicate: bool,
    fail: Option<(usize, CompileControlError)>,
) -> (
    Result<ExpressionControlFlow<u32>, ExpressionControlFlowError>,
    Vec<u32>,
) {
    let c = Control {
        fail,
        ..Default::default()
    };
    let out = (|| {
        let mut w = CompileCheckpoints::try_new(&c, CompilePhase::Validate)?;
        let ds = if duplicate {
            vec![region(), region()]
        } else {
            vec![region()]
        };
        let out = ExpressionControlFlow::try_new_in(
            ds,
            vec![invocation()],
            &Definitions(Box::from([17])),
            &mut |_| Ok(()),
            &mut w,
        );
        if let Err(ExpressionControlFlowError::Control(c)) = out {
            return Err(c.into());
        }
        w.finish()?;
        out
    })();
    let trace = c.trace.lock().unwrap().clone();
    (out, trace)
}
#[test]
fn original_empty_arc_headers_and_actual_duplicate_insert_completion_are_independent() {
    let facts = expression_control_flow_header_resource_facts::<u32>(0, 0).unwrap();
    assert_eq!(
        (
            facts.allocation_requests_upper_bound,
            facts.allocation_request_bytes_upper_bound
        ),
        (3, {
            use crate::owned_resources::layout::arc_layout;
            use std::alloc::Layout;
            // Three original publication operations now construct guarded
            // immutable payloads; use their actual public Arc requests.
            arc_layout(Layout::new::<
                ControlFlowOwned<BTreeMap<EvaluationRegionId, ExpressionEvaluationRegion>>,
            >())
            .unwrap()
            .size()
                + arc_layout(Layout::new::<
                    ControlFlowOwned<BTreeMap<ExpressionUseId, ExpressionInvocation<u32>>>,
                >())
                .unwrap()
                .size()
                + arc_layout(Layout::new::<ControlFlowOwned<Box<[ExpressionUseId]>>>())
                    .unwrap()
                    .size()
        })
    );
    let (out, trace) = run(true, None);
    assert_eq!(
        out.unwrap_err(),
        ExpressionControlFlowError::DuplicateIdentity
    );
    // One actual use-header capture, then both original BTree insertions. The
    // second insertion completed even though its identity is an ordinary error.
    assert_eq!(trace.iter().map(|n| *n as usize).sum::<usize>(), 3);
}
#[test]
fn actual_flow_success_and_ordinary_prefixes_keep_the_first_control_without_nested_entry() {
    for duplicate in [false, true] {
        let (out, trace) = run(duplicate, None);
        assert_eq!(out.is_ok(), !duplicate);
        assert!(!trace.is_empty());
        for at in 0..trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let (out, actual) = run(duplicate, Some((at, cause.clone())));
                assert!(matches!(out,Err(ExpressionControlFlowError::Control(c))if c==cause));
                assert_eq!(actual, trace[..=at]);
            }
        }
    }
}
#[test]
fn known_map_and_arc_admission_refuses_before_pending_control_or_source_operations() {
    for pending in [0, 254, 255] {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let c = Control {
                fail: Some((1, cause)),
                ..Default::default()
            };
            let mut w = CompileCheckpoints::try_new(&c, CompilePhase::Validate).unwrap();
            for _ in 0..pending {
                w.step().unwrap();
            }
            let mut observed = None;
            let out = ExpressionControlFlow::try_new_in(
                vec![region()],
                vec![invocation()],
                &Definitions(Box::from([17])),
                &mut |facts| {
                    observed = Some(*facts);
                    Err(CompileControlError::ResourceExhausted)
                },
                &mut w,
            );
            assert!(matches!(
                out,
                Err(ExpressionControlFlowError::Control(
                    CompileControlError::ResourceExhausted
                ))
            ));
            assert!(observed.unwrap().allocation_requests_upper_bound >= 3);
            assert_eq!(*c.trace.lock().unwrap(), [0]);
        }
    }
}
