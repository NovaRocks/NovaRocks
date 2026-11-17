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

fn borrowed(
    source: FragmentStructureInput,
    limits: PlanLimits,
    control: &Control,
) -> Result<Fragment, FragmentStructureError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = Fragment::try_from_structure_in(source, limits, &mut work);
    if matches!(&result, Err(FragmentStructureError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

#[test]
fn borrowed_original_sparse_structure_preserves_all_fields_and_only_caller_phase() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let source = input(2, &mut fixture_constants);
    let expected = source.clone();
    let plain = Control::default();
    let original = construct(source.clone(), PlanLimits::FROZEN, &plain).unwrap();
    let caller = Control::default();
    let decoded = borrowed(source, PlanLimits::FROZEN, &caller).unwrap();
    assert_eq!(decoded.id(), FragmentId::new(u32::MAX));
    assert_eq!(decoded.root(), NodeId::new(u32::MAX));
    assert_eq!(decoded.values(), &expected.values);
    assert_eq!(decoded.nodes(), &expected.nodes);
    assert_eq!(
        decoded.unit(NodeId::new(0)),
        expected.nodes[&NodeId::new(0)].expressions.as_deref()
    );
    assert_eq!(decoded.sink(), &expected.sink);
    assert_eq!(decoded.dop_domain(), expected.dop_domain);
    assert_eq!(decoded.runtime_filters(), &*expected.runtime_filters);
    assert_eq!(decoded.call_requests(), original.call_requests());
    assert!(decoded.call_requests().entries().is_empty());
    let original_trace = plain.trace.lock().unwrap().clone();
    assert_eq!(
        *caller.trace.lock().unwrap(),
        original_trace
            .iter()
            .map(|(_, units)| (CompilePhase::Decode, *units))
            .collect::<Vec<_>>()
    );
}

#[test]
fn borrowed_structure_actual_success_and_original_rejections_preserve_every_control_prefix() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    for case in 0..5 {
        let mut source = input(if case == 4 { 320 } else { 2 }, &mut fixture_constants);
        let mut limits = PlanLimits::FROZEN;
        match case {
            1 => source.values.get_mut(&ValueId::new(0)).unwrap().id = ValueId::new(9),
            2 => source.nodes.get_mut(&NodeId::new(0)).unwrap().id = NodeId::new(9),
            3 => limits.fragment_nodes = 1,
            _ => {}
        }
        let baseline = Control::default();
        let result = borrowed(source.clone(), limits, &baseline);
        assert_eq!(result.is_ok(), matches!(case, 0 | 4));
        if let Err(error) = &result {
            assert!(matches!(error, FragmentStructureError::Structure(_)));
        }
        let trace = baseline.trace.lock().unwrap().clone();
        if case == 4 {
            assert!(trace.iter().any(|(_, units)| *units == 256));
        }
        for stop in 0..trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let caller = Control {
                    stop: Some((stop, cause.clone())),
                    ..Control::default()
                };
                assert!(
                    matches!(borrowed(source.clone(), limits, &caller), Err(FragmentStructureError::Control(actual)) if actual == cause)
                );
                assert_eq!(*caller.trace.lock().unwrap(), trace[..=stop]);
            }
        }
    }
}
