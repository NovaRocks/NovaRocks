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

use super::tests::Definitions;
use super::*;
use novarocks_expression_contract::{BinaryOperator, ExprKind};
use std::{alloc::Layout, sync::Mutex};
const B: usize = 4 * 1024 * 1024;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    failure: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, p: CompilePhase, n: u32) -> Result<(), CompileControlError> {
        assert!(n <= 256);
        let mut t = self.trace.lock().unwrap();
        let at = t.len();
        t.push((p, n));
        if let Some((i, c)) = self.failure.clone()
            && i == at
        {
            Err(c)
        } else {
            Ok(())
        }
    }
}
impl Control {
    fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.trace.lock().unwrap().clone()
    }
}
fn limits() -> ControlProjectionLimits {
    ControlProjectionLimits {
        max_regions: 1000,
        max_use_references: 10000,

        max_allocation_requests: 100000,
        max_allocation_request_bytes: 256 * 1024 * 1024,
        max_coexisting_source_and_request_bytes: 256 * 1024 * 1024,
        max_work: 1_000_000_000,
    }
}
fn fixture() -> (Definitions, ExpressionControlFlow<ExprId>) {
    let mut definitions = Definitions::default();
    let leaf = definitions.boolean(true);
    let root = definitions.add(ExprKind::Binary {
        op: BinaryOperator::Eq,
        left: leaf,
        right: leaf,
        allow_throw_exception: None,
        decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
    });
    let region = EvaluationRegionId::new(u32::MAX);
    let inv = |id, definition, args: &[ExpressionUseId]| ExpressionInvocation {
        context: ExpressionEffectContext {
            use_id: ExpressionUseId::new(id),
            region,
            demand: EvaluationDemand::Value,
        },
        definition,
        control: ControlShape::Eager,
        arguments: args.into(),
    };
    let flow = definitions.flow(
        vec![ExpressionEvaluationRegion {
            id: region,
            parent: None,
            guard: None,
        }],
        vec![
            inv(0, leaf, &[]),
            inv(u32::MAX, leaf, &[]),
            inv(
                9,
                root,
                &[ExpressionUseId::new(0), ExpressionUseId::new(u32::MAX)],
            ),
        ],
    );
    (definitions, flow)
}
fn finish<T>(
    out: Result<T, ControlCodecError>,
    w: CompileCheckpoints<'_>,
) -> Result<T, ControlCodecError> {
    if let Err(ControlCodecError::Control(c)) = out {
        return Err(c.into());
    }
    w.finish()?;
    out
}
type ProjectionRun<T> = (
    Result<(T, ControlProjectionFacts), ControlCodecError>,
    Vec<(CompilePhase, u32)>,
);
fn encode(
    _f: &Definitions,
    r: &ExpressionControlFlow<ExprId>,
    failure: Option<(usize, CompileControlError)>,
) -> ProjectionRun<wire::ExpressionControl> {
    let c = Control {
        failure,
        ..Default::default()
    };
    let out = (|| {
        let mut w = CompileCheckpoints::try_new(&c, CompilePhase::Encode)?;
        let out = encode_expression_control_observed(r, B, limits(), &mut |_| Ok(()), &mut w);
        finish(out, w)
    })();
    (out, c.trace())
}
fn decode(
    f: &Definitions,
    input: &wire::ExpressionControl,
    failure: Option<(usize, CompileControlError)>,
) -> ProjectionRun<ExpressionControlFlow<ExprId>> {
    let c = Control {
        failure,
        ..Default::default()
    };
    let out = (|| {
        let mut w = CompileCheckpoints::try_new(&c, CompilePhase::Decode)?;
        let out =
            decode_expression_control_observed(f, input, B, limits(), &mut |_| Ok(()), &mut w);
        finish(out, w)
    })();
    (out, c.trace())
}
#[test]
fn complete_control_reuses_original_roots_and_sparse_independent_occurrences() {
    let (f, r) = fixture();
    let (out, trace) = encode(&f, &r, None);
    let (input, facts) = out.unwrap();
    assert_eq!(input.regions[0].id, u32::MAX);
    assert_eq!(
        input.uses.iter().map(|u| u.id).collect::<Vec<_>>(),
        [0, 9, u32::MAX]
    );
    assert_eq!(input.uses[1].argument_use_ids, [0, u32::MAX]);
    assert_eq!(input.uses[0].definition_id, input.uses[2].definition_id);
    assert_eq!((facts.region_count, facts.use_reference_count), (1, 5));
    // Actual sender buffers: two top-level Vecs and the ordered two-ID Vec.
    let payload = Layout::array::<wire::EvaluationRegion>(1).unwrap().size()
        + Layout::array::<wire::ExpressionUse>(3).unwrap().size()
        + 8;
    assert!(facts.allocation_requests_upper_bound >= 3);
    assert!(facts.allocation_request_bytes_upper_bound >= payload);
    assert_eq!(
        facts.coexisting_source_and_request_bytes_upper_bound,
        B + facts.allocation_request_bytes_upper_bound
    );
    assert!(trace.iter().all(|(p, _)| *p == CompilePhase::Encode));
    let (out, trace) = decode(&f, &input, None);
    assert_eq!(out.unwrap().0, r);
    assert!(trace.iter().all(|(p, _)| *p == CompilePhase::Decode));
}
#[test]
fn every_actual_control_success_and_ordinary_callback_preserves_caller_scope_first_cause() {
    let (f, r) = fixture();
    let (input, _) = encode(&f, &r, None).0.unwrap();
    let baseline = encode(&f, &r, None);
    assert!(baseline.0.is_ok());
    for at in 0..baseline.1.len() {
        for cause in CAUSES {
            let (out, trace) = encode(&f, &r, Some((at, cause.clone())));
            assert!(matches!(out,Err(ControlCodecError::Control(c))if c==cause));
            assert_eq!(trace, baseline.1[..=at]);
        }
    }
    for malformed in [false, true] {
        let mut raw = input.clone();
        if malformed {
            raw.uses[1].region_id = None;
        }
        let baseline = decode(&f, &raw, None);
        assert_eq!(baseline.0.is_ok(), !malformed);
        if malformed {
            assert!(matches!(
                baseline.0,
                Err(ControlCodecError::InvalidShape("use region is missing"))
            ));
        }
        for at in 0..baseline.1.len() {
            for cause in CAUSES {
                let (out, trace) = decode(&f, &raw, Some((at, cause.clone())));
                assert!(matches!(out,Err(ControlCodecError::Control(c))if c==cause));
                assert_eq!(trace, baseline.1[..=at]);
            }
        }
    }
}
#[test]
fn original_owned_arc_geometry_and_all_six_axes_have_independent_refusal_oracles() {
    // The checked flow publishes two guarded map payloads and one guarded
    // root-use slice derived by the flow owner. Independently compute the
    // Arc geometry; the retired wire RootBinding buffer is not charged.
    use novarocks_type_contract::{
        ExpressionEvaluationRegion, ExpressionInvocation, ValueTypeStorageLifetime,
        owned_resources::layout::arc_layout,
    };
    use std::collections::BTreeMap;
    let map_regions = arc_layout(Layout::new::<(
        BTreeMap<EvaluationRegionId, ExpressionEvaluationRegion>,
        ValueTypeStorageLifetime,
    )>())
    .unwrap()
    .size();
    let map_uses = arc_layout(Layout::new::<(
        BTreeMap<ExpressionUseId, ExpressionInvocation<u32>>,
        ValueTypeStorageLifetime,
    )>())
    .unwrap()
    .size();
    let root_slice = arc_layout(Layout::new::<(
        Box<[ExpressionUseId]>,
        ValueTypeStorageLifetime,
    )>())
    .unwrap()
    .size();
    let header = expression_control_flow_header_resource_facts::<u32>(0, 0).unwrap();
    assert_eq!(
        (
            header.allocation_requests_upper_bound,
            header.allocation_request_bytes_upper_bound
        ),
        (3, map_regions + map_uses + root_slice)
    );
    let (f, r) = fixture();
    let (input, golden) = encode(&f, &r, None).0.unwrap();
    let decoded = decode(&f, &input, None).0.unwrap().1;
    for direction in [false, true] {
        let g = if direction { decoded } else { golden };
        let exact = ControlProjectionLimits {
            max_regions: g.region_count,
            max_use_references: g.use_reference_count,
            max_allocation_requests: g.allocation_requests_upper_bound,
            max_allocation_request_bytes: g.allocation_request_bytes_upper_bound,
            max_coexisting_source_and_request_bytes: g
                .coexisting_source_and_request_bytes_upper_bound,
            max_work: g.cumulative_work_upper_bound,
        };
        let c = Control::default();
        let mut w = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
        let mut snapshots = Vec::new();
        let out = if direction {
            decode_expression_control_observed(
                &f,
                &input,
                B,
                exact,
                &mut |facts| {
                    snapshots.push(*facts);
                    Ok(())
                },
                &mut w,
            )
            .map(|(_, f)| f)
        } else {
            encode_expression_control_observed(
                &r,
                B,
                exact,
                &mut |facts| {
                    snapshots.push(*facts);
                    Ok(())
                },
                &mut w,
            )
            .map(|(_, f)| f)
        };
        assert_eq!(out.unwrap(), g);
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
    }
    for direction in [false, true] {
        let g = if direction { decoded } else { golden };
        for axis in 0..6 {
            let mut l = limits();
            match axis {
                0 => l.max_regions = g.region_count - 1,
                1 => l.max_use_references = g.use_reference_count - 1,
                2 => l.max_allocation_requests = g.allocation_requests_upper_bound - 1,
                3 => l.max_allocation_request_bytes = g.allocation_request_bytes_upper_bound - 1,
                4 => {
                    l.max_coexisting_source_and_request_bytes =
                        g.coexisting_source_and_request_bytes_upper_bound - 1
                }
                _ => l.max_work = g.cumulative_work_upper_bound - 1,
            }
            let c = Control::default();
            let mut w = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
            let out = if direction {
                decode_expression_control_observed(&f, &input, B, l, &mut |_| Ok(()), &mut w)
                    .map(|(_, f)| f)
            } else {
                encode_expression_control_observed(&r, B, l, &mut |_| Ok(()), &mut w)
                    .map(|(_, f)| f)
            };
            assert!(matches!(
                out,
                Err(ControlCodecError::Control(
                    CompileControlError::ResourceExhausted
                ))
            ));
        }
    }
}
#[test]
fn known_output_headers_win_before_pending_control_and_source_floor_is_ordinary() {
    let (f, r) = fixture();
    let (input, _) = encode(&f, &r, None).0.unwrap();
    for pending in [0, 254, 255] {
        for cause in CAUSES {
            let c = Control {
                failure: Some((1, cause)),
                ..Default::default()
            };
            let mut w = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
            for _ in 0..pending {
                w.step().unwrap();
            }
            let mut l = limits();
            l.max_allocation_requests = 0;
            let out = decode_expression_control_observed(&f, &input, B, l, &mut |_| Ok(()), &mut w);
            assert!(matches!(
                out,
                Err(ControlCodecError::Control(
                    CompileControlError::ResourceExhausted
                ))
            ));
            assert_eq!(c.trace(), [(CompilePhase::Decode, 0)]);
        }
    }
    let c = Control::default();
    let mut w = CompileCheckpoints::try_new(&c, CompilePhase::Encode).unwrap();
    let out = encode_expression_control_observed(&r, 0, limits(), &mut |_| Ok(()), &mut w);
    assert!(matches!(
        out,
        Err(ControlCodecError::InvalidShape(
            "Control source invoice is below necessary backing"
        ))
    ));
    w.finish().unwrap();
    assert_eq!(
        c.trace(),
        [(CompilePhase::Encode, 0), (CompilePhase::Encode, 0)]
    );

    let mut spare = input.clone();
    let arguments = &mut spare
        .uses
        .iter_mut()
        .find(|invocation| !invocation.argument_use_ids.is_empty())
        .unwrap()
        .argument_use_ids;
    let original_len = arguments.len();
    arguments.reserve_exact(B * 2);
    assert_eq!(arguments.len(), original_len);
    assert!(Layout::array::<u32>(arguments.capacity()).unwrap().size() > B);
    let c = Control::default();
    let mut w = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
    let out = decode_expression_control_observed(&f, &spare, B, limits(), &mut |_| Ok(()), &mut w);
    assert!(matches!(
        out,
        Err(ControlCodecError::InvalidShape(
            "Control source invoice is below necessary backing"
        ))
    ));
    let before_footer = c.trace();
    w.finish().unwrap();
    let after_footer = c.trace();
    assert_eq!(&after_footer[..before_footer.len()], before_footer);
    assert_eq!(after_footer.len(), before_footer.len() + 1);
    assert_eq!(after_footer.last().unwrap().0, CompilePhase::Decode);

    // The original convenience facade has no caller source invoice.
    let legacy_control = Control::default();
    assert_eq!(
        decode_expression_control(&f, &spare, &legacy_control).unwrap(),
        r
    );
}
