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

//! Exact runtime expression root fields in one immutable fragment.
//!
//! Field indices are frozen-plan positions, never ordinals in a flattened
//! expression list. Proof references do not create invocation sites.

use super::*;
use crate::{
    Distribution, ExprKind, FragmentBuilder, FragmentSink, LiteralValue, OutputPort, PhysicalNode,
    PhysicalProperties, PipelineDopDomain, RowMultiplicity, ValueType,
};
use arrow_schema::DataType;
use std::sync::Mutex;
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
fn fixture(id: u32) -> Fragment {
    let mut b = FragmentBuilder::new(FragmentId::new(id));
    let node = b.reserve_node_id().unwrap();
    let ty = ValueType::new(DataType::Int64, false);
    let expr = b
        .add_expression(node, ty.clone(), ExprKind::Literal(LiteralValue::Int64(3)))
        .unwrap();
    let a = b
        .add_value(
            ty.clone(),
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 0,
            },
        )
        .unwrap();
    let z = b
        .add_value(
            ty,
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 1,
            },
        )
        .unwrap();
    b.insert_node_unchecked(PhysicalNode {
        id: node,
        inputs: Box::default(),
        required_inputs: Box::default(),
        output_properties: PhysicalProperties {
            distribution: Distribution::Singleton,
            row_multiplicity: RowMultiplicity::SingleCopy,
            ordering: Box::default(),
        },
        output: OutputPort {
            node,
            columns: Box::from([a, z]),
        },
        kind: NodeKind::Values {
            rows: Box::from([Box::from([expr, expr])]),
        },
    })
    .unwrap();
    b.finish_definition(
        node,
        FragmentSink::Noop,
        PipelineDopDomain {
            min: 1,
            max: 1,
            requires_power_of_two: false,
        },
        crate::StagePlacementPolicy::Singleton,
    )
    .unwrap()
}
fn roots(f: &Fragment) -> PhysicalRootUses {
    let c = Control::default();
    let domain = novarocks_type_contract::EvaluationRegionId::new(0);
    let definition = *f.expressions().iter().next().unwrap().0;
    let inv = |id| novarocks_type_contract::ExpressionInvocation {
        context: novarocks_type_contract::ExpressionEffectContext {
            use_id: novarocks_type_contract::ExpressionUseId::new(id),
            region: domain,
            demand: EvaluationDemand::Value,
        },
        definition,
        control: novarocks_type_contract::ControlShape::Eager,
        arguments: Box::default(),
    };
    let flow = novarocks_type_contract::ExpressionControlFlow::try_new(
        vec![novarocks_type_contract::ExpressionEvaluationRegion {
            id: domain,
            parent: None,
            guard: None,
        }],
        vec![inv(0), inv(u32::MAX)],
        f.expressions(),
        CompilePhase::Validate,
        &c,
    )
    .unwrap();
    PhysicalRootUses::try_new(
        f,
        flow,
        vec![
            (
                ExpressionRootSite {
                    node: f.root(),
                    role: ExpressionRootRole::ValuesCell { row: 0, column: 0 },
                },
                novarocks_type_contract::ExpressionUseId::new(0),
            ),
            (
                ExpressionRootSite {
                    node: f.root(),
                    role: ExpressionRootRole::ValuesCell { row: 0, column: 1 },
                },
                novarocks_type_contract::ExpressionUseId::new(u32::MAX),
            ),
        ],
        &c,
    )
    .unwrap()
}
fn run(
    f: &Fragment,
    r: &PhysicalRootUses,
    fail: Option<(usize, CompileControlError)>,
) -> (Result<(), RootUseBindingError>, Vec<u32>) {
    let c = Control {
        fail,
        ..Default::default()
    };
    let out = (|| {
        let mut w = CompileCheckpoints::try_new(&c, CompilePhase::Validate)?;
        let out = r.validate_fragment_in(f, &mut |_| Ok(()), &mut w);
        if let Err(RootUseBindingError::Control(c)) = out {
            return Err(c.into());
        }
        w.finish()?;
        out
    })();
    let trace = c.trace.lock().unwrap().clone();
    (out, trace)
}
#[test]
fn actual_rebuilt_roots_keep_repeated_definition_sites_and_prefund_same_source_arc() {
    let f = fixture(771);
    let r = roots(&f);
    let c = Control::default();
    let mut w = CompileCheckpoints::try_new(&c, CompilePhase::Validate).unwrap();
    let mut finalfacts = None;
    let actual = PhysicalExpressionRoots::try_new_in(
        &f,
        &mut |facts| {
            finalfacts = Some(*facts);
            Ok(())
        },
        &mut w,
    )
    .unwrap();
    assert_eq!(actual.sites().len(), 2);
    assert_eq!(
        actual.sites().values().next().unwrap().expr,
        actual.sites().values().last().unwrap().expr
    );
    assert!(finalfacts.unwrap().allocation_requests_upper_bound >= 3);
    assert!(r.source_retained_floor().unwrap() >= std::mem::size_of::<PhysicalRootUses>());
}
#[test]
fn root_validation_actual_success_and_foreign_fragment_error_keep_all_original_prefixes() {
    let f = fixture(772);
    let r = roots(&f);
    let other = fixture(773);
    for source in [&f, &other] {
        let (out, trace) = run(source, &r, None);
        assert_eq!(out.is_ok(), source.id() == f.id());
        if source.id() != f.id() {
            assert_eq!(out.unwrap_err(), RootUseBindingError::WrongFragment);
        }
        for at in 0..trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let (out, actual) = run(source, &r, Some((at, cause.clone())));
                assert!(matches!(out,Err(RootUseBindingError::Control(c))if c==cause));
                assert_eq!(actual, trace[..=at]);
            }
        }
    }
}
#[test]
fn known_reconstruction_arc_is_admitted_before_late_controller() {
    let f = fixture(774);
    let r = roots(&f);
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
            let out = r.validate_fragment_in(
                &f,
                &mut |facts| {
                    assert!(facts.allocation_requests_upper_bound >= 1);
                    Err(CompileControlError::ResourceExhausted)
                },
                &mut w,
            );
            assert!(matches!(
                out,
                Err(RootUseBindingError::Control(
                    CompileControlError::ResourceExhausted
                ))
            ));
            assert_eq!(*c.trace.lock().unwrap(), [0]);
        }
    }
}
