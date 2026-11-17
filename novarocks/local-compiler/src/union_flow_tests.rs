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

use crate::{
    FragmentCompileError,
    union_flow::{UnionRoot, append_union_roots},
};
use novarocks_local_program::{
    ProgramChannelLayoutRole, ProgramChannelSite, ProgramExprId, ProgramExpressionArena,
    ProgramExpressionRootSite, ProgramExpressionUse, ProgramLexicalSource,
    ProgramNodeExpressionRole, ProgramNodeId, ProgramRootUseBinding, ProgramSlotBinding,
    ProgramUseRef,
};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, ControlShape, EvaluationDemand, EvaluationRegionId,
    ExpressionEffectContext, ExpressionEvaluationRegion, ExpressionUseId,
    MAX_CONTROL_USE_REFERENCES, PureCompileControl,
};
use std::{collections::BTreeSet, sync::Mutex};

#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut trace = self.trace.lock().unwrap();
        let ordinal = trace.len();
        trace.push((phase, units));
        match self.refusal.clone() {
            Some((stop, cause)) if stop == ordinal => Err(cause),
            _ => Ok(()),
        }
    }
}
impl Control {
    fn observed(&self) -> Vec<(CompilePhase, u32)> {
        self.trace.lock().unwrap().clone()
    }
}

#[derive(Clone, Debug, PartialEq)]
struct Records {
    domains: Vec<ExpressionEvaluationRegion>,
    uses: Vec<ProgramExpressionUse>,
    bindings: Vec<ProgramRootUseBinding>,
    slots: Vec<ProgramSlotBinding>,
}
impl Records {
    fn empty() -> Self {
        Self {
            domains: vec![],
            uses: vec![],
            bindings: vec![],
            slots: vec![],
        }
    }
    fn append(
        &mut self,
        roots: &[UnionRoot],
        control: &Control,
    ) -> Result<(), FragmentCompileError> {
        self.append_minted(roots, &BTreeSet::new(), control)
    }
    fn append_minted(
        &mut self,
        roots: &[UnionRoot],
        minted: &BTreeSet<ExpressionUseId>,
        control: &Control,
    ) -> Result<(), FragmentCompileError> {
        append_union_roots(
            roots,
            minted,
            &mut self.domains,
            &mut self.uses,
            &mut self.bindings,
            &mut self.slots,
            control,
        )
    }
    fn sparse() -> Self {
        let domains = [0, u32::MAX, 3]
            .map(|id| ExpressionEvaluationRegion {
                id: EvaluationRegionId::new(id),
                parent: None,
                guard: None,
            })
            .to_vec();
        let uses = [(0, 0), (u32::MAX, u32::MAX), (2, 3)]
            .map(|(id, domain)| ProgramExpressionUse {
                context: ExpressionEffectContext {
                    use_id: ExpressionUseId::new(id),
                    region: EvaluationRegionId::new(domain),
                    demand: EvaluationDemand::Value,
                },
                definition: ProgramExprId::new(0),
                control: ControlShape::Eager,
                arguments: Box::default(),
            })
            .to_vec();
        Self {
            domains,
            uses,
            bindings: vec![],
            slots: vec![],
        }
    }
}

fn input(node: usize, ordinal: u32) -> ProgramChannelSite {
    ProgramChannelSite::Layout {
        node: ProgramNodeId::new(node),
        role: ProgramChannelLayoutRole::NodeOutput,
        ordinal,
    }
}

#[test]
fn union_flow_sparse_max_ids_author_fresh_exact_root_contexts_and_channels() {
    let mut records = Records::sparse();
    let original = records.clone();
    let roots = [
        UnionRoot {
            node: ProgramNodeId::new(8),
            ordinal: 0,
            definition: ProgramExprId::new(5),
            source: input(4, 2),
        },
        UnionRoot {
            node: ProgramNodeId::new(8),
            ordinal: 1,
            definition: ProgramExprId::new(6),
            source: input(4, 0),
        },
        UnionRoot {
            node: ProgramNodeId::new(9),
            ordinal: 0,
            definition: ProgramExprId::new(7),
            source: input(6, 1),
        },
    ];
    records.append(&roots, &Control::default()).unwrap();
    assert_eq!(&records.domains[..3], &original.domains);
    assert_eq!(&records.uses[..3], &original.uses);
    // Hand-authored holes differ between the two independent ID namespaces.
    assert_eq!(
        records.domains[3..]
            .iter()
            .map(|d| d.id.get())
            .collect::<Vec<_>>(),
        [1, 2, 4]
    );
    assert_eq!(
        records.uses[3..]
            .iter()
            .map(|u| u.context.use_id.get())
            .collect::<Vec<_>>(),
        [1, 3, 4]
    );
    for (index, (use_id, domain, definition, node, ordinal, source)) in [
        (1, 1, 5, 8, 0, input(4, 2)),
        (3, 2, 6, 8, 1, input(4, 0)),
        (4, 4, 7, 9, 0, input(6, 1)),
    ]
    .into_iter()
    .enumerate()
    {
        let invocation = &records.uses[3 + index];
        assert_eq!(
            invocation.context,
            ExpressionEffectContext {
                use_id: ExpressionUseId::new(use_id),
                region: EvaluationRegionId::new(domain),
                demand: EvaluationDemand::Value,
            }
        );
        assert_eq!(invocation.definition, ProgramExprId::new(definition));
        assert_eq!(invocation.control, ControlShape::Eager);
        assert!(invocation.arguments.is_empty());
        assert_eq!(records.domains[3 + index].parent, None);
        assert_eq!(records.domains[3 + index].guard, None);
        assert_eq!(
            records.bindings[index],
            ProgramRootUseBinding {
                site: ProgramExpressionRootSite::Node {
                    node: ProgramNodeId::new(node),
                    role: ProgramNodeExpressionRole::ProjectOutput {
                        expression: ordinal
                    }
                },
                use_id: ExpressionUseId::new(use_id),
            }
        );
        assert_eq!(
            records.slots[index],
            ProgramSlotBinding {
                occurrence: ProgramUseRef {
                    arena: ProgramExpressionArena::Main,
                    use_id: ExpressionUseId::new(use_id)
                },
                source: ProgramLexicalSource::Input(source),
            }
        );
    }
}

// A package-minted identity that is not a flow occurrence, such as a
// relational call context, is never reused by a fresh occurrence.
#[test]
fn union_flow_fresh_uses_skip_every_package_minted_identity() {
    let mut records = Records::sparse();
    let roots = [0, 1, 2].map(|ordinal| UnionRoot {
        node: ProgramNodeId::new(8),
        ordinal,
        definition: ProgramExprId::new(5),
        source: input(4, 0),
    });
    // 1 and 3 are the smallest holes; 3 also stands for a minted occurrence
    // already in the flow's numbering.
    let minted = BTreeSet::from([1, 3, 4].map(ExpressionUseId::new));
    records
        .append_minted(&roots, &minted, &Control::default())
        .unwrap();
    assert_eq!(
        records.uses[3..]
            .iter()
            .map(|u| u.context.use_id.get())
            .collect::<Vec<_>>(),
        [5, 6, 7]
    );
    // Domains are an independent namespace and keep their smallest holes.
    assert_eq!(
        records.domains[3..]
            .iter()
            .map(|d| d.id.get())
            .collect::<Vec<_>>(),
        [1, 2, 4]
    );
}

#[test]
fn union_flow_duplicate_project_sources_keep_ordered_independent_occurrences() {
    let mut records = Records::empty();
    let source = input(3, 4);
    let roots = [0, 1, 2].map(|ordinal| UnionRoot {
        node: ProgramNodeId::new(7),
        ordinal,
        definition: ProgramExprId::new(11),
        source,
    });
    records.append(&roots, &Control::default()).unwrap();
    assert_eq!(records.uses.len(), 3);
    assert_eq!(records.domains.len(), 3);
    for (index, expected_id) in [0, 1, 2].into_iter().enumerate() {
        assert_eq!(records.uses[index].definition, ProgramExprId::new(11));
        assert_eq!(
            records.uses[index].context.use_id,
            ExpressionUseId::new(expected_id)
        );
        assert_eq!(
            records.uses[index].context.region,
            EvaluationRegionId::new(expected_id)
        );
        assert_eq!(
            records.bindings[index].site,
            ProgramExpressionRootSite::Node {
                node: ProgramNodeId::new(7),
                role: ProgramNodeExpressionRole::ProjectOutput {
                    expression: expected_id,
                },
            }
        );
        assert_eq!(
            records.slots[index].source,
            ProgramLexicalSource::Input(source)
        );
    }
    assert_ne!(records.slots[0].occurrence, records.slots[1].occurrence);
    assert_ne!(records.slots[1].occurrence, records.slots[2].occurrence);
    // These vectors test the append component, not a complete Project graph.
}

#[test]
fn union_flow_every_actual_small_control_prefix_preserves_all_three_first_causes() {
    let roots = [UnionRoot {
        node: ProgramNodeId::new(6),
        ordinal: 0,
        definition: ProgramExprId::new(3),
        source: input(2, 1),
    }];
    let control = Control::default();
    Records::sparse().append(&roots, &control).unwrap();
    let baseline = control.observed();
    assert_eq!(baseline[0], (CompilePhase::LowerProgram, 0));
    assert!(
        baseline.last().unwrap().1 > 0,
        "actual completed publication footer"
    );
    for stop in 0..baseline.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control {
                refusal: Some((stop, cause.clone())),
                ..Control::default()
            };
            let mut records = Records::sparse();
            assert!(matches!(records.append(&roots, &control),
                Err(FragmentCompileError::Control(actual)) if actual == cause));
            assert_eq!(control.observed(), baseline[..=stop]);
        }
    }
}

#[test]
fn union_flow_reference_ceiling_refuses_before_reserve_or_output_publication() {
    let mut records = Records::sparse();
    // Existing numerical references exactly fill the shared ceiling. This
    // input is a count-admission fixture, not a validated complete flow graph.
    let existing_uses = records.uses.len();
    records.uses[0].arguments =
        vec![ExpressionUseId::new(2); MAX_CONTROL_USE_REFERENCES - existing_uses]
            .into_boxed_slice();
    let roots = [UnionRoot {
        node: ProgramNodeId::new(6),
        ordinal: 0,
        definition: ProgramExprId::new(3),
        source: input(2, 1),
    }];
    let original = records.clone();
    let capacities = (
        records.domains.capacity(),
        records.uses.capacity(),
        records.bindings.capacity(),
        records.slots.capacity(),
    );
    let control = Control {
        refusal: Some((1, CompileControlError::Cancelled)),
        ..Control::default()
    };
    assert!(matches!(
        records.append(&roots, &control),
        Err(FragmentCompileError::Control(
            CompileControlError::ResourceExhausted
        ))
    ));
    assert_eq!(control.observed(), [(CompilePhase::LowerProgram, 0)]);
    assert_eq!(records, original);
    assert_eq!(
        (
            records.domains.capacity(),
            records.uses.capacity(),
            records.bindings.capacity(),
            records.slots.capacity()
        ),
        capacities
    );
}
