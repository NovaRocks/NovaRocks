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
use novarocks_type_contract::{
    CompileControlError, CompilePhase, ControlShape, EvaluationDemand, EvaluationDomainId,
    ExpressionControlFlow, ExpressionEffectContext, ExpressionEvaluationDomain,
    ExpressionInvocation, ExpressionUseId, PureCompileControl,
};
use std::{collections::BTreeMap, sync::Mutex};

struct Control {
    observations: Mutex<Vec<(CompilePhase, u32)>>,
    fail: Option<CompileControlError>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        self.observations.lock().unwrap().push((phase, units));
        if units > 0
            && let Some(failure) = self.fail.clone()
        {
            return Err(failure);
        }
        Ok(())
    }
}
fn control() -> Control {
    Control {
        observations: Mutex::default(),
        fail: None,
    }
}
fn expr() -> ExprId {
    ExprId::new(u32::MAX)
}
fn alternate() -> ExprId {
    ExprId::new(1000)
}
fn node() -> NodeId {
    NodeId::new(91)
}
// This fixture isolates the root-field projection. Full relational validity
// is exercised by the real FragmentBuilder and plan-contract suites.
fn fragment(kind: NodeKind) -> Fragment {
    let mut expressions = ExprArena::default();
    for id in [expr(), alternate()] {
        expressions.insert(ExprNode {
            id,
            owner: node(),
            lambda_scope: None,
            ty: ty(DataType::Boolean, false),
            kind: ExprKind::Literal(LiteralValue::Boolean(true)),
        });
    }
    FragmentParts {
        summaries: Box::default(),
        stage_placement: crate::StagePlacementPolicy::Singleton,
        id: FragmentId::new(19),
        root: node(),
        values: BTreeMap::new(),
        expressions,
        nodes: BTreeMap::from([(
            node(),
            PhysicalNode {
                id: node(),
                inputs: Box::default(),
                required_inputs: Box::default(),
                output_properties: singleton(),
                output: OutputPort {
                    node: node(),
                    columns: Box::default(),
                },
                kind,
            },
        )]),
        sink: FragmentSink::Noop,
        dop_domain: PipelineDopDomain {
            min: 1,
            max: 1,
            requires_power_of_two: false,
        },
        runtime_filters: Box::default(),
        call_requests: crate::FragmentCallRequests::unpublished_empty(FragmentId::new(19)),
    }
    .into()
}
fn site(role: ExpressionRootRole) -> ExpressionRootSite {
    ExpressionRootSite { node: node(), role }
}
fn uses(fragment: &Fragment) -> PhysicalExpressionRoots {
    PhysicalExpressionRoots::try_new(fragment, &control()).unwrap()
}
fn graph(
    fragment: &Fragment,
    definitions: &[(u32, ExprId, EvaluationDemand)],
) -> ExpressionControlFlow<ExprId> {
    let domain = EvaluationDomainId::new(u32::MAX);
    ExpressionControlFlow::try_new(
        vec![ExpressionEvaluationDomain {
            id: domain,
            parent: None,
            guard: None,
        }],
        definitions
            .iter()
            .map(|(id, definition, demand)| ExpressionInvocation {
                context: ExpressionEffectContext {
                    use_id: ExpressionUseId::new(*id),
                    domain,
                    demand: *demand,
                },
                definition: *definition,
                control: ControlShape::Eager,
                arguments: Box::default(),
            })
            .collect(),
        fragment.expressions(),
        CompilePhase::Validate,
        &control(),
    )
    .unwrap()
}

#[test]
fn exact_field_sites_keep_repeated_definitions_and_join_demands() {
    use EvaluationDemand::{TruthOnly, Value};
    use ExpressionRootRole::*;
    let filter = fragment(NodeKind::Filter {
        predicates: Box::from([expr(), expr()]),
    });
    assert_eq!(
        uses(&filter).sites(),
        &BTreeMap::from([
            (
                site(FilterPredicate { predicate: 0 }),
                ExprUse {
                    expr: expr(),
                    demand: TruthOnly
                }
            ),
            (
                site(FilterPredicate { predicate: 1 }),
                ExprUse {
                    expr: expr(),
                    demand: TruthOnly
                }
            )
        ])
    );
    let project = fragment(NodeKind::Project {
        expressions: Box::from([(expr(), ValueId::new(0)), (expr(), ValueId::new(1))]),
    });
    assert_eq!(uses(&project).sites().len(), 2);
    assert!(
        uses(&project)
            .sites()
            .values()
            .all(|root| root.demand == Value)
    );
    for (kind, demand) in [
        (JoinKind::Inner, TruthOnly),
        (JoinKind::NullAwareLeftAnti, Value),
    ] {
        let join = fragment(NodeKind::HashJoin {
            kind,
            condition: if kind == crate::JoinKind::NullAwareLeftAnti {
                crate::HashJoinCondition::NullAwareEqual {
                    value_equal_keys: Box::from([crate::JoinKey {
                        left: expr(),
                        right: expr(),
                        null_safe: false,
                    }]),
                    scope_keys: Box::default(),
                    eligibility: Some(expr()),
                }
            } else {
                crate::HashJoinCondition::Ordinary {
                    keys: Box::from([crate::JoinKey {
                        left: expr(),
                        right: expr(),
                        null_safe: false,
                    }]),
                    residual: Some(expr()),
                }
            },
            build_side: JoinSide::Right,
            distribution: JoinDistribution::BroadcastBuild,
            null_extended: Box::default(),
        });
        assert_eq!(
            uses(&join).sites(),
            &BTreeMap::from([
                (
                    site(if kind == JoinKind::NullAwareLeftAnti {
                        NaaValueEqualKey {
                            key: 0,
                            side: JoinSide::Left,
                        }
                    } else {
                        JoinKey {
                            key: 0,
                            side: JoinSide::Left,
                        }
                    }),
                    ExprUse {
                        expr: expr(),
                        demand: Value
                    }
                ),
                (
                    site(if kind == JoinKind::NullAwareLeftAnti {
                        NaaValueEqualKey {
                            key: 0,
                            side: JoinSide::Right,
                        }
                    } else {
                        JoinKey {
                            key: 0,
                            side: JoinSide::Right,
                        }
                    }),
                    ExprUse {
                        expr: expr(),
                        demand: Value
                    }
                ),
                (
                    site(if kind == JoinKind::NullAwareLeftAnti {
                        NaaEligibility
                    } else {
                        HashJoinResidual
                    }),
                    ExprUse {
                        expr: expr(),
                        demand
                    }
                )
            ])
        );
    }
}

#[test]
fn optional_and_non_scalar_fields_keep_original_multidimensional_positions() {
    use ExpressionRootRole::*;
    let changes = fragment(NodeKind::ChangeEventExpand {
        events: Box::from([
            ChangeEventSpec {
                predicate: None,
                effect: novarocks_connector_contract::ConnectorRowMutationEffect::Insert,
                assignments: Box::from([(ValueId::new(0), None), (ValueId::new(1), Some(expr()))]),
            },
            ChangeEventSpec {
                predicate: Some(alternate()),
                effect: novarocks_connector_contract::ConnectorRowMutationEffect::Insert,
                assignments: Box::default(),
            },
        ]),
        effect_output: ValueId::new(9),
    });
    let roots = uses(&changes);
    assert_eq!(roots.sites().len(), 2);
    assert!(roots.sites().contains_key(&site(ChangeAssignment {
        event: 0,
        assignment: 1
    })));
    assert_eq!(
        roots.sites()[&site(ChangePredicate { event: 1 })].expr,
        alternate()
    );
    let unpivot = fragment(NodeKind::Unpivot {
        spec: UnpivotSpec {
            passthrough: Box::default(),
            value_output: ValueId::new(0),
            literal_outputs: Box::default(),
            mappings: Box::from([UnpivotValueMapping {
                input: ValueId::new(1),
                constants: Box::from([
                    // Root-site extraction deliberately does not claim pool admission.
                    crate::UnpivotConstant::Int32List(crate::ConstantReference {
                        pool: crate::ConstantPoolId::new(u32::MAX),
                        ordinal: 1,
                    }),
                    crate::UnpivotConstant::Scalar(expr()),
                ]),
            }]),
            max_output_rows: 64,
            max_output_bytes: 1024,
        },
    });
    assert_eq!(
        uses(&unpivot).sites().keys().copied().collect::<Vec<_>>(),
        vec![site(UnpivotConstant {
            mapping: 0,
            constant: 1
        })]
    );
    let cells = fragment(NodeKind::Values {
        rows: Box::from([Box::default(), Box::from([expr(), alternate()])]),
    });
    assert_eq!(
        uses(&cells).sites().keys().copied().collect::<Vec<_>>(),
        vec![
            site(ValuesCell { row: 1, column: 0 }),
            site(ValuesCell { row: 1, column: 1 })
        ]
    );
}

#[test]
fn root_binding_requires_exact_complete_coverage_definition_and_demand() {
    use EvaluationDemand::{TruthOnly, Value};
    use ExpressionRootRole::*;
    let fragment = fragment(NodeKind::Filter {
        predicates: Box::from([expr(), expr()]),
    });
    let bindings = vec![
        (
            site(FilterPredicate { predicate: 0 }),
            ExpressionUseId::new(1000),
        ),
        (
            site(FilterPredicate { predicate: 1 }),
            ExpressionUseId::new(u32::MAX),
        ),
    ];
    let flow = graph(
        &fragment,
        &[(1000, expr(), TruthOnly), (u32::MAX, expr(), TruthOnly)],
    );
    let bound =
        PhysicalRootUses::try_new(&fragment, flow.clone(), bindings.clone(), &control()).unwrap();
    assert_eq!(bound.bindings().len(), 2);
    assert_eq!(bound.flow(), &flow);
    assert_eq!(
        PhysicalRootUses::try_new(&fragment, flow.clone(), vec![], &control()).unwrap_err(),
        RootUseBindingError::IncompleteCoverage
    );
    let mut shared = bindings.clone();
    shared[1].1 = shared[0].1;
    assert_eq!(
        PhysicalRootUses::try_new(&fragment, flow.clone(), shared, &control()).unwrap_err(),
        RootUseBindingError::SharedUse
    );
    let mut duplicate = bindings.clone();
    duplicate[1].0 = duplicate[0].0;
    assert_eq!(
        PhysicalRootUses::try_new(&fragment, flow.clone(), duplicate, &control()).unwrap_err(),
        RootUseBindingError::DuplicateSite
    );
    let mut wrong_site = bindings.clone();
    wrong_site[0].0.role = ProjectOutput { expression: 0 };
    assert_eq!(
        PhysicalRootUses::try_new(&fragment, flow.clone(), wrong_site, &control()).unwrap_err(),
        RootUseBindingError::InvalidSite
    );
    let mut wrong_use = bindings.clone();
    wrong_use[0].1 = ExpressionUseId::new(91);
    assert_eq!(
        PhysicalRootUses::try_new(&fragment, flow.clone(), wrong_use, &control()).unwrap_err(),
        RootUseBindingError::InvalidUse
    );
    let wrong_definition = graph(
        &fragment,
        &[
            (1000, alternate(), TruthOnly),
            (u32::MAX, expr(), TruthOnly),
        ],
    );
    assert_eq!(
        PhysicalRootUses::try_new(&fragment, wrong_definition, bindings.clone(), &control())
            .unwrap_err(),
        RootUseBindingError::WrongDefinition
    );
    let wrong_demand = graph(
        &fragment,
        &[(1000, expr(), Value), (u32::MAX, expr(), TruthOnly)],
    );
    assert_eq!(
        PhysicalRootUses::try_new(&fragment, wrong_demand, bindings, &control()).unwrap_err(),
        RootUseBindingError::WrongDemand
    );
}

#[test]
fn empty_fields_observe_typed_control_failure_before_unbounded_traversal() {
    for failure in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let fragment = fragment(NodeKind::Values {
            rows: vec![Box::default(); 1000].into_boxed_slice(),
        });
        let owner = Control {
            observations: Mutex::default(),
            fail: Some(failure.clone()),
        };
        assert_eq!(
            PhysicalExpressionRoots::try_new(&fragment, &owner).unwrap_err(),
            ExpressionRootError::Control(failure)
        );
        assert_eq!(
            *owner.observations.lock().unwrap(),
            vec![(CompilePhase::Validate, 0), (CompilePhase::Validate, 256)]
        );
    }
}

#[test]
fn scan_derived_roots_cover_real_value_origins_and_exclude_provider_proofs() {
    let proof_only = finish_scan_predicate_contract(&[PredicateGuaranteeKind::Exact], 0).unwrap();
    assert!(uses(&proof_only).sites().is_empty());
    let rechecked = finish_scan_predicate_contract(&[PredicateGuaranteeKind::Exact], 1).unwrap();
    let roots = uses(&rechecked);
    assert_eq!(roots.sites().len(), 1);
    assert_eq!(
        roots.sites().keys().next().unwrap().role,
        ExpressionRootRole::ScanResidual { predicate: 0 }
    );
    let binding = connector_binding();
    let column = ProviderColumnReference {
        column_payload: encoded(&binding, ConnectorCodecCategory::ReadColumn, 9),
    };
    let make = |origin: ValueOrigin| {
        let source = fragment(NodeKind::Scan {
            occurrence: ProviderReadOccurrenceId::new(1),
            relation: Box::new(metadata_relation(&binding, column.clone())),
            read_budget: scan_budget(),
            provider_outputs: Box::default(),
            residuals: Box::from([expr()]),
            derived_values: Box::from([ValueId::new(5)]),
        });
        Fragment::from(FragmentParts {
            summaries: Box::default(),
            stage_placement: crate::StagePlacementPolicy::Singleton,
            id: source.id(),
            root: source.root(),
            values: BTreeMap::from([(
                ValueId::new(5),
                ValueDef {
                    id: ValueId::new(5),
                    ty: ty(DataType::Boolean, false),
                    origin,
                },
            )]),
            expressions: source.expressions().clone(),
            nodes: source.nodes().clone(),
            sink: source.sink().clone(),
            dop_domain: source.dop_domain(),
            runtime_filters: source.runtime_filters().into(),
            call_requests: source.call_requests().clone(),
        })
    };
    let scan = make(ValueOrigin::Expr {
        node: node(),
        expr: alternate(),
    });
    assert_eq!(uses(&scan).sites().len(), 2);
    assert_eq!(
        uses(&scan).sites()[&site(ExpressionRootRole::ScanDerived { derived: 0 })],
        ExprUse {
            expr: alternate(),
            demand: EvaluationDemand::Value
        }
    );
    let wrong_origin = make(ValueOrigin::NodeOutput {
        node: node(),
        output_ordinal: 0,
    });
    assert_eq!(
        PhysicalExpressionRoots::try_new(&wrong_origin, &control()).unwrap_err(),
        ExpressionRootError::InvalidDerivedValue
    );
    let wrong_owner = make(ValueOrigin::Expr {
        node: NodeId::new(92),
        expr: alternate(),
    });
    assert_eq!(
        PhysicalExpressionRoots::try_new(&wrong_owner, &control()).unwrap_err(),
        ExpressionRootError::InvalidDerivedValue
    );
    let absent_expression = make(ValueOrigin::Expr {
        node: node(),
        expr: ExprId::new(2),
    });
    assert_eq!(
        PhysicalExpressionRoots::try_new(&absent_expression, &control()).unwrap_err(),
        ExpressionRootError::InvalidExpressionOwner
    );
}

#[test]
fn root_count_has_the_shared_near_and_over_occurrence_bounds() {
    let count = novarocks_type_contract::MAX_CONTROL_USE_REFERENCES;
    let near = fragment(NodeKind::Values {
        rows: Box::from([vec![expr(); count].into_boxed_slice()]),
    });
    assert_eq!(uses(&near).sites().len(), count);
    let over = fragment(NodeKind::Values {
        rows: Box::from([vec![expr(); count + 1].into_boxed_slice()]),
    });
    assert_eq!(
        PhysicalExpressionRoots::try_new(&over, &control()).unwrap_err(),
        ExpressionRootError::TooManyRoots
    );
}

#[test]
fn an_argument_use_cannot_replace_an_operator_root() {
    let fragment = fragment(NodeKind::Filter {
        predicates: Box::from([expr(), expr()]),
    });
    let domain = EvaluationDomainId::new(0);
    let invoke = |id, arguments| ExpressionInvocation {
        context: ExpressionEffectContext {
            use_id: ExpressionUseId::new(id),
            domain,
            demand: EvaluationDemand::TruthOnly,
        },
        definition: expr(),
        control: ControlShape::Conjunction,
        arguments,
    };
    let mut child = invoke(91, Box::default());
    child.control = ControlShape::Eager;
    let mut other = invoke(u32::MAX, Box::default());
    other.control = ControlShape::Eager;
    let flow = ExpressionControlFlow::try_new(
        vec![ExpressionEvaluationDomain {
            id: domain,
            parent: None,
            guard: None,
        }],
        vec![
            invoke(1000, Box::from([ExpressionUseId::new(91)])),
            child,
            other,
        ],
        fragment.expressions(),
        CompilePhase::Validate,
        &control(),
    )
    .unwrap();
    let bindings = vec![
        (
            site(ExpressionRootRole::FilterPredicate { predicate: 0 }),
            ExpressionUseId::new(91),
        ),
        (
            site(ExpressionRootRole::FilterPredicate { predicate: 1 }),
            ExpressionUseId::new(u32::MAX),
        ),
    ];
    assert_eq!(
        PhysicalRootUses::try_new(&fragment, flow, bindings, &control()).unwrap_err(),
        RootUseBindingError::InvalidUse
    );
}

#[test]
fn operator_roots_cannot_escape_from_a_lambda_lexical_scope() {
    let source = fragment(NodeKind::Project {
        expressions: Box::from([(expr(), ValueId::new(0))]),
    });
    let mut definitions = source.expressions().clone();
    let mut nested = definitions.get(expr()).unwrap().clone();
    nested.lambda_scope = Some(alternate());
    definitions.insert(nested);
    let fragment = Fragment::from(FragmentParts {
        summaries: Box::default(),
        stage_placement: crate::StagePlacementPolicy::Singleton,
        id: source.id(),
        root: source.root(),
        values: source.values().clone(),
        expressions: definitions,
        nodes: source.nodes().clone(),
        sink: source.sink().clone(),
        dop_domain: source.dop_domain(),
        runtime_filters: source.runtime_filters().into(),
        call_requests: source.call_requests().clone(),
    });
    assert_eq!(
        PhysicalExpressionRoots::try_new(&fragment, &control()).unwrap_err(),
        ExpressionRootError::InvalidExpressionScope
    );
}

#[test]
fn a_guarded_disconnected_use_cannot_become_an_operator_root() {
    use novarocks_type_contract::{DomainGuard, GuardKind};
    let fragment = fragment(NodeKind::Filter {
        predicates: Box::from([expr(), expr()]),
    });
    let domain = EvaluationDomainId::new(0);
    let branch = EvaluationDomainId::new(1);
    let otherwise = EvaluationDomainId::new(2);
    let invoke = |id, domain| ExpressionInvocation {
        context: ExpressionEffectContext {
            use_id: ExpressionUseId::new(id),
            domain,
            demand: EvaluationDemand::TruthOnly,
        },
        definition: expr(),
        control: ControlShape::Eager,
        arguments: Box::default(),
    };
    let mut owner = invoke(1000, domain);
    owner.control = ControlShape::If;
    owner.arguments = Box::from([
        ExpressionUseId::new(91),
        ExpressionUseId::new(92),
        ExpressionUseId::new(93),
    ]);
    let guarded = |id, kind| ExpressionEvaluationDomain {
        id,
        parent: Some(domain),
        guard: Some(DomainGuard {
            owner: owner.context.use_id,
            kind,
        }),
    };
    let flow = ExpressionControlFlow::try_new(
        vec![
            ExpressionEvaluationDomain {
                id: domain,
                parent: None,
                guard: None,
            },
            guarded(branch, GuardKind::IfThen),
            guarded(otherwise, GuardKind::IfElse),
        ],
        vec![
            owner,
            invoke(91, domain),
            invoke(92, branch),
            invoke(93, otherwise),
            invoke(u32::MAX, branch),
        ],
        fragment.expressions(),
        CompilePhase::Validate,
        &control(),
    )
    .unwrap();
    let bindings = vec![
        (
            site(ExpressionRootRole::FilterPredicate { predicate: 0 }),
            ExpressionUseId::new(1000),
        ),
        (
            site(ExpressionRootRole::FilterPredicate { predicate: 1 }),
            ExpressionUseId::new(u32::MAX),
        ),
    ];
    assert_eq!(
        PhysicalRootUses::try_new(&fragment, flow, bindings, &control()).unwrap_err(),
        RootUseBindingError::GuardedRoot
    );
}

fn conjunct_fragment() -> Fragment {
    let source = fragment(NodeKind::Filter {
        predicates: Box::from([ExprId::new(5000)]),
    });
    let mut expressions = source.expressions().clone();
    for (id, args) in [
        (ExprId::new(5000), vec![alternate(), expr()]),
        (alternate(), vec![expr(), expr()]),
    ] {
        expressions.insert(ExprNode {
            id,
            owner: node(),
            lambda_scope: None,
            ty: ty(DataType::Boolean, true),
            kind: ExprKind::Conjunction {
                args: args.into_boxed_slice(),
            },
        });
    }
    Fragment::from(FragmentParts {
        summaries: Box::default(),
        stage_placement: crate::StagePlacementPolicy::Singleton,
        id: source.id(),
        root: source.root(),
        values: source.values().clone(),
        expressions,
        nodes: source.nodes().clone(),
        sink: source.sink().clone(),
        dop_domain: source.dop_domain(),
        runtime_filters: source.runtime_filters().into(),
        call_requests: source.call_requests().clone(),
    })
}
fn conjunct_uses(
    fragment: &Fragment,
    eager_root: bool,
    swapped: bool,
) -> Result<PhysicalRootUses, RootUseBindingError> {
    let domain = EvaluationDomainId::new(0);
    let demand = if eager_root {
        EvaluationDemand::Value
    } else {
        EvaluationDemand::TruthOnly
    };
    let invoke = |id, definition, control, arguments, demand| ExpressionInvocation {
        context: ExpressionEffectContext {
            use_id: ExpressionUseId::new(id),
            domain,
            demand,
        },
        definition,
        control,
        arguments,
    };
    let arguments = if swapped {
        vec![ExpressionUseId::new(52), ExpressionUseId::new(51)]
    } else {
        vec![ExpressionUseId::new(51), ExpressionUseId::new(52)]
    };
    let flow = ExpressionControlFlow::try_new(
        vec![ExpressionEvaluationDomain {
            id: domain,
            parent: None,
            guard: None,
        }],
        vec![
            invoke(
                50,
                ExprId::new(5000),
                if eager_root {
                    ControlShape::Eager
                } else {
                    ControlShape::Conjunction
                },
                arguments.into_boxed_slice(),
                EvaluationDemand::TruthOnly,
            ),
            invoke(
                51,
                alternate(),
                ControlShape::Conjunction,
                Box::from([ExpressionUseId::new(53), ExpressionUseId::new(54)]),
                demand,
            ),
            invoke(52, expr(), ControlShape::Eager, Box::default(), demand),
            invoke(53, expr(), ControlShape::Eager, Box::default(), demand),
            invoke(54, expr(), ControlShape::Eager, Box::default(), demand),
        ],
        fragment.expressions(),
        CompilePhase::Validate,
        &control(),
    )
    .unwrap();
    PhysicalRootUses::try_new(
        fragment,
        flow,
        vec![(
            site(ExpressionRootRole::FilterPredicate { predicate: 0 }),
            ExpressionUseId::new(50),
        )],
        &control(),
    )
}
#[test]
fn positive_conjunct_sources_keep_actual_use_occurrences_and_nullable_truth_demand() {
    let fragment = conjunct_fragment();
    let roots = conjunct_uses(&fragment, false, false).unwrap();
    let site = site(ExpressionRootRole::FilterPredicate { predicate: 0 });
    let identity =
        PredicateConjunctSource::try_new(&fragment, &roots, site, vec![], &control()).unwrap();
    assert_eq!(identity.definition(), ExprId::new(5000));
    assert_eq!(identity.context().use_id, ExpressionUseId::new(50));
    let nested =
        PredicateConjunctSource::try_new(&fragment, &roots, site, vec![0, 1], &control()).unwrap();
    let repeated =
        PredicateConjunctSource::try_new(&fragment, &roots, site, vec![1], &control()).unwrap();
    assert_eq!(nested.definition(), repeated.definition());
    assert_ne!(nested.context().use_id, repeated.context().use_id);
    assert_eq!(nested.context().demand, EvaluationDemand::TruthOnly);
    assert_eq!(nested.argument_ordinals(), &[0, 1]);
    assert_eq!(
        nested.responsibility().anchor(),
        PredicateResponsibilityRef {
            fragment: fragment.id(),
            site,
            use_id: ExpressionUseId::new(50)
        }
    );
    assert_eq!(
        PredicateConjunctSource::try_new(&fragment, &roots, site, vec![2], &control()).unwrap_err(),
        PredicateSourceError::InvalidArgument
    );
    assert_eq!(
        PredicateConjunctSource::try_new(&fragment, &roots, site, vec![1, 0], &control())
            .unwrap_err(),
        PredicateSourceError::NotPositiveConjunction
    );
    assert_eq!(
        PredicateConjunctSource::try_new(
            &fragment,
            &roots,
            site,
            vec![0; novarocks_type_contract::MAX_CONTROL_DEPTH],
            &control()
        )
        .unwrap_err(),
        PredicateSourceError::TooDeep
    );
}
#[test]
fn a_predicate_source_cannot_cross_forged_control_or_reordered_use_edges() {
    let fragment = conjunct_fragment();
    // Intrinsic correspondence now rejects both before a predicate witness
    // can be constructed; an invalid graph is not a usable root authority.
    assert_eq!(
        conjunct_uses(&fragment, true, false).unwrap_err(),
        RootUseBindingError::WrongControl
    );
    assert_eq!(
        conjunct_uses(&fragment, false, true).unwrap_err(),
        RootUseBindingError::WrongArguments
    );
}

#[test]
fn truth_only_change_condition_does_not_grant_relation_search_responsibility() {
    let fragment = fragment(NodeKind::ChangeEventExpand {
        events: Box::from([ChangeEventSpec {
            predicate: Some(expr()),
            effect: novarocks_connector_contract::ConnectorRowMutationEffect::Insert,
            assignments: Box::default(),
        }]),
        effect_output: ValueId::new(0),
    });
    let site = site(ExpressionRootRole::ChangePredicate { event: 0 });
    let roots = PhysicalRootUses::try_new(
        &fragment,
        graph(&fragment, &[(0, expr(), EvaluationDemand::TruthOnly)]),
        vec![(site, ExpressionUseId::new(0))],
        &control(),
    )
    .unwrap();
    assert_eq!(
        ExactPredicateResponsibility::try_new(&fragment, &roots, site, &control()).unwrap_err(),
        PredicateSourceError::NotSearchPredicate
    );
}

#[test]
fn source_responsibility_rechecks_the_actual_fragment_field_and_boolean_type() {
    let source = conjunct_fragment();
    let roots = conjunct_uses(&source, false, false).unwrap();
    let site = site(ExpressionRootRole::FilterPredicate { predicate: 0 });
    let clone = |id, nodes, expressions| {
        Fragment::from(FragmentParts {
            summaries: source.summaries().into(),
            stage_placement: crate::StagePlacementPolicy::Singleton,
            id,
            root: source.root(),
            values: source.values().clone(),
            expressions,
            nodes,
            sink: source.sink().clone(),
            dop_domain: source.dop_domain(),
            runtime_filters: source.runtime_filters().into(),
            call_requests: source.call_requests().clone(),
        })
    };
    let different_fragment = clone(
        FragmentId::new(20),
        source.nodes().clone(),
        source.expressions().clone(),
    );
    assert_eq!(
        ExactPredicateResponsibility::try_new(&different_fragment, &roots, site, &control())
            .unwrap_err(),
        PredicateSourceError::InvalidFragment
    );
    let mut nodes = source.nodes().clone();
    nodes.get_mut(&node()).unwrap().kind = NodeKind::Filter {
        predicates: Box::from([expr()]),
    };
    let different_field = clone(source.id(), nodes, source.expressions().clone());
    assert_eq!(
        ExactPredicateResponsibility::try_new(&different_field, &roots, site, &control())
            .unwrap_err(),
        PredicateSourceError::InvalidUse
    );
    let mut definitions = source.expressions().clone();
    let mut non_boolean = definitions.get(ExprId::new(5000)).unwrap().clone();
    non_boolean.ty = ty(DataType::Int64, false);
    definitions.insert(non_boolean);
    let different_type = clone(source.id(), source.nodes().clone(), definitions);
    assert_eq!(
        ExactPredicateResponsibility::try_new(&different_type, &roots, site, &control())
            .unwrap_err(),
        PredicateSourceError::NotBoolean
    );
}
#[test]
fn positive_conjunct_paths_do_not_cross_not_or_null_testing() {
    for null_testing in [false, true] {
        let source = fragment(NodeKind::Filter {
            predicates: Box::from([alternate()]),
        });
        let mut expressions = source.expressions().clone();
        let mut parent = expressions.get(alternate()).unwrap().clone();
        parent.kind = if null_testing {
            ExprKind::IsNull {
                expr: expr(),
                negated: false,
            }
        } else {
            ExprKind::Unary {
                op: UnaryOperator::Not,
                expr: expr(),
            }
        };
        expressions.insert(parent);
        let fragment = Fragment::from(FragmentParts {
            summaries: Box::default(),
            stage_placement: crate::StagePlacementPolicy::Singleton,
            id: source.id(),
            root: source.root(),
            values: source.values().clone(),
            expressions,
            nodes: source.nodes().clone(),
            sink: source.sink().clone(),
            dop_domain: source.dop_domain(),
            runtime_filters: source.runtime_filters().into(),
            call_requests: source.call_requests().clone(),
        });
        let domain = EvaluationDomainId::new(0);
        let invoke = |id, definition, demand, arguments| ExpressionInvocation {
            context: ExpressionEffectContext {
                use_id: ExpressionUseId::new(id),
                domain,
                demand,
            },
            definition,
            control: ControlShape::Eager,
            arguments,
        };
        let flow = ExpressionControlFlow::try_new(
            vec![ExpressionEvaluationDomain {
                id: domain,
                parent: None,
                guard: None,
            }],
            vec![
                invoke(
                    0,
                    alternate(),
                    EvaluationDemand::TruthOnly,
                    Box::from([ExpressionUseId::new(1)]),
                ),
                invoke(1, expr(), EvaluationDemand::Value, Box::default()),
            ],
            fragment.expressions(),
            CompilePhase::Validate,
            &control(),
        )
        .unwrap();
        let site = site(ExpressionRootRole::FilterPredicate { predicate: 0 });
        let roots = PhysicalRootUses::try_new(
            &fragment,
            flow,
            vec![(site, ExpressionUseId::new(0))],
            &control(),
        )
        .unwrap();
        assert_eq!(
            PredicateConjunctSource::try_new(&fragment, &roots, site, vec![0], &control())
                .unwrap_err(),
            PredicateSourceError::NotPositiveConjunction
        );
    }
}

#[test]
fn positive_source_depth_matches_the_checked_invocation_depth_bound() {
    let source = fragment(NodeKind::Filter {
        predicates: Box::from([ExprId::new(0)]),
    });
    let mut expressions = source.expressions().clone();
    let depth = novarocks_type_contract::MAX_CONTROL_DEPTH;
    for index in 0..depth as u32 {
        expressions.insert(ExprNode {
            id: ExprId::new(index),
            owner: node(),
            lambda_scope: None,
            ty: ty(DataType::Boolean, true),
            kind: if index as usize + 1 == depth {
                ExprKind::Literal(LiteralValue::Null)
            } else {
                ExprKind::Conjunction {
                    args: Box::from([ExprId::new(index + 1)]),
                }
            },
        });
    }
    let fragment = Fragment::from(FragmentParts {
        summaries: Box::default(),
        stage_placement: crate::StagePlacementPolicy::Singleton,
        id: source.id(),
        root: source.root(),
        values: source.values().clone(),
        expressions,
        nodes: source.nodes().clone(),
        sink: source.sink().clone(),
        dop_domain: source.dop_domain(),
        runtime_filters: source.runtime_filters().into(),
        call_requests: source.call_requests().clone(),
    });
    let domain = EvaluationDomainId::new(0);
    let invocations = (0..depth as u32)
        .map(|index| ExpressionInvocation {
            context: ExpressionEffectContext {
                use_id: ExpressionUseId::new(index),
                domain,
                demand: EvaluationDemand::TruthOnly,
            },
            definition: ExprId::new(index),
            control: if index as usize + 1 == depth {
                ControlShape::Eager
            } else {
                ControlShape::Conjunction
            },
            arguments: if index as usize + 1 == depth {
                Box::default()
            } else {
                Box::from([ExpressionUseId::new(index + 1)])
            },
        })
        .collect();
    let flow = ExpressionControlFlow::try_new(
        vec![ExpressionEvaluationDomain {
            id: domain,
            parent: None,
            guard: None,
        }],
        invocations,
        fragment.expressions(),
        CompilePhase::Validate,
        &control(),
    )
    .unwrap();
    let site = site(ExpressionRootRole::FilterPredicate { predicate: 0 });
    let roots = PhysicalRootUses::try_new(
        &fragment,
        flow,
        vec![(site, ExpressionUseId::new(0))],
        &control(),
    )
    .unwrap();
    let near =
        PredicateConjunctSource::try_new(&fragment, &roots, site, vec![0; depth - 1], &control())
            .unwrap();
    assert_eq!(near.definition(), ExprId::new(depth as u32 - 1));
    assert!(
        fragment
            .expressions()
            .get(near.definition())
            .unwrap()
            .ty
            .nullable()
    );
    assert_eq!(
        PredicateConjunctSource::try_new(&fragment, &roots, site, vec![0; depth], &control())
            .unwrap_err(),
        PredicateSourceError::TooDeep
    );
    for failure in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let owner = Control {
            observations: Mutex::default(),
            fail: Some(failure.clone()),
        };
        assert_eq!(
            PredicateConjunctSource::try_new(&fragment, &roots, site, vec![0; depth - 1], &owner)
                .unwrap_err(),
            PredicateSourceError::Control(failure)
        );
        assert!(
            owner
                .observations
                .lock()
                .unwrap()
                .iter()
                .all(|(phase, work)| *phase == CompilePhase::Validate && *work <= 256)
        );
    }
}

#[path = "../expression_site/failure_tail_tests.rs"]
mod failure_tail_tests;

#[test]
fn naa_scope_and_value_roots_keep_separate_ordinals_and_original_value_demand() {
    use ExpressionRootRole::*;
    let join = fragment(NodeKind::HashJoin {
        kind: JoinKind::NullAwareLeftAnti,
        condition: crate::HashJoinCondition::NullAwareEqual {
            value_equal_keys: Box::from([crate::JoinKey {
                left: expr(),
                right: expr(),
                null_safe: false,
            }]),
            scope_keys: Box::from([crate::JoinKey {
                left: expr(),
                right: expr(),
                null_safe: true,
            }]),
            eligibility: Some(expr()),
        },
        build_side: JoinSide::Right,
        distribution: JoinDistribution::BroadcastBuild,
        null_extended: Box::default(),
    });
    let roots = uses(&join);
    for role in [
        NaaValueEqualKey {
            key: 0,
            side: JoinSide::Left,
        },
        NaaValueEqualKey {
            key: 0,
            side: JoinSide::Right,
        },
        NaaScopeKey {
            key: 0,
            side: JoinSide::Left,
        },
        NaaScopeKey {
            key: 0,
            side: JoinSide::Right,
        },
        NaaEligibility,
    ] {
        assert_eq!(
            roots.sites().get(&site(role)),
            Some(&ExprUse {
                expr: expr(),
                demand: EvaluationDemand::Value
            })
        );
    }
    assert_eq!(roots.sites().len(), 5);
}
