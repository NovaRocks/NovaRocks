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

use super::{EffectContractError, ScopedExpressionEffects};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, ControlShape, EvaluationDemand, EvaluationRegionId,
    ExpressionControlFlow, ExpressionDefinitionMembership, ExpressionEffectContext,
    ExpressionEffects, ExpressionEvaluationRegion, ExpressionInvocation, ExpressionUseId,
    FunctionVolatility, GuardKind, ObservableEffects, PureCompileControl, RegionGuard,
};

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
struct Definitions;
impl ExpressionDefinitionMembership<u32> for Definitions {
    fn definition_count(&self) -> usize {
        2
    }
    fn contains_definition(&self, id: u32) -> bool {
        matches!(id, 77 | 99)
    }
}

fn context(use_id: u32, region: u32, demand: EvaluationDemand) -> ExpressionEffectContext {
    ExpressionEffectContext {
        use_id: ExpressionUseId::new(use_id),
        region: EvaluationRegionId::new(region),
        demand,
    }
}

// Each ordered argument has an independently authored occurrence. Reusing a
// definition is legal; reusing its invocation would fail the common validator.
fn checked_flow(
    shape: ControlShape,
    demand: EvaluationDemand,
    arguments: &[(EvaluationDemand, Option<GuardKind>)],
) -> ExpressionControlFlow<u32> {
    let owner = context(u32::MAX, 0, demand);
    let mut regions = vec![ExpressionEvaluationRegion {
        id: owner.region,
        parent: None,
        guard: None,
    }];
    let mut uses = vec![ExpressionInvocation {
        context: owner,
        definition: 99,
        control: shape,
        arguments: (0..arguments.len())
            .map(|i| ExpressionUseId::new(10 + i as u32 * 10))
            .collect(),
    }];
    for (ordinal, &(child_demand, guard)) in arguments.iter().enumerate() {
        let region = if let Some(kind) = guard {
            let id = EvaluationRegionId::new(100 + ordinal as u32);
            regions.push(ExpressionEvaluationRegion {
                id,
                parent: Some(owner.region),
                guard: Some(RegionGuard {
                    owner: owner.use_id,
                    kind,
                }),
            });
            id.get()
        } else {
            0
        };
        uses.push(ExpressionInvocation {
            context: context(10 + ordinal as u32 * 10, region, child_demand),
            definition: 77,
            control: ControlShape::Eager,
            arguments: Box::default(),
        });
    }
    ExpressionControlFlow::try_new(
        regions,
        uses,
        &Definitions,
        CompilePhase::Validate,
        &Control,
    )
    .expect("authored control flow must satisfy the common validator")
}
fn owner(flow: &ExpressionControlFlow<u32>) -> ExpressionEffectContext {
    flow.uses()[&ExpressionUseId::new(u32::MAX)].context
}
fn child(flow: &ExpressionControlFlow<u32>, ordinal: usize) -> ExpressionEffectContext {
    let id = flow.uses()[&owner(flow).use_id].arguments[ordinal];
    flow.uses()[&id].context
}
fn signals() -> ExpressionEffects {
    ExpressionEffects {
        may_raise_invocation_data: false,
        value_stability: FunctionVolatility::Volatile,
        may_raise_row_error: true,
        has_instance_state: true,
        observable_effects: ObservableEffects {
            rng_sampling: true,
            warnings: true,
            controlled_wait: true,
        },
    }
}
fn assert_rejected(
    parent: ScopedExpressionEffects,
    argument: ScopedExpressionEffects,
    flow: &ExpressionControlFlow<u32>,
    ordinal: usize,
) {
    assert_eq!(
        parent.join_control_argument(argument, flow, ordinal),
        Err(EffectContractError::ProofScopeMismatch)
    );
}

#[test]
fn checked_if_and_coalesce_guards_join_conservatively_without_rebinding_children() {
    use EvaluationDemand::{TruthOnly, Value};
    let cases = [
        (
            ControlShape::If,
            Value,
            vec![
                (Value, None),
                (Value, Some(GuardKind::IfThen)),
                (Value, Some(GuardKind::IfElse)),
            ],
        ),
        (
            ControlShape::If,
            TruthOnly,
            vec![
                (Value, None),
                (Value, Some(GuardKind::IfThen)),
                (Value, Some(GuardKind::IfElse)),
            ],
        ),
        (
            ControlShape::Coalesce,
            TruthOnly,
            vec![
                (Value, None),
                (Value, Some(GuardKind::CoalesceAfterNull { ordinal: 1 })),
                (Value, Some(GuardKind::CoalesceAfterNull { ordinal: 2 })),
            ],
        ),
    ];
    for (shape, demand, arguments) in cases {
        let flow = checked_flow(shape, demand, &arguments);
        for (ordinal, (_, guard)) in arguments.iter().enumerate() {
            let parent = ScopedExpressionEffects::pure_value(owner(&flow));
            let argument = ScopedExpressionEffects::primitive(child(&flow, ordinal), signals());
            let joined = parent
                .join_control_argument(argument, &flow, ordinal)
                .unwrap();
            assert_eq!(joined.context(), owner(&flow));
            assert_eq!(joined.for_use(owner(&flow)).unwrap(), signals());
            assert_eq!(argument.context(), child(&flow, ordinal));
            if guard.is_some() {
                assert_eq!(
                    parent.join_same_region(argument),
                    Err(EffectContractError::ProofScopeMismatch)
                );
                assert!(joined.for_use(argument.context()).is_err());
            }
        }
    }
}

#[test]
fn checked_simple_and_searched_case_keep_ordered_when_then_and_else_guards() {
    use EvaluationDemand::{TruthOnly, Value};
    for simple in [false, true] {
        for demand in [Value, TruthOnly] {
            let mut arguments = Vec::new();
            if simple {
                arguments.push((Value, None));
            }
            for arm in 0..2 {
                arguments.push((Value, Some(GuardKind::CaseWhen { arm })));
                arguments.push((Value, Some(GuardKind::CaseThen { arm })));
            }
            arguments.push((Value, Some(GuardKind::CaseElse)));
            let flow = checked_flow(
                ControlShape::Case {
                    simple,
                    arms: 2,
                    has_else: true,
                },
                demand,
                &arguments,
            );
            let mut summary = ScopedExpressionEffects::pure_value(owner(&flow));
            for ordinal in 0..arguments.len() {
                summary = summary
                    .join_control_argument(
                        ScopedExpressionEffects::primitive(child(&flow, ordinal), signals()),
                        &flow,
                        ordinal,
                    )
                    .unwrap();
                // The two arms deliberately share definitions, not use identity.
                assert_eq!(flow.uses()[&child(&flow, ordinal).use_id].definition, 77);
            }
            assert_eq!(summary.context(), owner(&flow));
            assert_eq!(summary.for_use(owner(&flow)).unwrap(), signals());
        }
    }
}

#[test]
fn higher_order_guard_and_lambda_body_keep_exact_body_demand_and_parent_context() {
    use EvaluationDemand::{TruthOnly, Value};
    for demand in [Value, TruthOnly] {
        let basic = checked_flow(
            ControlShape::HigherOrder {
                body_ordinal: 1,
                body_demand: demand,
            },
            Value,
            &[
                (Value, None),
                (demand, Some(GuardKind::LambdaInvocation)),
                (Value, None),
            ],
        );
        let mut uses: Vec<_> = basic.uses().values().cloned().collect();
        let wrapper = child(&basic, 1);
        let body = context(0, wrapper.region.get(), demand);
        let lambda = uses
            .iter_mut()
            .find(|item| item.context == wrapper)
            .unwrap();
        lambda.control = ControlShape::LambdaBody;
        lambda.arguments = Box::new([body.use_id]);
        uses.push(ExpressionInvocation {
            context: body,
            definition: 77,
            control: ControlShape::Eager,
            arguments: Box::default(),
        });
        let flow = ExpressionControlFlow::try_new(
            basic.regions().values().copied().collect(),
            uses,
            &Definitions,
            CompilePhase::Validate,
            &Control,
        )
        .unwrap();
        let body_effects = ScopedExpressionEffects::primitive(body, signals());
        let wrapper_effects = ScopedExpressionEffects::pure_value(wrapper)
            .join_control_argument(body_effects, &flow, 0)
            .unwrap();
        assert_eq!(wrapper_effects.context(), wrapper);
        let joined = ScopedExpressionEffects::pure_value(owner(&flow))
            .join_control_argument(wrapper_effects, &flow, 1)
            .unwrap();
        assert_eq!(joined.for_use(owner(&flow)).unwrap(), signals());
        assert_eq!(body_effects.context(), body);
        assert_rejected(
            ScopedExpressionEffects::pure_value(owner(&flow)),
            body_effects,
            &flow,
            1,
        );
    }
}

#[test]
fn and_or_value_and_truth_only_join_matches_same_region_and_preserves_each_signal() {
    use EvaluationDemand::{TruthOnly, Value};
    let parts = [
        ExpressionEffects {
            value_stability: FunctionVolatility::Stable,
            may_raise_row_error: true,
            ..ExpressionEffects::PURE_VALUE
        },
        ExpressionEffects {
            has_instance_state: true,
            observable_effects: ObservableEffects {
                rng_sampling: true,
                warnings: false,
                controlled_wait: false,
            },
            ..ExpressionEffects::PURE_VALUE
        },
        ExpressionEffects {
            value_stability: FunctionVolatility::Volatile,
            observable_effects: ObservableEffects {
                rng_sampling: false,
                warnings: true,
                controlled_wait: true,
            },
            ..ExpressionEffects::PURE_VALUE
        },
    ];
    for shape in [
        ControlShape::Conjunction,
        ControlShape::Disjunction,
        ControlShape::Eager,
    ] {
        for demand in [Value, TruthOnly] {
            // Eager arguments always demand Value, even under a TruthOnly owner.
            let child_demand = if shape == ControlShape::Eager {
                Value
            } else {
                demand
            };
            let flow = checked_flow(shape, demand, &[(child_demand, None); 3]);
            let mut actual = ScopedExpressionEffects::pure_value(owner(&flow));
            let mut legacy = actual;
            for (ordinal, effects) in parts.into_iter().enumerate() {
                let argument = ScopedExpressionEffects::primitive(child(&flow, ordinal), effects);
                actual = actual
                    .join_control_argument(argument, &flow, ordinal)
                    .unwrap();
                legacy = legacy.join_same_region(argument).unwrap();
                assert_eq!(actual, legacy);
            }
            assert_eq!(actual.for_use(owner(&flow)).unwrap(), signals());
        }
    }
}

#[test]
fn exact_ordered_occurrences_reject_same_definition_sibling_wrong_owner_region_and_demand() {
    use EvaluationDemand::{TruthOnly, Value};
    let flow = checked_flow(
        ControlShape::If,
        Value,
        &[
            (Value, None),
            (Value, Some(GuardKind::IfThen)),
            (Value, Some(GuardKind::IfElse)),
        ],
    );
    let parent = ScopedExpressionEffects::pure_value(owner(&flow));
    let argument = ScopedExpressionEffects::primitive(child(&flow, 1), signals());
    assert_rejected(parent, argument, &flow, 2);
    assert_rejected(parent, argument, &flow, 0);
    for changed in [
        ExpressionEffectContext {
            use_id: ExpressionUseId::new(12345),
            ..owner(&flow)
        },
        ExpressionEffectContext {
            region: EvaluationRegionId::new(101),
            ..owner(&flow)
        },
        ExpressionEffectContext {
            demand: TruthOnly,
            ..owner(&flow)
        },
        child(&flow, 1),
    ] {
        assert_rejected(
            ScopedExpressionEffects::pure_value(changed),
            argument,
            &flow,
            1,
        );
    }
    for changed in [
        ExpressionEffectContext {
            use_id: child(&flow, 2).use_id,
            ..argument.context()
        },
        ExpressionEffectContext {
            region: owner(&flow).region,
            ..argument.context()
        },
        ExpressionEffectContext {
            demand: TruthOnly,
            ..argument.context()
        },
    ] {
        assert_rejected(
            parent,
            ScopedExpressionEffects::primitive(changed, signals()),
            &flow,
            1,
        );
    }
    let same_region = checked_flow(ControlShape::Conjunction, Value, &[(Value, None); 2]);
    let parent = ScopedExpressionEffects::pure_value(owner(&same_region));
    let sibling = ScopedExpressionEffects::primitive(child(&same_region, 1), signals());
    assert!(parent.join_same_region(sibling).is_ok());
    assert_rejected(parent, sibling, &same_region, 0);
}

#[test]
fn missing_ordinal_and_type_only_without_runtime_arguments_cannot_join_a_child() {
    use EvaluationDemand::Value;
    let eager = checked_flow(ControlShape::Eager, Value, &[(Value, None)]);
    let argument = ScopedExpressionEffects::primitive(child(&eager, 0), signals());
    for ordinal in [1, usize::MAX] {
        assert_rejected(
            ScopedExpressionEffects::pure_value(owner(&eager)),
            argument,
            &eager,
            ordinal,
        );
    }
    let type_only = checked_flow(ControlShape::TypeOnly, Value, &[]);
    for ordinal in [0, usize::MAX] {
        assert_rejected(
            ScopedExpressionEffects::pure_value(owner(&type_only)),
            argument,
            &type_only,
            ordinal,
        );
    }
}
