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

//! Definition/control correspondence belongs to the neutral unit author.
use super::support::*;
use novarocks_expression_contract::*;
use novarocks_failure_contract::{FailureClass, FailureProjection};
use novarocks_type_contract::*;

struct Membership(usize);
impl ExpressionDefinitionMembership<ExprId> for Membership {
    fn definition_count(&self) -> usize {
        self.0
    }
    fn contains_definition(&self, id: ExprId) -> bool {
        (id.get() as usize) < self.0
    }
}
fn rewrite_flow(parts: &mut UnitParts, edit: impl FnOnce(&mut Vec<ExpressionInvocation<ExprId>>)) {
    let mut uses = parts.control.uses().values().cloned().collect::<Vec<_>>();
    edit(&mut uses);
    let c = Control::good();
    let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
    parts.control = ExpressionControlFlow::try_new_in(
        parts.control.regions().values().copied().collect(),
        uses,
        &Membership(parts.definitions.len()),
        &mut |_| Ok(()),
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
}
fn invalid(parts: UnitParts, message: &str) {
    let c = Control::good();
    let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
    let error = BoundExprSet::try_from_parts_in(parts, limits(), &mut work).unwrap_err();
    assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
    assert!(error.to_string().contains(message), "{error}");
}
fn arithmetic(repeated: bool) -> BoundExprSet {
    let mut b = BoundExprSetBuilder::new(limits());
    let left = b
        .add(None, integer(true), ExprKind::Value(ValueId::new(0)))
        .unwrap();
    let right = if repeated {
        left
    } else {
        b.add(None, integer(true), ExprKind::Value(ValueId::new(1)))
            .unwrap()
    };
    let expression = b
        .add(
            None,
            integer(true),
            ExprKind::Binary {
                left,
                right,
                op: BinaryOperator::Subtract,
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                allow_throw_exception: Some(SemanticParameterRef {
                    id: SemanticParameterId::new(0),
                    expected_key: SemanticParameterKey::AllowThrowException,
                }),
            },
        )
        .unwrap();
    b.root(expression, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    no_calls(
        b,
        vec![ExprPort {
            values: if repeated {
                Box::new([(ValueId::new(0), integer(true))])
            } else {
                Box::new([
                    (ValueId::new(0), integer(true)),
                    (ValueId::new(1), integer(true)),
                ])
            },
        }],
        &Control::good(),
    )
    .unwrap()
}
#[test]
fn checked_constant_cannot_invent_a_child_from_an_existing_definition() {
    let (_pools, reference) = i64_source(1);
    let mut b = BoundExprSetBuilder::new(limits());
    let constant = b
        .add(None, integer(false), ExprKind::Constant(reference))
        .unwrap();
    b.root(constant, EvaluationDemand::Value, RootScope::AllPorts)
        .unwrap();
    let unit = no_calls(b, vec![], &Control::good()).unwrap();
    let mut parts = unit.into_parts();
    let root = parts.root_uses[0];
    rewrite_flow(&mut parts, |uses| {
        let first = uses.iter_mut().find(|u| u.context.use_id == root).unwrap();
        let mut child = first.clone();
        child.context.use_id = ExpressionUseId::new(u32::MAX);
        first.arguments = Box::new([child.context.use_id]);
        uses.push(child);
    });
    invalid(
        parts,
        "invocation child count differs from ordered definition occurrences",
    );
}
#[test]
fn arithmetic_order_is_checked_even_when_full_input_types_match() {
    let unit = arithmetic(false);
    let mut parts = unit.into_parts();
    let root = parts.root_uses[0];
    rewrite_flow(&mut parts, |uses| {
        uses.iter_mut()
            .find(|u| u.context.use_id == root)
            .unwrap()
            .arguments
            .swap(0, 1)
    });
    invalid(
        parts,
        "invocation child names a different definition occurrence",
    );
}
#[test]
fn repeated_definition_keeps_two_uses_and_shared_use_is_refused_by_flow_owner() {
    let unit = arithmetic(true);
    let root = unit.root_use(RootOrdinal::new(0)).unwrap();
    let flow = unit.control_flow();
    let arguments = &flow.uses()[&root].arguments;
    assert_eq!(arguments.len(), 2);
    assert_ne!(arguments[0], arguments[1]);
    assert_eq!(
        flow.uses()[&arguments[0]].definition,
        flow.uses()[&arguments[1]].definition
    );
    let mut uses = flow.uses().values().cloned().collect::<Vec<_>>();
    uses.iter_mut()
        .find(|u| u.context.use_id == root)
        .unwrap()
        .arguments[1] = arguments[0];
    let c = Control::good();
    let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
    assert_eq!(
        ExpressionControlFlow::try_new_in(
            flow.regions().values().copied().collect(),
            uses,
            &Membership(unit.definitions().len()),
            &mut |_| Ok(()),
            &mut work
        )
        .unwrap_err(),
        ExpressionControlFlowError::SharedUse
    );
}
fn case_unit() -> BoundExprSet {
    let mut b = BoundExprSetBuilder::new(limits());
    let ids = (0..5)
        .map(|id| {
            b.add(None, boolean(true), ExprKind::Value(ValueId::new(id)))
                .unwrap()
        })
        .collect::<Vec<_>>();
    let root = b
        .add(
            None,
            boolean(true),
            ExprKind::Case {
                operand: None,
                when_then: Box::new([(ids[0], ids[1]), (ids[2], ids[3])]),
                else_expr: Some(ids[4]),
            },
        )
        .unwrap();
    b.root(root, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    no_calls(
        b,
        vec![ExprPort {
            values: (0..5).map(|id| (ValueId::new(id), boolean(true))).collect(),
        }],
        &Control::good(),
    )
    .unwrap()
}
#[test]
fn case_metadata_and_ordered_when_then_occurrences_match_the_real_definition() {
    let unit = case_unit();
    let root = unit.root_use(RootOrdinal::new(0)).unwrap();
    let invocation = &unit.control_flow().uses()[&root];
    assert_eq!(
        invocation.control,
        ControlShape::Case {
            simple: false,
            arms: 2,
            has_else: true
        }
    );
    assert_eq!(
        invocation
            .arguments
            .iter()
            .map(|id| unit.control_flow().uses()[id].definition)
            .collect::<Vec<_>>(),
        (0..5).map(ExprId::new).collect::<Vec<_>>()
    );
    let mut changed = case_unit().into_parts();
    let definition = changed.roots[0].definition;
    changed.definitions[definition.get() as usize].kind = ExprKind::Case {
        operand: Some(ExprId::new(0)),
        when_then: Box::new([
            (ExprId::new(1), ExprId::new(2)),
            (ExprId::new(3), ExprId::new(4)),
        ]),
        else_expr: None,
    };
    invalid(changed, "intrinsic control differs from its definition");
    let mut swapped = unit.into_parts();
    rewrite_flow(&mut swapped, |uses| {
        let args = uses
            .iter()
            .find(|u| u.context.use_id == root)
            .unwrap()
            .arguments
            .clone();
        let left = uses
            .iter()
            .find(|u| u.context.use_id == args[0])
            .unwrap()
            .definition;
        let right = uses
            .iter()
            .find(|u| u.context.use_id == args[2])
            .unwrap()
            .definition;
        uses.iter_mut()
            .find(|u| u.context.use_id == args[0])
            .unwrap()
            .definition = right;
        uses.iter_mut()
            .find(|u| u.context.use_id == args[2])
            .unwrap()
            .definition = left;
    });
    invalid(
        swapped,
        "invocation child names a different definition occurrence",
    );
}
#[test]
fn structure_recheck_success_and_real_depth_error_keep_every_borrowed_prefix() {
    let mut b = BoundExprSetBuilder::new(limits());
    let leaf = b
        .add(None, boolean(true), ExprKind::Value(ValueId::new(0)))
        .unwrap();
    let parent = b
        .add(
            None,
            boolean(false),
            ExprKind::IsNull {
                expr: leaf,
                negated: false,
            },
        )
        .unwrap();
    let root = b
        .add(
            None,
            boolean(false),
            ExprKind::IsNull {
                expr: parent,
                negated: false,
            },
        )
        .unwrap();
    for _ in 0..320 {
        b.root(root, EvaluationDemand::Value, RootScope::Port(0))
            .unwrap();
    }
    let unit = no_calls(
        b,
        vec![ExprPort {
            values: Box::new([(ValueId::new(0), boolean(true))]),
        }],
        &Control::good(),
    )
    .unwrap();
    let run = |bound, control: &Control| -> Result<(), UnitError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
        let result = unit.validate_structure_in(
            UnitLimits {
                expression_semantic_depth: bound,
            },
            &mut work,
        );
        if matches!(result, Err(UnitError::Control(_))) {
            return result;
        }
        work.finish()?;
        result
    };
    for bound in [MAX_CONTROL_DEPTH, 2] {
        let good = Control::good();
        let result = run(bound, &good);
        if bound == 2 {
            assert_eq!(
                result.unwrap_err().failure_fact().class(),
                FailureClass::Resource
            );
        } else {
            result.unwrap();
        }
        let trace = good.trace();
        assert_eq!(trace[0], (CompilePhase::Decode, 0));
        assert!(
            trace
                .iter()
                .all(|(p, n)| *p == CompilePhase::Decode && *n <= 256)
        );
        if bound == MAX_CONTROL_DEPTH {
            // Source preflight flushes before each original map operation;
            // those smaller checkpoints still account for every actual root.
            assert!(
                trace
                    .iter()
                    .map(|(_, units)| u64::from(*units))
                    .sum::<u64>()
                    >= 320
            );
        }
        for at in 0..trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let refusing = Control::at(at, cause.clone());
                assert!(
                    matches!(run(bound,&refusing),Err(UnitError::Control(actual))if actual==cause)
                );
                assert_eq!(refusing.trace(), trace[..=at]);
            }
        }
    }
}

fn higher_unit(body_ordinal: u32) -> Result<BoundExprSet, UnitError> {
    let mut b = BoundExprSetBuilder::new(limits());
    let lambda = b.reserve()?;
    let body = b.add(
        Some(lambda),
        boolean(true),
        ExprKind::LambdaParameter { lambda, ordinal: 0 },
    )?;
    b.insert(
        lambda,
        None,
        boolean(true),
        ExprKind::Lambda {
            parameter_types: Box::new([boolean(true)]),
            body,
        },
    )?;
    let ordinary = b.add(None, boolean(true), ExprKind::Value(ValueId::new(0)))?;
    let f = function(
        "fixture/unit/ordered-lambda",
        vec![
            FunctionArgumentType::Value(boolean(true)),
            FunctionArgumentType::Lambda {
                parameter_types: TypedFactSlice::from_pure_boxed(Box::new([boolean(true)])),
                result_type: boolean(true),
            },
        ],
        boolean(true),
    );
    let original = request(&f);
    let call = b.add(
        None,
        boolean(true),
        ExprKind::FunctionCall {
            function: f,
            args: Box::new([ordinary, lambda]),
        },
    )?;
    b.request(call, original)?;
    b.definition_control(
        call,
        DefinitionControl {
            shape: ControlShape::HigherOrder {
                body_ordinal,
                body_demand: EvaluationDemand::TruthOnly,
            },
            temporal_sources: None,
        },
    )?;
    b.root(call, EvaluationDemand::Value, RootScope::Port(0))?;
    let c = Control::good();
    let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Validate)?;
    let sealed = b.seal(
        vec![ExprPort {
            values: Box::new([(ValueId::new(0), boolean(true))]),
        }],
        &mut work,
    )?;
    let unit = with_call_claims(
        sealed,
        ArgumentControl::HigherOrder {
            body_ordinal,
            body_demand: EvaluationDemand::TruthOnly,
        },
        &mut work,
    )?;
    work.finish()?;
    Ok(unit)
}
#[test]
fn truth_only_lambda_wrapper_and_body_keep_the_exact_original_invocation_guard() {
    let unit = higher_unit(1).unwrap();
    let root = unit.root_use(RootOrdinal::new(0)).unwrap();
    let owner = &unit.control_flow().uses()[&root];
    let lambda = &unit.control_flow().uses()[&owner.arguments[1]];
    let body = &unit.control_flow().uses()[&lambda.arguments[0]];
    assert_eq!(lambda.context.region, body.context.region);
    assert_eq!(lambda.context.demand, EvaluationDemand::TruthOnly);
    assert_eq!(body.context.demand, EvaluationDemand::TruthOnly);
    assert_eq!(
        unit.control_flow().regions()[&lambda.context.region].guard,
        Some(RegionGuard {
            owner: root,
            kind: GuardKind::LambdaInvocation
        })
    );
}
#[test]
fn higher_control_must_name_the_real_lambda_ordinal_and_wrapper() {
    let error = higher_unit(0).unwrap_err();
    assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
    let unit = higher_unit(1).unwrap();
    let mut parts = unit.into_parts();
    let root = parts.root_uses[0];
    rewrite_flow(&mut parts, |uses| {
        let lambda = uses
            .iter()
            .find(|u| u.context.use_id == root)
            .unwrap()
            .arguments[1];
        let wrapper = uses
            .iter_mut()
            .find(|u| u.context.use_id == lambda)
            .unwrap();
        wrapper.control = ControlShape::Eager;
        let body = wrapper.arguments[0];
        // The independent flow owner requires an Eager child's Value demand.
        // Keep that graph structurally valid so the unit checks the actual
        // Lambda definition's required LambdaBody wrapper.
        uses.iter_mut()
            .find(|u| u.context.use_id == body)
            .unwrap()
            .context
            .demand = EvaluationDemand::Value;
    });
    invalid(parts, "intrinsic control differs from its definition");
}

#[test]
fn relational_window_binding_cannot_enter_a_scalar_expression_unit() {
    // This malformed construction input proves the neutral kind gate only.
    // It is neither a Window expression nor an installed relation capability.
    let build = || {
        let mut b = BoundExprSetBuilder::new(limits());
        let mut binding = function("fixture/unit/non-scalar", vec![], integer(false));
        binding.kind = FunctionKind::Window;
        let definition = b
            .add(
                None,
                integer(false),
                ExprKind::FunctionCall {
                    function: binding,
                    args: Box::default(),
                },
            )
            .unwrap();
        b.root(definition, EvaluationDemand::Value, RootScope::AllPorts)
            .unwrap();
        (b, definition)
    };
    let (_, definition) = build();
    let invoke = |control: &Control| -> Result<SealedUnit, UnitError> {
        let (b, _) = build();
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
        let result = b.seal(vec![], &mut work);
        if matches!(&result, Err(UnitError::Control(_))) {
            return result;
        }
        work.finish()?;
        result
    };
    let control = Control::good();
    let error = match invoke(&control) {
        Err(error) => error,
        Ok(_) => panic!("a Window binding entered the scalar unit"),
    };
    assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
    assert!(
        matches!(&error, UnitError::Invalid { definition: Some(id), root: None, message }
        if *id == definition && message.contains("scalar call has non-scalar binding")),
        "{error}"
    );
    let trace = control.trace();
    assert!(!trace.is_empty());
    for at in 0..trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control::at(at, cause.clone());
            assert!(matches!(invoke(&control), Err(UnitError::Control(actual)) if actual == cause));
            assert_eq!(control.trace(), trace[..=at]);
        }
    }
}
