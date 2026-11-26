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

//! Actual SQL journal composition, distinct from structural physical fixtures.

use super::super::contract_lowering::{
    NodeExpressionEffects,
    lowered_scalar_source_tests::{authored, lower_call, lower_draft_with_control, text},
    lowered_window_table_source_tests::{finish, list_value, table_fixture},
    pure_node_effects_for_test,
};
use super::super::expression_occurrences::author_physical_occurrences_observed;
use super::*;
use crate::compiler::SqlAuthoredPhysicalPlan;
use std::sync::Mutex;

#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let index = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(index <= stop, "callback after primary refusal");
        }
        trace.push((phase, units));
        match self.refusal.clone() {
            Some((stop, cause)) if stop == index => Err(cause),
            _ => Ok(()),
        }
    }
}
fn input<'a>(
    owner: &'a SqlAuthoredPhysicalPlan,
    fragment: &'a Fragment,
    occurrences: &'a AuthoredPhysicalOccurrences<'a>,
    policy: ConstantValidation,
    relations: &'a BTreeMap<PhysicalCallSite, PhysicalRelationalCallSourceScope<'a>>,
    expressions: &'a BTreeMap<novarocks_physical_plan::NodeId, NodeExpressionEffects>,
) -> PhysicalFragmentEffectsInput<'a> {
    PhysicalFragmentEffectsInput {
        fragment,
        occurrences,
        constants: owner.plan().constants(),
        parameters: owner.plan().parameters(),
        literal_policy: policy,
        node_effects: expressions,
        relational_scopes: relations,
    }
}
fn scopes<'a>(
    owner: &'a SqlAuthoredPhysicalPlan,
    fragment: &'a Fragment,
    occurrences: &AuthoredPhysicalOccurrences<'_>,
) -> BTreeMap<PhysicalCallSite, PhysicalRelationalCallSourceScope<'a>> {
    let control = Control::default();
    let mut work =
        CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization).unwrap();
    let mut scopes = BTreeMap::new();
    for &(site, context) in &occurrences.relational_contexts {
        let node = &fragment.nodes()[&site.node()];
        let decimal = match site {
            PhysicalCallSite::Table { .. } => owner
                .checked_table_source_observed(fragment, node, &mut work)
                .unwrap()
                .captured()
                .binding()
                .decimal_overflow_policy(),
            PhysicalCallSite::Window { .. } => owner
                .checked_window_source_observed(
                    fragment,
                    node,
                    site,
                    window_source(node, site).unwrap(),
                    &mut work,
                )
                .unwrap()
                .captured()
                .binding()
                .decimal_overflow_policy(),
            _ => panic!("actual table/window fixture lifecycle"),
        };
        scopes.insert(
            site,
            PhysicalRelationalCallSourceScope {
                source: node,
                decimal_overflow_policy: decimal,
                environment: &[],
                proof_scope: CallProofScope::Relation(context.identity),
            },
        );
    }
    work.finish().unwrap();
    scopes
}
fn scalar_expression() -> crate::analysis::TypedExpr {
    let argument = crate::analysis::TypedExpr {
        kind: crate::analysis::ExprKind::Cast {
            cast_mode: None,
            expr: novarocks_parser::ast::AstBox::new(text("MiXeD")),
            target: arrow::datatypes::DataType::Utf8,
            decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
        },
        value_type: novarocks_type_contract::FunctionValueType::new(
            arrow::datatypes::DataType::Utf8,
            false,
        ),
    };
    lower_call(argument, DecimalOverflowPolicy::OutputNull)
}
fn prefixes(
    mut invoke: impl FnMut(
        &Control,
    ) -> Result<AuthoredPhysicalFragmentEffects, PhysicalFragmentEffectsError>,
    success: bool,
) {
    let baseline = Control::default();
    let result = invoke(&baseline);
    assert_eq!(result.is_ok(), success, "{result:?}");
    let trace = baseline.trace.into_inner().unwrap();
    assert!(trace.len() > 2);
    for index in 0..trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control {
                trace: Default::default(),
                refusal: Some((index, cause.clone())),
            };
            assert!(
                matches!(invoke(&control), Err(PhysicalFragmentEffectsError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=index]);
        }
    }
}
#[test]
fn sql_scalar_fresh_composer_preserves_original_none_and_each_actual_context() {
    let owner = authored(scalar_expression());
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let mut calls = 0;
    for fragment in owner.plan().fragments().values() {
        for node in fragment.nodes().values() {
            let Some(unit) = &node.expressions else {
                continue;
            };
            for (&id, claim) in unit.claims() {
                let invocation = &unit.control_flow().uses()[&id];
                let source = unit.definition(invocation.definition).unwrap();
                let entry = owner
                    .checked_expression_call_source_observed(fragment, node, source, &mut work)
                    .unwrap();
                assert!(matches!(
                    &entry.canonical_operational().unwrap().request().arguments[0],
                    novarocks_functions::FunctionArgument::Value { constant: None, .. }
                ));
                assert_eq!(claim.context, invocation.context);
                assert_eq!(
                    claim.decimal_overflow_policy,
                    entry.captured().binding().decimal_overflow_policy()
                );
                calls += 1;
            }
        }
    }
    assert_eq!(calls, 1);
    work.finish().unwrap();
    // Scalar claims now finish at the original lowering, before any package stage.
    let baseline = Control::default();
    lower_draft_with_control(scalar_expression(), &baseline).unwrap();
    let trace = baseline.trace.into_inner().unwrap();
    assert!(trace.len() > 2);
    for index in 0..trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control {
                trace: Default::default(),
                refusal: Some((index, cause.clone())),
            };
            assert!(
                matches!(lower_draft_with_control(scalar_expression(), &control), Err(super::super::contract_lowering::ContractLoweringError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=index]);
        }
    }
}
#[test]
fn sql_call_fresh_composer_refuses_changed_policy_and_foreign_constant_parameter_sources() {
    // The remaining composer authenticates actual relational sources. Scalar
    // policy/claims were already frozen by that same statement's lowering.
    let (plan, _) = table_fixture(vec![list_value(1)], false);
    let owner = finish(&plan);
    let other = finish(&plan);
    let fragment = owner
        .plan()
        .fragments()
        .values()
        .find(|f| {
            f.nodes()
                .values()
                .any(|n| matches!(n.kind, NodeKind::TableFunction { .. }))
        })
        .unwrap();
    let occurrences = author_physical_occurrences_observed(
        fragment,
        owner.function_catalog().as_ref(),
        &Control::default(),
    )
    .unwrap();
    let mut relations = scopes(&owner, fragment, &occurrences);
    let expressions = pure_node_effects_for_test(fragment);
    let policy = novarocks_functions::ConstantValidation::INPUT_DERIVED;
    let mut request = input(
        &owner,
        fragment,
        &occurrences,
        policy,
        &relations,
        &expressions,
    );
    request.constants = other.plan().constants();
    assert!(matches!(
        author_sql_fragment_effects_observed(
            owner.relational_sources(),
            request,
            &Control::default()
        ),
        Err(PhysicalFragmentEffectsError::InvalidSource(_))
    ));
    let mut request = input(
        &owner,
        fragment,
        &occurrences,
        policy,
        &relations,
        &expressions,
    );
    request.parameters = other.plan().parameters();
    assert!(matches!(
        author_sql_fragment_effects_observed(
            owner.relational_sources(),
            request,
            &Control::default()
        ),
        Err(PhysicalFragmentEffectsError::InvalidSource(_))
    ));
    let original = relations.values().next().unwrap().decimal_overflow_policy;
    relations
        .values_mut()
        .next()
        .unwrap()
        .decimal_overflow_policy = match original {
        DecimalOverflowPolicy::ReportError => DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::OutputNull => DecimalOverflowPolicy::ReportError,
    };
    prefixes(
        |control| {
            author_sql_fragment_effects_observed(
                owner.relational_sources(),
                input(
                    &owner,
                    fragment,
                    &occurrences,
                    policy,
                    &relations,
                    &expressions,
                ),
                control,
            )
        },
        false,
    );
    relations
        .values_mut()
        .next()
        .unwrap()
        .decimal_overflow_policy = original;
    let changed = novarocks_constant_contract::ConstantValidation::bounded(
        novarocks_constant_contract::ConstantPolicy {
            max_rows: policy.max_rows + 1,
            ..policy
        },
    );
    assert!(matches!(
        author_sql_fragment_effects_observed(
            owner.relational_sources(),
            input(
                &owner,
                fragment,
                &occurrences,
                changed,
                &relations,
                &expressions
            ),
            &Control::default()
        ),
        Err(PhysicalFragmentEffectsError::InvalidSource(_))
    ));
}
#[test]
fn sql_window_fresh_composer_uses_real_row_number_source_journal() {
    let owner = crate::compiler::compile_authored_aggregate_for_test(
        "SELECT ROW_NUMBER() OVER (ORDER BY order_key) FROM orders",
    );
    let mut total = 0;
    for fragment in owner.plan().fragments().values() {
        if !fragment
            .nodes()
            .values()
            .any(|n| matches!(&n.kind, NodeKind::Window(spec) if !spec.calls.is_empty()))
        {
            continue;
        }
        let occurrences = author_physical_occurrences_observed(
            fragment,
            owner.function_catalog().as_ref(),
            &Control::default(),
        )
        .unwrap();
        let relations = scopes(&owner, fragment, &occurrences);
        let expressions = pure_node_effects_for_test(fragment);
        let result = author_sql_fragment_effects_observed(
            owner.relational_sources(),
            input(
                &owner,
                fragment,
                &occurrences,
                novarocks_functions::ConstantValidation::INPUT_DERIVED,
                &relations,
                &expressions,
            ),
            &Control::default(),
        )
        .unwrap();
        assert_eq!(result.calls.entries().len(), relations.len());
        assert!(
            result
                .calls
                .entries()
                .keys()
                .all(|site| matches!(site, PhysicalCallSite::Window { .. }))
        );
        total += relations.len();
    }
    assert_eq!(total, 1);
}
#[test]
fn sql_table_fresh_composer_borrows_selected_list_and_whole_relation() {
    for left in [false, true] {
        let (plan, _) = table_fixture(vec![list_value(1)], left);
        let owner = finish(&plan);
        let fragment = owner
            .plan()
            .fragments()
            .values()
            .find(|f| {
                f.nodes()
                    .values()
                    .any(|n| matches!(n.kind, NodeKind::TableFunction { .. }))
            })
            .unwrap();
        let occurrences = author_physical_occurrences_observed(
            fragment,
            owner.function_catalog().as_ref(),
            &Control::default(),
        )
        .unwrap();
        let relations = scopes(&owner, fragment, &occurrences);
        let expressions = pure_node_effects_for_test(fragment);
        let result = author_sql_fragment_effects_observed(
            owner.relational_sources(),
            input(
                &owner,
                fragment,
                &occurrences,
                novarocks_functions::ConstantValidation::INPUT_DERIVED,
                &relations,
                &expressions,
            ),
            &Control::default(),
        )
        .unwrap();
        assert_eq!(relations.len(), 1);
        assert_eq!(result.calls.entries().len(), 1);
    }
}
#[test]
fn sql_if_fresh_composer_keeps_distinct_guarded_branch_domains_from_original_sources() {
    let owner = crate::compiler::compile_authored_aggregate_for_test(
        "SELECT IF(order_key > 0, ABS(order_key), ABS(order_key + 1)) FROM orders",
    );
    let mut branches = 0;
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    for fragment in owner.plan().fragments().values() {
        for node in fragment.nodes().values() {
            let Some(unit) = &node.expressions else {
                continue;
            };
            let mut domains = std::collections::BTreeSet::new();
            let mut definitions = std::collections::BTreeSet::new();
            for (&id, call) in unit.claims() {
                let original = &unit.control_flow().uses()[&id];
                let source = unit.definition(original.definition).unwrap();
                let novarocks_expression_contract::ExprKind::FunctionCall { function, .. } =
                    &source.kind
                else {
                    unreachable!()
                };
                if !function.function_id.as_str().contains("/abs/") {
                    continue;
                }
                owner
                    .checked_expression_call_source_observed(fragment, node, source, &mut work)
                    .unwrap();
                assert_eq!(call.context, original.context);
                assert!(domains.insert(call.context.region));
                assert!(definitions.insert(original.definition));
                let domain = &unit.control_flow().regions()[&call.context.region];
                assert!(domain.guard.is_some());
                branches += 1;
            }
            if !domains.is_empty() {
                assert_eq!(domains.len(), 2);
            }
        }
    }
    work.finish().unwrap();
    assert_eq!(branches, 2);
}
#[test]
fn sql_repeat_fresh_composer_consumes_late_nullable_operational_requests() {
    use super::super::contract_lowering::lowered_scalar_source_tests::lowered_canonical_scalar_tests::{repeat_plan, finish as finish_repeat};
    let owner = finish_repeat(&repeat_plan(true));
    let mut count = 0;
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    for fragment in owner.plan().fragments().values() {
        for node in fragment.nodes().values() {
            let Some(unit) = &node.expressions else {
                continue;
            };
            for (&id, claim) in unit.claims() {
                let source = unit
                    .definition(unit.control_flow().uses()[&id].definition)
                    .unwrap();
                let entry = owner
                    .checked_expression_call_source_observed(fragment, node, source, &mut work)
                    .unwrap();
                let original = entry.captured().request();
                let canonical = entry.canonical_operational().unwrap().request();
                let novarocks_functions::FunctionArgument::Value {
                    value_type: original,
                    constant: None,
                } = &original.arguments[0]
                else {
                    panic!("original nonconstant source")
                };
                let novarocks_functions::FunctionArgument::Value {
                    value_type: late,
                    constant: None,
                } = &canonical.arguments[0]
                else {
                    panic!("late nonconstant source")
                };
                assert!(!original.nullable());
                assert!(late.nullable());
                assert!(source.ty.nullable());
                assert!(canonical.expected_result_type.is_none());
                assert_eq!(claim.context, unit.control_flow().uses()[&id].context);
                count += 1;
            }
        }
    }
    work.finish().unwrap();
    assert_eq!(count, 2);
    // The original late-nullable requests and scalar claims finish in lowering.
    let plan = repeat_plan(true);
    let invoke = |control: &Control| {
        super::super::contract_lowering::lowered_scalar_source_tests::lowered_canonical_scalar_tests::lower_draft_with_control(&plan, control)
    };
    let baseline = Control::default();
    invoke(&baseline).unwrap();
    let trace = baseline.trace.into_inner().unwrap();
    assert!(trace.len() > 2);
    for index in 0..trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control {
                trace: Default::default(),
                refusal: Some((index, cause.clone())),
            };
            assert!(
                matches!(invoke(&control), Err(super::super::contract_lowering::ContractLoweringError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=index]);
        }
    }
}
