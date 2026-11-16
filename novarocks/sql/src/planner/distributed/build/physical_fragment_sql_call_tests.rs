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
    lowered_scalar_source_tests::{authored, lower_call, text},
    lowered_window_table_source_tests::{finish, list_value, table_fixture},
};
use super::super::expression_occurrences::author_physical_occurrences_observed;
use super::*;
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
fn scopes<'a>(
    owner: &'a SqlAuthoredPhysicalPlan,
    fragment: &'a Fragment,
    occurrences: &AuthoredPhysicalOccurrences,
) -> (
    ConstantPolicy,
    BTreeMap<ExpressionUseId, PhysicalCallSourceScope<'a>>,
    BTreeMap<PhysicalCallSite, PhysicalRelationalCallSourceScope<'a>>,
) {
    let control = Control::default();
    let mut work =
        CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization).unwrap();
    let mut policy = None;
    let mut expressions = BTreeMap::new();
    let mut relations = BTreeMap::new();
    for (&id, invocation) in occurrences.root_uses.flow().uses() {
        let source = fragment.expressions().get(invocation.definition).unwrap();
        if matches!(
            source.kind,
            novarocks_physical_plan::ExprKind::FunctionCall { .. }
                | novarocks_physical_plan::ExprKind::WindowCall { .. }
        ) {
            let entry = owner
                .checked_expression_call_source_observed(fragment, source, &mut work)
                .unwrap();
            let captured = entry.captured();
            if let Some(policy) = policy {
                assert_eq!(policy, captured.constant_policy());
            } else {
                policy = Some(captured.constant_policy());
            }
            expressions.insert(
                id,
                PhysicalCallSourceScope {
                    source,
                    decimal_overflow_policy: captured.binding().decimal_overflow_policy(),
                    environment: &[],
                    proof_scope: CallProofScope::Domain(invocation.context.domain),
                },
            );
        }
    }
    for &(site, context) in &occurrences.relational_contexts {
        let PhysicalCallSite::Table { node } = site else {
            panic!("table fixture lifecycle")
        };
        let source = &fragment.nodes()[&node];
        let entry = owner
            .checked_table_source_observed(fragment, source, &mut work)
            .unwrap();
        let captured = entry.captured();
        if let Some(policy) = policy {
            assert_eq!(policy, captured.constant_policy());
        } else {
            policy = Some(captured.constant_policy());
        }
        relations.insert(
            site,
            PhysicalRelationalCallSourceScope {
                source,
                decimal_overflow_policy: captured.binding().decimal_overflow_policy(),
                environment: &[],
                proof_scope: CallProofScope::Domain(context.domain),
            },
        );
    }
    work.finish().unwrap();
    (policy.unwrap(), expressions, relations)
}
fn input<'a>(
    owner: &'a SqlAuthoredPhysicalPlan,
    fragment: &'a Fragment,
    occurrences: &'a AuthoredPhysicalOccurrences<'a>,
    policy: ConstantPolicy,
    expressions: &'a BTreeMap<ExpressionUseId, PhysicalCallSourceScope<'a>>,
    relations: &'a BTreeMap<PhysicalCallSite, PhysicalRelationalCallSourceScope<'a>>,
) -> PhysicalFragmentEffectsInput<'a> {
    PhysicalFragmentEffectsInput {
        fragment,
        occurrences,
        constants: owner.plan().constants(),
        parameters: owner.plan().parameters(),
        literal_policy: policy,
        expression_scopes: expressions,
        relational_scopes: relations,
    }
}
fn scalar_owner() -> SqlAuthoredPhysicalPlan {
    let argument = crate::analysis::TypedExpr {
        kind: crate::analysis::ExprKind::Cast {
            array_failure_mode: None,
            expr: Box::new(text("MiXeD")),
            target: arrow::datatypes::DataType::Utf8,
            decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
        },
        value_type: novarocks_type_contract::FunctionValueType::new(
            arrow::datatypes::DataType::Utf8,
            false,
        ),
    };
    authored(lower_call(argument, DecimalOverflowPolicy::OutputNull))
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
                matches!(invoke(&control),Err(PhysicalFragmentEffectsError::Control(actual)) if actual==cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=index]);
        }
    }
}
#[test]
fn sql_scalar_fresh_composer_preserves_original_none_and_each_actual_context() {
    let owner = scalar_owner();
    let fragment = owner.plan().fragments().values().next().unwrap();
    let occurrences = author_physical_occurrences_observed(
        fragment,
        owner.function_catalog().as_ref(),
        &Control::default(),
    )
    .unwrap();
    let (policy, expressions, relations) = scopes(&owner, fragment, &occurrences);
    assert_eq!(expressions.len(), 1);
    let source = expressions.values().next().unwrap().source;
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let entry = owner
        .checked_expression_call_source_observed(fragment, source, &mut work)
        .unwrap();
    assert!(matches!(
        &entry.canonical_operational().unwrap().request().arguments[0],
        novarocks_functions::FunctionArgument::Value { constant: None, .. }
    ));
    work.finish().unwrap();
    let result = author_sql_fragment_effects_observed(
        &owner,
        input(
            &owner,
            fragment,
            &occurrences,
            policy,
            &expressions,
            &relations,
        ),
        &Control::default(),
    )
    .unwrap();
    assert_eq!(result.calls.entries().len(), expressions.len());
    for (&id, scope) in &expressions {
        let call = &result.calls.entries()[&PhysicalCallSite::Expression(id)];
        assert_eq!(
            call.context,
            occurrences.root_uses.flow().uses()[&id].context
        );
        assert_eq!(call.decimal_overflow_policy, scope.decimal_overflow_policy);
    }
    prefixes(
        |control| {
            author_sql_fragment_effects_observed(
                &owner,
                input(
                    &owner,
                    fragment,
                    &occurrences,
                    policy,
                    &expressions,
                    &relations,
                ),
                control,
            )
        },
        true,
    );
}
#[test]
fn sql_call_fresh_composer_refuses_changed_policy_and_foreign_constant_parameter_sources() {
    let owner = scalar_owner();
    let other = scalar_owner();
    let fragment = owner.plan().fragments().values().next().unwrap();
    let occurrences = author_physical_occurrences_observed(
        fragment,
        owner.function_catalog().as_ref(),
        &Control::default(),
    )
    .unwrap();
    let (policy, mut expressions, relations) = scopes(&owner, fragment, &occurrences);
    {
        let source = other.plan().constants();
        let mut request = input(
            &owner,
            fragment,
            &occurrences,
            policy,
            &expressions,
            &relations,
        );
        request.constants = source;
        assert!(matches!(
            author_sql_fragment_effects_observed(&owner, request, &Control::default()),
            Err(PhysicalFragmentEffectsError::InvalidSource(_))
        ));
    }
    let mut request = input(
        &owner,
        fragment,
        &occurrences,
        policy,
        &expressions,
        &relations,
    );
    request.parameters = other.plan().parameters();
    assert!(matches!(
        author_sql_fragment_effects_observed(&owner, request, &Control::default()),
        Err(PhysicalFragmentEffectsError::InvalidSource(_))
    ));
    expressions
        .values_mut()
        .next()
        .unwrap()
        .decimal_overflow_policy = DecimalOverflowPolicy::ReportError;
    prefixes(
        |control| {
            author_sql_fragment_effects_observed(
                &owner,
                input(
                    &owner,
                    fragment,
                    &occurrences,
                    policy,
                    &expressions,
                    &relations,
                ),
                control,
            )
        },
        false,
    );
    expressions
        .values_mut()
        .next()
        .unwrap()
        .decimal_overflow_policy = DecimalOverflowPolicy::OutputNull;
    let changed = ConstantPolicy {
        max_rows: policy.max_rows + 1,
        ..policy
    };
    assert!(matches!(
        author_sql_fragment_effects_observed(
            &owner,
            input(
                &owner,
                fragment,
                &occurrences,
                changed,
                &expressions,
                &relations
            ),
            &Control::default()
        ),
        Err(PhysicalFragmentEffectsError::Expressions(
            PhysicalExpressionEffectsError::InvalidSource(_)
        ))
    ));
}
#[test]
fn sql_window_fresh_composer_uses_real_row_number_source_journal() {
    let owner = crate::compiler::compile_authored_aggregate_for_test(
        "SELECT ROW_NUMBER() OVER (ORDER BY order_key) FROM orders",
    );
    let mut total = 0;
    for fragment in owner.plan().fragments().values() {
        if !fragment.expressions().iter().any(|(_, source)| {
            matches!(
                source.kind,
                novarocks_physical_plan::ExprKind::WindowCall { .. }
            )
        }) {
            continue;
        }
        let occurrences = author_physical_occurrences_observed(
            fragment,
            owner.function_catalog().as_ref(),
            &Control::default(),
        )
        .unwrap();
        let (policy, expressions, relations) = scopes(&owner, fragment, &occurrences);
        let result = author_sql_fragment_effects_observed(
            &owner,
            input(
                &owner,
                fragment,
                &occurrences,
                policy,
                &expressions,
                &relations,
            ),
            &Control::default(),
        )
        .unwrap();
        assert_eq!(result.calls.entries().len(), expressions.len());
        total += expressions.len();
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
            .find(|fragment| {
                fragment
                    .nodes()
                    .values()
                    .any(|node| matches!(node.kind, NodeKind::TableFunction { .. }))
            })
            .unwrap();
        let occurrences = author_physical_occurrences_observed(
            fragment,
            owner.function_catalog().as_ref(),
            &Control::default(),
        )
        .unwrap();
        let (policy, expressions, relations) = scopes(&owner, fragment, &occurrences);
        let result = author_sql_fragment_effects_observed(
            &owner,
            input(
                &owner,
                fragment,
                &occurrences,
                policy,
                &expressions,
                &relations,
            ),
            &Control::default(),
        )
        .unwrap();
        assert_eq!(relations.len(), 1);
        assert_eq!(result.calls.entries().len(), expressions.len() + 1);
    }
}

#[test]
fn sql_if_fresh_composer_keeps_distinct_guarded_branch_domains_from_original_sources() {
    let owner = crate::compiler::compile_authored_aggregate_for_test(
        "SELECT IF(order_key > 0, ABS(order_key), ABS(order_key + 1)) FROM orders",
    );
    let mut branches = 0;
    for fragment in owner.plan().fragments().values() {
        if !fragment.expressions().iter().any(|(_, source)| {
            matches!(
                source.kind,
                novarocks_physical_plan::ExprKind::FunctionCall { .. }
            )
        }) {
            continue;
        }
        let occurrences = author_physical_occurrences_observed(
            fragment,
            owner.function_catalog().as_ref(),
            &Control::default(),
        )
        .unwrap();
        let (policy, expressions, relations) = scopes(&owner, fragment, &occurrences);
        let result = author_sql_fragment_effects_observed(
            &owner,
            input(
                &owner,
                fragment,
                &occurrences,
                policy,
                &expressions,
                &relations,
            ),
            &Control::default(),
        )
        .unwrap();
        assert_eq!(result.calls.entries().len(), expressions.len());
        let mut domains = std::collections::BTreeSet::new();
        let mut definitions = std::collections::BTreeSet::new();
        for (&id, scope) in &expressions {
            let novarocks_physical_plan::ExprKind::FunctionCall { function, .. } =
                &scope.source.kind
            else {
                continue;
            };
            if !function.function_id.as_str().contains("/abs/") {
                continue;
            }
            let call = &result.calls.entries()[&PhysicalCallSite::Expression(id)];
            let original = &occurrences.root_uses.flow().uses()[&id];
            assert_eq!(call.context, original.context);
            assert!(domains.insert(call.context.domain));
            assert!(definitions.insert(original.definition));
            let domain = &occurrences.root_uses.flow().domains()[&call.context.domain];
            assert!(domain.guard.is_some());
            assert_eq!(result.summaries[&id].context(), call.context);
            branches += 1;
        }
        assert_eq!(domains.len(), 2);
    }
    assert_eq!(branches, 2);
}

#[test]
fn sql_repeat_fresh_composer_consumes_late_nullable_operational_requests() {
    use super::super::contract_lowering::lowered_scalar_source_tests::lowered_canonical_scalar_tests::{repeat_plan,finish as finish_repeat};
    let owner = finish_repeat(&repeat_plan(true));
    let fragment = owner
        .plan()
        .fragments()
        .values()
        .find(|fragment| {
            fragment.expressions().iter().any(|(_, source)| {
                matches!(
                    source.kind,
                    novarocks_physical_plan::ExprKind::FunctionCall { .. }
                )
            })
        })
        .unwrap();
    let occurrences = author_physical_occurrences_observed(
        fragment,
        owner.function_catalog().as_ref(),
        &Control::default(),
    )
    .unwrap();
    let (policy, expressions, relations) = scopes(&owner, fragment, &occurrences);
    assert_eq!(expressions.len(), 2);
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    for scope in expressions.values() {
        let entry = owner
            .checked_expression_call_source_observed(fragment, scope.source, &mut work)
            .unwrap();
        let original = entry.captured().request();
        let canonical = entry.canonical_operational().unwrap().request();
        let novarocks_functions::FunctionArgument::Value {
            value_type: original,
            constant: None,
        } = &original.arguments[0]
        else {
            panic!("original nonconstant source");
        };
        let novarocks_functions::FunctionArgument::Value {
            value_type: late,
            constant: None,
        } = &canonical.arguments[0]
        else {
            panic!("late nonconstant source");
        };
        assert!(!original.nullable());
        assert!(late.nullable());
        assert!(scope.source.ty.nullable());
        assert!(canonical.expected_result_type.is_none());
    }
    work.finish().unwrap();
    prefixes(
        |control| {
            author_sql_fragment_effects_observed(
                &owner,
                input(
                    &owner,
                    fragment,
                    &occurrences,
                    policy,
                    &expressions,
                    &relations,
                ),
                control,
            )
        },
        true,
    );
}
