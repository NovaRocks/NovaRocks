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

use super::super::expression_occurrences::author_physical_occurrences_observed;
use super::*;
use novarocks_type_contract::FunctionInstanceState;
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
            assert!(index <= stop, "callback after originating control refusal");
        }
        trace.push((phase, units));
        match self.refusal.clone() {
            Some((stop, cause)) if stop == index => Err(cause),
            _ => Ok(()),
        }
    }
}
fn source_scopes<'a>(
    owner: &'a SqlAuthoredPhysicalPlan,
    fragment: &'a Fragment,
    occurrences: &AuthoredPhysicalOccurrences,
) -> (
    ConstantPolicy,
    BTreeMap<PhysicalCallSite, PhysicalRelationalCallSourceScope<'a>>,
) {
    let control = crate::compiler::SqlCompileControl::unbounded();
    let mut work =
        CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization).unwrap();
    let mut policy = None;
    let scopes = occurrences
        .relational_contexts
        .iter()
        .map(|&(site, context)| {
            let id = match site {
                PhysicalCallSite::Aggregate { node, .. }
                | PhysicalCallSite::TopNState { node, .. } => node,
                _ => panic!("actual aggregate fixture contains a different lifecycle"),
            };
            let node = &fragment.nodes()[&id];
            let source = aggregate_source(node, site).unwrap();
            let entry = owner
                .checked_aggregate_source_observed(fragment, node, site, source, &mut work)
                .unwrap();
            let captured = entry.captured();
            if let Some(original) = policy {
                assert_eq!(original, captured.constant_policy());
            } else {
                policy = Some(captured.constant_policy());
            }
            (
                site,
                PhysicalRelationalCallSourceScope {
                    source: node,
                    decimal_overflow_policy: captured.binding().decimal_overflow_policy(),
                    environment: &[],
                    proof_scope: CallProofScope::Region(context.region),
                },
            )
        })
        .collect();
    work.finish().unwrap();
    (policy.expect("actual aggregate fragment"), scopes)
}
fn input<'a>(
    owner: &'a SqlAuthoredPhysicalPlan,
    fragment: &'a Fragment,
    occurrences: &'a AuthoredPhysicalOccurrences<'a>,
    policy: ConstantPolicy,
    scopes: &'a BTreeMap<PhysicalCallSite, PhysicalRelationalCallSourceScope<'a>>,
    expressions: &'a BTreeMap<ExpressionUseId, PhysicalCallSourceScope<'a>>,
) -> PhysicalFragmentEffectsInput<'a> {
    PhysicalFragmentEffectsInput {
        fragment,
        occurrences,
        constants: owner.plan().constants(),
        parameters: owner.plan().parameters(),
        literal_policy: policy,
        expression_scopes: expressions,
        relational_scopes: scopes,
    }
}
fn aggregate_fragments(owner: &SqlAuthoredPhysicalPlan) -> impl Iterator<Item = &Fragment> {
    owner.plan().fragments().values().filter(|fragment| {
        fragment.nodes().values().any(
            |node| matches!(&node.kind, NodeKind::Aggregate { calls, .. } if !calls.is_empty()),
        )
    })
}
fn prefixes(
    mut invoke: impl FnMut(
        &Control,
    ) -> Result<AuthoredPhysicalFragmentEffects, PhysicalFragmentEffectsError>,
    success: bool,
) {
    let control = Control::default();
    let result = invoke(&control);
    assert_eq!(result.is_ok(), success, "{result:?}");
    let expected = control.trace.into_inner().unwrap();
    assert_eq!(
        expected.first(),
        Some(&(CompilePhase::FunctionSpecialization, 0))
    );
    if success {
        assert!(expected.len() >= 2);
        assert!(expected.iter().any(|(_, units)| *units > 0));
    } else {
        let original = result.unwrap_err();
        if matches!(
            &original,
            PhysicalFragmentEffectsError::InvalidSource(
                "SQL aggregate composition loans a foreign plan, pool or parameter source"
            )
        ) {
            // Identity is rejected before the first bounded flush. These two
            // pending source checks do not authorize a later error footer.
            assert_eq!(expected, [(CompilePhase::FunctionSpecialization, 0)]);
        }
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control {
                trace: Default::default(),
                refusal: Some((expected.len(), cause)),
            };
            let actual = invoke(&control).unwrap_err();
            match (&actual, &original) {
                (
                    PhysicalFragmentEffectsError::InvalidSource(actual),
                    PhysicalFragmentEffectsError::InvalidSource(original),
                ) => assert_eq!(actual, original),
                (
                    PhysicalFragmentEffectsError::MissingScope(actual),
                    PhysicalFragmentEffectsError::MissingScope(original),
                ) => assert_eq!(actual, original),
                (
                    PhysicalFragmentEffectsError::Aggregate(
                        PhysicalAggregateOccurrenceError::InvalidSource(actual),
                    ),
                    PhysicalFragmentEffectsError::Aggregate(
                        PhysicalAggregateOccurrenceError::InvalidSource(original),
                    ),
                ) => assert_eq!(actual, original),
                _ => panic!(
                    "different original SQL composition refusal: {actual:?}; expected {original:?}"
                ),
            }
            assert_eq!(*control.trace.lock().unwrap(), expected);
        }
    }
    for index in 0..expected.len() {
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
            assert_eq!(*control.trace.lock().unwrap(), expected[..=index]);
        }
    }
}

#[test]
fn genuine_sql_partial_final_composer_covers_every_actual_call_from_original_journal() {
    let owner = crate::compiler::compile_authored_aggregate_for_test(
        "SELECT COUNT(*), COUNT(7), MIN(order_key), MAX(order_key) FROM orders",
    );
    let catalog = owner.function_catalog().as_ref();
    let mut update = 0;
    let mut merge = 0;
    for fragment in aggregate_fragments(&owner) {
        let occurrences =
            author_physical_occurrences_observed(fragment, catalog, &Control::default()).unwrap();
        let (policy, scopes) = source_scopes(&owner, fragment, &occurrences);
        let expressions = BTreeMap::new();
        let result = author_sql_fragment_effects_observed(
            &owner,
            input(
                &owner,
                fragment,
                &occurrences,
                policy,
                &scopes,
                &expressions,
            ),
            &Control::default(),
        )
        .unwrap();
        assert_eq!(
            result.calls.entries().len(),
            occurrences.relational_contexts.len()
        );
        for &(site, context) in &occurrences.relational_contexts {
            let id = match site {
                PhysicalCallSite::Aggregate { node, .. } => node,
                _ => panic!("aggregate"),
            };
            let source = aggregate_source(&fragment.nodes()[&id], site).unwrap();
            match source.binding.phase {
                AggregatePhase::Single | AggregatePhase::Partial { .. } => update += 1,
                AggregatePhase::Intermediate { .. } | AggregatePhase::Final { .. } => merge += 1,
            }
            let frozen = &result.calls.entries()[&site];
            assert_eq!(frozen.context, context);
            assert_eq!(
                frozen.effects.instance_state,
                FunctionInstanceState::AggregateInstance
            );
            assert_eq!(
                frozen.decimal_overflow_policy,
                scopes[&site].decimal_overflow_policy
            );
        }
        result
            .calls
            .validate_fragment(fragment, &occurrences.root_uses, &Control::default())
            .unwrap();
    }
    assert!(
        update >= 4 && merge >= 4,
        "the genuine optimizer must supply both phase families"
    );
}

#[test]
fn genuine_sql_composer_success_and_original_policy_refusal_observe_all_real_prefixes() {
    let owner = crate::compiler::compile_authored_aggregate_for_test("SELECT COUNT(*) FROM orders");
    let catalog = owner.function_catalog().as_ref();
    for fragment in aggregate_fragments(&owner) {
        let occurrences =
            author_physical_occurrences_observed(fragment, catalog, &Control::default()).unwrap();
        let (policy, mut scopes) = source_scopes(&owner, fragment, &occurrences);
        let expressions = BTreeMap::new();
        prefixes(
            |control| {
                author_sql_fragment_effects_observed(
                    &owner,
                    input(
                        &owner,
                        fragment,
                        &occurrences,
                        policy,
                        &scopes,
                        &expressions,
                    ),
                    control,
                )
            },
            true,
        );
        for scope in scopes.values_mut() {
            scope.decimal_overflow_policy = match scope.decimal_overflow_policy {
                DecimalOverflowPolicy::ReportError => DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::OutputNull => DecimalOverflowPolicy::ReportError,
            };
        }
        let refusal = author_sql_fragment_effects_observed(
            &owner,
            input(
                &owner,
                fragment,
                &occurrences,
                policy,
                &scopes,
                &expressions,
            ),
            &Control::default(),
        );
        assert!(matches!(
            refusal,
            Err(PhysicalFragmentEffectsError::Aggregate(
                PhysicalAggregateOccurrenceError::InvalidSource(_)
            ))
        ));
        prefixes(
            |control| {
                author_sql_fragment_effects_observed(
                    &owner,
                    input(
                        &owner,
                        fragment,
                        &occurrences,
                        policy,
                        &scopes,
                        &expressions,
                    ),
                    control,
                )
            },
            false,
        );
    }
}

#[test]
fn genuine_sql_composer_refuses_foreign_equal_owner_pool_parameters_and_extra_missing_scopes() {
    let owner = crate::compiler::compile_authored_aggregate_for_test("SELECT COUNT(*) FROM orders");
    let foreign =
        crate::compiler::compile_authored_aggregate_for_test("SELECT COUNT(*) FROM orders");
    let cloned = owner.clone();
    let catalog = owner.function_catalog().as_ref();
    for fragment in aggregate_fragments(&owner) {
        let occurrences =
            author_physical_occurrences_observed(fragment, catalog, &Control::default()).unwrap();
        let (policy, mut scopes) = source_scopes(&owner, fragment, &occurrences);
        let expressions = BTreeMap::new();
        assert!(
            author_sql_fragment_effects_observed(
                &cloned,
                input(
                    &owner,
                    fragment,
                    &occurrences,
                    policy,
                    &scopes,
                    &expressions
                ),
                &Control::default()
            )
            .is_ok()
        );
        prefixes(
            |control| {
                author_sql_fragment_effects_observed(
                    &foreign,
                    input(
                        &owner,
                        fragment,
                        &occurrences,
                        policy,
                        &scopes,
                        &expressions,
                    ),
                    control,
                )
            },
            false,
        );
        for pool in [true, false] {
            prefixes(
                |control| {
                    let mut source = input(
                        &owner,
                        fragment,
                        &occurrences,
                        policy,
                        &scopes,
                        &expressions,
                    );
                    if pool {
                        source.constants = foreign.plan().constants();
                    } else {
                        source.parameters = foreign.plan().parameters();
                    }
                    author_sql_fragment_effects_observed(&owner, source, control)
                },
                false,
            );
        }
        let (site, scope) = scopes.pop_first().unwrap();
        assert!(
            matches!(author_sql_fragment_effects_observed(&owner, input(&owner, fragment, &occurrences, policy, &scopes, &expressions), &Control::default()), Err(PhysicalFragmentEffectsError::MissingScope(actual)) if actual == site)
        );
        prefixes(
            |control| {
                author_sql_fragment_effects_observed(
                    &owner,
                    input(
                        &owner,
                        fragment,
                        &occurrences,
                        policy,
                        &scopes,
                        &expressions,
                    ),
                    control,
                )
            },
            false,
        );
        scopes.insert(site, scope);
        let extra = match site {
            PhysicalCallSite::Aggregate { node, call } => PhysicalCallSite::Aggregate {
                node,
                call: call + 1,
            },
            _ => panic!("aggregate"),
        };
        let original = &scopes[&site];
        scopes.insert(
            extra,
            PhysicalRelationalCallSourceScope {
                source: original.source,
                decimal_overflow_policy: original.decimal_overflow_policy,
                environment: original.environment,
                proof_scope: original.proof_scope,
            },
        );
        prefixes(
            |control| {
                author_sql_fragment_effects_observed(
                    &owner,
                    input(
                        &owner,
                        fragment,
                        &occurrences,
                        policy,
                        &scopes,
                        &expressions,
                    ),
                    control,
                )
            },
            false,
        );
    }
}

#[test]
fn genuine_sql_composer_refuses_captured_constant_policy_change_and_direct_merge_reconstruction() {
    let owner = crate::compiler::compile_authored_aggregate_for_test("SELECT COUNT(*) FROM orders");
    let catalog = owner.function_catalog().as_ref();
    let mut final_seen = false;
    for fragment in aggregate_fragments(&owner) {
        let occurrences =
            author_physical_occurrences_observed(fragment, catalog, &Control::default()).unwrap();
        let (policy, scopes) = source_scopes(&owner, fragment, &occurrences);
        let expressions = BTreeMap::new();
        let mut changed = policy;
        changed.max_retained_buffer_bytes -= 1;
        prefixes(
            |control| {
                author_sql_fragment_effects_observed(
                    &owner,
                    input(
                        &owner,
                        fragment,
                        &occurrences,
                        changed,
                        &scopes,
                        &expressions,
                    ),
                    control,
                )
            },
            false,
        );
        if fragment.nodes().values().any(|node| matches!(&node.kind, NodeKind::Aggregate { calls, .. } if calls.iter().any(|call| matches!(call.binding.phase, AggregatePhase::Final { .. })))) {
            let result = author_physical_fragment_effects_observed(input(&owner, fragment, &occurrences, policy, &scopes, &expressions), catalog, &Control::default());
            assert!(matches!(result, Err(PhysicalFragmentEffectsError::AggregateRequest(PhysicalAggregateRequestError::MissingLogicalSource(_)))));
            final_seen = true;
        }
    }
    assert!(final_seen);
}
