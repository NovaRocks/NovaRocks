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

//! Genuine SQL completion/journal merge requests; no manual journal constructor.

use super::super::{
    expression_occurrences::author_physical_occurrences_observed,
    lowered_draft::CheckedAggregateLogicalSourceEntry,
    physical_aggregate_requests::{
        PhysicalAggregateRequestError, author_physical_aggregate_merge_request_observed,
    },
    physical_expression_effects::{
        PhysicalExpressionEffectsInput, author_physical_expression_effects_observed,
    },
};
use super::*;
use novarocks_functions::{AggregateKernelPhase, PreparedPureKernel};
use novarocks_physical_plan::{AggregatePhase, NodeKind};
use novarocks_type_contract::{CompilePhase, EvaluationDemand, PureCompileControl};
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
            assert!(index <= stop, "callback after first refusal");
        }
        trace.push((phase, units));
        match self.refusal.clone() {
            Some((stop, cause)) if stop == index => Err(cause),
            _ => Ok(()),
        }
    }
}
fn final_call(
    owner: &crate::compiler::SqlAuthoredPhysicalPlan,
) -> (&Fragment, &PhysicalNode, PhysicalCallSite, &AggregateCall) {
    for fragment in owner.plan().fragments().values() {
        for node in fragment.nodes().values() {
            if let NodeKind::Aggregate { calls, .. } = &node.kind {
                for (ordinal, call) in calls.iter().enumerate() {
                    if matches!(call.binding.phase, AggregatePhase::Final { .. }) {
                        return (
                            fragment,
                            node,
                            PhysicalCallSite::Aggregate {
                                node: node.id,
                                call: u32::try_from(ordinal).unwrap(),
                            },
                            call,
                        );
                    }
                }
            }
        }
    }
    panic!("the genuine SQL completion must emit a Final call");
}
fn loan<'a>(
    owner: &'a crate::compiler::SqlAuthoredPhysicalPlan,
    control: &dyn PureCompileControl,
) -> CheckedAggregateLogicalSourceEntry<'a> {
    let (fragment, node, site, call) = final_call(owner);
    let mut work =
        CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization).unwrap();
    let entry = owner
        .checked_aggregate_source_observed(fragment, node, site, call, &mut work)
        .unwrap();
    work.finish().unwrap();
    entry
}
fn setup<'a>(
    entry: &CheckedAggregateLogicalSourceEntry<'a>,
    owner: &crate::compiler::SqlAuthoredPhysicalPlan,
) -> (
    AuthoredPhysicalOccurrences<'a>,
    BTreeMap<ExpressionUseId, ScopedExpressionEffects>,
) {
    let control = Control::default();
    let catalog = crate::functions::builtin_sql_function_catalog();
    let occurrences =
        author_physical_occurrences_observed(entry.fragment(), catalog, &control).unwrap();
    let expressions = author_physical_expression_effects_observed(
        PhysicalExpressionEffectsInput {
            temporal_sources: None,
            fragment: entry.fragment(),
            roots: &occurrences.root_uses,
            constants: owner.plan().constants(),
            parameters: owner.plan().parameters(),
            literal_policy: entry.captured().constant_policy(),
            call_scopes: &BTreeMap::new(),
        },
        catalog,
        &control,
    )
    .unwrap();
    (occurrences, expressions.summaries)
}
fn run(
    entry: &CheckedAggregateLogicalSourceEntry<'_>,
    occurrences: &AuthoredPhysicalOccurrences,
    children: &BTreeMap<ExpressionUseId, ScopedExpressionEffects>,
    parameters: &SemanticParameters,
    policy: DecimalOverflowPolicy,
    control: &dyn PureCompileControl,
) -> Result<FreshPhysicalAggregateOccurrence, PhysicalAggregateOccurrenceError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
    let result = (|| {
        let request = author_physical_aggregate_merge_request_observed(entry, &mut work).map_err(
            |error| match error {
                PhysicalAggregateRequestError::Control(cause) => {
                    PhysicalAggregateOccurrenceError::Control(cause)
                }
                _ => PhysicalAggregateOccurrenceError::InvalidSource("merge request refused"),
            },
        )?;
        let selected = Arc::clone(request.selected());
        let result = prepare_physical_aggregate_merge_occurrence_observed(
            PhysicalAggregateMergeOccurrenceInput {
                fragment: entry.fragment(),
                node: entry.node(),
                source: entry.source(),
                request: &request,
                occurrences,
                child_effects: children,
                parameters,
                environment: &[],
                decimal_overflow_policy: policy,
                proof_scope: CallProofScope::Unconditional,
            },
            crate::functions::builtin_sql_function_catalog(),
            &mut work,
        )?;
        assert!(Arc::ptr_eq(
            &selected,
            result.preparation.call_contract().selected_owner()
        ));
        Ok(result)
    })();
    if matches!(&result, Err(PhysicalAggregateOccurrenceError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn prefixes(
    mut invoke: impl FnMut(
        &Control,
    )
        -> Result<FreshPhysicalAggregateOccurrence, PhysicalAggregateOccurrenceError>,
    success: bool,
) {
    let baseline = Control::default();
    assert_eq!(invoke(&baseline).is_ok(), success);
    let expected = baseline.trace.into_inner().unwrap();
    assert!(expected.len() > 2);
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
                matches!(invoke(&control), Err(PhysicalAggregateOccurrenceError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), expected[..=index]);
        }
    }
}

#[test]
fn merge_genuine_count_star_keeps_logical_zero_and_physical_state_one() {
    let owner = crate::compiler::compile_authored_aggregate_for_test("SELECT COUNT(*) FROM orders");
    let control = Control::default();
    let entry = loan(&owner, &control);
    let mut work =
        CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization).unwrap();
    let request = author_physical_aggregate_merge_request_observed(&entry, &mut work).unwrap();
    work.finish().unwrap();
    assert_eq!(request.request().logical_argument_count, 0);
    assert!(request.request().arguments.is_empty());
    assert_eq!(entry.source().arguments.len(), 1);
    assert_eq!(request.state_id(), entry.source().arguments[0]);
    assert!(std::ptr::eq(
        request.state(),
        entry
            .fragment()
            .expressions()
            .get(request.state_id())
            .unwrap()
    ));
    assert_eq!(request.phase(), AggregateKernelPhase::Final);
    let (occurrences, children) = setup(&entry, &owner);
    let result = run(
        &entry,
        &occurrences,
        &children,
        owner.plan().parameters(),
        request.decimal_overflow_policy(),
        &Control::default(),
    )
    .unwrap();
    assert_eq!(
        result.preparation.implementation().abi,
        PureKernelAbi::AggregateWindowV1
    );
    let PreparedPureKernel::Aggregate(kernel) = result.preparation.prepared() else {
        panic!("ordinary COUNT merge lifecycle")
    };
    assert_eq!(kernel.contract().phase(), AggregateKernelPhase::Final);
    assert_eq!(kernel.contract().call().logical_argument_count(), 0);
    assert_eq!(
        kernel.contract().state_input_type(),
        Some(&request.state().ty)
    );
    assert_eq!(
        kernel.contract().final_type(),
        &entry.source().binding.function.result_type
    );
    assert_eq!(
        kernel.contract().state_format().as_str(),
        entry.source().binding.state_format.as_str()
    );
    assert_eq!(
        result.frozen.effects.argument_control,
        ArgumentControl::Aggregate
    );
}

#[test]
fn merge_genuine_count_min_max_keep_original_complete_logical_signature() {
    for name in ["COUNT", "MIN", "MAX"] {
        let owner = crate::compiler::compile_authored_aggregate_for_test(&format!(
            "SELECT {name}(order_key) FROM orders"
        ));
        let entry = loan(&owner, &Control::default());
        let (occurrences, children) = setup(&entry, &owner);
        let result = run(
            &entry,
            &occurrences,
            &children,
            owner.plan().parameters(),
            entry.captured().binding().decimal_overflow_policy(),
            &Control::default(),
        )
        .unwrap();
        assert_eq!(entry.captured().request().logical_argument_count, 1);
        let PreparedPureKernel::Aggregate(kernel) = result.preparation.prepared() else {
            panic!("aggregate merge")
        };
        assert_eq!(kernel.contract().call().logical_argument_count(), 1);
        assert_eq!(
            kernel.contract().intermediate_type(),
            &entry.source().binding.intermediate_type
        );
        assert_eq!(
            kernel.contract().state_input_type(),
            Some(
                &entry
                    .fragment()
                    .expressions()
                    .get(entry.source().arguments[0])
                    .unwrap()
                    .ty
            )
        );
        assert!(!kernel.contract().distinct());
        assert!(kernel.contract().order_keys().is_empty());
    }
}

#[test]
fn merge_genuine_journal_request_and_fresh_success_have_every_original_control_prefix() {
    let owner = crate::compiler::compile_authored_aggregate_for_test("SELECT COUNT(*) FROM orders");
    let entry = loan(&owner, &Control::default());
    let (occurrences, children) = setup(&entry, &owner);
    prefixes(
        |control| {
            run(
                &entry,
                &occurrences,
                &children,
                owner.plan().parameters(),
                entry.captured().binding().decimal_overflow_policy(),
                control,
            )
        },
        true,
    );
}

#[test]
fn merge_original_policy_refusal_keeps_ordinary_tail_and_every_control_prefix() {
    let owner =
        crate::compiler::compile_authored_aggregate_for_test("SELECT MIN(order_key) FROM orders");
    let entry = loan(&owner, &Control::default());
    let (occurrences, children) = setup(&entry, &owner);
    let wrong = match entry.captured().binding().decimal_overflow_policy() {
        DecimalOverflowPolicy::OutputNull => DecimalOverflowPolicy::ReportError,
        DecimalOverflowPolicy::ReportError => DecimalOverflowPolicy::OutputNull,
    };
    let baseline = Control::default();
    assert!(matches!(
        run(
            &entry,
            &occurrences,
            &children,
            owner.plan().parameters(),
            wrong,
            &baseline
        ),
        Err(PhysicalAggregateOccurrenceError::InvalidSource(
            "aggregate merge occurrence changes the original logical source policy"
        ))
    ));
    assert!(baseline.trace.lock().unwrap().last().is_some());
    prefixes(
        |control| {
            run(
                &entry,
                &occurrences,
                &children,
                owner.plan().parameters(),
                wrong,
                control,
            )
        },
        false,
    );
}

#[test]
fn merge_missing_actual_state_effects_refuses_instead_of_using_pure_default() {
    let owner = crate::compiler::compile_authored_aggregate_for_test("SELECT COUNT(*) FROM orders");
    let entry = loan(&owner, &Control::default());
    let (occurrences, _) = setup(&entry, &owner);
    let children = BTreeMap::new();
    assert!(matches!(
        run(
            &entry,
            &occurrences,
            &children,
            owner.plan().parameters(),
            entry.captured().binding().decimal_overflow_policy(),
            &Control::default()
        ),
        Err(PhysicalAggregateOccurrenceError::Relational(
            PhysicalRelationalEffectsError::MissingChildEffects(_)
        ))
    ));
    prefixes(
        |control| {
            run(
                &entry,
                &occurrences,
                &children,
                owner.plan().parameters(),
                entry.captured().binding().decimal_overflow_policy(),
                control,
            )
        },
        false,
    );
}

#[test]
fn merge_state_summary_context_preserves_conservative_child_row_error() {
    let owner =
        crate::compiler::compile_authored_aggregate_for_test("SELECT MIN(order_key) FROM orders");
    let entry = loan(&owner, &Control::default());
    let (occurrences, mut children) = setup(&entry, &owner);
    let site = ExpressionRootSite {
        node: entry.node().id,
        role: ExpressionRootRole::AggregateArgument {
            call: match entry.site() {
                PhysicalCallSite::Aggregate { call, .. } => call,
                _ => unreachable!(),
            },
            argument: 0,
        },
    };
    let id = occurrences.root_uses.bindings()[&site];
    let context = occurrences.root_uses.flow().uses()[&id].context;
    assert_eq!(context.demand, EvaluationDemand::Value);
    let mut effects = ExpressionEffects::PURE_VALUE;
    effects.may_raise_row_error = true;
    children.insert(id, ScopedExpressionEffects::primitive(context, effects));
    let result = run(
        &entry,
        &occurrences,
        &children,
        owner.plan().parameters(),
        entry.captured().binding().decimal_overflow_policy(),
        &Control::default(),
    )
    .unwrap();
    assert!(
        result
            .preparation
            .effects()
            .for_use(result.frozen.context)
            .unwrap()
            .may_raise_row_error
    );
    assert_ne!(result.frozen.context.region, context.region);
}

#[test]
fn merge_real_count_attachment_frozen_preparation_preserves_same_arc_and_state_demand() {
    use novarocks_functions::{
        EngineFunctionCatalogBuilder, InstalledPureKernel, PureImplementationDeclaration,
        PureImplementationId,
    };
    use novarocks_type_contract::{AggregateStateFormatId, FunctionId, FunctionOverloadId};
    let original = crate::functions::builtin_engine_function_catalog();
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder
        .register(
            original
                .definition("count", FunctionKind::Aggregate)
                .unwrap()
                .clone(),
        )
        .unwrap();
    let catalog = builder
        .seal_pure([InstalledPureKernel {
            function: FunctionId::try_new("builtin.aggregate/count/v1").unwrap(),
            kind: FunctionKind::Aggregate,
            implementation: PureImplementationDeclaration {
                overload: FunctionOverloadId::try_new("builtin.aggregate/count/derived-v1")
                    .unwrap(),
                implementation: PureImplementationId::try_new(
                    "builtin.aggregate/count/selected-v1",
                )
                .unwrap(),
                abi: PureKernelAbi::AggregateWindowV1,
            },
            aggregate_state_format: Some(
                AggregateStateFormatId::try_new("novarocks/count/state-v1").unwrap(),
            ),
        }])
        .unwrap();
    let owner = crate::compiler::compile_authored_aggregate_for_test("SELECT COUNT(*) FROM orders");
    let entry = loan(&owner, &Control::default());
    let (occurrences, children) = setup(&entry, &owner);
    let control = Control::default();
    let mut work =
        CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization).unwrap();
    let request = author_physical_aggregate_merge_request_observed(&entry, &mut work).unwrap();
    let context = relational_context_observed(&occurrences, entry.site(), &mut work).unwrap();
    let PhysicalCallSite::Aggregate { node, call } = entry.site() else {
        unreachable!()
    };
    let (id, effects) = relational_root_effects_observed(
        &occurrences.root_uses,
        ExpressionRootSite {
            node,
            role: ExpressionRootRole::AggregateArgument { call, argument: 0 },
        },
        request.state_id(),
        &children,
        &mut work,
    )
    .unwrap();
    let state_context = occurrences.root_uses.flow().uses()[&id].context;
    let input = CallEffectInput {
        context,
        argument_uses: novarocks_functions::CallArgumentUses::AggregateMerge {
            phase: request.phase(),
            state_context,
            state_input_type: &request.state().ty,
        },
        function_id: &request.function().function_id,
        kind: FunctionKind::Aggregate,
        selected: request.selected().as_ref(),
        request: request.request(),
        environment: &[],
        parameters: owner.plan().parameters(),
        decimal_overflow_policy: request.decimal_overflow_policy(),
        proof_scope: CallProofScope::Unconditional,
    };
    let options = || request.preparation(ScopedExpressionEffects::primitive(context, effects));
    let fresh = catalog
        .prepare_fresh(input, Arc::clone(request.selected()), options(), &control)
        .unwrap();
    let frozen = catalog
        .prepare_frozen(
            input,
            Arc::clone(request.selected()),
            fresh.call_contract().effects(),
            options(),
            &control,
        )
        .unwrap();
    work.finish().unwrap();
    assert_eq!(
        fresh.call_contract().effects(),
        frozen.call_contract().effects()
    );
    assert!(Arc::ptr_eq(
        request.selected(),
        frozen.call_contract().selected_owner()
    ));
    let PreparedPureKernel::Aggregate(kernel) = frozen.prepared() else {
        panic!("real COUNT aggregate frozen handle")
    };
    assert_eq!(kernel.contract().phase(), AggregateKernelPhase::Final);
    assert_eq!(kernel.contract().call().logical_argument_count(), 0);
    assert_eq!(
        kernel.contract().state_input_type(),
        Some(&request.state().ty)
    );
}

#[test]
fn merge_foreign_equal_sql_owner_cannot_replace_the_original_checked_journal_loan() {
    let owner = crate::compiler::compile_authored_aggregate_for_test("SELECT COUNT(*) FROM orders");
    let foreign =
        crate::compiler::compile_authored_aggregate_for_test("SELECT COUNT(*) FROM orders");
    let entry = loan(&owner, &Control::default());
    let (fragment, node, _, source) = final_call(&foreign);
    let (occurrences, children) = setup(&entry, &owner);
    let mut invoke = |control: &Control| -> Result<
        FreshPhysicalAggregateOccurrence,
        PhysicalAggregateOccurrenceError,
    > {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
        let result = (|| {
            let request = author_physical_aggregate_merge_request_observed(&entry, &mut work)
                .map_err(|error| match error {
                    PhysicalAggregateRequestError::Control(cause) => {
                        PhysicalAggregateOccurrenceError::Control(cause)
                    }
                    _ => PhysicalAggregateOccurrenceError::InvalidSource("merge request refused"),
                })?;
            prepare_physical_aggregate_merge_occurrence_observed(
                PhysicalAggregateMergeOccurrenceInput {
                    fragment,
                    node,
                    source,
                    request: &request,
                    occurrences: &occurrences,
                    child_effects: &children,
                    parameters: owner.plan().parameters(),
                    environment: &[],
                    decimal_overflow_policy: request.decimal_overflow_policy(),
                    proof_scope: CallProofScope::Unconditional,
                },
                crate::functions::builtin_sql_function_catalog(),
                &mut work,
            )
        })();
        if matches!(&result, Err(PhysicalAggregateOccurrenceError::Control(_))) {
            return result;
        }
        work.finish()?;
        result
    };
    assert!(matches!(
        invoke(&Control::default()),
        Err(PhysicalAggregateOccurrenceError::InvalidSource(
            "aggregate merge occurrence borrows a different original journal source"
        ))
    ));
    prefixes(&mut invoke, false);
}
