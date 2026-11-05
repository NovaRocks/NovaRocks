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

//! Genuine SQL Update journal requests, with no synthetic checked-entry constructor.

use super::super::lowered_draft::{
    AggregateRuntimeDemand, CheckedAggregateLogicalSourceEntry, SqlSourceJournalError,
};
use super::*;
use novarocks_functions::FunctionResultType;
use novarocks_type_contract::{
    ExpressionEffects, FunctionArgumentEvaluation, FunctionFailureBehavior,
    FunctionIntrinsicRowError, FunctionVolatility, PureCompileControl,
};
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
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop, "callback after original refusal");
        }
        trace.push((phase, units));
        match self.refusal.clone() {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}

fn call(
    owner: &crate::compiler::SqlAuthoredPhysicalPlan,
    update: bool,
) -> (&Fragment, &PhysicalNode, PhysicalCallSite, &AggregateCall) {
    for fragment in owner.plan().fragments().values() {
        for node in fragment.nodes().values() {
            if let NodeKind::Aggregate { calls, .. } = &node.kind {
                for (ordinal, source) in calls.iter().enumerate() {
                    let matches = if update {
                        matches!(source.binding.phase, AggregatePhase::Partial { .. })
                    } else {
                        matches!(source.binding.phase, AggregatePhase::Final { .. })
                    };
                    if matches {
                        return (
                            fragment,
                            node,
                            PhysicalCallSite::Aggregate {
                                node: node.id,
                                call: u32::try_from(ordinal).unwrap(),
                            },
                            source,
                        );
                    }
                }
            }
        }
    }
    panic!("genuine SQL completion must emit the requested Partial/Final phase");
}

fn loan<'a>(
    owner: &'a crate::compiler::SqlAuthoredPhysicalPlan,
    update: bool,
) -> CheckedAggregateLogicalSourceEntry<'a> {
    let (fragment, node, site, source) = call(owner, update);
    let control = Control::default();
    let mut work =
        CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization).unwrap();
    let entry = owner
        .checked_aggregate_source_observed(fragment, node, site, source, &mut work)
        .unwrap();
    work.finish().unwrap();
    entry
}

fn run<'source>(
    entry: &CheckedAggregateLogicalSourceEntry<'source>,
    control: &dyn PureCompileControl,
) -> Result<AuthoredPhysicalAggregateUpdateRequest<'source>, PhysicalAggregateRequestError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
    let result = author_physical_aggregate_update_request_from_journal_observed(entry, &mut work);
    if matches!(&result, Err(PhysicalAggregateRequestError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

fn prefixes(
    mut invoke: impl FnMut(&Control) -> Result<(), PhysicalAggregateRequestError>,
    success: bool,
) {
    let baseline = Control::default();
    assert_eq!(invoke(&baseline).is_ok(), success);
    let expected = baseline.trace.into_inner().unwrap();
    assert!(expected.len() > 1);
    // The footer can legally carry zero or positive pending work.
    for at in 0..expected.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control {
                trace: Default::default(),
                refusal: Some((at, cause.clone())),
            };
            assert!(
                matches!(invoke(&control), Err(PhysicalAggregateRequestError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), expected[..=at]);
        }
    }
}

fn effects() -> ScopedExpressionEffects {
    ScopedExpressionEffects::primitive(
        novarocks_type_contract::ExpressionEffectContext {
            use_id: novarocks_type_contract::ExpressionUseId::new(91),
            domain: novarocks_type_contract::EvaluationDomainId::new(92),
            demand: novarocks_type_contract::EvaluationDemand::Value,
        },
        ExpressionEffects::PURE_VALUE,
    )
}

#[test]
fn journal_update_partial_count_min_max_borrow_canonical_request_and_selected_arc() {
    for sql in [
        "SELECT COUNT(*) FROM orders",
        "SELECT COUNT(order_key) FROM orders",
        "SELECT MIN(order_key) FROM orders",
        "SELECT MAX(order_key) FROM orders",
    ] {
        let owner = crate::compiler::compile_authored_aggregate_for_test(sql);
        let entry = loan(&owner, true);
        assert_eq!(entry.runtime(), AggregateRuntimeDemand::Update);
        let request = run(&entry, &Control::default()).unwrap();
        assert!(std::ptr::eq(request.source(), entry.source()));
        assert!(std::ptr::eq(request.node(), entry.node()));
        assert_eq!(request.site(), entry.site());
        let original = entry.captured().request();
        let canonical = entry
            .canonical()
            .expect("actual Partial canonical emission");
        assert!(canonical.belongs_to(entry.captured()));
        let operational = canonical.request();
        let actual = request.request();
        assert!(std::ptr::eq(actual.arguments, operational.arguments));
        assert!(actual.expected_result_type.is_none());
        assert!(operational.expected_result_type.is_none());
        assert!(entry.captured().binding().result_constraint().is_none());
        let FunctionResultType::Scalar(original_result) =
            &entry.captured().binding().resolved().selected.result_type
        else {
            panic!("original aggregate scalar result")
        };
        assert!(std::ptr::eq(
            original.expected_result_type.unwrap(),
            original_result
        ));
        assert!(Arc::ptr_eq(request.selected(), canonical.selected()));
        let logical_count = if sql.contains("COUNT(*)") { 0 } else { 1 };
        assert_eq!(actual.logical_argument_count, logical_count);
        assert_eq!(actual.arguments.len(), logical_count);
        assert_eq!(request.source().arguments.len(), logical_count);
        assert_eq!(
            request.captured_decimal_overflow_policy(),
            Some(entry.captured().binding().decimal_overflow_policy())
        );
        assert_eq!(
            request.captured_constant_policy(),
            Some(entry.captured().constant_policy())
        );
        assert_eq!(
            request.selected().as_ref(),
            &entry.captured().binding().resolved().selected
        );
        let PureCallPreparation::Aggregate { options, .. } = request.preparation(effects()) else {
            panic!("actual Aggregate update preparation")
        };
        assert_eq!(options.phase, AggregateKernelPhase::Partial);
        assert_eq!(options.distinct, entry.source().distinct);
        assert!(options.order_keys.is_empty());
        assert!(options.state_input_type.is_none());
        assert_eq!(
            request
                .selected()
                .aggregate
                .as_ref()
                .unwrap()
                .intermediate_type,
            entry.source().binding.intermediate_type
        );
        assert_eq!(
            request.selected().aggregate.as_ref().unwrap().state_format,
            entry.source().binding.state_format
        );
    }
}

#[test]
fn journal_update_literal_keeps_original_captured_constant_and_physical_pool_backing() {
    let owner = crate::compiler::compile_authored_aggregate_for_test("SELECT MIN(7) FROM orders");
    let entry = loan(&owner, true);
    let request = run(&entry, &Control::default()).unwrap();
    let original = entry.captured().request();
    let FunctionArgument::Value {
        constant: Some(original_value),
        value_type,
    } = &original.arguments[0]
    else {
        panic!("actual literal source remains a captured constant")
    };
    let FunctionArgument::Value {
        constant: Some(value),
        value_type: actual_type,
    } = &request.request().arguments[0]
    else {
        panic!("journal request must not discard the constant")
    };
    let canonical = entry
        .canonical()
        .expect("actual Partial canonical emission");
    assert!(canonical.belongs_to(entry.captured()));
    assert!(std::ptr::eq(
        request.request().arguments,
        canonical.request().arguments
    ));
    assert!(Arc::ptr_eq(request.selected(), canonical.selected()));
    assert!(request.request().expected_result_type.is_none());
    // Operational projection owns a cloned CV handle, never a fresh admission.
    // The original capture and physical pool retain the same backing and Field.
    assert_eq!(actual_type, value_type);
    assert_eq!(value.ordinal(), original_value.ordinal());
    assert!(Arc::ptr_eq(
        value.pool().array(),
        original_value.pool().array()
    ));
    assert!(Arc::ptr_eq(
        value.pool().field_ref(),
        original_value.pool().field_ref()
    ));
    let control = Control::default();
    assert_eq!(
        value
            .signed_integer_observed(CompilePhase::FunctionSpecialization, &control)
            .unwrap(),
        Some(7)
    );
    let expression = entry
        .fragment()
        .expressions()
        .get(entry.source().arguments[0])
        .unwrap();
    let novarocks_physical_plan::ExprKind::Constant(reference) = &expression.kind else {
        panic!("original literal lowering publishes the captured pool reference")
    };
    let mut work =
        CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization).unwrap();
    let physical = owner
        .plan()
        .constants()
        .resolve_source_observed(*reference, &mut work)
        .unwrap();
    work.finish().unwrap();
    assert_eq!(physical.ordinal(), value.ordinal());
    assert!(Arc::ptr_eq(physical.pool().array(), value.pool().array()));
    assert!(Arc::ptr_eq(
        physical.pool().field_ref(),
        value.pool().field_ref()
    ));
}

#[test]
fn journal_update_computed_and_cast_arguments_remain_original_nonconstant_none() {
    for sql in [
        "SELECT MIN(order_key + 1) FROM orders",
        "SELECT MIN(CAST(order_key AS DOUBLE)) FROM orders",
    ] {
        let owner = crate::compiler::compile_authored_aggregate_for_test(sql);
        let entry = loan(&owner, true);
        let request = run(&entry, &Control::default()).unwrap();
        let FunctionArgument::Value {
            constant,
            value_type,
        } = &entry.captured().request().arguments[0]
        else {
            panic!("actual scalar argument")
        };
        assert!(
            constant.is_none(),
            "an actual column-dependent source is not a SQL constant"
        );
        let FunctionArgument::Value {
            constant: actual,
            value_type: actual_type,
        } = &request.request().arguments[0]
        else {
            panic!("actual scalar argument")
        };
        assert!(actual.is_none());
        assert_eq!(actual_type, value_type);
        let canonical = entry
            .canonical()
            .expect("actual Partial canonical emission");
        assert!(canonical.belongs_to(entry.captured()));
        assert!(std::ptr::eq(
            request.request().arguments,
            canonical.request().arguments
        ));
        assert!(Arc::ptr_eq(request.selected(), canonical.selected()));
        assert!(request.request().expected_result_type.is_none());
        let expression = entry
            .fragment()
            .expressions()
            .get(entry.source().arguments[0])
            .unwrap();
        assert_eq!(&expression.ty, actual_type);
    }
}

#[test]
fn journal_update_success_has_every_actual_original_control_prefix() {
    for sql in [
        "SELECT COUNT(*) FROM orders",
        "SELECT MIN(7) FROM orders",
        "SELECT MIN(order_key + 1) FROM orders",
    ] {
        let owner = crate::compiler::compile_authored_aggregate_for_test(sql);
        let entry = loan(&owner, true);
        prefixes(|control| run(&entry, control).map(|_| ()), true);
    }
}

#[test]
fn journal_update_final_phase_refusal_keeps_ordinary_footer_and_every_control_prefix() {
    let owner = crate::compiler::compile_authored_aggregate_for_test("SELECT COUNT(*) FROM orders");
    let entry = loan(&owner, false);
    let baseline = Control::default();
    assert!(matches!(
        run(&entry, &baseline),
        Err(PhysicalAggregateRequestError::MissingLogicalSource(
            AggregatePhase::Final { .. }
        ))
    ));
    assert_eq!(
        *baseline.trace.lock().unwrap(),
        [
            (CompilePhase::FunctionSpecialization, 0),
            (CompilePhase::FunctionSpecialization, 1)
        ]
    );
    prefixes(|control| run(&entry, control).map(|_| ()), false);
}

#[test]
fn journal_update_foreign_equal_owner_is_refused_before_a_checked_loan_is_published() {
    let owner =
        crate::compiler::compile_authored_aggregate_for_test("SELECT MIN(order_key) FROM orders");
    let foreign =
        crate::compiler::compile_authored_aggregate_for_test("SELECT MIN(order_key) FROM orders");
    let (fragment, node, site, source) = call(&foreign, true);
    let invoke = |control: &Control| {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
        let result =
            owner.checked_aggregate_source_observed(fragment, node, site, source, &mut work);
        if matches!(&result, Err(SqlSourceJournalError::Control(_))) {
            return result.map(|_| ());
        }
        work.finish()?;
        result.map(|_| ())
    };
    let baseline = Control::default();
    assert!(matches!(
        invoke(&baseline),
        Err(SqlSourceJournalError::InvalidSource(
            "aggregate journal loans a foreign plan or node"
        ))
    ));
    let expected = baseline.trace.into_inner().unwrap();
    for at in 0..expected.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control {
                trace: Default::default(),
                refusal: Some((at, cause.clone())),
            };
            assert!(
                matches!(invoke(&control), Err(SqlSourceJournalError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), expected[..=at]);
        }
    }
}

#[test]
fn journal_update_direct_physical_component_stays_explicit_and_has_no_capture_policy() {
    let owner =
        crate::compiler::compile_authored_aggregate_for_test("SELECT MIN(order_key) FROM orders");
    let entry = loan(&owner, true);
    let control = Control::default();
    let mut work =
        CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization).unwrap();
    let direct = author_physical_aggregate_update_request_observed(
        entry.source(),
        entry.node(),
        entry.site(),
        entry.fragment(),
        owner.plan().constants(),
        entry.captured().constant_policy(),
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    assert!(matches!(
        direct.arguments,
        AggregateUpdateArguments::OwnedPhysical(_)
    ));
    assert_eq!(direct.captured_decimal_overflow_policy(), None);
    assert_eq!(direct.captured_constant_policy(), None);
    assert_eq!(direct.request().logical_argument_count, 1);
    assert!(matches!(
        direct.request().arguments[0],
        FunctionArgument::Value { constant: None, .. }
    ));
    let journal = run(&entry, &Control::default()).unwrap();
    assert_eq!(direct.selected(), journal.selected());
    assert!(matches!(
        journal.arguments,
        AggregateUpdateArguments::Canonical { .. }
    ));
    assert!(Arc::ptr_eq(
        journal.selected(),
        entry.canonical().unwrap().selected()
    ));
}

#[test]
fn journal_update_shared_signature_comparison_rejects_full_type_drift_and_ignores_legacy_bits() {
    let owner =
        crate::compiler::compile_authored_aggregate_for_test("SELECT MIN(order_key) FROM orders");
    let entry = loan(&owner, true);
    // This directly tests the private correspondence helper, not a fabricated
    // journal token or source certification of an altered physical call.
    let mut changed = entry.source().clone();
    changed
        .binding
        .function
        .legacy_metadata
        .as_mut()
        .expect("actual legacy fixture")
        .volatility = FunctionVolatility::Volatile;
    changed
        .binding
        .function
        .legacy_metadata
        .as_mut()
        .expect("actual legacy fixture")
        .argument_evaluation = FunctionArgumentEvaluation::ShortCircuit;
    changed
        .binding
        .function
        .legacy_metadata
        .as_mut()
        .expect("actual legacy fixture")
        .failure_behavior = FunctionFailureBehavior::ReturnsNull;
    changed
        .binding
        .function
        .legacy_metadata
        .as_mut()
        .expect("actual legacy fixture")
        .intrinsic_row_error = FunctionIntrinsicRowError::MayRaise;
    let control = Control::default();
    let mut work =
        CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization).unwrap();
    let selected =
        captured_selected_correspondence_observed(entry.captured(), &changed, &mut work).unwrap();
    work.finish().unwrap();
    assert_eq!(
        selected.as_ref(),
        &entry.captured().binding().resolved().selected
    );
    changed.binding.function.result_type = changed
        .binding
        .function
        .result_type
        .with_nullable(!changed.binding.function.result_type.nullable())
        .unwrap();
    let invoke = |control: &Control| {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
        let result =
            captured_selected_correspondence_observed(entry.captured(), &changed, &mut work);
        if matches!(&result, Err(PhysicalAggregateRequestError::Control(_))) {
            return result.map(|_| ());
        }
        work.finish()?;
        result.map(|_| ())
    };
    assert!(matches!(
        invoke(&Control::default()),
        Err(PhysicalAggregateRequestError::InvalidSource(
            "merge binding differs from its captured final result type"
        ))
    ));
    prefixes(invoke, false);
}

#[cfg(test)]
#[path = "array_state_source_tests.rs"]
mod array_state_source_tests;
