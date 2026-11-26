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

use super::super::contract_lowering::original_emission_test_observer::OriginalWindowEmission;
use super::super::{
    contract_lowering::lowered_window_table_source_tests::{
        finish, integer, observe_window_emission, policy, window_fixture, window_source,
    },
    lowered_draft::SqlSourceJournalError,
};
use super::*;
use crate::{
    analysis::{ExprKind as SqlExprKind, SortItem, TypedExpr},
    compiler::SqlAuthoredPhysicalPlan,
};
use arrow::array::{Array, Int64Array};
use arrow::datatypes::DataType;
use novarocks_constant_contract::ConstantPool;
use novarocks_type_contract::{PureCompileControl, WindowFrameExclusion, WindowFrameUnits};
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
            assert!(at <= stop, "callback after first refusal");
        }
        trace.push((phase, units));
        match self.refusal.clone() {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}
fn loan<'a>(owner: &'a SqlAuthoredPhysicalPlan) -> CheckedWindowLogicalSourceEntry<'a> {
    let (fragment, node, site, source) = window_source(owner);
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let entry = owner
        .checked_window_source_observed(fragment, node, site, source, &mut work)
        .unwrap();
    work.finish().unwrap();
    entry
}
fn run<'a>(
    entry: &CheckedWindowLogicalSourceEntry<'a>,
    pools: &ConstantPools,
    control: &dyn PureCompileControl,
) -> Result<AuthoredPhysicalWindowRequest<'a>, PhysicalWindowRequestError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
    let result = author_physical_window_request_from_journal_observed(entry, pools, &mut work);
    if matches!(&result, Err(PhysicalWindowRequestError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn run_emission<'source>(
    emission: &OriginalWindowEmission<'source>,
    control: &dyn PureCompileControl,
) -> Result<AuthoredPhysicalWindowRequest<'source>, PhysicalWindowRequestError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
    let result = author_window_request_from_emission_for_test(emission, &mut work);
    if matches!(&result, Err(PhysicalWindowRequestError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn all_emission_prefixes(emission: &OriginalWindowEmission<'_>) {
    let baseline = Control::default();
    assert!(run_emission(emission, &baseline).is_ok());
    let expected = baseline.trace.into_inner().unwrap();
    assert!(expected.len() >= 2);
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
                matches!(run_emission(emission, &control), Err(PhysicalWindowRequestError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), expected[..=at]);
        }
    }
}
fn all_prefixes(entry: &CheckedWindowLogicalSourceEntry<'_>, pools: &ConstantPools, success: bool) {
    let baseline = Control::default();
    assert_eq!(run(entry, pools, &baseline).is_ok(), success);
    let expected = baseline.trace.into_inner().unwrap();
    assert!(expected.len() >= 2);
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
                matches!(run(entry, pools, &control), Err(PhysicalWindowRequestError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), expected[..=at]);
        }
    }
}
fn constant(argument: &FunctionArgument) -> Option<&novarocks_constant_contract::ConstantValue> {
    match argument {
        FunctionArgument::Value { constant, .. } => constant.as_ref(),
        _ => panic!("Value source"),
    }
}

#[test]
fn real_sql_window_journal_borrows_original_box_selection_constraint_and_policies() {
    for (sql, count) in [
        (
            "SELECT ROW_NUMBER() OVER (ORDER BY order_key) FROM orders",
            0,
        ),
        ("SELECT RANK() OVER (ORDER BY order_key) FROM orders", 0),
        (
            "SELECT LEAD(order_key, 2, 7) OVER (ORDER BY order_key) FROM orders",
            3,
        ),
        ("SELECT NTILE(3) OVER (ORDER BY order_key) FROM orders", 1),
        (
            "SELECT MIN(order_key) OVER (ORDER BY order_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM orders",
            1,
        ),
    ] {
        let owner = crate::compiler::compile_authored_aggregate_for_test(sql);
        let entry = loan(&owner);
        let canonical = entry.canonical_operational();
        let actual = run(&entry, owner.plan().constants(), &Control::default()).unwrap();
        assert!(std::ptr::eq(actual.source(), entry.source()));
        assert!(std::ptr::eq(
            actual.request().arguments,
            canonical.request().arguments
        ));
        assert!(Arc::ptr_eq(actual.selected(), canonical.selected()));
        assert_eq!(actual.request().logical_argument_count, count);
        assert!(actual.request().expected_result_type.is_none());
        assert!(entry.captured().request().expected_result_type.is_some());
        assert_eq!(
            actual.captured_decimal_overflow_policy(),
            Some(entry.captured().binding().decimal_overflow_policy())
        );
        assert_eq!(
            actual.captured_constant_policy(),
            Some(entry.captured().constant_policy())
        );
        if sql.contains("LEAD") {
            assert!(constant(&actual.request().arguments[0]).is_none());
        }
    }
}

#[test]
fn window_journal_nonzero_cv_and_order_share_original_pool_and_frame_author() {
    let ty = FunctionValueType::new(DataType::Int64, false);
    let root = Arc::new(ty.try_to_field("window.original").unwrap());
    let pool = ConstantPool::try_new(
        root.clone(),
        ty.clone(),
        Int64Array::from(vec![999, 7, 41]).to_data(),
        policy(),
        CompilePhase::Validate,
        &Control::default(),
    )
    .unwrap();
    let value = |ordinal| TypedExpr {
        kind: SqlExprKind::Constant(pool.value(ordinal).unwrap()),
        value_type: ty.clone(),
    };
    let (plan, _) = window_fixture(
        vec![value(1)],
        vec![SortItem {
            expr: value(2),
            asc: false,
            nulls_first: true,
        }],
    );
    let control = Control::default();
    let mut seen = 0;
    let result = observe_window_emission(&plan, &control, |entry, _| {
        seen += 1;
        let authored = run_emission(&entry, &control).unwrap();
        assert!(std::ptr::eq(authored.source(), entry.source()));
        assert!(std::ptr::eq(
            authored.request().arguments,
            entry.canonical().request().arguments
        ));
        assert!(Arc::ptr_eq(
            authored.selected(),
            entry.canonical().selected()
        ));
        assert_eq!(
            authored.captured_decimal_overflow_policy(),
            Some(entry.captured().binding().decimal_overflow_policy())
        );
        assert_eq!(
            authored.captured_constant_policy(),
            Some(entry.captured().constant_policy())
        );
        assert_eq!(authored.request().logical_argument_count, 1);
        assert_eq!(authored.request().arguments.len(), 2);
        for (index, ordinal, expected) in [(0, 1, 7), (1, 2, 41)] {
            let actual = constant(&authored.request().arguments[index]).unwrap();
            let original = constant(&entry.captured().request().arguments[index]).unwrap();
            assert_eq!(actual.ordinal(), ordinal);
            assert_eq!(actual.value_type(), &ty);
            assert_eq!(actual.try_i64().unwrap(), Some(expected));
            assert!(Arc::ptr_eq(actual.pool().array(), pool.array()));
            assert!(Arc::ptr_eq(actual.pool().field_ref(), &root));
            assert_eq!(
                actual.pool().backing_identity(),
                original.pool().backing_identity()
            );
        }
        let options = authored.aggregate.as_ref().unwrap();
        assert_eq!(options.phase, AggregateKernelPhase::Single);
        assert_eq!(
            options.order_keys.as_ref(),
            &[AggregateOrderKey {
                ascending: false,
                nulls_first: true
            }]
        );
        assert_eq!(
            authored.window.frame(),
            Some(&novarocks_type_contract::WindowFrame {
                units: WindowFrameUnits::Rows,
                start: novarocks_type_contract::WindowBound::Preceding(2),
                end: novarocks_type_contract::WindowBound::CurrentRow,
                exclusion: WindowFrameExclusion::NoOthers
            })
        );
        assert!(!authored.window.ignore_nulls());
        all_emission_prefixes(&entry);
        Ok(())
    });
    assert_eq!(seen, 1);
    result.unwrap();
}

#[test]
fn window_journal_computed_none_and_original_typed_null_remain_distinct() {
    let computed = TypedExpr {
        kind: SqlExprKind::Cast {
            cast_mode: None,
            expr: novarocks_parser::ast::AstBox::new(integer(7)),
            target: DataType::Int64,
            decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
        },
        value_type: FunctionValueType::new(DataType::Int64, false),
    };
    for (argument, is_null) in [
        (computed, false),
        (
            TypedExpr {
                kind: SqlExprKind::Literal(crate::analysis::LiteralValue::Null),
                value_type: FunctionValueType::new(DataType::Int64, true),
            },
            true,
        ),
    ] {
        let (plan, _) = window_fixture(vec![argument], vec![]);
        let owner = finish(&plan);
        let entry = loan(&owner);
        let authored = run(&entry, owner.plan().constants(), &Control::default()).unwrap();
        if is_null {
            let original = constant(&entry.captured().request().arguments[0]).unwrap();
            let retained = constant(&authored.request().arguments[0]).unwrap();
            assert_eq!(original.ordinal(), retained.ordinal());
            assert!(Arc::ptr_eq(
                original.pool().array(),
                retained.pool().array()
            ));
            assert!(Arc::ptr_eq(
                original.pool().field_ref(),
                retained.pool().field_ref()
            ));
            assert!(
                constant(&authored.request().arguments[0])
                    .unwrap()
                    .is_null_observed(CompilePhase::Validate, &Control::default())
                    .unwrap()
            );
        } else {
            assert!(constant(&entry.captured().request().arguments[0]).is_none());
            assert!(constant(&authored.request().arguments[0]).is_none());
        }
        assert!(std::ptr::eq(
            authored.request().arguments,
            entry.canonical_operational().request().arguments
        ));
    }
}

#[test]
fn window_journal_small_success_and_wrong_lifecycle_preserve_every_first_cause() {
    let owner = crate::compiler::compile_authored_aggregate_for_test(
        "SELECT MIN(order_key) OVER (ORDER BY order_key ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM orders",
    );
    let entry = loan(&owner);
    all_prefixes(&entry, owner.plan().constants(), true);
    let scalar_owner =
        crate::compiler::compile_authored_aggregate_for_test("SELECT ABS(order_key) FROM orders");
    let (fragment, node) = scalar_owner
        .plan()
        .fragments()
        .values()
        .find_map(|fragment| {
            fragment.nodes().values().find_map(|node| {
                node.expressions
                    .as_ref()?
                    .definitions()
                    .iter()
                    .any(|source| {
                        matches!(
                            source.kind,
                            novarocks_expression_contract::ExprKind::FunctionCall { .. }
                        )
                    })
                    .then_some((fragment, node))
            })
        })
        .unwrap();
    // Scalar and Window receipts are disjoint Rust types. Exercise the actual
    // relational-source gate at this genuine scalar node; no Window slot exists.
    let invoke = |control: &Control| {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
        let result = scalar_owner.checked_window_source_observed(
            fragment,
            node,
            novarocks_physical_plan::PhysicalCallSite::Window {
                node: node.id,
                call: 0,
            },
            entry.source(),
            &mut work,
        );
        if matches!(&result, Err(SqlSourceJournalError::Control(_))) {
            return result;
        }
        work.finish()?;
        result
    };
    let baseline_control = Control::default();
    assert!(matches!(
        invoke(&baseline_control),
        Err(SqlSourceJournalError::InvalidSource(
            "window journal loans a different actual call slot"
        ))
    ));
    let baseline = baseline_control.trace.into_inner().unwrap();
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
            assert!(
                matches!(invoke(&control), Err(SqlSourceJournalError::Control(actual)) if actual == cause)
            );
            assert_eq!(control.trace.into_inner().unwrap(), baseline[..=stop]);
        }
    }
}

#[test]
fn window_journal_foreign_owner_refuses_before_request_and_direct_component_stays_owned() {
    let sql = "SELECT MIN(order_key) OVER (ORDER BY order_key) FROM orders";
    let owner = crate::compiler::compile_authored_aggregate_for_test(sql);
    let foreign = crate::compiler::compile_authored_aggregate_for_test(sql);
    let (fragment, node, site, source) = window_source(&owner);
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    assert!(matches!(
        foreign.checked_window_source_observed(fragment, node, site, source, &mut work),
        Err(SqlSourceJournalError::InvalidSource(_))
    ));
    work.finish().unwrap();
    let control = Control::default();
    let mut work =
        CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization).unwrap();
    let direct = author_physical_window_request_observed(
        source,
        node,
        owner.plan().constants(),
        policy(),
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    assert!(direct.captured_constant_policy().is_none());
    assert!(direct.captured_decimal_overflow_policy().is_none());
    assert!(std::ptr::eq(
        direct.request().expected_result_type.unwrap(),
        &source.binding.result_type
    ));
}

#[test]
fn window_journal_wide_order_channels_observe_real_comparison_quantum() {
    let orders = (0..320)
        .map(|_| SortItem {
            expr: integer(41),
            asc: true,
            nulls_first: false,
        })
        .collect();
    let (plan, _) = window_fixture(vec![integer(7)], orders);
    let control = Control::default();
    let mut seen = 0;
    let result = observe_window_emission(&plan, &control, |entry, _| {
        seen += 1;
        let baseline = Control::default();
        let authored = run_emission(&entry, &baseline).unwrap();
        assert_eq!(authored.request().arguments.len(), 321);
        assert_eq!(authored.request().logical_argument_count, 1);
        assert!(std::ptr::eq(
            authored.request().arguments,
            entry.canonical().request().arguments
        ));
        let expected = baseline.trace.into_inner().unwrap();
        assert!(expected.iter().any(|(_, units)| *units == 256));
        for at in [
            0,
            expected
                .iter()
                .position(|(_, units)| *units == 256)
                .unwrap(),
            expected.len() - 1,
        ] {
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
                    matches!(run_emission(&entry, &control), Err(PhysicalWindowRequestError::Control(actual)) if actual == cause)
                );
                assert_eq!(*control.trace.lock().unwrap(), expected[..=at]);
            }
        }
        Ok(())
    });
    assert_eq!(seen, 1);
    result.unwrap();
}
