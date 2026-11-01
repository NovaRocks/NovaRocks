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

//! Journal requests borrow the original Table emitter's complete relation.

use super::super::{
    contract_lowering::lowered_window_table_source_tests::{
        finish, list_value, policy, table_fixture, table_source,
    },
    lowered_draft::SqlSourceJournalError,
};
use super::*;
use crate::{
    analysis::{ExprKind, TypedExpr},
    compiler::SqlAuthoredPhysicalPlan,
};
use novarocks_physical_plan::{ExprKind as PhysicalExprKind, TableFunctionOutput};
use novarocks_type_contract::PureCompileControl;
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
fn run<'source>(
    owner: &'source SqlAuthoredPhysicalPlan,
    fragment: &'source Fragment,
    source: &'source PhysicalNode,
    control: &dyn PureCompileControl,
) -> Result<AuthoredPhysicalTableRequest<'source>, PhysicalTableRequestError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
    let result = (|| {
        let entry = owner.checked_table_source_observed(fragment, source, &mut work)?;
        author_physical_table_request_from_journal_observed(&entry, &mut work)
    })();
    if matches!(&result, Err(PhysicalTableRequestError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn prefixes(
    mut invoke: impl FnMut(&Control) -> Result<(), PhysicalTableRequestError>,
    success: bool,
) {
    let baseline = Control::default();
    let result = invoke(&baseline);
    assert_eq!(result.is_ok(), success, "{result:?}");
    let expected = baseline.trace.into_inner().unwrap();
    assert!(expected.len() >= 2);
    for stop in 0..expected.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control {
                trace: Mutex::new(Vec::new()),
                refusal: Some((stop, cause.clone())),
            };
            assert!(
                matches!(invoke(&control), Err(PhysicalTableRequestError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), expected[..=stop]);
        }
    }
}
fn constant(argument: &FunctionArgument) -> &novarocks_functions::ConstantValue {
    let FunctionArgument::Value {
        constant: Some(value),
        ..
    } = argument
    else {
        panic!("original selected constant channel")
    };
    value
}
fn emitted(
    owner: &SqlAuthoredPhysicalPlan,
    fragment: &Fragment,
    id: ExprId,
) -> novarocks_functions::ConstantValue {
    let PhysicalExprKind::Constant(reference) = fragment.expressions().get(id).unwrap().kind else {
        panic!("actual selected physical constant")
    };
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let value = owner
        .plan()
        .constants()
        .resolve_source_observed(reference, &mut work)
        .unwrap();
    work.finish().unwrap();
    value
}

#[test]
fn genuine_sql_unnest_journal_borrows_original_whole_relation_and_same_selected_arc() {
    let owner = crate::compiler::compile_authored_aggregate_for_test(
        "SELECT u.* FROM orders, UNNEST([order_key]) u",
    );
    let (fragment, source) = table_source(&owner);
    let control = Control::default();
    let mut work =
        CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization).unwrap();
    let entry = owner
        .checked_table_source_observed(fragment, source, &mut work)
        .unwrap();
    let canonical = entry.canonical_operational();
    let request = author_physical_table_request_from_journal_observed(&entry, &mut work).unwrap();
    assert!(std::ptr::eq(request.source(), source));
    assert!(Arc::ptr_eq(request.selected(), canonical.selected()));
    assert!(std::ptr::eq(
        request.request().arguments,
        canonical.request().arguments
    ));
    assert!(request.request().expected_result_type.is_none());
    assert_eq!(request.request().logical_argument_count, 1);
    let FunctionResultType::Relation(results) = &request.selected().result_type else {
        panic!("whole relation");
    };
    assert_eq!(results.as_ref(), request.function().result_types.as_ref());
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0],
        novarocks_type_contract::FunctionValueType::new(arrow::datatypes::DataType::Int64, true)
    );
    assert_eq!(
        request.captured_decimal_overflow_policy(),
        Some(entry.captured().binding().decimal_overflow_policy())
    );
    assert_eq!(
        request.captured_constant_policy(),
        Some(entry.captured().constant_policy())
    );
    let clone = owner.clone();
    let cloned_entry = clone
        .checked_table_source_observed(fragment, source, &mut work)
        .unwrap();
    let cloned =
        author_physical_table_request_from_journal_observed(&cloned_entry, &mut work).unwrap();
    assert!(Arc::ptr_eq(request.selected(), cloned.selected()));
    assert!(std::ptr::eq(
        request.request().arguments,
        cloned.request().arguments
    ));
    work.finish().unwrap();
}

fn original_list_value(ordinal: u32) -> crate::analysis::TypedExpr {
    use arrow::array::Array;
    let field = std::sync::Arc::new(
        arrow::datatypes::Field::new("original.element", arrow::datatypes::DataType::Int64, true)
            .with_metadata(std::collections::HashMap::<String, String>::from([(
                "source.child".into(),
                "kept".into(),
            )])),
    );
    let array = arrow::array::ListArray::new(
        field,
        arrow::buffer::OffsetBuffer::new(arrow::buffer::ScalarBuffer::from(vec![0i32, 1, 3, 3])),
        std::sync::Arc::new(arrow::array::Int64Array::from(vec![
            Some(999),
            Some(7),
            None,
        ])),
        Some(arrow::buffer::NullBuffer::from(vec![true, true, false])),
    );
    let ty = novarocks_type_contract::FunctionValueType::new(array.data_type().clone(), true);
    let source_field = std::sync::Arc::new(arrow::datatypes::Field::new(
        "original.list",
        array.data_type().clone(),
        true,
    ));
    let pool = novarocks_functions::ConstantPool::try_new(
        source_field,
        ty.clone(),
        array.to_data(),
        policy(),
        CompilePhase::Validate,
        &Control::default(),
    )
    .unwrap();
    crate::analysis::TypedExpr {
        kind: ExprKind::Constant(pool.value(ordinal).unwrap()),
        value_type: ty,
    }
}

#[test]
fn table_journal_selected_nonzero_cv_ordinals_keep_field_backing_metadata_and_left_outputs() {
    let (source_plan, original_binding) =
        table_fixture(vec![original_list_value(1), original_list_value(2)], true);
    let owner = finish(&source_plan);
    let (fragment, node) = table_source(&owner);
    let request = run(&owner, fragment, node, &Control::default()).unwrap();
    let control = Control::default();
    let mut work =
        CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization).unwrap();
    let entry = owner
        .checked_table_source_observed(fragment, node, &mut work)
        .unwrap();
    assert!(std::ptr::eq(
        entry.captured().binding().resolved(),
        original_binding.resolved()
    ));
    assert!(Arc::ptr_eq(
        request.selected(),
        entry.canonical_operational().selected()
    ));
    assert!(std::ptr::eq(
        request.request().arguments,
        entry.canonical_operational().request().arguments
    ));
    assert_eq!(request.request().arguments.len(), 2);
    let NodeKind::TableFunction {
        function,
        outputs,
        left_outer,
        ..
    } = &node.kind
    else {
        unreachable!()
    };
    assert!(*left_outer);
    let FunctionResultType::Relation(results) = &request.selected().result_type else {
        panic!("relation");
    };
    assert_eq!(results.as_ref(), function.result_types.as_ref());
    assert_eq!(
        results.as_ref(),
        &[
            novarocks_type_contract::FunctionValueType::new(
                arrow::datatypes::DataType::Int64,
                true
            ),
            novarocks_type_contract::FunctionValueType::new(
                arrow::datatypes::DataType::Int64,
                true
            )
        ]
    );
    for (index, ordinal) in [1, 2].into_iter().enumerate() {
        let original = constant(&entry.captured().request().arguments[index]);
        let operational = constant(&request.request().arguments[index]);
        let actual = emitted(&owner, fragment, entry.arguments()[index]);
        assert_eq!(original.ordinal(), ordinal);
        assert_eq!(operational.ordinal(), ordinal);
        assert_eq!(actual.ordinal(), ordinal);
        assert!(Arc::ptr_eq(
            original.pool().array(),
            operational.pool().array()
        ));
        assert!(Arc::ptr_eq(original.pool().array(), actual.pool().array()));
        assert!(Arc::ptr_eq(
            original.pool().field_ref(),
            operational.pool().field_ref()
        ));
        assert!(Arc::ptr_eq(
            original.pool().field_ref(),
            actual.pool().field_ref()
        ));
        assert_eq!(original.value_type(), operational.value_type());
        let arrow::datatypes::DataType::List(field) = &original.value_type().data_type() else {
            panic!("original nested field");
        };
        assert_eq!(field.name(), "item");
        assert!(field.metadata().is_empty());
        let arrow::datatypes::DataType::List(source_field) = original.field().data_type() else {
            panic!("source nested field")
        };
        assert_eq!(source_field.name(), "original.element");
        assert_eq!(source_field.metadata()["source.child"], "kept");
    }
    for output in outputs {
        if let TableFunctionOutput::FunctionResult {
            result_ordinal,
            value,
        } = output
        {
            let mut expected = results[*result_ordinal as usize].clone();
            expected = expected.with_nullable(true).unwrap();
            assert_eq!(fragment.values()[value].ty, expected);
        }
    }
    work.finish().unwrap();
}

#[test]
fn table_journal_computed_none_stays_none_when_emitted_as_constant() {
    let value = list_value(1);
    let input = TypedExpr {
        value_type: value.value_type.clone(),
        kind: ExprKind::Nested(Box::new(value)),
    };
    let (source, _) = table_fixture(vec![input], false);
    let owner = finish(&source);
    let (fragment, node) = table_source(&owner);
    let journal = run(&owner, fragment, node, &Control::default()).unwrap();
    assert!(matches!(
        journal.request().arguments,
        [FunctionArgument::Value { constant: None, .. }]
    ));
    let NodeKind::TableFunction { arguments, .. } = &node.kind else {
        unreachable!()
    };
    assert!(matches!(
        fragment.expressions().get(arguments[0]).unwrap().kind,
        PhysicalExprKind::Constant(_)
    ));
    let control = Control::default();
    let mut work =
        CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization).unwrap();
    let direct = author_physical_table_request_observed(
        node,
        fragment,
        owner.plan().constants(),
        policy(),
        &mut work,
    )
    .unwrap();
    assert!(matches!(
        direct.request().arguments,
        [FunctionArgument::Value {
            constant: Some(_),
            ..
        }]
    ));
    assert!(direct.captured_constant_policy().is_none());
    assert!(direct.captured_decimal_overflow_policy().is_none());
    assert_eq!(
        journal.selected().result_type,
        direct.selected().result_type
    );
    work.finish().unwrap();
}

#[test]
fn table_journal_typed_null_is_original_cv_not_relation_or_scalar_retagging() {
    let (source, _) = table_fixture(vec![list_value(2)], true);
    let owner = finish(&source);
    let (fragment, node) = table_source(&owner);
    let request = run(&owner, fragment, node, &Control::default()).unwrap();
    let selected = constant(&request.request().arguments[0]);
    assert_eq!(selected.ordinal(), 2);
    assert!(
        selected
            .is_null_observed(CompilePhase::Validate, &Control::default())
            .unwrap()
    );
    assert!(matches!(
        selected.value_type().data_type(),
        arrow::datatypes::DataType::List(_)
    ));
    assert!(request.request().expected_result_type.is_none());
    assert_eq!(request.captured_constant_policy(), Some(policy()));
    assert!(matches!(
        request.selected().result_type,
        FunctionResultType::Relation(_)
    ));
}

#[test]
fn table_journal_rejects_foreign_node_and_missing_entry_without_direct_fallback() {
    let (source, _) = table_fixture(vec![list_value(1)], false);
    let owner = finish(&source);
    let (fragment, node) = table_source(&owner);
    let foreign = node.clone();
    assert_eq!(foreign.id, node.id);
    assert!(matches!(
        run(&owner, fragment, &foreign, &Control::default()),
        Err(PhysicalTableRequestError::Journal(
            SqlSourceJournalError::InvalidSource(_)
        ))
    ));
    prefixes(
        |control| run(&owner, fragment, &foreign, control).map(|_| ()),
        false,
    );
    let missing = fragment
        .nodes()
        .values()
        .find(|candidate| !matches!(candidate.kind, NodeKind::TableFunction { .. }))
        .unwrap();
    assert!(matches!(
        run(&owner, fragment, missing, &Control::default()),
        Err(PhysicalTableRequestError::Journal(
            SqlSourceJournalError::MissingEntry
        ))
    ));
    prefixes(
        |control| run(&owner, fragment, missing, control).map(|_| ()),
        false,
    );
}

#[test]
fn table_journal_every_actual_small_callback_preserves_three_original_control_causes() {
    let (source, _) = table_fixture(vec![list_value(1), list_value(2)], true);
    let owner = finish(&source);
    let (fragment, node) = table_source(&owner);
    prefixes(
        |control| run(&owner, fragment, node, control).map(|_| ()),
        true,
    );
}
