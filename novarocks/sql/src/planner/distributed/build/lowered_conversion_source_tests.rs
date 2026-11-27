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

use super::tests::{column, dop, stats, values, version};
#[path = "lowered_conversion_canonical_tests.rs"]
mod lowered_conversion_canonical_tests;

use super::*;
use crate::compiler::SqlAuthoredPhysicalPlan;
use crate::planner::distributed::build::lowered_draft::{
    CheckedExpressionLogicalSourceEntry, SqlSourceJournalError,
};
use crate::planner::payload::PlanProjectNode;
use arrow::array::{Array, FixedSizeBinaryArray};
use novarocks_constant_contract::ConstantPool;
use novarocks_functions::{FunctionArgument, FunctionResultType};
use novarocks_type_contract::{
    CharacterKind, CharacterType, DecimalOverflowPolicy, TrinoCastMode, ValueKind, ValueLayout,
    ValueLogicalType, ValueTypeRef, ValueTypeStorageLifetime,
};
use std::sync::Mutex;

const CONVERSION: &str = "builtin.scalar/value_domain_conversion/v1";
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl Control {
    fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.trace.lock().unwrap().clone()
    }
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
fn policy() -> novarocks_functions::ConstantValidation {
    crate::constant::test_constant_policy()
}
fn largeint(nullable: bool) -> ValueType {
    ValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        nullable,
        ValueLogicalType::LargeInt,
    )
    .unwrap()
}
fn selected_largeint() -> TypedExpr {
    let ty = largeint(false);
    let field = Arc::new(ty.try_to_field("original_largeint").unwrap());
    let pool = ConstantPool::try_new(
        field,
        ty.clone(),
        FixedSizeBinaryArray::try_from_iter(
            [0_i128.to_be_bytes(), 7_i128.to_be_bytes()].into_iter(),
        )
        .unwrap()
        .to_data(),
        policy(),
        CompilePhase::Validate,
        &Control::default(),
    )
    .unwrap();
    TypedExpr {
        kind: ExprKind::Constant(pool.value(1).unwrap()),
        value_type: ty,
    }
}
fn cast(input: TypedExpr, target: ValueType, decimal: DecimalOverflowPolicy) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::Cast {
            cast_mode: None,
            expr: novarocks_parser::ast::AstBox::new(input),
            target: target.data_type().clone(),
            decimal_overflow_policy: decimal,
        },
        value_type: target,
    }
}

fn bounded_varchar() -> ValueType {
    let storage = ValueTypeStorageLifetime::pure();
    let character = CharacterType::new(CharacterKind::Varchar, 16).unwrap();
    let value = ValueTypeRef::try_new(
        ValueKind::BoundedCharacter(character),
        false,
        storage.clone(),
    )
    .unwrap();
    let layout = ValueLayout::try_from_normalized_arrow(DataType::Utf8, storage).unwrap();
    ValueType::try_from_value(value, layout).unwrap()
}

fn cast_to_bounded_varchar(input: TypedExpr, decimal: DecimalOverflowPolicy) -> TypedExpr {
    let converted = cast(input, ValueType::new(DataType::Utf8, false), decimal);
    let target = bounded_varchar();
    TypedExpr {
        kind: ExprKind::Cast {
            cast_mode: Some(TrinoCastMode::Explicit),
            expr: novarocks_parser::ast::AstBox::new(converted),
            target: DataType::Utf8,
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
        },
        value_type: target,
    }
}
fn project(expression: TypedExpr) -> PhysicalPlanNode {
    let mut output = column(
        71,
        "converted",
        expression.value_type.data_type().clone(),
        expression.value_type.nullable(),
    );
    output.value_type = expression.value_type.clone();
    PhysicalPlanNode {
        kind: PhysicalPlanKind::Project(PlanProjectNode {
            items: vec![crate::analysis::ProjectItem {
                expr: expression,
                output_name: "converted".into(),
                output_column_id: output.column_id,
            }],
            output_qualifier: None,
        }),
        children: vec![values(vec![], vec![vec![]])],
        output_columns: vec![output],
        stats: stats(),
        probe_runtime_filters: vec![],
    }
}
fn finish(expression: TypedExpr) -> SqlAuthoredPhysicalPlan {
    let control = Control::default();
    lower_final_physical_plan(
        &project(expression),
        version(),
        dop(),
        crate::functions::builtin_sql_function_catalog().snapshot(),
        novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
        false,
        policy(),
        &control,
    )
    .unwrap()
    .finish_observed(&control)
    .unwrap()
}
fn conversion(
    owner: &SqlAuthoredPhysicalPlan,
) -> (
    &Fragment,
    &novarocks_physical_plan::PhysicalNode,
    &novarocks_expression_contract::ExprNode,
) {
    owner
        .plan()
        .fragments()
        .values()
        .find_map(|fragment| {
            fragment.nodes().values().find_map(|node| {
                node.expressions
                    .as_ref()?
                    .definitions()
                    .iter()
                    .find_map(|source| {
                        if let ContractExprKind::FunctionCall { function, .. } = &source.kind
                            && function.function_id.as_str() == CONVERSION
                        {
                            Some((fragment, node, source))
                        } else {
                            None
                        }
                    })
            })
        })
        .expect("real conversion emission")
}
fn loan<'a>(
    owner: &'a SqlAuthoredPhysicalPlan,
    fragment: &'a Fragment,
    unit_node: &'a novarocks_physical_plan::PhysicalNode,
    source: &'a novarocks_expression_contract::ExprNode,
    control: &dyn PureCompileControl,
) -> Result<CheckedExpressionLogicalSourceEntry<'a>, SqlSourceJournalError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
    let result = owner.checked_conversion_source_observed(fragment, unit_node, source, &mut work);
    if matches!(&result, Err(SqlSourceJournalError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn source_constant(
    owner: &SqlAuthoredPhysicalPlan,
    unit_node: &novarocks_physical_plan::PhysicalNode,
    id: ExprId,
) -> novarocks_functions::ConstantValue {
    let ContractExprKind::Constant(reference) = unit_node
        .expressions
        .as_ref()
        .unwrap()
        .definition(id)
        .unwrap()
        .kind
    else {
        panic!("original constant channel")
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
fn conversion_journal_constant_input_preserves_original_none_and_full_binding_loans() {
    // A typed emitter fixture: this is the actual conversion producer, not a
    // reconstruction of an original SQL invocation from physical constants.
    let input = selected_largeint();
    let ExprKind::Constant(original) = &input.kind else {
        unreachable!()
    };
    let owner = finish(cast(
        input.clone(),
        ValueType::new(DataType::Utf8, false),
        DecimalOverflowPolicy::ReportError,
    ));
    let (fragment, unit_node, source) = conversion(&owner);
    let receipt = loan(&owner, fragment, unit_node, source, &Control::default()).unwrap();
    let request = receipt.captured().request();
    assert_eq!(request.arguments.len(), 1);
    let FunctionArgument::Value {
        value_type,
        constant,
    } = &request.arguments[0]
    else {
        unreachable!()
    };
    assert_eq!(value_type, &largeint(false));
    assert!(constant.is_none());
    let projection_control = Control::default();
    let mut projection_work =
        CompileCheckpoints::try_new(&projection_control, CompilePhase::FunctionSpecialization)
            .unwrap();
    let projected = receipt
        .operational_arguments_observed(owner.plan().constants(), &mut projection_work)
        .unwrap();
    projection_work.finish().unwrap();
    assert!(
        matches!(&projected[0], FunctionArgument::Value { value_type, constant: None }
        if value_type == &largeint(false))
    );
    let actual = source_constant(&owner, unit_node, receipt.arguments()[0]);
    assert_eq!(actual.ordinal(), 1);
    assert!(Arc::ptr_eq(actual.pool().array(), original.pool().array()));
    assert!(Arc::ptr_eq(
        actual.pool().field_ref(),
        original.pool().field_ref()
    ));
    assert_eq!(actual.try_largeint().unwrap(), Some(7));
    let binding = receipt.captured().binding();
    assert_eq!(binding.function_id.as_str(), CONVERSION);
    assert!(
        binding
            .selected
            .overload
            .as_str()
            .contains("largeint_to_utf8_text")
    );
    assert_eq!(
        binding.decimal_overflow_policy(),
        DecimalOverflowPolicy::ReportError
    );
    assert_eq!(receipt.captured().constant_policy(), policy());
    let FunctionResultType::Scalar(expected) = &binding.selected.result_type else {
        unreachable!()
    };
    assert!(std::ptr::eq(
        request.expected_result_type.unwrap(),
        expected
    ));
    assert_eq!(expected, &ValueType::new(DataType::Utf8, false));
    let clone = owner.clone();
    let other = loan(&clone, fragment, unit_node, source, &Control::default()).unwrap();
    assert!(Arc::ptr_eq(
        owner.function_catalog(),
        clone.function_catalog()
    ));
    assert!(std::ptr::eq(receipt.captured(), other.captured()));
    assert!(std::ptr::eq(
        receipt.captured().binding().resolved(),
        other.captured().binding().resolved()
    ));
}

#[test]
fn conversion_journal_intermediate_call_is_separate_from_following_cast() {
    let owner = finish(cast_to_bounded_varchar(
        selected_largeint(),
        DecimalOverflowPolicy::OutputNull,
    ));
    let (fragment, unit_node, source) = conversion(&owner);
    let receipt = loan(&owner, fragment, unit_node, source, &Control::default()).unwrap();
    assert_eq!(source.ty, ValueType::new(DataType::Utf8, false));
    let wrapper = unit_node
        .expressions
        .as_ref()
        .unwrap()
        .definitions()
        .iter()
        .find(|node| match &node.kind {
            ContractExprKind::Cast {
                expr,
                target,
                cast_mode,
                ..
            } if *expr == source.id => {
                assert_eq!(target, &DataType::Utf8);
                assert_eq!(*cast_mode, TrinoCastMode::Explicit);
                true
            }
            _ => false,
        })
        .expect("actual second-stage same-domain CAST");
    assert_ne!(wrapper.id, source.id);
    assert_eq!(wrapper.ty, bounded_varchar());
    assert!(std::ptr::eq(receipt.source(), source));
    assert!(matches!(
        loan(&owner, fragment, unit_node, wrapper, &Control::default()),
        Err(SqlSourceJournalError::MissingEntry)
    ));
}

#[test]
fn conversion_journal_identity_and_null_fastpaths_have_no_phantom_source() {
    let target = largeint(false);
    let owner = finish(cast(
        selected_largeint(),
        target.clone(),
        DecimalOverflowPolicy::ReportError,
    ));
    let mut actual_targets = 0;
    for fragment in owner.plan().fragments().values() {
        for unit_node in fragment.nodes().values() {
            let Some(unit) = &unit_node.expressions else {
                continue;
            };
            assert!(!unit.definitions().iter().any(|source| matches!(&source.kind,
                ContractExprKind::FunctionCall { function, .. } if function.function_id.as_str() == CONVERSION)));
            for source in unit.definitions().iter().filter(|node| node.ty == target) {
                assert!(matches!(
                    loan(&owner, fragment, unit_node, source, &Control::default()),
                    Err(SqlSourceJournalError::MissingEntry)
                ));
                actual_targets += 1;
            }
        }
    }
    assert!(actual_targets > 0, "actual identity source remains present");

    // The NULL fastpath is an original visitor emission fixture only, not
    // publication proof. It types the NULL where it is lowered, so no
    // original in the source type is left behind for nothing to read.
    let expression = cast(
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Null),
            value_type: largeint(true),
        },
        ValueType::new(DataType::Utf8, true),
        DecimalOverflowPolicy::ReportError,
    );
    let control = Control::default();
    let mut visitor = ContractLoweringVisitor::new(
        version(),
        dop(),
        None,
        crate::functions::builtin_sql_function_catalog().snapshot(),
        novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
        false,
        policy(),
        &control,
    )
    .unwrap();
    let id = visitor
        .lower_expression(NodeId::new(73), &expression, &BTreeMap::new())
        .unwrap();
    assert!(visitor.call_sources.expression_entries.is_empty());
    let fragment = &visitor.fragments[&ROOT_FRAGMENT_ID];
    let unit = fragment.construction_unit(NodeId::new(73)).unwrap();
    assert!(!unit.definitions().any(|source| matches!(&source.kind,
        ContractExprKind::FunctionCall { function, .. } if function.function_id.as_str() == CONVERSION)));
    let source = unit.definition(id).unwrap();
    assert_eq!(source.ty, ValueType::new(DataType::Utf8, true));
    assert!(matches!(source.kind, ContractExprKind::Constant(_)));
    assert_eq!(unit.len(), 1);
    visitor.work.finish().unwrap();
}

#[test]
fn conversion_journal_lambda_scope_matches_the_actual_intermediate_emission() {
    // Direct original visitor fixture, deliberately not a complete fragment
    // or installed higher-order invocation capability claim.
    let parameter = TypedExpr {
        kind: ExprKind::LambdaParamRef {
            name: "largeint_arg".into(),
            slot_id: 11,
        },
        value_type: largeint(false),
    };
    let expression = TypedExpr {
        kind: ExprKind::LambdaFunction {
            params: vec![crate::analysis::LambdaParam {
                name: "largeint_arg".into(),
                slot_id: 11,
                value_type: largeint(false),
            }],
            body: novarocks_parser::ast::AstBox::new(cast(
                parameter,
                ValueType::new(DataType::Utf8, false),
                DecimalOverflowPolicy::ReportError,
            )),
        },
        value_type: ValueType::new(DataType::Null, true),
    };
    let control = Control::default();
    let mut visitor = ContractLoweringVisitor::new(
        version(),
        dop(),
        None,
        crate::functions::builtin_sql_function_catalog().snapshot(),
        novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
        false,
        policy(),
        &control,
    )
    .unwrap();
    let lambda = visitor
        .lower_expression(NodeId::new(73), &expression, &BTreeMap::new())
        .unwrap();
    let (key, entry) = visitor
        .call_sources
        .expression_entries
        .iter()
        .next()
        .unwrap();
    let crate::planner::distributed::build::unit_source::ScalarSourceSite::Node {
        fragment: _,
        node: key_node,
    } = key.site.clone()
    else {
        panic!("the original fixture owns a real node source")
    };
    assert_eq!(visitor.call_sources.expression_entries.len(), 1);
    assert_eq!(entry.lambda_scope, Some(lambda));
    let source=visitor.fragments[&ROOT_FRAGMENT_ID].construction_unit(key_node).unwrap().definitions().find(|node|
        matches!(&node.kind,ContractExprKind::FunctionCall { function,.. } if function.function_id.as_str()==CONVERSION)).unwrap();
    assert_eq!(key_node, NodeId::new(73));
    assert_eq!(source.lambda_scope, Some(lambda));
    let FunctionArgument::Value { constant, .. } = &entry.captured.request().arguments[0] else {
        unreachable!()
    };
    assert!(constant.is_none());
    validate_expression_source_entry_observed(
        entry,
        crate::planner::distributed::build::unit_source::ScalarUnitOwner::Node(key_node),
        source,
        &mut visitor.work,
    )
    .unwrap();
    visitor.work.finish().unwrap();
}

#[test]
fn conversion_journal_foreign_missing_and_each_loan_callback_preserve_original_control() {
    let owner = finish(cast(
        selected_largeint(),
        ValueType::new(DataType::Utf8, false),
        DecimalOverflowPolicy::OutputNull,
    ));
    let (fragment, unit_node, source) = conversion(&owner);
    let foreign_fragment = fragment.clone();
    let foreign_source = source.clone();
    let missing = unit_node
        .expressions
        .as_ref()
        .unwrap()
        .definition(match &source.kind {
            ContractExprKind::FunctionCall { args, .. } => args[0],
            _ => unreachable!(),
        })
        .unwrap();
    for (fragment, source) in [
        (fragment, source),
        (&foreign_fragment, source),
        (fragment, &foreign_source),
        (fragment, missing),
    ] {
        let control = Control::default();
        let result = loan(&owner, fragment, unit_node, source, &control);
        if std::ptr::eq(source, missing) {
            assert!(matches!(result, Err(SqlSourceJournalError::MissingEntry)));
        } else if std::ptr::eq(fragment, &foreign_fragment) || std::ptr::eq(source, &foreign_source)
        {
            assert!(matches!(
                result,
                Err(SqlSourceJournalError::InvalidSource(_))
            ));
        } else {
            assert!(result.is_ok());
        }
        let trace = control.trace();
        assert_eq!(trace[0], (CompilePhase::Validate, 0));
        for stop in 0..trace.len() {
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
                    matches!(loan(&owner, fragment, unit_node, source,&control),Err(SqlSourceJournalError::Control(actual)) if actual==cause)
                );
                assert_eq!(control.trace(), trace[..=stop]);
            }
        }
    }
}

#[test]
fn conversion_emission_capture_and_record_stop_at_every_actual_control_refusal() {
    let source = project(cast(
        selected_largeint(),
        ValueType::new(DataType::Utf8, false),
        DecimalOverflowPolicy::ReportError,
    ));
    let functions = crate::functions::builtin_sql_function_catalog().snapshot();
    let control = Control::default();
    assert!(
        lower_final_physical_plan(
            &source,
            version(),
            dop(),
            functions.clone(),
            novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
            false,
            policy(),
            &control
        )
        .is_ok()
    );
    let trace = control.trace();
    for stop in 0..trace.len() {
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
                matches!(lower_final_physical_plan(&source,version(),dop(),functions.clone(),novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(), false,policy(),&control),
            Err(ContractLoweringError::Control(actual)) if actual==cause)
            );
            assert_eq!(control.trace(), trace[..=stop]);
        }
    }
}
