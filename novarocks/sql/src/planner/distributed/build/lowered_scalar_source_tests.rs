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
use super::*;
use crate::binding::SqlFunctionBinding;
use crate::compiler::SqlAuthoredPhysicalPlan;
use crate::planner::distributed::build::lowered_draft::{
    CheckedExpressionLogicalSourceEntry, SqlOperationalProjectionError, SqlSourceJournalError,
};
use crate::planner::payload::PlanProjectNode;
use arrow::array::{Array, StringArray};
use novarocks_constant_contract::ConstantPool;
use novarocks_functions::{FunctionArgument, FunctionResultType};
use novarocks_type_contract::DecimalOverflowPolicy;
use std::sync::Mutex;

#[path = "lowered_canonical_scalar_tests.rs"]
pub(in crate::planner::distributed::build) mod lowered_canonical_scalar_tests;

#[path = "lowered_canonical_constraint_tests.rs"]
mod lowered_canonical_constraint_tests;

#[path = "lowered_operational_channel_tests.rs"]
pub(in crate::planner::distributed::build) mod operational_tests;

#[path = "lowered_operational_lambda_tests.rs"]
mod lowered_operational_lambda_tests;

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
            assert!(at <= stop, "callback after the original refusal");
        }
        trace.push((phase, units));
        match self.refusal.clone() {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}
pub(in crate::planner::distributed::build) fn policy() -> novarocks_functions::ConstantPolicy {
    crate::constant::test_constant_policy()
}
pub(in crate::planner::distributed::build) fn text(text: &str) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::Literal(LiteralValue::String(text.into())),
        value_type: ValueType::new(DataType::Utf8, false),
    }
}
// These typed fixtures exercise the actual emitter, not an optimizer or SQL
// producer certification. The selected LOWER contract is the actual catalog's.
pub(in crate::planner::distributed::build) fn lower_call(
    argument: TypedExpr,
    decimal: DecimalOverflowPolicy,
) -> TypedExpr {
    let arguments =
        [crate::analysis::function_argument(&argument, policy(), &Control::default()).unwrap()];
    let resolved = crate::functions::builtin_sql_function_catalog()
        .resolve_scalar_binding("lower", &arguments, &Control::default())
        .unwrap();
    let FunctionResultType::Scalar(result) = &resolved.selected.result_type else {
        panic!("actual lower scalar result")
    };
    let ty = result.clone();
    let volatility = resolved.semantics.volatility;
    TypedExpr {
        kind: ExprKind::FunctionCall {
            name: "lower".into(),
            args: vec![argument],
            distinct: false,
            binding: SqlFunctionBinding::new(resolved, decimal),
            volatility,
        },
        value_type: ty,
    }
}
fn emission_plan(expression: TypedExpr) -> PhysicalPlanNode {
    let mut output = column(
        71,
        "source_result",
        expression.value_type.data_type().clone(),
        expression.value_type.nullable(),
    );
    output.value_type = expression.value_type.clone();
    PhysicalPlanNode {
        kind: PhysicalPlanKind::Project(PlanProjectNode {
            items: vec![crate::analysis::ProjectItem {
                expr: expression,
                output_name: "source_result".into(),
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
pub(in crate::planner::distributed::build) fn authored(
    expression: TypedExpr,
) -> SqlAuthoredPhysicalPlan {
    let control = Control::default();
    lower_draft_with_control(expression, &control)
        .unwrap()
        .finish_observed(&control)
        .unwrap()
}

pub(in crate::planner::distributed::build) fn lower_draft_with_control(
    expression: TypedExpr,
    control: &dyn PureCompileControl,
) -> Result<LoweredSqlPhysicalDraft, ContractLoweringError> {
    lower_final_physical_plan(
        &emission_plan(expression),
        version(),
        dop(),
        crate::functions::builtin_sql_function_catalog().snapshot(),
        false,
        policy(),
        control,
    )
}

pub(in crate::planner::distributed::build) fn analyzed_lambda_expression_for_test() -> TypedExpr {
    // The real analyzer supplies the selected Lambda and LOWER declarations.
    // This emitter fixture does not assert an installed ARRAY_MAP pure kernel
    // or bypass the public compiler's implementation-presence admission.
    let statements =
        novarocks_parser::parse("SELECT ARRAY_MAP(x -> LOWER(x), ['AA','BB']) AS mapped").unwrap();
    let [novarocks_parser::ast::Statement::Query(query)] = statements.as_slice() else {
        panic!("one Lambda query")
    };
    let (query, _, _) = crate::analyzer::analyze(
        query,
        &crate::catalog::memory::PlannerMemoryCatalog::default(),
        "default",
    )
    .unwrap();
    let crate::analysis::QueryBody::Select(select) = query.body else {
        panic!("Lambda projection")
    };
    select.projection.into_iter().next().unwrap().expr
}

pub(in crate::planner::distributed::build) fn authored_lambda_source_for_test()
-> SqlAuthoredPhysicalPlan {
    authored(analyzed_lambda_expression_for_test())
}

pub(in crate::planner::distributed::build) fn scalar_source(
    owner: &SqlAuthoredPhysicalPlan,
) -> (
    &Fragment,
    &novarocks_physical_plan::PhysicalNode,
    &novarocks_expression_contract::ExprNode,
) {
    for fragment in owner.plan().fragments().values() {
        for node in fragment.nodes().values() {
            let Some(unit) = &node.expressions else {
                continue;
            };
            for source in unit.definitions() {
                if let ContractExprKind::FunctionCall { function, .. } = &source.kind
                    && function.function_id.as_str().contains("lower")
                {
                    return (fragment, node, source);
                }
            }
        }
    }
    panic!("actual nonfolded LOWER emission")
}
fn loan<'a>(
    owner: &'a SqlAuthoredPhysicalPlan,
    fragment: &'a Fragment,
    unit_node: &'a novarocks_physical_plan::PhysicalNode,
    source: &'a novarocks_expression_contract::ExprNode,
    control: &dyn PureCompileControl,
) -> Result<CheckedExpressionLogicalSourceEntry<'a>, SqlSourceJournalError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
    let result = owner.checked_scalar_source_observed(fragment, unit_node, source, &mut work);
    if matches!(&result, Err(SqlSourceJournalError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn constant(argument: &FunctionArgument) -> Option<&novarocks_functions::ConstantValue> {
    let FunctionArgument::Value { constant, .. } = argument else {
        panic!("value argument")
    };
    constant.as_ref()
}

fn operational_arguments(
    owner: &SqlAuthoredPhysicalPlan,
    receipt: &CheckedExpressionLogicalSourceEntry<'_>,
    control: &dyn PureCompileControl,
) -> Result<Box<[FunctionArgument]>, SqlOperationalProjectionError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
    let result = receipt.operational_arguments_observed(owner.plan().constants(), &mut work);
    if matches!(&result, Err(SqlOperationalProjectionError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn physical_constant(
    owner: &SqlAuthoredPhysicalPlan,
    receipt: &CheckedExpressionLogicalSourceEntry<'_>,
    id: ExprId,
) -> novarocks_functions::ConstantValue {
    let node = receipt.definitions().definition(id).unwrap();
    let ContractExprKind::Constant(reference) = &node.kind else {
        panic!("constant emission")
    };
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let result = owner
        .plan()
        .constants()
        .resolve_source_observed(*reference, &mut work)
        .unwrap();
    work.finish().unwrap();
    result
}

#[test]
fn scalar_journal_real_sql_retains_same_owner_binding_and_request_loans() {
    // This goes through the real analyzer, optimizer, provider/statistics
    // completion and final draft. The table column prevents constant folding.
    let owner = crate::compiler::compile_authored_aggregate_for_test(
        "SELECT LOWER(CAST(order_key AS VARCHAR)) AS lower_key FROM orders",
    );
    let clone = owner.clone();
    assert!(Arc::ptr_eq(owner.plan_arc(), clone.plan_arc()));
    assert!(Arc::ptr_eq(
        owner.function_catalog(),
        clone.function_catalog()
    ));
    let (fragment, unit_node, source) = scalar_source(&owner);
    let original = loan(&owner, fragment, unit_node, source, &Control::default()).unwrap();
    let cloned = loan(&clone, fragment, unit_node, source, &Control::default()).unwrap();
    assert!(std::ptr::eq(original.captured(), cloned.captured()));
    assert!(std::ptr::eq(
        original.captured().binding().resolved(),
        cloned.captured().binding().resolved()
    ));
    assert_eq!(
        original.definitions().owner(),
        crate::planner::distributed::build::unit_source::ScalarUnitOwner::Node(unit_node.id)
    );
    assert!(std::ptr::eq(
        fragment.nodes().get(&unit_node.id).unwrap(),
        unit_node
    ));
    assert!(std::ptr::eq(original.source(), source));
    assert!(constant(&original.captured().request().arguments[0]).is_none());
    let ContractExprKind::FunctionCall { args, .. } = &source.kind else {
        unreachable!()
    };
    assert_eq!(original.arguments(), args.as_ref());
    assert_eq!(original.captured().constant_policy(), policy());
    let foreign_fragment = fragment.clone();
    let foreign_source = source.clone();
    let foreign_node = unit_node.clone();
    for (fragment, node, source, reason) in [
        (
            &foreign_fragment,
            unit_node,
            source,
            "expression journal loans a foreign plan or node",
        ),
        (
            fragment,
            &foreign_node,
            source,
            "expression journal loans a foreign plan or node",
        ),
        (
            fragment,
            unit_node,
            &foreign_source,
            "expression journal loans a foreign unit definition",
        ),
    ] {
        assert!(matches!(
            loan(&owner, fragment, node, source, &Control::default()),
            Err(SqlSourceJournalError::InvalidSource(actual)) if actual == reason
        ));
    }
}

#[test]
fn scalar_journal_literal_and_selected_cv_emit_the_same_original_backing_once() {
    let control = Control::default();
    let ty = ValueType::new(DataType::Utf8, true);
    let field = Arc::new(ty.try_to_field("original_selected_utf8").unwrap());
    let pool = ConstantPool::try_new(
        field.clone(),
        ty.clone(),
        StringArray::from(vec![Some("hidden"), Some("İΣé"), None]).to_data(),
        policy(),
        CompilePhase::Validate,
        &control,
    )
    .unwrap();
    let cv = pool.value(1).unwrap();
    for input in [
        text("MiXeD-é"),
        TypedExpr {
            kind: ExprKind::Constant(cv.clone()),
            value_type: ty.clone(),
        },
    ] {
        let owner = authored(lower_call(input, DecimalOverflowPolicy::ReportError));
        let (fragment, unit_node, source) = scalar_source(&owner);
        let receipt = loan(&owner, fragment, unit_node, source, &Control::default()).unwrap();
        assert_eq!(
            receipt.captured().binding().decimal_overflow_policy(),
            DecimalOverflowPolicy::ReportError
        );
        assert_eq!(receipt.captured().constant_policy(), policy());
        let request = receipt.captured().request();
        let captured = constant(&request.arguments[0]).unwrap();
        let operational = operational_arguments(&owner, &receipt, &Control::default()).unwrap();
        let projected = constant(&operational[0]).unwrap();
        assert_eq!(projected.value_type(), captured.value_type());
        assert_eq!(projected.ordinal(), captured.ordinal());
        assert_eq!(
            projected.pool().backing_identity(),
            captured.pool().backing_identity()
        );
        assert!(Arc::ptr_eq(
            projected.pool().field_ref(),
            captured.pool().field_ref()
        ));
        let emitted = physical_constant(&owner, &receipt, receipt.arguments()[0]);
        assert_eq!(emitted.ordinal(), captured.ordinal());
        assert!(Arc::ptr_eq(emitted.pool().array(), captured.pool().array()));
        assert!(Arc::ptr_eq(
            emitted.pool().field_ref(),
            captured.pool().field_ref()
        ));
        if captured.ordinal() == 1 {
            assert!(Arc::ptr_eq(captured.pool().array(), pool.array()));
            assert!(Arc::ptr_eq(captured.pool().field_ref(), &field));
            assert_eq!(emitted.try_utf8().unwrap(), Some("İΣé"));
        } else {
            assert_eq!(emitted.try_utf8().unwrap(), Some("MiXeD-é"));
        }
    }
}

#[test]
fn scalar_journal_original_cast_and_nested_none_survive_constant_emission() {
    for argument in [
        TypedExpr {
            kind: ExprKind::Cast {
                expr: Box::new(text("ABC")),
                target: DataType::Utf8,
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
            },
            value_type: ValueType::new(DataType::Utf8, false),
        },
        TypedExpr {
            kind: ExprKind::Nested(Box::new(text("ABC"))),
            value_type: ValueType::new(DataType::Utf8, false),
        },
    ] {
        let owner = authored(lower_call(argument, DecimalOverflowPolicy::OutputNull));
        let (fragment, unit_node, source) = scalar_source(&owner);
        let receipt = loan(&owner, fragment, unit_node, source, &Control::default()).unwrap();
        assert!(constant(&receipt.captured().request().arguments[0]).is_none());
        let operational = operational_arguments(&owner, &receipt, &Control::default()).unwrap();
        assert!(constant(&operational[0]).is_none());
        assert_eq!(
            physical_constant(&owner, &receipt, receipt.arguments()[0])
                .try_utf8()
                .unwrap(),
            Some("ABC")
        );
    }
}

#[test]
fn scalar_journal_lambda_body_call_keeps_its_actual_emission_scope() {
    // Direct original visitor fixture: this tests emitter scope association,
    // not an installed HOF/optimizer or a complete fragment publication.
    let parameter_type = ValueType::new(DataType::Utf8, true);
    let parameter = TypedExpr {
        kind: ExprKind::LambdaParamRef {
            name: "x".into(),
            slot_id: 9,
        },
        value_type: parameter_type.clone(),
    };
    let expression = TypedExpr {
        kind: ExprKind::LambdaFunction {
            params: vec![crate::analysis::LambdaParam {
                name: "x".into(),
                slot_id: 9,
                value_type: parameter_type,
            }],
            body: Box::new(lower_call(parameter, DecimalOverflowPolicy::ReportError)),
        },
        value_type: ValueType::new(DataType::Null, true),
    };
    let control = Control::default();
    let mut visitor = ContractLoweringVisitor::new(
        version(),
        dop(),
        None,
        crate::functions::builtin_sql_function_catalog().snapshot(),
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
        fragment: key_fragment,
        node: key_node,
    } = key.site.clone()
    else {
        panic!("the original fixture owns a real node source")
    };
    assert_eq!(visitor.call_sources.expression_entries.len(), 1);
    assert_eq!(key_fragment, ROOT_FRAGMENT_ID);
    assert_eq!(key_node, NodeId::new(73));
    assert_eq!(entry.lambda_scope, Some(lambda));
    assert!(constant(&entry.captured.request().arguments[0]).is_none());
    let source = visitor.fragments[&ROOT_FRAGMENT_ID]
        .construction_unit(key_node)
        .unwrap()
        .definition(key.definition)
        .unwrap();
    assert_eq!(source.lambda_scope, Some(lambda));
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
fn scalar_journal_loan_success_and_ordinary_errors_observe_every_original_control_prefix() {
    let owner = authored(lower_call(text("ABC"), DecimalOverflowPolicy::OutputNull));
    let (fragment, unit_node, source) = scalar_source(&owner);
    let foreign_source = source.clone();
    let argument_id = match &source.kind {
        ContractExprKind::FunctionCall { args, .. } => args[0],
        _ => unreachable!(),
    };
    let missing = unit_node
        .expressions
        .as_ref()
        .unwrap()
        .definition(argument_id)
        .unwrap();
    for source in [source, &foreign_source, missing] {
        let control = Control::default();
        let result = loan(&owner, fragment, unit_node, source, &control);
        if std::ptr::eq(source, missing) {
            assert!(matches!(result, Err(SqlSourceJournalError::MissingEntry)));
        } else if std::ptr::eq(source, &foreign_source) {
            assert!(matches!(
                result,
                Err(SqlSourceJournalError::InvalidSource(_))
            ));
        } else {
            assert!(result.is_ok());
        }
        let baseline = control.trace();
        assert!(
            baseline
                .iter()
                .all(|(phase, _)| *phase == CompilePhase::Validate)
        );
        assert_eq!(baseline[0].1, 0);
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
                    matches!(loan(&owner, fragment, unit_node, source, &control),
                    Err(SqlSourceJournalError::Control(actual)) if actual == cause)
                );
                assert_eq!(control.trace(), baseline[..=stop]);
            }
        }
    }
}

#[test]
fn scalar_journal_bare_null_and_cast_null_keep_distinct_original_request_sources() {
    // Already-typed emitter fixtures. These exercise the original bare-NULL
    // canonicalization and Cast folding; they do not certify parser/optimizer
    // producer reachability or strengthen the journal's request-data claim.
    let null = TypedExpr {
        kind: ExprKind::Literal(LiteralValue::Null),
        value_type: ValueType::new(DataType::Null, true),
    };
    let owner = authored(lower_call(null.clone(), DecimalOverflowPolicy::ReportError));
    let (fragment, unit_node, source) = scalar_source(&owner);
    let receipt = loan(&owner, fragment, unit_node, source, &Control::default()).unwrap();
    let captured_request = receipt.captured().request();
    let FunctionArgument::Value {
        value_type,
        constant: Some(captured),
    } = &captured_request.arguments[0]
    else {
        panic!("original bare NULL request")
    };
    assert_eq!(value_type, &ValueType::new(DataType::Null, true));
    assert_eq!(captured.value_type(), value_type);
    let selected_argument = &receipt
        .captured()
        .binding()
        .resolved()
        .selected
        .argument_types[0];
    assert_eq!(
        selected_argument,
        &FunctionArgumentType::Value(ValueType::new(DataType::Utf8, true))
    );
    let emitted = physical_constant(&owner, &receipt, receipt.arguments()[0]);
    assert_eq!(emitted.value_type(), &ValueType::new(DataType::Utf8, true));
    assert_eq!(emitted.try_utf8().unwrap(), None);
    let operational = operational_arguments(&owner, &receipt, &Control::default()).unwrap();
    let projected = constant(&operational[0]).unwrap();
    assert_eq!(projected.value_type(), emitted.value_type());
    assert_eq!(
        projected.pool().backing_identity(),
        emitted.pool().backing_identity()
    );
    assert_ne!(
        projected.pool().backing_identity(),
        captured.pool().backing_identity()
    );
    // Conversion authors a new typed NULL; the retained Null source is neither
    // retyped nor presented as the resulting UTF8 constant's backing.
    assert!(!Arc::ptr_eq(
        captured.pool().array(),
        emitted.pool().array()
    ));
    let cast = TypedExpr {
        kind: ExprKind::Cast {
            expr: Box::new(null),
            target: DataType::Utf8,
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
        },
        value_type: ValueType::new(DataType::Utf8, true),
    };
    // Cast(NULL) currently leaves an unreachable original NULL definition in
    // this typed fixture. Whole-plan validation refuses that existing shape;
    // inspect actual emission only, without claiming publication acceptance.
    let control = Control::default();
    let mut visitor = ContractLoweringVisitor::new(
        version(),
        dop(),
        None,
        crate::functions::builtin_sql_function_catalog().snapshot(),
        false,
        policy(),
        &control,
    )
    .unwrap();
    let emitted_call = visitor
        .lower_expression(
            NodeId::new(74),
            &lower_call(cast, DecimalOverflowPolicy::ReportError),
            &BTreeMap::new(),
        )
        .unwrap();
    let entry = &visitor.call_sources.expression_entries
        [&crate::planner::distributed::build::unit_source::ScalarDefinitionAddress::new(
            crate::planner::distributed::build::unit_source::ScalarSourceSite::Node {
                fragment: ROOT_FRAGMENT_ID,
                node: NodeId::new(74),
            },
            emitted_call,
        )];
    assert!(constant(&entry.captured.request().arguments[0]).is_none());
    let emitted = visitor.fragments[&ROOT_FRAGMENT_ID]
        .construction_unit(NodeId::new(74))
        .unwrap()
        .definition(entry.arguments[0])
        .unwrap();
    assert_eq!(emitted.ty, ValueType::new(DataType::Utf8, true));
    assert!(matches!(emitted.kind, ContractExprKind::Constant(_)));
}

#[test]
fn scalar_journal_capture_and_actual_recording_preserve_every_lowering_control_prefix() {
    let success = lower_call(text("SOURCE"), DecimalOverflowPolicy::ReportError);
    let mut ordinary = success.clone();
    let ExprKind::FunctionCall { args, .. } = &mut ordinary.kind else {
        unreachable!()
    };
    args.push(text("EXTRA"));
    let functions = crate::functions::builtin_sql_function_catalog().snapshot();
    for (expression, expected_success) in [(success, true), (ordinary, false)] {
        let source = emission_plan(expression);
        let invoke = |control: &Control| {
            lower_final_physical_plan(
                &source,
                version(),
                dop(),
                functions.clone(),
                false,
                policy(),
                control,
            )
        };
        let control = Control::default();
        assert_eq!(invoke(&control).is_ok(), expected_success);
        let baseline = control.trace();
        assert!(baseline.len() >= 2);
        if expected_success {
            assert!(
                baseline
                    .iter()
                    .any(|(phase, _)| *phase == CompilePhase::FunctionSpecialization)
            );
        }
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
                    matches!(invoke(&control), Err(ContractLoweringError::Control(actual)) if actual == cause)
                );
                assert_eq!(control.trace(), baseline[..=stop]);
            }
        }
    }
}
