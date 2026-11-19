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

use super::*;
use crate::{
    analysis::{ExprKind as TypedKind, LiteralValue as TypedLiteral, TypedExpr},
    compiler::SqlAuthoredPhysicalPlan,
    planner::distributed::build::{
        SqlSourceJournalError,
        contract_lowering::lowered_scalar_source_tests::{
            authored, lower_call as lower,
            operational_tests::selected_source_owner as selected_owner, policy, text,
        },
    },
};
use arrow::{
    array::{Array, StringArray},
    datatypes::DataType,
};
use novarocks_constant_contract::ConstantPool;
use novarocks_physical_plan::{ConstantReference, Fragment};
use novarocks_type_contract::{DecimalOverflowPolicy, PureCompileControl, ValueLogicalType};
use std::sync::Mutex;

const PHASE: CompilePhase = CompilePhase::FunctionSpecialization;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
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
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
fn source<'a>(
    owner: &'a SqlAuthoredPhysicalPlan,
    id: &str,
) -> (
    &'a Fragment,
    &'a novarocks_physical_plan::PhysicalNode,
    &'a ExprNode,
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
                    .find_map(|expression| {
                        matches!(&expression.kind, ExprKind::FunctionCall { function, .. }
                    if function.function_id.as_str().contains(id))
                        .then_some((fragment, node, expression))
                    })
            })
        })
        .expect("actual emitted node-local call")
}
#[derive(Debug)]
enum Error {
    Control(CompileControlError),
    Journal(SqlSourceJournalError),
    Request(PhysicalScalarRequestError),
}
impl From<CompileControlError> for Error {
    fn from(cause: CompileControlError) -> Self {
        Self::Control(cause)
    }
}
impl From<SqlSourceJournalError> for Error {
    fn from(error: SqlSourceJournalError) -> Self {
        match error {
            SqlSourceJournalError::Control(cause) => Self::Control(cause),
            error => Self::Journal(error),
        }
    }
}
impl From<PhysicalScalarRequestError> for Error {
    fn from(error: PhysicalScalarRequestError) -> Self {
        match error {
            PhysicalScalarRequestError::Control(cause) => Self::Control(cause),
            error => Self::Request(error),
        }
    }
}
fn probe<'a>(
    owner: &'a SqlAuthoredPhysicalPlan,
    fragment: &'a Fragment,
    node: &'a novarocks_physical_plan::PhysicalNode,
    source: &'a ExprNode,
    control: &dyn PureCompileControl,
) -> Result<AuthoredPhysicalScalarRequest<'a>, Error> {
    let mut work = CompileCheckpoints::try_new(control, PHASE)?;
    let result = (|| {
        let entry =
            owner.checked_expression_call_source_observed(fragment, node, source, &mut work)?;
        author_physical_scalar_request_from_journal_observed(&entry, &mut work).map_err(Error::from)
    })();
    if matches!(&result, Err(Error::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn cv(argument: &FunctionArgument) -> Option<&novarocks_functions::ConstantValue> {
    let FunctionArgument::Value { constant, .. } = argument else {
        panic!("actual value argument")
    };
    constant.as_ref()
}
fn emitted_cv(
    owner: &SqlAuthoredPhysicalPlan,
    reference: ConstantReference,
) -> novarocks_functions::ConstantValue {
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, PHASE).unwrap();
    let value = owner
        .plan()
        .constants()
        .resolve_source_observed(reference, &mut work)
        .unwrap();
    work.finish().unwrap();
    value
}

#[test]
fn scalar_journal_borrows_original_request_slice_selection_and_nonzero_cv_backing() {
    let (owner, pool, field) = selected_owner();
    let (fragment, node, expression) = source(&owner, "lower");
    let request = probe(&owner, fragment, node, expression, &Control::default()).unwrap();
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, PHASE).unwrap();
    let entry = owner
        .checked_expression_call_source_observed(fragment, node, expression, &mut work)
        .unwrap();
    let canonical = entry.canonical_operational().unwrap();
    assert!(std::ptr::eq(
        request.request().arguments,
        canonical.request().arguments
    ));
    assert!(Arc::ptr_eq(request.selected(), canonical.selected()));
    assert_eq!(request.request().logical_argument_count, 1);
    assert!(request.request().expected_result_type.is_none());
    assert!(std::ptr::eq(
        request.function(),
        match &expression.kind {
            ExprKind::FunctionCall { function, .. } => function,
            _ => unreachable!(),
        }
    ));
    let selected = cv(&request.request().arguments[0]).unwrap();
    assert_eq!(selected.ordinal(), 1);
    assert_eq!(selected.try_utf8().unwrap(), Some("MiXeD-中国"));
    assert!(Arc::ptr_eq(selected.pool().array(), pool.array()));
    assert!(Arc::ptr_eq(selected.pool().field_ref(), &field));
    assert_eq!(request.captured_constant_policy(), Some(policy()));
    assert_eq!(
        request.captured_decimal_overflow_policy(),
        Some(DecimalOverflowPolicy::ReportError)
    );
    work.finish().unwrap();
    let clone = owner.clone();
    let again = probe(&clone, fragment, node, expression, &Control::default()).unwrap();
    assert!(std::ptr::eq(
        request.request().arguments,
        again.request().arguments
    ));
    assert!(Arc::ptr_eq(request.selected(), again.selected()));
}

#[test]
fn scalar_journal_keeps_original_none_for_cast_and_nested_physical_constants() {
    for argument in [
        TypedExpr {
            kind: TypedKind::Cast {
                array_failure_mode: None,
                expr: novarocks_parser::ast::AstBox::new(text("MiXeD")),
                target: DataType::Utf8,
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
            },
            value_type: FunctionValueType::new(DataType::Utf8, false),
        },
        TypedExpr {
            kind: TypedKind::Nested(novarocks_parser::ast::AstBox::new(text("MiXeD"))),
            value_type: FunctionValueType::new(DataType::Utf8, false),
        },
    ] {
        let owner = authored(lower(argument, DecimalOverflowPolicy::OutputNull));
        let (fragment, node, expression) = source(&owner, "lower");
        let request = probe(&owner, fragment, node, expression, &Control::default()).unwrap();
        let ExprKind::FunctionCall { args, .. } = &expression.kind else {
            unreachable!()
        };
        assert!(matches!(
            &node
                .expressions
                .as_ref()
                .unwrap()
                .definition(args[0])
                .unwrap()
                .kind,
            ExprKind::Constant(_)
        ));
        assert!(cv(&request.request().arguments[0]).is_none());
        assert!(request.request().expected_result_type.is_none());
        assert_eq!(
            request.captured_decimal_overflow_policy(),
            Some(DecimalOverflowPolicy::OutputNull)
        );
        let direct_control = Control::default();
        let mut work = CompileCheckpoints::try_new(&direct_control, PHASE).unwrap();
        let direct = author_physical_scalar_request_observed(
            expression,
            super::super::unit_source::UnitDefinitions::from_node(node),
            owner.plan().constants(),
            policy(),
            &mut work,
        )
        .unwrap();
        assert!(cv(&direct.request().arguments[0]).is_some());
        assert_eq!(direct.request().expected_result_type, Some(&expression.ty));
        assert_eq!(direct.captured_constant_policy(), None);
        assert_eq!(direct.captured_decimal_overflow_policy(), None);
        work.finish().unwrap();
    }
}

#[test]
fn scalar_journal_conversion_retains_original_none_and_true_intermediate_constraint() {
    let json =
        FunctionValueType::try_with_logical_type(DataType::Utf8, false, ValueLogicalType::Json)
            .unwrap();
    let field = Arc::new(json.try_to_field("json_original").unwrap());
    let pool = ConstantPool::try_new(
        field.clone(),
        json.clone(),
        StringArray::from(vec!["unused", "{\"kept\":7}"]).to_data(),
        policy(),
        CompilePhase::Validate,
        &Control::default(),
    )
    .unwrap();
    let input = TypedExpr {
        kind: TypedKind::Constant(pool.value(1).unwrap()),
        value_type: json.clone(),
    };
    let owner = authored(TypedExpr {
        kind: TypedKind::Cast {
            array_failure_mode: None,
            expr: novarocks_parser::ast::AstBox::new(input),
            target: DataType::LargeUtf8,
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
        },
        value_type: FunctionValueType::new(DataType::LargeUtf8, false),
    });
    let (fragment, node, expression) = source(&owner, "value_domain_conversion");
    let request = probe(&owner, fragment, node, expression, &Control::default()).unwrap();
    let FunctionArgument::Value {
        value_type,
        constant,
    } = &request.request().arguments[0]
    else {
        unreachable!()
    };
    assert_eq!(value_type, &json);
    assert!(constant.is_none());
    assert_eq!(
        request.request().expected_result_type,
        Some(&FunctionValueType::new(DataType::Utf8, false))
    );
    assert_eq!(
        request.selected().result_type,
        FunctionResultType::Scalar(FunctionValueType::new(DataType::Utf8, false))
    );
    let ExprKind::FunctionCall { args, .. } = &expression.kind else {
        unreachable!()
    };
    let ExprKind::Constant(reference) = node
        .expressions
        .as_ref()
        .unwrap()
        .definition(args[0])
        .unwrap()
        .kind
    else {
        panic!("conversion physical source CV")
    };
    let emitted = emitted_cv(&owner, reference);
    assert_eq!(emitted.ordinal(), 1);
    assert!(Arc::ptr_eq(emitted.pool().array(), pool.array()));
    assert!(Arc::ptr_eq(emitted.pool().field_ref(), &field));
}

#[test]
fn scalar_journal_canonical_null_loans_actual_typed_cv_without_retagging_original() {
    let owner = authored(lower(
        TypedExpr {
            kind: TypedKind::Literal(TypedLiteral::Null),
            value_type: FunctionValueType::new(DataType::Null, true),
        },
        DecimalOverflowPolicy::OutputNull,
    ));
    let (fragment, node, expression) = source(&owner, "lower");
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, PHASE).unwrap();
    let entry = owner
        .checked_expression_call_source_observed(fragment, node, expression, &mut work)
        .unwrap();
    let original = cv(&entry.captured().request().arguments[0]).unwrap();
    let request = author_physical_scalar_request_from_journal_observed(&entry, &mut work).unwrap();
    let actual = cv(&request.request().arguments[0]).unwrap();
    assert_eq!(
        original.value_type(),
        &FunctionValueType::new(DataType::Null, true)
    );
    assert_eq!(
        actual.value_type(),
        &FunctionValueType::new(DataType::Utf8, true)
    );
    assert_eq!(actual.try_utf8().unwrap(), None);
    assert_ne!(
        original.pool().backing_identity(),
        actual.pool().backing_identity()
    );
    let ExprKind::FunctionCall { args, .. } = &expression.kind else {
        unreachable!()
    };
    let ExprKind::Constant(reference) = node
        .expressions
        .as_ref()
        .unwrap()
        .definition(args[0])
        .unwrap()
        .kind
    else {
        unreachable!()
    };
    let emitted = emitted_cv(&owner, reference);
    assert!(Arc::ptr_eq(actual.pool().array(), emitted.pool().array()));
    assert!(Arc::ptr_eq(
        actual.pool().field_ref(),
        emitted.pool().field_ref()
    ));
    work.finish().unwrap();
}

#[test]
fn scalar_journal_foreign_source_and_every_actual_callback_preserve_original_causes() {
    let (owner, _, _) = selected_owner();
    let (fragment, node, expression) = source(&owner, "lower");
    let foreign = expression.clone();
    for actual in [expression, &foreign] {
        let baseline = Control::default();
        let result = probe(&owner, fragment, node, actual, &baseline);
        if std::ptr::eq(actual, expression) {
            assert!(result.is_ok());
        } else {
            assert!(matches!(
                result,
                Err(Error::Journal(SqlSourceJournalError::InvalidSource(_)))
            ));
        }
        let trace = baseline.trace();
        assert!(!trace.is_empty());
        assert!(trace.iter().any(|(_, work)| *work > 0));
        for stop in 0..trace.len() {
            for cause in CAUSES {
                let control = Control {
                    refusal: Some((stop, cause.clone())),
                    ..Control::default()
                };
                assert!(
                    matches!(probe(&owner, fragment, node, actual, &control), Err(Error::Control(actual)) if actual == cause)
                );
                assert_eq!(control.trace(), trace[..=stop]);
            }
        }
    }
    let (other_owner, _, _) = selected_owner();
    assert!(matches!(
        probe(
            &other_owner,
            fragment,
            node,
            expression,
            &Control::default()
        ),
        Err(Error::Journal(SqlSourceJournalError::InvalidSource(_)))
    ));
}

#[test]
fn scalar_journal_real_sql_retains_actual_catalogue_and_none_after_cast() {
    let owner = crate::compiler::compile_authored_aggregate_for_test(
        "SELECT LOWER(CAST(order_key AS VARCHAR)) AS lower_key FROM orders",
    );
    let (fragment, node, expression) = source(&owner, "lower");
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, PHASE).unwrap();
    let entry = owner
        .checked_expression_call_source_observed(fragment, node, expression, &mut work)
        .unwrap();
    assert!(Arc::ptr_eq(
        entry.function_catalog(),
        owner.function_catalog()
    ));
    let request = author_physical_scalar_request_from_journal_observed(&entry, &mut work).unwrap();
    assert!(cv(&request.request().arguments[0]).is_none());
    assert!(std::ptr::eq(
        request.request().arguments,
        entry.canonical_operational().unwrap().request().arguments
    ));
    assert!(Arc::ptr_eq(
        request.selected(),
        entry.canonical_operational().unwrap().selected()
    ));
    assert_eq!(request.request().logical_argument_count, 1);
    assert!(request.request().expected_result_type.is_none());
    work.finish().unwrap();
}
