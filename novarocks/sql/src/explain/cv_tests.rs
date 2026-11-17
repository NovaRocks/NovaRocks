// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use crate::analysis::{BinOp, ExprKind, LiteralValue, ProjectItem, SortItem, TypedExpr};
use crate::compiler::SqlCompileError;
use crate::planner::logical::LogicalPlanKind;
use arrow::{
    array::{Array, Int64Array, StringArray},
    datatypes::DataType,
};
use novarocks_constant_contract::{ConstantPolicy, ConstantPool, ConstantValue};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
};
use std::sync::{Arc, Mutex};

struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    stop: Option<(usize, CompileControlError)>,
}
impl Control {
    fn new(stop: Option<(usize, CompileControlError)>) -> Self {
        Self {
            trace: Mutex::new(Vec::new()),
            stop,
        }
    }
    fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.trace.lock().unwrap().clone()
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut trace = self.trace.lock().unwrap();
        if let Some((at, _)) = self.stop {
            assert!(
                trace.len() < at,
                "no callback is allowed after primary refusal"
            );
        }
        trace.push((phase, units));
        match self.stop.clone() {
            Some((at, error)) if trace.len() == at => Err(error),
            _ => Ok(()),
        }
    }
}
fn policy() -> ConstantPolicy {
    // Explicit fixture ceiling; this is not a deployable profile or MEM grant.
    ConstantPolicy {
        max_rows: 4,
        max_array_nodes: 16,
        max_logical_elements: 1 << 20,
        max_retained_buffer_bytes: 8 << 20,
        max_type_depth: 64,
        max_type_nodes: 64,
        max_dictionary_depth: 8,
        max_metadata_bytes: 65536,
        max_library_validation_work: 64 << 20,
        max_library_validation_bytes: 64 << 20,
    }
}
fn selected(array: &dyn Array, ordinal: u32) -> ConstantValue {
    let ty = FunctionValueType::new(array.data_type().clone(), false);
    ConstantPool::try_new(
        Arc::new(ty.try_to_field("diagnostic_pool").unwrap()),
        ty,
        array.to_data(),
        policy(),
        CompilePhase::Validate,
        &Control::new(None),
    )
    .unwrap()
    .value(ordinal)
    .unwrap()
}
fn constant(value: ConstantValue) -> TypedExpr {
    TypedExpr {
        value_type: value.value_type().clone(),
        kind: ExprKind::Constant(value),
    }
}
fn literal(value: LiteralValue, carrier: DataType) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::Literal(value),
        value_type: FunctionValueType::new(carrier, false),
    }
}
fn format_sort_items(
    items: &[SortItem],
    control: &dyn PureCompileControl,
) -> Result<Vec<String>, SqlCompileError> {
    let diagnostic = super::completed::RenderDiagnostic::new(control);
    let mut output = super::completed::ExplainRenderOutput::new_observed(
        super::completed::ExplainRenderBudget::default(),
        control,
    )?;
    for item in items {
        let result = output.push(format_args!(
            "{}",
            super::logical::SortItems(std::slice::from_ref(item), Some(&diagnostic))
        ));
        if let Some(error) = diagnostic.take_error() {
            return Err(error);
        }
        result?;
    }
    output.finish_observed()
}

fn assert_original_refusals<T>(format: impl Fn(&Control) -> Result<T, SqlCompileError>) {
    let baseline = Control::new(None);
    assert!(format(&baseline).is_ok());
    let trace = baseline.trace();
    assert_eq!(trace.first(), Some(&(CompilePhase::LowerProgram, 0)));
    assert!(trace.iter().all(|(_, units)| *units <= 256));
    assert!(trace.iter().any(|(_, units)| *units > 0));
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in 1..=trace.len() {
            let refused = Control::new(Some((at, cause.clone())));
            assert!(
                matches!(format(&refused), Err(actual) if actual == SqlCompileError::from(cause.clone()))
            );
            assert_eq!(refused.trace(), trace[..at]);
        }
    }
}

#[test]
fn explain_selected_constants_preserve_scalar_text_and_column_first_equality() {
    let value = selected(&Int64Array::from(vec![999, 17, -999]), 1);
    assert_eq!(value.ordinal(), 1);
    let expr = constant(value);
    assert_eq!(format_expr(&expr, &Control::new(None)).unwrap(), "17");
    let equality = TypedExpr {
        kind: ExprKind::BinaryOp {
            left: novarocks_parser::ast::AstBox::new(expr),
            op: BinOp::Eq,
            right: novarocks_parser::ast::AstBox::new(TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: crate::column_id::ColumnId::new_for_test(4),
                    qualifier: Some("t".into()),
                    column: "k".into(),
                },
                value_type: FunctionValueType::new(DataType::Int64, false),
            }),
            decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        },
        value_type: FunctionValueType::new(DataType::Boolean, false),
    };
    assert_eq!(
        format_expr(&equality, &Control::new(None)).unwrap(),
        "t.k = 17"
    );
    for (value, expected) in [("hello", "'hello'"), ("", "''")] {
        let expr = constant(selected(&StringArray::from(vec!["unselected", value]), 1));
        assert_eq!(format_expr(&expr, &Control::new(None)).unwrap(), expected);
    }
}

#[test]
fn logical_project_and_shared_sort_use_the_selected_constant_author() {
    let expr = constant(selected(&Int64Array::from(vec![99, 17]), 1));
    let item = ProjectItem {
        expr: expr.clone(),
        output_name: "answer".into(),
        output_column_id: crate::column_id::ColumnId::new_for_test(8),
    };
    let plan = LogicalPlanNode::new(
        LogicalPlanKind::Project(crate::planner::payload::PlanProjectNode {
            items: vec![item],
            output_qualifier: None,
        }),
        vec![LogicalPlanNode::new(
            LogicalPlanKind::Values(crate::planner::payload::PlanValuesNode {
                rows: vec![],
                columns: vec![],
            }),
            vec![],
            None,
        )],
        None,
    );
    assert_eq!(
        explain_plan_checked(&plan, ExplainLevel::Normal, &Control::new(None)).unwrap(),
        ["PROJECT [17 AS answer]", "  VALUES (0 rows)"]
    );
    let items = [SortItem {
        expr,
        asc: false,
        nulls_first: true,
    }];
    assert_eq!(
        format_sort_items(&items, &Control::new(None)).unwrap(),
        ["17 DESC NULLS FIRST"]
    );
    assert_original_refusals(|control| explain_plan_checked(&plan, ExplainLevel::Normal, control));
}

#[test]
fn diagnostic_recursive_and_byte_work_preserve_entry_quantum_tail_refusals() {
    let binary = literal(LiteralValue::Binary(vec![0xab; 1025]), DataType::Binary);
    let baseline = Control::new(None);
    assert_eq!(
        format_expr(&binary, &baseline).unwrap(),
        format!("X'{}'", "AB".repeat(1025))
    );
    assert!(baseline.trace().iter().any(|(_, units)| *units == 256));
    assert_original_refusals(|control| format_expr(&binary, control));

    let case = TypedExpr {
        kind: ExprKind::Case {
            operand: None,
            when_then: (0..320)
                .map(|_| {
                    (
                        literal(LiteralValue::Bool(true), DataType::Boolean),
                        literal(LiteralValue::Int(7), DataType::Int64),
                    )
                })
                .collect(),
            else_expr: Some(novarocks_parser::ast::AstBox::new(constant(selected(
                &Int64Array::from(vec![99, 17]),
                1,
            )))),
        },
        value_type: FunctionValueType::new(DataType::Int64, false),
    };
    assert_original_refusals(|control| format_expr(&case, control));
}

#[test]
fn selected_constant_diagnostic_delegates_the_original_control_without_replay() {
    let text = "é".repeat(160_000);
    let expr = constant(selected(
        &StringArray::from(vec!["unselected", text.as_str()]),
        1,
    ));
    let baseline = Control::new(None);
    let formatted = format_expr(&expr, &baseline).unwrap();
    assert_eq!(formatted, format!("'{text}'"));
    assert!(baseline.trace().iter().any(|(_, units)| *units == 256));
    assert_original_refusals(|control| format_expr(&expr, control));
}
