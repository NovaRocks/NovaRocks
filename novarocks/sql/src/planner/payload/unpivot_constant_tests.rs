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
use crate::analysis::{ExprKind, UnpivotConstant};
use crate::compiler::SqlCompileError;
use arrow::array::{ArrayRef, StringArray, StructArray};
use arrow::datatypes::{Field, Fields};
use novarocks_functions::{ConstantPolicy, ConstantPool, ConstantValue};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, FunctionValueType, PureCompileControl, ValueLogicalType,
};
use std::collections::HashMap;
use std::sync::Mutex;

struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl Control {
    fn good() -> Self {
        Self {
            trace: Mutex::new(Vec::new()),
            refusal: None,
        }
    }
    fn at(index: usize, cause: CompileControlError) -> Self {
        Self {
            trace: Mutex::new(Vec::new()),
            refusal: Some((index, cause)),
        }
    }
    fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.trace.lock().unwrap().clone()
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut trace = self.trace.lock().unwrap();
        let index = trace.len();
        if let Some((at, _)) = self.refusal {
            assert!(index <= at, "rejected control was invoked again");
        }
        trace.push((phase, units));
        match self.refusal.clone() {
            Some((at, cause)) if at == index => Err(cause),
            _ => Ok(()),
        }
    }
}
fn policy() -> ConstantPolicy {
    crate::constant::test_constant_policy()
}
fn cv(array: ArrayRef, ty: FunctionValueType, ordinal: u32) -> ConstantValue {
    ConstantPool::try_new(
        Arc::new(
            Field::new("actual.source", array.data_type().clone(), ty.nullable()).with_metadata(
                std::collections::HashMap::<String, String>::from([(
                    "provider.fact".into(),
                    "retained".into(),
                )]),
            ),
        ),
        ty,
        array.to_data(),
        policy(),
        CompilePhase::Validate,
        crate::optimizer::test_optimizer_control(),
    )
    .unwrap()
    .value(ordinal)
    .unwrap()
}
fn text(values: Vec<Option<&str>>, ordinal: u32) -> ConstantValue {
    let nullable = values.iter().any(Option::is_none);
    cv(
        Arc::new(StringArray::from(values)),
        FunctionValueType::new(DataType::Utf8, nullable),
        ordinal,
    )
}
fn column(id: u32, name: &str, value_type: FunctionValueType) -> OutputColumn {
    OutputColumn {
        column_id: ColumnId(id),
        name: name.into(),
        value_type,
        is_internal: false,
    }
}
struct Fixture {
    input: Vec<OutputColumn>,
    mappings: Vec<PlanUnpivotValueMapping>,
    output: Vec<OutputColumn>,
}
impl Fixture {
    fn new(value: ConstantValue) -> Self {
        let expression = TypedExpr {
            value_type: value.value_type().clone(),
            kind: ExprKind::Constant(value),
        };
        Self {
            input: vec![column(
                1,
                "value",
                FunctionValueType::new(DataType::Int64, false),
            )],
            output: vec![
                column(2, "value", FunctionValueType::new(DataType::Int64, false)),
                column(3, "label", expression.value_type.clone()),
            ],
            mappings: vec![PlanUnpivotValueMapping {
                input_value_column_id: ColumnId(1),
                constants: vec![UnpivotConstant::Scalar(expression)],
            }],
        }
    }
    fn construct(
        &self,
        control: &dyn PureCompileControl,
    ) -> Result<PlanUnpivotNode, SqlCompileError> {
        PlanUnpivotNode::try_new(
            &self.input,
            Vec::new(),
            ColumnId(2),
            vec![ColumnId(3)],
            self.mappings.clone(),
            self.output.clone(),
            1024,
            32 * 1024 * 1024,
            policy(),
            control,
        )
    }
    fn scalar_mut(&mut self) -> &mut TypedExpr {
        let UnpivotConstant::Scalar(expression) = &mut self.mappings[0].constants[0] else {
            panic!("scalar fixture");
        };
        expression
    }
}
#[test]
fn actual_unpivot_cv_retains_selected_ordinal_backing_and_complete_source_field() {
    let value = text(
        vec![Some("unused prefix"), Some("chosen"), Some("unused suffix")],
        1,
    );
    let backing = value.pool().backing_identity();
    let source_field = value.pool().field_ref().clone();
    let fixture = Fixture::new(value);
    let node = fixture.construct(&Control::good()).unwrap();
    let UnpivotConstant::Scalar(expression) = &node.value_mappings[0].constants[0] else {
        panic!("scalar");
    };
    let ExprKind::Constant(selected) = &expression.kind else {
        panic!("checked source");
    };
    assert_eq!(selected.ordinal(), 1);
    assert_eq!(selected.pool().backing_identity(), backing);
    assert!(Arc::ptr_eq(selected.pool().field_ref(), &source_field));
    assert_eq!(selected.try_utf8().unwrap(), Some("chosen"));
    let null = Fixture::new(text(vec![Some("unused"), None], 1));
    let node = null.construct(&Control::good()).unwrap();
    assert!(node.output_columns[1].value_type.nullable());
    let UnpivotConstant::Scalar(expression) = &node.value_mappings[0].constants[0] else {
        panic!("scalar");
    };
    let ExprKind::Constant(value) = &expression.kind else {
        panic!("checked NULL");
    };
    assert_eq!(value.try_utf8().unwrap(), None);
}
#[test]
fn actual_unpivot_cv_refuses_declared_source_and_output_domain_drift() {
    let mut mismatch = Fixture::new(text(vec![Some("chosen")], 0));
    mismatch.scalar_mut().value_type = mismatch
        .scalar_mut()
        .value_type
        .with_nullable(true)
        .unwrap();
    assert!(
        mismatch
            .construct(&Control::good())
            .unwrap_err()
            .to_string()
            .contains("materialized constant source type")
    );
    let ty =
        FunctionValueType::try_with_logical_type(DataType::Utf8, false, ValueLogicalType::Json)
            .unwrap();
    let json = cv(Arc::new(StringArray::from(vec!["{}"])), ty, 0);
    let mut nominal = Fixture::new(json);
    assert!(nominal.construct(&Control::good()).is_ok());
    nominal.output[1].value_type =
        novarocks_type_contract::FunctionValueType::try_with_logical_type(
            nominal.output[1].value_type.data_type().clone(),
            nominal.output[1].value_type.nullable(),
            ValueLogicalType::Physical,
        )
        .unwrap();
    assert!(
        nominal
            .construct(&Control::good())
            .unwrap_err()
            .to_string()
            .contains("type mismatch")
    );
    let mut nonconstant = Fixture::new(text(vec![Some("chosen")], 0));
    nonconstant.scalar_mut().kind = ExprKind::ColumnRef {
        column_id: ColumnId(1),
        qualifier: None,
        column: "value".into(),
    };
    assert!(
        nonconstant
            .construct(&Control::good())
            .unwrap_err()
            .to_string()
            .contains("not a syntax or checked constant")
    );
}
#[test]
fn actual_unpivot_cv_byte_limit_counts_selected_payload_not_unused_pool_rows() {
    const CAP: usize = 16 * 1024 * 1024;
    let large = "x".repeat(CAP + 1);
    let small_selected = Fixture::new(text(vec![Some(&large), Some("selected")], 1));
    assert!(small_selected.construct(&Control::good()).is_ok());
    let exact = Fixture::new(text(vec![Some(&large[..CAP])], 0));
    assert!(exact.construct(&Control::good()).is_ok());
    let over = Fixture::new(text(vec![Some(&large)], 0));
    assert!(
        over.construct(&Control::good())
            .unwrap_err()
            .to_string()
            .contains("decoded constant byte limit")
    );
}
fn assert_cause(error: SqlCompileError, cause: CompileControlError) {
    assert!(matches!(
        (error, cause),
        (SqlCompileError::Cancelled, CompileControlError::Cancelled)
            | (
                SqlCompileError::DeadlineExceeded,
                CompileControlError::DeadlineExceeded
            )
            | (
                SqlCompileError::ResourceExhausted,
                CompileControlError::ResourceExhausted
            )
    ));
}
fn check_callbacks(fixture: &Fixture, success: bool, quantum_only: bool) {
    let good = Control::good();
    assert_eq!(fixture.construct(&good).is_ok(), success);
    let trace = good.trace();
    assert_eq!(trace.first(), Some(&(CompilePhase::Validate, 0)));
    assert_eq!(trace.last().unwrap().0, CompilePhase::Validate);
    assert!(trace.len() >= 2);
    if quantum_only {
        assert!(trace.iter().any(|(_, units)| *units == 256));
    }
    for index in 0..trace.len() {
        if quantum_only && index != 0 && index != trace.len() - 1 && trace[index].1 != 256 {
            continue;
        }
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let refusing = Control::at(index, cause.clone());
            assert_cause(fixture.construct(&refusing).unwrap_err(), cause);
            assert_eq!(refusing.trace(), trace[..=index]);
        }
    }
}
#[test]
fn actual_unpivot_cv_controls_preserve_success_ordinary_tail_and_wide_type_quantum() {
    let success = Fixture::new(text(vec![Some("prefix"), Some("chosen")], 1));
    check_callbacks(&success, true, false);
    let mut mismatch = Fixture::new(text(vec![Some("chosen")], 0));
    mismatch.scalar_mut().value_type = mismatch
        .scalar_mut()
        .value_type
        .with_nullable(true)
        .unwrap();
    check_callbacks(&mismatch, false, false);
    let mut output_mismatch = Fixture::new(text(vec![Some("chosen")], 0));
    output_mismatch.output[1].value_type = FunctionValueType::new(DataType::Int64, false);
    check_callbacks(&output_mismatch, false, false);
    let fields = Fields::from(
        (0..320)
            .map(|index| {
                Arc::new(
                    Field::new(format!("child_{index}"), DataType::Utf8, false).with_metadata(
                        std::collections::HashMap::<String, String>::from([(
                            "provider.attribute".into(),
                            "kept".into(),
                        )]),
                    ),
                )
            })
            .collect::<Vec<_>>(),
    );
    let columns: Vec<ArrayRef> = (0..320)
        .map(|_| Arc::new(StringArray::from(vec!["prefix", "chosen"])) as ArrayRef)
        .collect();
    let array = StructArray::new(fields.clone(), columns, None);
    let mut wide = Fixture::new(cv(
        Arc::new(array),
        FunctionValueType::new(DataType::Struct(fields.clone()), false),
        1,
    ));
    wide.scalar_mut().value_type = FunctionValueType::new(DataType::Struct(fields.clone()), false);
    wide.output[1].value_type = FunctionValueType::new(DataType::Struct(fields), false);
    check_callbacks(&wide, true, true);
    let mut drift = wide;
    let DataType::Struct(fields) = &drift.output[1].value_type.data_type() else {
        panic!("struct");
    };
    let changed = fields[0].as_ref().clone().with_metadata(HashMap::new());
    drift.output[1].value_type = novarocks_type_contract::FunctionValueType::try_with_logical_type(
        DataType::Struct(Fields::from(vec![Arc::new(changed)])),
        drift.output[1].value_type.nullable(),
        drift.output[1].value_type.logical_type(),
    )
    .unwrap();
    assert!(
        drift
            .construct(&Control::good())
            .unwrap_err()
            .to_string()
            .contains("type mismatch")
    );
}

#[test]
fn actual_unpivot_passthrough_and_value_outputs_preserve_complete_domains_and_nullable_or() {
    let fixture = Fixture::new(text(vec![Some("label")], 0));
    let original = fixture.construct(&Control::good()).unwrap();
    let json =
        FunctionValueType::try_with_logical_type(DataType::Utf8, false, ValueLogicalType::Json)
            .unwrap();
    let uuid = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        false,
        ValueLogicalType::Uuid,
    )
    .unwrap();
    let physical_utf8 = FunctionValueType::new(DataType::Utf8, false);
    let physical_fixed = FunctionValueType::new(DataType::FixedSizeBinary(16), false);
    for (source, wrong) in [(json, physical_utf8), (uuid, physical_fixed)] {
        // Actual value role permits only the existing per-output nullable OR.
        let mut input = fixture.input.clone();
        input[0].value_type = source.clone();
        let mut node = original.clone();
        node.output_columns[0].value_type = source.clone();
        check_relation_callbacks(&node, &input, true);
        node.output_columns[0].value_type = wrong.clone();
        check_relation_callbacks(&node, &input, false);

        // Passthrough is an exact relation, including root nullability.
        let mut node = original.clone();
        let mut input = fixture.input.clone();
        input.push(column(9, "source", source.clone()));
        node.passthrough_columns.push(PlanUnpivotPassthroughColumn {
            input_column_id: ColumnId(9),
            output_column_id: ColumnId(10),
        });
        node.output_columns
            .push(column(10, "renamed.output", source.clone()));
        check_relation_callbacks(&node, &input, true);
        node.output_columns.last_mut().unwrap().value_type = wrong;
        check_relation_callbacks(&node, &input, false);
        node.output_columns.last_mut().unwrap().value_type = source.clone();
        node.output_columns.last_mut().unwrap().value_type = node
            .output_columns
            .last_mut()
            .unwrap()
            .value_type
            .with_nullable(true)
            .unwrap();
        check_relation_callbacks(&node, &input, false);

        let mut node = original.clone();
        let mut input = fixture.input.clone();
        input[0].value_type = source.clone();
        let mut nullable_source = source;
        nullable_source = nullable_source.with_nullable(true).unwrap();
        input.push(column(8, "nullable.input", nullable_source.clone()));
        let mut second = node.value_mappings[0].clone();
        second.input_value_column_id = ColumnId(8);
        node.value_mappings.push(second);
        node.output_columns[0].value_type = nullable_source;
        check_relation_callbacks(&node, &input, true);
        node.output_columns[0].value_type = node.output_columns[0]
            .value_type
            .with_nullable(false)
            .unwrap();
        check_relation_callbacks(&node, &input, false);
    }

    let child = Arc::new(dictionary_field(7, true, true));
    let ty = FunctionValueType::new(DataType::Struct(Fields::from(vec![child.clone()])), false);
    let mut input = fixture.input.clone();
    input[0].value_type = ty.clone();
    let mut node = original.clone();
    node.output_columns[0].value_type = ty;
    check_relation_callbacks(&node, &input, true);
    for (changed, compatible) in [
        (dictionary_field(8, true, true), true),
        (dictionary_field(7, false, true), true),
        (dictionary_field(7, true, false), true),
        (dictionary_field(7, true, true).with_nullable(false), false),
    ] {
        node.output_columns[0].value_type =
            novarocks_type_contract::FunctionValueType::try_with_logical_type(
                DataType::Struct(Fields::from(vec![Arc::new(changed)])),
                node.output_columns[0].value_type.nullable(),
                node.output_columns[0].value_type.logical_type(),
            )
            .unwrap();
        check_relation_callbacks(&node, &input, compatible);
    }
}

fn check_relation_callbacks(node: &PlanUnpivotNode, input: &[OutputColumn], success: bool) {
    let good = Control::good();
    assert_eq!(
        node.validate_against(input, policy(), &good).is_ok(),
        success
    );
    let trace = good.trace();
    assert_eq!(trace.first(), Some(&(CompilePhase::Validate, 0)));
    assert_eq!(trace.last().unwrap().0, CompilePhase::Validate);
    for index in 0..trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let refusing = Control::at(index, cause.clone());
            assert_cause(
                node.validate_against(input, policy(), &refusing)
                    .unwrap_err(),
                cause,
            );
            assert_eq!(refusing.trace(), trace[..=index]);
        }
    }
}

#[allow(deprecated)]
fn dictionary_field(id: i64, ordered: bool, annotated: bool) -> Field {
    Field::new_dict(
        "nested",
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
        true,
        id,
        ordered,
    )
    .with_metadata(if annotated {
        HashMap::from([("provider.fact".into(), "kept".into())])
    } else {
        HashMap::new()
    })
}
