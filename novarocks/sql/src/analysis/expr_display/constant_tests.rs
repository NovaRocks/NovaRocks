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
use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, LargeBinaryArray, LargeStringArray, StringArray,
    StringViewArray,
};
use arrow::datatypes::DataType;
use novarocks_functions::{ConstantPool, ConstantValue};
use novarocks_type_contract::{CompileControlError, FunctionValueType, ValueLogicalType};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refuse: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::LowerProgram);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        trace.push((phase, units));
        match self.refuse {
            Some((at, cause)) if trace.len() == at + 1 => Err(cause),
            _ => Ok(()),
        }
    }
}
fn source(ty: DataType, selected: &str) -> ConstantPool {
    let values = vec![Some("unused.private.pool.value"), None, Some(selected)];
    let array: ArrayRef = match ty {
        DataType::Utf8 => Arc::new(StringArray::from(values)),
        DataType::LargeUtf8 => Arc::new(LargeStringArray::from(values)),
        DataType::Utf8View => Arc::new(StringViewArray::from(values)),
        _ => unreachable!(),
    };
    let ty = FunctionValueType::new(array.data_type().clone(), true);
    let field = ty.try_to_field("provider.original").unwrap().with_metadata(
        [(
            "provider.fact".to_owned(),
            "not.a.rendered.value".to_owned(),
        )]
        .into(),
    );
    ConstantPool::try_new(
        Arc::new(field),
        ty,
        array.to_data(),
        crate::constant::test_constant_policy(),
        CompilePhase::Validate,
        &crate::compiler::SqlCompileControl::unbounded(),
    )
    .unwrap()
}
fn constant(value: ConstantValue) -> TypedExpr {
    TypedExpr {
        value_type: value.value_type().clone(),
        kind: ExprKind::Constant(value),
    }
}
fn column(name: &str) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::ColumnRef {
            qualifier: None,
            column: name.into(),
            column_id: crate::column_id::ColumnId::UNSET,
        },
        value_type: FunctionValueType::new(DataType::Int64, true),
    }
}
fn nested(value: TypedExpr) -> TypedExpr {
    TypedExpr {
        value_type: value.value_type.clone(),
        kind: ExprKind::Nested(Box::new(value)),
    }
}
fn case(value: TypedExpr) -> TypedExpr {
    TypedExpr {
        value_type: value.value_type.clone(),
        kind: ExprKind::Case {
            operand: None,
            when_then: vec![(
                TypedExpr {
                    value_type: FunctionValueType::new(DataType::Boolean, false),
                    kind: ExprKind::Literal(query_ir::LiteralValue::Bool(true)),
                },
                nested(value),
            )],
            else_expr: None,
        },
    }
}

#[test]
fn selected_constant_display_uses_original_ordinal_and_never_unused_pool_values() {
    for ty in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
        let pool = source(ty, "chosen.界");
        let expr = constant(pool.value(2).unwrap());
        let control = Control::default();
        assert_eq!(
            typed_expr_display_name(&expr, &control).unwrap(),
            "'chosen.界'"
        );
        assert_eq!(
            typed_expr_display_name(&constant(pool.value(1).unwrap()), &Control::default())
                .unwrap(),
            "NULL"
        );
        let rendered = typed_expr_display_name(&case(expr), &Control::default()).unwrap();
        assert_eq!(rendered, "CASE WHEN TRUE THEN ('chosen.界') END");
        assert!(!rendered.contains("unused.private"));
        assert!(!rendered.contains("provider.fact"));
        assert_eq!(pool.field().name(), "provider.original");
        assert_eq!(
            pool.field().metadata().get("provider.fact").unwrap(),
            "not.a.rendered.value"
        );
    }
}

#[test]
fn selected_field_path_and_aggregate_labels_preserve_existing_source_spelling() {
    let pool = source(DataType::Utf8, "a");
    let args = vec![column("c13"), constant(pool.value(2).unwrap())];
    let expr = TypedExpr {
        value_type: FunctionValueType::new(DataType::Int64, true),
        kind: ExprKind::FunctionCall {
            binding: crate::analysis::test_function_binding(
                "__struct_subfield",
                &args,
                DataType::Int64,
                true,
                crate::functions::FunctionVolatility::Immutable,
            ),
            name: "__struct_subfield".into(),
            args,
            distinct: false,
            volatility: crate::functions::FunctionVolatility::Immutable,
        },
    };
    assert_eq!(
        typed_expr_display_name(&expr, &Control::default()).unwrap(),
        "c13.a"
    );
    assert_eq!(
        agg_call_display_name_from_parts(
            "array_unique_agg",
            &[expr],
            false,
            &[],
            &Control::default()
        )
        .unwrap(),
        "array_unique_agg(c13.a)"
    );
    let separator = source(DataType::Utf8, "|");
    assert_eq!(
        agg_call_display_name_from_parts(
            "string_agg",
            &[column("value"), constant(separator.value(2).unwrap())],
            false,
            &[],
            &Control::default()
        )
        .unwrap(),
        "group_concat(value SEPARATOR '|')"
    );
}

#[test]
fn selected_display_rejects_foreign_full_type_and_preserves_ordinary_failure_tail() {
    let pool = source(DataType::Utf8, "chosen");
    let mut bad = constant(pool.value(2).unwrap());
    bad.value_type = novarocks_type_contract::FunctionValueType::try_with_logical_type(
        bad.value_type.data_type().clone(),
        bad.value_type.nullable(),
        ValueLogicalType::Json,
    )
    .unwrap();
    let baseline = Control::default();
    assert!(matches!(
        typed_expr_display_name(&case(bad.clone()), &baseline),
        Err(SqlCompileError::InvalidRequest(_))
    ));
    let expected = baseline.trace.lock().unwrap().clone();
    assert!(expected.len() >= 2);
    assert!(expected.last().unwrap().1 > 0);
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let at = expected.len() - 1;
        let control = Control {
            trace: Mutex::new(Vec::new()),
            refuse: Some((at, cause)),
        };
        assert_eq!(
            typed_expr_display_name(&case(bad.clone()), &control),
            Err(SqlCompileError::from(cause))
        );
        assert_eq!(*control.trace.lock().unwrap(), expected[..=at]);
    }
}

#[test]
fn every_selected_display_control_refusal_preserves_exact_prefix_without_publication() {
    let text = "界".repeat(120);
    let pool = source(DataType::Utf8, &text);
    let expr = case(constant(pool.value(2).unwrap()));
    let baseline = Control::default();
    assert_eq!(
        typed_expr_display_name(&expr, &baseline).unwrap(),
        format!("CASE WHEN TRUE THEN ('{text}') END")
    );
    let expected = baseline.trace.lock().unwrap().clone();
    assert!(expected.iter().any(|(_, units)| *units == 256));
    for at in 0..expected.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control {
                trace: Mutex::new(Vec::new()),
                refuse: Some((at, cause)),
            };
            assert_eq!(
                typed_expr_display_name(&expr, &control),
                Err(SqlCompileError::from(cause))
            );
            assert_eq!(*control.trace.lock().unwrap(), expected[..=at]);
        }
    }
    assert_eq!(pool.value(2).unwrap().ordinal(), 2);
    assert_eq!(pool.field().name(), "provider.original");
}

fn binary_source(ty: DataType) -> ConstantPool {
    let values = vec![
        Some(b"poison.unselected".as_slice()),
        None,
        Some(b"".as_slice()),
        Some([0x00, 0xab, 0xff].as_slice()),
    ];
    let array: ArrayRef = match ty {
        DataType::Binary => Arc::new(BinaryArray::from(values)),
        DataType::LargeBinary => Arc::new(LargeBinaryArray::from(values)),
        DataType::BinaryView => Arc::new(BinaryViewArray::from(values)),
        _ => unreachable!(),
    };
    let ty = FunctionValueType::new(array.data_type().clone(), true);
    ConstantPool::try_new(
        Arc::new(ty.try_to_field("provider.binary").unwrap()),
        ty,
        array.to_data(),
        crate::constant::test_constant_policy(),
        CompilePhase::Validate,
        &crate::compiler::SqlCompileControl::unbounded(),
    )
    .unwrap()
}

#[test]
fn binary_constant_display_preserves_selected_bytes_empty_and_null() {
    for ty in [
        DataType::Binary,
        DataType::LargeBinary,
        DataType::BinaryView,
    ] {
        let pool = binary_source(ty);
        for (ordinal, expected, diagnostic) in [
            (1, "NULL", "NULL"),
            (2, "X''", ""),
            (3, "X'00ABFF'", "00abff"),
        ] {
            let value = pool.value(ordinal).unwrap();
            let identity = value.pool().backing_identity();
            assert_eq!(
                typed_expr_display_name(&constant(value.clone()), &Control::default()).unwrap(),
                expected
            );
            let mut streamed = String::new();
            crate::constant::write_constant_observed(&value, &Control::default(), &mut streamed)
                .unwrap()
                .unwrap();
            assert_eq!(streamed, expected);
            assert_eq!(
                value
                    .format_diagnostic_observed(CompilePhase::LowerProgram, &Control::default())
                    .unwrap(),
                diagnostic
            );
            assert_eq!(value.ordinal(), ordinal);
            assert_eq!(value.pool().backing_identity(), identity);
        }
        assert_eq!(
            pool.value(0).unwrap().try_binary().unwrap(),
            Some(b"poison.unselected".as_slice())
        );
    }
}

#[test]
fn binary_display_preserves_first_control_and_writer_refusal() {
    let pool = binary_source(DataType::Binary);
    let value = pool.value(3).unwrap();
    let accepted = Control::default();
    assert_eq!(
        crate::constant::format_constant_observed(&value, &accepted).unwrap(),
        "X'00ABFF'"
    );
    let expected = accepted.trace.lock().unwrap().clone();
    for at in 0..expected.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let refused = Control {
                refuse: Some((at, cause)),
                ..Default::default()
            };
            assert_eq!(
                crate::constant::format_constant_observed(&value, &refused),
                Err(SqlCompileError::from(cause))
            );
            assert_eq!(*refused.trace.lock().unwrap(), expected[..=at]);
        }
    }
    struct RefusingWriter;
    impl std::fmt::Write for RefusingWriter {
        fn write_str(&mut self, _: &str) -> std::fmt::Result {
            Err(std::fmt::Error)
        }
    }
    assert_eq!(
        crate::constant::write_constant_observed(&value, &Control::default(), &mut RefusingWriter),
        Ok(Err(std::fmt::Error))
    );
}
