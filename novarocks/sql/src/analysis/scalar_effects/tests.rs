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
use crate::analysis::{ExprKind, TypedExpr};
use crate::column_id::ColumnId;
use crate::optimizer::scalar::ScalarArena;
use arrow::datatypes::DataType;
use novarocks_parser::ast::AstBox;

fn column(id: u32, value_type: FunctionValueType) -> TypedExpr {
    TypedExpr {
        value_type,
        kind: ExprKind::ColumnRef {
            column_id: ColumnId(id),
            qualifier: None,
            column: format!("c{id}"),
        },
    }
}

fn explicit_cast(source: TypedExpr, target: DataType) -> TypedExpr {
    let nullable = source.value_type.nullable();
    TypedExpr {
        value_type: FunctionValueType::new(target.clone(), nullable),
        kind: ExprKind::Cast {
            cast_mode: Some(TrinoCastMode::Explicit),
            expr: AstBox::new(source),
            target,
            decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
        },
    }
}

fn query(control: &dyn PureCompileControl) -> ScalarEffectQuery<'_> {
    ScalarEffectQuery {
        functions: Some(crate::functions::builtin_sql_function_catalog()),
        allow_throw_exception: true,
        control,
    }
}

#[test]
fn typed_and_arena_share_actual_cast_and_comparison_effects() {
    let control = crate::optimizer::test_optimizer_control();
    let query = query(control);
    let mut arena = ScalarArena::new();
    let exact = explicit_cast(
        column(1, FunctionValueType::new(DataType::Int64, true)),
        DataType::Float64,
    );
    let parsed = explicit_cast(
        column(2, FunctionValueType::new(DataType::Utf8, true)),
        DataType::Int64,
    );
    let compared = TypedExpr {
        value_type: FunctionValueType::new(DataType::Boolean, true),
        kind: ExprKind::BinaryOp {
            left: AstBox::new(exact.clone()),
            op: BinOp::Eq,
            right: AstBox::new(column(3, FunctionValueType::new(DataType::Int64, false))),
            decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
        },
    };
    for (expr, row_error) in [(exact, false), (parsed, true), (compared, false)] {
        let id = crate::planner::optimizer_bridge::scalar::intern_typed(&mut arena, &expr, control)
            .unwrap();
        let typed = query.typed(&expr).unwrap();
        assert_eq!(typed, arena.effects(id));
        assert_eq!(typed.expression.may_raise_row_error, row_error);
        assert!(!typed.expression.may_raise_invocation_data);
    }
}

#[test]
fn conversion_effects_require_the_exact_value_conversion_and_include_the_carrier_tail() {
    use arrow::datatypes::Field;
    use novarocks_type_contract::NR_LOGICAL_TYPE_KEY;
    use std::sync::Arc;

    let control = crate::optimizer::test_optimizer_control();
    let query = query(control);
    let field = Field::new("source", DataType::LargeBinary, false)
        .with_metadata([(NR_LOGICAL_TYPE_KEY.to_owned(), "variant".to_owned())]);
    let source =
        FunctionValueType::from_legacy_declared_projection(DataType::List(Arc::new(field)), false);
    let target = FunctionValueType::new(
        DataType::List(Arc::new(Field::new("item", DataType::LargeBinary, true))),
        true,
    );
    assert!(matches!(
        plan_conversion(query.functions, &source, &target, None, control).unwrap(),
        PlannedConversion::ValueConversion { tail: Some(_), .. }
    ));
    let proven = query.cast(&source, &target, None).unwrap();
    assert_ne!(proven, ScalarEffects::UNKNOWN);
    assert_eq!(
        ScalarEffectQuery {
            functions: None,
            ..query
        }
        .cast(&source, &target, None)
        .unwrap(),
        ScalarEffects::UNKNOWN,
    );
}

#[test]
fn author_control_failure_does_not_become_an_unknown_effect() {
    struct Refuse(CompileControlError);
    impl PureCompileControl for Refuse {
        fn checkpoint(
            &self,
            _: CompilePhase,
            _: u32,
        ) -> std::result::Result<(), CompileControlError> {
            Err(self.0.clone())
        }
    }
    let wide = FunctionValueType::new(DataType::Float64, false);
    let integer = FunctionValueType::new(DataType::Int64, false);
    for error in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let control = Refuse(error.clone());
        assert_eq!(
            query(&control).cast(&integer, &wide, Some(TrinoCastMode::Explicit)),
            Err(SqlCompileError::from(error)),
        );
    }
}
