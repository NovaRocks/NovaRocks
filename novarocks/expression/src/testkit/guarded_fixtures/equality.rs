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

//! Binary and simple CASE fixtures authored as one neutral expression unit.

use super::{
    Fixture, SEED_42, Shape, catalogue, compile_project_unit, fixture_constant_policy, literal,
    project_port, random, round_value, unit_draft,
};
use crate::{ComparisonSite, testkit::checked_constants};
use arrow::{
    array::{Decimal128Array, Float64Array, Int64Array},
    datatypes::DataType,
    record_batch::RecordBatch,
};
use novarocks_constant_contract::ConstantPools;
use novarocks_expression_contract::{
    BinaryOperator, BoundExprSetBuilder, ExprKind, RootScope, ValueId,
};
use novarocks_type_contract::{
    DecimalOverflowPolicy, EvaluationDemand, ExpressionUseId, FunctionValueType,
};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Clone, Copy)]
pub enum EqualityCase {
    Eq,
    NotEq,
    Columns,
    RandomOperand,
    RandomLabels,
    ErrorOperand,
    ErrorLabel,
}
pub fn equality_program(kind: EqualityCase) -> Fixture {
    let mut source_pools = ConstantPools::empty();
    let functions = catalogue(Shape::Decimal);
    let mut builder = unit_draft();
    let floating = FunctionValueType::new(DataType::Float64, true);
    let decimal = FunctionValueType::new(DataType::Decimal128(38, 0), true);
    let columns = [
        (ValueId::new(901), floating.clone()),
        (ValueId::new(71), floating.clone()),
        (ValueId::new(72), floating.clone()),
        (ValueId::new(3), decimal.clone()),
        (ValueId::new(99), decimal.clone()),
    ];
    let port = project_port(columns.clone(), &mut source_pools);
    let value = |builder: &mut BoundExprSetBuilder, index: usize| {
        builder
            .add(
                None,
                columns[index].1.clone(),
                ExprKind::Value(columns[index].0),
            )
            .unwrap()
    };
    let mut authors = BTreeMap::new();
    let result_type = if matches!(kind, EqualityCase::Eq | EqualityCase::NotEq) {
        FunctionValueType::new(DataType::Boolean, true)
    } else {
        FunctionValueType::new(DataType::Int64, false)
    };
    let expr = if matches!(kind, EqualityCase::Eq | EqualityCase::NotEq) {
        let left = value(&mut builder, 0);
        let right = value(&mut builder, 1);
        builder
            .add(
                None,
                result_type.clone(),
                ExprKind::Binary {
                    op: if matches!(kind, EqualityCase::Eq) {
                        BinaryOperator::Eq
                    } else {
                        BinaryOperator::NotEq
                    },
                    left,
                    right,
                    decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                    allow_throw_exception: None,
                },
            )
            .unwrap()
    } else {
        let (operand, first, second) = match kind {
            EqualityCase::Columns => (
                value(&mut builder, 0),
                value(&mut builder, 1),
                value(&mut builder, 2),
            ),
            EqualityCase::RandomOperand => {
                let operand = random(&mut builder, &mut authors, &functions, &mut source_pools);
                let first = literal(
                    &mut builder,
                    floating.clone(),
                    checked_constants::f64_bits(
                        &(floating.clone()),
                        SEED_42[0],
                        fixture_constant_policy(),
                    ),
                    &mut source_pools,
                );
                let second = literal(
                    &mut builder,
                    (floating).clone(),
                    checked_constants::f64_bits(&(floating), SEED_42[1], fixture_constant_policy()),
                    &mut source_pools,
                );
                (operand, first, second)
            }
            EqualityCase::RandomLabels => (
                value(&mut builder, 0),
                random(&mut builder, &mut authors, &functions, &mut source_pools),
                random(&mut builder, &mut authors, &functions, &mut source_pools),
            ),
            EqualityCase::ErrorOperand => {
                let source = value(&mut builder, 3);
                let operand = round_value(
                    &mut builder,
                    &mut authors,
                    &functions,
                    source,
                    &mut source_pools,
                );
                (
                    operand,
                    value(&mut builder, 4),
                    literal(
                        &mut builder,
                        decimal.clone(),
                        checked_constants::null(&(decimal.clone()), fixture_constant_policy()),
                        &mut source_pools,
                    ),
                )
            }
            EqualityCase::ErrorLabel => {
                let operand = value(&mut builder, 3);
                let source = value(&mut builder, 4);
                let first = round_value(
                    &mut builder,
                    &mut authors,
                    &functions,
                    source,
                    &mut source_pools,
                );
                // The later label is a separate occurrence of the original
                // operand definition; no failed first label may reach it.
                (operand, first, operand)
            }
            EqualityCase::Eq | EqualityCase::NotEq => unreachable!(),
        };
        let then_first = literal(
            &mut builder,
            result_type.clone(),
            checked_constants::i64(&(result_type.clone()), 10, fixture_constant_policy()),
            &mut source_pools,
        );
        let then_second = literal(
            &mut builder,
            result_type.clone(),
            checked_constants::i64(&(result_type.clone()), 20, fixture_constant_policy()),
            &mut source_pools,
        );
        let otherwise = literal(
            &mut builder,
            result_type.clone(),
            checked_constants::i64(&(result_type.clone()), 30, fixture_constant_policy()),
            &mut source_pools,
        );
        builder
            .add(
                None,
                result_type.clone(),
                ExprKind::Case {
                    ieee_float_equality: false,
                    operand: Some(operand),
                    when_then: Box::from([(first, then_first), (second, then_second)]),
                    else_expr: Some(otherwise),
                },
            )
            .unwrap()
    };
    let expr = builder
        .root(expr, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    Fixture {
        unit: compile_project_unit(builder, port, &functions, &authors, source_pools),
        root: expr,
    }
}

pub fn equality_batch(
    program: &Fixture,
    left: Vec<Option<f64>>,
    first: Vec<Option<f64>>,
    second: Vec<Option<f64>>,
    decimal: Vec<Option<i128>>,
    decimal_label: Vec<Option<i128>>,
) -> RecordBatch {
    RecordBatch::try_new(
        program.input_schema().clone(),
        vec![
            Arc::new(Float64Array::from(left)),
            Arc::new(Float64Array::from(first)),
            Arc::new(Float64Array::from(second)),
            Arc::new(
                Decimal128Array::from(decimal)
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            ),
            Arc::new(
                Decimal128Array::from(decimal_label)
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            ),
        ],
    )
    .unwrap()
}
pub fn float_batch(
    program: &Fixture,
    left: Vec<Option<f64>>,
    first: Vec<Option<f64>>,
    second: Vec<Option<f64>>,
) -> RecordBatch {
    let rows = left.len();
    equality_batch(
        program,
        left,
        first,
        second,
        vec![None; rows],
        vec![None; rows],
    )
}
pub fn integers(output: &novarocks_functions::SelectedValues<'_>) -> Vec<Option<i64>> {
    output
        .values()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .iter()
        .collect()
}
pub fn root_use(program: &Fixture) -> ExpressionUseId {
    program.unit.root(program.root).unwrap().use_id
}
pub fn assert_case_recipes(program: &Fixture, ty: DataType) {
    let occurrence = root_use(program);
    for arm in 0..2 {
        let recipe = program
            .unit
            .recipes()
            .comparison(ComparisonSite::CaseWhen { occurrence, arm })
            .unwrap();
        assert_eq!(recipe.left_type().data_type().clone(), ty);
        assert_eq!(recipe.right_type().data_type().clone(), ty);
    }
    assert!(
        program
            .unit
            .recipes()
            .comparison(ComparisonSite::CaseWhen { occurrence, arm: 2 })
            .is_none()
    );
}
