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

//! Actual IsNull, Not and stateful Boolean boundary units.

use super::{
    Fixture, Shape, author, catalogue, compile_project_unit, compile_project_unit_with_layout,
    fixture_constant_policy, integer_argument, project_port, round_value, unit_draft,
};
use crate::testkit::{add_call, checked_constants, fixture_scope};
use arrow::{
    array::{BooleanArray, Decimal128Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use novarocks_constant_contract::ConstantPools;
use novarocks_expression_contract::{
    BoundExprSetBuilder, ExprId, ExprKind, RootScope, UnaryOperator, ValueId,
};
use novarocks_type_contract::{
    ControlShape, EvaluationDemand, FunctionValueType, SemanticParameters,
};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Clone, Copy)]
pub enum UnaryCase {
    IsNull,
    IsNotNull,
    PureAnd,
    PureOr,
    StateBoundary,
    NotFilter,
}

fn unary(builder: &mut BoundExprSetBuilder, child: ExprId, negated: bool) -> ExprId {
    builder
        .add(
            None,
            FunctionValueType::new(DataType::Boolean, false),
            ExprKind::IsNull {
                expr: child,
                negated,
            },
        )
        .unwrap()
}

pub fn unary_program(case: UnaryCase) -> Fixture {
    let mut source_pools = ConstantPools::empty();
    let functions = catalogue(Shape::Decimal);
    let flag = ValueId::new(901);
    let decimal_value = ValueId::new(71);
    let bool_type = FunctionValueType::new(DataType::Boolean, true);
    let decimal = FunctionValueType::new(DataType::Decimal128(38, 0), true);
    let integer = FunctionValueType::new(DataType::Int64, false);
    let columns = [(flag, bool_type.clone()), (decimal_value, decimal.clone())];
    let mut builder = unit_draft();
    let port = project_port(columns, &mut source_pools);
    let flag_expr = if matches!(case, UnaryCase::IsNull | UnaryCase::IsNotNull) {
        None
    } else {
        Some(
            builder
                .add(None, bool_type.clone(), ExprKind::Value(flag))
                .unwrap(),
        )
    };
    let mut authors = BTreeMap::new();
    let expression = if matches!(case, UnaryCase::NotFilter) {
        let actual_true = builder
            .add(
                None,
                (FunctionValueType::new(DataType::Boolean, false)).clone(),
                checked_constants::kind(
                    &mut source_pools,
                    checked_constants::boolean(
                        &(FunctionValueType::new(DataType::Boolean, false)),
                        true,
                        fixture_constant_policy(),
                    ),
                ),
            )
            .unwrap();
        let conjunction = builder
            .add(
                None,
                bool_type.clone(),
                ExprKind::Conjunction {
                    args: Box::from([flag_expr.unwrap(), actual_true]),
                },
            )
            .unwrap();
        builder
            .add(
                None,
                bool_type.clone(),
                ExprKind::Unary {
                    op: UnaryOperator::Not,
                    expr: conjunction,
                },
            )
            .unwrap()
    } else {
        let value = builder
            .add(None, decimal.clone(), ExprKind::Value(decimal_value))
            .unwrap();
        let round = round_value(
            &mut builder,
            &mut authors,
            &functions,
            value,
            &mut source_pools,
        );
        let test = unary(&mut builder, round, matches!(case, UnaryCase::IsNotNull));
        match case {
            UnaryCase::IsNull | UnaryCase::IsNotNull => test,
            UnaryCase::PureAnd => builder
                .add(
                    None,
                    bool_type.clone(),
                    ExprKind::Conjunction {
                        args: Box::from([test, flag_expr.unwrap()]),
                    },
                )
                .unwrap(),
            UnaryCase::PureOr => builder
                .add(
                    None,
                    bool_type.clone(),
                    ExprKind::Disjunction {
                        args: Box::from([test, flag_expr.unwrap()]),
                    },
                )
                .unwrap(),
            UnaryCase::StateBoundary => {
                let bound = builder
                    .add(
                        None,
                        integer.clone(),
                        checked_constants::kind(
                            &mut source_pools,
                            checked_constants::i64(
                                &(integer.clone()),
                                1,
                                fixture_constant_policy(),
                            ),
                        ),
                    )
                    .unwrap();
                let rand = add_call(
                    &mut builder,
                    &mut authors,
                    author(
                        &functions,
                        "rand",
                        vec![integer_argument(integer, 1)],
                        ControlShape::Eager,
                    ),
                    vec![bound],
                );
                let rand_null = unary(&mut builder, rand, false);
                builder
                    .add(
                        None,
                        bool_type.clone(),
                        ExprKind::Disjunction {
                            args: Box::from([test, rand_null, flag_expr.unwrap()]),
                        },
                    )
                    .unwrap()
            }
            UnaryCase::NotFilter => unreachable!(),
        }
    };
    let expression = builder
        .root(
            expression,
            if matches!(case, UnaryCase::NotFilter) {
                EvaluationDemand::TruthOnly
            } else {
                EvaluationDemand::Value
            },
            RootScope::Port(0),
        )
        .unwrap();
    let unit = if matches!(case, UnaryCase::NotFilter) {
        // The input Project forwards the exact Filter result columns, so its
        // runtime fields carry the original result names.
        let scope = fixture_scope(
            &port,
            Arc::new(Schema::new(vec![
                Field::new("input_0", DataType::Boolean, true),
                Field::new("input_1", DataType::Decimal128(38, 0), true),
            ])),
        );
        compile_project_unit_with_layout(
            builder,
            port,
            scope,
            &functions,
            &authors,
            source_pools,
            &SemanticParameters::try_new([]).unwrap(),
        )
    } else {
        compile_project_unit(builder, port, &functions, &authors, source_pools)
    };
    Fixture {
        unit,
        root: expression,
    }
}

pub fn unary_batch(
    program: &Fixture,
    flags: Vec<Option<bool>>,
    decimals: Vec<Option<i128>>,
) -> RecordBatch {
    RecordBatch::try_new(
        program.input_schema().clone(),
        vec![
            Arc::new(BooleanArray::from(flags)),
            Arc::new(
                Decimal128Array::from(decimals)
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            ),
        ],
    )
    .unwrap()
}
pub fn booleans(output: &novarocks_functions::SelectedValues<'_>) -> Vec<Option<bool>> {
    output
        .values()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .iter()
        .collect()
}
pub fn max38() -> i128 {
    10_i128.pow(38) - 1
}
