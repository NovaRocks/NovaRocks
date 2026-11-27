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

use super::{
    Fixture, Shape, catalogue, compile_project_unit, fixture_constant_policy, project_port, random,
    round_value, unit_draft,
};
use crate::testkit::checked_constants;
use arrow::{
    array::{BooleanArray, Decimal128Array, Float64Array},
    datatypes::DataType,
    record_batch::RecordBatch,
};
use novarocks_constant_contract::ConstantPools;
use novarocks_expression_contract::{BoundExprSetBuilder, ExprId, ExprKind, RootScope, ValueId};
use novarocks_type_contract::{EvaluationDemand, FunctionValueType};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Clone, Copy)]
pub enum CaseKind {
    Priority,
    NoElse,
    Random,
    DeferredRandom,
    ErrorThen,
}
fn literal_i64(
    builder: &mut BoundExprSetBuilder,
    value: i64,
    source_pools: &mut ConstantPools,
) -> ExprId {
    builder
        .add(
            None,
            (FunctionValueType::new(DataType::Int64, false)).clone(),
            checked_constants::kind(
                source_pools,
                checked_constants::i64(
                    &(FunctionValueType::new(DataType::Int64, false)),
                    value,
                    fixture_constant_policy(),
                ),
            ),
        )
        .unwrap()
}
pub fn case_program(kind: CaseKind) -> Fixture {
    let mut source_pools = ConstantPools::empty();
    let functions = catalogue(Shape::Decimal);
    let first = ValueId::new(901);
    let second = ValueId::new(3);
    let decimal_value = ValueId::new(71);
    let fallback = ValueId::new(72);
    let boolean = FunctionValueType::new(DataType::Boolean, true);
    let decimal = FunctionValueType::new(DataType::Decimal128(38, 0), true);
    let columns = [
        (first, boolean.clone()),
        (second, boolean.clone()),
        (decimal_value, decimal.clone()),
        (fallback, decimal.clone()),
    ];
    let mut builder = unit_draft();
    let port = project_port(columns, &mut source_pools);
    let when_first = builder
        .add(None, boolean.clone(), ExprKind::input(first))
        .unwrap();
    let mut authors = BTreeMap::new();
    let (when_second, then_first, then_second, otherwise, result_type) = match kind {
        CaseKind::Priority | CaseKind::NoElse => {
            let when_second = builder.add(None, boolean, ExprKind::input(second)).unwrap();
            let first = literal_i64(&mut builder, 11, &mut source_pools);
            let second = literal_i64(&mut builder, 22, &mut source_pools);
            let otherwise = if matches!(kind, CaseKind::Priority) {
                Some(literal_i64(&mut builder, 33, &mut source_pools))
            } else {
                None
            };
            (
                when_second,
                first,
                second,
                otherwise,
                FunctionValueType::new(DataType::Int64, matches!(kind, CaseKind::NoElse)),
            )
        }
        CaseKind::Random | CaseKind::DeferredRandom => {
            let when_second = if matches!(kind, CaseKind::DeferredRandom) {
                let value = builder
                    .add(None, decimal.clone(), ExprKind::input(decimal_value))
                    .unwrap();
                let round = round_value(
                    &mut builder,
                    &mut authors,
                    &functions,
                    value,
                    &mut source_pools,
                );
                builder
                    .add(
                        None,
                        FunctionValueType::new(DataType::Boolean, false),
                        ExprKind::IsNull {
                            expr: round,
                            negated: false,
                        },
                    )
                    .unwrap()
            } else {
                builder.add(None, boolean, ExprKind::input(second)).unwrap()
            };
            let first = random(&mut builder, &mut authors, &functions, &mut source_pools);
            let second = random(&mut builder, &mut authors, &functions, &mut source_pools);
            let otherwise = random(&mut builder, &mut authors, &functions, &mut source_pools);
            (
                when_second,
                first,
                second,
                Some(otherwise),
                FunctionValueType::new(DataType::Float64, true),
            )
        }
        CaseKind::ErrorThen => {
            let when_second = builder.add(None, boolean, ExprKind::input(second)).unwrap();
            let value = builder
                .add(None, decimal.clone(), ExprKind::input(decimal_value))
                .unwrap();
            let first = round_value(
                &mut builder,
                &mut authors,
                &functions,
                value,
                &mut source_pools,
            );
            let fallback = builder
                .add(None, decimal.clone(), ExprKind::input(fallback))
                .unwrap();
            // Repeated definition is allowed, but the THEN and ELSE occurrences
            // must have distinct required-domain uses and independent mappings.
            (when_second, first, fallback, Some(fallback), decimal)
        }
    };
    let case = builder
        .add(
            None,
            result_type,
            ExprKind::Case {
                ieee_float_equality: false,
                operand: None,
                when_then: Box::from([(when_first, then_first), (when_second, then_second)]),
                else_expr: otherwise,
            },
        )
        .unwrap();
    let case = builder
        .root(case, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let unit = compile_project_unit(builder, port, &functions, &authors, source_pools);
    Fixture { unit, root: case }
}

pub fn case_batch(
    program: &Fixture,
    first: Vec<Option<bool>>,
    second: Vec<Option<bool>>,
    values: Vec<Option<i128>>,
) -> RecordBatch {
    let rows = first.len();
    RecordBatch::try_new(
        Arc::clone(program.input_schema()),
        vec![
            Arc::new(BooleanArray::from(first)),
            Arc::new(BooleanArray::from(second)),
            Arc::new(
                Decimal128Array::from(values)
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            ),
            Arc::new(
                Decimal128Array::from(vec![Some(71); rows])
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            ),
        ],
    )
    .unwrap()
}
pub fn rand_bits(output: &novarocks_functions::SelectedValues<'_>) -> Vec<Option<u64>> {
    output
        .values()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .iter()
        .map(|value| value.map(f64::to_bits))
        .collect()
}
