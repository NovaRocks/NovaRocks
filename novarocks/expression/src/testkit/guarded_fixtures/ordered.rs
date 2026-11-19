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
    Fixture, GUARDED_CONTROL, Shape, add_call, argument, author, catalogue, compile_project_unit,
    fixture_constant_policy, literal, project_port, random, round_value, unit_draft,
};
use crate::{CompiledExpressionInstance, testkit::checked_constants};
use arrow::{
    array::{ArrayRef, BooleanArray},
    datatypes::DataType,
    record_batch::RecordBatch,
};
use novarocks_constant_contract::ConstantPools;
use novarocks_expression_contract::{BinaryOperator, ExprKind, RootScope, ValueId};
use novarocks_type_contract::{
    ControlShape, DecimalOverflowPolicy, EvaluationDemand, FunctionValueType,
};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Clone, Copy)]
pub enum Source {
    Columns,
    Random,
    Rounded,
}
pub fn ordered_program(
    op: BinaryOperator,
    ty: DataType,
    source_kind: Source,
    truth_only: bool,
) -> Fixture {
    ordered_value_program(
        op,
        FunctionValueType::new(ty, true),
        source_kind,
        truth_only,
    )
}
pub fn ordered_value_program(
    op: BinaryOperator,
    scalar: FunctionValueType,
    source_kind: Source,
    truth_only: bool,
) -> Fixture {
    let mut source_pools = ConstantPools::empty();
    let functions = catalogue(Shape::Decimal);
    let mut builder = unit_draft();
    let flag = FunctionValueType::new(DataType::Boolean, true);
    let columns = [
        (ValueId::new(901), scalar.clone()),
        (ValueId::new(71), scalar.clone()),
        (ValueId::new(72), flag.clone()),
    ];
    let [left_input, right_input, flag_input] = columns.each_ref().map(|(id, _)| *id);
    let port = project_port(columns, &mut source_pools);
    let mut authors = BTreeMap::new();
    let (left, right) = if matches!(source_kind, Source::Random) {
        let condition = builder
            .add(None, flag.clone(), ExprKind::Value(flag_input))
            .unwrap();
        let left_rand = random(&mut builder, &mut authors, &functions, &mut source_pools);
        let null = literal(
            &mut builder,
            scalar.clone(),
            checked_constants::null(&(scalar.clone()), fixture_constant_policy()),
            &mut source_pools,
        );
        let left = add_call(
            &mut builder,
            &mut authors,
            author(
                &functions,
                "if",
                vec![
                    argument(flag, None),
                    argument(scalar.clone(), None),
                    argument(scalar.clone(), None),
                ],
                ControlShape::If,
            ),
            vec![condition, left_rand, null],
        );
        (
            left,
            random(&mut builder, &mut authors, &functions, &mut source_pools),
        )
    } else {
        let left = builder
            .add(None, scalar.clone(), ExprKind::Value(left_input))
            .unwrap();
        let right = builder
            .add(None, scalar.clone(), ExprKind::Value(right_input))
            .unwrap();
        if matches!(source_kind, Source::Rounded) {
            (
                round_value(
                    &mut builder,
                    &mut authors,
                    &functions,
                    left,
                    &mut source_pools,
                ),
                round_value(
                    &mut builder,
                    &mut authors,
                    &functions,
                    right,
                    &mut source_pools,
                ),
            )
        } else {
            (left, right)
        }
    };
    let result_type = FunctionValueType::new(DataType::Boolean, true);
    let expr = builder
        .add(
            None,
            result_type,
            ExprKind::Binary {
                op,
                left,
                right,
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                allow_throw_exception: None,
            },
        )
        .unwrap();
    let expr = builder
        .root(
            expr,
            if truth_only {
                EvaluationDemand::TruthOnly
            } else {
                EvaluationDemand::Value
            },
            RootScope::Port(0),
        )
        .unwrap();
    let unit = compile_project_unit(builder, port, &functions, &authors, source_pools);
    Fixture { unit, root: expr }
}

pub fn ordered_instance(program: &Fixture) -> CompiledExpressionInstance {
    program.instance(&GUARDED_CONTROL)
}

pub fn ordered_batch(
    program: &Fixture,
    left: ArrayRef,
    right: ArrayRef,
    flags: Vec<Option<bool>>,
) -> RecordBatch {
    RecordBatch::try_new(
        Arc::clone(program.input_schema()),
        vec![left, right, Arc::new(BooleanArray::from(flags))],
    )
    .unwrap()
}
