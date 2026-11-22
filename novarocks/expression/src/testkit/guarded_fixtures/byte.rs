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
    Fixture, Shape, argument, author, catalogue, compile_project_unit, fixture_constant_policy,
    integer_argument, project_port, unit_draft,
};
use crate::testkit::{add_call, checked_constants};
use arrow::{
    array::{
        Array, ArrayRef, BinaryArray, BooleanArray, Decimal128Array, LargeBinaryArray,
        LargeStringArray, StringArray,
    },
    datatypes::DataType,
    record_batch::RecordBatch,
};
use novarocks_constant_contract::ConstantPools;
use novarocks_expression_contract::{ExprKind, RootScope, ValueId};
use novarocks_type_contract::{ControlShape, EvaluationDemand, FunctionValueType};
use std::{collections::BTreeMap, sync::Arc};

pub fn carriers() -> [DataType; 4] {
    [
        DataType::Utf8,
        DataType::Binary,
        DataType::LargeUtf8,
        DataType::LargeBinary,
    ]
}

// Typed NULL definitions author the incoming layout; actual byte values below
// come from source channels, not a hypothetical string literal compiler.
pub fn byte_program(carrier: DataType, required_error: bool) -> Fixture {
    let mut source_pools = ConstantPools::empty();
    let functions = catalogue(Shape::Decimal);
    let mut builder = unit_draft();
    let boolean = FunctionValueType::new(DataType::Boolean, true);
    let bytes = FunctionValueType::new(carrier, true);
    let decimal = FunctionValueType::new(DataType::Decimal128(38, 0), true);
    let columns = [
        (ValueId::new(901), boolean.clone()),
        (ValueId::new(71), bytes.clone()),
        (ValueId::new(72), bytes.clone()),
        (ValueId::new(3), decimal.clone()),
    ];
    let port = project_port(columns.clone(), &mut source_pools);
    let mut authors = BTreeMap::new();
    let left = builder
        .add(None, bytes.clone(), ExprKind::Value(columns[1].0))
        .unwrap();
    let right = builder
        .add(None, bytes.clone(), ExprKind::Value(columns[2].0))
        .unwrap();
    let (condition, matched, otherwise) = if required_error {
        let value = builder
            .add(None, decimal.clone(), ExprKind::Value(columns[3].0))
            .unwrap();
        let digits = builder
            .add(
                None,
                (FunctionValueType::new(DataType::Int64, false)).clone(),
                checked_constants::kind(
                    &mut source_pools,
                    checked_constants::i64(
                        &(FunctionValueType::new(DataType::Int64, false)),
                        -1,
                        fixture_constant_policy(),
                    ),
                ),
            )
            .unwrap();
        let rounded = add_call(
            &mut builder,
            &mut authors,
            author(
                &functions,
                "round",
                vec![
                    argument(decimal, None),
                    integer_argument(FunctionValueType::new(DataType::Int64, false), -1),
                ],
                ControlShape::Eager,
            ),
            vec![value, digits],
        );
        let condition = builder
            .add(
                None,
                FunctionValueType::new(DataType::Boolean, false),
                ExprKind::IsNull {
                    expr: rounded,
                    negated: false,
                },
            )
            .unwrap();
        (condition, left, right)
    } else {
        let flag = builder
            .add(None, boolean.clone(), ExprKind::Value(columns[0].0))
            .unwrap();
        let matched = add_call(
            &mut builder,
            &mut authors,
            author(
                &functions,
                "if",
                vec![
                    argument(boolean, None),
                    argument(bytes.clone(), None),
                    argument(bytes.clone(), None),
                ],
                ControlShape::If,
            ),
            vec![flag, left, right],
        );
        let otherwise = add_call(
            &mut builder,
            &mut authors,
            author(
                &functions,
                "coalesce",
                vec![argument(bytes.clone(), None), argument(bytes.clone(), None)],
                ControlShape::Coalesce,
            ),
            vec![left, right],
        );
        (flag, matched, otherwise)
    };
    let expr = builder
        .add(
            None,
            bytes.clone(),
            ExprKind::Case {
                ieee_float_equality: false,
                operand: None,
                when_then: Box::from([(condition, matched)]),
                else_expr: Some(otherwise),
            },
        )
        .unwrap();
    let expr = builder
        .root(expr, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let unit = compile_project_unit(builder, port, &functions, &authors, source_pools);
    Fixture { unit, root: expr }
}

fn byte_array(ty: &DataType, values: &[Option<&[u8]>]) -> ArrayRef {
    match ty {
        DataType::Utf8 => Arc::new(StringArray::from(
            values
                .iter()
                .map(|v| v.map(|b| std::str::from_utf8(b).unwrap()))
                .collect::<Vec<_>>(),
        )),
        DataType::LargeUtf8 => Arc::new(LargeStringArray::from(
            values
                .iter()
                .map(|v| v.map(|b| std::str::from_utf8(b).unwrap()))
                .collect::<Vec<_>>(),
        )),
        DataType::Binary => Arc::new(BinaryArray::from(values.to_vec())),
        DataType::LargeBinary => Arc::new(LargeBinaryArray::from(values.to_vec())),
        _ => unreachable!("four offset carriers only"),
    }
}
pub fn byte_batch(
    program: &Fixture,
    flags: Vec<Option<bool>>,
    left: &[Option<&[u8]>],
    right: &[Option<&[u8]>],
    decimals: Vec<Option<i128>>,
) -> RecordBatch {
    let schema = program.input_schema().clone();
    let ty = schema.field(1).data_type();
    // Nonempty prefix/suffix storage proves source array offsets are honoured.
    let sliced = |values: &[Option<&[u8]>]| {
        let mut backing = vec![Some(b"unused-prefix".as_slice())];
        backing.extend_from_slice(values);
        backing.push(Some(b"unused-suffix".as_slice()));
        byte_array(ty, &backing).slice(1, values.len())
    };
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(BooleanArray::from(flags)),
            sliced(left),
            sliced(right),
            Arc::new(
                Decimal128Array::from(decimals)
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            ),
        ],
    )
    .unwrap()
}
pub fn bytes_of(output: &novarocks_functions::SelectedValues<'_>) -> Vec<Option<Vec<u8>>> {
    let array = output.values();
    (0..array.len())
        .map(|row| {
            if array.is_null(row) {
                return None;
            }
            let value: &[u8] = match array.data_type() {
                DataType::Utf8 => array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(row)
                    .as_bytes(),
                DataType::LargeUtf8 => array
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .unwrap()
                    .value(row)
                    .as_bytes(),
                DataType::Binary => array
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap()
                    .value(row),
                DataType::LargeBinary => array
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .unwrap()
                    .value(row),
                _ => unreachable!("four offset carriers only"),
            };
            Some(value.to_vec())
        })
        .collect()
}
