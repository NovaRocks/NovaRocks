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

use super::{Control, FrameFixture, POLICY, draft, finish_frame};
use crate::testkit::{ClaimAuthor, add_call, checked_constants};
use arrow::{
    array::{BooleanArray, Decimal128Array, Int64Array},
    datatypes::DataType,
    record_batch::RecordBatch,
};
use novarocks_constant_contract::ConstantPools;
use novarocks_expression_contract::ExprKind;
use novarocks_expression_contract::ValueId;
use novarocks_functions::{
    ConstantValue, EngineFunctionCatalogBuilder, FunctionArgument, FunctionId, FunctionKind,
    FunctionOverloadId, InstalledPureKernel, PureEngineFunctionCatalog,
    PureImplementationDeclaration, PureImplementationId, PureKernelAbi,
};
use novarocks_type_contract::{
    CompilePhase, ControlShape, EvaluationDemand, FunctionValueType, SemanticParameters,
};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Clone, Copy)]
pub enum Shape {
    Random,
    Decimal,
}
fn catalogue(shape: Shape) -> PureEngineFunctionCatalog {
    let actual =
        novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    let mut manifest = vec![];
    for (name, overloads, abi) in [
        (
            "if",
            &["(bool,any<T>,any<T>)->any<T>;widen;legacy"][..],
            PureKernelAbi::ControlIntrinsicV1,
        ),
        (
            "coalesce",
            &["(any<T>...)->any<T>;widen;legacy"][..],
            PureKernelAbi::ControlIntrinsicV1,
        ),
        (
            "rand",
            &[
                "()->f64;strict;legacy",
                "(i8)->i8;strict;coerce",
                "(i16)->i16;strict;coerce",
                "(i32)->i32;strict;coerce",
                "(i64)->i64;strict;coerce",
            ][..],
            PureKernelAbi::ScalarV1,
        ),
        ("round", &["dynamic-v1"][..], PureKernelAbi::ScalarV1),
    ] {
        if name == "round" && matches!(shape, Shape::Random) {
            continue;
        }
        builder
            .register(
                actual
                    .definition(name, FunctionKind::Scalar)
                    .unwrap()
                    .clone(),
            )
            .unwrap();
        let implementation = if name == "round" {
            "selected-v2"
        } else {
            "selected-v1"
        };
        for overload in overloads {
            manifest.push(InstalledPureKernel {
                function: FunctionId::try_new(format!("builtin.scalar/{name}/v1")).unwrap(),
                kind: FunctionKind::Scalar,
                implementation: PureImplementationDeclaration {
                    overload: FunctionOverloadId::try_new(format!(
                        "builtin.scalar/{name}/{overload}"
                    ))
                    .unwrap(),
                    implementation: PureImplementationId::try_new(format!(
                        "builtin.scalar/{name}/{implementation}"
                    ))
                    .unwrap(),
                    abi,
                },
                aggregate_state_format: None,
            });
        }
    }
    // Independent exact installed records seal only these actual test owners.
    builder.seal_pure(manifest).unwrap()
}
fn integer_constant(ty: &FunctionValueType, value: i64) -> ConstantValue {
    ConstantValue::from_i64(
        Arc::new(ty.try_to_field("fixture").unwrap()),
        ty.clone(),
        value,
        POLICY,
        CompilePhase::FunctionSpecialization,
        &Control,
    )
    .unwrap()
}
fn integer_argument(ty: FunctionValueType, value: i64) -> FunctionArgument {
    let constant = integer_constant(&ty, value);
    argument(ty, Some(constant))
}
pub(super) fn argument(ty: FunctionValueType, constant: Option<ConstantValue>) -> FunctionArgument {
    FunctionArgument::Value {
        value_type: ty,
        constant,
    }
}
pub(super) fn author(
    functions: &PureEngineFunctionCatalog,
    name: &str,
    arguments: Vec<FunctionArgument>,
    shape: ControlShape,
) -> ClaimAuthor {
    author_with_target(functions, name, arguments, shape, None)
}

fn author_with_target(
    functions: &PureEngineFunctionCatalog,
    name: &str,
    arguments: Vec<FunctionArgument>,
    shape: ControlShape,
    expected_result_type: Option<FunctionValueType>,
) -> ClaimAuthor {
    ClaimAuthor::with_legacy_binding(
        functions,
        name,
        arguments,
        shape,
        expected_result_type,
        POLICY,
        &Control,
    )
}

pub fn program(shape: Shape) -> FrameFixture {
    let mut source_pools = ConstantPools::empty();
    let functions = catalogue(shape);
    let mut builder = draft();
    let flag = ValueId::new(901);
    let bound = ValueId::new(3);
    let decimal_value = ValueId::new(71);
    let fallback = ValueId::new(72);
    let bool_type = FunctionValueType::new(DataType::Boolean, true);
    let integer = FunctionValueType::new(DataType::Int64, true);
    let scalar_integer = FunctionValueType::new(DataType::Int64, false);
    let decimal = FunctionValueType::new(DataType::Decimal128(38, 0), true);
    let mut columns = vec![(flag, bool_type.clone()), (bound, integer.clone())];
    if matches!(shape, Shape::Decimal) {
        columns.extend([
            (decimal_value, decimal.clone()),
            (fallback, decimal.clone()),
        ]);
    }
    // The checked producer source pools preceded the consumer constants.
    for (_, ty) in &columns {
        checked_constants::kind(&mut source_pools, checked_constants::null(ty, POLICY));
    }
    let mut authors = BTreeMap::new();
    let flag_expr = builder
        .add(None, bool_type.clone(), ExprKind::input(flag))
        .unwrap();
    let root = if matches!(shape, Shape::Random) {
        let bound_expr = builder
            .add(None, integer.clone(), ExprKind::input(bound))
            .unwrap();
        let from_column = add_call(
            &mut builder,
            &mut authors,
            author(
                &functions,
                "rand",
                vec![argument(integer.clone(), None)],
                ControlShape::Eager,
            ),
            vec![bound_expr],
        );
        let null = builder
            .add(
                None,
                integer.clone(),
                checked_constants::kind(
                    &mut source_pools,
                    checked_constants::null(&(integer.clone()), POLICY),
                ),
            )
            .unwrap();
        let conditional = add_call(
            &mut builder,
            &mut authors,
            author(
                &functions,
                "if",
                vec![
                    argument(bool_type, None),
                    argument(integer.clone(), None),
                    argument(integer.clone(), None),
                ],
                ControlShape::If,
            ),
            vec![flag_expr, from_column, null],
        );
        let literal_bound = builder
            .add(
                None,
                scalar_integer.clone(),
                checked_constants::kind(
                    &mut source_pools,
                    checked_constants::i64(&(scalar_integer.clone()), 1, POLICY),
                ),
            )
            .unwrap();
        let from_constant = add_call(
            &mut builder,
            &mut authors,
            author(
                &functions,
                "rand",
                vec![integer_argument(scalar_integer.clone(), 1)],
                ControlShape::Eager,
            ),
            vec![literal_bound],
        );
        add_call(
            &mut builder,
            &mut authors,
            author(
                &functions,
                "coalesce",
                vec![
                    argument(integer.clone(), None),
                    argument(scalar_integer, None),
                ],
                ControlShape::Coalesce,
            ),
            vec![conditional, from_constant],
        )
    } else {
        let value = builder
            .add(None, decimal.clone(), ExprKind::input(decimal_value))
            .unwrap();
        let fallback_expr = builder
            .add(None, decimal.clone(), ExprKind::input(fallback))
            .unwrap();
        let digit_type = FunctionValueType::new(DataType::Int32, false);
        let places = ConstantValue::from_i32(
            Arc::new(digit_type.try_to_field("places").unwrap()),
            digit_type.clone(),
            -1,
            POLICY,
            CompilePhase::FunctionSpecialization,
            &Control,
        )
        .unwrap();
        let digits = builder
            .add(
                None,
                digit_type.clone(),
                checked_constants::kind(&mut source_pools, places.clone()),
            )
            .unwrap();
        let rounded = add_call(
            &mut builder,
            &mut authors,
            author(
                &functions,
                "round",
                vec![
                    argument(decimal.clone(), None),
                    argument(digit_type, Some(places)),
                ],
                ControlShape::Eager,
            ),
            vec![value, digits],
        );
        let coalesced = add_call(
            &mut builder,
            &mut authors,
            author(
                &functions,
                "coalesce",
                vec![
                    argument(decimal.clone(), None),
                    argument(decimal.clone(), None),
                ],
                ControlShape::Coalesce,
            ),
            vec![rounded, fallback_expr],
        );
        add_call(
            &mut builder,
            &mut authors,
            author(
                &functions,
                "if",
                vec![
                    argument(bool_type, None),
                    argument(decimal.clone(), None),
                    argument(decimal, None),
                ],
                ControlShape::If,
            ),
            vec![flag_expr, coalesced, fallback_expr],
        )
    };
    finish_frame(
        &functions,
        builder,
        (root, EvaluationDemand::Value),
        columns,
        SemanticParameters::try_new([]).unwrap(),
        source_pools,
        authors,
    )
}

pub fn batch(
    program: &FrameFixture,
    flags: Vec<Option<bool>>,
    bounds: Vec<Option<i64>>,
) -> RecordBatch {
    RecordBatch::try_new(
        program.schema.clone(),
        vec![
            Arc::new(BooleanArray::from(flags)),
            Arc::new(Int64Array::from(bounds)),
        ],
    )
    .unwrap()
}
pub fn decimal_batch(
    program: &FrameFixture,
    flags: Vec<Option<bool>>,
    values: Vec<Option<i128>>,
    fallback: Vec<Option<i128>>,
) -> RecordBatch {
    let rows = flags.len();
    RecordBatch::try_new(
        program.schema.clone(),
        vec![
            Arc::new(BooleanArray::from(flags)),
            Arc::new(Int64Array::from(vec![Some(42); rows])),
            Arc::new(
                Decimal128Array::from(values)
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            ),
            Arc::new(
                Decimal128Array::from(fallback)
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            ),
        ],
    )
    .unwrap()
}
