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
    Fixture, argument, author, compile_project_unit_with_parameters, fixture_constant_policy,
    project_port, unit_draft,
};
use crate::testkit::{add_call, checked_constants};
use arrow::{
    array::{ArrayRef, BooleanArray, StringArray},
    datatypes::DataType,
    record_batch::RecordBatch,
};
use novarocks_constant_contract::ConstantPools;
use novarocks_expression_contract::{ExprKind, RootScope, ValueId};
use novarocks_functions::{
    EngineFunctionCatalogBuilder, FunctionId, FunctionKind, FunctionOverloadId,
    InstalledPureKernel, PureEngineFunctionCatalog, PureImplementationDeclaration,
    PureImplementationId, PureKernelAbi,
};
use novarocks_type_contract::{
    ControlShape, DecimalOverflowPolicy, EvaluationDemand, FunctionValueType, SemanticParameters,
    TemporalSourceKind as Kind, TemporalSourceShape as Source,
};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Clone, Copy)]
pub enum Shape {
    PlainSec,
    PlainFormat,
    FormatOverride,
    Roundtrip,
    SparseIf,
    CastOther,
    SparseCastOther,
}
fn allow_parameter() -> novarocks_type_contract::SemanticParameterRef {
    novarocks_type_contract::SemanticParameterRef {
        id: novarocks_type_contract::SemanticParameterId::new(0),
        expected_key: novarocks_type_contract::SemanticParameterKey::AllowThrowException,
    }
}
fn catalogue() -> PureEngineFunctionCatalog {
    let actual =
        novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    let mut manifest = Vec::new();
    for (name, abi) in [
        ("time_to_sec", PureKernelAbi::ControlIntrinsicV1),
        ("time_format", PureKernelAbi::ControlIntrinsicV1),
        ("if", PureKernelAbi::ControlIntrinsicV1),
        ("sec_to_time", PureKernelAbi::ScalarV1),
    ] {
        let definition = actual.definition(name, FunctionKind::Scalar).unwrap();
        builder.register(definition.clone()).unwrap();
        for signature in definition.canonical_signatures() {
            manifest.push(InstalledPureKernel {
                function: FunctionId::try_new(format!("builtin.scalar/{name}/v1")).unwrap(),
                kind: FunctionKind::Scalar,
                implementation: PureImplementationDeclaration {
                    overload: FunctionOverloadId::try_new(format!(
                        "builtin.scalar/{name}/{signature}"
                    ))
                    .unwrap(),
                    implementation: PureImplementationId::try_new(format!(
                        "builtin.scalar/{name}/selected-v1"
                    ))
                    .unwrap(),
                    abi,
                },
                aggregate_state_format: None,
            });
        }
    }
    builder.seal_pure(manifest).unwrap()
}
pub fn program(fixture: Shape, dtype: DataType) -> Fixture {
    let mut source_pools = ConstantPools::empty();
    let functions = catalogue();
    let value = ValueId::new(901);
    let format_value = ValueId::new(3);
    let flag = ValueId::new(71);
    let ty = FunctionValueType::new(dtype.clone(), true);
    let string = FunctionValueType::new(DataType::Utf8, true);
    let boolean = FunctionValueType::new(DataType::Boolean, true);
    let mut builder = unit_draft();
    let port = project_port(
        [
            (value, ty.clone()),
            (format_value, string.clone()),
            (flag, boolean.clone()),
        ],
        &mut source_pools,
    );
    let source_expr = builder
        .add(None, ty.clone(), ExprKind::Value(value))
        .unwrap();
    let format_expr = if matches!(fixture, Shape::PlainFormat | Shape::FormatOverride) {
        Some(
            builder
                .add(None, string.clone(), ExprKind::Value(format_value))
                .unwrap(),
        )
    } else {
        None
    };
    let mut authors = BTreeMap::new();
    let mut normal = source_expr;
    let normal_type;
    let (name, shape) = match fixture {
        Shape::PlainSec | Shape::SparseIf => {
            normal_type = ty.clone();
            ("time_to_sec", Source::SecondsDirect)
        }
        Shape::CastOther | Shape::SparseCastOther => {
            // The frozen raw source-error baseline uses an identity cast. A
            // Date32-to-timestamp cast can panic before this source phase.
            normal_type = ty.clone();
            normal = builder
                .add(
                    None,
                    normal_type.clone(),
                    ExprKind::Cast {
                        array_failure_mode: None,
                        expr: source_expr,
                        target: normal_type.data_type().clone(),
                        decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                        allow_throw_exception: allow_parameter(),
                    },
                )
                .unwrap();
            ("time_to_sec", Source::SecondsCastOther)
        }
        Shape::PlainFormat => {
            normal_type = ty.clone();
            ("time_format", Source::FormatOrdinary)
        }
        Shape::FormatOverride => {
            normal_type = FunctionValueType::new(
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                true,
            );
            normal = builder
                .add(
                    None,
                    normal_type.clone(),
                    ExprKind::Cast {
                        array_failure_mode: None,
                        expr: source_expr,
                        target: normal_type.data_type().clone(),
                        decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                        allow_throw_exception: allow_parameter(),
                    },
                )
                .unwrap();
            ("time_format", Source::FormatUtf8Override)
        }
        Shape::Roundtrip => {
            normal_type = string.clone();
            normal = add_call(
                &mut builder,
                &mut authors,
                author(
                    &functions,
                    "sec_to_time",
                    vec![argument(ty.clone(), None)],
                    ControlShape::Eager,
                ),
                vec![source_expr],
            );
            // The unsupported transformed TIME child must stay undemanded.
            normal = builder
                .add(
                    None,
                    FunctionValueType::new(
                        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                        true,
                    ),
                    ExprKind::Cast {
                        array_failure_mode: None,
                        expr: normal,
                        target: DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                        decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                        allow_throw_exception: allow_parameter(),
                    },
                )
                .unwrap();
            ("time_to_sec", Source::SecondsRoundtrip)
        }
    };
    let mut arguments = vec![argument(
        if matches!(fixture, Shape::Roundtrip) {
            FunctionValueType::new(
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                true,
            )
        } else {
            normal_type
        },
        None,
    )];
    let mut args = vec![normal];
    if shape.kind() == Kind::TimeFormat {
        arguments.push(argument(string, None));
        args.push(format_expr.expect("format fixture has its reachable source"));
    }
    let mut root_expr = add_call(
        &mut builder,
        &mut authors,
        author(
            &functions,
            name,
            arguments,
            ControlShape::TemporalSource(shape),
        ),
        args,
    );
    if matches!(fixture, Shape::SparseIf | Shape::SparseCastOther) {
        let flag_expr = builder
            .add(None, boolean.clone(), ExprKind::Value(flag))
            .unwrap();
        let integer = FunctionValueType::new(DataType::Int64, true);
        let null = builder
            .add(
                None,
                integer.clone(),
                checked_constants::kind(
                    &mut source_pools,
                    checked_constants::null(&(integer.clone()), fixture_constant_policy()),
                ),
            )
            .unwrap();
        root_expr = add_call(
            &mut builder,
            &mut authors,
            author(
                &functions,
                "if",
                vec![
                    argument(boolean, None),
                    argument(integer.clone(), None),
                    argument(integer, None),
                ],
                ControlShape::If,
            ),
            vec![flag_expr, root_expr, null],
        );
    }
    let output_root = builder
        .root(root_expr, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let parameters = if matches!(
        fixture,
        Shape::PlainSec | Shape::PlainFormat | Shape::SparseIf
    ) {
        SemanticParameters::try_new([]).unwrap()
    } else {
        SemanticParameters::try_new([(
            novarocks_type_contract::SemanticParameterId::new(0),
            novarocks_type_contract::SemanticParameterValue::AllowThrowException(false),
        )])
        .unwrap()
    };
    let unit = compile_project_unit_with_parameters(
        builder,
        port,
        &functions,
        &authors,
        source_pools,
        &parameters,
    );
    Fixture {
        unit,
        root: output_root,
    }
}
pub fn batch(
    program: &Fixture,
    values: ArrayRef,
    formats: Vec<Option<&str>>,
    flags: Vec<Option<bool>>,
) -> RecordBatch {
    RecordBatch::try_new(
        program.input_schema().clone(),
        vec![
            values,
            Arc::new(StringArray::from(formats)),
            Arc::new(BooleanArray::from(flags)),
        ],
    )
    .unwrap()
}
