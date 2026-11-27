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

//! Original hidden value conversion bindings through the checked unit compiler.

use super::{
    Fixture, Shape, argument, catalogue, compile_project_unit, fixture_constant_policy,
    project_port, unit_draft,
};
use crate::testkit::{ClaimAuthor, HarnessControl, add_call};
use arrow::datatypes::DataType;
use novarocks_constant_contract::ConstantPools;
use novarocks_expression_contract::{BoundExprSetBuilder, ExprId, ExprKind, RootScope, ValueId};
use novarocks_functions::{
    EngineFunctionCatalogBuilder, FunctionBindingError, FunctionBindingRequest, FunctionId,
    FunctionKind, FunctionOverloadId, FunctionResultType, InstalledPureKernel,
    PureEngineFunctionCatalog, PureImplementationDeclaration, PureImplementationId, PureKernelAbi,
    builtin::value_conversion::{
        ARRAY_COMMON_COERCION, VALUE_CONVERSION_NAME, resolve_array_common_coercion_binding,
        value_conversion_definition,
    },
};
use novarocks_type_contract::{
    ControlShape, EvaluationDemand, FunctionValueType, ValueLogicalType,
};
use std::collections::BTreeMap;

fn conversion_catalogue() -> PureEngineFunctionCatalog {
    let actual = catalogue(Shape::Decimal);
    let mut builder = EngineFunctionCatalogBuilder::new();
    let mut manifest = Vec::new();
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
        ("round", &["dynamic-v1"][..], PureKernelAbi::ScalarV1),
    ] {
        builder
            .register(
                actual
                    .metadata()
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
    builder
        .register(value_conversion_definition().unwrap())
        .unwrap();
    // This independent installed manifest covers every implementation
    // of the original hidden definition, without claiming a Server catalogue.
    for stem in [
        "array_common_coercion",
        "operator_decimal_float_coercion",
        "opaque_to_binary_same_structure",
        "signed_to_largeint",
        "boolean_to_largeint",
        "decimal128_to_largeint",
        "decimal256_to_largeint",
        "float32_to_largeint_null_invalid",
        "float64_to_largeint_null_invalid",
        "largeint_to_signed_null_overflow",
        "largeint_to_decimal_exact",
        "largeint_to_float_round",
        "largeint_to_utf8_text",
        "utf8_to_largeint_null_invalid",
        "null_to_typed_nullable",
        "temporal",
        "temporal_widen",
        "temporal_zone",
        "temporal_clock",
        "try_temporal",
        "try_temporal_zone",
        "try_temporal_clock",
        "nested_temporal_write_assignment",
    ] {
        manifest.push(InstalledPureKernel {
            function: FunctionId::try_new("builtin.scalar/value_domain_conversion/v1").unwrap(),
            kind: FunctionKind::Scalar,
            implementation: PureImplementationDeclaration {
                overload: FunctionOverloadId::try_new(format!(
                    "builtin.scalar/value_domain_conversion/{stem}/v1"
                ))
                .unwrap(),
                implementation: PureImplementationId::try_new(format!(
                    "builtin.scalar/value_domain_conversion/{stem}/selected-v1"
                ))
                .unwrap(),
                abi: PureKernelAbi::ScalarV1,
            },
            aggregate_state_format: None,
        });
    }
    builder.seal_pure(manifest).unwrap()
}

pub fn nominal(carrier: DataType, domain: ValueLogicalType) -> FunctionValueType {
    FunctionValueType::try_with_logical_type(carrier, true, domain).unwrap()
}
pub fn largeint_type() -> FunctionValueType {
    nominal(DataType::FixedSizeBinary(16), ValueLogicalType::LargeInt)
}

// Recreate the original source port and compile the original neutral Value root.
pub fn conversion_program(
    columns: &[FunctionValueType],
    build: impl FnOnce(
        &PureEngineFunctionCatalog,
        &mut BoundExprSetBuilder,
        &mut BTreeMap<ExprId, ClaimAuthor>,
        &[ExprId],
        &mut ConstantPools,
    ) -> ExprId,
) -> Fixture {
    let mut source_pools = ConstantPools::empty();
    let functions = conversion_catalogue();
    let mut builder = unit_draft();
    let values = columns
        .iter()
        .enumerate()
        .map(|(ordinal, ty)| {
            assert!(ty.nullable());
            (
                ValueId::new(901 + u32::try_from(ordinal).unwrap() * 71),
                ty.clone(),
            )
        })
        .collect::<Vec<_>>();
    let port = project_port(values.clone(), &mut source_pools);
    let leaves = columns
        .iter()
        .zip(values)
        .map(|(ty, (value, _))| {
            builder
                .add(None, ty.clone(), ExprKind::input(value))
                .unwrap()
        })
        .collect::<Vec<_>>();
    let mut authors = BTreeMap::new();
    let expression = build(
        &functions,
        &mut builder,
        &mut authors,
        &leaves,
        &mut source_pools,
    );
    let output_root = builder
        .root(expression, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    Fixture {
        unit: compile_project_unit(builder, port, &functions, &authors, source_pools),
        root: output_root,
    }
}

pub fn converted(
    functions: &PureEngineFunctionCatalog,
    builder: &mut BoundExprSetBuilder,
    authors: &mut BTreeMap<ExprId, ClaimAuthor>,
    value: ExprId,
    source: FunctionValueType,
    target: FunctionValueType,
) -> ExprId {
    add_call(
        builder,
        authors,
        ClaimAuthor::with_legacy_binding(
            functions,
            VALUE_CONVERSION_NAME,
            vec![argument(source, None)],
            ControlShape::Eager,
            Some(target),
            fixture_constant_policy(),
            &HarnessControl,
        ),
        vec![value],
    )
}
pub fn direct_program(source: FunctionValueType, target: FunctionValueType) -> Fixture {
    conversion_program(
        std::slice::from_ref(&source),
        |functions, builder, authors, leaves, _source_pools| {
            converted(
                functions,
                builder,
                authors,
                leaves[0],
                source.clone(),
                target,
            )
        },
    )
}

/// Freeze an explicitly selected ARRAY common coercion in the real fixture unit.
pub fn array_common_coerced(
    functions: &PureEngineFunctionCatalog,
    builder: &mut BoundExprSetBuilder,
    authors: &mut BTreeMap<ExprId, ClaimAuthor>,
    value: ExprId,
    source: FunctionValueType,
    target: FunctionValueType,
) -> ExprId {
    let arguments = vec![argument(source, None)];
    let metadata = functions.metadata();
    assert!(matches!(
        metadata.resolve_bound_trusted(
            VALUE_CONVERSION_NAME,
            FunctionKind::Scalar,
            FunctionBindingRequest {
                arguments: &arguments,
                logical_argument_count: 1,
                expected_result_type: Some(&target),
            },
            &HarnessControl,
        ),
        Err(FunctionBindingError::NoMatchingOverload)
    ));
    let bound =
        resolve_array_common_coercion_binding(metadata, &arguments[0], &target, &HarnessControl)
            .unwrap();
    assert_eq!(bound.selected.overload.as_str(), ARRAY_COMMON_COERCION);
    assert_eq!(
        bound.selected.result_type,
        FunctionResultType::Scalar(target.clone())
    );
    add_call(
        builder,
        authors,
        ClaimAuthor::from_resolved_legacy_binding(
            bound,
            arguments,
            ControlShape::Eager,
            Some(target),
            fixture_constant_policy(),
        ),
        vec![value],
    )
}
