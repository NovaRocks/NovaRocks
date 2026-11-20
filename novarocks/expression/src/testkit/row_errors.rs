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

//! Shared checked ROUND and RAND roots for row error and required-result tests.
use super::{
    ClaimAuthor, add_call as call, checked_constants, declare_call_controls, finish_unit,
    register_call_requests,
};
use crate::{
    CompiledExprSet, CompiledExpressionInstance, EvaluationEnvironment, ScopeColumn, ScopeLayout,
    ScopeLayouts, compile_expr_set_pure,
};
use arrow::{
    array::{Decimal128Array, Int64Array},
    datatypes::DataType,
    record_batch::RecordBatch,
};
use novarocks_constant_contract::ConstantPools;
use novarocks_expression_contract::{
    BoundExprSetBuilder, ExprKind, ExprPort, RootOrdinal, RootScope, UnitLimits, ValueId,
};
use novarocks_functions::{
    ConstantPolicy, ConstantValue, EngineFunctionCatalogBuilder, FunctionArgument, FunctionId,
    FunctionKind, FunctionOverloadId, InstalledPureKernel, KernelEvaluationControl, KernelFailure,
    PureEngineFunctionCatalog, PureImplementationDeclaration, PureImplementationId, PureKernelAbi,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, EvaluationDemand, FunctionValueType,
    MAX_CONTROL_DEPTH, PureCompileControl, SemanticParameters, ValueTypeStorageLifetime,
};
use std::{collections::BTreeMap, sync::Arc, time::Duration};

pub struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("ROUND must not wait")
    }
}

fn catalogue() -> PureEngineFunctionCatalog {
    let actual =
        novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    for name in ["round", "rand"] {
        builder
            .register(
                actual
                    .definition(name, FunctionKind::Scalar)
                    .unwrap()
                    .clone(),
            )
            .unwrap();
    }
    // This is an explicitly sealed two-family test subset, not Server coverage.
    let mut manifest = vec![];
    for (name, overloads) in [
        ("round", vec!["builtin.scalar/round/dynamic-v1"]),
        (
            "rand",
            vec![
                "builtin.scalar/rand/()->f64;strict;legacy",
                "builtin.scalar/rand/(i64)->f64;strict;legacy",
            ],
        ),
    ] {
        for overload in overloads {
            manifest.push(InstalledPureKernel {
                function: FunctionId::try_new(format!("builtin.scalar/{name}/v1")).unwrap(),
                kind: FunctionKind::Scalar,
                implementation: PureImplementationDeclaration {
                    overload: FunctionOverloadId::try_new(overload).unwrap(),
                    implementation: PureImplementationId::try_new(format!(
                        "builtin.scalar/{name}/selected-v1"
                    ))
                    .unwrap(),
                    abi: PureKernelAbi::ScalarV1,
                },
                aggregate_state_format: None,
            });
        }
    }
    builder.seal_pure(manifest).unwrap()
}

fn author(
    functions: &PureEngineFunctionCatalog,
    name: &str,
    arguments: Vec<FunctionArgument>,
) -> ClaimAuthor {
    ClaimAuthor::with_exact_eager_binding(functions, name, arguments, constant_policy(), &Control)
}
fn constant_policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 16,
        max_array_nodes: 128,
        max_logical_elements: 1024,
        max_retained_buffer_bytes: 1 << 20,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 64,
        max_metadata_bytes: 1 << 20,
        max_library_validation_work: 1 << 20,
        max_library_validation_bytes: 1 << 20,
    }
}
fn integer_constant(ty: &FunctionValueType, value: i64) -> ConstantValue {
    ConstantValue::from_i64(
        Arc::new(ty.try_to_field("fixture").unwrap()),
        ty.clone(),
        value,
        constant_policy(),
        CompilePhase::FunctionSpecialization,
        &Control,
    )
    .unwrap()
}
fn integer_argument(ty: FunctionValueType, value: i64) -> FunctionArgument {
    let constant = integer_constant(&ty, value);
    argument(ty, Some(constant))
}
fn argument(ty: FunctionValueType, constant: Option<ConstantValue>) -> FunctionArgument {
    FunctionArgument::Value {
        value_type: ty,
        constant,
    }
}
#[derive(Clone, Copy)]
pub enum Shape {
    Single,
    Nested,
    ObservableSibling,
}

pub struct Program {
    pub unit: Arc<CompiledExprSet>,
    pub roots: Vec<RootOrdinal>,
}

pub fn program(shape: Shape) -> Program {
    let mut constants = ConstantPools::empty();
    let functions = catalogue();
    let value = ValueId::new(333);
    let digits_value = ValueId::new(901);
    let decimal = FunctionValueType::new(DataType::Decimal128(38, 0), true);
    let integer = FunctionValueType::new(DataType::Int64, true);
    let port_values = vec![(value, decimal.clone()), (digits_value, integer.clone())];
    let mut builder = BoundExprSetBuilder::new(UnitLimits {
        expression_semantic_depth: MAX_CONTROL_DEPTH,
    });
    let mut authors = BTreeMap::new();
    let first = if matches!(shape, Shape::ObservableSibling) {
        let ty = FunctionValueType::new(DataType::Int64, false);
        let seed = builder
            .add(
                None,
                ty.clone(),
                checked_constants::kind(
                    &mut constants,
                    checked_constants::i64(&ty, 42, constant_policy()),
                ),
            )
            .unwrap();
        call(
            &mut builder,
            &mut authors,
            author(&functions, "rand", vec![integer_argument(ty, 42)]),
            vec![seed],
        )
    } else {
        builder
            .add(None, decimal.clone(), ExprKind::Value(value))
            .unwrap()
    };
    let source_type = if matches!(shape, Shape::ObservableSibling) {
        authors[&first].result()
    } else {
        decimal.clone()
    };
    let digit = builder
        .add(
            None,
            integer.clone(),
            if matches!(shape, Shape::ObservableSibling) {
                ExprKind::Value(digits_value)
            } else {
                checked_constants::kind(
                    &mut constants,
                    checked_constants::i64(&integer, -1, constant_policy()),
                )
            },
        )
        .unwrap();
    let first_round = call(
        &mut builder,
        &mut authors,
        author(
            &functions,
            "round",
            vec![
                argument(source_type, None),
                argument(
                    integer.clone(),
                    if matches!(shape, Shape::ObservableSibling) {
                        None
                    } else {
                        Some(integer_constant(&integer, -1))
                    },
                ),
            ],
        ),
        vec![first, digit],
    );
    let root = if matches!(shape, Shape::Nested) {
        let ty = authors[&first_round].result();
        let zero = builder
            .add(
                None,
                integer.clone(),
                checked_constants::kind(
                    &mut constants,
                    checked_constants::i64(&integer, 0, constant_policy()),
                ),
            )
            .unwrap();
        call(
            &mut builder,
            &mut authors,
            author(
                &functions,
                "round",
                vec![argument(ty, None), integer_argument(integer, 0)],
            ),
            vec![first_round, zero],
        )
    } else {
        first_round
    };
    let mut roots = vec![
        builder
            .root(root, EvaluationDemand::Value, RootScope::Port(0))
            .unwrap(),
    ];
    if matches!(shape, Shape::ObservableSibling) {
        roots.push(
            builder
                .root(first, EvaluationDemand::Value, RootScope::Port(0))
                .unwrap(),
        );
    }
    register_call_requests(&mut builder, &authors, &mut constants);
    let parameters = SemanticParameters::try_new([]).unwrap();
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Validate).unwrap();
    declare_call_controls(
        &mut builder,
        &functions,
        &constants,
        &parameters,
        constant_policy(),
        &mut work,
    );
    let sealed = builder
        .seal(
            vec![ExprPort {
                values: port_values.clone().into_boxed_slice(),
            }],
            &mut work,
        )
        .unwrap();
    // These fixed definitions were authored after their actual children.
    for invocation in sealed.uses().values() {
        for child in invocation.arguments.iter() {
            assert!(sealed.uses()[child].definition.get() < invocation.definition.get());
        }
    }
    let unit = finish_unit(
        &functions,
        sealed,
        &authors,
        &constants,
        &parameters,
        constant_policy(),
        &mut work,
    );
    work.finish().unwrap();
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::LowerProgram).unwrap();
    let schema = Arc::new(arrow::datatypes::Schema::new(
        port_values
            .iter()
            .enumerate()
            .map(|(ordinal, (_, ty))| ty.try_to_field(format!("input_{ordinal}")).unwrap())
            .collect::<Vec<_>>(),
    ));
    let scope = ScopeLayout::try_new(
        schema,
        port_values.iter().map(|(_, ty)| ty.clone()).collect(),
        port_values
            .iter()
            .enumerate()
            .map(|(ordinal, (value, ty))| ScopeColumn {
                value: *value,
                ordinal: u32::try_from(ordinal).unwrap(),
                ty: ty.clone(),
            })
            .collect(),
        ValueTypeStorageLifetime::pure(),
        &mut work,
    )
    .unwrap();
    let compiled = compile_expr_set_pure(
        &unit,
        EvaluationEnvironment {
            constants: &constants,
            parameters: &parameters,
        },
        ScopeLayouts {
            ports: &[scope],
            all_ports: None,
        },
        constant_policy(),
        &functions,
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    Program {
        unit: compiled,
        roots,
    }
}
pub fn instance(program: &Program, expression: usize) -> CompiledExpressionInstance {
    CompiledExpressionInstance::try_new(program.unit.clone(), program.roots[expression], &Control)
        .unwrap()
}
pub fn batch(
    program: &Program,
    values: Vec<Option<i128>>,
    digits: Vec<Option<i64>>,
) -> RecordBatch {
    let root = program.unit.root(program.roots[0]).unwrap();
    RecordBatch::try_new(
        program.unit.scope(root.scope).unwrap().schema().clone(),
        vec![
            Arc::new(
                Decimal128Array::from(values)
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            ),
            Arc::new(Int64Array::from(digits)),
        ],
    )
    .unwrap()
}
