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

//! Shared neutral units for guarded expression fixtures.

use super::{
    ClaimAuthor, HarnessControl, add_call, checked_constants, compile_fixture_unit,
    declare_call_controls, finish_unit, fixture_scope, register_call_requests, register_constant,
};
use crate::{
    CompiledExprSet, CompiledExpressionInstance, EvaluationEnvironment, ScopeLayout, ScopeLayouts,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use novarocks_constant_contract::{ConstantPolicy, ConstantPools, ConstantValue};
use novarocks_expression_contract::{
    BoundExprSetBuilder, ExprId, ExprKind, ExprPort, RootOrdinal, RootScope, UnitLimits, ValueId,
};
use novarocks_functions::{
    EngineFunctionCatalogBuilder, FunctionArgument, FunctionId, FunctionKind, FunctionOverloadId,
    InstalledPureKernel, KernelDiagnostic, KernelEvaluationControl, KernelFailure,
    PureEngineFunctionCatalog, PureImplementationDeclaration, PureImplementationId, PureKernelAbi,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, ControlShape, EvaluationDemand, FunctionValueType,
    SemanticParameters,
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

pub mod byte;
pub mod case;
pub mod equality;
pub mod nary;
pub mod ordered;
pub mod temporal;
pub mod unary;
pub mod value_conversion;

pub struct Fixture {
    pub unit: Arc<CompiledExprSet>,
    pub root: RootOrdinal,
}

impl Fixture {
    pub fn input_schema(&self) -> &SchemaRef {
        self.unit.scope(RootScope::Port(0)).unwrap().schema()
    }

    pub fn instance(&self, control: &dyn KernelEvaluationControl) -> CompiledExpressionInstance {
        CompiledExpressionInstance::try_new(Arc::clone(&self.unit), self.root, control).unwrap()
    }
}

pub fn unit_draft() -> BoundExprSetBuilder {
    // Keep the frozen depth used to author the former physical fixture unit.
    BoundExprSetBuilder::new(UnitLimits {
        expression_semantic_depth: 256,
    })
}

#[derive(Clone, Copy)]
pub enum Shape {
    Random,
    Decimal,
}
pub fn catalogue(shape: Shape) -> PureEngineFunctionCatalog {
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
            &["()->f64;strict;legacy", "(i64)->f64;strict;legacy"][..],
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
                        "builtin.scalar/{name}/selected-v1"
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

pub fn fixture_constant_policy() -> ConstantPolicy {
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

pub fn author(
    functions: &PureEngineFunctionCatalog,
    name: &str,
    arguments: Vec<FunctionArgument>,
    shape: ControlShape,
) -> ClaimAuthor {
    ClaimAuthor::with_legacy_binding(
        functions,
        name,
        arguments,
        shape,
        None,
        fixture_constant_policy(),
        &HarnessControl,
    )
}

pub fn argument(ty: FunctionValueType, constant: Option<ConstantValue>) -> FunctionArgument {
    FunctionArgument::Value {
        value_type: ty,
        constant,
    }
}

fn integer_argument(ty: FunctionValueType, value: i64) -> FunctionArgument {
    let constant = checked_constants::i64(&ty, value, fixture_constant_policy());
    argument(ty, Some(constant))
}

fn literal(
    builder: &mut BoundExprSetBuilder,
    ty: FunctionValueType,
    value: ConstantValue,
    source_pools: &mut ConstantPools,
) -> ExprId {
    builder
        .add(None, ty, checked_constants::kind(source_pools, value))
        .unwrap()
}

fn random(
    builder: &mut BoundExprSetBuilder,
    authors: &mut BTreeMap<ExprId, ClaimAuthor>,
    functions: &PureEngineFunctionCatalog,
    source_pools: &mut ConstantPools,
) -> ExprId {
    let integer = FunctionValueType::new(DataType::Int64, false);
    let seed = literal(
        builder,
        integer.clone(),
        checked_constants::i64(&integer, 42, fixture_constant_policy()),
        source_pools,
    );
    add_call(
        builder,
        authors,
        author(
            functions,
            "rand",
            vec![integer_argument(integer, 42)],
            ControlShape::Eager,
        ),
        vec![seed],
    )
}

pub fn round_value(
    builder: &mut BoundExprSetBuilder,
    authors: &mut BTreeMap<ExprId, ClaimAuthor>,
    functions: &PureEngineFunctionCatalog,
    value: ExprId,
    source_pools: &mut ConstantPools,
) -> ExprId {
    let integer = FunctionValueType::new(DataType::Int64, false);
    let digits = literal(
        builder,
        integer.clone(),
        checked_constants::i64(&integer, -1, fixture_constant_policy()),
        source_pools,
    );
    add_call(
        builder,
        authors,
        author(
            functions,
            "round",
            vec![
                argument(
                    FunctionValueType::new(DataType::Decimal128(38, 0), true),
                    None,
                ),
                integer_argument(integer, -1),
            ],
            ControlShape::Eager,
        ),
        vec![value, digits],
    )
}

fn project_port(
    columns: impl Into<Box<[(ValueId, FunctionValueType)]>>,
    source_pools: &mut ConstantPools,
) -> ExprPort {
    let values = columns.into();
    // Preserve the original source Project's typed NULL pool prefix so root
    // literal IDs and original request references retain their actual ordinals.
    for (_, ty) in values.iter() {
        register_constant(
            source_pools,
            &checked_constants::null(ty, fixture_constant_policy()),
        );
    }
    ExprPort { values }
}

fn compile_project_unit(
    draft: BoundExprSetBuilder,
    port: ExprPort,
    functions: &PureEngineFunctionCatalog,
    authors: &BTreeMap<ExprId, ClaimAuthor>,
    constants: ConstantPools,
) -> Arc<CompiledExprSet> {
    compile_project_unit_with_parameters(
        draft,
        port,
        functions,
        authors,
        constants,
        &SemanticParameters::try_new([]).unwrap(),
    )
}

fn compile_project_unit_with_parameters(
    draft: BoundExprSetBuilder,
    port: ExprPort,
    functions: &PureEngineFunctionCatalog,
    authors: &BTreeMap<ExprId, ClaimAuthor>,
    constants: ConstantPools,
    parameters: &SemanticParameters,
) -> Arc<CompiledExprSet> {
    let fields = port
        .values
        .iter()
        .enumerate()
        .map(|(ordinal, (_, ty))| {
            Field::new(
                format!("local_1_{ordinal}"),
                ty.typed_binding().layout().data_type().clone(),
                ty.nullable(),
            )
        })
        .collect::<Vec<_>>();
    let scope = fixture_scope(&port, Arc::new(Schema::new(fields)));
    compile_project_unit_with_layout(
        draft, port, scope, functions, authors, constants, parameters,
    )
}

fn compile_project_unit_with_layout(
    mut draft: BoundExprSetBuilder,
    port: ExprPort,
    scope: ScopeLayout,
    functions: &PureEngineFunctionCatalog,
    authors: &BTreeMap<ExprId, ClaimAuthor>,
    mut constants: ConstantPools,
    parameters: &SemanticParameters,
) -> Arc<CompiledExprSet> {
    register_call_requests(&mut draft, authors, &mut constants);
    let mut work = CompileCheckpoints::try_new(&HarnessControl, CompilePhase::Validate).unwrap();
    declare_call_controls(&mut draft, authors, &mut work);
    let sealed = draft.seal(vec![port], &mut work).unwrap();
    let unit = finish_unit(
        functions,
        sealed,
        authors,
        parameters,
        &mut work,
        |_, _, children| children,
    );
    work.finish().unwrap();
    compile_fixture_unit(
        &unit,
        EvaluationEnvironment {
            constants: &constants,
            parameters,
        },
        ScopeLayouts {
            ports: std::slice::from_ref(&scope),
            all_ports: Some(&scope),
        },
        fixture_constant_policy(),
        functions,
    )
}

#[derive(Clone, Copy)]
pub struct GuardedControl(&'static str);

pub const GUARDED_CONTROL: GuardedControl = GuardedControl("guarded numeric calls must not wait");
pub const NARY_CONTROL: GuardedControl = GuardedControl("intrinsic Boolean roots must not wait");

impl KernelEvaluationControl for GuardedControl {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("{}", self.0)
    }
}

pub struct CallbackControl {
    cause: KernelFailure,
    stop_at: usize,
    pub trace: Mutex<Vec<u32>>,
    refused: Mutex<bool>,
    waiting: GuardedControl,
}

impl CallbackControl {
    pub fn new(cause: KernelFailure, stop_at: usize, waiting: GuardedControl) -> Self {
        Self {
            cause,
            stop_at,
            trace: Mutex::new(vec![]),
            refused: Mutex::new(false),
            waiting,
        }
    }
}

impl KernelEvaluationControl for CallbackControl {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        let mut refused = self.refused.lock().unwrap();
        assert!(!*refused, "no callback after primary refusal");
        let mut trace = self.trace.lock().unwrap();
        trace.push(units);
        if trace.len() == self.stop_at {
            *refused = true;
            Err(self.cause.clone())
        } else {
            Ok(())
        }
    }

    fn wait(&self, duration: Duration) -> Result<(), KernelFailure> {
        self.waiting.wait(duration)
    }
}

pub fn causes(owner: &str) -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(KernelDiagnostic::new(&format!("{owner}-invalid"))),
        KernelFailure::Internal(KernelDiagnostic::new(&format!("{owner}-internal"))),
        KernelFailure::Operational(KernelDiagnostic::new(&format!("{owner}-operational"))),
        KernelFailure::InstanceFailed,
    ]
}

// Frozen independent rand 0.8.5 StdRng seed_from_u64 oracle, not evaluator output.
pub const SEED_42: [u64; 6] = [
    0x3fe0d98eec6444e4,
    0x3fe15e014267f5aa,
    0x3fe45dec0e3bca26,
    0x3fd9fa4b5e3f5d8c,
    0x3fa19561bff02330,
    0x3fda8ea728e783e0,
];
pub fn guarded_program(shape: Shape) -> Fixture {
    let mut source_pools = ConstantPools::empty();
    let functions = catalogue(shape);
    let flag = ValueId::new(901);
    let seed = ValueId::new(3);
    let decimal_value = ValueId::new(71);
    let fallback = ValueId::new(72);
    let bool_type = FunctionValueType::new(DataType::Boolean, true);
    let integer = FunctionValueType::new(DataType::Int64, true);
    let scalar_integer = FunctionValueType::new(DataType::Int64, false);
    let floating = FunctionValueType::new(DataType::Float64, true);
    let decimal = FunctionValueType::new(DataType::Decimal128(38, 0), true);
    let mut builder = unit_draft();
    let mut columns = vec![(flag, bool_type.clone()), (seed, integer.clone())];
    if matches!(shape, Shape::Decimal) {
        columns.extend([
            (decimal_value, decimal.clone()),
            (fallback, decimal.clone()),
        ]);
    }
    let port = project_port(columns, &mut source_pools);
    let mut authors = BTreeMap::new();
    let flag_expr = builder
        .add(None, bool_type.clone(), ExprKind::Value(flag))
        .unwrap();
    let root = if matches!(shape, Shape::Random) {
        let seed_expr = builder
            .add(None, integer.clone(), ExprKind::Value(seed))
            .unwrap();
        let from_column = add_call(
            &mut builder,
            &mut authors,
            author(
                &functions,
                "rand",
                vec![argument(integer, None)],
                ControlShape::Eager,
            ),
            vec![seed_expr],
        );
        let null = builder
            .add(
                None,
                floating.clone(),
                checked_constants::kind(
                    &mut source_pools,
                    checked_constants::null(&(floating.clone()), fixture_constant_policy()),
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
                    argument(floating.clone(), None),
                    argument(floating.clone(), None),
                ],
                ControlShape::If,
            ),
            vec![flag_expr, from_column, null],
        );
        let literal_seed = builder
            .add(
                None,
                scalar_integer.clone(),
                checked_constants::kind(
                    &mut source_pools,
                    checked_constants::i64(
                        &(scalar_integer.clone()),
                        42,
                        fixture_constant_policy(),
                    ),
                ),
            )
            .unwrap();
        let from_constant = add_call(
            &mut builder,
            &mut authors,
            author(
                &functions,
                "rand",
                vec![integer_argument(scalar_integer, 42)],
                ControlShape::Eager,
            ),
            vec![literal_seed],
        );
        add_call(
            &mut builder,
            &mut authors,
            author(
                &functions,
                "coalesce",
                vec![argument(floating.clone(), None), argument(floating, None)],
                ControlShape::Coalesce,
            ),
            vec![conditional, from_constant],
        )
    } else {
        let value = builder
            .add(None, decimal.clone(), ExprKind::Value(decimal_value))
            .unwrap();
        let fallback_expr = builder
            .add(None, decimal.clone(), ExprKind::Value(fallback))
            .unwrap();
        let digits = builder
            .add(
                None,
                scalar_integer.clone(),
                checked_constants::kind(
                    &mut source_pools,
                    checked_constants::i64(
                        &(scalar_integer.clone()),
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
                    argument(decimal.clone(), None),
                    integer_argument(scalar_integer, -1),
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
    let root = builder
        .root(root, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let unit = compile_project_unit(builder, port, &functions, &authors, source_pools);
    Fixture { unit, root }
}
