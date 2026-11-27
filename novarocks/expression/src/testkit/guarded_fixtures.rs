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
use novarocks_constant_contract::{ConstantPools, ConstantValidation, ConstantValue};
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

pub fn fixture_constant_policy() -> ConstantValidation {
    novarocks_constant_contract::ConstantValidation::bounded(
        novarocks_constant_contract::ConstantPolicy {
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
        },
    )
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
    let integer = FunctionValueType::new(DataType::Int64, true);
    let bound = checked_constants::i64(&integer, 1, fixture_constant_policy());
    let input = literal(builder, integer.clone(), bound.clone(), source_pools);
    let sample = add_call(
        builder,
        authors,
        author(
            functions,
            "rand",
            vec![argument(integer, Some(bound))],
            ControlShape::Eager,
        ),
        vec![input],
    );
    builder
        .add(
            None,
            FunctionValueType::new(DataType::Float64, true),
            ExprKind::Cast {
                cast_mode: novarocks_type_contract::TrinoCastMode::Explicit,
                expr: sample,
                target: DataType::Float64,
            },
        )
        .unwrap()
}

pub fn round_value(
    builder: &mut BoundExprSetBuilder,
    authors: &mut BTreeMap<ExprId, ClaimAuthor>,
    functions: &PureEngineFunctionCatalog,
    value: ExprId,
    source_pools: &mut ConstantPools,
) -> ExprId {
    let integer = FunctionValueType::new(DataType::Int32, false);
    let places = ConstantValue::from_i32(
        Arc::new(integer.try_to_field("places").unwrap()),
        integer.clone(),
        -1,
        fixture_constant_policy(),
        CompilePhase::FunctionSpecialization,
        &HarnessControl,
    )
    .unwrap();
    let digits = literal(builder, integer.clone(), places.clone(), source_pools);
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
                argument(integer, Some(places)),
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
    declare_call_controls(&mut draft, functions, &constants, parameters, &mut work);
    let sealed = draft.seal(vec![port], &mut work).unwrap();
    let unit = finish_unit(
        functions, sealed, authors, &constants, parameters, &mut work,
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

pub fn guarded_program(shape: Shape) -> Fixture {
    let mut source_pools = ConstantPools::empty();
    let functions = catalogue(shape);
    let flag = ValueId::new(901);
    let bound = ValueId::new(3);
    let decimal_value = ValueId::new(71);
    let fallback = ValueId::new(72);
    let bool_type = FunctionValueType::new(DataType::Boolean, true);
    let integer = FunctionValueType::new(DataType::Int64, true);
    let scalar_integer = FunctionValueType::new(DataType::Int64, false);
    let decimal = FunctionValueType::new(DataType::Decimal128(38, 0), true);
    let mut builder = unit_draft();
    let mut columns = vec![(flag, bool_type.clone()), (bound, integer.clone())];
    if matches!(shape, Shape::Decimal) {
        columns.extend([
            (decimal_value, decimal.clone()),
            (fallback, decimal.clone()),
        ]);
    }
    let port = project_port(columns, &mut source_pools);
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
                    checked_constants::null(&(integer.clone()), fixture_constant_policy()),
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
                    checked_constants::i64(&(scalar_integer.clone()), 1, fixture_constant_policy()),
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
            fixture_constant_policy(),
            CompilePhase::FunctionSpecialization,
            &HarnessControl,
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
    let root = builder
        .root(root, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let unit = compile_project_unit(builder, port, &functions, &authors, source_pools);
    Fixture { unit, root }
}
