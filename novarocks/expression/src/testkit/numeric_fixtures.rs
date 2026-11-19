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
    ClaimAuthor, HarnessControl, add_call, checked_constants, declare_call_controls, finish_unit,
    fixture_scope, register_call_requests, register_constant, try_finish_unit,
};
use crate::{
    CompiledExprSet, EvaluationEnvironment, ExprCompileError, ScopeLayout, ScopeLayouts,
    StaticExprKind, compile_expr_set_pure,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, i256};
use novarocks_constant_contract::{ConstantPolicy, ConstantPools, ConstantValue};
use novarocks_expression_contract::{
    BinaryOperator, BoundExprSet, BoundExprSetBuilder, ExprId, ExprKind, ExprPort, RootOrdinal,
    RootScope, UnitLimits, ValueId,
};
use novarocks_failure_contract::{FailureClass, FailureProjection};
use novarocks_functions::{
    CastOperation, EngineFunctionCatalogBuilder, FunctionArgument, FunctionId, FunctionKind,
    FunctionOverloadId, InstalledPureKernel, PureEngineFunctionCatalog,
    PureImplementationDeclaration, PureImplementationId, PureKernelAbi,
};
use novarocks_type_contract::{
    ArithmeticOperator, CompileCheckpoints, CompilePhase, ControlShape, DecimalOverflowPolicy,
    EvaluationDemand, FunctionValueType, PureCompileControl, SemanticParameterId,
    SemanticParameterKey, SemanticParameterRef, SemanticParameterValue, SemanticParameters,
    ValueLogicalType, arithmetic_result_value_type_with_op,
};
use std::{collections::BTreeMap, sync::Arc};

fn catalogue() -> PureEngineFunctionCatalog {
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
fn author(
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

fn argument(ty: FunctionValueType, constant: Option<ConstantValue>) -> FunctionArgument {
    FunctionArgument::Value {
        value_type: ty,
        constant,
    }
}
fn integer_argument(ty: FunctionValueType, value: i64) -> FunctionArgument {
    let constant = checked_constants::i64(&ty, value, fixture_constant_policy());
    argument(ty, Some(constant))
}

/// The target unit and its original root ordinal, with its actual Arrow scope.
#[derive(Debug)]
pub struct CompiledFixture {
    pub unit: Arc<CompiledExprSet>,
    pub root: RootOrdinal,
}
impl CompiledFixture {
    pub fn input_schema(&self) -> &SchemaRef {
        self.unit.scope(RootScope::Port(0)).unwrap().schema()
    }
}

fn unit_draft() -> BoundExprSetBuilder {
    // Match the original frozen source PlanLimits::FROZEN expression depth.
    BoundExprSetBuilder::new(UnitLimits {
        expression_semantic_depth: 256,
    })
}
fn named_single_port(unit: &BoundExprSet, names: &[&str]) -> ScopeLayout {
    let [port] = unit.ports() else {
        panic!("these fixtures each author one declared input port");
    };
    assert_eq!(port.values.len(), names.len());
    let fields = port
        .values
        .iter()
        .zip(names)
        .map(|((_, ty), name)| {
            // The original Project field uses the plain canonical carrier.
            Field::new(
                *name,
                ty.typed_binding().layout().data_type().clone(),
                ty.nullable(),
            )
        })
        .collect::<Vec<_>>();
    fixture_scope(port, Arc::new(Schema::new(fields)))
}

struct SinglePortFixture {
    input: ExprPort,
    source_prefix: Option<BoundExprSetBuilder>,
    unit: BoundExprSetBuilder,
    root: RootOrdinal,
    functions: PureEngineFunctionCatalog,
    authors: BTreeMap<ExprId, ClaimAuthor>,
    constants: ConstantPools,
    allow: bool,
}

pub struct PreparedFixture {
    unit: BoundExprSet,
    source_prefix: Option<BoundExprSet>,
    root: RootOrdinal,
    functions: PureEngineFunctionCatalog,
    constants: ConstantPools,
    parameters: SemanticParameters,
}

impl SinglePortFixture {
    fn prepare(self) -> Result<PreparedFixture, crate::CallAuthorError> {
        let Self {
            input,
            source_prefix,
            mut unit,
            root,
            functions,
            authors,
            mut constants,
            allow,
        } = self;
        let parameters = SemanticParameters::try_new([(
            allow_ref().id,
            SemanticParameterValue::AllowThrowException(allow),
        )])
        .unwrap();
        register_call_requests(&mut unit, &authors, &mut constants);
        let mut work =
            CompileCheckpoints::try_new(&HarnessControl, CompilePhase::Validate).unwrap();
        declare_call_controls(
            &mut unit,
            &functions,
            &constants,
            &parameters,
            fixture_constant_policy(),
            &mut work,
        );

        // Preserve the checked source unit when its cast recipes were observed.
        // Its declared Port(0) is empty; the output unit sees the actual typed shape.
        let source_prefix = match source_prefix {
            Some(source) => {
                let sealed = source
                    .seal(
                        vec![ExprPort {
                            values: Box::default(),
                        }],
                        &mut work,
                    )
                    .unwrap();
                Some(try_finish_unit(
                    &functions,
                    sealed,
                    &BTreeMap::new(),
                    &constants,
                    &parameters,
                    fixture_constant_policy(),
                    &mut work,
                )?)
            }
            None => None,
        };
        let sealed = unit.seal(vec![input], &mut work).unwrap();
        let unit = try_finish_unit(
            &functions,
            sealed,
            &authors,
            &constants,
            &parameters,
            fixture_constant_policy(),
            &mut work,
        )?;
        work.finish().unwrap();
        Ok(PreparedFixture {
            unit,
            source_prefix,
            root,
            functions,
            constants,
            parameters,
        })
    }
}

impl PreparedFixture {
    fn compile_unit(
        &self,
        unit: &BoundExprSet,
        field_names: &[&str],
        control: &dyn PureCompileControl,
    ) -> Result<Arc<CompiledExprSet>, ExprCompileError> {
        let environment = EvaluationEnvironment {
            constants: &self.constants,
            parameters: &self.parameters,
        };
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
        let result = {
            let port = named_single_port(unit, field_names);
            compile_expr_set_pure(
                unit,
                environment,
                ScopeLayouts {
                    ports: std::slice::from_ref(&port),
                    all_ports: Some(&port),
                },
                fixture_constant_policy(),
                &self.functions,
                &mut work,
            )
        };
        // Mirrors the enclosing LocalCompiler's ordinary-error footer; an
        // existing control refusal is already primary and cannot be rechecked.
        if let Err(error) = &result
            && (error.is_primary_construction()
                || matches!(
                    error.failure_fact().class(),
                    FailureClass::Control | FailureClass::Resource
                ))
        {
            return result;
        }
        work.finish()?;
        result
    }

    pub fn compile(
        &self,
        control: &dyn PureCompileControl,
    ) -> Result<CompiledFixture, ExprCompileError> {
        let names = &LOCAL_PROJECT_FIELDS[..self.unit.ports()[0].values.len()];
        Ok(CompiledFixture {
            unit: self.compile_unit(&self.unit, names, control)?,
            root: self.root,
        })
    }
}
const LOCAL_PROJECT_FIELDS: [&str; 3] = ["local_1_0", "local_1_1", "local_1_2"];

#[derive(Clone, Copy, Debug)]
pub enum Wrap {
    Bare,
    IsNull,
    Coalesce,
    If,
    And,
    Or,
    NullParent,
    ConstantRight,
}
pub fn operation(op: BinaryOperator) -> ArithmeticOperator {
    match op {
        BinaryOperator::Add => ArithmeticOperator::Add,
        BinaryOperator::Subtract => ArithmeticOperator::Subtract,
        BinaryOperator::Multiply => ArithmeticOperator::Multiply,
        BinaryOperator::Divide => ArithmeticOperator::Divide,
        BinaryOperator::Modulo => ArithmeticOperator::Modulo,
        _ => panic!("exact five arithmetic operators only"),
    }
}
fn allow_ref() -> SemanticParameterRef {
    SemanticParameterRef {
        id: SemanticParameterId::new(u32::MAX),
        expected_key: SemanticParameterKey::AllowThrowException,
    }
}
fn literal(
    builder: &mut BoundExprSetBuilder,
    ty: &FunctionValueType,
    value: novarocks_functions::ConstantValue,
    source_pools: &mut ConstantPools,
) -> ExprId {
    builder
        .add(
            None,
            ty.clone(),
            checked_constants::kind(source_pools, value),
        )
        .unwrap()
}
fn arithmetic(
    builder: &mut BoundExprSetBuilder,
    op: BinaryOperator,
    left: ExprId,
    right: ExprId,
    result: &FunctionValueType,
) -> ExprId {
    builder
        .add(
            None,
            result.clone(),
            ExprKind::Binary {
                op,
                left,
                right,
                decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                allow_throw_exception: Some(allow_ref()),
            },
        )
        .unwrap()
}

#[derive(Clone, Copy)]
pub enum Source {
    Column,
    Constant,
    Add,
    Round,
    RandCondition,
}
fn result_literal(
    builder: &mut BoundExprSetBuilder,
    result: &FunctionValueType,
    policy: DecimalOverflowPolicy,
    source_pools: &mut ConstantPools,
) -> ExprId {
    if matches!(result.data_type(), DataType::Timestamp(_, None)) {
        return literal(
            builder,
            result,
            checked_constants::timestamp(result, 71, fixture_constant_policy()),
            source_pools,
        );
    }
    if *result.data_type() == DataType::Int64 {
        return literal(
            builder,
            result,
            checked_constants::i64(result, 71, fixture_constant_policy()),
            source_pools,
        );
    }
    let source = FunctionValueType::new(DataType::Int64, false);
    let value = literal(
        builder,
        &source,
        checked_constants::i64(&source, 71, fixture_constant_policy()),
        source_pools,
    );
    builder
        .add(
            None,
            result.clone(),
            ExprKind::Cast {
                expr: value,
                target: result.data_type().clone(),
                decimal_overflow_policy: policy,
                allow_throw_exception: allow_ref(),
            },
        )
        .unwrap()
}

pub fn cast_fixture(
    source_type: FunctionValueType,
    result_type: FunctionValueType,
    source: Source,
    wrap: Wrap,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> Result<PreparedFixture, crate::CallAuthorError> {
    let mut source_pools = ConstantPools::empty();
    let functions = catalogue();
    let boolean = FunctionValueType::new(DataType::Boolean, true);
    let seed_type = FunctionValueType::new(DataType::Int64, true);
    let columns = [
        (ValueId::new(91), source_type.clone()),
        (ValueId::new(7), seed_type.clone()),
        (ValueId::new(333), boolean.clone()),
    ];
    let mut source_prefix = unit_draft();
    let mut builder = unit_draft();
    for (_, ty) in &columns {
        let definition = if !ty.nullable() && *ty.data_type() == DataType::Float32 {
            let exact = source_prefix
                .add(
                    None,
                    (FunctionValueType::new(DataType::Float64, false)).clone(),
                    checked_constants::kind(
                        &mut source_pools,
                        checked_constants::f64_bits(
                            &(FunctionValueType::new(DataType::Float64, false)),
                            0.0_f64.to_bits(),
                            fixture_constant_policy(),
                        ),
                    ),
                )
                .unwrap();
            source_prefix
                .add(
                    None,
                    ty.clone(),
                    ExprKind::Cast {
                        expr: exact,
                        target: DataType::Float32,
                        decimal_overflow_policy: policy,
                        allow_throw_exception: allow_ref(),
                    },
                )
                .unwrap()
        } else {
            let value = if ty.nullable() {
                checked_constants::null(ty, fixture_constant_policy())
            } else {
                match ty.data_type() {
                    DataType::Binary => checked_constants::binary(
                        ty,
                        &(b"native producer".to_vec().into_boxed_slice()),
                        fixture_constant_policy(),
                    ),
                    DataType::Decimal128(..) => {
                        checked_constants::decimal128(ty, 0, fixture_constant_policy())
                    }
                    DataType::Decimal256(..) => checked_constants::decimal256(
                        ty,
                        i256::ZERO.to_be_bytes(),
                        fixture_constant_policy(),
                    ),
                    DataType::Boolean => {
                        checked_constants::boolean(ty, false, fixture_constant_policy())
                    }
                    DataType::Date32 => checked_constants::date32(ty, 0, fixture_constant_policy()),
                    DataType::FixedSizeBinary(16)
                        if ty.logical_type()
                            == novarocks_type_contract::ValueLogicalType::LargeInt =>
                    {
                        checked_constants::largeint(ty, 0, fixture_constant_policy())
                    }
                    DataType::Int64 => checked_constants::i64(ty, 0, fixture_constant_policy()),
                    DataType::UInt64 => checked_constants::u64(ty, 0, fixture_constant_policy()),
                    DataType::Timestamp(_, None) => {
                        checked_constants::timestamp(ty, 0, fixture_constant_policy())
                    }
                    DataType::Float64 => checked_constants::f64_bits(
                        ty,
                        0.0_f64.to_bits(),
                        fixture_constant_policy(),
                    ),
                    _ => {
                        panic!("nonnullable fixture requires an implemented exact numeric literal")
                    }
                }
            };
            source_prefix
                .add(
                    None,
                    ty.clone(),
                    checked_constants::kind(&mut source_pools, value),
                )
                .unwrap()
        };
        source_prefix
            .root(definition, EvaluationDemand::Value, RootScope::Port(0))
            .unwrap();
    }
    let mut authors = BTreeMap::new();
    let mut child = if matches!(source, Source::Constant) {
        if *source_type.data_type() == DataType::Float32 {
            let exact = literal(
                &mut builder,
                &FunctionValueType::new(DataType::Float64, false),
                checked_constants::f64_bits(
                    &FunctionValueType::new(DataType::Float64, false),
                    (-0.0_f64).to_bits(),
                    fixture_constant_policy(),
                ),
                &mut source_pools,
            );
            builder
                .add(
                    None,
                    source_type.clone(),
                    ExprKind::Cast {
                        expr: exact,
                        target: DataType::Float32,
                        decimal_overflow_policy: policy,
                        allow_throw_exception: allow_ref(),
                    },
                )
                .unwrap()
        } else {
            let value = match source_type.data_type() {
                DataType::Binary => checked_constants::binary(
                    &source_type,
                    &(b"native\0producer".to_vec().into_boxed_slice()),
                    fixture_constant_policy(),
                ),
                DataType::Decimal128(..) => {
                    checked_constants::decimal128(&source_type, 1, fixture_constant_policy())
                }
                DataType::Decimal256(..) => checked_constants::decimal256(
                    &source_type,
                    i256::from_i128(1).to_be_bytes(),
                    fixture_constant_policy(),
                ),
                DataType::Boolean => {
                    checked_constants::boolean(&source_type, true, fixture_constant_policy())
                }
                DataType::Date32 => {
                    checked_constants::date32(&source_type, 71, fixture_constant_policy())
                }
                DataType::FixedSizeBinary(16)
                    if source_type.logical_type()
                        == novarocks_type_contract::ValueLogicalType::LargeInt =>
                {
                    checked_constants::largeint(&source_type, i128::MIN, fixture_constant_policy())
                }
                DataType::Int64 => {
                    checked_constants::i64(&source_type, 71, fixture_constant_policy())
                }
                DataType::UInt64 => {
                    checked_constants::u64(&source_type, u64::MAX, fixture_constant_policy())
                }
                DataType::Timestamp(_, None) => {
                    checked_constants::timestamp(&source_type, 71, fixture_constant_policy())
                }
                DataType::Float64 => checked_constants::f64_bits(
                    &source_type,
                    (-0.0_f64).to_bits(),
                    fixture_constant_policy(),
                ),
                _ => panic!("constant fixture requires an implemented exact numeric literal"),
            };
            literal(&mut builder, &source_type, value, &mut source_pools)
        }
    } else {
        builder
            .add(None, source_type.clone(), ExprKind::Value(columns[0].0))
            .unwrap()
    };
    if matches!(source, Source::Add) {
        let rhs = literal(
            &mut builder,
            &source_type,
            checked_constants::i64(&source_type, 1, fixture_constant_policy()),
            &mut source_pools,
        );
        child = arithmetic(&mut builder, BinaryOperator::Add, child, rhs, &source_type);
    }
    if matches!(source, Source::Round) {
        child = add_call(
            &mut builder,
            &mut authors,
            author(
                &functions,
                "round",
                vec![argument(source_type.clone(), None)],
                ControlShape::Eager,
            ),
            vec![child],
        );
        assert_eq!(authors[&child].result(), source_type);
    }
    let cast = builder
        .add(
            None,
            result_type.clone(),
            ExprKind::Cast {
                expr: child,
                target: result_type.data_type().clone(),
                decimal_overflow_policy: policy,
                allow_throw_exception: allow_ref(),
            },
        )
        .unwrap();
    let expression = match wrap {
        Wrap::Bare | Wrap::ConstantRight => cast,
        Wrap::Coalesce | Wrap::If => {
            let fallback = result_literal(&mut builder, &result_type, policy, &mut source_pools);
            if matches!(wrap, Wrap::Coalesce) {
                add_call(
                    &mut builder,
                    &mut authors,
                    author(
                        &functions,
                        "coalesce",
                        vec![
                            argument(result_type.clone(), None),
                            argument(result_type.clone(), None),
                        ],
                        ControlShape::Coalesce,
                    ),
                    vec![cast, fallback],
                )
            } else {
                let flag = if matches!(source, Source::RandCondition) {
                    let seed = builder
                        .add(None, seed_type.clone(), ExprKind::Value(columns[1].0))
                        .unwrap();
                    let random = add_call(
                        &mut builder,
                        &mut authors,
                        author(
                            &functions,
                            "rand",
                            vec![argument(seed_type, None)],
                            ControlShape::Eager,
                        ),
                        vec![seed],
                    );
                    builder
                        .add(
                            None,
                            FunctionValueType::new(DataType::Boolean, false),
                            ExprKind::IsNull {
                                expr: random,
                                negated: true,
                            },
                        )
                        .unwrap()
                } else {
                    builder
                        .add(None, boolean.clone(), ExprKind::Value(columns[2].0))
                        .unwrap()
                };
                let flag_type = if matches!(source, Source::RandCondition) {
                    FunctionValueType::new(DataType::Boolean, false)
                } else {
                    boolean.clone()
                };
                add_call(
                    &mut builder,
                    &mut authors,
                    author(
                        &functions,
                        "if",
                        vec![
                            argument(flag_type, None),
                            argument(result_type.clone(), None),
                            argument(result_type.clone(), None),
                        ],
                        ControlShape::If,
                    ),
                    vec![flag, cast, fallback],
                )
            }
        }
        Wrap::IsNull | Wrap::And | Wrap::Or => {
            let root_type = FunctionValueType::new(DataType::Boolean, false);
            let is_null = builder
                .add(
                    None,
                    root_type.clone(),
                    ExprKind::IsNull {
                        expr: cast,
                        negated: false,
                    },
                )
                .unwrap();
            if matches!(wrap, Wrap::IsNull) {
                is_null
            } else {
                let decide = literal(
                    &mut builder,
                    &root_type,
                    checked_constants::boolean(
                        &root_type,
                        matches!(wrap, Wrap::Or),
                        fixture_constant_policy(),
                    ),
                    &mut source_pools,
                );
                builder
                    .add(
                        None,
                        root_type.clone(),
                        if matches!(wrap, Wrap::Or) {
                            ExprKind::Disjunction {
                                args: Box::from([decide, is_null]),
                            }
                        } else {
                            ExprKind::Conjunction {
                                args: Box::from([decide, is_null]),
                            }
                        },
                    )
                    .unwrap()
            }
        }
        Wrap::NullParent => panic!("this fixture has no null arithmetic parent"),
    };
    let expression = builder
        .root(expression, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    SinglePortFixture {
        input: ExprPort {
            values: Box::from(columns),
        },
        source_prefix: Some(source_prefix),
        unit: builder,
        root: expression,
        functions,
        authors,
        constants: source_pools,
        allow,
    }
    .prepare()
}
pub fn compiled(
    source: FunctionValueType,
    result: FunctionValueType,
    mode: Source,
    wrap: Wrap,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> CompiledFixture {
    let fixture = cast_fixture(source.clone(), result.clone(), mode, wrap, policy, allow)
        .expect("cast fixture admitted by the call author");
    let source_unit = fixture
        .compile_unit(
            fixture.source_prefix.as_ref().unwrap(),
            &[],
            &HarnessControl,
        )
        .unwrap();
    let program = fixture.compile(&HarnessControl).unwrap();
    let mut count = 0;
    for unit in [&source_unit, &program.unit] {
        for (&id, invocation) in unit.control_flow().uses() {
            let definition = unit.definitions().node(invocation.definition).unwrap();
            if matches!(definition.kind(), StaticExprKind::PreparedCast { .. }) {
                let recipe = unit.recipes().cast(id).unwrap();
                assert_eq!(recipe.operation(), CastOperation::Carrier);
                // A narrowed ELSE is itself an authored I64 -> target Cast.
                // Identify the primary occurrence by both complete source facts.
                let primary = recipe.source_type() == &source && recipe.result_type() == &result;
                assert_eq!(recipe.decimal_overflow_policy(), policy);
                assert_eq!(recipe.allow_throw_exception(), allow);
                count += usize::from(primary);
            }
        }
    }
    assert_eq!(count, 1);
    program
}
pub fn build_program(
    op: BinaryOperator,
    left_carrier: DataType,
    right_carrier: DataType,
    wrap: Wrap,
    allow: bool,
) -> CompiledFixture {
    let mut source_pools = ConstantPools::empty();
    let functions = catalogue();
    let left_type = FunctionValueType::new(left_carrier, true);
    let right_type = FunctionValueType::new(right_carrier, true);
    let boolean = FunctionValueType::new(DataType::Boolean, true);
    let columns = [
        (ValueId::new(91), left_type.clone()),
        (ValueId::new(7), right_type.clone()),
        (ValueId::new(333), boolean.clone()),
    ];
    // The source Project's checked nulls precede the actual target CVs.
    for (_, ty) in &columns {
        let value = checked_constants::null(ty, fixture_constant_policy());
        register_constant(&mut source_pools, &value);
    }
    let mut builder = unit_draft();
    let left = builder
        .add(None, left_type.clone(), ExprKind::Value(columns[0].0))
        .unwrap();
    let right = if matches!(wrap, Wrap::ConstantRight) {
        literal(
            &mut builder,
            &right_type,
            checked_constants::i64(&right_type, 2, fixture_constant_policy()),
            &mut source_pools,
        )
    } else {
        builder
            .add(None, right_type.clone(), ExprKind::Value(columns[1].0))
            .unwrap()
    };
    let mut result_type =
        arithmetic_result_value_type_with_op(&left_type, &right_type, operation(op)).unwrap();
    result_type = result_type
        .with_nullable(true)
        .expect("valid test root nullability");
    let computed = arithmetic(&mut builder, op, left, right, &result_type);
    let mut authors = BTreeMap::new();
    let root = match wrap {
        Wrap::Bare | Wrap::ConstantRight => computed,
        Wrap::NullParent => {
            let null = literal(
                &mut builder,
                &result_type,
                checked_constants::null(&result_type, fixture_constant_policy()),
                &mut source_pools,
            );
            arithmetic(
                &mut builder,
                BinaryOperator::Add,
                null,
                computed,
                &result_type,
            )
        }
        Wrap::Coalesce | Wrap::If => {
            let fallback = literal(
                &mut builder,
                &result_type,
                checked_constants::i64(&result_type, 71, fixture_constant_policy()),
                &mut source_pools,
            );
            let (name, shape, arguments, defs) = if matches!(wrap, Wrap::If) {
                let flag = builder
                    .add(None, boolean.clone(), ExprKind::Value(columns[2].0))
                    .unwrap();
                (
                    "if",
                    ControlShape::If,
                    vec![
                        argument(boolean, None),
                        argument(result_type.clone(), None),
                        argument(result_type.clone(), None),
                    ],
                    vec![flag, computed, fallback],
                )
            } else {
                (
                    "coalesce",
                    ControlShape::Coalesce,
                    vec![
                        argument(result_type.clone(), None),
                        argument(result_type.clone(), None),
                    ],
                    vec![computed, fallback],
                )
            };
            add_call(
                &mut builder,
                &mut authors,
                author(&functions, name, arguments, shape),
                defs,
            )
        }
        Wrap::IsNull | Wrap::And | Wrap::Or => {
            result_type = FunctionValueType::new(DataType::Boolean, false);
            let is_null = builder
                .add(
                    None,
                    result_type.clone(),
                    ExprKind::IsNull {
                        expr: computed,
                        negated: false,
                    },
                )
                .unwrap();
            if matches!(wrap, Wrap::IsNull) {
                is_null
            } else {
                let decide = literal(
                    &mut builder,
                    &result_type,
                    checked_constants::boolean(
                        &result_type,
                        matches!(wrap, Wrap::Or),
                        fixture_constant_policy(),
                    ),
                    &mut source_pools,
                );
                builder
                    .add(
                        None,
                        result_type.clone(),
                        if matches!(wrap, Wrap::Or) {
                            ExprKind::Disjunction {
                                args: Box::from([decide, is_null]),
                            }
                        } else {
                            ExprKind::Conjunction {
                                args: Box::from([decide, is_null]),
                            }
                        },
                    )
                    .unwrap()
            }
        }
    };
    let root = builder
        .root(root, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    SinglePortFixture {
        input: ExprPort {
            values: Box::from(columns),
        },
        source_prefix: None,
        unit: builder,
        root,
        functions,
        authors,
        constants: source_pools,
        allow,
    }
    .prepare()
    .unwrap()
    .compile(&HarnessControl)
    .unwrap()
}
fn scalar_literal(ty: &FunctionValueType, value: i128) -> novarocks_functions::ConstantValue {
    match (&ty.data_type(), ty.logical_type()) {
        (DataType::Decimal128(..), ValueLogicalType::Physical) => {
            checked_constants::decimal128(ty, value, fixture_constant_policy())
        }
        (DataType::Decimal256(..), ValueLogicalType::Physical) => checked_constants::decimal256(
            ty,
            i256::from_i128(value).to_be_bytes(),
            fixture_constant_policy(),
        ),
        (DataType::FixedSizeBinary(16), ValueLogicalType::LargeInt) => {
            checked_constants::largeint(ty, value, fixture_constant_policy())
        }
        (DataType::Int64, ValueLogicalType::Physical) => {
            checked_constants::i64(ty, i64::try_from(value).unwrap(), fixture_constant_policy())
        }
        _ => panic!("explicit numeric scalar fixture author"),
    }
}
pub fn typed_program(
    op: BinaryOperator,
    left_type: FunctionValueType,
    right_type: FunctionValueType,
    wrap: Wrap,
    allow: bool,
    policy: DecimalOverflowPolicy,
    right_literal: Option<novarocks_functions::ConstantValue>,
) -> CompiledFixture {
    let mut source_pools = ConstantPools::empty();
    let functions = catalogue();
    let boolean = FunctionValueType::new(DataType::Boolean, true);
    let columns = [
        (ValueId::new(91), left_type.clone()),
        (ValueId::new(7), right_type.clone()),
        (ValueId::new(333), boolean.clone()),
    ];
    // The source Project's checked nulls precede the actual target CVs.
    for (_, ty) in &columns {
        let value = checked_constants::null(ty, fixture_constant_policy());
        register_constant(&mut source_pools, &value);
    }
    let mut builder = unit_draft();
    let left = builder
        .add(None, left_type.clone(), ExprKind::Value(columns[0].0))
        .unwrap();
    let right = if let Some(value) = right_literal {
        literal(&mut builder, &right_type, value, &mut source_pools)
    } else {
        builder
            .add(None, right_type.clone(), ExprKind::Value(columns[1].0))
            .unwrap()
    };
    let mut result_type =
        arithmetic_result_value_type_with_op(&left_type, &right_type, operation(op)).unwrap();
    result_type = result_type
        .with_nullable(true)
        .expect("valid test root nullability");
    let computed = builder
        .add(
            None,
            result_type.clone(),
            ExprKind::Binary {
                op,
                left,
                right,
                decimal_overflow_policy: policy,
                allow_throw_exception: Some(allow_ref()),
            },
        )
        .unwrap();
    let mut authors = BTreeMap::new();
    let root = match wrap {
        Wrap::Bare | Wrap::ConstantRight => computed,
        Wrap::NullParent => panic!("numeric fixture has no untyped parent result rule"),
        Wrap::Coalesce | Wrap::If => {
            let fallback = literal(
                &mut builder,
                &result_type,
                scalar_literal(&result_type, 71),
                &mut source_pools,
            );
            let (name, shape, arguments, defs) = if matches!(wrap, Wrap::If) {
                let flag = builder
                    .add(None, boolean.clone(), ExprKind::Value(columns[2].0))
                    .unwrap();
                (
                    "if",
                    ControlShape::If,
                    vec![
                        argument(boolean, None),
                        argument(result_type.clone(), None),
                        argument(result_type.clone(), None),
                    ],
                    vec![flag, computed, fallback],
                )
            } else {
                (
                    "coalesce",
                    ControlShape::Coalesce,
                    vec![
                        argument(result_type.clone(), None),
                        argument(result_type.clone(), None),
                    ],
                    vec![computed, fallback],
                )
            };
            add_call(
                &mut builder,
                &mut authors,
                author(&functions, name, arguments, shape),
                defs,
            )
        }
        Wrap::IsNull | Wrap::And | Wrap::Or => {
            result_type = FunctionValueType::new(DataType::Boolean, false);
            let is_null = builder
                .add(
                    None,
                    result_type.clone(),
                    ExprKind::IsNull {
                        expr: computed,
                        negated: false,
                    },
                )
                .unwrap();
            if matches!(wrap, Wrap::IsNull) {
                is_null
            } else {
                let decide = literal(
                    &mut builder,
                    &result_type,
                    checked_constants::boolean(
                        &result_type,
                        matches!(wrap, Wrap::Or),
                        fixture_constant_policy(),
                    ),
                    &mut source_pools,
                );
                builder
                    .add(
                        None,
                        result_type.clone(),
                        if matches!(wrap, Wrap::Or) {
                            ExprKind::Disjunction {
                                args: Box::from([decide, is_null]),
                            }
                        } else {
                            ExprKind::Conjunction {
                                args: Box::from([decide, is_null]),
                            }
                        },
                    )
                    .unwrap()
            }
        }
    };
    let root = builder
        .root(root, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    SinglePortFixture {
        input: ExprPort {
            values: Box::from(columns),
        },
        source_prefix: None,
        unit: builder,
        root,
        functions,
        authors,
        constants: source_pools,
        allow,
    }
    .prepare()
    .unwrap()
    .compile(&HarnessControl)
    .unwrap()
}
pub fn inherited_round_fixture_with_target(allow: bool, target: DataType) -> CompiledFixture {
    let mut source_pools = ConstantPools::empty();
    let functions = catalogue();
    let float = FunctionValueType::new(DataType::Float64, true);
    let decimal = FunctionValueType::new(DataType::Decimal128(38, 0), true);
    let inputs = [
        (ValueId::new(91), float.clone()),
        (ValueId::new(7), decimal.clone()),
    ];
    for (_, ty) in &inputs {
        let value = checked_constants::null(ty, fixture_constant_policy());
        register_constant(&mut source_pools, &value);
    }
    let mut builder = unit_draft();
    let decimal_expr = builder
        .add(None, decimal.clone(), ExprKind::Value(inputs[1].0))
        .unwrap();
    let digits_type = FunctionValueType::new(DataType::Int64, false);
    let digits = builder
        .add(
            None,
            digits_type.clone(),
            checked_constants::kind(
                &mut source_pools,
                checked_constants::i64(&(digits_type.clone()), -1, fixture_constant_policy()),
            ),
        )
        .unwrap();
    let mut authors = BTreeMap::new();
    let round = add_call(
        &mut builder,
        &mut authors,
        author(
            &functions,
            "round",
            vec![argument(decimal, None), integer_argument(digits_type, -1)],
            ControlShape::Eager,
        ),
        vec![decimal_expr, digits],
    );
    let boolean = FunctionValueType::new(DataType::Boolean, false);
    let condition = builder
        .add(
            None,
            boolean.clone(),
            ExprKind::IsNull {
                expr: round,
                negated: false,
            },
        )
        .unwrap();
    let left = builder
        .add(None, float.clone(), ExprKind::Value(inputs[0].0))
        .unwrap();
    let right = builder
        .add(None, float.clone(), ExprKind::Value(inputs[0].0))
        .unwrap();
    let child = add_call(
        &mut builder,
        &mut authors,
        author(
            &functions,
            "if",
            vec![
                argument(boolean, None),
                argument(float.clone(), None),
                argument(float, None),
            ],
            ControlShape::If,
        ),
        vec![condition, left, right],
    );
    let result_type = FunctionValueType::new(target.clone(), true);
    let expression = builder
        .add(
            None,
            result_type.clone(),
            ExprKind::Cast {
                expr: child,
                target,
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                allow_throw_exception: allow_ref(),
            },
        )
        .unwrap();
    let expression = builder
        .root(expression, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    SinglePortFixture {
        input: ExprPort {
            values: Box::from(inputs),
        },
        source_prefix: None,
        unit: builder,
        root: expression,
        functions,
        authors,
        constants: source_pools,
        allow,
    }
    .prepare()
    .unwrap()
    .compile(&HarnessControl)
    .unwrap()
}

pub fn narrow_unsigned_constant(narrow: DataType) -> CompiledFixture {
    let mut constants = ConstantPools::empty();
    let mut unit = unit_draft();
    let wide = FunctionValueType::new(DataType::UInt64, false);
    let literal = literal(
        &mut unit,
        &wide,
        checked_constants::u64(&wide, 42, fixture_constant_policy()),
        &mut constants,
    );
    let inner = unit
        .add(
            None,
            FunctionValueType::new(narrow.clone(), true),
            ExprKind::Cast {
                expr: literal,
                target: narrow,
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                allow_throw_exception: allow_ref(),
            },
        )
        .unwrap();
    let output = unit
        .add(
            None,
            FunctionValueType::new(DataType::UInt64, true),
            ExprKind::Cast {
                expr: inner,
                target: DataType::UInt64,
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                allow_throw_exception: allow_ref(),
            },
        )
        .unwrap();
    let root = unit
        .root(output, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    SinglePortFixture {
        input: ExprPort {
            values: Box::default(),
        },
        source_prefix: None,
        unit,
        root,
        functions: catalogue(),
        authors: BTreeMap::new(),
        constants,
        allow: false,
    }
    .prepare()
    .unwrap()
    .compile(&HarnessControl)
    .unwrap()
}

#[derive(Clone, Copy)]
pub enum NullSafeRight {
    Column,
    Random,
    Round,
}

pub fn nullsafe_program(ty: FunctionValueType, right_kind: NullSafeRight) -> CompiledFixture {
    let mut source_pools = ConstantPools::empty();
    let functions = catalogue();
    let ids = [ValueId::new(903), ValueId::new(71)];
    for _ in ids {
        let value = checked_constants::null(&ty, fixture_constant_policy());
        register_constant(&mut source_pools, &value);
    }
    let mut builder = unit_draft();
    let mut authors = BTreeMap::new();
    let left = builder
        .add(None, ty.clone(), ExprKind::Value(ids[0]))
        .unwrap();
    let right = match right_kind {
        NullSafeRight::Column => builder
            .add(None, ty.clone(), ExprKind::Value(ids[1]))
            .unwrap(),
        NullSafeRight::Random => {
            let integer = FunctionValueType::new(DataType::Int64, false);
            let seed = literal(
                &mut builder,
                &integer,
                checked_constants::i64(&integer, 42, fixture_constant_policy()),
                &mut source_pools,
            );
            add_call(
                &mut builder,
                &mut authors,
                author(
                    &functions,
                    "rand",
                    vec![integer_argument(integer, 42)],
                    ControlShape::Eager,
                ),
                vec![seed],
            )
        }
        NullSafeRight::Round => {
            let source = builder
                .add(None, ty.clone(), ExprKind::Value(ids[1]))
                .unwrap();
            let integer = FunctionValueType::new(DataType::Int64, false);
            let digits = literal(
                &mut builder,
                &integer,
                checked_constants::i64(&integer, -1, fixture_constant_policy()),
                &mut source_pools,
            );
            add_call(
                &mut builder,
                &mut authors,
                author(
                    &functions,
                    "round",
                    vec![argument(ty.clone(), None), integer_argument(integer, -1)],
                    ControlShape::Eager,
                ),
                vec![source, digits],
            )
        }
    };
    let result_type = FunctionValueType::new(DataType::Boolean, false);
    let expr = builder
        .add(
            None,
            result_type.clone(),
            ExprKind::Binary {
                op: BinaryOperator::EqForNull,
                left,
                right,
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                allow_throw_exception: None,
            },
        )
        .unwrap();
    let output_root = builder
        .root(expr, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let input = ExprPort {
        values: ids.map(|id| (id, ty.clone())).into(),
    };
    register_call_requests(&mut builder, &authors, &mut source_pools);
    let parameters = SemanticParameters::try_new([]).unwrap();
    let mut work = CompileCheckpoints::try_new(&HarnessControl, CompilePhase::Validate).unwrap();
    declare_call_controls(
        &mut builder,
        &functions,
        &source_pools,
        &parameters,
        fixture_constant_policy(),
        &mut work,
    );
    let sealed = builder.seal(vec![input], &mut work).unwrap();
    let unit = finish_unit(
        &functions,
        sealed,
        &authors,
        &source_pools,
        &parameters,
        fixture_constant_policy(),
        &mut work,
    );
    work.finish().unwrap();
    PreparedFixture {
        unit,
        source_prefix: None,
        root: output_root,
        functions,
        constants: source_pools,
        parameters,
    }
    .compile(&HarnessControl)
    .unwrap()
}

use super::callback_causes;
use novarocks_functions::{KernelEvaluationControl, KernelFailure};
use novarocks_type_contract::CompileControlError;
use std::{sync::Mutex, time::Duration};

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
        panic!("guarded numeric calls must not wait")
    }
}
pub fn causes() -> [KernelFailure; 7] {
    callback_causes([
        "guarded-origin-invalid",
        "guarded-origin-internal",
        "guarded-origin-operational",
    ])
}

pub struct CompileCallbacks {
    cause: CompileControlError,
    stop_at: usize,
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refused: Mutex<bool>,
}
impl CompileCallbacks {
    pub fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.trace.lock().unwrap().clone()
    }

    pub fn new(cause: CompileControlError, stop_at: usize) -> Self {
        Self {
            cause,
            stop_at,
            trace: Mutex::new(Vec::new()),
            refused: Mutex::new(false),
        }
    }
}
impl PureCompileControl for CompileCallbacks {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut refused = self.refused.lock().unwrap();
        assert!(!*refused, "no compile callback after refusal");
        let mut trace = self.trace.lock().unwrap();
        trace.push((phase, units));
        if trace.len() == self.stop_at {
            *refused = true;
            Err(self.cause.clone())
        } else {
            Ok(())
        }
    }
}

use crate::CompiledExpressionInstance;
use arrow::array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array,
};
use arrow::record_batch::RecordBatch;
use novarocks_type_contract::ExpressionUseId;

pub fn instance(program: &CompiledFixture) -> CompiledExpressionInstance {
    CompiledExpressionInstance::try_new(program.unit.clone(), program.root, &Control).unwrap()
}
pub fn root_unit(program: &CompiledFixture) -> &Arc<CompiledExprSet> {
    &program.unit
}
pub fn cast_root_use(program: &CompiledFixture) -> ExpressionUseId {
    program.unit.root(program.root).unwrap().use_id
}
pub fn signed(carrier: &DataType, values: &[Option<i64>]) -> ArrayRef {
    match carrier {
        DataType::Int8 => Arc::new(Int8Array::from_iter(
            values.iter().map(|v| v.map(|v| i8::try_from(v).unwrap())),
        )),
        DataType::Int16 => Arc::new(Int16Array::from_iter(
            values.iter().map(|v| v.map(|v| i16::try_from(v).unwrap())),
        )),
        DataType::Int32 => Arc::new(Int32Array::from_iter(
            values.iter().map(|v| v.map(|v| i32::try_from(v).unwrap())),
        )),
        DataType::Int64 => Arc::new(Int64Array::from(values.to_vec())),
        _ => panic!("signed fixture only"),
    }
}
pub fn input(
    program: &CompiledFixture,
    left: &[Option<i64>],
    right: &[Option<i64>],
    flags: &[Option<bool>],
) -> RecordBatch {
    let schema = program.input_schema().clone();
    let left = signed(schema.field(0).data_type(), left);
    let right = signed(schema.field(1).data_type(), right);
    RecordBatch::try_new(
        schema,
        vec![left, right, Arc::new(BooleanArray::from(flags.to_vec()))],
    )
    .unwrap()
}
pub fn errors(output: &novarocks_functions::SelectedValues<'_>) -> Vec<usize> {
    output
        .errors()
        .iter()
        .map(|error| error.selected_ordinal())
        .collect()
}

pub fn bound(ty: &DataType) -> (i64, i64) {
    match ty {
        DataType::Int8 => (i8::MIN.into(), i8::MAX.into()),
        DataType::Int16 => (i16::MIN.into(), i16::MAX.into()),
        DataType::Int32 => (i32::MIN.into(), i32::MAX.into()),
        DataType::Int64 => (i64::MIN, i64::MAX),
        _ => panic!("signed type"),
    }
}
pub fn signed_values(output: &novarocks_functions::SelectedValues<'_>) -> Vec<Option<i64>> {
    let array = output.values();
    match array.data_type() {
        DataType::Int8 => array
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap()
            .iter()
            .map(|v| v.map(i64::from))
            .collect(),
        DataType::Int16 => array
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap()
            .iter()
            .map(|v| v.map(i64::from))
            .collect(),
        DataType::Int32 => array
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .map(|v| v.map(i64::from))
            .collect(),
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect(),
        _ => panic!("exact frozen integral output"),
    }
}

pub fn sliced_batch(program: &CompiledFixture, values: &[Option<i64>]) -> RecordBatch {
    let schema = program.input_schema().clone();
    let mut padded = vec![Some(99)];
    padded.extend_from_slice(values);
    padded.push(Some(-99));
    let source = signed(schema.field(0).data_type(), &padded).slice(1, values.len());
    RecordBatch::try_new(
        schema,
        vec![
            source,
            Arc::new(Int64Array::from(vec![Some(42); values.len()])),
            Arc::new(BooleanArray::from(vec![Some(true); values.len()])),
        ],
    )
    .unwrap()
}

pub fn float_batch(program: &CompiledFixture, values: &[Option<f64>]) -> RecordBatch {
    let schema = program.input_schema().clone();
    let mut padded = vec![Some(99.0)];
    padded.extend_from_slice(values);
    padded.push(Some(-99.0));
    let array: ArrayRef = match schema.field(0).data_type() {
        DataType::Float32 => Arc::new(Float32Array::from(
            padded
                .iter()
                .map(|v| v.map(|v| v as f32))
                .collect::<Vec<_>>(),
        )),
        DataType::Float64 => Arc::new(Float64Array::from(padded)),
        _ => panic!("actual frozen float source"),
    };
    RecordBatch::try_new(
        schema,
        vec![
            array.slice(1, values.len()),
            Arc::new(Int64Array::from(vec![Some(42); values.len()])),
            Arc::new(BooleanArray::from(vec![Some(true); values.len()])),
        ],
    )
    .unwrap()
}
