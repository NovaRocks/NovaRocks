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

// Full checked compiler fixtures use actual builtin owners. They do not thaw
// ExprArena or claim Server catalogue, host allocation, or native integration.
use super::CompiledExpressionInstance;
use crate::exec::expr::compiled_program::checked_constants;
use arrow::{
    array::{Array, BooleanArray, Decimal128Array, Float64Array, Int64Array},
    datatypes::DataType,
    record_batch::RecordBatch,
};
use novarocks_connector_contract::PureProviderProgramCatalog;
use novarocks_expression::CompiledExprSet;
use novarocks_expression_contract::{
    BoundFunction, DefinitionControl, ExprId, ExprKind, ExpressionCallClaim, PhysicalCallRequest,
    RootScope, SealedUnit,
};
use novarocks_functions::{
    CallEffectInput, ConstantPolicy, ConstantValue, EngineFunctionCatalogBuilder, FunctionArgument,
    FunctionBindingRequest, FunctionBindingSelection, FunctionId, FunctionKind, FunctionOverloadId,
    FunctionResultType, InstalledPureKernel, KernelDiagnostic, KernelEvaluationControl,
    KernelFailure, PureCallPreparation, PureEngineFunctionCatalog, PureImplementationDeclaration,
    PureImplementationId, PureKernelAbi, ScopedExpressionEffects, Selection,
};
use novarocks_local_compiler::{
    LocalCompileOptions, compile_fragment, validate_fragment_providers,
};
use novarocks_local_program::{KernelAbiVersion, LocalProgram, ProgramNodeKind, ProgramRootRef};
use novarocks_physical_plan::{
    FragmentBuilder, FragmentCuts, FragmentId, FragmentPackage, FragmentPackageInput, FragmentSink,
    FrozenFragmentPruning, NodeId, PipelineDopDomain, PlanVersionId, RequiredContracts,
    ResultField, ResultPort, ValueDef, ValueId, ValueOrigin,
};
use novarocks_type_contract::{
    CallProofScope, CompileCheckpoints, CompileControlError, CompilePhase, ControlShape,
    DecimalOverflowPolicy, EvaluationDemand, ExpressionInvocation, FunctionValueType,
    PureCompileControl, SemanticParameters,
};
use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
    time::Duration,
};

struct Control;
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

#[derive(Clone, Copy)]
enum Shape {
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
fn options() -> LocalCompileOptions {
    LocalCompileOptions {
        pipeline_dop: NonZeroUsize::new(1).unwrap(),
        root_sink_dop: Some(NonZeroUsize::new(1).unwrap()),
        kernel_abi: KernelAbiVersion::CURRENT,
        constants: ConstantPolicy {
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
    }
}
pub(crate) struct Author {
    function: BoundFunction,
    selected: Arc<FunctionBindingSelection>,
    arguments: Vec<FunctionArgument>,
    logical_argument_count: usize,
    constant_policy: ConstantPolicy,
    decimal_policy: DecimalOverflowPolicy,
    expected_result_type: Option<FunctionValueType>,
    pub(crate) shape: ControlShape,
}
impl Author {
    pub(crate) fn with_decimal_policy(mut self, policy: DecimalOverflowPolicy) -> Self {
        self.decimal_policy = policy;
        self
    }
    fn request(&self) -> FunctionBindingRequest<'_> {
        FunctionBindingRequest {
            arguments: &self.arguments,
            logical_argument_count: self.logical_argument_count,
            expected_result_type: self.expected_result_type.as_ref(),
        }
    }
    pub(crate) fn result(&self) -> FunctionValueType {
        let FunctionResultType::Scalar(result) = &self.selected.result_type else {
            panic!("scalar owner")
        };
        result.clone()
    }
}
// Transfer the first resolver request; physical child shapes are not request sources.
fn original_request_sources(
    builder: &mut FragmentBuilder,
    author_node: NodeId,
    authors: &BTreeMap<ExprId, Author>,
    mut pools: novarocks_physical_plan::ConstantPools,
) -> novarocks_physical_plan::ConstantPools {
    use novarocks_physical_plan::StaticFunctionArgument;
    for (&definition, owner) in authors {
        let original = owner.request();
        let arguments = original
            .arguments
            .iter()
            .map(|argument| match argument {
                FunctionArgument::Value {
                    value_type,
                    constant,
                } => {
                    let constant = constant.as_ref().map(|value| {
                        crate::exec::expr::test_support::register_constant(&mut pools, value)
                    });
                    StaticFunctionArgument::Value {
                        value_type: value_type.clone(),
                        constant,
                    }
                }
                FunctionArgument::Lambda {
                    parameter_types,
                    result_type,
                } => StaticFunctionArgument::Lambda {
                    parameter_types: parameter_types.clone(),
                    result_type: result_type.clone(),
                },
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        builder
            .unit(author_node)
            .unwrap()
            .request(
                definition,
                PhysicalCallRequest {
                    arguments,
                    logical_argument_count: original.logical_argument_count,
                    expected_result_type: original.expected_result_type.cloned(),
                    constant_policy: owner.constant_policy,
                },
            )
            .unwrap();
    }
    pools
}
fn integer_constant(ty: &FunctionValueType, value: i64) -> ConstantValue {
    ConstantValue::from_i64(
        Arc::new(ty.try_to_field("fixture").unwrap()),
        ty.clone(),
        value,
        options().constants,
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
pub(crate) fn author(
    functions: &PureEngineFunctionCatalog,
    name: &str,
    arguments: Vec<FunctionArgument>,
    shape: ControlShape,
) -> Author {
    author_with_target(functions, name, arguments, shape, None)
}

fn trusted_author(
    functions: &PureEngineFunctionCatalog,
    name: &str,
    arguments: Vec<FunctionArgument>,
    shape: ControlShape,
    expected_result_type: FunctionValueType,
) -> Author {
    author_with_target(
        functions,
        name,
        arguments,
        shape,
        Some(expected_result_type),
    )
}

fn author_with_target(
    functions: &PureEngineFunctionCatalog,
    name: &str,
    arguments: Vec<FunctionArgument>,
    shape: ControlShape,
    expected_result_type: Option<FunctionValueType>,
) -> Author {
    let logical_argument_count = arguments.len();
    let request = FunctionBindingRequest {
        arguments: &arguments,
        logical_argument_count,
        expected_result_type: expected_result_type.as_ref(),
    };
    let bound = if expected_result_type.is_some() {
        functions
            .metadata()
            .resolve_bound_trusted(name, FunctionKind::Scalar, request, &Control)
    } else {
        functions
            .metadata()
            .resolve_bound_user(name, FunctionKind::Scalar, request, &Control)
    }
    .unwrap();
    let selected = Arc::new(bound.selected.clone());
    let FunctionResultType::Scalar(result) = &selected.result_type else {
        panic!("scalar owner")
    };
    let function = BoundFunction {
        function_id: bound.function_id,
        overload: selected.overload.clone(),
        kind: bound.kind,
        argument_types: selected.argument_types.clone(),
        result_type: result.clone(),
        legacy_metadata: Some(novarocks_expression_contract::LegacyBindingMetadata {
            volatility: bound.semantics.volatility,
            argument_evaluation: bound.semantics.argument_evaluation,
            failure_behavior: bound.semantics.failure_behavior,
            intrinsic_row_error: bound.semantics.intrinsic_row_error,
            semantic_parameters: Box::default(),
        }),
    };
    Author {
        function,
        selected,
        arguments,
        logical_argument_count,
        constant_policy: options().constants,
        decimal_policy: DecimalOverflowPolicy::ReportError,
        expected_result_type,
        shape,
    }
}
pub(crate) fn call(
    builder: &mut FragmentBuilder,
    authors: &mut BTreeMap<ExprId, Author>,
    owner: Author,
    args: Vec<ExprId>,
) -> ExprId {
    let expr = builder
        .unit(NodeId::new(0))
        .unwrap()
        .add(
            None,
            owner.result(),
            ExprKind::FunctionCall {
                function: owner.function.clone(),
                args: args.into_boxed_slice(),
            },
        )
        .unwrap();
    authors.insert(expr, owner);
    expr
}

fn program(shape: Shape) -> Arc<LocalProgram> {
    let mut source_pools = novarocks_physical_plan::ConstantPools::empty();
    let functions = catalogue(shape);
    let fragment_id = FragmentId::new(193);
    let source = NodeId::new(u32::MAX);
    let input = NodeId::new(41);
    let output = NodeId::new(0);
    let flag = ValueId::new(901);
    let seed = ValueId::new(3);
    let decimal_value = ValueId::new(71);
    let fallback = ValueId::new(72);
    let bool_type = FunctionValueType::new(DataType::Boolean, true);
    let integer = FunctionValueType::new(DataType::Int64, true);
    let scalar_integer = FunctionValueType::new(DataType::Int64, false);
    let floating = FunctionValueType::new(DataType::Float64, true);
    let decimal = FunctionValueType::new(DataType::Decimal128(38, 0), true);
    let mut builder = FragmentBuilder::new(fragment_id);
    builder
        .add_values(source, Box::from([Box::default()]), Box::default())
        .unwrap();
    let mut columns = vec![(flag, bool_type.clone()), (seed, integer.clone())];
    if matches!(shape, Shape::Decimal) {
        columns.extend([
            (decimal_value, decimal.clone()),
            (fallback, decimal.clone()),
        ]);
    }
    let mut items = vec![];
    let mut output_values = vec![];
    for (id, ty) in columns {
        // Checked typed NULL source definitions establish the actual input
        // layout; evaluation receives the complete matching Arrow input port.
        let expr = builder
            .unit(input)
            .unwrap()
            .add(
                None,
                ty.clone(),
                checked_constants::kind(
                    &mut source_pools,
                    checked_constants::null(&(ty.clone()), options().constants),
                ),
            )
            .unwrap();
        let root = builder
            .unit(input)
            .unwrap()
            .root(expr, EvaluationDemand::Value, RootScope::Port(0))
            .unwrap();
        builder
            .insert_value(ValueDef {
                id,
                ty,
                origin: ValueOrigin::Expr { node: input, root },
            })
            .unwrap();
        items.push((root, id));
        output_values.push(id);
    }
    builder
        .add_project(
            input,
            source,
            items.into_boxed_slice(),
            output_values.into_boxed_slice(),
        )
        .unwrap();
    let mut authors = BTreeMap::new();
    let flag_expr = builder
        .unit(output)
        .unwrap()
        .add(None, bool_type.clone(), ExprKind::Value(flag))
        .unwrap();
    let root = if matches!(shape, Shape::Random) {
        let seed_expr = builder
            .unit(output)
            .unwrap()
            .add(None, integer.clone(), ExprKind::Value(seed))
            .unwrap();
        let from_column = call(
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
            .unit(output)
            .unwrap()
            .add(
                None,
                floating.clone(),
                checked_constants::kind(
                    &mut source_pools,
                    checked_constants::null(&(floating.clone()), options().constants),
                ),
            )
            .unwrap();
        let conditional = call(
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
            .unit(output)
            .unwrap()
            .add(
                None,
                scalar_integer.clone(),
                checked_constants::kind(
                    &mut source_pools,
                    checked_constants::i64(&(scalar_integer.clone()), 42, options().constants),
                ),
            )
            .unwrap();
        let from_constant = call(
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
        call(
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
            .unit(output)
            .unwrap()
            .add(None, decimal.clone(), ExprKind::Value(decimal_value))
            .unwrap();
        let fallback_expr = builder
            .unit(output)
            .unwrap()
            .add(None, decimal.clone(), ExprKind::Value(fallback))
            .unwrap();
        let digits = builder
            .unit(output)
            .unwrap()
            .add(
                None,
                scalar_integer.clone(),
                checked_constants::kind(
                    &mut source_pools,
                    checked_constants::i64(&(scalar_integer.clone()), -1, options().constants),
                ),
            )
            .unwrap();
        let rounded = call(
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
        let coalesced = call(
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
        call(
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
    let result_type = authors[&root].result();
    let root = builder
        .unit(output)
        .unwrap()
        .root(root, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let result_value = builder
        .add_value(
            result_type.clone(),
            ValueOrigin::Expr { node: output, root },
        )
        .unwrap();
    builder
        .add_project(
            output,
            input,
            Box::from([(root, result_value)]),
            Box::from([result_value]),
        )
        .unwrap();
    let result = ResultPort {
        scalar_schema: None,
        fragment: fragment_id,
        output: builder.construction_node(output).unwrap().output.clone(),
        fields: Box::from([ResultField {
            domain: crate::test_result_domain::result_value_domain(&result_type),
            name: "guarded_result".into(),
            alias: None,
            value: result_value,
            ty: result_type,
        }]),
    };
    compile_checked_fragment(&functions, builder, output, &authors, result, source_pools)
}

pub(crate) fn finish_fixture_units(
    functions: &PureEngineFunctionCatalog,
    builder: &mut FragmentBuilder,
    author_node: NodeId,
    authors: &BTreeMap<ExprId, Author>,
    parameters: &SemanticParameters,
    source_pools: novarocks_physical_plan::ConstantPools,
    mut primitive_effects: impl FnMut(
        &SealedUnit,
        &ExpressionInvocation<ExprId>,
        ScopedExpressionEffects,
    ) -> ScopedExpressionEffects,
) -> novarocks_physical_plan::ConstantPools {
    let constants = original_request_sources(builder, author_node, authors, source_pools);
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Validate).unwrap();
    for (&definition, owner) in authors {
        let temporal_sources = if let ControlShape::TemporalSource(shape) = owner.shape {
            let draft = builder.construction_unit(author_node).unwrap();
            let ExprKind::FunctionCall { args, .. } = &draft.definition(definition).unwrap().kind
            else {
                panic!("original temporal call definition")
            };
            Some(
                novarocks_expression_contract::temporal_source_definitions_from_lookup_observed(
                    shape.kind(),
                    |id| draft.definition(id),
                    args,
                    &mut work,
                )
                .unwrap(),
            )
        } else {
            None
        };
        builder
            .unit(author_node)
            .unwrap()
            .definition_control(
                definition,
                DefinitionControl {
                    shape: owner.shape,
                    temporal_sources,
                },
            )
            .unwrap();
    }
    let nodes = builder
        .construction_nodes()
        .map(|(&node, _)| node)
        .filter(|node| builder.construction_unit(*node).is_some())
        .collect::<Vec<_>>();
    for node in nodes {
        let ports = builder.node_ports(node).unwrap();
        let sealed = builder
            .take_unit_builder(node)
            .unwrap()
            .seal(ports, &mut work)
            .unwrap();
        let flow = sealed.control_flow();
        let mut summaries = BTreeMap::new();
        let mut claims = BTreeMap::new();
        // Consume only the SDK's actual root/argument graph. Use IDs remain
        // opaque and never determine dependency or preparation order.
        let mut pending = sealed
            .root_use_ids()
            .iter()
            .rev()
            .map(|&id| (id, false))
            .collect::<Vec<_>>();
        let mut ordered = Vec::new();
        while let Some((id, complete)) = pending.pop() {
            if complete {
                ordered.push(id);
            } else {
                pending.push((id, true));
                for &child in sealed.uses()[&id].arguments.iter().rev() {
                    pending.push((child, false));
                }
            }
            work.step().unwrap();
        }
        for id in ordered {
            let invocation = &sealed.uses()[&id];
            let context = invocation.context;
            let mut children = ScopedExpressionEffects::pure_value(context);
            for (ordinal, child) in invocation.arguments.iter().enumerate() {
                children = children
                    .join_control_argument(summaries[child], flow, ordinal)
                    .unwrap();
            }
            let owner = if node == author_node {
                authors.get(&invocation.definition)
            } else {
                None
            };
            let summary = if let Some(owner) = owner {
                let argument_uses = if owner.shape == ControlShape::NoArguments {
                    vec![None; owner.logical_argument_count]
                } else {
                    invocation
                        .arguments
                        .iter()
                        .map(|id| Some(*id))
                        .collect::<Vec<_>>()
                };
                let preparation = if matches!(
                    owner.shape,
                    ControlShape::If | ControlShape::Coalesce | ControlShape::TemporalSource(_)
                ) {
                    PureCallPreparation::ControlIntrinsic {
                        arguments: children,
                    }
                } else {
                    PureCallPreparation::Scalar {
                        arguments: children,
                    }
                };
                let source_plan = sealed.temporal_source(context.use_id).cloned();
                let source_channels = source_plan
                    .as_ref()
                    .map(|source| {
                        source
                            .sources
                            .iter()
                            .map(|channel| novarocks_functions::TemporalSourceChannel {
                                role: channel.role,
                                context: flow.uses()[&channel.use_id].context,
                                value_type: &sealed.definition(channel.definition).unwrap().ty,
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                let definition = sealed.definition(invocation.definition).unwrap();
                let ExprKind::FunctionCall { args, .. } = &definition.kind else {
                    panic!("actual authored call")
                };
                let regexp_count_pattern_source =
                    if owner.function.function_id.as_str() == "builtin.scalar/regexp_count/v1" {
                        Some(
                            novarocks_expression_contract::regexp_count_pattern_source_observed(
                                sealed.definitions(),
                                args,
                                &mut work,
                            )
                            .unwrap(),
                        )
                    } else {
                        None
                    };
                let to_base64_byte_source =
                    if owner.function.function_id.as_str() == "builtin.scalar/to_base64/v1" {
                        Some(
                            novarocks_expression_contract::to_base64_byte_source_observed(
                                sealed.definitions(),
                                args,
                                &mut work,
                            )
                            .unwrap(),
                        )
                    } else {
                        None
                    };
                let token = functions
                    .prepare_fresh(
                        CallEffectInput {
                            context: context.into(),
                            argument_uses: match &source_plan {
                                Some(source) => {
                                    novarocks_functions::CallArgumentUses::TemporalSources {
                                        facts: &source.facts,
                                        channels: &source_channels,
                                    }
                                }
                                None => match regexp_count_pattern_source {
                                    Some(source) => {
                                        novarocks_functions::CallArgumentUses::RegexpCountPattern {
                                            source,
                                            channels: &argument_uses,
                                        }
                                    }
                                    None => match to_base64_byte_source {
                                        Some(source) => {
                                            novarocks_functions::CallArgumentUses::ToBase64Bytes {
                                                source,
                                                channels: &argument_uses,
                                            }
                                        }
                                        None => {
                                            novarocks_functions::CallArgumentUses::SelectedChannels(
                                                &argument_uses,
                                            )
                                        }
                                    },
                                },
                            },
                            function_id: &owner.function.function_id,
                            kind: FunctionKind::Scalar,
                            selected: owner.selected.as_ref(),
                            request: owner.request(),
                            environment: &[],
                            parameters,
                            decimal_overflow_policy: owner.decimal_policy,
                            proof_scope: CallProofScope::Region(context.region),
                        },
                        owner.selected.clone(),
                        preparation,
                        &Control,
                    )
                    .unwrap();
                claims.insert(
                    context.use_id,
                    ExpressionCallClaim {
                        context,
                        effects: token.call_contract().effects().clone(),
                        decimal_overflow_policy: owner.decimal_policy,
                        regexp_count_pattern_source,
                        to_base64_byte_source,
                        temporal_source: source_plan,
                    },
                );
                ScopedExpressionEffects::primitive(
                    context,
                    token.effects().for_use(context).unwrap(),
                )
            } else {
                primitive_effects(&sealed, invocation, children)
            };
            summaries.insert(context.use_id, summary);
        }
        let unit = sealed.finish(claims, &mut work).unwrap();
        builder.install_unit(node, Arc::new(unit)).unwrap();
    }
    work.finish().unwrap();
    constants
}
fn compile_checked_fragment(
    functions: &PureEngineFunctionCatalog,
    builder: FragmentBuilder,
    root_node: NodeId,
    authors: &BTreeMap<ExprId, Author>,
    result: ResultPort,
    source_pools: novarocks_physical_plan::ConstantPools,
) -> Arc<LocalProgram> {
    compile_checked_fragment_with_parameters(
        functions,
        builder,
        root_node,
        authors,
        result,
        SemanticParameters::try_new([]).unwrap(),
        source_pools,
    )
}
pub(crate) fn compile_checked_fragment_with_parameters(
    functions: &PureEngineFunctionCatalog,
    mut builder: FragmentBuilder,
    root_node: NodeId,
    authors: &BTreeMap<ExprId, Author>,
    result: ResultPort,
    parameters: SemanticParameters,
    source_pools: novarocks_physical_plan::ConstantPools,
) -> Arc<LocalProgram> {
    let fragment_id = builder.id();
    let constants = finish_fixture_units(
        functions,
        &mut builder,
        root_node,
        authors,
        &parameters,
        source_pools,
        |_, _, children| children,
    );
    let fragment = builder
        .finish_definition(
            root_node,
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let package = Arc::new(
        FragmentPackage::try_new(
            FragmentPackageInput {
                version: PlanVersionId::try_new([193; 16]).unwrap(),
                required: RequiredContracts::default(),
                constants,
                fragment,
                statement_constant_policy: options().constants,
                cuts: FragmentCuts::default(),
                result: Some(result),
                parameters,
                scans: BTreeMap::new(),
                writes: BTreeMap::new(),
                annotations: Box::default(),
                pruning: FrozenFragmentPruning::try_new(fragment_id, vec![], &Control).unwrap(),
                layouts: Default::default(),
            },
            package_admission(),
            &Control,
        )
        .unwrap(),
    );
    let providers =
        PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &Control).unwrap();
    Arc::new(
        compile_fragment(
            validate_fragment_providers(package, &providers, &Control).unwrap(),
            functions,
            options(),
            &Control,
        )
        .unwrap(),
    )
}
fn root(program: &LocalProgram) -> ProgramRootRef {
    let ProgramNodeKind::Project { exprs, .. } =
        program.graph().nodes()[program.graph().root().index()].kind()
    else {
        panic!("actual output Project")
    };
    exprs[0]
}
fn root_unit(program: &LocalProgram) -> &Arc<CompiledExprSet> {
    program.unit(root(program).unit).unwrap()
}
fn instance(program: &Arc<LocalProgram>) -> CompiledExpressionInstance {
    let reference = root(program);
    CompiledExpressionInstance::try_new(
        program.unit(reference.unit).unwrap().clone(),
        reference.root,
        &Control,
    )
    .unwrap()
}
fn batch(program: &LocalProgram, flags: Vec<Option<bool>>, seeds: Vec<Option<i64>>) -> RecordBatch {
    RecordBatch::try_new(
        program.graph().nodes()[1].output_layout().schema().clone(),
        vec![
            Arc::new(BooleanArray::from(flags)),
            Arc::new(Int64Array::from(seeds)),
        ],
    )
    .unwrap()
}
fn decimal_batch(
    program: &LocalProgram,
    flags: Vec<Option<bool>>,
    values: Vec<Option<i128>>,
    fallback: Vec<Option<i128>>,
) -> RecordBatch {
    let rows = flags.len();
    RecordBatch::try_new(
        program.graph().nodes()[1].output_layout().schema().clone(),
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

// Frozen independent rand 0.8.5 StdRng seed_from_u64 oracle, not evaluator output.
const SEED_42: [u64; 6] = [
    0x3fe0d98eec6444e4,
    0x3fe15e014267f5aa,
    0x3fe45dec0e3bca26,
    0x3fd9fa4b5e3f5d8c,
    0x3fa19561bff02330,
    0x3fda8ea728e783e0,
];
const SEED_1_FIRST: u64 = 0x3fef2d034c9a6603;

#[test]
fn nested_if_coalesce_maps_sparse_original_rows_and_constant_versus_column_rng() {
    let program = program(Shape::Random);
    let input = batch(
        &program,
        vec![
            Some(false),
            Some(true),
            Some(false),
            Some(false),
            Some(true),
            None,
        ],
        vec![Some(42), Some(1), None, Some(99), Some(42), None],
    );
    let rows = [1, 3, 5];
    let selection = Selection::try_sparse(6, &rows).unwrap();
    let mut evaluator = instance(&program);
    let output = evaluator.evaluate(&input, selection, &Control).unwrap();
    assert_eq!(output.selection(), selection);
    assert!(output.errors().is_empty());
    let values = output
        .values()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(
        values
            .iter()
            .map(|v| v.unwrap().to_bits())
            .collect::<Vec<_>>(),
        vec![SEED_1_FIRST, SEED_42[0], SEED_42[1]]
    );
    let next = batch(
        &program,
        vec![Some(true), Some(false), None],
        vec![Some(1), None, None],
    );
    let output = evaluator
        .evaluate(&next, Selection::all(3), &Control)
        .unwrap();
    let values = output
        .values()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(
        values
            .iter()
            .map(|v| v.unwrap().to_bits())
            .collect::<Vec<_>>(),
        vec![SEED_1_FIRST, SEED_42[2], SEED_42[3]]
    );
}
#[test]
fn guarded_constant_rng_stays_uninstantiated_until_coalesce_really_needs_it() {
    let program = program(Shape::Random);
    let mut evaluator = instance(&program);
    let true_only = batch(&program, vec![Some(true); 3], vec![Some(42); 3]);
    let output = evaluator
        .evaluate(&true_only, Selection::all(3), &Control)
        .unwrap();
    let values = output
        .values()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!(values.iter().all(|v| v.unwrap().to_bits() == SEED_42[0]));
    assert_eq!(evaluator.instances.len(), 1);
    let false_only = batch(&program, vec![Some(false); 2], vec![None; 2]);
    let output = evaluator
        .evaluate(&false_only, Selection::all(2), &Control)
        .unwrap();
    let values = output
        .values()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(
        values
            .iter()
            .map(|v| v.unwrap().to_bits())
            .collect::<Vec<_>>(),
        SEED_42[..2]
    );
    assert_eq!(evaluator.instances.len(), 2);
}
#[test]
fn empty_guarded_selection_creates_no_kernel_instance_and_does_not_advance_rng() {
    let program = program(Shape::Random);
    let mut evaluator = instance(&program);
    let input = batch(&program, vec![Some(false); 3], vec![None; 3]);
    let rows = [];
    let output = evaluator
        .evaluate(&input, Selection::try_sparse(3, &rows).unwrap(), &Control)
        .unwrap();
    assert!(output.values().is_empty());
    assert!(output.errors().is_empty());
    assert!(evaluator.instances.is_empty());
    let one = batch(&program, vec![None], vec![None]);
    let output = evaluator
        .evaluate(&one, Selection::all(1), &Control)
        .unwrap();
    assert_eq!(
        output
            .values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0)
            .to_bits(),
        SEED_42[0]
    );
    assert_eq!(evaluator.instances.len(), 1);
}
#[test]
fn guarded_round_skips_inactive_overflow_and_remaps_required_error_without_null_fallback() {
    let program = program(Shape::Decimal);
    let max38 = 10_i128.pow(38) - 1;
    let input = decimal_batch(
        &program,
        vec![
            Some(true),
            None,
            Some(true),
            Some(true),
            Some(true),
            Some(true),
            Some(false),
        ],
        vec![
            Some(max38),
            Some(max38),
            Some(max38),
            None,
            Some(25),
            Some(max38),
            Some(max38),
        ],
        vec![Some(71); 7],
    );
    let rows = [1, 3, 5, 6];
    let selection = Selection::try_sparse(7, &rows).unwrap();
    let mut evaluator = instance(&program);
    let output = evaluator.evaluate(&input, selection, &Control).unwrap();
    let values = output
        .values()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(values.value(0), 71);
    assert_eq!(values.value(1), 71);
    assert!(values.is_null(2));
    assert_eq!(values.value(3), 71);
    assert_eq!(output.errors().len(), 1);
    assert_eq!(output.errors()[0].selected_ordinal(), 2);
    assert!(output.errors()[0].message().contains("overflow"));
    let clean = decimal_batch(&program, vec![Some(true)], vec![Some(25)], vec![Some(71)]);
    let output = evaluator
        .evaluate(&clean, Selection::all(1), &Control)
        .unwrap();
    assert!(output.errors().is_empty());
    assert_eq!(
        output
            .values()
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .value(0),
        30
    );
}
#[test]
fn only_false_or_null_if_rows_never_instantiate_guarded_round_even_with_valid_overflow_values() {
    let program = program(Shape::Decimal);
    let mut evaluator = instance(&program);
    let input = decimal_batch(
        &program,
        vec![Some(false), None],
        vec![Some(10_i128.pow(38) - 1); 2],
        vec![Some(17); 2],
    );
    let output = evaluator
        .evaluate(&input, Selection::all(2), &Control)
        .unwrap();
    assert!(output.errors().is_empty());
    assert!(evaluator.instances.is_empty());
    assert_eq!(
        output
            .values()
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(17), Some(17)]
    );
}

struct CallbackControl {
    cause: KernelFailure,
    stop_at: usize,
    trace: Mutex<Vec<u32>>,
    refused: Mutex<bool>,
}
impl CallbackControl {
    fn new(cause: KernelFailure, stop_at: usize) -> Self {
        Self {
            cause,
            stop_at,
            trace: Mutex::new(vec![]),
            refused: Mutex::new(false),
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
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("guarded numeric calls must not wait")
    }
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(KernelDiagnostic::new("guarded-origin-invalid")),
        KernelFailure::Internal(KernelDiagnostic::new("guarded-origin-internal")),
        KernelFailure::Operational(KernelDiagnostic::new("guarded-origin-operational")),
        KernelFailure::InstanceFailed,
    ]
}
#[test]
fn every_guarded_constructor_callback_preserves_all_original_failure_categories() {
    let program = program(Shape::Random);
    let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
    let _ = CompiledExpressionInstance::try_new(
        root_unit(&program).clone(),
        root(&program).root,
        &recorder,
    )
    .unwrap();
    let trace = recorder.trace.lock().unwrap().clone();
    assert!(!trace.is_empty());
    for index in 1..=trace.len() {
        for cause in causes() {
            let control = CallbackControl::new(cause.clone(), index);
            assert!(
                matches!(CompiledExpressionInstance::try_new(root_unit(&program).clone(), root(&program).root, &control), Err(actual) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..index]);
        }
    }
}
#[test]
fn every_guarded_evaluation_callback_preserves_primary_failure_and_never_replays_state() {
    let program = program(Shape::Random);
    let input = batch(
        &program,
        (0..320)
            .map(|row| {
                if row % 3 == 0 {
                    None
                } else {
                    Some(row % 2 == 0)
                }
            })
            .collect(),
        vec![Some(42); 320],
    );
    let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
    let _ = instance(&program)
        .evaluate(&input, Selection::all(320), &recorder)
        .unwrap();
    let trace = recorder.trace.lock().unwrap().clone();
    assert!(trace.contains(&256));
    assert!(trace.iter().all(|units| *units <= 256));
    for index in 1..=trace.len() {
        for cause in causes() {
            let mut evaluator = instance(&program);
            let control = CallbackControl::new(cause.clone(), index);
            assert!(
                matches!(evaluator.evaluate(&input, Selection::all(320), &control), Err(actual) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..index]);
            let after = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
            assert!(matches!(
                evaluator.evaluate(&input, Selection::all(320), &after),
                Err(KernelFailure::InstanceFailed)
            ));
            assert!(after.trace.lock().unwrap().is_empty());
        }
    }
}

#[path = "unary_tests.rs"]
mod unary_tests;

#[path = "case_tests.rs"]
mod case_tests;

#[path = "byte_guarded_tests.rs"]
mod byte_guarded_tests;

#[path = "equality_tests.rs"]
mod equality_tests;

#[path = "arithmetic_tests.rs"]
mod arithmetic_tests;

#[path = "value_conversion_tests.rs"]
mod value_conversion_tests;

// Conservative retained-source invoice and independent projection ceilings for
// these small fixtures only; this is not a production default or a MEM grant.
fn package_admission() -> novarocks_physical_plan::FragmentPackageAdmission {
    novarocks_physical_plan::FragmentPackageAdmission {
        plan_limits: novarocks_physical_plan::PlanLimits::FROZEN,
        source_retained_bytes: 64 * 1024 * 1024,
        property_projection_limits: novarocks_physical_plan::PropertyProofProjectionLimits {
            max_request_bytes: 16 * 1024 * 1024,
            max_coexisting_bytes: 256 * 1024 * 1024,
            max_projection_work: 16 * 1024 * 1024,
        },
    }
}

#[path = "temporal_tests.rs"]
mod temporal_tests;

#[path = "regexp_count_tests.rs"]
mod regexp_count_tests;

#[path = "parse_url_tests.rs"]
mod parse_url_tests;

#[path = "to_base64_tests.rs"]
mod to_base64_tests;

#[path = "to_binary_after_tests.rs"]
mod to_binary_after_tests;

#[path = "ds_hll_state_frame_tests.rs"]
mod ds_hll_state_frame_tests;

#[path = "ds_hll_state_private_frame_tests.rs"]
mod ds_hll_state_private_frame_tests;

#[path = "percentile_raw_private_frame_tests.rs"]
mod percentile_raw_private_frame_tests;

#[path = "scalar_invocation_frame_tests.rs"]
mod scalar_invocation_frame_tests;

#[test]
fn constant_entry_reuses_actual_prepared_if_coalesce_and_declines_rand() {
    use super::constant_node::{
        ConstantNode, ConstantNodeOutcome, DeclineReason, evaluate_constant_node,
    };
    let program = program(Shape::Random);
    let calls = root_unit(&program).calls();
    for (identity, cases) in [
        (
            "builtin.scalar/if/v1",
            vec![
                (Some(true), Some(17.0), Some(42.0), 17.0),
                (Some(false), Some(17.0), Some(42.0), 42.0),
                (None, Some(17.0), Some(42.0), 42.0),
            ],
        ),
        (
            "builtin.scalar/coalesce/v1",
            vec![
                (None, None, Some(42.0), 42.0),
                (None, Some(17.0), Some(42.0), 17.0),
            ],
        ),
    ] {
        let prepared = calls
            .values()
            .find(|call| call.call_contract().function_id().as_str() == identity)
            .unwrap()
            .prepared();
        let selected = prepared.call_contract().selected();
        let FunctionResultType::Scalar(result) = &selected.result_type else {
            panic!("scalar control");
        };
        for (flag, left, right, expected) in cases {
            let mut children = vec![];
            for (ordinal, ty) in selected.argument_types.iter().enumerate() {
                let novarocks_functions::FunctionArgumentType::Value(ty) = ty else {
                    panic!("value control");
                };
                let array: arrow::array::ArrayRef = if ty.data_type() == &DataType::Boolean {
                    Arc::new(BooleanArray::from(vec![flag]))
                } else {
                    let first = if identity == "builtin.scalar/if/v1" {
                        ordinal == 1
                    } else {
                        ordinal == 0
                    };
                    Arc::new(Float64Array::from(vec![if first { left } else { right }]))
                };
                children.push(
                    ConstantValue::from_scalar_array(
                        Arc::new(ty.try_to_field("source").unwrap()),
                        ty.clone(),
                        array.to_data(),
                        options().constants,
                        CompilePhase::FunctionSpecialization,
                        &Control,
                    )
                    .unwrap(),
                );
            }
            let output = evaluate_constant_node(
                ConstantNode::Call(prepared),
                &children,
                result,
                &Control,
                CompilePhase::FunctionSpecialization,
            )
            .unwrap();
            let ConstantNodeOutcome::Value(output) = output else {
                panic!("control must produce its selected value");
            };
            assert_eq!(
                output
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(0),
                expected
            );
        }
    }
    let rand = calls
        .values()
        .find(|call| call.call_contract().function_id().as_str() == "builtin.scalar/rand/v1")
        .unwrap()
        .prepared();
    let FunctionResultType::Scalar(result) = &rand.call_contract().selected().result_type else {
        panic!("scalar rand");
    };
    assert!(matches!(
        evaluate_constant_node(
            ConstantNode::Call(rand),
            &[],
            result,
            &Control,
            CompilePhase::FunctionSpecialization
        )
        .unwrap(),
        ConstantNodeOutcome::Declined(DeclineReason::EffectfulCall)
    ));
}

#[test]
fn constant_entry_strict_null_skips_creation_and_opaque_requirement_declines_before_creation() {
    use super::constant_node::{
        ConstantNode, ConstantNodeOutcome, DeclineReason, evaluate_constant_node,
    };
    use novarocks_functions::{
        PreparedPureKernel, PreparedScalarKernel, ScalarCallContract, ScalarKernelInstance,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[derive(Debug)]
    struct Counted {
        actual: Arc<dyn PreparedScalarKernel>,
        creates: Arc<AtomicUsize>,
        opaque: bool,
    }
    impl PreparedScalarKernel for Counted {
        fn contract(&self) -> &Arc<ScalarCallContract> {
            self.actual.contract()
        }
        fn instance_retained_upper_bound(&self) -> usize {
            self.actual.instance_retained_upper_bound()
        }
        fn instance_inline_allocation_bytes(&self) -> usize {
            if self.opaque { 1 } else { 0 }
        }
        fn create_instance(&self) -> Result<Box<dyn ScalarKernelInstance>, KernelFailure> {
            self.creates.fetch_add(1, Ordering::SeqCst);
            self.actual.create_instance()
        }
    }
    let program = program(Shape::Decimal);
    let actual = root_unit(&program)
        .calls()
        .values()
        .find(|call| call.call_contract().function_id().as_str() == "builtin.scalar/round/v1")
        .unwrap()
        .prepared();
    let PreparedPureKernel::Scalar(actual) = actual else {
        panic!("actual scalar ROUND owner")
    };
    assert_eq!(actual.instance_inline_allocation_bytes(), 0);
    assert_eq!(
        actual.contract().effects().null_behavior,
        novarocks_type_contract::FunctionNullBehavior::Strict
    );
    let FunctionResultType::Scalar(result) = &actual.contract().selected().result_type else {
        panic!("scalar result")
    };
    let make_children = |blocked| {
        actual
            .contract()
            .selected()
            .argument_types
            .iter()
            .map(|ty| {
                let novarocks_functions::FunctionArgumentType::Value(ty) = ty else {
                    panic!("value argument")
                };
                let array: arrow::array::ArrayRef = match ty.data_type() {
                    DataType::Decimal128(p, s) => Arc::new(
                        Decimal128Array::from(vec![if blocked { None } else { Some(123) }])
                            .with_precision_and_scale(*p, *s)
                            .unwrap(),
                    ),
                    DataType::Int64 => Arc::new(Int64Array::from(vec![Some(-1)])),
                    other => panic!("unexpected actual ROUND signature: {other:?}"),
                };
                ConstantValue::from_scalar_array(
                    Arc::new(ty.try_to_field("source").unwrap()),
                    ty.clone(),
                    array.to_data(),
                    options().constants,
                    CompilePhase::FunctionSpecialization,
                    &Control,
                )
                .unwrap()
            })
            .collect::<Vec<_>>()
    };
    let creates = Arc::new(AtomicUsize::new(0));
    let call = PreparedPureKernel::Scalar(Arc::new(Counted {
        actual: actual.clone(),
        creates: creates.clone(),
        opaque: false,
    }));
    let outcome = evaluate_constant_node(
        ConstantNode::Call(&call),
        &make_children(true),
        result,
        &Control,
        CompilePhase::FunctionSpecialization,
    )
    .unwrap();
    let ConstantNodeOutcome::Value(array) = outcome else {
        panic!("strict NULL is a successful SQL NULL")
    };
    assert!(array.is_null(0));
    assert_eq!(creates.load(Ordering::SeqCst), 0);
    assert!(matches!(
        evaluate_constant_node(
            ConstantNode::Call(&call),
            &make_children(false),
            result,
            &Control,
            CompilePhase::FunctionSpecialization
        )
        .unwrap(),
        ConstantNodeOutcome::Value(_)
    ));
    assert_eq!(
        creates.load(Ordering::SeqCst),
        1,
        "the sentinel observes actual admitted creation"
    );
    creates.store(0, Ordering::SeqCst);
    let opaque = PreparedPureKernel::Scalar(Arc::new(Counted {
        actual: actual.clone(),
        creates: creates.clone(),
        opaque: true,
    }));
    assert!(matches!(
        evaluate_constant_node(
            ConstantNode::Call(&opaque),
            &make_children(false),
            result,
            &Control,
            CompilePhase::FunctionSpecialization
        )
        .unwrap(),
        ConstantNodeOutcome::Declined(DeclineReason::OpaqueHostRequired)
    ));
    assert_eq!(creates.load(Ordering::SeqCst), 0);
}
