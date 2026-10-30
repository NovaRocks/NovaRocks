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

//! Window lowering: the analytic Sort, the local Analytic with one prepared
//! call per physical window call, retired call and frame-offset occurrences,
//! re-rooted argument occurrences, per-call frames and refusals. Every binding
//! comes from the real builtin resolver over a sealed subset of the real
//! catalogue, and each call's frozen effects from a fresh preparation by its
//! installed owner, as the FE freezes them.

use super::*;
use arrow_array::{Array, Int64Array};
use arrow_schema::{DataType, Field};
use novarocks_connector_contract::PureProviderProgramCatalog;
use novarocks_functions::{
    AggregateKernelPhase, AggregatePreparationOptions, CallEffectInput, ConstantPolicy,
    ConstantPool, EngineFunctionCatalogBuilder, FunctionArgument, FunctionBindingRequest,
    FunctionKind, FunctionResultType, InstalledPureKernel, PreparedPureKernel, PureCallPreparation,
    PureEngineFunctionCatalog, ScopedExpressionEffects, WindowCallOptions,
};
use novarocks_local_program::{
    AnalyticOutputColumn, KernelAbiVersion, LocalProgram, ProgramCallSite, ProgramExpressionArena,
    ProgramExpressionRootSite, ProgramNodeExpressionRole, ProgramNodeId, ProgramNodeKind,
    ProgramStateTemplate, WindowBoundary, WindowFrame as ProgramFrame, WindowFunctionKind,
    WindowType,
};
use novarocks_physical_plan::{
    AggregateBinding, AggregatePhase, BoundFunction, ConstantPoolId, ConstantPools,
    ConstantReference, ExprId, ExprKind, Fragment, FragmentBuilder, FragmentCuts, FragmentId,
    FragmentPackage, FragmentPackageAdmission, FragmentPackageInput, FragmentSink,
    FrozenFragmentCalls, FrozenFragmentPruning, FrozenPhysicalCall, FunctionArgumentType,
    LiteralValue, NodeId, NodeKind, NullOrdering, PhysicalCallDefinition, PhysicalCallRequest,
    PhysicalCallSite, PhysicalExpressionRoots, PhysicalRootUses, PipelineDopDomain, PlanLimits,
    PlanVersionId, PropertyProofProjectionLimits, RequiredContracts, ResultField, ResultPort,
    SortDirection, SortExpr, SortMode, StaticFunctionArgument, ValueOrigin, WindowExpression,
    WindowSpec,
};
use novarocks_type_contract::{
    CallProofScope, CompileControlError, CompilePhase, ControlShape, DecimalOverflowPolicy,
    EvaluationDemand, EvaluationDomainId, ExpressionControlFlow, ExpressionEffectContext,
    ExpressionEffects, ExpressionEvaluationDomain, ExpressionInvocation, ExpressionUseId,
    FunctionValueType, PureCompileControl, SemanticParameters, WindowBound, WindowFrame,
    WindowFrameExclusion, WindowFrameUnits,
};
use std::{collections::BTreeMap, num::NonZeroUsize, sync::Arc};

const VALUES: NodeId = NodeId::new(0);
const SORT: NodeId = NodeId::new(1);
const WINDOW: NodeId = NodeId::new(2);
const POLICY: DecimalOverflowPolicy = DecimalOverflowPolicy::ReportError;
const CHILD_USE_BASE: u32 = 1_000_000;
const CONSTANT_POOL: u32 = 7;
const P: usize = 0;
const O: usize = 1;
const X: usize = 2;

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
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

fn admission() -> FragmentPackageAdmission {
    FragmentPackageAdmission {
        plan_limits: PlanLimits::FROZEN,
        source_retained_bytes: 64 * 1024 * 1024,
        property_projection_limits: PropertyProofProjectionLimits {
            max_request_bytes: 16 * 1024 * 1024,
            max_coexisting_bytes: 256 * 1024 * 1024,
            max_projection_work: 16 * 1024 * 1024,
        },
    }
}

/// A sealed subset of the real builtin catalogue holding exactly the named
/// definitions, each installed record read from the definition's own owner.
fn catalog() -> PureEngineFunctionCatalog {
    let actual =
        novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    let mut installed = Vec::new();
    for (name, kind) in [
        ("row_number", FunctionKind::Window),
        ("first_value", FunctionKind::Window),
        ("last_value", FunctionKind::Window),
        ("lead", FunctionKind::Window),
        ("count", FunctionKind::Aggregate),
        ("sum", FunctionKind::Aggregate),
        ("min", FunctionKind::Aggregate),
        ("max", FunctionKind::Aggregate),
    ] {
        let definition = actual.definition(name, kind).unwrap().clone();
        let declaration = definition.binding_declaration().unwrap();
        for overload in declaration.overloads() {
            let record = actual
                .pure_overload_declaration_observed(
                    declaration.function_id(),
                    declaration.kind(),
                    &overload.identity,
                    &Control,
                )
                .unwrap();
            installed.push(InstalledPureKernel {
                function: declaration.function_id().clone(),
                kind: declaration.kind(),
                implementation: record.implementation().clone(),
                aggregate_state_format: overload
                    .aggregate
                    .as_ref()
                    .map(|aggregate| aggregate.state_format.clone()),
            });
        }
        builder.register(definition).unwrap();
    }
    builder.seal_pure(installed).unwrap()
}

#[derive(Clone, Copy)]
enum Arg {
    Column(usize),
    Constant(i64),
}

#[derive(Clone)]
struct Call {
    name: &'static str,
    kind: FunctionKind,
    args: Vec<Arg>,
    frame: Option<(WindowFrameUnits, WindowBound<u64>, WindowBound<u64>)>,
    ignore_nulls: bool,
    distinct: bool,
    /// One function ORDER BY channel over `x`, appended to the binding.
    function_order: bool,
    /// Bound by the complete builtin metadata, which installs no pure owner
    /// for it; its frozen effects are the declared aggregate effects.
    uninstalled: bool,
}
impl Call {
    fn window(name: &'static str, args: &[Arg]) -> Self {
        Self {
            name,
            kind: FunctionKind::Window,
            args: args.to_vec(),
            frame: None,
            ignore_nulls: false,
            distinct: false,
            function_order: false,
            uninstalled: false,
        }
    }
    fn aggregate(name: &'static str, args: &[Arg]) -> Self {
        Self {
            kind: FunctionKind::Aggregate,
            ..Self::window(name, args)
        }
    }
    fn uninstalled_aggregate(name: &'static str, args: &[Arg]) -> Self {
        Self {
            uninstalled: true,
            ..Self::aggregate(name, args)
        }
    }
    fn framed(
        mut self,
        units: WindowFrameUnits,
        start: WindowBound<u64>,
        end: WindowBound<u64>,
    ) -> Self {
        self.frame = Some((units, start, end));
        self
    }
}

/// `PARTITION BY p` when `partitioned`, `ORDER BY o` when `ordered`.
struct Shape {
    partitioned: bool,
    ordered: bool,
    calls: Vec<Call>,
}

fn int64(nullable: bool) -> FunctionValueType {
    FunctionValueType::new(DataType::Int64, nullable)
}

fn ascending(expr: ExprId) -> SortExpr {
    SortExpr {
        expr,
        direction: SortDirection::Ascending,
        null_ordering: NullOrdering::First,
    }
}

fn references(fragment: &Fragment, definition: ExprId) -> Vec<ExprId> {
    let mut references = Vec::new();
    fragment
        .expressions()
        .get(definition)
        .unwrap()
        .kind
        .expression_references_observed::<std::convert::Infallible>(|id| {
            references.push(id);
            Ok(())
        })
        .unwrap();
    references
}

/// `Values(p, o, x) -> Sort -> Window(shape) -> Result`.
fn package(shape: &Shape, catalog: &PureEngineFunctionCatalog) -> Arc<FragmentPackage> {
    let complete =
        novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
    let mut builder = FragmentBuilder::new(FragmentId::new(47));
    let columns = (0..3)
        .map(|ordinal| {
            builder
                .add_value(
                    int64(true),
                    ValueOrigin::NodeOutput {
                        node: VALUES,
                        output_ordinal: ordinal,
                    },
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    let cells = [[Some(1), Some(2), Some(3)], [Some(1), None, None]]
        .iter()
        .map(|row| {
            row.iter()
                .map(|cell| {
                    builder
                        .add_expression(
                            VALUES,
                            int64(true),
                            ExprKind::Literal(cell.map_or(LiteralValue::Null, LiteralValue::Int64)),
                        )
                        .unwrap()
                })
                .collect::<Box<[_]>>()
        })
        .collect::<Box<[_]>>();
    builder
        .add_values(VALUES, cells, columns.clone().into_boxed_slice())
        .unwrap();
    let constants = shape
        .calls
        .iter()
        .flat_map(|call| call.args.iter())
        .filter_map(|arg| match arg {
            Arg::Constant(value) => Some(*value),
            Arg::Column(_) => None,
        })
        .collect::<Vec<_>>();
    let pool = (!constants.is_empty()).then(|| {
        ConstantPool::try_new(
            Arc::new(Field::new("window.constants", DataType::Int64, false)),
            int64(false),
            Int64Array::from(constants.clone()).into_data(),
            constant_policy(),
            CompilePhase::Validate,
            &Control,
        )
        .unwrap()
    });
    let mut pools = ConstantPools::empty();
    if let Some(pool) = &pool {
        pools
            .insert(ConstantPoolId::new(CONSTANT_POOL), pool.clone())
            .unwrap();
    }
    let pool_value = |ordinal: u32| pool.as_ref().unwrap().value(ordinal).unwrap();
    let key = |builder: &mut FragmentBuilder, owner, column: usize| {
        ascending(
            builder
                .add_expression(owner, int64(true), ExprKind::Value(columns[column]))
                .unwrap(),
        )
    };
    let order = if shape.ordered {
        Box::from([key(&mut builder, SORT, O)])
    } else {
        Box::default()
    };
    let mode = if shape.partitioned {
        SortMode::Analytic {
            partition_by: Box::from([key(&mut builder, SORT, P)]),
        }
    } else {
        SortMode::Global
    };
    builder.add_sort(SORT, VALUES, order, mode).unwrap();
    let partition_by = if shape.partitioned {
        Box::from([key(&mut builder, WINDOW, P)])
    } else {
        Box::default()
    };
    let order_by = if shape.ordered {
        Box::from([key(&mut builder, WINDOW, O)])
    } else {
        Box::default()
    };
    let mut constant_ordinal = 0;
    let mut expressions = Vec::new();
    let mut requests = Vec::new();
    let mut output = columns.clone();
    for call in &shape.calls {
        let mut arguments = Vec::new();
        let mut request = Vec::new();
        let mut static_constants = Vec::new();
        for arg in &call.args {
            match arg {
                Arg::Column(column) => {
                    arguments.push(
                        builder
                            .add_expression(WINDOW, int64(true), ExprKind::Value(columns[*column]))
                            .unwrap(),
                    );
                    request.push(FunctionArgument::Value {
                        value_type: int64(true),
                        constant: None,
                    });
                    static_constants.push(None);
                }
                Arg::Constant(_) => {
                    let reference = ConstantReference {
                        pool: ConstantPoolId::new(CONSTANT_POOL),
                        ordinal: constant_ordinal,
                    };
                    arguments.push(
                        builder
                            .add_expression(WINDOW, int64(false), ExprKind::Constant(reference))
                            .unwrap(),
                    );
                    request.push(FunctionArgument::Value {
                        value_type: int64(false),
                        constant: Some(pool_value(constant_ordinal)),
                    });
                    static_constants.push(Some(reference));
                    constant_ordinal += 1;
                }
            }
        }
        let metadata = if call.uninstalled {
            &complete
        } else {
            catalog.metadata()
        };
        let bound = metadata
            .resolve_bound_user(
                call.name,
                call.kind,
                FunctionBindingRequest {
                    arguments: &request,
                    logical_argument_count: request.len(),
                    expected_result_type: None,
                },
                &Control,
            )
            .unwrap_or_else(|error| panic!("{} binds: {error}", call.name));
        let FunctionResultType::Scalar(result_type) = bound.selected.result_type.clone() else {
            panic!("window result is scalar");
        };
        let mut argument_types = bound.selected.argument_types.to_vec();
        let mut function_order_by = Vec::new();
        if call.function_order {
            function_order_by.push(key(&mut builder, WINDOW, X));
            argument_types.push(FunctionArgumentType::Value(int64(true)));
            static_constants.push(None);
        }
        let mut function = BoundFunction::from_exact_signature(
            bound.function_id.clone(),
            bound.selected.overload.clone(),
            call.kind,
            novarocks_type_contract::TypedFactSlice::from_pure_boxed(
                argument_types.into_boxed_slice(),
            ),
            result_type.clone(),
        );
        function.legacy_metadata = Some(novarocks_physical_plan::LegacyBindingMetadata {
            volatility: bound.semantics.volatility,
            argument_evaluation: bound.semantics.argument_evaluation,
            failure_behavior: bound.semantics.failure_behavior,
            intrinsic_row_error: bound.semantics.intrinsic_row_error,
            semantic_parameters: Box::default(),
        });
        let aggregate_binding = (call.kind == FunctionKind::Aggregate).then(|| {
            let aggregate = bound.selected.aggregate.as_ref().unwrap();
            Box::new(AggregateBinding {
                state_interpretation: None,
                state_argument_contract: aggregate.state_argument_contract,
                function: function.clone(),
                phase: AggregatePhase::Single,
                logical_argument_count: u32::try_from(call.args.len()).unwrap(),
                intermediate_type: aggregate.intermediate_type.clone(),
                state_format: aggregate.state_format.clone(),
            })
        });
        let frame = call.frame.map(|(units, start, end)| {
            let mut bound = |bound: WindowBound<u64>| match bound {
                WindowBound::UnboundedPreceding => WindowBound::UnboundedPreceding,
                WindowBound::CurrentRow => WindowBound::CurrentRow,
                WindowBound::UnboundedFollowing => WindowBound::UnboundedFollowing,
                WindowBound::Preceding(offset) | WindowBound::Following(offset) => {
                    let literal = builder
                        .add_expression(
                            WINDOW,
                            int64(false),
                            ExprKind::Literal(LiteralValue::Int64(i64::try_from(offset).unwrap())),
                        )
                        .unwrap();
                    if matches!(bound, WindowBound::Preceding(_)) {
                        WindowBound::Preceding(literal)
                    } else {
                        WindowBound::Following(literal)
                    }
                }
            };
            WindowFrame {
                units,
                start: bound(start),
                end: bound(end),
                exclusion: WindowFrameExclusion::NoOthers,
            }
        });
        let request_types = function.argument_types.clone();
        let expression = builder
            .add_expression(
                WINDOW,
                result_type.clone(),
                ExprKind::WindowCall {
                    function,
                    distinct: call.distinct,
                    args: arguments.into_boxed_slice(),
                    function_order_by: function_order_by.into_boxed_slice(),
                    frame,
                    ignore_nulls: call.ignore_nulls,
                    aggregate_binding,
                },
            )
            .unwrap();
        let value = builder
            .add_value(
                result_type,
                ValueOrigin::Expr {
                    node: WINDOW,
                    expr: expression,
                },
            )
            .unwrap();
        output.push(value);
        expressions.push(WindowExpression {
            expression,
            output: value,
        });
        requests.push((
            PhysicalCallDefinition::Expression(expression),
            PhysicalCallRequest {
                arguments: request_types
                    .iter()
                    .zip(static_constants)
                    .map(|(argument, constant)| match argument {
                        FunctionArgumentType::Value(value_type) => StaticFunctionArgument::Value {
                            value_type: value_type.clone(),
                            constant,
                        },
                        FunctionArgumentType::Lambda { .. } => panic!("window value channel"),
                    })
                    .collect(),
                logical_argument_count: call.args.len(),
                expected_result_type: None,
                constant_policy: constant_policy(),
            },
        ));
    }
    builder
        .add_row_widening(
            WINDOW,
            SORT,
            output.into_boxed_slice(),
            NodeKind::Window(WindowSpec {
                partition_by,
                order_by,
                expressions: expressions.into_boxed_slice(),
            }),
        )
        .unwrap();
    let fragment = builder
        .finish_definition(
            WINDOW,
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap()
        .with_call_requests_observed(requests, &Control)
        .unwrap();
    let roots = PhysicalExpressionRoots::try_new(&fragment, &Control).unwrap();
    let domain = EvaluationDomainId::new(0);
    let context = |id: u32| ExpressionEffectContext {
        use_id: ExpressionUseId::new(id),
        domain,
        demand: EvaluationDemand::Value,
    };
    let mut uses = Vec::new();
    let mut bindings = Vec::new();
    let mut window_uses = Vec::new();
    let mut next_child = CHILD_USE_BASE;
    for (ordinal, (site, root)) in roots.sites().iter().enumerate() {
        let id = u32::try_from(ordinal).unwrap();
        let mut children = Vec::new();
        if matches!(
            fragment.expressions().get(root.expr).unwrap().kind,
            ExprKind::WindowCall { .. }
        ) {
            for reference in references(&fragment, root.expr) {
                uses.push(ExpressionInvocation {
                    context: context(next_child),
                    definition: reference,
                    control: ControlShape::Eager,
                    arguments: Box::default(),
                });
                children.push(ExpressionUseId::new(next_child));
                next_child += 1;
            }
            window_uses.push((ExpressionUseId::new(id), root.expr, children.clone()));
        }
        uses.push(ExpressionInvocation {
            context: ExpressionEffectContext {
                demand: root.demand,
                ..context(id)
            },
            definition: root.expr,
            control: ControlShape::Eager,
            arguments: children.into_boxed_slice(),
        });
        bindings.push((*site, ExpressionUseId::new(id)));
    }
    let flow = ExpressionControlFlow::try_new(
        vec![ExpressionEvaluationDomain {
            id: domain,
            parent: None,
            guard: None,
        }],
        uses,
        fragment.expressions(),
        CompilePhase::Validate,
        &Control,
    )
    .unwrap();
    let root_uses = PhysicalRootUses::try_new(&fragment, flow, bindings, &Control).unwrap();
    let parameters = SemanticParameters::try_new([]).unwrap();
    let mut frozen = Vec::new();
    for ((use_id, expression, children), call) in window_uses.into_iter().zip(&shape.calls) {
        let ExprKind::WindowCall {
            function,
            args,
            ignore_nulls,
            aggregate_binding,
            ..
        } = &fragment.expressions().get(expression).unwrap().kind
        else {
            unreachable!()
        };
        // The fresh preparation sees only the logical channels; a refused
        // function ORDER BY channel has no preparation of its own.
        let logical = args.len();
        let request = fragment
            .call_requests()
            .get(PhysicalCallDefinition::Expression(expression))
            .unwrap()
            .arguments[..logical]
            .iter()
            .map(|argument| match argument {
                StaticFunctionArgument::Value {
                    value_type,
                    constant,
                } => FunctionArgument::Value {
                    value_type: value_type.clone(),
                    constant: constant.map(|reference| pool_value(reference.ordinal)),
                },
                StaticFunctionArgument::Lambda { .. } => panic!("window value channel"),
            })
            .collect::<Vec<_>>();
        let selection = Arc::new(novarocks_functions::FunctionBindingSelection {
            overload: function.overload.clone(),
            argument_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(
                function.argument_types[..logical]
                    .to_vec()
                    .into_boxed_slice(),
            ),
            result_type: FunctionResultType::Scalar(function.result_type.clone()),
            aggregate: aggregate_binding.as_deref().map(|binding| {
                novarocks_functions::AggregateBindingSelection {
                    state_argument_contract: binding.state_argument_contract,
                    intermediate_type: binding.intermediate_type.clone(),
                    state_format: binding.state_format.clone(),
                }
            }),
        });
        let argument_uses = children[..logical]
            .iter()
            .map(|child| Some(*child))
            .collect::<Vec<_>>();
        let call_context = context(use_id.get());
        if call.uninstalled {
            frozen.push(FrozenPhysicalCall {
                materialized_writer_input: None,
                regexp_count_pattern_source: None,
                to_base64_byte_source: None,
                temporal_source: None,
                site: PhysicalCallSite::Expression(use_id),
                context: call_context,
                effects: declared_aggregate_effects(CallProofScope::Domain(domain)),
                decimal_overflow_policy: POLICY,
            });
            continue;
        }
        let arguments =
            ScopedExpressionEffects::primitive(call_context, ExpressionEffects::PURE_VALUE);
        let options = if aggregate_binding.is_some() {
            PureCallPreparation::Aggregate {
                arguments,
                options: AggregatePreparationOptions {
                    state_interpretation: None,
                    phase: AggregateKernelPhase::Single,
                    distinct: false,
                    order_keys: Arc::from([]),
                    state_input_type: None,
                },
            }
        } else {
            let frame = call.frame.map(|(units, start, end)| WindowFrame {
                units,
                start,
                end,
                exclusion: WindowFrameExclusion::NoOthers,
            });
            PureCallPreparation::Window {
                arguments,
                options: WindowCallOptions::try_new(frame, *ignore_nulls, &Control).unwrap(),
            }
        };
        let token = catalog
            .prepare_fresh(
                CallEffectInput {
                    context: call_context,
                    argument_uses: novarocks_functions::CallArgumentUses::SelectedChannels(
                        &argument_uses,
                    ),
                    function_id: &function.function_id,
                    kind: function.kind,
                    selected: selection.as_ref(),
                    request: FunctionBindingRequest {
                        arguments: &request,
                        logical_argument_count: logical,
                        expected_result_type: None,
                    },
                    environment: &[],
                    parameters: &parameters,
                    decimal_overflow_policy: POLICY,
                    proof_scope: CallProofScope::Domain(domain),
                },
                Arc::clone(&selection),
                options,
                &Control,
            )
            .unwrap_or_else(|error| panic!("fixture window call prepares: {error}"));
        frozen.push(FrozenPhysicalCall {
            materialized_writer_input: None,
            regexp_count_pattern_source: None,
            to_base64_byte_source: None,
            temporal_source: None,
            site: PhysicalCallSite::Expression(use_id),
            context: call_context,
            effects: token.call_contract().effects().clone(),
            decimal_overflow_policy: POLICY,
        });
    }
    let calls = FrozenFragmentCalls::try_new(&fragment, &root_uses, frozen, &Control).unwrap();
    let output = fragment.nodes()[&WINDOW].output.clone();
    let result = ResultPort {
        scalar_schema: None,
        fragment: fragment.id(),
        output: output.clone(),
        fields: output
            .columns
            .iter()
            .enumerate()
            .map(|(ordinal, value)| ResultField {
                domain: novarocks_physical_plan::ResultValueDomain::Plain,
                name: format!("c{ordinal}").into_boxed_str(),
                alias: None,
                value: *value,
                ty: fragment.values()[value].ty.clone(),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    };
    let id = fragment.id();
    Arc::new(
        FragmentPackage::try_new(
            FragmentPackageInput {
                constants: pools,
                version: PlanVersionId::try_new([97; 16]).unwrap(),
                required: RequiredContracts::default(),
                fragment,
                expression_uses: root_uses,
                calls,
                pruning: FrozenFragmentPruning::try_new(id, vec![], &Control).unwrap(),
                cuts: FragmentCuts::default(),
                result: Some(result),
                parameters,
                scans: BTreeMap::new(),
                writes: BTreeMap::new(),
                annotations: Box::default(),
            },
            admission(),
            &Control,
        )
        .unwrap(),
    )
}

/// The effects every installed builtin aggregate owner declares and freezes.
fn declared_aggregate_effects(proof_scope: CallProofScope) -> novarocks_type_contract::CallEffects {
    novarocks_type_contract::CallEffects {
        value_stability: novarocks_functions::FunctionVolatility::Immutable,
        own_row_error: novarocks_type_contract::FunctionIntrinsicRowError::NotRowEvaluated,
        failure_behavior: novarocks_functions::FunctionFailureBehavior::Propagate,
        null_behavior: novarocks_type_contract::FunctionNullBehavior::CalledOnNull,
        argument_control: novarocks_type_contract::ArgumentControl::Aggregate,
        instance_state: novarocks_type_contract::FunctionInstanceState::AggregateInstance,
        observable_effects: novarocks_type_contract::ObservableEffects::NONE,
        environment: Box::new([]),
        proof_scope,
    }
}

fn try_compile(shape: &Shape) -> Result<LocalProgram, FragmentCompileError> {
    let catalog = catalog();
    let providers =
        PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &Control).unwrap();
    let validated =
        validate_fragment_providers(package(shape, &catalog), &providers, &Control).unwrap();
    compile_fragment(
        validated,
        &catalog,
        LocalCompileOptions {
            pipeline_dop: NonZeroUsize::new(1).unwrap(),
            root_sink_dop: Some(NonZeroUsize::new(1).unwrap()),
            kernel_abi: KernelAbiVersion::CURRENT,
            constants: constant_policy(),
            exchange_wait: std::time::Duration::from_secs(120),
        },
        &Control,
    )
}

/// The one Analytic node of a compiled window fragment.
fn analytic(program: &LocalProgram) -> (ProgramNodeId, &ProgramNodeKind) {
    let root = program.graph().root();
    (root, program.graph().nodes()[root.index()].kind())
}

fn frame(
    window_type: WindowType,
    start: Option<WindowBoundary>,
    end: Option<WindowBoundary>,
) -> Option<ProgramFrame> {
    Some(ProgramFrame {
        start,
        end,
        window_type,
    })
}

fn mixed() -> Shape {
    use WindowBound::{CurrentRow, Following, Preceding, UnboundedFollowing};
    let x = Arg::Column(X);
    let mut first = Call::window("first_value", &[x]).framed(
        WindowFrameUnits::Rows,
        Preceding(1),
        Following(2),
    );
    first.ignore_nulls = true;
    Shape {
        partitioned: true,
        ordered: true,
        calls: vec![
            Call::window("row_number", &[]),
            first,
            Call::window("last_value", &[x]).framed(
                WindowFrameUnits::Range,
                CurrentRow,
                UnboundedFollowing,
            ),
            Call::aggregate("count", &[x]),
            Call::window("lead", &[x, Arg::Constant(2), Arg::Constant(0)]),
        ],
    }
}

#[test]
fn every_window_call_lowers_to_a_prepared_call_with_its_own_explicit_frame() {
    use WindowBoundary::{CurrentRow, Following, Preceding};
    let program = try_compile(&mixed()).unwrap();
    let (node, kind) = analytic(&program);
    let ProgramNodeKind::Analytic {
        input,
        partition_exprs,
        order_by_exprs,
        functions,
        output_columns,
    } = kind
    else {
        panic!("the window lowers to the local Analytic owner");
    };
    assert_eq!((partition_exprs.len(), order_by_exprs.len()), (1, 1));
    // The analytic Sort below leads with its partition key.
    let ProgramNodeKind::Sort {
        partition_exprs: sort_partition,
        order_by: sort_order,
        partition_limit: None,
        use_top_n: false,
        ..
    } = program.graph().nodes()[input.index()].kind()
    else {
        panic!("an analytic Sort feeds the window");
    };
    assert_eq!((sort_partition.len(), sort_order.len()), (1, 1));
    assert!(sort_partition[0].asc && sort_partition[0].nulls_first);
    // Mixed frames in one node; a frameless call gets the derived default.
    let default = frame(WindowType::Range, None, Some(CurrentRow));
    let frames = functions.iter().map(|call| call.frame).collect::<Vec<_>>();
    assert_eq!(
        frames,
        vec![
            default,
            frame(WindowType::Rows, Some(Preceding(1)), Some(Following(2))),
            frame(WindowType::Range, Some(CurrentRow), None),
            default,
            default,
        ]
    );
    assert!(
        functions
            .iter()
            .all(|call| matches!(call.kind, WindowFunctionKind::Prepared))
    );
    assert_eq!(
        functions
            .iter()
            .map(|call| (
                call.ignore_nulls,
                call.args.len(),
                call.aggregate_binding.is_some()
            ))
            .collect::<Vec<_>>(),
        vec![
            (false, 0, false),
            (true, 1, false),
            (false, 1, false),
            (false, 1, true),
            (false, 3, false)
        ]
    );
    // The output is the whole input on its own channels, then the calls.
    let input_slots = program.graph().nodes()[input.index()]
        .output_layout()
        .slots();
    let expected = input_slots
        .iter()
        .map(|slot| AnalyticOutputColumn::InputSlotId(*slot))
        .chain((0..5).map(AnalyticOutputColumn::Window))
        .collect::<Vec<_>>();
    assert_eq!(output_columns, &expected);
    let output_slots = program.graph().nodes()[node.index()]
        .output_layout()
        .slots();
    assert_eq!(&output_slots[..3], input_slots);
}

#[test]
fn window_call_occurrences_retire_and_their_arguments_become_window_input_roots() {
    let shape = mixed();
    let catalog = catalog();
    let source = package(&shape, &catalog);
    let program = try_compile(&shape).unwrap();
    let (node, _) = analytic(&program);
    let snapshot = program
        .checked()
        .channels()
        .expressions()
        .resolved_calls()
        .snapshot();
    let flow = &snapshot.flows()[&ProgramExpressionArena::Main];
    let bindings = snapshot.bindings();
    let physical = source.expression_uses();
    let mut calls = 0;
    for (site, use_id) in physical.bindings() {
        let novarocks_physical_plan::ExpressionRootRole::WindowCall { call } = site.role else {
            continue;
        };
        calls += 1;
        let invocation = &physical.flow().uses()[use_id];
        // The call's own occurrence is the frozen relational context: it
        // leaves the flow, and so does every frame-offset occurrence.
        assert!(!flow.uses().contains_key(use_id));
        let channels = shape.calls[call as usize].args.len();
        for offset in &invocation.arguments[channels..] {
            assert!(!flow.uses().contains_key(offset));
        }
        // Each argument occurrence is now an independent WindowInput root.
        for (argument, child) in invocation.arguments[..channels].iter().enumerate() {
            let site = ProgramExpressionRootSite::Node {
                node,
                role: ProgramNodeExpressionRole::WindowInput {
                    call,
                    argument: argument as u32,
                },
            };
            assert_eq!(bindings.get(&site), Some(child));
            assert!(flow.root_use_ids().contains(child));
        }
        // The prepared call sits at its Window site with the frozen context.
        let frozen = source.calls().entries()[&PhysicalCallSite::Expression(*use_id)].context;
        let resolved = &program
            .checked()
            .channels()
            .expressions()
            .resolved_calls()
            .calls()[&ProgramCallSite::Window { node, call }];
        assert_eq!(resolved.call_contract().context(), frozen);
        let ProgramStateTemplate::WindowPartition {
            node: owner,
            call: ordinal,
            kernel,
        } = resolved.state_template()
        else {
            panic!("a window call owns a partition lifecycle");
        };
        assert_eq!((owner, ordinal), (node, call));
        let PreparedPureKernel::Window(actual) = resolved.specialization().prepared() else {
            panic!("an exact window kernel");
        };
        assert!(Arc::ptr_eq(kernel, actual));
        // The prepared options keep the frozen absence of a frame.
        let explicit = shape.calls[call as usize].frame.is_some();
        assert_eq!(kernel.contract().options().frame().is_some(), explicit);
    }
    assert_eq!(calls, 5);
    for role in [
        ProgramNodeExpressionRole::WindowPartition { key: 0 },
        ProgramNodeExpressionRole::WindowOrder { key: 0 },
    ] {
        assert!(bindings.contains_key(&ProgramExpressionRootSite::Node { node, role }));
    }
}

#[test]
fn a_frameless_call_without_order_by_frames_the_whole_partition() {
    let program = try_compile(&Shape {
        partitioned: true,
        ordered: false,
        calls: vec![
            Call::window("first_value", &[Arg::Column(X)]),
            Call::aggregate("count", &[]),
        ],
    })
    .unwrap();
    let (_, ProgramNodeKind::Analytic { functions, .. }) = analytic(&program) else {
        panic!("the window lowers to the local Analytic owner");
    };
    for call in functions {
        assert_eq!(call.frame, frame(WindowType::Rows, None, None));
    }
}

#[test]
fn unsupported_window_shapes_are_refused_by_feature() {
    let x = Arg::Column(X);
    let refused = |call: Call| {
        try_compile(&Shape {
            partitioned: true,
            ordered: true,
            calls: vec![Call::window("row_number", &[]), call],
        })
        .unwrap_err()
    };
    let feature = |error: FragmentCompileError| match error {
        FragmentCompileError::Unsupported {
            node: Some(node),
            feature,
        } if node == WINDOW => feature,
        other => panic!("expected a window refusal, got {other}"),
    };
    let mut distinct = Call::aggregate("count", &[x]);
    distinct.distinct = true;
    assert_eq!(feature(refused(distinct)), "DISTINCT window call");
    let mut ordered = Call::aggregate("count", &[x]);
    ordered.function_order = true;
    assert_eq!(feature(refused(ordered)), "window function ORDER BY");
    assert_eq!(
        feature(refused(Call::window("first_value", &[x]).framed(
            WindowFrameUnits::Groups,
            WindowBound::UnboundedPreceding,
            WindowBound::CurrentRow,
        ))),
        "GROUPS window frame"
    );
}

#[test]
fn window_unknown_identity_is_not_a_capability_refusal() {
    let error = try_compile(&Shape {
        partitioned: true,
        ordered: true,
        calls: vec![Call::uninstalled_aggregate("avg", &[Arg::Column(X)])],
    })
    .unwrap_err();
    assert!(!error.is_unsupported());
    assert!(matches!(
        &error,
        FragmentCompileError::Owner { error, .. }
            if matches!(error.downcast_ref::<crate::expressions::ExpressionLoweringError>(),
                Some(crate::expressions::ExpressionLoweringError::Specialization(
                    novarocks_functions::FunctionSpecializationFailure::Binding(
                        novarocks_functions::FunctionBindingError::UnknownFunction))))
    ));
}

/// Lookup-level coverage: a pure seal cannot contain a metadata-only entry.
#[test]
fn window_metadata_without_pure_owner_remains_a_capability_refusal() {
    let catalog = catalog();
    let source = package(
        &Shape {
            partitioned: true,
            ordered: true,
            calls: vec![Call::window("row_number", &[])],
        },
        &catalog,
    );
    let NodeKind::Window(spec) = &source.fragment().nodes()[&WINDOW].kind else {
        panic!("expected the fixture window")
    };
    let ExprKind::WindowCall { function, .. } = &source
        .fragment()
        .expressions()
        .get(spec.expressions[0].expression)
        .unwrap()
        .kind
    else {
        panic!("expected the fixture window call")
    };
    let original = catalog
        .metadata()
        .definition("row_number", FunctionKind::Window)
        .unwrap();
    let metadata_only = novarocks_functions::FunctionDefinition::try_new_bound(
        original.canonical_name(),
        original.visibility(),
        original.binding_declaration().unwrap().clone(),
        Arc::new(original.binding_resolver().unwrap().clone()),
    )
    .unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder.register(metadata_only).unwrap();
    let metadata = builder.seal_bound().unwrap();
    let error = crate::window::window_function_abi(function, &metadata, &Control).unwrap_err();
    assert!(error.is_unsupported());
    assert!(matches!(
        &error,
        FragmentCompileError::Owner { error, .. }
            if matches!(error.downcast_ref::<crate::expressions::ExpressionLoweringError>(),
                Some(crate::expressions::ExpressionLoweringError::Specialization(
                    novarocks_functions::FunctionSpecializationFailure::MissingPureImplementation(actual)))
                    if actual == &function.overload)
    ));
}

#[test]
fn sum_min_max_over_prepare_through_their_installed_window_kernels() {
    use WindowBound::{CurrentRow, Following, Preceding, UnboundedFollowing, UnboundedPreceding};
    let x = Arg::Column(X);
    let shape = Shape {
        partitioned: true,
        ordered: true,
        calls: vec![
            Call::aggregate("sum", &[x]),
            Call::aggregate("min", &[x]).framed(WindowFrameUnits::Rows, Preceding(1), Following(1)),
            Call::aggregate("max", &[x]).framed(
                WindowFrameUnits::Range,
                UnboundedPreceding,
                UnboundedFollowing,
            ),
            Call::aggregate("sum", &[x]).framed(WindowFrameUnits::Rows, CurrentRow, Following(2)),
        ],
    };
    let program = try_compile(&shape).unwrap();
    let (node, kind) = analytic(&program);
    let ProgramNodeKind::Analytic { functions, .. } = kind else {
        panic!("the window lowers to the local Analytic owner");
    };
    use WindowBoundary::{CurrentRow as Current, Following as After, Preceding as Before};
    assert_eq!(
        functions.iter().map(|call| call.frame).collect::<Vec<_>>(),
        vec![
            frame(WindowType::Range, None, Some(Current)),
            frame(WindowType::Rows, Some(Before(1)), Some(After(1))),
            frame(WindowType::Range, None, None),
            frame(WindowType::Rows, Some(Current), Some(After(2))),
        ]
    );
    let calls = program
        .checked()
        .channels()
        .expressions()
        .resolved_calls()
        .calls();
    for (call, name) in ["sum", "min", "max", "sum"].into_iter().enumerate() {
        let function = &functions[call];
        assert!(matches!(function.kind, WindowFunctionKind::Prepared));
        assert!(function.aggregate_binding.is_some());
        let resolved = &calls[&ProgramCallSite::Window {
            node,
            call: call as u32,
        }];
        // Admission is the installed kernel of the frozen overload, never a
        // name: the prepared kernel is the owner's aggregate window adapter.
        assert_eq!(
            resolved.specialization().implementation().abi,
            novarocks_functions::PureKernelAbi::AggregateWindowV1
        );
        assert_eq!(
            resolved.call_contract().function_id().as_str(),
            format!("builtin.aggregate/{name}/v1")
        );
        let PreparedPureKernel::Window(kernel) = resolved.specialization().prepared() else {
            panic!("an aggregate OVER prepares an exact window kernel");
        };
        assert!(kernel.contract().aggregate().is_some());
    }
}

#[derive(Debug)]
struct WindowFundingRefusal;
impl std::fmt::Display for WindowFundingRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("original Window output funding refusal")
    }
}
impl std::error::Error for WindowFundingRefusal {}

impl novarocks_failure_contract::FailureProjection for WindowFundingRefusal {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::*;
        FailureFact::new(
            FailureClass::Resource,
            FailureReason::new(
                domain::LOCAL_COMPILER,
                std::num::NonZeroU16::new(1001).unwrap(),
            ),
        )
    }
}
struct WindowOutputHost {
    reject: bool,
    calls: Vec<crate::ProjectOutputRequestFacts>,
    completed: usize,
}
impl crate::ProjectMetadataScope for WindowOutputHost {
    type HostError = WindowFundingRefusal;
    fn materialize<C, B>(
        &mut self,
        facts: &crate::ProjectOutputRequestFacts,
        body: B,
    ) -> Result<crate::ProjectMetadataOutput, crate::ProjectMetadataFailure<Self::HostError, C>>
    where
        C: From<FragmentCompileError>,
        B: FnOnce() -> Result<crate::ProjectMetadataOutput, C>,
    {
        self.calls.push(*facts);
        if self.reject {
            return Err(crate::ProjectMetadataFailure::Host(WindowFundingRefusal));
        }
        let output = body().map_err(crate::ProjectMetadataFailure::Body)?;
        assert!(output.exprs.is_empty());
        assert!(output.expr_slot_ids.is_empty());
        assert!(output.layout.schema().metadata().as_arc().is_none());
        self.completed += 1;
        Ok(output)
    }
}

fn compile_hosted_window_output(
    host: &mut WindowOutputHost,
) -> Result<LocalProgram, FragmentCompileError> {
    let catalog = catalog();
    let source = Arc::try_unwrap(package(&mixed(), &catalog)).unwrap();
    let source = Arc::new(source.with_original_metadata_namespace(
        novarocks_type_contract::owned_resources::metadata_materialization::MaterializedFieldNamespace::from_original_loans(Arc::from([])),
    ));
    let providers =
        PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &Control).unwrap();
    let validated = validate_fragment_providers(source, &providers, &Control).unwrap();
    crate::compile_fragment_with_project_metadata_host(
        validated,
        &catalog,
        LocalCompileOptions {
            pipeline_dop: NonZeroUsize::new(1).unwrap(),
            root_sink_dop: Some(NonZeroUsize::new(1).unwrap()),
            kernel_abi: KernelAbiVersion::CURRENT,
            constants: constant_policy(),
            exchange_wait: std::time::Duration::from_secs(120),
        },
        &Control,
        host,
    )
}

#[test]
fn window_typed_output_actual_birth_is_inside_original_compile_observer() {
    // This is the pure callback/body contract. Native tests separately prove
    // original GLOBAL funding, attributed allocation origins and last aliases.
    let direct = try_compile(&mixed()).unwrap();
    let mut host = WindowOutputHost {
        reject: false,
        calls: Vec::new(),
        completed: 0,
    };
    let hosted = compile_hosted_window_output(&mut host).unwrap();
    assert_eq!(host.calls.len(), 1);
    assert_eq!(host.calls[0].node, WINDOW);
    assert!(host.calls[0].requests.allocation_requests_upper_bound > 0);
    assert!(host.calls[0].requests.allocation_request_bytes_upper_bound > 0);
    assert_eq!(host.completed, 1);
    let direct = direct.graph().nodes()[direct.graph().root().index()].output_layout();
    let hosted = hosted.graph().nodes()[hosted.graph().root().index()].output_layout();
    assert_eq!(hosted.schema(), direct.schema());
    assert_eq!(hosted.slots(), direct.slots());
    for ordinal in 0..hosted.slots().len() {
        assert_eq!(
            hosted.value_binding_at(ordinal),
            direct.value_binding_at(ordinal)
        );
    }
}

#[test]
fn window_typed_output_funding_refusal_precedes_actual_birth() {
    let mut host = WindowOutputHost {
        reject: true,
        calls: Vec::new(),
        completed: 0,
    };
    let error = compile_hosted_window_output(&mut host).unwrap_err();
    assert_eq!(host.calls.len(), 1);
    assert_eq!(host.calls[0].node, WINDOW);
    assert_eq!(host.completed, 0);
    let FragmentCompileError::ProjectMetadataHost { error } = error else {
        panic!("the original funding cause must remain nominal");
    };
    assert_eq!(error.to_string(), "original Window output funding refusal");
}
