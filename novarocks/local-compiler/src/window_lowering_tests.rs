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
//! call per physical relation call, unchanged unit-local argument occurrences,
//! constant-only frame offsets, per-call frames and refusals. Every binding
//! comes from the real builtin resolver over a sealed subset of the real
//! catalogue, and each call's frozen effects from a fresh preparation by its
//! installed owner, as the FE freezes them.

use super::*;
use arrow_array::{Array, Int64Array};
use arrow_schema::{DataType, Field};
use novarocks_connector_contract::PureProviderProgramCatalog;
use novarocks_expression_contract::{
    BoundFunction, ExprKind, PhysicalCallRequest, RootOrdinal, RootScope,
};
use novarocks_functions::{
    AggregateKernelPhase, AggregatePreparationOptions, AggregateWindowPreparationOptions,
    CallEffectInput, ConstantPool, ConstantValidation, EngineFunctionCatalogBuilder,
    FunctionArgument, FunctionBindingRequest, FunctionKind, FunctionResultType,
    InstalledPureKernel, PreparedPureKernel, PureCallPreparation, PureEngineFunctionCatalog,
    ScopedRelationalEffects, WindowCallOptions,
};
use novarocks_local_program::{
    AnalyticOutputColumn, KernelAbiVersion, LocalProgram, ProgramCallSite, ProgramNodeId,
    ProgramNodeKind, ProgramStateTemplate, WindowBoundary, WindowFrame as ProgramFrame,
    WindowFunctionKind, WindowType,
};
use novarocks_physical_plan::StaticFunctionArgument;
use novarocks_physical_plan::{
    AggregateBinding, AggregatePhase, ConstantPoolId, ConstantPools, ConstantReference,
    FragmentBuilder, FragmentCuts, FragmentId, FragmentPackage, FragmentPackageAdmission,
    FragmentPackageInput, FragmentSink, FrozenFragmentPruning, FrozenPhysicalCall,
    FunctionArgumentType, NodeId, NodeKind, NullOrdering, PhysicalCallSite, PipelineDopDomain,
    PlanLimits, PlanVersionId, PropertyProofProjectionLimits, RequiredContracts, ResultField,
    ResultPort, SortDirection, SortExpr, SortMode, ValueOrigin, WindowCallSpec, WindowSpec,
};
use novarocks_type_contract::{
    CallProofScope, CompileCheckpoints, CompileControlError, CompilePhase, DecimalOverflowPolicy,
    EvaluationDemand, ExpressionEffects, FunctionValueType, PureCompileControl, RelationCallId,
    RelationEffectContext, SemanticParameters, WindowBound, WindowFrame, WindowFrameExclusion,
    WindowFrameUnits,
};
use std::{collections::BTreeMap, num::NonZeroUsize, sync::Arc};

const VALUES: NodeId = NodeId::new(0);
const SORT: NodeId = NodeId::new(1);
const WINDOW: NodeId = NodeId::new(2);
const POLICY: DecimalOverflowPolicy = DecimalOverflowPolicy::ReportError;
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

fn constant_policy() -> ConstantValidation {
    novarocks_functions::ConstantValidation::bounded(novarocks_functions::ConstantPolicy {
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
    })
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

fn ascending(expr: RootOrdinal) -> SortExpr {
    SortExpr {
        expr,
        direction: SortDirection::Ascending,
        null_ordering: NullOrdering::First,
    }
}

trait RootFixture {
    fn add_root(
        &mut self,
        node: NodeId,
        ty: FunctionValueType,
        kind: ExprKind,
        scope: RootScope,
    ) -> Result<RootOrdinal, novarocks_expression_contract::UnitError>;
}
impl RootFixture for FragmentBuilder {
    fn add_root(
        &mut self,
        node: NodeId,
        ty: FunctionValueType,
        kind: ExprKind,
        scope: RootScope,
    ) -> Result<RootOrdinal, novarocks_expression_contract::UnitError> {
        let draft = self
            .unit(node)
            .expect("fixture node must keep its original draft");
        let definition = draft.add(None, ty, kind)?;
        draft.root(definition, EvaluationDemand::Value, scope)
    }
}
fn install_pure_unit(builder: &mut FragmentBuilder, node: NodeId) {
    if builder.construction_unit(node).is_none() {
        return;
    }
    let ports = builder.node_ports(node).unwrap();
    let draft = builder.take_unit_builder(node).unwrap();
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Validate).unwrap();
    let unit = draft
        .seal(ports, &mut work)
        .unwrap()
        .finish(BTreeMap::new(), &mut work)
        .unwrap();
    work.finish().unwrap();
    builder.install_unit(node, Arc::new(unit)).unwrap();
}

fn package(shape: &Shape, catalog: &PureEngineFunctionCatalog) -> Arc<FragmentPackage> {
    let complete =
        novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
    let mut pools = ConstantPools::empty();
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
                        .add_root(
                            VALUES,
                            int64(true),
                            match cell {
                                Some(value) => crate::test_constants::i64(
                                    &mut pools,
                                    &int64(true),
                                    *value,
                                    constant_policy(),
                                ),
                                None => crate::test_constants::null(
                                    &mut pools,
                                    &int64(true),
                                    constant_policy(),
                                ),
                            },
                            RootScope::AllPorts,
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
    if let Some(pool) = &pool {
        pools
            .insert(ConstantPoolId::new(CONSTANT_POOL), pool.clone())
            .unwrap();
    }
    let pool_value = |ordinal: u32| pool.as_ref().unwrap().value(ordinal).unwrap();
    let key = |builder: &mut FragmentBuilder, owner, column: usize| {
        ascending(
            builder
                .add_root(
                    owner,
                    int64(true),
                    ExprKind::Value(columns[column]),
                    RootScope::Port(0),
                )
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
                            .add_root(
                                WINDOW,
                                int64(true),
                                ExprKind::Value(columns[*column]),
                                RootScope::Port(0),
                            )
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
                            .add_root(
                                WINDOW,
                                int64(false),
                                ExprKind::Constant(reference),
                                RootScope::Port(0),
                            )
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
        function.legacy_metadata = Some(novarocks_expression_contract::LegacyBindingMetadata {
            volatility: bound.semantics.volatility,
            argument_evaluation: bound.semantics.argument_evaluation,
            failure_behavior: bound.semantics.failure_behavior,
            intrinsic_row_error: bound.semantics.intrinsic_row_error,
            semantic_parameters: Box::default(),
        });
        let aggregate_binding = (call.kind == FunctionKind::Aggregate).then(|| {
            let aggregate = bound.selected.aggregate.as_ref().unwrap();
            Box::new(AggregateBinding {
                function: function.clone(),
                phase: AggregatePhase::Single,
                logical_argument_count: u32::try_from(call.args.len()).unwrap(),
                merge_state: Some(novarocks_physical_plan::AggregateMergeBinding {
                    state_interpretation: None,
                    state_argument_contract: aggregate.state_argument_contract,
                    intermediate_type: aggregate.intermediate_type.clone(),
                    state_format: aggregate.state_format.clone(),
                }),
            })
        });
        let frame = call.frame.map(|(units, start, end)| {
            let mut bound = |bound: WindowBound<u64>| match bound {
                WindowBound::UnboundedPreceding => WindowBound::UnboundedPreceding,
                WindowBound::CurrentRow => WindowBound::CurrentRow,
                WindowBound::UnboundedFollowing => WindowBound::UnboundedFollowing,
                WindowBound::Preceding(offset) | WindowBound::Following(offset) => {
                    let ExprKind::Constant(literal) = crate::test_constants::i64(
                        &mut pools,
                        &int64(false),
                        i64::try_from(offset).unwrap(),
                        constant_policy(),
                    ) else {
                        panic!("actual window offset constant");
                    };
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
        let value = builder
            .add_value(
                result_type,
                ValueOrigin::NodeOutput {
                    node: WINDOW,
                    output_ordinal: u32::try_from(output.len()).unwrap(),
                },
            )
            .unwrap();
        output.push(value);
        let call_ordinal = u32::try_from(expressions.len()).unwrap();
        expressions.push(WindowCallSpec {
            binding: function,
            distinct: call.distinct,
            arguments: arguments.into_boxed_slice(),
            order_by: function_order_by.into_boxed_slice(),
            frame,
            ignore_nulls: call.ignore_nulls,
            aggregate_binding,
            output: value,
        });
        requests.push((
            PhysicalCallSite::Window {
                node: WINDOW,
                call: call_ordinal,
            },
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
                calls: expressions.into_boxed_slice(),
            }),
        )
        .unwrap();
    for node in [VALUES, SORT, WINDOW] {
        install_pure_unit(&mut builder, node);
    }
    let fragment = builder
        .finish_structure(
            WINDOW,
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            PlanLimits::FROZEN,
            &Control,
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap()
        .with_call_requests_observed(requests, &Control)
        .unwrap();
    let parameters = SemanticParameters::try_new([]).unwrap();
    let mut frozen = Vec::new();
    let NodeKind::Window(spec) = &fragment.nodes()[&WINDOW].kind else {
        panic!("fixture Window");
    };
    for (ordinal, (item, call)) in spec.calls.iter().zip(&shape.calls).enumerate() {
        let function = &item.binding;
        let args = &item.arguments;
        let ignore_nulls = item.ignore_nulls;
        let aggregate_binding = &item.aggregate_binding;
        let site = PhysicalCallSite::Window {
            node: WINDOW,
            call: ordinal as u32,
        };
        let call_context = RelationEffectContext::call(RelationCallId::new(ordinal as u32));
        // The fresh preparation sees only the logical channels; a refused
        // function ORDER BY channel has no preparation of its own.
        let logical = args.len();
        let request = fragment.call_requests().get(site).unwrap().arguments[..logical]
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
                    state_argument_contract: binding
                        .merge_state
                        .as_ref()
                        .unwrap()
                        .state_argument_contract,
                    intermediate_type: binding
                        .merge_state
                        .as_ref()
                        .unwrap()
                        .intermediate_type
                        .clone(),
                    state_format: binding.merge_state.as_ref().unwrap().state_format.clone(),
                }
            }),
        });
        let argument_uses = args
            .iter()
            .map(|root| {
                Some(
                    fragment.nodes()[&WINDOW]
                        .expressions
                        .as_ref()
                        .unwrap()
                        .root_use(*root)
                        .unwrap(),
                )
            })
            .collect::<Vec<_>>();
        if call.uninstalled {
            frozen.push(FrozenPhysicalCall {
                materialized_writer_input: None,
                site,
                context: call_context,
                effects: declared_aggregate_effects(CallProofScope::Relation(
                    call_context.identity,
                )),
                decimal_overflow_policy: POLICY,
            });
            continue;
        }
        let arguments =
            ScopedRelationalEffects::primitive(call_context, ExpressionEffects::PURE_VALUE);
        let options = if aggregate_binding.is_some() {
            PureCallPreparation::AggregateWindow {
                arguments,
                options: AggregateWindowPreparationOptions {
                    aggregate: AggregatePreparationOptions {
                        state_interpretation: None,
                        phase: AggregateKernelPhase::Single,
                        distinct: false,
                        order_keys: Arc::from([]),
                        state_input_type: None,
                    },
                    window: WindowCallOptions::try_new(
                        call.frame.map(|(units, start, end)| WindowFrame {
                            units,
                            start,
                            end,
                            exclusion: WindowFrameExclusion::NoOthers,
                        }),
                        ignore_nulls,
                        &Control,
                    )
                    .unwrap(),
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
                options: WindowCallOptions::try_new(frame, ignore_nulls, &Control).unwrap(),
            }
        };
        let token = catalog
            .prepare_fresh(
                CallEffectInput {
                    context: call_context.into(),
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
                    proof_scope: CallProofScope::Relation(call_context.identity),
                },
                Arc::clone(&selection),
                options,
                &Control,
            )
            .unwrap_or_else(|error| panic!("fixture window call prepares: {error}"));
        frozen.push(FrozenPhysicalCall {
            materialized_writer_input: None,
            site,
            context: call_context,
            effects: token.call_contract().effects().clone(),
            decimal_overflow_policy: POLICY,
        });
    }
    let fragment = fragment.with_calls_observed(frozen, &Control).unwrap();
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
                pruning: FrozenFragmentPruning::try_new(id, vec![], &Control).unwrap(),
                cuts: FragmentCuts::default(),
                result: Some(result),
                parameters,
                scans: BTreeMap::new(),
                writes: BTreeMap::new(),
                annotations: Box::default(),
                layouts: Default::default(),
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
        mode:
            novarocks_local_program::LocalSortMode::Partitioned {
                partition_by: sort_partition,
            },
        order_by: sort_order,
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
            .all(|call| matches!(call.kind, WindowFunctionKind::Prepared { .. }))
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
    let ProgramNodeKind::Analytic {
        functions,
        partition_exprs,
        order_by_exprs,
        ..
    } = analytic(&program).1
    else {
        panic!("compiled Window");
    };
    let source_unit = source.fragment().nodes()[&WINDOW]
        .expressions
        .as_ref()
        .unwrap();
    let mut calls = 0;
    for (call, function) in functions.iter().enumerate() {
        calls += 1;
        let site = PhysicalCallSite::Window {
            node: WINDOW,
            call: call as u32,
        };
        let frozen = source.calls().entries()[&site].context;
        let resolved = &program.resolved_calls().calls()[&ProgramCallSite::Window {
            node,
            call: call as u32,
        }];
        assert_eq!(resolved.call_contract().context(), frozen.into());
        for (argument, reference) in function.args.iter().enumerate() {
            let unit = program.unit(reference.unit).unwrap();
            let compiled_root = unit.root(reference.root).unwrap();
            let source_call = match &source.fragment().nodes()[&WINDOW].kind {
                NodeKind::Window(spec) => &spec.calls[call],
                _ => unreachable!(),
            };
            let original = source_call.arguments[argument];
            assert_eq!(
                compiled_root.use_id,
                source_unit.root_use(original).unwrap()
            );
            assert_eq!(compiled_root.demand, EvaluationDemand::Value);
            assert!(
                unit.control_flow()
                    .root_use_ids()
                    .contains(&compiled_root.use_id)
            );
            assert_eq!(
                unit.control_flow().uses()[&compiled_root.use_id].context,
                source_unit.control_flow().uses()[&compiled_root.use_id].context
            );
        }
        let ProgramStateTemplate::WindowPartition {
            node: owner,
            call: ordinal,
            kernel,
        } = resolved.state_template()
        else {
            panic!("a window call owns a partition lifecycle");
        };
        assert_eq!((owner, ordinal), (node, call as u32));
        let PreparedPureKernel::Window(actual) = resolved.specialization().prepared() else {
            panic!("an exact window kernel");
        };
        assert!(Arc::ptr_eq(kernel, actual));
        let explicit = shape.calls[call].frame.is_some();
        assert_eq!(kernel.contract().options().frame().is_some(), explicit);
    }
    assert_eq!(calls, 5);
    assert_eq!((partition_exprs.len(), order_by_exprs.len()), (1, 1));
    for reference in partition_exprs.iter().chain(order_by_exprs) {
        let unit = program.unit(reference.unit).unwrap();
        assert!(unit.root(reference.root).is_some());
    }
    // Relational calls and frame offsets have no scalar definitions or uses.
    assert!(program.units().iter().all(|unit| unit.calls().is_empty()));
    let NodeKind::Window(spec) = &source.fragment().nodes()[&WINDOW].kind else {
        unreachable!()
    };
    let logical_roots = spec.partition_by.len()
        + spec.order_by.len()
        + spec
            .calls
            .iter()
            .map(|call| call.arguments.len() + call.order_by.len())
            .sum::<usize>();
    assert_eq!(source_unit.roots().len(), logical_roots);
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
    let function = &spec.calls[0].binding;
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
    let calls = program.resolved_calls().calls();
    for (call, name) in ["sum", "min", "max", "sum"].into_iter().enumerate() {
        let function = &functions[call];
        assert!(matches!(function.kind, WindowFunctionKind::Prepared { .. }));
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
