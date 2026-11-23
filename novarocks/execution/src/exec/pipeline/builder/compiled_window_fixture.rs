// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Physical-plan authoring of real window calls for compiled-path tests.
//!
//! One fragment `Values -> [analytic or global Sort] -> Window -> Result`.
//! Every binding comes from the real builtin resolver over a sealed subset of
//! the real builtin catalogue, never a forged one. Each window call carries
//! what the FE freezes for it:
//! - its original request, with each constant argument's checked pool value;
//! - its frozen effects, obtained from a fresh preparation by the installed
//!   owner with the same inputs the BE compiler later uses;
//! - its frozen context, the actual relational window call slot.
//!
//! Scalar parameters and sort keys have explicit roots in their actual node
//! units. Checked frame offsets retain constant addresses without scalar uses;
//! the SDK owns every scalar occurrence and control region.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, Int64Array};
use arrow::datatypes::{DataType, Field};
use novarocks_expression_contract::{
    BoundFunction, ExprKind, PhysicalCallRequest, RootOrdinal, RootScope,
};
use novarocks_functions::{
    AggregateKernelPhase, AggregatePreparationOptions, CallEffectInput, ConstantPool,
    EngineFunctionCatalogBuilder, FunctionArgument, FunctionBindingRequest, FunctionKind,
    FunctionResultType, InstalledPureKernel, PureCallPreparation, PureEngineFunctionCatalog,
    ScopedRelationalEffects, WindowCallOptions,
};
use novarocks_local_program::LocalProgram;
use novarocks_physical_plan::{
    AggregateBinding, AggregatePhase, ConstantPoolId, ConstantPools, ConstantReference,
    FragmentCuts, FragmentId, FragmentPackage, FragmentPackageAdmission, FragmentPackageInput,
    FragmentSink, FrozenFragmentPruning, FrozenPhysicalCall, FunctionArgumentType, NodeId,
    NodeKind, NullOrdering, PhysicalCallSite, PipelineDopDomain, PlanLimits, PlanVersionId,
    PropertyProofProjectionLimits, RequiredContracts, ResultField, ResultPort, SortDirection,
    SortExpr, SortMode, StaticFunctionArgument, ValueOrigin, WindowCallSpec, WindowSpec,
};
use novarocks_type_contract::{
    ArgumentControl, CallEffects, CallProofScope, CompilePhase, DecimalOverflowPolicy,
    EvaluationDemand, ExpressionEffects, FunctionFailureBehavior, FunctionInstanceState,
    FunctionIntrinsicRowError, FunctionNullBehavior, FunctionValueType, FunctionVolatility,
    ObservableEffects, RelationCallId, RelationEffectContext, SemanticParameters, WindowBound,
    WindowFrame, WindowFrameExclusion, WindowFrameUnits,
};

use super::family_fixture::{FixtureControl, constant_policy};

/// The frozen decimal policy every fixture call authors explicitly.
const POLICY: DecimalOverflowPolicy = DecimalOverflowPolicy::ReportError;
const CONSTANT_POOL: u32 = 7;
pub(super) const VALUES: NodeId = NodeId::new(0);
pub(super) const SORT: NodeId = NodeId::new(1);
pub(super) const WINDOW: NodeId = NodeId::new(2);

/// A sealed subset of the real builtin catalogue holding exactly the named
/// definitions. Each installed record is read from the owner the metadata
/// definition itself carries, as the Server's candidate seal does.
pub(super) fn window_catalog(definitions: &[(&str, FunctionKind)]) -> PureEngineFunctionCatalog {
    let actual =
        novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
    window_catalog_from_actual(definitions, &actual)
}
pub(super) fn window_catalog_from_actual(
    definitions: &[(&str, FunctionKind)],
    actual: &novarocks_functions::EngineFunctionCatalog,
) -> PureEngineFunctionCatalog {
    let mut builder = EngineFunctionCatalogBuilder::new();
    let mut installed = Vec::new();
    for (name, kind) in definitions {
        let definition = actual.definition(name, *kind).unwrap().clone();
        let declaration = definition.binding_declaration().unwrap();
        for overload in declaration.overloads() {
            let record = actual
                .pure_overload_declaration_observed(
                    declaration.function_id(),
                    declaration.kind(),
                    &overload.identity,
                    &FixtureControl,
                )
                .unwrap_or_else(|error| panic!("{name} is installed: {error}"));
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

/// Every installed window owner and every installed aggregate OVER owner.
pub(super) fn installed_catalog() -> PureEngineFunctionCatalog {
    window_catalog(&[
        ("row_number", FunctionKind::Window),
        ("rank", FunctionKind::Window),
        ("dense_rank", FunctionKind::Window),
        ("cume_dist", FunctionKind::Window),
        ("percent_rank", FunctionKind::Window),
        ("first_value", FunctionKind::Window),
        ("last_value", FunctionKind::Window),
        ("ntile", FunctionKind::Window),
        ("lead", FunctionKind::Window),
        ("lag", FunctionKind::Window),
        ("count", FunctionKind::Aggregate),
        ("sum", FunctionKind::Aggregate),
        ("min", FunctionKind::Aggregate),
        ("max", FunctionKind::Aggregate),
    ])
}

/// The effects every installed builtin aggregate owner declares and freezes.
fn declared_aggregate_effects(proof_scope: CallProofScope) -> CallEffects {
    CallEffects {
        value_stability: FunctionVolatility::Immutable,
        own_row_error: FunctionIntrinsicRowError::NotRowEvaluated,
        failure_behavior: FunctionFailureBehavior::Propagate,
        null_behavior: FunctionNullBehavior::CalledOnNull,
        argument_control: ArgumentControl::Aggregate,
        instance_state: FunctionInstanceState::AggregateInstance,
        observable_effects: ObservableEffects::NONE,
        environment: Box::new([]),
        proof_scope,
    }
}

/// One call argument: an input column, a checked BIGINT constant, or an
/// input column times `i64::MAX` under ALLOW_THROW, whose overflow is a row
/// data error of the argument root.
#[derive(Clone, Copy, Debug)]
pub(super) enum Arg {
    Column(usize),
    Constant(i64),
    Overflowing(usize),
}

fn allow_throw() -> novarocks_type_contract::SemanticParameterRef {
    novarocks_type_contract::SemanticParameterRef {
        id: novarocks_type_contract::SemanticParameterId::new(u32::MAX),
        expected_key: novarocks_type_contract::SemanticParameterKey::AllowThrowException,
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct Frame {
    pub units: WindowFrameUnits,
    pub start: WindowBound<u64>,
    pub end: WindowBound<u64>,
}

#[derive(Clone, Debug)]
pub(super) struct Call {
    pub name: &'static str,
    pub kind: FunctionKind,
    pub args: Vec<Arg>,
    pub frame: Option<Frame>,
    pub ignore_nulls: bool,
    pub distinct: bool,
    /// Bound by the complete builtin metadata, which installs no pure owner
    /// for it; its frozen effects are the declared aggregate effects.
    pub uninstalled: bool,
}

impl Call {
    pub(super) fn window(name: &'static str, args: &[Arg]) -> Self {
        Self {
            name,
            kind: FunctionKind::Window,
            args: args.to_vec(),
            frame: None,
            ignore_nulls: false,
            distinct: false,
            uninstalled: false,
        }
    }
    pub(super) fn aggregate(name: &'static str, args: &[Arg]) -> Self {
        Self {
            kind: FunctionKind::Aggregate,
            ..Self::window(name, args)
        }
    }
    pub(super) fn framed(
        mut self,
        units: WindowFrameUnits,
        start: WindowBound<u64>,
        end: WindowBound<u64>,
    ) -> Self {
        self.frame = Some(Frame { units, start, end });
        self
    }
    pub(super) fn ignoring_nulls(mut self) -> Self {
        self.ignore_nulls = true;
        self
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct Key {
    pub column: usize,
    pub ascending: bool,
    pub nulls_first: bool,
}

/// `PARTITION BY` input columns (ascending, NULLS FIRST, as the FE authors
/// them), `ORDER BY` keys and the calls of one Window node.
#[derive(Clone, Debug, Default)]
pub(super) struct Shape {
    pub partition: Vec<usize>,
    pub order: Vec<Key>,
    pub calls: Vec<Call>,
}

fn sort_expr(expr: RootOrdinal, ascending: bool, nulls_first: bool) -> SortExpr {
    SortExpr {
        expr,
        direction: if ascending {
            SortDirection::Ascending
        } else {
            SortDirection::Descending
        },
        null_ordering: if nulls_first {
            NullOrdering::First
        } else {
            NullOrdering::Last
        },
    }
}

fn int64(nullable: bool) -> FunctionValueType {
    FunctionValueType::new(DataType::Int64, nullable)
}

/// Physical literals have no Int8/Int16/Int32 variant. Author those fixture
/// cells as an exact Int64 literal followed by an explicit typed Cast. Keep
/// the original Values author unchanged for every other fixture shape.
fn typed_literal_values(
    builder: &mut crate::exec::operators::family_fixture::constant_sources::FragmentBuilder,
    types: &[FunctionValueType],
    rows: &[Vec<novarocks_functions::ConstantValue>],
) -> Vec<novarocks_physical_plan::ValueId> {
    let needs_cast = |literal: &novarocks_functions::ConstantValue, ty: &FunctionValueType| {
        literal.value_type().data_type() == &DataType::Int64
            && !super::family_fixture::constant_sources::is_null(literal)
            && matches!(
                ty.data_type(),
                DataType::Int8 | DataType::Int16 | DataType::Int32
            )
    };
    let has_cast = rows.iter().any(|row| {
        assert_eq!(row.len(), types.len());
        row.iter()
            .zip(types)
            .any(|(literal, ty)| needs_cast(literal, ty))
    });
    if !has_cast {
        return super::family_fixture::values(builder, VALUES, types, rows);
    }
    let columns = types
        .iter()
        .enumerate()
        .map(|(ordinal, ty)| {
            builder
                .add_value(
                    ty.clone(),
                    ValueOrigin::NodeOutput {
                        node: VALUES,
                        output_ordinal: u32::try_from(ordinal).unwrap(),
                    },
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    let cells = rows
        .iter()
        .map(|row| {
            assert_eq!(row.len(), types.len());
            row.iter()
                .zip(types)
                .map(|(literal, ty)| {
                    let definition = if needs_cast(literal, ty) {
                        let source = builder.constant(VALUES, int64(false), literal.clone());
                        builder
                            .unit(VALUES)
                            .unwrap()
                            .add(
                                None,
                                ty.clone(),
                                ExprKind::Cast {
                                    cast_mode: novarocks_type_contract::TrinoCastMode::Explicit,
                                    expr: source,
                                    target: ty.data_type().clone(),
                                },
                            )
                            .unwrap()
                    } else {
                        builder.constant(VALUES, ty.clone(), literal.clone())
                    };
                    builder
                        .unit(VALUES)
                        .unwrap()
                        .root(definition, EvaluationDemand::Value, RootScope::AllPorts)
                        .unwrap()
                })
                .collect::<Vec<_>>()
                .into_boxed_slice()
        })
        .collect::<Vec<_>>();
    builder
        .add_values(
            VALUES,
            cells.into_boxed_slice(),
            columns.clone().into_boxed_slice(),
        )
        .unwrap();
    columns
}

/// The package of `Values(rows) -> Sort -> Window(shape) -> Result` over
/// nullable BIGINT columns. A window without keys reads Values directly.
pub(super) fn package(
    rows: &[Vec<Option<i64>>],
    width: usize,
    shape: &Shape,
    catalog: &PureEngineFunctionCatalog,
    max_dop: u32,
) -> Arc<FragmentPackage> {
    let types = vec![int64(true); width];
    let literal_rows = rows
        .iter()
        .map(|row| {
            assert_eq!(row.len(), width);
            row.iter()
                .map(|cell| {
                    cell.map_or(
                        crate::exec::operators::family_fixture::constant_sources::null(),
                        crate::exec::operators::family_fixture::constant_sources::i64,
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    package_typed(&literal_rows, &types, shape, catalog, max_dop)
}
/// The SAME physical fixture author with explicit actual source types and
/// literal values. No finished plan is cloned or relabelled; original BIGINT
/// wrapper passes exactly its former types/values. Constant and overflowing
/// test channels retain their original BIGINT contracts.
pub(super) fn package_typed(
    literal_rows: &[Vec<novarocks_functions::ConstantValue>],
    types: &[FunctionValueType],
    shape: &Shape,
    catalog: &PureEngineFunctionCatalog,
    max_dop: u32,
) -> Arc<FragmentPackage> {
    let fixture_sources = crate::exec::operators::family_fixture::constant_sources::Sources::new();

    let complete =
        novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
    let mut builder =
        crate::exec::operators::family_fixture::constant_sources::FragmentBuilder::new(
            FragmentId::new(43),
            &fixture_sources,
        );
    let columns = typed_literal_values(&mut builder, types, literal_rows);
    // Constants: one non-null BIGINT pool, one ordinal per constant argument.
    let constants = shape
        .calls
        .iter()
        .flat_map(|call| call.args.iter())
        .filter_map(|arg| match arg {
            Arg::Constant(value) => Some(*value),
            Arg::Column(_) | Arg::Overflowing(_) => None,
        })
        .collect::<Vec<_>>();
    let pool = (!constants.is_empty()).then(|| {
        ConstantPool::try_new(
            Arc::new(Field::new("window.constants", DataType::Int64, false)),
            int64(false),
            Int64Array::from(constants.clone()).into_data(),
            constant_policy(),
            CompilePhase::Validate,
            &FixtureControl,
        )
        .unwrap()
    });
    let mut pools = ConstantPools::empty();
    if let Some(pool) = &pool {
        pools
            .insert(ConstantPoolId::new(CONSTANT_POOL), pool.clone())
            .unwrap();
    }
    let pool_value = |ordinal: u32| {
        pool.as_ref()
            .expect("a constant argument has its pool")
            .value(ordinal)
            .unwrap()
    };
    // The sort the FE places below a window that partitions or orders.
    let input = if shape.partition.is_empty() && shape.order.is_empty() {
        VALUES
    } else {
        let order = shape
            .order
            .iter()
            .map(|key| {
                let expr = builder
                    .unit(SORT)
                    .unwrap()
                    .add(
                        None,
                        types[key.column].clone(),
                        ExprKind::Value(columns[key.column]),
                    )
                    .unwrap();
                sort_expr(
                    builder
                        .unit(SORT)
                        .unwrap()
                        .root(expr, EvaluationDemand::Value, RootScope::Port(0))
                        .unwrap(),
                    key.ascending,
                    key.nulls_first,
                )
            })
            .collect::<Box<[_]>>();
        let mode = if shape.partition.is_empty() {
            SortMode::Global
        } else {
            SortMode::Analytic {
                partition_by: shape
                    .partition
                    .iter()
                    .map(|column| {
                        let expr = builder
                            .unit(SORT)
                            .unwrap()
                            .add(
                                None,
                                types[*column].clone(),
                                ExprKind::Value(columns[*column]),
                            )
                            .unwrap();
                        sort_expr(
                            builder
                                .unit(SORT)
                                .unwrap()
                                .root(expr, EvaluationDemand::Value, RootScope::Port(0))
                                .unwrap(),
                            true,
                            true,
                        )
                    })
                    .collect(),
            }
        };
        builder.add_sort(SORT, VALUES, order, mode).unwrap();
        SORT
    };
    let partition_by = shape
        .partition
        .iter()
        .map(|column| {
            let expr = builder
                .unit(WINDOW)
                .unwrap()
                .add(
                    None,
                    types[*column].clone(),
                    ExprKind::Value(columns[*column]),
                )
                .unwrap();
            sort_expr(
                builder
                    .unit(WINDOW)
                    .unwrap()
                    .root(expr, EvaluationDemand::Value, RootScope::Port(0))
                    .unwrap(),
                true,
                true,
            )
        })
        .collect::<Box<[_]>>();
    let order_by = shape
        .order
        .iter()
        .map(|key| {
            let expr = builder
                .unit(WINDOW)
                .unwrap()
                .add(
                    None,
                    types[key.column].clone(),
                    ExprKind::Value(columns[key.column]),
                )
                .unwrap();
            sort_expr(
                builder
                    .unit(WINDOW)
                    .unwrap()
                    .root(expr, EvaluationDemand::Value, RootScope::Port(0))
                    .unwrap(),
                key.ascending,
                key.nulls_first,
            )
        })
        .collect::<Box<[_]>>();
    let mut constant_ordinal = 0u32;
    let mut calls = Vec::new();
    let mut requests = Vec::new();
    let mut output = columns.clone();
    for call in &shape.calls {
        let mut arguments = Vec::new();
        let mut request = Vec::new();
        let mut static_request = Vec::new();
        for arg in &call.args {
            match arg {
                Arg::Column(column) => {
                    arguments.push(
                        builder
                            .unit(WINDOW)
                            .unwrap()
                            .add(
                                None,
                                types[*column].clone(),
                                ExprKind::Value(columns[*column]),
                            )
                            .unwrap(),
                    );
                    request.push(FunctionArgument::Value {
                        value_type: types[*column].clone(),
                        constant: None,
                    });
                    static_request.push(None);
                }
                Arg::Overflowing(column) => {
                    assert_eq!(
                        types[*column],
                        int64(true),
                        "existing overflow fixture requires its actual BIGINT source"
                    );
                    let value = builder
                        .unit(WINDOW)
                        .unwrap()
                        .add(
                            None,
                            types[*column].clone(),
                            ExprKind::Value(columns[*column]),
                        )
                        .unwrap();
                    let max = builder.constant(
                        WINDOW,
                        int64(false),
                        crate::exec::operators::family_fixture::constant_sources::i64(i64::MAX),
                    );
                    arguments.push(
                        builder
                            .unit(WINDOW)
                            .unwrap()
                            .add(
                                None,
                                int64(true),
                                ExprKind::Binary {
                                    op: novarocks_expression_contract::BinaryOperator::Multiply,
                                    left: value,
                                    right: max,
                                    decimal_overflow_policy: POLICY,
                                    allow_throw_exception: Some(allow_throw()),
                                },
                            )
                            .unwrap(),
                    );
                    request.push(FunctionArgument::Value {
                        value_type: int64(true),
                        constant: None,
                    });
                    static_request.push(None);
                }
                Arg::Constant(_) => {
                    let reference = ConstantReference {
                        pool: ConstantPoolId::new(CONSTANT_POOL),
                        ordinal: constant_ordinal,
                    };
                    arguments.push(
                        builder
                            .unit(WINDOW)
                            .unwrap()
                            .add(None, int64(false), ExprKind::Constant(reference))
                            .unwrap(),
                    );
                    request.push(FunctionArgument::Value {
                        value_type: int64(false),
                        constant: Some(pool_value(constant_ordinal)),
                    });
                    static_request.push(Some(reference));
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
                &FixtureControl,
            )
            .unwrap_or_else(|error| panic!("{} binds: {error}", call.name));
        let FunctionResultType::Scalar(result_type) = bound.selected.result_type.clone() else {
            panic!("window result is scalar");
        };
        let mut function = BoundFunction::from_exact_signature(
            bound.function_id.clone(),
            bound.selected.overload.clone(),
            call.kind,
            bound.selected.argument_types.clone(),
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
            let aggregate = bound
                .selected
                .aggregate
                .as_ref()
                .expect("aggregate selection");
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
        let frame = call.frame.map(|frame| {
            let mut bound = |bound: WindowBound<u64>| match bound {
                WindowBound::UnboundedPreceding => WindowBound::UnboundedPreceding,
                WindowBound::CurrentRow => WindowBound::CurrentRow,
                WindowBound::UnboundedFollowing => WindowBound::UnboundedFollowing,
                WindowBound::Preceding(offset) | WindowBound::Following(offset) => {
                    let literal = builder.constant_kind(
                        &int64(false),
                        &crate::exec::operators::family_fixture::constant_sources::i64(
                            i64::try_from(offset).unwrap(),
                        ),
                    );
                    let ExprKind::Constant(literal) = literal else {
                        unreachable!("checked frame offset is constant");
                    };
                    if matches!(bound, WindowBound::Preceding(_)) {
                        WindowBound::Preceding(literal)
                    } else {
                        WindowBound::Following(literal)
                    }
                }
            };
            WindowFrame {
                units: frame.units,
                start: bound(frame.start),
                end: bound(frame.end),
                exclusion: WindowFrameExclusion::NoOthers,
            }
        });
        let arguments = arguments
            .into_iter()
            .map(|definition| {
                builder
                    .unit(WINDOW)
                    .unwrap()
                    .root(definition, EvaluationDemand::Value, RootScope::Port(0))
                    .unwrap()
            })
            .collect::<Box<[_]>>();
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
        let site = PhysicalCallSite::Window {
            node: WINDOW,
            call: u32::try_from(calls.len()).unwrap(),
        };
        calls.push(WindowCallSpec {
            binding: function,
            distinct: call.distinct,
            arguments,
            order_by: Box::default(),
            frame,
            ignore_nulls: call.ignore_nulls,
            aggregate_binding,
            output: value,
        });
        requests.push((
            site,
            PhysicalCallRequest {
                arguments: bound
                    .selected
                    .argument_types
                    .iter()
                    .zip(static_request)
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
            input,
            output.into_boxed_slice(),
            NodeKind::Window(WindowSpec {
                partition_by,
                order_by,
                calls: calls.into_boxed_slice(),
            }),
        )
        .unwrap();
    super::family_fixture::seal_no_call_units(&mut builder);
    let fragment = builder
        .finish_structure(
            WINDOW,
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: max_dop,
                requires_power_of_two: false,
            },
            PlanLimits::FROZEN,
            &FixtureControl,
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let (fragment, sources) = fragment.into_parts();
    crate::exec::operators::family_fixture::constant_sources::merge(&mut pools, &sources);
    let fragment = fragment
        .with_call_requests_observed(requests, &FixtureControl)
        .unwrap();
    let overflowing = shape
        .calls
        .iter()
        .flat_map(|call| call.args.iter())
        .any(|arg| matches!(arg, Arg::Overflowing(_)));
    let parameters = if overflowing {
        SemanticParameters::try_new([(
            allow_throw().id,
            novarocks_type_contract::SemanticParameterValue::AllowThrowException(true),
        )])
        .unwrap()
    } else {
        SemanticParameters::try_new([]).unwrap()
    };
    let NodeKind::Window(spec) = &fragment.nodes()[&WINDOW].kind else {
        unreachable!()
    };
    // This fixture explicitly declares its complete Port(0) layout from the
    // same Values -> optional Sort columns and their original complete types.
    // This pure preparation is not a runtime Graph-layout receipt.
    assert_eq!(
        fragment.nodes()[&input].output.columns.as_ref(),
        columns.as_slice()
    );
    let scalar_parameters = fragment.unit(WINDOW).map(|unit| {
        let mut work = novarocks_type_contract::CompileCheckpoints::try_new(
            &FixtureControl,
            CompilePhase::Validate,
        )
        .unwrap();
        let fields = types
            .iter()
            .enumerate()
            .map(|(ordinal, ty)| ty.try_to_field(format!("c{ordinal}")).unwrap())
            .collect::<Vec<_>>();
        let canonical = columns
            .iter()
            .copied()
            .zip(types)
            .enumerate()
            .map(|(ordinal, (value, ty))| novarocks_expression::ScopeColumn {
                value,
                ordinal: u32::try_from(ordinal).unwrap(),
                ty: ty.clone(),
            })
            .collect::<Vec<_>>();
        let scope = novarocks_expression::ScopeLayout::try_new(
            Arc::new(arrow::datatypes::Schema::new(fields)),
            Arc::from(types),
            Arc::from(canonical),
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
            &mut work,
        )
        .unwrap();
        let compiled = novarocks_expression::compile_expr_set_pure(
            unit,
            novarocks_expression::EvaluationEnvironment {
                constants: &pools,
                parameters: &parameters,
            },
            novarocks_expression::ScopeLayouts {
                ports: std::slice::from_ref(&scope),
                all_ports: None,
            },
            constant_policy(),
            catalog,
            &mut work,
        )
        .unwrap();
        work.finish().unwrap();
        compiled
    });
    let mut frozen = Vec::new();
    for (ordinal, (source, call)) in spec.calls.iter().zip(&shape.calls).enumerate() {
        let site = PhysicalCallSite::Window {
            node: WINDOW,
            call: u32::try_from(ordinal).unwrap(),
        };
        let context =
            RelationEffectContext::call(RelationCallId::new(u32::try_from(ordinal).unwrap()));
        let function = &source.binding;
        let request = fragment
            .call_requests()
            .get(site)
            .unwrap()
            .arguments
            .iter()
            .map(|argument| match argument {
                StaticFunctionArgument::Value {
                    value_type,
                    constant,
                } => FunctionArgument::Value {
                    value_type: value_type.clone(),
                    constant: constant.map(|reference| {
                        pools.entries()[&reference.pool]
                            .value(reference.ordinal)
                            .unwrap()
                    }),
                },
                StaticFunctionArgument::Lambda { .. } => panic!("window value channel"),
            })
            .collect::<Vec<_>>();
        let selection = Arc::new(novarocks_functions::FunctionBindingSelection {
            overload: function.overload.clone(),
            argument_types: function.argument_types.clone(),
            result_type: FunctionResultType::Scalar(function.result_type.clone()),
            aggregate: source.aggregate_binding.as_deref().map(|binding| {
                novarocks_functions::AggregateBindingSelection {
                    state_argument_contract: binding.state_argument_contract,
                    intermediate_type: binding.intermediate_type.clone(),
                    state_format: binding.state_format.clone(),
                }
            }),
        });
        let argument_uses = source
            .arguments
            .iter()
            .map(|root| Some(fragment.unit(WINDOW).unwrap().root_use(*root).unwrap()))
            .collect::<Vec<_>>();
        if call.uninstalled {
            frozen.push(FrozenPhysicalCall {
                materialized_writer_input: None,
                site,
                context,
                effects: declared_aggregate_effects(CallProofScope::Relation(context.identity)),
                decimal_overflow_policy: POLICY,
            });
            continue;
        }
        // The actual unit compiler authors primitive own-effects and child
        // composition, including ALLOW_THROW overflow. Only empty argument
        // lists start and finish at the neutral pure-value join identity.
        let mut children = ExpressionEffects::PURE_VALUE;
        for root in source
            .arguments
            .iter()
            .chain(source.order_by.iter().map(|key| &key.expr))
        {
            let unit = fragment.unit(WINDOW).unwrap();
            let use_id = unit.root_use(*root).unwrap();
            let scalar_context = unit.control_flow().uses()[&use_id].context;
            let effects = scalar_parameters.as_ref().unwrap().effects()[&use_id]
                .for_use(scalar_context)
                .unwrap();
            children = children.join(effects);
        }
        let arguments = ScopedRelationalEffects::primitive(context, children);
        // A call's frozen effects do not depend on its frame or DISTINCT.
        let options = if source.aggregate_binding.is_some() {
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
            let frame = source.frame.map(|frame| {
                let offset = |reference: ConstantReference| {
                    u64::try_from(
                        pools.entries()[&reference.pool]
                            .value(reference.ordinal)
                            .unwrap()
                            .try_i64()
                            .unwrap()
                            .unwrap(),
                    )
                    .unwrap()
                };
                let bound = |bound: WindowBound<ConstantReference>| match bound {
                    WindowBound::UnboundedPreceding => WindowBound::UnboundedPreceding,
                    WindowBound::Preceding(reference) => WindowBound::Preceding(offset(reference)),
                    WindowBound::CurrentRow => WindowBound::CurrentRow,
                    WindowBound::Following(reference) => WindowBound::Following(offset(reference)),
                    WindowBound::UnboundedFollowing => WindowBound::UnboundedFollowing,
                };
                WindowFrame {
                    units: frame.units,
                    start: bound(frame.start),
                    end: bound(frame.end),
                    exclusion: frame.exclusion,
                }
            });
            PureCallPreparation::Window {
                arguments,
                options: WindowCallOptions::try_new(frame, source.ignore_nulls, &FixtureControl)
                    .unwrap(),
            }
        };
        let token = catalog
            .prepare_fresh(
                CallEffectInput {
                    context: context.into(),
                    argument_uses: novarocks_functions::CallArgumentUses::SelectedChannels(
                        &argument_uses,
                    ),
                    function_id: &function.function_id,
                    kind: function.kind,
                    selected: selection.as_ref(),
                    request: FunctionBindingRequest {
                        arguments: &request,
                        logical_argument_count: source.arguments.len(),
                        expected_result_type: None,
                    },
                    environment: &[],
                    parameters: &parameters,
                    decimal_overflow_policy: POLICY,
                    proof_scope: CallProofScope::Relation(context.identity),
                },
                Arc::clone(&selection),
                options,
                &FixtureControl,
            )
            .unwrap_or_else(|error| panic!("fixture window call prepares: {error}"));
        frozen.push(FrozenPhysicalCall {
            materialized_writer_input: None,
            site,
            context,
            effects: token.call_contract().effects().clone(),
            decimal_overflow_policy: POLICY,
        });
    }
    let fragment = fragment
        .with_calls_observed(frozen, &FixtureControl)
        .unwrap_or_else(|error| panic!("frozen window calls validate: {error}"));
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
                domain: crate::test_result_domain::result_value_domain(
                    &fragment.values()[value].ty,
                ),
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
                version: PlanVersionId::try_new([93; 16]).unwrap(),
                required: RequiredContracts::default(),
                fragment,
                statement_constant_policy: constant_policy(),
                pruning: FrozenFragmentPruning::try_new(id, vec![], &FixtureControl).unwrap(),
                cuts: FragmentCuts::default(),
                result: Some(result),
                parameters,
                scans: BTreeMap::new(),
                writes: BTreeMap::new(),
                annotations: Box::default(),
                layouts: Default::default(),
            },
            admission(),
            &FixtureControl,
        )
        .unwrap_or_else(|error| panic!("window package validates: {error:?}")),
    )
}

// Explicit small-fixture admission; these are test inputs, not defaults.
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

/// Compile with the real local compiler and the given catalogue.
pub(super) fn try_compile(
    package: Arc<FragmentPackage>,
    catalog: &PureEngineFunctionCatalog,
    dop: usize,
) -> Result<Arc<LocalProgram>, String> {
    let providers =
        novarocks_connector_contract::PureProviderProgramCatalog::<std::io::Error>::try_new(
            &[],
            vec![],
            &FixtureControl,
        )
        .unwrap();
    let validated =
        novarocks_local_compiler::validate_fragment_providers(package, &providers, &FixtureControl)
            .unwrap();
    novarocks_local_compiler::compile_fragment(
        validated,
        catalog,
        novarocks_local_compiler::LocalCompileOptions {
            pipeline_dop: std::num::NonZeroUsize::new(dop).unwrap(),
            root_sink_dop: Some(std::num::NonZeroUsize::new(1).unwrap()),
            kernel_abi: novarocks_local_program::KernelAbiVersion::CURRENT,
            constants: constant_policy(),
        },
        &FixtureControl,
    )
    .map(Arc::new)
    .map_err(|error| error.to_string())
}

pub(super) fn compile(
    package: Arc<FragmentPackage>,
    catalog: &PureEngineFunctionCatalog,
    dop: usize,
) -> Arc<LocalProgram> {
    try_compile(package, catalog, dop)
        .unwrap_or_else(|error| panic!("window fragment compiles: {error}"))
}
