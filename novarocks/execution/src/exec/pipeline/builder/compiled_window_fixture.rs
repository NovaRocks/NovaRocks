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
//! - its frozen context, the `WindowCall` root occurrence itself.
//!
//! Every expression root is a leaf except a `WindowCall`, whose occurrence
//! has one leaf child use per argument and per frame offset, in definition
//! order. All occurrences share one evaluation domain.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, Int64Array};
use arrow::datatypes::{DataType, Field};
use novarocks_functions::{
    AggregateKernelPhase, AggregatePreparationOptions, CallEffectInput, ConstantPool,
    EngineFunctionCatalogBuilder, FunctionArgument, FunctionBindingRequest, FunctionKind,
    FunctionResultType, InstalledPureKernel, PureCallPreparation, PureEngineFunctionCatalog,
    ScopedExpressionEffects, WindowCallOptions,
};
use novarocks_local_program::LocalProgram;
use novarocks_physical_plan::{
    AggregateBinding, AggregatePhase, BoundFunction, ConstantPoolId, ConstantPools,
    ConstantReference, ExprId, ExprKind, FragmentBuilder, FragmentCuts, FragmentId,
    FragmentPackage, FragmentPackageAdmission, FragmentPackageInput, FragmentSink,
    FrozenFragmentCalls, FrozenFragmentPruning, FrozenPhysicalCall, FunctionArgumentType,
    LiteralValue, NodeId, NodeKind, NullOrdering, PhysicalCallDefinition, PhysicalCallRequest,
    PhysicalCallSite, PhysicalExpressionRoots, PhysicalRootUses, PipelineDopDomain, PlanLimits,
    PlanVersionId, PropertyProofProjectionLimits, RequiredContracts, ResultField, ResultPort,
    SortDirection, SortExpr, SortMode, StaticFunctionArgument, ValueOrigin, WindowExpression,
    WindowSpec,
};
use novarocks_type_contract::{
    ArgumentControl, CallEffects, CallProofScope, CompilePhase, ControlShape,
    DecimalOverflowPolicy, EvaluationDomainId, ExpressionControlFlow, ExpressionEffectContext,
    ExpressionEffects, ExpressionEvaluationDomain, ExpressionInvocation, ExpressionUseId,
    FunctionFailureBehavior, FunctionInstanceState, FunctionIntrinsicRowError,
    FunctionNullBehavior, FunctionValueType, FunctionVolatility, ObservableEffects,
    SemanticParameters, WindowBound, WindowFrame, WindowFrameExclusion, WindowFrameUnits,
};

use super::family_fixture::{FixtureControl, constant_policy};

/// The frozen decimal policy every fixture call authors explicitly.
const POLICY: DecimalOverflowPolicy = DecimalOverflowPolicy::ReportError;
/// Child occurrences of window calls start far above every root occurrence.
const CHILD_USE_BASE: u32 = 1_000_000;
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

fn sort_expr(expr: ExprId, ascending: bool, nulls_first: bool) -> SortExpr {
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
    builder: &mut FragmentBuilder,
    types: &[FunctionValueType],
    rows: &[Vec<LiteralValue>],
) -> (Vec<novarocks_physical_plan::ValueId>, bool) {
    let needs_cast = |literal: &LiteralValue, ty: &FunctionValueType| {
        matches!(literal, LiteralValue::Int64(_))
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
        return (
            super::family_fixture::values(builder, VALUES, types, rows),
            false,
        );
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
                    if needs_cast(literal, ty) {
                        let source = builder
                            .add_expression(
                                VALUES,
                                int64(false),
                                ExprKind::Literal(literal.clone()),
                            )
                            .unwrap();
                        builder
                            .add_expression(
                                VALUES,
                                ty.clone(),
                                ExprKind::Cast {
                                    expr: source,
                                    target: ty.data_type().clone(),
                                    decimal_overflow_policy: POLICY,
                                    allow_throw_exception: allow_throw(),
                                },
                            )
                            .unwrap()
                    } else {
                        builder
                            .add_expression(VALUES, ty.clone(), ExprKind::Literal(literal.clone()))
                            .unwrap()
                    }
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
    (columns, true)
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
                .map(|cell| cell.map_or(LiteralValue::Null, LiteralValue::Int64))
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
    literal_rows: &[Vec<LiteralValue>],
    types: &[FunctionValueType],
    shape: &Shape,
    catalog: &PureEngineFunctionCatalog,
    max_dop: u32,
) -> Arc<FragmentPackage> {
    let complete =
        novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
    let mut builder = FragmentBuilder::new(FragmentId::new(43));
    let (columns, integral_literal_cast) = typed_literal_values(&mut builder, types, literal_rows);
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
                    .add_expression(
                        SORT,
                        types[key.column].clone(),
                        ExprKind::Value(columns[key.column]),
                    )
                    .unwrap();
                sort_expr(expr, key.ascending, key.nulls_first)
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
                            .add_expression(
                                SORT,
                                types[*column].clone(),
                                ExprKind::Value(columns[*column]),
                            )
                            .unwrap();
                        sort_expr(expr, true, true)
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
                .add_expression(
                    WINDOW,
                    types[*column].clone(),
                    ExprKind::Value(columns[*column]),
                )
                .unwrap();
            sort_expr(expr, true, true)
        })
        .collect::<Box<[_]>>();
    let order_by = shape
        .order
        .iter()
        .map(|key| {
            let expr = builder
                .add_expression(
                    WINDOW,
                    types[key.column].clone(),
                    ExprKind::Value(columns[key.column]),
                )
                .unwrap();
            sort_expr(expr, key.ascending, key.nulls_first)
        })
        .collect::<Box<[_]>>();
    let mut constant_ordinal = 0u32;
    let mut expressions = Vec::new();
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
                            .add_expression(
                                WINDOW,
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
                        .add_expression(
                            WINDOW,
                            types[*column].clone(),
                            ExprKind::Value(columns[*column]),
                        )
                        .unwrap();
                    let max = builder
                        .add_expression(
                            WINDOW,
                            int64(false),
                            ExprKind::Literal(LiteralValue::Int64(i64::MAX)),
                        )
                        .unwrap();
                    arguments.push(
                        builder
                            .add_expression(
                                WINDOW,
                                int64(true),
                                ExprKind::Binary {
                                    op: novarocks_physical_plan::BinaryOperator::Multiply,
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
                            .add_expression(WINDOW, int64(false), ExprKind::Constant(reference))
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
        function.legacy_metadata = Some(novarocks_physical_plan::LegacyBindingMetadata {
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
                units: frame.units,
                start: bound(frame.start),
                end: bound(frame.end),
                exclusion: WindowFrameExclusion::NoOthers,
            }
        });
        let expression = builder
            .add_expression(
                WINDOW,
                result_type.clone(),
                ExprKind::WindowCall {
                    function,
                    distinct: call.distinct,
                    args: arguments.into_boxed_slice(),
                    function_order_by: Box::default(),
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
                max: max_dop,
                requires_power_of_two: false,
            },
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap()
        .with_call_requests_observed(requests, &FixtureControl)
        .unwrap();
    // Root occurrences, then one leaf child per WindowCall reference.
    let roots = PhysicalExpressionRoots::try_new(&fragment, &FixtureControl).unwrap();
    let domain = EvaluationDomainId::new(0);
    let context = |id: u32| ExpressionEffectContext {
        use_id: ExpressionUseId::new(id),
        domain,
        demand: novarocks_type_contract::EvaluationDemand::Value,
    };
    let mut uses = Vec::new();
    let mut bindings = Vec::new();
    let mut window_uses = Vec::new();
    let mut next_child = CHILD_USE_BASE;
    // One eager Value occurrence of `definition` and of each of its children.
    fn occurrence(
        fragment: &novarocks_physical_plan::Fragment,
        definition: ExprId,
        uses: &mut Vec<ExpressionInvocation<ExprId>>,
        next: &mut u32,
        context: &dyn Fn(u32) -> ExpressionEffectContext,
    ) -> ExpressionUseId {
        let id = *next;
        *next += 1;
        let children = references(fragment, definition)
            .into_iter()
            .map(|child| occurrence(fragment, child, uses, next, context))
            .collect::<Box<[_]>>();
        uses.push(ExpressionInvocation {
            context: context(id),
            definition,
            control: ControlShape::Eager,
            arguments: children,
        });
        ExpressionUseId::new(id)
    }
    for (ordinal, (site, root)) in roots.sites().iter().enumerate() {
        let id = u32::try_from(ordinal).unwrap();
        let definition = &fragment.expressions().get(root.expr).unwrap().kind;
        let mut children = Vec::new();
        for reference in references(&fragment, root.expr) {
            children.push(occurrence(
                &fragment,
                reference,
                &mut uses,
                &mut next_child,
                &context,
            ));
        }
        if matches!(definition, ExprKind::WindowCall { .. }) {
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
        &FixtureControl,
    )
    .unwrap();
    let root_uses = PhysicalRootUses::try_new(&fragment, flow, bindings, &FixtureControl).unwrap();
    let overflowing = shape
        .calls
        .iter()
        .flat_map(|call| call.args.iter())
        .any(|arg| matches!(arg, Arg::Overflowing(_)));
    let parameters = if overflowing || integral_literal_cast {
        SemanticParameters::try_new([(
            allow_throw().id,
            novarocks_type_contract::SemanticParameterValue::AllowThrowException(overflowing),
        )])
        .unwrap()
    } else {
        SemanticParameters::try_new([]).unwrap()
    };
    let mut frozen = Vec::new();
    for ((use_id, expression, children), call) in window_uses.into_iter().zip(&shape.calls) {
        let ExprKind::WindowCall {
            function,
            args,
            frame,
            ignore_nulls,
            aggregate_binding,
            ..
        } = &fragment.expressions().get(expression).unwrap().kind
        else {
            unreachable!()
        };
        let request = fragment
            .call_requests()
            .get(PhysicalCallDefinition::Expression(expression))
            .unwrap()
            .arguments
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
            argument_types: function.argument_types.clone(),
            result_type: FunctionResultType::Scalar(function.result_type.clone()),
            aggregate: aggregate_binding.as_deref().map(|binding| {
                novarocks_functions::AggregateBindingSelection {
                    state_argument_contract: binding.state_argument_contract,
                    intermediate_type: binding.intermediate_type.clone(),
                    state_format: binding.state_format.clone(),
                }
            }),
        });
        let argument_uses = children[..args.len()]
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
        // A call's frozen effects do not depend on its frame or DISTINCT.
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
            let frame = frame.map(|frame| {
                let offset = |id: ExprId| match fragment.expressions().get(id).unwrap().kind {
                    ExprKind::Literal(LiteralValue::Int64(value)) => u64::try_from(value).unwrap(),
                    _ => unreachable!("fixture offsets are literals"),
                };
                let bound = |bound: WindowBound<ExprId>| match bound {
                    WindowBound::UnboundedPreceding => WindowBound::UnboundedPreceding,
                    WindowBound::Preceding(id) => WindowBound::Preceding(offset(id)),
                    WindowBound::CurrentRow => WindowBound::CurrentRow,
                    WindowBound::Following(id) => WindowBound::Following(offset(id)),
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
                options: WindowCallOptions::try_new(frame, *ignore_nulls, &FixtureControl).unwrap(),
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
                        logical_argument_count: args.len(),
                        expected_result_type: None,
                    },
                    environment: &[],
                    parameters: &parameters,
                    decimal_overflow_policy: POLICY,
                    proof_scope: CallProofScope::Domain(domain),
                },
                Arc::clone(&selection),
                options,
                &FixtureControl,
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
    let calls = FrozenFragmentCalls::try_new(&fragment, &root_uses, frozen, &FixtureControl)
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
                expression_uses: root_uses,
                calls,
                pruning: FrozenFragmentPruning::try_new(id, vec![], &FixtureControl).unwrap(),
                cuts: FragmentCuts::default(),
                result: Some(result),
                parameters,
                scans: BTreeMap::new(),
                writes: BTreeMap::new(),
                annotations: Box::default(),
            },
            admission(),
            &FixtureControl,
        )
        .unwrap_or_else(|error| panic!("window package validates: {error:?}")),
    )
}

/// The ordered child definitions of one expression.
fn references(fragment: &novarocks_physical_plan::Fragment, definition: ExprId) -> Vec<ExprId> {
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
