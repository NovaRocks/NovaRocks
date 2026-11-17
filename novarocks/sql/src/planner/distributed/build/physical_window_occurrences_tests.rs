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

use super::super::{
    expression_occurrences::author_physical_occurrences_observed,
    physical_window_requests::{
        PhysicalWindowRequestError, author_physical_window_request_observed,
    },
};
use super::*;
use crate::functions::build_builtin_engine_function_catalog;
use arrow::{
    array::{Array, DictionaryArray, Int8Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Int8Type},
};
use novarocks_functions::{
    AggregateKernelPhase, ConstantPolicy, ConstantPool, EngineFunctionCatalog, FunctionArgument,
    FunctionArgumentType, FunctionResultType, PreparedPureKernel, WindowCallContract,
};
use novarocks_physical_plan::{
    AggregateBinding, AggregatePhase, BoundFunction, ConstantPoolId, ConstantPools,
    ConstantReference, Fragment, FragmentBuilder, FragmentId, FragmentSink, LiteralValue, NodeId,
    NodeKind, PhysicalRootUses, PipelineDopDomain, PlanLimits, ValueOrigin, WindowBound,
    WindowExpression, WindowFrame, WindowFrameExclusion, WindowFrameUnits, WindowSpec,
};
use novarocks_type_contract::{
    AggregateStateFormatId, ArgumentControl, CompilePhase, FunctionInstanceState,
    FunctionIntrinsicRowError, FunctionNullBehavior, FunctionValueType, PureCompileControl,
    SemanticParameterId, SemanticParameterKey, SemanticParameterValue,
};
use std::sync::Mutex;
const PHASE: CompilePhase = CompilePhase::FunctionSpecialization;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop, "callback after primary refusal");
        }
        trace.push((phase, units));
        match self.refusal.clone() {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}
impl Control {
    fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.trace.lock().unwrap().clone()
    }
}
fn ty(carrier: DataType, nullable: bool) -> FunctionValueType {
    FunctionValueType::new(carrier, nullable)
}

fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 8,
        max_array_nodes: 32,
        max_logical_elements: 4096,
        max_retained_buffer_bytes: 4 << 20,
        max_type_depth: 16,
        max_type_nodes: 64,
        max_dictionary_depth: 4,
        max_metadata_bytes: 65536,
        max_library_validation_work: 64 << 20,
        max_library_validation_bytes: 64 << 20,
    }
}

fn empty() -> (FragmentBuilder, NodeId, NodeId) {
    let mut builder = FragmentBuilder::new(FragmentId::new(71));
    let input = NodeId::new(7);
    let owner = NodeId::new(901);
    builder
        .add_values(
            input,
            Box::from([Box::<[ExprId]>::default()]),
            Box::default(),
        )
        .unwrap();
    (builder, input, owner)
}

fn literal(
    builder: &mut FragmentBuilder,
    owner: NodeId,
    ty: FunctionValueType,
    value: LiteralValue,
) -> ExprId {
    builder
        .add_expression(owner, ty, ExprKind::Literal(value))
        .unwrap()
}

fn finish(builder: FragmentBuilder, root: NodeId) -> Fragment {
    builder
        .finish_structure(
            root,
            FragmentSink::Noop,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            PlanLimits::FROZEN,
            &Control::default(),
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap()
}

fn uses(fragment: &Fragment, catalog: &EngineFunctionCatalog) -> PhysicalRootUses {
    author_physical_occurrences_observed(fragment, catalog, &Control::default())
        .unwrap()
        .root_uses
}

fn scalar(resolved: novarocks_functions::ResolvedFunctionBinding) -> BoundFunction {
    let FunctionResultType::Scalar(result_type) = resolved.selected.result_type else {
        panic!("scalar result")
    };
    BoundFunction {
        function_id: resolved.function_id,
        overload: resolved.selected.overload,
        kind: resolved.kind,
        argument_types: resolved.selected.argument_types,
        result_type,
        legacy_metadata: Some(novarocks_physical_plan::LegacyBindingMetadata {
            volatility: resolved.semantics.volatility,
            argument_evaluation: resolved.semantics.argument_evaluation,
            failure_behavior: resolved.semantics.failure_behavior,
            intrinsic_row_error: resolved.semantics.intrinsic_row_error,
            semantic_parameters: Box::default(),
        }),
    }
}
fn window_binding(
    catalog: &EngineFunctionCatalog,
    name: &str,
    types: &[FunctionValueType],
) -> BoundFunction {
    let args: Vec<_> = types
        .iter()
        .cloned()
        .map(|value_type| FunctionArgument::Value {
            value_type,
            constant: None,
        })
        .collect();
    scalar(
        SqlFunctionCatalog::resolve_window_binding(catalog, name, &args, &Control::default())
            .unwrap(),
    )
}
fn count_binding(catalog: &EngineFunctionCatalog, types: &[FunctionValueType]) -> AggregateBinding {
    let args: Vec<_> = types
        .iter()
        .cloned()
        .map(|value_type| FunctionArgument::Value {
            value_type,
            constant: None,
        })
        .collect();
    let resolved = catalog
        .resolve_aggregate_binding("count", args.len(), &args, &Control::default())
        .unwrap();
    let aggregate = resolved.selected.aggregate.as_ref().unwrap();
    let state_argument_contract = aggregate.state_argument_contract;
    let intermediate_type = aggregate.intermediate_type.clone();
    let state_format = AggregateStateFormatId::try_new(aggregate.state_format.as_str()).unwrap();
    AggregateBinding {
        state_interpretation: None,
        state_argument_contract,
        function: scalar(resolved),
        phase: AggregatePhase::Single,
        logical_argument_count: args.len() as u32,
        intermediate_type,
        state_format,
    }
}
// The fixture spells every authored WindowCall field at its call sites.
fn window(
    builder: &mut FragmentBuilder,
    input: NodeId,
    owner: NodeId,
    function: BoundFunction,
    args: &[ExprId],
    frame: Option<WindowFrame>,
    ignore_nulls: bool,
    aggregate: Option<AggregateBinding>,
    distinct: bool,
) -> ExprId {
    window_with_passthrough(
        builder,
        input,
        owner,
        function,
        args,
        frame,
        ignore_nulls,
        aggregate,
        distinct,
        &[],
    )
}

fn window_with_passthrough(
    builder: &mut FragmentBuilder,
    input: NodeId,
    owner: NodeId,
    function: BoundFunction,
    args: &[ExprId],
    frame: Option<WindowFrame>,
    ignore_nulls: bool,
    aggregate: Option<AggregateBinding>,
    distinct: bool,
    passthrough: &[novarocks_physical_plan::ValueId],
) -> ExprId {
    let result = function.result_type.clone();
    let root = builder
        .add_expression(
            owner,
            result.clone(),
            ExprKind::WindowCall {
                function,
                distinct,
                args: args.into(),
                function_order_by: Box::default(),
                frame,
                ignore_nulls,
                aggregate_binding: aggregate.map(Box::new),
            },
        )
        .unwrap();
    let output = builder
        .add_value(
            result,
            ValueOrigin::Expr {
                node: owner,
                expr: root,
            },
        )
        .unwrap();
    builder
        .add_row_widening(
            owner,
            input,
            passthrough
                .iter()
                .copied()
                .chain(std::iter::once(output))
                .collect(),
            NodeKind::Window(WindowSpec {
                partition_by: Box::default(),
                order_by: Box::default(),
                expressions: Box::from([WindowExpression {
                    expression: root,
                    output,
                }]),
            }),
        )
        .unwrap();
    root
}
fn request<'a>(
    source: &'a ExprNode,
    fragment: &Fragment,
    pools: &ConstantPools,
    control: &Control,
) -> Result<AuthoredPhysicalWindowRequest<'a>, PhysicalWindowRequestError> {
    let mut work = CompileCheckpoints::try_new(control, PHASE)?;
    let result =
        author_physical_window_request_observed(source, fragment, pools, policy(), &mut work);
    if matches!(&result, Err(PhysicalWindowRequestError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn pure_children(
    flow: &ExpressionControlFlow<ExprId>,
) -> BTreeMap<ExpressionUseId, ScopedExpressionEffects> {
    flow.uses()
        .iter()
        .map(|(&id, item)| (id, ScopedExpressionEffects::pure_value(item.context)))
        .collect()
}
fn root_use(roots: &PhysicalRootUses, root: ExprId) -> ExpressionUseId {
    *roots
        .flow()
        .uses()
        .iter()
        .find(|(_, item)| item.definition == root)
        .unwrap()
        .0
}
fn input<'a>(
    source: &'a ExprNode,
    request: &'a AuthoredPhysicalWindowRequest<'a>,
    flow: &'a ExpressionControlFlow<ExprId>,
    id: ExpressionUseId,
    children: &'a BTreeMap<ExpressionUseId, ScopedExpressionEffects>,
    params: &'a SemanticParameters,
) -> PhysicalWindowOccurrenceInput<'a> {
    PhysicalWindowOccurrenceInput {
        source,
        request,
        flow,
        use_id: id,
        child_effects: children,
        parameters: params,
        environment: &[],
        decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
        proof_scope: CallProofScope::Region(flow.uses()[&id].context.region),
    }
}
fn run(
    input: PhysicalWindowOccurrenceInput<'_>,
    catalog: &EngineFunctionCatalog,
    control: &Control,
) -> Result<FreshPhysicalWindowOccurrence, PhysicalWindowOccurrenceError> {
    let mut work = CompileCheckpoints::try_new(control, PHASE)?;
    let result = prepare_physical_window_occurrence_observed(input, catalog, &mut work);
    if matches!(&result, Err(PhysicalWindowOccurrenceError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn window_contract(result: &FreshPhysicalWindowOccurrence) -> &WindowCallContract {
    let PreparedPureKernel::Window(kernel) = result.preparation.prepared() else {
        panic!("actual WindowV1/ AggregateWindowV1 prepared representation")
    };
    kernel.contract()
}
fn prefixes(
    mut invoke: impl FnMut(
        &Control,
    ) -> Result<FreshPhysicalWindowOccurrence, PhysicalWindowOccurrenceError>,
    success: bool,
    sampled: bool,
) -> Vec<(CompilePhase, u32)> {
    let baseline = Control::default();
    assert_eq!(invoke(&baseline).is_ok(), success);
    let trace = baseline.trace();
    assert_eq!(trace.first(), Some(&(PHASE, 0)));
    assert!(trace.len() > 1);
    let indices: Vec<_> = if sampled {
        let mut ids = vec![0, trace.len() - 1];
        ids.extend(
            trace
                .iter()
                .enumerate()
                .filter_map(|(at, (_, units))| (*units == 256).then_some(at)),
        );
        ids.sort_unstable();
        ids.dedup();
        ids
    } else {
        (0..trace.len()).collect()
    };
    for at in indices {
        for cause in CAUSES {
            let control = Control {
                trace: Mutex::default(),
                refusal: Some((at, cause.clone())),
            };
            assert!(
                matches!(invoke(&control),Err(PhysicalWindowOccurrenceError::Control(actual)) if actual==cause),
                "at {at}: {cause:?}"
            );
            assert_eq!(control.trace(), trace[..=at]);
        }
    }
    trace
}
fn request_prefixes(source: &ExprNode, fragment: &Fragment, pools: &ConstantPools, success: bool) {
    let baseline = Control::default();
    assert_eq!(request(source, fragment, pools, &baseline).is_ok(), success);
    let trace = baseline.trace();
    for at in 0..trace.len() {
        for cause in CAUSES {
            let control = Control {
                trace: Mutex::default(),
                refusal: Some((at, cause.clone())),
            };
            assert!(
                matches!(request(source,fragment,pools,&control),Err(PhysicalWindowRequestError::Control(actual)) if actual==cause)
            );
            assert_eq!(control.trace(), trace[..=at]);
        }
    }
}
fn prepared(
    fragment: &Fragment,
    root: ExprId,
    roots: &PhysicalRootUses,
    pools: &ConstantPools,
    catalog: &EngineFunctionCatalog,
) -> FreshPhysicalWindowOccurrence {
    let source = fragment.expressions().get(root).unwrap();
    let req = request(source, fragment, pools, &Control::default()).unwrap();
    let params = SemanticParameters::try_new([]).unwrap();
    let children = pure_children(roots.flow());
    let result = run(
        input(
            source,
            &req,
            roots.flow(),
            root_use(roots, root),
            &children,
            &params,
        ),
        catalog,
        &Control::default(),
    )
    .unwrap();
    assert!(Arc::ptr_eq(
        req.selected(),
        result.preparation.call_contract().selected_owner()
    ));
    result
}

#[test]
fn window_occurrences_real_rank_row_number_keep_zero_channels_optional_frame_ignore_and_partition_effects()
 {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let pools = ConstantPools::empty();
    for name in ["rank", "row_number"] {
        for some_frame in [false, true] {
            for ignore in [false, true] {
                let (mut builder, leaf, owner) = empty();
                let frame = some_frame.then_some(WindowFrame {
                    units: WindowFrameUnits::Rows,
                    start: WindowBound::UnboundedPreceding,
                    end: WindowBound::CurrentRow,
                    exclusion: WindowFrameExclusion::NoOthers,
                });
                let root = window(
                    &mut builder,
                    leaf,
                    owner,
                    window_binding(&catalog, name, &[]),
                    &[],
                    frame,
                    ignore,
                    None,
                    false,
                );
                let fragment = finish(builder, owner);
                let roots = uses(&fragment, &catalog);
                let source = fragment.expressions().get(root).unwrap();
                let req = request(source, &fragment, &pools, &Control::default()).unwrap();
                assert!(std::ptr::eq(req.source(), source));
                assert_eq!(req.request().logical_argument_count, 0);
                assert!(req.request().arguments.is_empty());
                let result = prepared(&fragment, root, &roots, &pools, &catalog);
                let contract = window_contract(&result);
                assert_eq!(contract.options().ignore_nulls(), ignore);
                assert_eq!(contract.options().frame().is_some(), some_frame);
                if let Some(frame) = contract.options().frame() {
                    assert_eq!(frame.units, WindowFrameUnits::Rows);
                    assert_eq!(
                        frame.start,
                        novarocks_type_contract::WindowBound::UnboundedPreceding
                    );
                    assert_eq!(frame.end, novarocks_type_contract::WindowBound::CurrentRow);
                }
                assert!(contract.aggregate().is_none());
                assert_eq!(
                    result.frozen.effects.argument_control,
                    ArgumentControl::Window
                );
                assert_eq!(
                    result.frozen.effects.instance_state,
                    FunctionInstanceState::WindowPartition
                );
                assert_eq!(
                    result.frozen.effects.own_row_error,
                    FunctionIntrinsicRowError::NotRowEvaluated
                );
                assert_eq!(
                    result.frozen.effects.null_behavior,
                    FunctionNullBehavior::CalledOnNull
                );
            }
        }
    }
}

#[test]
fn window_occurrences_lead_lag_checked_default_nonzero_ordinal_keeps_original_d_source() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let integer = ty(DataType::Int64, false);
    let field = Arc::new(Field::new("default original", DataType::Int64, false));
    let pool = ConstantPool::try_new(
        field.clone(),
        integer.clone(),
        Int64Array::from(vec![999, 42, 777]).to_data(),
        policy(),
        CompilePhase::Validate,
        &Control::default(),
    )
    .unwrap();
    let mut pools = ConstantPools::empty();
    pools
        .insert(ConstantPoolId::new(u32::MAX), pool.clone())
        .unwrap();
    for name in ["lead", "lag"] {
        let (mut builder, leaf, owner) = empty();
        let text = ty(DataType::Utf8, true);
        let value = literal(
            &mut builder,
            owner,
            text.clone(),
            LiteralValue::Utf8("value".into()),
        );
        let offset = literal(&mut builder, owner, integer.clone(), LiteralValue::Int64(1));
        let default = builder
            .add_expression(
                owner,
                integer.clone(),
                ExprKind::Constant(ConstantReference {
                    pool: ConstantPoolId::new(u32::MAX),
                    ordinal: 1,
                }),
            )
            .unwrap();
        let root = window(
            &mut builder,
            leaf,
            owner,
            window_binding(&catalog, name, &[text, integer.clone(), integer.clone()]),
            &[value, offset, default],
            None,
            false,
            None,
            false,
        );
        let fragment = finish(builder, owner);
        let roots = uses(&fragment, &catalog);
        let req = request(
            fragment.expressions().get(root).unwrap(),
            &fragment,
            &pools,
            &Control::default(),
        )
        .unwrap();
        let FunctionArgument::Value {
            value_type,
            constant: Some(value),
        } = &req.request().arguments[2]
        else {
            panic!("checked D")
        };
        assert_eq!(value_type, &integer);
        assert_eq!(value.ordinal(), 1);
        assert_eq!(value.pool().backing_identity(), pool.backing_identity());
        assert!(Arc::ptr_eq(value.pool().field_ref(), &field));
        assert_eq!(
            value
                .int64_observed(CompilePhase::Validate, &Control::default())
                .unwrap(),
            Some(42)
        );
        let result = prepared(&fragment, root, &roots, &pools, &catalog);
        assert_eq!(
            window_contract(&result).call().selected().argument_types[2],
            FunctionArgumentType::Value(integer.clone())
        );
        assert_eq!(
            window_contract(&result).result_type().data_type().clone(),
            DataType::Utf8
        );
        assert!(window_contract(&result).result_type().nullable());
    }
}

#[test]
fn window_occurrences_ntile_constant_valid_null_and_dynamic_are_distinct_owner_paths() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let pools = ConstantPools::empty();
    let params = SemanticParameters::try_new([]).unwrap();
    for mode in 0..3 {
        let (mut builder, leaf, owner) = empty();
        let integer = ty(DataType::Int64, mode == 1);
        let literal_ = literal(
            &mut builder,
            owner,
            integer.clone(),
            if mode == 1 {
                LiteralValue::Null
            } else {
                LiteralValue::Int64(3)
            },
        );
        let arg = if mode == 2 {
            builder
                .add_expression(
                    owner,
                    integer.clone(),
                    ExprKind::Unary {
                        op: novarocks_physical_plan::UnaryOperator::Plus,
                        expr: literal_,
                    },
                )
                .unwrap()
        } else {
            literal_
        };
        let root = window(
            &mut builder,
            leaf,
            owner,
            window_binding(&catalog, "ntile", &[integer]),
            &[arg],
            None,
            false,
            None,
            false,
        );
        let fragment = finish(builder, owner);
        let roots = uses(&fragment, &catalog);
        let source = fragment.expressions().get(root).unwrap();
        let req = request(source, &fragment, &pools, &Control::default()).unwrap();
        let children = pure_children(roots.flow());
        let id = root_use(&roots, root);
        let outcome = run(
            input(source, &req, roots.flow(), id, &children, &params),
            &catalog,
            &Control::default(),
        );
        assert_eq!(outcome.is_ok(), mode == 0);
        if mode != 0 {
            assert!(matches!(
                outcome,
                Err(PhysicalWindowOccurrenceError::Function(_))
            ));
        }
        prefixes(
            |control| {
                run(
                    input(source, &req, roots.flow(), id, &children, &params),
                    &catalog,
                    control,
                )
            },
            mode == 0,
            false,
        );
    }
}

#[test]
fn window_occurrences_count_over_keeps_single_signature_state_format_and_complete_null_encoded_sources()
 {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let empty_pools = ConstantPools::empty();
    for value in [false, true] {
        for ignore in [false, true] {
            let (mut builder, leaf, owner) = empty();
            let integer = ty(DataType::Int64, true);
            let args = if value {
                vec![literal(
                    &mut builder,
                    owner,
                    integer.clone(),
                    LiteralValue::Int64(7),
                )]
            } else {
                vec![]
            };
            let binding = count_binding(
                &catalog,
                if value {
                    std::slice::from_ref(&integer)
                } else {
                    &[]
                },
            );
            let intermediate = binding.intermediate_type.clone();
            let state_format = binding.state_format.as_str().to_owned();
            let root = window(
                &mut builder,
                leaf,
                owner,
                binding.function.clone(),
                &args,
                None,
                ignore,
                Some(binding),
                false,
            );
            let fragment = finish(builder, owner);
            let roots = uses(&fragment, &catalog);
            let result = prepared(&fragment, root, &roots, &empty_pools, &catalog);
            let contract = window_contract(&result);
            let aggregate = contract.aggregate().unwrap();
            assert_eq!(aggregate.phase(), AggregateKernelPhase::Single);
            assert_eq!(aggregate.intermediate_type(), &intermediate);
            assert_eq!(aggregate.state_format().as_str(), state_format);
            assert!(!aggregate.distinct());
            assert!(aggregate.order_keys().is_empty());
            assert_eq!(contract.options().ignore_nulls(), ignore);
            assert_eq!(contract.call().logical_argument_count(), usize::from(value));
        }
    }
    let dictionary = DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![Some(0), Some(1)]),
        Arc::new(StringArray::from(vec![Some("value"), None])),
    )
    .unwrap();
    let encoded = ty(dictionary.data_type().clone(), true);
    let pool = ConstantPool::try_new(
        Arc::new(Field::new("encoded", dictionary.data_type().clone(), true)),
        encoded.clone(),
        dictionary.to_data(),
        policy(),
        CompilePhase::Validate,
        &Control::default(),
    )
    .unwrap();
    let mut pools = ConstantPools::empty();
    pools.insert(ConstantPoolId::new(7), pool).unwrap();
    for encoded_root in [false, true] {
        let (mut builder, leaf, owner) = empty();
        let argument_type = if encoded_root {
            encoded.clone()
        } else {
            ty(DataType::Null, true)
        };
        let arg = builder
            .add_expression(
                owner,
                argument_type.clone(),
                if encoded_root {
                    ExprKind::Constant(ConstantReference {
                        pool: ConstantPoolId::new(7),
                        ordinal: 0,
                    })
                } else {
                    ExprKind::Literal(LiteralValue::Null)
                },
            )
            .unwrap();
        let binding = count_binding(&catalog, &[argument_type]);
        let root = window(
            &mut builder,
            leaf,
            owner,
            binding.function.clone(),
            &[arg],
            None,
            false,
            Some(binding),
            false,
        );
        let fragment = finish(builder, owner);
        let roots = uses(&fragment, &catalog);
        let source = fragment.expressions().get(root).unwrap();
        let req = request(source, &fragment, &pools, &Control::default()).unwrap();
        let children = pure_children(roots.flow());
        let params = SemanticParameters::try_new([]).unwrap();
        // This validates pure selected preparation, not a runtime Dictionary capability.
        let prepared = run(
            input(
                source,
                &req,
                roots.flow(),
                root_use(&roots, root),
                &children,
                &params,
            ),
            &catalog,
            &Control::default(),
        )
        .unwrap();
        let aggregate = window_contract(&prepared).aggregate().unwrap();
        assert_eq!(aggregate.phase(), AggregateKernelPhase::Single);
        assert_eq!(aggregate.call().logical_argument_count(), 1);
        assert_eq!(
            aggregate.call().selected().argument_types.as_ref(),
            &[req.request().arguments[0].argument_type()]
        );
    }
}

#[test]
fn window_occurrences_frame_pool_offsets_are_ordered_children_not_function_channels() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let integer = ty(DataType::Int64, false);
    let pool = ConstantPool::try_new(
        Arc::new(Field::new("offset", DataType::Int64, false)),
        integer.clone(),
        Int64Array::from(vec![99, 2, 1, 0]).to_data(),
        policy(),
        CompilePhase::Validate,
        &Control::default(),
    )
    .unwrap();
    let mut pools = ConstantPools::empty();
    pools.insert(ConstantPoolId::new(0), pool).unwrap();
    for bad_zero in [false, true] {
        let (mut builder, leaf, owner) = empty();
        let mut ids = Vec::new();
        for ordinal in [if bad_zero { 3 } else { 1 }, 2] {
            ids.push(
                builder
                    .add_expression(
                        owner,
                        integer.clone(),
                        ExprKind::Constant(ConstantReference {
                            pool: ConstantPoolId::new(0),
                            ordinal,
                        }),
                    )
                    .unwrap(),
            );
        }
        let frame = WindowFrame {
            units: WindowFrameUnits::Rows,
            start: WindowBound::Preceding(ids[0]),
            end: WindowBound::Preceding(ids[1]),
            exclusion: WindowFrameExclusion::NoOthers,
        };
        let root = window(
            &mut builder,
            leaf,
            owner,
            window_binding(&catalog, "rank", &[]),
            &[],
            Some(frame),
            true,
            None,
            false,
        );
        let fragment = finish(builder, owner);
        let roots = uses(&fragment, &catalog);
        let source = fragment.expressions().get(root).unwrap();
        let result = request(source, &fragment, &pools, &Control::default());
        if bad_zero {
            assert!(matches!(
                result,
                Err(PhysicalWindowRequestError::Argument(_))
            ));
            request_prefixes(source, &fragment, &pools, false);
        } else {
            let req = result.unwrap();
            assert_eq!(req.request().logical_argument_count, 0);
            assert!(req.request().arguments.is_empty());
            let id = root_use(&roots, root);
            let invocation = &roots.flow().uses()[&id];
            assert_eq!(invocation.arguments.len(), 2);
            for (ordinal, &child) in invocation.arguments.iter().enumerate() {
                assert_eq!(roots.flow().uses()[&child].definition, ids[ordinal]);
            }
            let prepared = prepared(&fragment, root, &roots, &pools, &catalog);
            let frame = window_contract(&prepared).options().frame().unwrap();
            assert_eq!(
                frame.start,
                novarocks_type_contract::WindowBound::Preceding(2)
            );
            assert_eq!(
                frame.end,
                novarocks_type_contract::WindowBound::Preceding(1)
            );
            // The generic flow remains lawful while the source association is
            // intentionally wrong. This tests the leaf's own ordered-edge gate,
            // not a claim that the modified graph passes RootUses admission.
            let mut invocations: Vec<_> = roots.flow().uses().values().cloned().collect();
            invocations
                .iter_mut()
                .find(|item| item.context.use_id == id)
                .unwrap()
                .arguments
                .reverse();
            let wrong_flow = ExpressionControlFlow::try_new(
                roots.flow().regions().values().copied().collect(),
                invocations,
                fragment.expressions(),
                PHASE,
                &Control::default(),
            )
            .unwrap();
            let wrong_children = pure_children(&wrong_flow);
            let params = SemanticParameters::try_new([]).unwrap();
            assert!(matches!(
                run(
                    input(source, &req, &wrong_flow, id, &wrong_children, &params),
                    &catalog,
                    &Control::default()
                ),
                Err(PhysicalWindowOccurrenceError::InvalidSource(
                    "window source child order differs from actual edge"
                ))
            ));
            prefixes(
                |control| {
                    run(
                        input(source, &req, &wrong_flow, id, &wrong_children, &params),
                        &catalog,
                        control,
                    )
                },
                false,
                false,
            );
            request_prefixes(source, &fragment, &pools, true);
        }
    }
}

#[test]
fn window_occurrences_foreign_source_missing_ordered_effects_environment_and_proof_refuse() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let (mut builder, leaf, owner) = empty();
    let offset = literal(
        &mut builder,
        owner,
        ty(DataType::Int64, false),
        LiteralValue::Int64(1),
    );
    let root = window(
        &mut builder,
        leaf,
        owner,
        window_binding(&catalog, "rank", &[]),
        &[],
        Some(WindowFrame {
            units: WindowFrameUnits::Rows,
            start: WindowBound::Preceding(offset),
            end: WindowBound::CurrentRow,
            exclusion: WindowFrameExclusion::NoOthers,
        }),
        false,
        None,
        false,
    );
    let fragment = finish(builder, owner);
    let roots = uses(&fragment, &catalog);
    let source = fragment.expressions().get(root).unwrap();
    let pools = ConstantPools::empty();
    let req = request(source, &fragment, &pools, &Control::default()).unwrap();
    let foreign = source.clone();
    let foreign_req = request(&foreign, &fragment, &pools, &Control::default()).unwrap();
    let id = root_use(&roots, root);
    let params = SemanticParameters::try_new([]).unwrap();
    let children = pure_children(roots.flow());
    assert!(matches!(
        run(
            input(source, &foreign_req, roots.flow(), id, &children, &params),
            &catalog,
            &Control::default()
        ),
        Err(PhysicalWindowOccurrenceError::InvalidSource(_))
    ));
    prefixes(
        |control| {
            run(
                input(source, &foreign_req, roots.flow(), id, &children, &params),
                &catalog,
                control,
            )
        },
        false,
        false,
    );
    let child = roots.flow().uses()[&id].arguments[0];
    let mut missing = children.clone();
    missing.remove(&child);
    assert!(
        matches!(run(input(source,&req,roots.flow(),id,&missing,&params),&catalog,&Control::default()),Err(PhysicalWindowOccurrenceError::MissingChildEffects(actual)) if actual==child)
    );
    prefixes(
        |control| {
            run(
                input(source, &req, roots.flow(), id, &missing, &params),
                &catalog,
                control,
            )
        },
        false,
        false,
    );
    let mut forged = children.clone();
    forged.insert(
        child,
        ScopedExpressionEffects::pure_value(roots.flow().uses()[&id].context),
    );
    assert!(matches!(
        run(
            input(source, &req, roots.flow(), id, &forged, &params),
            &catalog,
            &Control::default()
        ),
        Err(PhysicalWindowOccurrenceError::Effects(_))
    ));
    let refs = [SemanticParameterRef {
        id: SemanticParameterId::new(7),
        expected_key: SemanticParameterKey::TimeZone,
    }];
    let params =
        SemanticParameters::try_new([(refs[0].id, SemanticParameterValue::TimeZone("UTC".into()))])
            .unwrap();
    let mut call = input(source, &req, roots.flow(), id, &children, &params);
    call.environment = &refs;
    assert!(matches!(
        run(call, &catalog, &Control::default()),
        Err(PhysicalWindowOccurrenceError::Function(_))
    ));
    let mut call = input(source, &req, roots.flow(), id, &children, &params);
    call.proof_scope =
        CallProofScope::Region(novarocks_type_contract::EvaluationRegionId::new(u32::MAX));
    assert!(matches!(
        run(call, &catalog, &Control::default()),
        Err(PhysicalWindowOccurrenceError::Function(_))
    ));
    prefixes(
        |control| {
            let mut call = input(source, &req, roots.flow(), id, &children, &params);
            call.environment = &refs;
            run(call, &catalog, control)
        },
        false,
        false,
    );
    let mut absent = input(source, &req, roots.flow(), id, &children, &params);
    absent.use_id = ExpressionUseId::new(u32::MAX);
    assert!(
        matches!(run(absent,&catalog,&Control::default()),Err(PhysicalWindowOccurrenceError::MissingUse(actual)) if actual==ExpressionUseId::new(u32::MAX))
    );
}

#[test]
fn window_occurrences_actual_rank_success_and_invalid_option_callbacks_keep_three_first_causes() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let pools = ConstantPools::empty();
    let params = SemanticParameters::try_new([]).unwrap();
    for exclusion in [
        WindowFrameExclusion::NoOthers,
        WindowFrameExclusion::CurrentRow,
    ] {
        let (mut builder, leaf, owner) = empty();
        let root = window(
            &mut builder,
            leaf,
            owner,
            window_binding(&catalog, "rank", &[]),
            &[],
            Some(WindowFrame {
                units: WindowFrameUnits::Rows,
                start: WindowBound::UnboundedPreceding,
                end: WindowBound::CurrentRow,
                exclusion,
            }),
            false,
            None,
            false,
        );
        let fragment = finish(builder, owner);
        let roots = uses(&fragment, &catalog);
        let source = fragment.expressions().get(root).unwrap();
        let req = request(source, &fragment, &pools, &Control::default()).unwrap();
        let children = pure_children(roots.flow());
        let id = root_use(&roots, root);
        prefixes(
            |control| {
                run(
                    input(source, &req, roots.flow(), id, &children, &params),
                    &catalog,
                    control,
                )
            },
            exclusion == WindowFrameExclusion::NoOthers,
            false,
        );
        request_prefixes(source, &fragment, &pools, true);
    }
}

#[test]
fn window_occurrences_wide_complete_source_types_sample_actual_comparison_quantum() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let fields: Vec<_> = (0..320)
        .map(|at| {
            Arc::new(
                Field::new(format!("child-{at}"), DataType::Int64, true).with_metadata(
                    std::collections::HashMap::from([("provider.id".into(), at.to_string())]),
                ),
            )
        })
        .collect();
    let source_type = ty(DataType::Struct(fields.into()), true);
    let mut builder = FragmentBuilder::new(FragmentId::new(71));
    let leaf = NodeId::new(7);
    let owner = NodeId::new(901);
    let initial = literal(&mut builder, leaf, source_type.clone(), LiteralValue::Null);
    let value = builder
        .add_value(
            source_type.clone(),
            ValueOrigin::NodeOutput {
                node: leaf,
                output_ordinal: 0,
            },
        )
        .unwrap();
    builder
        .add_values(leaf, Box::from([Box::from([initial])]), Box::from([value]))
        .unwrap();
    let arg = builder
        .add_expression(owner, source_type.clone(), ExprKind::Value(value))
        .unwrap();
    let root = window_with_passthrough(
        &mut builder,
        leaf,
        owner,
        window_binding(&catalog, "lead", std::slice::from_ref(&source_type)),
        &[arg],
        None,
        false,
        None,
        false,
        &[value],
    );
    let fragment = finish(builder, owner);
    let roots = uses(&fragment, &catalog);
    let source = fragment.expressions().get(root).unwrap();
    let pools = ConstantPools::empty();
    let req = request(source, &fragment, &pools, &Control::default()).unwrap();
    let FunctionArgument::Value {
        value_type,
        constant,
    } = &req.request().arguments[0]
    else {
        panic!("value")
    };
    assert!(constant.is_none());
    assert_eq!(value_type, &source_type);
    let children = pure_children(roots.flow());
    let params = SemanticParameters::try_new([]).unwrap();
    let trace = prefixes(
        |control| {
            run(
                input(
                    source,
                    &req,
                    roots.flow(),
                    root_use(&roots, root),
                    &children,
                    &params,
                ),
                &catalog,
                control,
            )
        },
        true,
        true,
    );
    assert!(trace.contains(&(PHASE, 256)));
}

fn analyze_window_admission_sql(sql: &str) -> Result<(), crate::analyze_error::AnalyzeError> {
    let functions = build_builtin_engine_function_catalog()
        .unwrap()
        .snapshot_for_scalar_presence();
    let tables = crate::catalog::memory::PlannerMemoryCatalog::default();
    let statements = novarocks_parser::parse(sql).unwrap();
    let [novarocks_parser::ast::Statement::Query(query)] = statements.as_slice() else {
        panic!("an authored SQL query");
    };
    crate::analyzer::analyze_with_function_catalog(
        query,
        &tables,
        "default",
        functions.as_ref(),
        crate::constant::test_constant_policy(),
        &crate::compiler::SqlCompileControl::unbounded(),
    )
    .map(|_| ())
}

#[test]
fn window_missing_profiles_are_named_at_actual_sql_binding_before_package_emission() {
    for sql in [
        "SELECT lead('x', 1, 1.5) OVER ()",
        "SELECT lead(TRUE, 1, FALSE) OVER ()",
        "SELECT min('x') OVER ()",
        "SELECT max('x') OVER ()",
    ] {
        let error = analyze_window_admission_sql(sql).expect_err("static capability refusal");
        assert_eq!(
            error.kind(),
            crate::analyze_error::AnalyzeErrorKind::UnavailableImplementation,
            "{sql}"
        );
        assert_eq!(
            error.code().as_str(),
            "sql.analyze.unavailable_implementation",
            "{sql}"
        );
        assert!(
            error.span().is_some(),
            "the original OVER call span is retained"
        );
    }
    for sql in [
        "SELECT min('x')",
        "SELECT max('x')",
        "SELECT min(7) OVER ()",
        "SELECT max(7) OVER ()",
        "SELECT lead('x') OVER ()",
        "SELECT lag(7, 1, 0) OVER ()",
    ] {
        analyze_window_admission_sql(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
    let error = analyze_window_admission_sql("SELECT lead(missing, 1, 0) OVER ()")
        .expect_err("unknown input column");
    assert_eq!(
        error.kind(),
        crate::analyze_error::AnalyzeErrorKind::UnknownColumn
    );
}
