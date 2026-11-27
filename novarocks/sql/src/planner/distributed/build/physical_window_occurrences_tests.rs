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

use super::super::contract_lowering::{NodeExpressionEffects, node_effects_with_test_summaries};
use super::super::{
    expression_occurrences::author_physical_occurrences_observed,
    physical_window_requests::{
        PhysicalWindowRequestError, author_physical_window_request_observed,
    },
};
use super::*;
use crate::functions::build_builtin_engine_function_catalog;
use crate::test_constants as checked;
use arrow::{
    array::{Array, DictionaryArray, Int8Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Int8Type},
};
use novarocks_functions::ScopedExpressionEffects;
use novarocks_functions::{
    AggregateKernelPhase, ConstantPool, ConstantValidation, EngineFunctionCatalog,
    FunctionArgument, FunctionArgumentType, FunctionResultType, PreparedPureKernel,
    WindowCallContract,
};
use novarocks_type_contract::{
    AggregateStateFormatId, ArgumentControl, CompilePhase, EvaluationDemand, FunctionInstanceState,
    FunctionIntrinsicRowError, FunctionNullBehavior, FunctionValueType, PureCompileControl,
    SemanticParameterId, SemanticParameterKey, SemanticParameterValue,
};
use std::collections::BTreeMap;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};
use {
    novarocks_expression_contract::{BoundFunction, ExprId, ExprKind, RootOrdinal, RootScope},
    novarocks_physical_plan::{
        AggregateBinding, AggregatePhase, ConstantPoolId, ConstantPools, ConstantReference,
        FragmentId, FragmentSink, NodeId, NodeKind, PipelineDopDomain, PlanLimits, ValueOrigin,
        WindowBound, WindowCallSpec, WindowFrame, WindowFrameExclusion, WindowFrameUnits,
        WindowSpec,
    },
};
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

fn policy() -> ConstantValidation {
    novarocks_constant_contract::ConstantValidation::bounded(
        novarocks_constant_contract::ConstantPolicy {
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
        },
    )
}

struct FragmentBuilder {
    builder: novarocks_physical_plan::FragmentBuilder,
    pools: ConstantPools,
}
impl Deref for FragmentBuilder {
    type Target = novarocks_physical_plan::FragmentBuilder;
    fn deref(&self) -> &Self::Target {
        &self.builder
    }
}
impl DerefMut for FragmentBuilder {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.builder
    }
}
impl FragmentBuilder {
    fn new(id: FragmentId) -> Self {
        Self {
            builder: novarocks_physical_plan::FragmentBuilder::new(id),
            pools: ConstantPools::empty(),
        }
    }
    fn add_expression(
        &mut self,
        node: NodeId,
        ty: FunctionValueType,
        kind: ExprKind,
    ) -> Result<ExprId, novarocks_expression_contract::UnitError> {
        self.builder.unit(node).unwrap().add(None, ty, kind)
    }
    fn constant_kind(&mut self, ty: &FunctionValueType, source: checked::Source) -> ExprKind {
        checked::insert(&mut self.pools, source(ty))
    }
}
struct Fragment {
    fragment: novarocks_physical_plan::Fragment,
    pools: ConstantPools,
    effects: BTreeMap<NodeId, NodeExpressionEffects>,
}
impl Deref for Fragment {
    type Target = novarocks_physical_plan::Fragment;
    fn deref(&self) -> &Self::Target {
        &self.fragment
    }
}
impl Fragment {
    fn constants(&self) -> &ConstantPools {
        &self.pools
    }
}
fn empty() -> (FragmentBuilder, NodeId, NodeId) {
    let mut builder = FragmentBuilder::new(FragmentId::new(71));
    let input = NodeId::new(7);
    let owner = NodeId::new(901);
    builder
        .add_values(
            input,
            Box::from([Box::<[RootOrdinal]>::default()]),
            Box::default(),
        )
        .unwrap();
    (builder, input, owner)
}
fn literal(
    builder: &mut FragmentBuilder,
    owner: NodeId,
    ty: FunctionValueType,
    value: checked::Source,
) -> ExprId {
    let kind = builder.constant_kind(&ty, value);
    builder.add_expression(owner, ty, kind).unwrap()
}
fn root(
    builder: &mut FragmentBuilder,
    node: NodeId,
    definition: ExprId,
    scope: RootScope,
) -> RootOrdinal {
    builder
        .unit(node)
        .unwrap()
        .root(definition, EvaluationDemand::Value, scope)
        .unwrap()
}
fn finish(mut builder: FragmentBuilder, root: NodeId, external: &ConstantPools) -> Fragment {
    let mut pools = external.clone();
    checked::merge(&mut pools, &builder.pools);
    let parameters = SemanticParameters::try_new([]).unwrap();
    let nodes = builder
        .construction_nodes()
        .map(|(id, _)| *id)
        .collect::<Vec<_>>();
    let mut effects = BTreeMap::new();
    for node in nodes {
        if builder.construction_unit(node).is_none() {
            continue;
        }
        let ports = builder.node_ports(node).unwrap();
        let draft = builder.take_unit_builder(node).unwrap();
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
        let sealed = draft.seal(ports, &mut work).unwrap();
        let authored = super::super::expression_component_test_fixture::author_component_calls(
            &sealed,
            &pools,
            &parameters,
            DecimalOverflowPolicy::OutputNull,
            &mut work,
        )
        .unwrap();
        let unit = Arc::new(sealed.finish(authored.claims, &mut work).unwrap());
        work.finish().unwrap();
        effects.insert(
            node,
            node_effects_with_test_summaries(unit.clone(), authored.summaries),
        );
        builder.install_unit(node, unit).unwrap();
    }
    let fragment = builder
        .builder
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
        .unwrap();
    Fragment {
        fragment,
        pools,
        effects,
    }
}
fn uses<'a>(
    fragment: &'a Fragment,
    catalog: &EngineFunctionCatalog,
) -> AuthoredPhysicalOccurrences<'a> {
    author_physical_occurrences_observed(fragment, catalog, &Control::default()).unwrap()
}
fn window_source(fragment: &Fragment, site: PhysicalCallSite) -> (&PhysicalNode, &WindowCallSpec) {
    let PhysicalCallSite::Window { node, call } = site else {
        panic!("Window fixture site")
    };
    let node = &fragment.nodes()[&node];
    let NodeKind::Window(spec) = &node.kind else {
        panic!("Window fixture node")
    };
    (node, &spec.calls[call as usize])
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
        legacy_metadata: Some(novarocks_expression_contract::LegacyBindingMetadata {
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
) -> PhysicalCallSite {
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
) -> PhysicalCallSite {
    let result = function.result_type.clone();
    let arguments = args
        .iter()
        .map(|&id| root(builder, owner, id, RootScope::Port(0)))
        .collect();
    let output = builder
        .add_value(
            result,
            ValueOrigin::NodeOutput {
                node: owner,
                output_ordinal: u32::try_from(passthrough.len()).unwrap(),
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
                calls: Box::from([WindowCallSpec {
                    binding: function,
                    distinct,
                    arguments,
                    order_by: Box::default(),
                    frame,
                    ignore_nulls,
                    aggregate_binding: aggregate.map(Box::new),
                    output,
                }]),
            }),
        )
        .unwrap();
    PhysicalCallSite::Window {
        node: owner,
        call: 0,
    }
}

fn request<'a>(
    source: &'a WindowCallSpec,
    fragment: &Fragment,
    pools: &ConstantPools,
    control: &Control,
) -> Result<AuthoredPhysicalWindowRequest<'a>, PhysicalWindowRequestError> {
    let mut sources = pools.clone();
    checked::merge(&mut sources, fragment.constants());
    let mut work = CompileCheckpoints::try_new(control, PHASE)?;
    let result = author_physical_window_request_observed(
        source,
        window_node(fragment),
        &sources,
        policy(),
        &mut work,
    );
    if matches!(&result, Err(PhysicalWindowRequestError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn window_node(fragment: &Fragment) -> &PhysicalNode {
    fragment
        .nodes()
        .values()
        .find(|node| matches!(node.kind, NodeKind::Window(_)))
        .unwrap()
}
fn pure_children(fragment: &Fragment) -> BTreeMap<NodeId, NodeExpressionEffects> {
    fragment
        .effects
        .iter()
        .map(|(&node, effects)| {
            (
                node,
                node_effects_with_test_summaries(
                    fragment.nodes()[&node]
                        .expressions
                        .as_ref()
                        .unwrap()
                        .clone(),
                    effects.summaries().clone(),
                ),
            )
        })
        .collect()
}
fn input<'a>(
    source: &'a WindowCallSpec,
    request: &'a AuthoredPhysicalWindowRequest<'a>,
    occurrences: &'a AuthoredPhysicalOccurrences<'a>,
    site: PhysicalCallSite,
    children: &'a BTreeMap<NodeId, NodeExpressionEffects>,
    params: &'a SemanticParameters,
) -> PhysicalWindowOccurrenceInput<'a> {
    let context = occurrences
        .relational_contexts
        .iter()
        .find(|(actual, _)| *actual == site)
        .unwrap()
        .1;
    PhysicalWindowOccurrenceInput {
        node: &occurrences.fragment().nodes()[&site.node()],
        site,
        source,
        request,
        occurrences,
        child_effects: children.get(&site.node()),
        parameters: params,
        environment: &[],
        decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
        proof_scope: CallProofScope::Relation(context.identity),
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
fn request_prefixes(
    source: &WindowCallSpec,
    fragment: &Fragment,
    pools: &ConstantPools,
    success: bool,
) {
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
    root: PhysicalCallSite,
    roots: &AuthoredPhysicalOccurrences,
    pools: &ConstantPools,
    catalog: &EngineFunctionCatalog,
) -> FreshPhysicalWindowOccurrence {
    let source = window_source(fragment, root).1;
    let req = request(source, fragment, pools, &Control::default()).unwrap();
    let params = SemanticParameters::try_new([]).unwrap();
    let children = pure_children(fragment);
    let result = run(
        input(source, &req, roots, root, &children, &params),
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
                let fragment = finish(builder, owner, &pools);
                let roots = uses(&fragment, &catalog);
                let source = window_source(&fragment, root).1;
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
        let nullable_integer = ty(DataType::Int64, true);
        let value = literal(
            &mut builder,
            owner,
            nullable_integer.clone(),
            checked::i64(11),
        );
        let offset = literal(&mut builder, owner, integer.clone(), checked::i64(1));
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
            window_binding(
                &catalog,
                name,
                &[nullable_integer, integer.clone(), integer.clone()],
            ),
            &[value, offset, default],
            None,
            false,
            None,
            false,
        );
        let fragment = finish(builder, owner, &pools);
        let roots = uses(&fragment, &catalog);
        let req = request(
            window_source(&fragment, root).1,
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
            DataType::Int64
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
        let integer = ty(DataType::Int64, mode == 1);
        let (mut builder, leaf, owner, arg) = if mode == 2 {
            // A runtime parameter is an actual input Value. Unary Plus is not
            // an emitted SQL primitive and cannot manufacture a dynamic source.
            let mut builder = FragmentBuilder::new(FragmentId::new(71));
            let leaf = NodeId::new(7);
            let owner = NodeId::new(901);
            let seed = literal(&mut builder, leaf, integer.clone(), checked::i64(3));
            let seed_root = root(&mut builder, leaf, seed, RootScope::AllPorts);
            let value = builder
                .add_value(
                    integer.clone(),
                    ValueOrigin::NodeOutput {
                        node: leaf,
                        output_ordinal: 0,
                    },
                )
                .unwrap();
            builder
                .add_values(
                    leaf,
                    Box::from([Box::from([seed_root])]),
                    Box::from([value]),
                )
                .unwrap();
            let arg = builder
                .add_expression(owner, integer.clone(), ExprKind::input(value))
                .unwrap();
            (builder, leaf, owner, arg)
        } else {
            let (mut builder, leaf, owner) = empty();
            let arg = literal(
                &mut builder,
                owner,
                integer.clone(),
                if mode == 1 {
                    checked::null()
                } else {
                    checked::i64(3)
                },
            );
            (builder, leaf, owner, arg)
        };
        let passthrough = builder
            .construction_nodes()
            .find(|(id, _)| **id == leaf)
            .unwrap()
            .1
            .output
            .columns
            .clone();
        let root = window_with_passthrough(
            &mut builder,
            leaf,
            owner,
            window_binding(&catalog, "ntile", &[integer]),
            &[arg],
            None,
            false,
            None,
            false,
            &passthrough,
        );
        let fragment = finish(builder, owner, &pools);
        let roots = uses(&fragment, &catalog);
        let source = window_source(&fragment, root).1;
        let req = request(source, &fragment, &pools, &Control::default()).unwrap();
        let FunctionArgument::Value {
            value_type,
            constant,
        } = &req.request().arguments[0]
        else {
            panic!("original NTILE value channel")
        };
        assert_eq!(value_type, &ty(DataType::Int64, mode == 1));
        assert_eq!(constant.is_none(), mode == 2);
        if let Some(value) = constant {
            assert_eq!(
                value.int64_observed(PHASE, &Control::default()).unwrap(),
                (mode == 0).then_some(3)
            );
        }
        let children = pure_children(&fragment);

        let outcome = run(
            input(source, &req, &roots, root, &children, &params),
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
                    input(source, &req, &roots, root, &children, &params),
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
                    checked::i64(7),
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
            let fragment = finish(builder, owner, &empty_pools);
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
        let kind = if encoded_root {
            ExprKind::Constant(ConstantReference {
                pool: ConstantPoolId::new(7),
                ordinal: 0,
            })
        } else {
            builder.constant_kind(&argument_type, checked::null())
        };
        let arg = builder
            .add_expression(owner, argument_type.clone(), kind)
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
        let fragment = finish(builder, owner, &pools);
        let roots = uses(&fragment, &catalog);
        let source = window_source(&fragment, root).1;
        let req = request(source, &fragment, &pools, &Control::default()).unwrap();
        let children = pure_children(&fragment);
        let params = SemanticParameters::try_new([]).unwrap();
        // This validates pure selected preparation, not a runtime Dictionary capability.
        let prepared = run(
            input(source, &req, &roots, root, &children, &params),
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
        integer,
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
        let references = [
            ConstantReference {
                pool: ConstantPoolId::new(0),
                ordinal: if bad_zero { 3 } else { 1 },
            },
            ConstantReference {
                pool: ConstantPoolId::new(0),
                ordinal: 2,
            },
        ];
        let frame = WindowFrame {
            units: WindowFrameUnits::Rows,
            start: WindowBound::Preceding(references[0]),
            end: WindowBound::Preceding(references[1]),
            exclusion: WindowFrameExclusion::NoOthers,
        };
        let site = window(
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
        let fragment = finish(builder, owner, &pools);
        let occurrences = uses(&fragment, &catalog);
        let (node, source) = window_source(&fragment, site);
        assert!(
            node.expressions.is_none(),
            "static frame constants do not create scalar uses"
        );
        assert_eq!(
            source.frame.as_ref().unwrap().start,
            WindowBound::Preceding(references[0])
        );
        assert_eq!(
            source.frame.as_ref().unwrap().end,
            WindowBound::Preceding(references[1])
        );
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
            let prepared = prepared(&fragment, site, &occurrences, &pools, &catalog);
            let frame = window_contract(&prepared).options().frame().unwrap();
            assert_eq!(
                frame.start,
                novarocks_type_contract::WindowBound::Preceding(2)
            );
            assert_eq!(
                frame.end,
                novarocks_type_contract::WindowBound::Preceding(1)
            );
            // The original checked references, rather than a scalar flow,
            // preserve static start/end order. A different actual call source
            // cannot supply the original occurrence's preparation facts.
            let mut reversed = source.clone();
            reversed.frame.as_mut().unwrap().start = WindowBound::Preceding(references[1]);
            reversed.frame.as_mut().unwrap().end = WindowBound::Preceding(references[0]);
            assert!(matches!(
                request(&reversed, &fragment, &pools, &Control::default()),
                Err(PhysicalWindowRequestError::Function(
                    super::super::expression_occurrences::ExpressionOccurrenceError::Function(
                        novarocks_functions::FunctionSpecializationFailure::Kernel(
                            novarocks_functions::KernelFailure::InvalidProgram(ref diagnostic)
                        )
                    )
                )) if diagnostic.message() == "window frame start follows its end"
            ));
            request_prefixes(&reversed, &fragment, &pools, false);
            // This distinct source has a lawful static frame. Preparation may
            // succeed for it, but it is not the original call's source loan.
            let mut changed = source.clone();
            changed.frame.as_mut().unwrap().end = WindowBound::CurrentRow;
            let changed_req = request(&changed, &fragment, &pools, &Control::default()).unwrap();
            let children = pure_children(&fragment);
            let params = SemanticParameters::try_new([]).unwrap();
            assert!(matches!(
                run(
                    input(source, &changed_req, &occurrences, site, &children, &params),
                    &catalog,
                    &Control::default()
                ),
                Err(PhysicalWindowOccurrenceError::InvalidSource(
                    "window occurrence has a different actual node or call slot"
                ))
            ));
            prefixes(
                |control| {
                    run(
                        input(source, &changed_req, &occurrences, site, &children, &params),
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
fn expression_effects_runtime_window_requires_explicit_scope_and_keeps_its_lifecycle() {
    use super::super::physical_fragment_effects::{
        PhysicalFragmentEffectsError, PhysicalFragmentEffectsInput,
        PhysicalRelationalCallSourceScope, author_physical_fragment_effects_observed,
    };

    let catalog = build_builtin_engine_function_catalog().unwrap();
    let (mut builder, leaf, owner) = empty();
    let site = window(
        &mut builder,
        leaf,
        owner,
        window_binding(&catalog, "rank", &[]),
        &[],
        None,
        false,
        None,
        false,
    );
    let pools = ConstantPools::empty();
    let fragment = finish(builder, owner, &pools);
    let occurrences = uses(&fragment, &catalog);
    let (source, _) = window_source(&fragment, site);
    assert!(source.expressions.is_none());
    let [(actual_site, context)] = occurrences.relational_contexts.as_slice() else {
        panic!("one relational Window occurrence");
    };
    assert_eq!(*actual_site, site);
    let parameters = SemanticParameters::try_new([]).unwrap();
    let run = |scopes: &BTreeMap<_, _>, control: &Control| {
        author_physical_fragment_effects_observed(
            PhysicalFragmentEffectsInput {
                fragment: &fragment,
                occurrences: &occurrences,
                constants: &pools,
                parameters: &parameters,
                literal_policy: policy(),
                node_effects: &fragment.effects,
                relational_scopes: scopes,
            },
            &catalog,
            control,
        )
    };
    let missing = BTreeMap::new();
    let present = BTreeMap::from([(
        site,
        PhysicalRelationalCallSourceScope {
            source,
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            environment: &[],
            proof_scope: CallProofScope::Relation(context.identity),
        },
    )]);
    let baseline = Control::default();
    assert!(
        matches!(run(&missing, &baseline), Err(PhysicalFragmentEffectsError::MissingScope(actual)) if actual == site)
    );
    let missing_trace = baseline.trace();
    let baseline = Control::default();
    let authored = run(&present, &baseline).unwrap();
    assert_eq!(authored.calls.entries().len(), 1);
    assert_eq!(
        authored.calls.entries()[&site].effects.instance_state,
        FunctionInstanceState::WindowPartition
    );
    authored
        .calls
        .validate_fragment(&fragment, &Control::default())
        .unwrap();
    for (scopes, expected) in [(&missing, missing_trace), (&present, baseline.trace())] {
        assert_eq!(expected.first(), Some(&(PHASE, 0)));
        for (at, _) in expected.iter().enumerate() {
            for cause in CAUSES {
                let control = Control {
                    refusal: Some((at, cause.clone())),
                    ..Control::default()
                };
                assert!(
                    matches!(run(scopes, &control), Err(PhysicalFragmentEffectsError::Control(actual)) if actual == cause)
                );
                assert_eq!(control.trace(), expected[..=at]);
            }
        }
    }
}

#[test]
fn window_occurrences_foreign_source_missing_ordered_effects_environment_and_proof_refuse() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let (mut builder, leaf, owner) = empty();
    // Missing runtime child facts apply to the original LEAD argument root.
    // A checked static frame offset has no expression use to remove.
    let argument_type = ty(DataType::Int64, false);
    let argument = literal(&mut builder, owner, argument_type.clone(), checked::i64(1));
    let site = window(
        &mut builder,
        leaf,
        owner,
        window_binding(&catalog, "lead", &[argument_type]),
        &[argument],
        None,
        false,
        None,
        false,
    );
    let pools = ConstantPools::empty();
    let fragment = finish(builder, owner, &pools);
    let occurrences = uses(&fragment, &catalog);
    let (node, source) = window_source(&fragment, site);
    let unit = node.expressions.as_ref().unwrap();
    let child = unit.root_use_ids()[source.arguments[0].get() as usize];
    let context = unit.control_flow().uses()[&child].context;
    let req = request(source, &fragment, &pools, &Control::default()).unwrap();
    let foreign = source.clone();
    let foreign_req = request(&foreign, &fragment, &pools, &Control::default()).unwrap();
    let params = SemanticParameters::try_new([]).unwrap();
    let children = pure_children(&fragment);
    assert!(matches!(
        run(
            input(source, &foreign_req, &occurrences, site, &children, &params),
            &catalog,
            &Control::default()
        ),
        Err(PhysicalWindowOccurrenceError::InvalidSource(_))
    ));
    prefixes(
        |control| {
            run(
                input(source, &foreign_req, &occurrences, site, &children, &params),
                &catalog,
                control,
            )
        },
        false,
        false,
    );
    let mut missing = pure_children(&fragment);
    let mut missing_summaries = missing[&owner].summaries().clone();
    missing_summaries.remove(&child);
    missing.insert(
        owner,
        node_effects_with_test_summaries(unit.clone(), missing_summaries),
    );
    assert!(
        matches!(run(input(source, &req, &occurrences, site, &missing, &params), &catalog, &Control::default()), Err(PhysicalWindowOccurrenceError::MissingChildEffects(actual)) if actual == child)
    );
    prefixes(
        |control| {
            run(
                input(source, &req, &occurrences, site, &missing, &params),
                &catalog,
                control,
            )
        },
        false,
        false,
    );
    let mut forged = pure_children(&fragment);
    let mut forged_summaries = forged[&owner].summaries().clone();
    forged_summaries.insert(
        child,
        ScopedExpressionEffects::pure_value(novarocks_type_contract::ExpressionEffectContext {
            demand: EvaluationDemand::TruthOnly,
            ..context
        }),
    );
    forged.insert(
        owner,
        node_effects_with_test_summaries(unit.clone(), forged_summaries),
    );
    assert!(matches!(
        run(
            input(source, &req, &occurrences, site, &forged, &params),
            &catalog,
            &Control::default()
        ),
        Err(PhysicalWindowOccurrenceError::Effects(_))
    ));
    prefixes(
        |control| {
            run(
                input(source, &req, &occurrences, site, &forged, &params),
                &catalog,
                control,
            )
        },
        false,
        false,
    );
    let refs = [SemanticParameterRef {
        id: SemanticParameterId::new(7),
        expected_key: SemanticParameterKey::TimeZone,
    }];
    let params = SemanticParameters::try_new([(
        refs[0].id,
        SemanticParameterValue::TimeZone(
            novarocks_type_contract::TimeZoneKey::parse("UTC").unwrap(),
        ),
    )])
    .unwrap();
    let mut call = input(source, &req, &occurrences, site, &children, &params);
    call.environment = &refs;
    assert!(matches!(
        run(call, &catalog, &Control::default()),
        Err(PhysicalWindowOccurrenceError::Function(_))
    ));
    prefixes(
        |control| {
            let mut call = input(source, &req, &occurrences, site, &children, &params);
            call.environment = &refs;
            run(call, &catalog, control)
        },
        false,
        false,
    );
    let mut call = input(source, &req, &occurrences, site, &children, &params);
    call.proof_scope = CallProofScope::Region(context.region);
    assert!(matches!(
        run(call, &catalog, &Control::default()),
        Err(PhysicalWindowOccurrenceError::Function(_))
    ));
    prefixes(
        |control| {
            let mut call = input(source, &req, &occurrences, site, &children, &params);
            call.proof_scope = CallProofScope::Region(context.region);
            run(call, &catalog, control)
        },
        false,
        false,
    );
    let NodeKind::Window(spec) = &node.kind else {
        unreachable!()
    };
    let absent_site = PhysicalCallSite::Window {
        node: owner,
        call: u32::try_from(spec.calls.len()).unwrap(),
    };
    let mut absent = input(source, &req, &occurrences, site, &children, &params);
    absent.site = absent_site;
    assert!(matches!(
        run(absent, &catalog, &Control::default()),
        Err(PhysicalWindowOccurrenceError::InvalidSource(
            "window occurrence has a different actual node or call slot"
        ))
    ));
    prefixes(
        |control| {
            let mut call = input(source, &req, &occurrences, site, &children, &params);
            call.site = absent_site;
            run(call, &catalog, control)
        },
        false,
        false,
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
        let fragment = finish(builder, owner, &pools);
        let roots = uses(&fragment, &catalog);
        let source = window_source(&fragment, root).1;
        let req = request(source, &fragment, &pools, &Control::default()).unwrap();
        let children = pure_children(&fragment);

        prefixes(
            |control| {
                run(
                    input(source, &req, &roots, root, &children, &params),
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
    let initial = literal(&mut builder, leaf, source_type.clone(), checked::null());
    let initial_root = root(&mut builder, leaf, initial, RootScope::AllPorts);
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
        .add_values(
            leaf,
            Box::from([Box::from([initial_root])]),
            Box::from([value]),
        )
        .unwrap();
    let arg = builder
        .add_expression(owner, source_type.clone(), ExprKind::input(value))
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
    let pools = ConstantPools::empty();
    let fragment = finish(builder, owner, &pools);
    let roots = uses(&fragment, &catalog);
    let source = window_source(&fragment, root).1;
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
    let children = pure_children(&fragment);
    let params = SemanticParameters::try_new([]).unwrap();
    let trace = prefixes(
        |control| {
            run(
                input(source, &req, &roots, root, &children, &params),
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
    let query = &novarocks_parser::parser::parse_query(sql).unwrap();
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
        "SELECT map_agg(1, 'x') OVER ()",
        "SELECT min(7, 2) OVER ()",
        "SELECT max(7, 2) OVER ()",
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
        "SELECT min('x') OVER ()",
        "SELECT max('x') OVER ()",
        "SELECT lead(TRUE, 1, FALSE) OVER (ORDER BY 1)",
        "SELECT min(7) OVER ()",
        "SELECT max(7) OVER ()",
        "SELECT lead('x') OVER (ORDER BY 1)",
        "SELECT lag(7, 1, 0) OVER (ORDER BY 1)",
    ] {
        analyze_window_admission_sql(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
    let error = analyze_window_admission_sql("SELECT lead(missing, 1, 0) OVER (ORDER BY 1)")
        .expect_err("unknown input column");
    assert_eq!(
        error.kind(),
        crate::analyze_error::AnalyzeErrorKind::UnknownColumn
    );
}
