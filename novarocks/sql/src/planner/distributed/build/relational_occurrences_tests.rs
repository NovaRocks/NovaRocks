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

use super::*;
use crate::test_constants::{self as checked, Fragment, FragmentBuilder};
use arrow::datatypes::{DataType, Field};
use novarocks_functions::{
    EngineFunctionCatalog, FunctionArgument, FunctionBindingError, FunctionKind,
    FunctionResolutionError, FunctionResultType, FunctionValueType, ResolvedAggregateSignature,
    ResolvedFunctionBinding, builtin::catalogue::build_builtin_engine_function_catalog,
};
use novarocks_type_contract::{
    AggregateStateFormatId, CallEffects, CallProofScope, DecimalOverflowPolicy, EvaluationDemand,
    FunctionFailureBehavior, FunctionInstanceState, FunctionIntrinsicRowError,
    FunctionNullBehavior, FunctionOverloadId, FunctionVolatility, ObservableEffects,
    RelationEffectContext, RelationEffectRole,
};
use std::sync::{Arc, Mutex};
use {
    novarocks_expression_contract::{BoundFunction, ExprKind, RootOrdinal, RootScope},
    novarocks_physical_plan::{
        AggregateBinding, AggregateCall, AggregateCallId, AggregateGrouping, AggregatePhase,
        BoundTableFunction, Distribution, FragmentId, FragmentSink, FrozenCallError,
        FrozenFragmentCalls, FrozenPhysicalCall, NodeId, NodeKind, OutputPort, PhysicalNode,
        PhysicalProperties, PipelineDopDomain, PlanLimits, RowMultiplicity, TableFunctionOutput,
        ValueOrigin,
    },
};

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
        assert!(matches!(
            phase,
            CompilePhase::Validate | CompilePhase::FunctionSpecialization
        ));
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop, "callback after first refusal");
        }
        trace.push((phase, units));
        match self.refusal.clone() {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}
fn run<'a>(
    fragment: &'a Fragment,
    catalog: &dyn SqlFunctionCatalog,
    control: &Control,
) -> Result<AuthoredPhysicalOccurrences<'a>, ExpressionOccurrenceError> {
    author_physical_occurrences_observed(fragment, catalog, control)
}
fn prefixes(
    fragment: &Fragment,
    catalog: &dyn SqlFunctionCatalog,
    success: bool,
    wide: bool,
) -> Vec<(CompilePhase, u32)> {
    let baseline = Control::default();
    let result = run(fragment, catalog, &baseline);
    let source_count = result
        .as_ref()
        .map_or(0, |authored| authored.relational_contexts.len());
    assert_eq!(result.is_ok(), success);
    let trace = baseline.trace.lock().unwrap().clone();
    assert_eq!(trace.first(), Some(&(CompilePhase::Validate, 0)));
    assert!(
        trace.len() > 1,
        "ordinary exits must observe their actual tail"
    );
    if wide {
        let observed: u64 = trace
            .iter()
            .filter(|(phase, _)| *phase == CompilePhase::Validate)
            .map(|(_, units)| u64::from(*units))
            .sum();
        assert!(source_count > novarocks_type_contract::MAX_UNOBSERVED_COMPILE_WORK as usize);
        assert!(
            observed >= u64::try_from(source_count).unwrap(),
            "every actual relation source contributes observed work"
        );
    }
    let positions: Vec<_> = if wide {
        // The enclosing author flushes before each real function owner and
        // allocation. Sample the actual callback crossing one cumulative work
        // quantum, rather than inventing a 256-unit callback at that boundary.
        let mut observed = 0u64;
        let quantum = trace
            .iter()
            .position(|(phase, units)| {
                if *phase == CompilePhase::Validate {
                    observed += u64::from(*units);
                }
                observed >= u64::from(novarocks_type_contract::MAX_UNOBSERVED_COMPILE_WORK)
            })
            .expect("actual source work crosses one quantum");
        vec![0, quantum, trace.len() - 1]
    } else {
        (0..trace.len()).collect()
    };
    for at in positions {
        for cause in CAUSES {
            let control = Control {
                trace: Mutex::default(),
                refusal: Some((at, cause.clone())),
            };
            assert!(
                matches!(run(fragment,catalog,&control),Err(ExpressionOccurrenceError::Control(actual)) if actual==cause),
                "at {at}: {cause:?}"
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
    trace
}
fn ty(dt: DataType, nullable: bool) -> FunctionValueType {
    FunctionValueType::new(dt, nullable)
}
fn properties() -> PhysicalProperties {
    PhysicalProperties {
        distribution: Distribution::Singleton,
        row_multiplicity: RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    }
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
fn empty() -> (FragmentBuilder, NodeId) {
    let mut builder = FragmentBuilder::new(FragmentId::new(71));
    let leaf = NodeId::new(7);
    builder
        .add_values(
            leaf,
            Box::from([Box::<[RootOrdinal]>::default()]),
            Box::default(),
        )
        .unwrap();
    (builder, leaf)
}
fn scalar(resolved: ResolvedFunctionBinding) -> BoundFunction {
    let FunctionResultType::Scalar(result_type) = resolved.selected.result_type else {
        unreachable!()
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
fn count(catalog: &EngineFunctionCatalog, types: &[FunctionValueType]) -> AggregateBinding {
    let args = types
        .iter()
        .cloned()
        .map(|value_type| FunctionArgument::Value {
            value_type,
            constant: None,
        })
        .collect::<Vec<_>>();
    let resolved = catalog
        .resolve_aggregate_binding("count", args.len(), &args, &Control::default())
        .unwrap();
    let selected = resolved.selected.aggregate.as_ref().unwrap();
    let state_argument_contract = selected.state_argument_contract;
    let intermediate_type = selected.intermediate_type.clone();
    let state_format = AggregateStateFormatId::try_new(selected.state_format.as_str()).unwrap();
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
fn aggregate(
    builder: &mut FragmentBuilder,
    input: NodeId,
    owner: NodeId,
    bindings: Vec<(AggregateBinding, Box<[RootOrdinal]>)>,
) {
    let mut calls = Vec::new();
    let mut output = Vec::new();
    for (ordinal, (binding, arguments)) in bindings.into_iter().enumerate() {
        let id = AggregateCallId::new(ordinal as u32);
        let value = builder
            .add_value(
                binding.function.result_type.clone(),
                ValueOrigin::AggregateResult { call: id },
            )
            .unwrap();
        output.push(value);
        calls.push(AggregateCall {
            filter_root: None,
            id,
            binding,
            arguments,
            distinct: false,
            order_by: Box::default(),
            output: value,
        });
    }
    builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: owner,
            inputs: Box::from([input]),
            required_inputs: Box::from([properties()]),
            output_properties: properties(),
            output: OutputPort {
                node: owner,
                columns: output.into_boxed_slice(),
            },
            kind: NodeKind::Aggregate {
                group_by: Box::default(),
                calls: calls.into_boxed_slice(),
                grouping: AggregateGrouping::Complete,
            },
        })
        .unwrap();
}
fn count_fragment(
    catalog: &EngineFunctionCatalog,
    n: usize,
    mut alter: impl FnMut(&mut AggregateBinding),
) -> Fragment {
    let (mut builder, input) = empty();
    let owner = NodeId::new(901);
    let mut binding = count(catalog, &[]);
    alter(&mut binding);
    aggregate(
        &mut builder,
        input,
        owner,
        (0..n).map(|_| (binding.clone(), Box::default())).collect(),
    );
    finish(builder, owner)
}
fn assert_contexts(authored: &AuthoredPhysicalOccurrences<'_>) {
    for (ordinal, (_, context)) in authored.relational_contexts.iter().enumerate() {
        assert_eq!(context.identity.call.get() as usize, ordinal);
        assert_eq!(context.identity.role, RelationEffectRole::Call);
        assert!(context.is_call());
        assert_eq!(context.demand, EvaluationDemand::Value);
    }
    for pair in authored.relational_contexts.windows(2) {
        assert_ne!(pair[0].1.identity.call, pair[1].1.identity.call);
    }
    assert_eq!(authored.writer_value_uses().count(), 0);
}
fn effects(control: ArgumentControl, context: RelationEffectContext) -> CallEffects {
    // Explicit public structural claims test FrozenFragmentCalls identities.
    // These are not installed preparation receipts or a SQL source journal.
    CallEffects {
        value_stability: FunctionVolatility::Immutable,
        own_row_error: if control == ArgumentControl::Aggregate {
            FunctionIntrinsicRowError::NotRowEvaluated
        } else {
            FunctionIntrinsicRowError::NoRowError
        },
        failure_behavior: FunctionFailureBehavior::Propagate,
        null_behavior: if control == ArgumentControl::Aggregate {
            FunctionNullBehavior::CalledOnNull
        } else {
            FunctionNullBehavior::Strict
        },
        argument_control: control,
        instance_state: if control == ArgumentControl::Aggregate {
            FunctionInstanceState::AggregateInstance
        } else {
            FunctionInstanceState::None
        },
        observable_effects: ObservableEffects::NONE,
        environment: Box::default(),
        proof_scope: CallProofScope::Relation(context.identity),
    }
}
fn seal_function_node(
    builder: &mut FragmentBuilder,
    node: NodeId,
    pools: &novarocks_physical_plan::ConstantPools,
    _catalog: &EngineFunctionCatalog,
) {
    let control = Control::default();
    let mut work =
        CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization).unwrap();
    let sealed = super::super::expression_component_test_fixture::seal_node_observed(
        builder,
        node,
        pools,
        &novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
        checked::policy(),
        &mut work,
    )
    .unwrap();
    let parameters = novarocks_type_contract::SemanticParameters::try_new([]).unwrap();
    work.flush().unwrap();
    let authored = super::super::expression_component_test_fixture::author_component_calls(
        &sealed,
        pools,
        &parameters,
        DecimalOverflowPolicy::ReportError,
        &mut work,
    )
    .unwrap();
    let unit = sealed.finish(authored.claims, &mut work).unwrap();
    work.finish().unwrap();
    builder.install_unit(node, Arc::new(unit)).unwrap();
}
#[derive(Clone, Debug)]
struct MetadataOnly;
impl SqlFunctionCatalog for MetadataOnly {
    fn snapshot(&self) -> Arc<dyn SqlFunctionCatalog> {
        Arc::new(self.clone())
    }
    fn resolve_scalar_signature(
        &self,
        _: &str,
        _: &[DataType],
        _: &dyn PureCompileControl,
    ) -> Result<crate::functions::ResolvedScalarFunction, crate::functions::ResolveError> {
        panic!("no name re-resolution")
    }
    fn resolve_aggregate_signature(
        &self,
        _: &str,
        _: &[DataType],
        _: &dyn PureCompileControl,
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        panic!("no aggregate re-resolution")
    }
    fn resolve_aggregate_trusted(
        &self,
        _: &str,
        _: &[DataType],
        _: &dyn PureCompileControl,
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        panic!("no trusted re-resolution")
    }
    fn contains_aggregate(&self, _: &str) -> bool {
        panic!("no name discovery")
    }
    fn volatility(&self, _: &str) -> novarocks_functions::FunctionVolatility {
        panic!("no legacy effect inference")
    }
}

#[test]
fn relational_rootless_count_star_has_its_own_value_use_and_unguarded_domain() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let fragment = count_fragment(&catalog, 1, |_| {});
    assert!(
        fragment
            .nodes()
            .values()
            .all(|node| node.expressions.is_none())
    );
    let NodeKind::Aggregate { calls, .. } = &fragment.nodes()[&NodeId::new(901)].kind else {
        unreachable!()
    };
    assert_eq!(
        calls[0].binding.function.result_type,
        ty(DataType::Int64, false)
    );
    assert_eq!(
        calls[0].binding.intermediate_type,
        ty(DataType::Int64, false)
    );
    assert_eq!(
        calls[0].binding.state_format.as_str(),
        "novarocks/count/state-v1"
    );
    assert_eq!(calls[0].binding.logical_argument_count, 0);
    assert_eq!(calls[0].binding.phase, AggregatePhase::Single);
    let authored = run(&fragment, &catalog, &Control::default()).unwrap();
    assert_eq!(authored.relational_contexts.len(), 1);
    assert_eq!(
        authored.relational_contexts[0].0,
        PhysicalCallSite::Aggregate {
            node: NodeId::new(901),
            call: 0
        }
    );
    assert_eq!(
        authored.relational_contexts[0].1.identity.call,
        novarocks_type_contract::RelationCallId::new(0)
    );
    assert_contexts(&authored);
    prefixes(&fragment, &catalog, true, false);
}
#[test]
fn relational_count_value_and_scalar_argument_uses_stay_disjoint_and_frozen_collision_is_rejected()
{
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let (mut builder, input) = empty();
    let owner = NodeId::new(901);
    let text = ty(DataType::Utf8, false);
    let mut pools = novarocks_physical_plan::ConstantPools::empty();
    let kind = checked::insert(&mut pools, checked::utf8("MiXeD".into())(&text));
    let literal = builder.add_expression(owner, text.clone(), kind).unwrap();
    let args = [FunctionArgument::Value {
        value_type: text,
        constant: None,
    }];
    let lower = scalar(
        catalog
            .resolve_scalar_binding("lower", &args, &Control::default())
            .unwrap(),
    );
    let lowered = builder
        .add_expression(
            owner,
            lower.result_type.clone(),
            ExprKind::FunctionCall {
                function: lower.clone(),
                args: Box::from([literal]),
            },
        )
        .unwrap();
    let argument = builder
        .unit(owner)
        .unwrap()
        .root(lowered, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let binding = count(&catalog, &[lower.result_type]);
    aggregate(
        &mut builder,
        input,
        owner,
        vec![(binding, Box::from([argument]))],
    );
    seal_function_node(&mut builder, owner, &pools, &catalog);
    let fragment = finish(builder, owner);
    let unit = fragment.nodes()[&owner].expressions.as_deref().unwrap();
    assert_eq!(unit.roots().len(), 1);
    assert_eq!(unit.control_flow().uses().len(), 2);
    assert_eq!(unit.claims().len(), 1);
    assert_eq!(unit.ports().len(), 1);
    assert!(unit.ports()[0].values.is_empty());
    assert_eq!(unit.root(argument).unwrap().scope, RootScope::Port(0));
    assert_eq!(unit.root(argument).unwrap().demand, EvaluationDemand::Value);
    let scalar_use = unit.root_use_ids()[argument.get() as usize];
    let scalar_context = unit.control_flow().uses()[&scalar_use].context;
    let authored = run(&fragment, &catalog, &Control::default()).unwrap();
    assert_eq!(authored.relational_contexts.len(), 1);
    assert_contexts(&authored);
    let (site, context) = authored.relational_contexts[0];
    // Equal underlying numbers are legal across independent typed spaces.
    // A scalar context cannot be assigned to FrozenPhysicalCall.context.
    assert_eq!(context.identity.call.get(), scalar_use.get());
    assert_eq!(scalar_context.use_id, scalar_use);
    assert_eq!(scalar_context.demand, EvaluationDemand::Value);
    let aggregate_call = FrozenPhysicalCall {
        site,
        context,
        materialized_writer_input: None,
        effects: effects(ArgumentControl::Aggregate, context),
        decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
    };
    FrozenFragmentCalls::try_new(&fragment, vec![aggregate_call.clone()], &Control::default())
        .unwrap();
    let mut wrong_role = aggregate_call;
    wrong_role.context = RelationEffectContext::materialized_input(context.identity.call);
    assert!(matches!(
        FrozenFragmentCalls::try_new(&fragment, vec![wrong_role], &Control::default()),
        Err(FrozenCallError::WrongContext)
    ));
    // A collision remains rejectable in the actual relation namespace.
    let pair = count_fragment(&catalog, 2, |_| {});
    let pair_uses = run(&pair, &catalog, &Control::default()).unwrap();
    let calls: Vec<_> = pair_uses
        .relational_contexts
        .iter()
        .map(|&(site, context)| FrozenPhysicalCall {
            site,
            context,
            materialized_writer_input: None,
            effects: effects(ArgumentControl::Aggregate, context),
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
        })
        .collect();
    FrozenFragmentCalls::try_new(&pair, calls.clone(), &Control::default()).unwrap();
    let mut bad = calls;
    bad[1].context.identity.call = bad[0].context.identity.call;
    assert!(matches!(
        FrozenFragmentCalls::try_new(&pair, bad, &Control::default()),
        Err(FrozenCallError::DuplicateIdentity)
    ));
    prefixes(&fragment, &catalog, true, false);
}
#[test]
fn relational_unnest_keeps_real_argument_root_separate_from_table_context_and_original_source() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let mut builder = FragmentBuilder::new(FragmentId::new(71));
    let leaf = NodeId::new(7);
    let owner = NodeId::new(901);
    let field = Arc::new(
        Field::new("original-item", DataType::Int64, true).with_metadata(
            std::collections::HashMap::from([("unknown-source".into(), "preserved".into())]),
        ),
    );
    let list = ty(DataType::List(field.clone()), true);
    let DataType::List(original_layout) = list.data_type() else {
        unreachable!()
    };
    let original_layout = original_layout.clone();
    let literal = builder.constant(leaf, list.clone(), checked::null());
    let cell = builder
        .unit(leaf)
        .unwrap()
        .root(literal, EvaluationDemand::Value, RootScope::AllPorts)
        .unwrap();
    let value = builder
        .add_value(
            list.clone(),
            ValueOrigin::NodeOutput {
                node: leaf,
                output_ordinal: 0,
            },
        )
        .unwrap();
    builder
        .add_values(leaf, Box::from([Box::from([cell])]), Box::from([value]))
        .unwrap();
    let argument = builder
        .add_expression(owner, list.clone(), ExprKind::input(value))
        .unwrap();
    let parameter = builder
        .unit(owner)
        .unwrap()
        .root(argument, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let request = [FunctionArgument::Value {
        value_type: list,
        constant: None,
    }];
    let resolved = catalog
        .resolve_table_binding("unnest", &request, &Control::default())
        .unwrap();
    let FunctionResultType::Relation(result_types) = resolved.selected.result_type else {
        unreachable!()
    };
    let function = BoundTableFunction {
        function_id: resolved.function_id,
        overload: resolved.selected.overload,
        argument_types: resolved.selected.argument_types,
        result_types: result_types.clone(),
        legacy_metadata: Some(novarocks_expression_contract::LegacyBindingMetadata {
            volatility: resolved.semantics.volatility,
            argument_evaluation: resolved.semantics.argument_evaluation,
            failure_behavior: resolved.semantics.failure_behavior,
            intrinsic_row_error: resolved.semantics.intrinsic_row_error,
            semantic_parameters: Box::default(),
        }),
    };
    assert_eq!(result_types.len(), 1);
    let output = builder
        .add_value(
            result_types[0].clone(),
            ValueOrigin::NodeOutput {
                node: owner,
                output_ordinal: 0,
            },
        )
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: owner,
            inputs: Box::from([leaf]),
            required_inputs: Box::from([properties()]),
            output_properties: properties(),
            output: OutputPort {
                node: owner,
                columns: Box::from([output]),
            },
            kind: NodeKind::TableFunction {
                function,
                arguments: Box::from([parameter]),
                outputs: Box::from([TableFunctionOutput::FunctionResult {
                    result_ordinal: 0,
                    value: output,
                }]),
                left_outer: false,
            },
        })
        .unwrap();
    let fragment = finish(builder, owner);
    let authored = run(&fragment, &catalog, &Control::default()).unwrap();
    let units: Vec<_> = fragment
        .nodes()
        .values()
        .filter_map(|node| node.expressions.as_deref())
        .collect();
    assert_eq!(
        units.iter().map(|unit| unit.roots().len()).sum::<usize>(),
        2
    );
    assert_eq!(
        units
            .iter()
            .map(|unit| unit.control_flow().uses().len())
            .sum::<usize>(),
        2
    );
    assert_eq!(authored.relational_contexts.len(), 1);
    assert_contexts(&authored);
    assert_eq!(
        authored.relational_contexts[0].0,
        PhysicalCallSite::Table { node: owner }
    );
    let unit = fragment.nodes()[&owner].expressions.as_deref().unwrap();
    let DataType::List(original) = unit.definition(argument).unwrap().ty.data_type() else {
        unreachable!()
    };
    assert!(Arc::ptr_eq(original, &original_layout));
    assert_eq!(original.name(), "item");
    assert!(original.metadata().is_empty());
    assert_eq!(field.metadata()["unknown-source"], "preserved");
    let source = unit.root(parameter).unwrap();
    assert_eq!(source.definition, argument);
    assert_eq!(source.demand, EvaluationDemand::Value);
    assert_eq!(source.scope, RootScope::Port(0));
    let scalar = unit.control_flow().uses()[&unit.root_use_ids()[parameter.get() as usize]].context;
    assert_eq!(scalar.demand, EvaluationDemand::Value);
    assert_eq!(
        authored.relational_contexts[0].1.identity.role,
        RelationEffectRole::Call
    );
    prefixes(&fragment, &catalog, true, false);
}
#[test]
fn relational_metadata_only_missing_owner_and_unknown_overload_preserve_ordinary_tails_and_controls()
 {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let fragment = count_fragment(&catalog, 1, |_| {});
    assert!(matches!(
        run(&fragment, &MetadataOnly, &Control::default()),
        Err(ExpressionOccurrenceError::Function(
            FunctionSpecializationFailure::InvalidInput(
                "SQL function snapshot has no installed pure overload declaration owner"
            )
        ))
    ));
    prefixes(&fragment, &MetadataOnly, false, false);
    let missing = FunctionOverloadId::try_new("fixture/missing-overload").unwrap();
    let fragment = count_fragment(&catalog, 1, |binding| {
        binding.function.overload = missing.clone()
    });
    assert!(
        matches!(run(&fragment,&catalog,&Control::default()),Err(ExpressionOccurrenceError::Function(FunctionSpecializationFailure::Binding(FunctionBindingError::UnknownOverload(actual)))) if actual==missing)
    );
    prefixes(&fragment, &catalog, false, false);
}
#[test]
fn relational_exact_owner_kind_mismatch_refuses_without_an_eager_fallback() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let abs = catalog
        .definition("abs", FunctionKind::Scalar)
        .unwrap()
        .binding_declaration()
        .unwrap()
        .function_id()
        .clone();
    // The structural signature remains a genuine COUNT shape. Identity/kind
    // authentication belongs to the installed catalogue, not legacy fields.
    let fragment = count_fragment(&catalog, 1, |binding| {
        binding.function.function_id = abs.clone()
    });
    assert!(matches!(
        run(&fragment, &catalog, &Control::default()),
        Err(ExpressionOccurrenceError::Function(
            FunctionSpecializationFailure::InvalidInput(
                "pure preparation has a different exact kind or selected owner"
            )
        ))
    ));
    prefixes(&fragment, &catalog, false, false);
}
#[test]
fn relational_320_real_count_calls_observe_source_quantum_and_distinct_actual_contexts() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let fragment = count_fragment(&catalog, 320, |_| {});
    let authored = run(&fragment, &catalog, &Control::default()).unwrap();
    assert!(
        fragment
            .nodes()
            .values()
            .all(|node| node.expressions.is_none())
    );
    assert_eq!(authored.relational_contexts.len(), 320);
    assert_eq!(authored.writer_value_uses().count(), 0);
    assert_contexts(&authored);
    for (ordinal, (site, context)) in authored.relational_contexts.iter().enumerate() {
        assert_eq!(
            *site,
            PhysicalCallSite::Aggregate {
                node: NodeId::new(901),
                call: ordinal as u32
            }
        );
        assert_eq!(context.identity.call.get() as usize, ordinal);
    }
    // The sole physical relational walker owns the uninterrupted source
    // traversal. Its real 320-call loop still reaches an exact 256 callback;
    // the installed-owner composer intentionally flushes more often.
    let invoke = |control: &Control| -> Result<Vec<PhysicalCallSite>, FrozenCallError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
        let mut sites = Vec::new();
        let result = novarocks_physical_plan::visit_relational_calls_observed(
            &fragment,
            &mut work,
            |site, _, _| {
                sites.push(site);
                Ok::<_, FrozenCallError>(())
            },
        )
        .map(|()| sites);
        work.finish_result(result, FrozenCallError::Control)
    };
    let baseline = Control::default();
    let sites = invoke(&baseline).unwrap();
    assert_eq!(
        sites,
        authored
            .relational_contexts
            .iter()
            .map(|(site, _)| *site)
            .collect::<Vec<_>>()
    );
    let trace = baseline.trace.lock().unwrap().clone();
    assert_eq!(trace.first(), Some(&(CompilePhase::Validate, 0)));
    assert!(
        trace
            .iter()
            .all(|(phase, units)| *phase == CompilePhase::Validate && *units <= 256)
    );
    assert!(trace.contains(&(CompilePhase::Validate, 256)));
    for at in 0..trace.len() {
        for cause in CAUSES {
            let control = Control {
                trace: Mutex::default(),
                refusal: Some((at, cause.clone())),
            };
            assert!(
                matches!(invoke(&control), Err(FrozenCallError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
    // The actual installed-owner composition retains its own entry, original
    // cumulative work boundary and final-tail sampling for all three causes.
    prefixes(&fragment, &catalog, true, true);
}
