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
use arrow::datatypes::{DataType, Field};
use novarocks_functions::{
    EngineFunctionCatalog, FunctionArgument, FunctionBindingError, FunctionKind,
    FunctionResolutionError, FunctionResultType, FunctionValueType, ResolvedAggregateSignature,
    ResolvedFunctionBinding, builtin::catalogue::build_builtin_engine_function_catalog,
};
use novarocks_physical_plan::{
    AggregateBinding, AggregateCall, AggregateCallId, AggregateGrouping, AggregatePhase,
    BoundFunction, BoundTableFunction, Distribution, ExprKind, FragmentBuilder, FragmentId,
    FragmentSink, FrozenCallError, FrozenFragmentCalls, FrozenPhysicalCall, LiteralValue, NodeId,
    NodeKind, OutputPort, PhysicalNode, PhysicalProperties, PipelineDopDomain, PlanLimits,
    RowMultiplicity, TableFunctionOutput, ValueOrigin,
};
use novarocks_type_contract::{
    AggregateStateFormatId, CallEffects, CallProofScope, DecimalOverflowPolicy,
    FunctionFailureBehavior, FunctionInstanceState, FunctionIntrinsicRowError,
    FunctionNullBehavior, FunctionOverloadId, FunctionVolatility, ObservableEffects,
};
use std::sync::{Arc, Mutex};

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
    assert_eq!(run(fragment, catalog, &baseline).is_ok(), success);
    let trace = baseline.trace.lock().unwrap().clone();
    assert_eq!(trace.first(), Some(&(CompilePhase::Validate, 0)));
    assert!(
        trace.len() > 1,
        "ordinary exits must observe their actual tail"
    );
    if wide {
        assert!(trace.contains(&(CompilePhase::Validate, 256)));
    }
    let positions: Vec<_> = if wide {
        let quantum = trace
            .iter()
            .position(|item| *item == (CompilePhase::Validate, 256))
            .unwrap();
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
            Box::from([Box::<[ExprId]>::default()]),
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
        legacy_metadata: Some(novarocks_physical_plan::LegacyBindingMetadata {
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
    bindings: Vec<(AggregateBinding, Box<[ExprId]>)>,
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
fn assert_contexts(authored: &AuthoredPhysicalOccurrences) {
    let expression_count = authored.root_uses.flow().uses().len();
    for (ordinal, (_, context)) in authored.relational_contexts.iter().enumerate() {
        assert_eq!(context.use_id.get() as usize, expression_count + ordinal);
        assert!(
            !authored
                .root_uses
                .flow()
                .uses()
                .contains_key(&context.use_id)
        );
        assert_eq!(
            context.demand,
            novarocks_type_contract::EvaluationDemand::Value
        );
        let domain = authored.root_uses.flow().regions()[&context.region];
        assert!(domain.parent.is_none());
        assert!(domain.guard.is_none());
    }
    for pair in authored.relational_contexts.windows(2) {
        assert_ne!(pair[0].1.use_id, pair[1].1.use_id);
        assert_ne!(pair[0].1.region, pair[1].1.region);
    }
}
fn effects(control: ArgumentControl, domain: EvaluationRegionId) -> CallEffects {
    // Explicit public structural claims for the ID-collision test. These do
    // not prove installed preparation or authorize an execution lifecycle.
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
        proof_scope: CallProofScope::Region(domain),
    }
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
    assert!(fragment.expressions().is_empty());
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
    assert!(authored.root_uses.bindings().is_empty());
    assert!(authored.root_uses.flow().uses().is_empty());
    assert_eq!(authored.relational_contexts.len(), 1);
    assert_eq!(
        authored.relational_contexts[0].0,
        PhysicalCallSite::Aggregate {
            node: NodeId::new(901),
            call: 0
        }
    );
    assert_eq!(
        authored.relational_contexts[0].1.use_id,
        ExpressionUseId::new(0)
    );
    assert_eq!(authored.root_uses.flow().regions().len(), 1);
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
    let literal = builder
        .add_expression(
            owner,
            text.clone(),
            ExprKind::Literal(LiteralValue::Utf8("MiXeD".into())),
        )
        .unwrap();
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
    let binding = count(&catalog, &[lower.result_type]);
    aggregate(
        &mut builder,
        input,
        owner,
        vec![(binding, Box::from([lowered]))],
    );
    let fragment = finish(builder, owner);
    let authored = run(&fragment, &catalog, &Control::default()).unwrap();
    assert_eq!(authored.root_uses.bindings().len(), 1);
    assert_eq!(authored.root_uses.flow().uses().len(), 2);
    assert_eq!(authored.relational_contexts.len(), 1);
    assert_contexts(&authored);
    let scalar_use = *authored.root_uses.bindings().values().next().unwrap();
    let scalar_context = authored.root_uses.flow().uses()[&scalar_use].context;
    let (site, context) = authored.relational_contexts[0];
    assert_eq!(context.use_id, ExpressionUseId::new(2));
    assert_ne!(context.region, scalar_context.region);
    let scalar_call = FrozenPhysicalCall {
        materialized_writer_input: None,
        regexp_count_pattern_source: None,
        to_base64_byte_source: None,
        temporal_source: None,
        site: PhysicalCallSite::Expression(scalar_use),
        context: scalar_context,
        effects: effects(ArgumentControl::Eager, scalar_context.region),
        decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
    };
    let aggregate_call = FrozenPhysicalCall {
        materialized_writer_input: None,
        regexp_count_pattern_source: None,
        to_base64_byte_source: None,
        temporal_source: None,
        site,
        context,
        effects: effects(ArgumentControl::Aggregate, context.region),
        decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
    };
    FrozenFragmentCalls::try_new(
        &fragment,
        &authored.root_uses,
        vec![scalar_call.clone(), aggregate_call.clone()],
        &Control::default(),
    )
    .unwrap();
    let mut bad = aggregate_call;
    bad.context.use_id = scalar_use;
    assert!(matches!(
        FrozenFragmentCalls::try_new(
            &fragment,
            &authored.root_uses,
            vec![scalar_call, bad],
            &Control::default()
        ),
        Err(FrozenCallError::SharedUse)
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
    let literal = builder
        .add_expression(leaf, list.clone(), ExprKind::Literal(LiteralValue::Null))
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
        .add_values(leaf, Box::from([Box::from([literal])]), Box::from([value]))
        .unwrap();
    let argument = builder
        .add_expression(owner, list.clone(), ExprKind::Value(value))
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
        legacy_metadata: Some(novarocks_physical_plan::LegacyBindingMetadata {
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
                arguments: Box::from([argument]),
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
    assert_eq!(authored.root_uses.bindings().len(), 2);
    assert_eq!(authored.root_uses.flow().uses().len(), 2);
    assert_eq!(authored.relational_contexts.len(), 1);
    assert_contexts(&authored);
    assert_eq!(
        authored.relational_contexts[0].0,
        PhysicalCallSite::Table { node: owner }
    );
    let DataType::List(original) = &fragment.expressions().get(argument).unwrap().ty.data_type()
    else {
        unreachable!()
    };
    assert!(Arc::ptr_eq(original, &original_layout));
    assert_eq!(original.name(), "item");
    assert!(original.metadata().is_empty());
    assert_eq!(field.metadata()["unknown-source"], "preserved");
    let source = authored
        .root_uses
        .roots()
        .sites()
        .values()
        .find(|root| root.expr == argument)
        .unwrap();
    assert_eq!(
        source.demand,
        novarocks_type_contract::EvaluationDemand::Value
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
    assert!(authored.root_uses.flow().uses().is_empty());
    assert!(authored.root_uses.bindings().is_empty());
    assert_eq!(authored.relational_contexts.len(), 320);
    assert_eq!(authored.root_uses.flow().regions().len(), 320);
    assert_contexts(&authored);
    for (ordinal, (site, context)) in authored.relational_contexts.iter().enumerate() {
        assert_eq!(
            *site,
            PhysicalCallSite::Aggregate {
                node: NodeId::new(901),
                call: ordinal as u32
            }
        );
        assert_eq!(context.use_id.get() as usize, ordinal);
    }
    // Wide source sampling covers actual entry, collector quantum and final
    // tail; the small cases above test every actual callback for all causes.
    prefixes(&fragment, &catalog, true, true);
}
