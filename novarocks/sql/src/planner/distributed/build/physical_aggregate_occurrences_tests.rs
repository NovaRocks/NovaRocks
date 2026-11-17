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
    expression_occurrences::{AuthoredPhysicalOccurrences, author_physical_occurrences_observed},
    physical_aggregate_requests::{
        AuthoredPhysicalAggregateUpdateRequest, PhysicalAggregateRequestError,
        author_physical_aggregate_update_request_observed,
    },
    physical_expression_effects::{
        PhysicalExpressionEffectsInput, author_physical_expression_effects_observed,
    },
    physical_relational_effects::PhysicalRelationalEffectsError,
};
use super::*;
use crate::functions::build_builtin_engine_function_catalog;
use crate::test_constants::{self as checked, Fragment, FragmentBuilder};
use arrow::{
    array::{Array, Int64Array},
    datatypes::{DataType, Field},
};
use novarocks_functions::{
    AggregateKernelPhase, ConstantPolicy, ConstantPool, EngineFunctionCatalog, FunctionArgument,
    FunctionResultType, PreparedPureKernel, PureCallPreparation,
};
use novarocks_physical_plan::{
    AggregateBinding, AggregateCallId, AggregateGrouping, AggregatePhase, AggregateSequenceId,
    BinaryOperator, BoundFunction, ConstantPoolId, ConstantPools, ConstantReference, Distribution,
    ExprId, ExprKind, FragmentId, FragmentSink, NodeId, NodeKind, NullOrdering,
    OrderedComparisonAlgorithm, OutputPort, PhysicalProperties, PipelineDopDomain, PlanLimits,
    RowMultiplicity, SortDirection, SortExpr, TopNPhase, TopNReduction, TopNSequenceId, ValueId,
    ValueOrigin,
};
use novarocks_type_contract::{
    AggregateStateFormatId, CompilePhase, ExpressionEffectContext, FunctionInstanceState,
    FunctionIntrinsicRowError, FunctionNullBehavior, FunctionValueType, PureCompileControl,
    SemanticParameterId, SemanticParameterKey, SemanticParameterValue,
};
use std::sync::{Arc, Mutex};

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
            assert!(at <= stop, "callback after first refusal");
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
fn ty(dt: DataType, nullable: bool) -> FunctionValueType {
    FunctionValueType::new(dt, nullable)
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
fn props() -> PhysicalProperties {
    PhysicalProperties {
        distribution: Distribution::Singleton,
        row_multiplicity: RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    }
}
fn finish(b: FragmentBuilder, root: NodeId) -> Fragment {
    b.finish_structure(
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
fn binding(
    catalog: &EngineFunctionCatalog,
    name: &str,
    t: Option<&FunctionValueType>,
    order: usize,
    phase: AggregatePhase,
) -> AggregateBinding {
    let types: Vec<_> = t
        .into_iter()
        .chain((0..order).map(|_| t.unwrap()))
        .cloned()
        .map(|value_type| FunctionArgument::Value {
            value_type,
            constant: None,
        })
        .collect();
    let r = catalog
        .resolve_aggregate_binding(name, usize::from(t.is_some()), &types, &Control::default())
        .unwrap();
    let aggregate = r.selected.aggregate.as_ref().unwrap();
    let state_argument_contract = aggregate.state_argument_contract;
    let intermediate_type = aggregate.intermediate_type.clone();
    let state_format = AggregateStateFormatId::try_new(aggregate.state_format.as_str()).unwrap();
    let FunctionResultType::Scalar(result_type) = r.selected.result_type else {
        panic!("scalar aggregate result")
    };
    AggregateBinding {
        state_interpretation: None,
        state_argument_contract,
        function: BoundFunction {
            function_id: r.function_id,
            overload: r.selected.overload,
            kind: r.kind,
            argument_types: r.selected.argument_types,
            result_type,
            legacy_metadata: Some(novarocks_physical_plan::LegacyBindingMetadata {
                volatility: r.semantics.volatility,
                argument_evaluation: r.semantics.argument_evaluation,
                failure_behavior: r.semantics.failure_behavior,
                intrinsic_row_error: r.semantics.intrinsic_row_error,
                semantic_parameters: Box::default(),
            }),
        },
        phase,
        logical_argument_count: u32::from(t.is_some()),
        intermediate_type,
        state_format,
    }
}
#[derive(Clone, Copy)]
enum Source {
    Star,
    Value,
    Literal,
    Pool,
    ChildError,
}
struct Fixture {
    fragment: Fragment,
    pools: ConstantPools,
    catalog: EngineFunctionCatalog,
    owner: NodeId,
}
impl Fixture {
    fn new(
        name: &str,
        t: FunctionValueType,
        source: Source,
        phase: AggregatePhase,
        n: usize,
        order: usize,
        alter: impl FnOnce(&mut AggregateBinding),
    ) -> Self {
        let catalog = build_builtin_engine_function_catalog().unwrap();
        let mut pools = ConstantPools::empty();
        if matches!(source, Source::Pool) {
            let a = Int64Array::from(vec![Some(99), Some(7), None]);
            let p = ConstantPool::try_new(
                Arc::new(Field::new("whole-source", DataType::Int64, true)),
                t.clone(),
                a.to_data(),
                policy(),
                CompilePhase::Validate,
                &Control::default(),
            )
            .unwrap();
            pools.insert(ConstantPoolId::new(u32::MAX), p).unwrap();
        }
        let mut b = FragmentBuilder::new(FragmentId::new(71));
        let leaf = NodeId::new(7);
        let owner = NodeId::new(901);
        let v = if matches!(source, Source::Star) {
            b.add_values(
                leaf,
                Box::from([Box::<[ExprId]>::default()]),
                Box::default(),
            )
            .unwrap();
            None
        } else {
            let seed = b.constant(
                leaf,
                t.clone(),
                if t.nullable() {
                    checked::null()
                } else {
                    checked::i64(11)
                },
            );
            let v = b
                .add_value(
                    t.clone(),
                    ValueOrigin::NodeOutput {
                        node: leaf,
                        output_ordinal: 0,
                    },
                )
                .unwrap();
            b.add_values(leaf, Box::from([Box::from([seed])]), Box::from([v]))
                .unwrap();
            Some(v)
        };
        let kind = match source {
            Source::Star => None,
            Source::Value => Some(ExprKind::Value(v.unwrap())),
            Source::Literal => Some(if *t.data_type() == DataType::Utf8 {
                b.constant_kind(&t, checked::utf8("中国".into()))
            } else {
                b.constant_kind(&t, checked::i64(7))
            }),
            Source::Pool => Some(ExprKind::Constant(ConstantReference {
                pool: ConstantPoolId::new(u32::MAX),
                ordinal: 1,
            })),
            Source::ChildError => {
                let a = b.constant(owner, ty(DataType::Int64, false), checked::i64(i64::MAX));
                let c = b.constant(owner, ty(DataType::Int64, false), checked::i64(1));
                let add = b
                    .add_expression(
                        owner,
                        ty(DataType::Int64, true),
                        ExprKind::Binary {
                            op: BinaryOperator::Add,
                            left: a,
                            right: c,
                            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                            allow_throw_exception: Some(allow_ref()),
                        },
                    )
                    .unwrap();
                let test = b
                    .add_expression(
                        owner,
                        ty(DataType::Boolean, false),
                        ExprKind::IsNull {
                            expr: add,
                            negated: false,
                        },
                    )
                    .unwrap();
                let yes = b.constant(owner, t.clone(), checked::i64(1));
                let no = b.constant(owner, t.clone(), checked::i64(2));
                Some(ExprKind::Case {
                    operand: None,
                    when_then: Box::from([(test, yes)]),
                    else_expr: Some(no),
                })
            }
        };
        let arg = kind.map(|kind| b.add_expression(owner, t.clone(), kind).unwrap());
        let mut binding = binding(
            &catalog,
            name,
            (!matches!(source, Source::Star)).then_some(&t),
            0,
            phase,
        );
        // MIN/MAX do not install ORDER BY updates. This explicit construction
        // input carries the declared channels through the original structural
        // gate; its actual installed resolver must refuse this stale signature.
        if order != 0 {
            let mut channels = binding.function.argument_types.to_vec();
            let ordered_type = channels[0].clone();
            channels.extend((0..order).map(|_| ordered_type.clone()));
            binding.function.argument_types =
                novarocks_type_contract::TypedFactSlice::from_pure_boxed(
                    channels.into_boxed_slice(),
                );
        }
        alter(&mut binding);
        let arguments: Box<[ExprId]> = if matches!(source, Source::Star) {
            Box::default()
        } else {
            Box::from([arg.unwrap()])
        };
        let keys: Box<[SortExpr]> = (0..order)
            .map(|i| SortExpr {
                expr: arg.unwrap(),
                direction: if i % 2 == 0 {
                    SortDirection::Descending
                } else {
                    SortDirection::Ascending
                },
                null_ordering: if i % 2 == 0 {
                    NullOrdering::First
                } else {
                    NullOrdering::Last
                },
            })
            .collect();
        let mut calls = Vec::new();
        let mut output = Vec::new();
        for at in 0..n {
            let id = AggregateCallId::new(if n == 1 {
                u32::MAX
            } else {
                u32::try_from(at).unwrap()
            });
            let (outtype, origin) = if phase.produces_final_result() {
                (
                    binding.function.result_type.clone(),
                    ValueOrigin::AggregateResult { call: id },
                )
            } else {
                (
                    binding.intermediate_type.clone(),
                    ValueOrigin::AggregateState { call: id, phase },
                )
            };
            let out = b.add_value(outtype, origin).unwrap();
            output.push(out);
            calls.push(AggregateCall {
                id,
                binding: binding.clone(),
                arguments: arguments.clone(),
                distinct: false,
                order_by: keys.clone(),
                output: out,
            });
        }
        b.insert_node_unchecked(PhysicalNode {
            id: owner,
            inputs: Box::from([leaf]),
            required_inputs: Box::from([props()]),
            output_properties: props(),
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
        let fragment = finish(b, owner);
        checked::merge(&mut pools, fragment.constants());
        Self {
            fragment,
            pools,
            catalog,
            owner,
        }
    }
    fn node(&self) -> &PhysicalNode {
        &self.fragment.nodes()[&self.owner]
    }
    fn call(&self, ordinal: usize) -> &AggregateCall {
        let NodeKind::Aggregate { calls, .. } = &self.node().kind else {
            panic!("aggregate")
        };
        &calls[ordinal]
    }
    fn site(&self, ordinal: u32) -> PhysicalCallSite {
        PhysicalCallSite::Aggregate {
            node: self.owner,
            call: ordinal,
        }
    }
    fn occurrences(&self) -> AuthoredPhysicalOccurrences<'_> {
        author_physical_occurrences_observed(&self.fragment, &self.catalog, &Control::default())
            .unwrap()
    }
}
fn allow_ref() -> SemanticParameterRef {
    SemanticParameterRef {
        id: SemanticParameterId::new(71),
        expected_key: SemanticParameterKey::AllowThrowException,
    }
}
fn request_at<'a>(
    source: &'a AggregateCall,
    node: &'a PhysicalNode,
    site: PhysicalCallSite,
    f: &Fixture,
    c: &Control,
) -> Result<AuthoredPhysicalAggregateUpdateRequest<'a>, PhysicalAggregateRequestError> {
    let mut w = CompileCheckpoints::try_new(c, PHASE)?;
    let r = author_physical_aggregate_update_request_observed(
        source,
        node,
        site,
        &f.fragment,
        &f.pools,
        policy(),
        &mut w,
    );
    if matches!(&r, Err(PhysicalAggregateRequestError::Control(_))) {
        return r;
    }
    w.finish()?;
    r
}
fn request<'a>(
    f: &'a Fixture,
    ordinal: usize,
    c: &Control,
) -> Result<AuthoredPhysicalAggregateUpdateRequest<'a>, PhysicalAggregateRequestError> {
    request_at(
        f.call(ordinal),
        f.node(),
        f.site(u32::try_from(ordinal).unwrap()),
        f,
        c,
    )
}
fn context(o: &AuthoredPhysicalOccurrences, site: PhysicalCallSite) -> ExpressionEffectContext {
    o.relational_contexts
        .iter()
        .find(|(s, _)| *s == site)
        .unwrap()
        .1
}
fn child(o: &AuthoredPhysicalOccurrences, f: &Fixture, call: u32, arg: u32) -> ExpressionUseId {
    o.root_uses.bindings()[&ExpressionRootSite {
        node: f.owner,
        role: ExpressionRootRole::AggregateArgument {
            call,
            argument: arg,
        },
    }]
}
fn summaries(
    o: &AuthoredPhysicalOccurrences,
) -> BTreeMap<ExpressionUseId, ScopedExpressionEffects> {
    o.root_uses
        .flow()
        .uses()
        .iter()
        .map(|(&id, u)| (id, ScopedExpressionEffects::pure_value(u.context)))
        .collect()
}
fn input<'a>(
    f: &'a Fixture,
    r: &'a AuthoredPhysicalAggregateUpdateRequest<'a>,
    o: &'a AuthoredPhysicalOccurrences<'a>,
    e: &'a BTreeMap<ExpressionUseId, ScopedExpressionEffects>,
    p: &'a SemanticParameters,
) -> PhysicalAggregateOccurrenceInput<'a> {
    PhysicalAggregateOccurrenceInput {
        fragment: &f.fragment,
        node: f.node(),
        source: r.source(),
        request: r,
        occurrences: o,
        child_effects: e,
        parameters: p,
        environment: &[],
        decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
        proof_scope: CallProofScope::Region(context(o, r.site()).region),
    }
}
fn run(
    input: PhysicalAggregateOccurrenceInput<'_>,
    catalog: &dyn SqlFunctionCatalog,
    c: &Control,
) -> Result<FreshPhysicalAggregateOccurrence, PhysicalAggregateOccurrenceError> {
    let mut w = CompileCheckpoints::try_new(c, PHASE)?;
    let r = prepare_physical_aggregate_occurrence_observed(input, catalog, &mut w);
    if matches!(&r, Err(PhysicalAggregateOccurrenceError::Control(_))) {
        return r;
    }
    w.finish()?;
    r
}
fn prefixes(
    mut invoke: impl FnMut(
        &Control,
    )
        -> Result<FreshPhysicalAggregateOccurrence, PhysicalAggregateOccurrenceError>,
    success: bool,
    wide: bool,
) {
    let c = Control::default();
    assert_eq!(invoke(&c).is_ok(), success);
    let trace = c.trace();
    assert_eq!(trace.first(), Some(&(PHASE, 0)));
    assert!(trace.len() > 1);
    let positions: Vec<_> = if wide {
        assert!(trace.iter().any(|(_, n)| *n == 256));
        (0..trace.len())
            .filter(|&i| i == 0 || i + 1 == trace.len() || trace[i].1 == 256)
            .collect()
    } else {
        (0..trace.len()).collect()
    };
    for at in positions {
        for cause in CAUSES {
            let c = Control {
                trace: Mutex::default(),
                refusal: Some((at, cause.clone())),
            };
            assert!(
                matches!(invoke(&c),Err(PhysicalAggregateOccurrenceError::Control(actual)) if actual==cause),
                "at {at}: {cause:?}"
            );
            assert_eq!(c.trace(), trace[..=at]);
        }
    }
}
fn request_prefixes<'a>(
    mut invoke: impl FnMut(
        &Control,
    ) -> Result<
        AuthoredPhysicalAggregateUpdateRequest<'a>,
        PhysicalAggregateRequestError,
    >,
    success: bool,
) {
    let c = Control::default();
    assert_eq!(invoke(&c).is_ok(), success);
    let trace = c.trace();
    assert!(trace.len() > 1);
    for at in 0..trace.len() {
        for cause in CAUSES {
            let c = Control {
                trace: Mutex::default(),
                refusal: Some((at, cause.clone())),
            };
            assert!(
                matches!(invoke(&c),Err(PhysicalAggregateRequestError::Control(actual)) if actual==cause)
            );
            assert_eq!(c.trace(), trace[..=at]);
        }
    }
}

#[test]
fn aggregate_occurrences_installed_count_min_max_single_partial_keep_complete_selected_state() {
    for name in ["count", "min", "max"] {
        for phase in [
            AggregatePhase::Single,
            AggregatePhase::Partial {
                sequence: AggregateSequenceId::new(u32::MAX),
            },
        ] {
            for nullable in [false, true] {
                let f = Fixture::new(
                    name,
                    ty(DataType::Int64, nullable),
                    Source::Value,
                    phase,
                    1,
                    0,
                    |_| {},
                );
                let o = f.occurrences();
                let e = summaries(&o);
                let p = SemanticParameters::try_new([]).unwrap();
                let r = request(&f, 0, &Control::default()).unwrap();
                let result =
                    run(input(&f, &r, &o, &e, &p), &f.catalog, &Control::default()).unwrap();
                assert_eq!(result.frozen.site, f.site(0));
                assert_eq!(result.frozen.context, context(&o, f.site(0)));
                assert!(Arc::ptr_eq(
                    r.selected(),
                    result.preparation.call_contract().selected_owner()
                ));
                assert_eq!(
                    result.preparation.implementation().abi,
                    PureKernelAbi::AggregateWindowV1
                );
                assert_eq!(
                    &result.frozen.effects,
                    result.preparation.call_contract().effects()
                );
                assert!(
                    matches!(&r.request().arguments[0], FunctionArgument::Value{value_type,constant:None} if value_type == &ty(DataType::Int64,nullable))
                );
                let PreparedPureKernel::Aggregate(kernel) = result.preparation.prepared() else {
                    panic!("aggregate update handle")
                };
                let c = kernel.contract();
                assert_eq!(
                    c.phase(),
                    if phase == AggregatePhase::Single {
                        AggregateKernelPhase::Single
                    } else {
                        AggregateKernelPhase::Partial
                    }
                );
                assert_eq!(c.intermediate_type(), &f.call(0).binding.intermediate_type);
                assert_eq!(
                    c.state_format().as_str(),
                    f.call(0).binding.state_format.as_str()
                );
                assert_eq!(c.final_type(), &f.call(0).binding.function.result_type);
                assert_eq!(c.call().logical_argument_count(), 1);
                assert!(c.order_keys().is_empty());
                assert!(c.state_input_type().is_none());
                assert_eq!(
                    result.frozen.effects.argument_control,
                    ArgumentControl::Aggregate
                );
                assert_eq!(
                    result.frozen.effects.instance_state,
                    FunctionInstanceState::AggregateInstance
                );
                assert_eq!(
                    result.frozen.effects.own_row_error,
                    FunctionIntrinsicRowError::NotRowEvaluated
                );
                assert_eq!(
                    result.frozen.effects.null_behavior,
                    FunctionNullBehavior::CalledOnNull
                );
                prefixes(
                    |c| run(input(&f, &r, &o, &e, &p), &f.catalog, c),
                    true,
                    false,
                );
            }
        }
    }
    let f = Fixture::new(
        "count",
        ty(DataType::Int64, true),
        Source::Star,
        AggregatePhase::Single,
        1,
        0,
        |_| {},
    );
    let o = f.occurrences();
    assert!(
        o.root_uses
            .bindings()
            .values()
            .all(|id| o.root_uses.flow().uses().contains_key(id))
    );
    assert!(o.root_uses.bindings().is_empty());
    let e = summaries(&o);
    let p = SemanticParameters::try_new([]).unwrap();
    let r = request(&f, 0, &Control::default()).unwrap();
    assert_eq!(r.request().logical_argument_count, 0);
    assert!(r.request().arguments.is_empty());
    assert_eq!(
        r.selected().result_type,
        FunctionResultType::Scalar(ty(DataType::Int64, false))
    );
    prefixes(
        |c| run(input(&f, &r, &o, &e, &p), &f.catalog, c),
        true,
        false,
    );
}
#[test]
fn aggregate_occurrences_static_literal_cv_and_value_keep_original_payload_source() {
    for s in [Source::Literal, Source::Pool, Source::Value] {
        let f = Fixture::new(
            "min",
            ty(DataType::Int64, true),
            s,
            AggregatePhase::Single,
            1,
            0,
            |_| {},
        );
        let r = request(&f, 0, &Control::default()).unwrap();
        assert!(std::ptr::eq(r.source(), f.call(0)));
        assert!(std::ptr::eq(r.node(), f.node()));
        assert_eq!(
            r.request().expected_result_type,
            Some(&f.call(0).binding.function.result_type)
        );
        let FunctionArgument::Value {
            value_type,
            constant,
        } = &r.request().arguments[0]
        else {
            panic!("value")
        };
        assert_eq!(value_type, &ty(DataType::Int64, true));
        match s {
            Source::Pool => {
                let c = constant.as_ref().unwrap();
                assert_eq!(c.ordinal(), 1);
                assert!(std::ptr::eq(
                    c.pool().data(),
                    f.pools.entries()[&ConstantPoolId::new(u32::MAX)].data()
                ));
                assert!(!c.is_null_observed(PHASE, &Control::default()).unwrap());
            }
            Source::Literal => assert!(
                !constant
                    .as_ref()
                    .unwrap()
                    .is_null_observed(PHASE, &Control::default())
                    .unwrap()
            ),
            Source::Value => assert!(constant.is_none()),
            _ => unreachable!(),
        }
        request_prefixes(|c| request(&f, 0, c), true);
        let cloned = f.call(0).clone();
        assert!(matches!(
            request_at(&cloned, f.node(), f.site(0), &f, &Control::default()),
            Err(PhysicalAggregateRequestError::InvalidSource(_))
        ));
        request_prefixes(|c| request_at(&cloned, f.node(), f.site(0), &f, c), false);
    }
}
#[test]
fn aggregate_occurrences_repeated_definition_has_independent_child_and_relational_context() {
    let f = Fixture::new(
        "count",
        ty(DataType::Int64, true),
        Source::Value,
        AggregatePhase::Single,
        2,
        0,
        |_| {},
    );
    assert_eq!(f.call(0).arguments, f.call(1).arguments);
    let o = f.occurrences();
    let a = child(&o, &f, 0, 0);
    let b = child(&o, &f, 1, 0);
    assert_ne!(a, b);
    assert_ne!(
        o.root_uses.flow().uses()[&a].context.region,
        o.root_uses.flow().uses()[&b].context.region
    );
    assert_ne!(context(&o, f.site(0)).region, context(&o, f.site(1)).region);
    let e = summaries(&o);
    let p = SemanticParameters::try_new([]).unwrap();
    for at in 0..2 {
        let r = request(&f, at, &Control::default()).unwrap();
        prefixes(
            |c| run(input(&f, &r, &o, &e, &p), &f.catalog, c),
            true,
            false,
        );
    }
}
#[test]
fn aggregate_occurrences_order_channels_keep_order_and_are_refused_by_actual_owner() {
    let f = Fixture::new(
        "min",
        ty(DataType::Int64, true),
        Source::Value,
        AggregatePhase::Single,
        1,
        2,
        |_| {},
    );
    let o = f.occurrences();
    let e = summaries(&o);
    let p = SemanticParameters::try_new([]).unwrap();
    let r = request(&f, 0, &Control::default()).unwrap();
    assert_eq!(r.request().logical_argument_count, 1);
    assert_eq!(r.request().arguments.len(), 3);
    let PureCallPreparation::Aggregate { options, .. } =
        r.preparation(ScopedExpressionEffects::pure_value(context(&o, f.site(0))))
    else {
        panic!("aggregate options")
    };
    assert_eq!(
        options
            .order_keys
            .iter()
            .map(|k| (k.ascending, k.nulls_first))
            .collect::<Vec<_>>(),
        [(false, true), (true, false)]
    );
    assert!(!options.distinct);
    assert!(options.state_input_type.is_none());
    let arg = child(&o, &f, 0, 0);
    let order = o.root_uses.bindings()[&ExpressionRootSite {
        node: f.owner,
        role: ExpressionRootRole::AggregateOrder { call: 0, key: 0 },
    }];
    assert_ne!(arg, order);
    assert!(run(input(&f, &r, &o, &e, &p), &f.catalog, &Control::default()).is_err());
    prefixes(
        |c| run(input(&f, &r, &o, &e, &p), &f.catalog, c),
        false,
        false,
    );
}
#[test]
fn aggregate_occurrences_genuine_composed_arithmetic_child_error_survives_lifecycle() {
    let f = Fixture::new(
        "count",
        ty(DataType::Int64, true),
        Source::ChildError,
        AggregatePhase::Single,
        1,
        0,
        |_| {},
    );
    let o = f.occurrences();
    let p = SemanticParameters::try_new([(
        allow_ref().id,
        SemanticParameterValue::AllowThrowException(false),
    )])
    .unwrap();
    let e = author_physical_expression_effects_observed(
        PhysicalExpressionEffectsInput {
            temporal_sources: None,
            fragment: &f.fragment,
            roots: &o.root_uses,
            constants: &f.pools,
            parameters: &p,
            literal_policy: policy(),
            call_scopes: &BTreeMap::new(),
        },
        &f.catalog,
        &Control::default(),
    )
    .unwrap();
    let id = child(&o, &f, 0, 0);
    assert!(
        e.summaries[&id]
            .for_use(o.root_uses.flow().uses()[&id].context)
            .unwrap()
            .may_raise_row_error
    );
    let r = request(&f, 0, &Control::default()).unwrap();
    let actual = run(
        input(&f, &r, &o, &e.summaries, &p),
        &f.catalog,
        &Control::default(),
    )
    .unwrap();
    assert_eq!(
        actual.frozen.effects.own_row_error,
        FunctionIntrinsicRowError::NotRowEvaluated
    );
    assert!(
        actual
            .preparation
            .effects()
            .for_use(context(&o, f.site(0)))
            .unwrap()
            .may_raise_row_error
    );
    prefixes(
        |c| run(input(&f, &r, &o, &e.summaries, &p), &f.catalog, c),
        true,
        false,
    );
}
#[test]
fn aggregate_occurrences_missing_foreign_source_scope_environment_and_proof_keep_typed_tails() {
    let f = Fixture::new(
        "count",
        ty(DataType::Int64, true),
        Source::Value,
        AggregatePhase::Single,
        1,
        0,
        |_| {},
    );
    let o = f.occurrences();
    let r = request(&f, 0, &Control::default()).unwrap();
    let e = summaries(&o);
    let p = SemanticParameters::try_new([]).unwrap();
    let id = child(&o, &f, 0, 0);
    let mut missing = e.clone();
    missing.remove(&id);
    assert!(
        matches!(run(input(&f,&r,&o,&missing,&p),&f.catalog,&Control::default()),Err(PhysicalAggregateOccurrenceError::Relational(PhysicalRelationalEffectsError::MissingChildEffects(actual))) if actual==id)
    );
    prefixes(
        |c| run(input(&f, &r, &o, &missing, &p), &f.catalog, c),
        false,
        false,
    );
    let mut foreign = e.clone();
    foreign.insert(
        id,
        ScopedExpressionEffects::pure_value(context(&o, f.site(0))),
    );
    prefixes(
        |c| run(input(&f, &r, &o, &foreign, &p), &f.catalog, c),
        false,
        false,
    );
    let cloned = f.call(0).clone();
    prefixes(
        |c| {
            let mut i = input(&f, &r, &o, &e, &p);
            i.source = &cloned;
            run(i, &f.catalog, c)
        },
        false,
        false,
    );
    let cloned_node = f.node().clone();
    prefixes(
        |c| {
            let mut i = input(&f, &r, &o, &e, &p);
            i.node = &cloned_node;
            run(i, &f.catalog, c)
        },
        false,
        false,
    );
    let mut o2 = o.clone();
    o2.relational_contexts.push(o2.relational_contexts[0]);
    prefixes(
        |c| run(input(&f, &r, &o2, &e, &p), &f.catalog, c),
        false,
        false,
    );
    let mut o3 = o.clone();
    o3.relational_contexts[0].1 = o.root_uses.flow().uses()[&id].context;
    prefixes(
        |c| run(input(&f, &r, &o3, &e, &p), &f.catalog, c),
        false,
        false,
    );
    let rootless = Fixture::new(
        "count",
        ty(DataType::Int64, true),
        Source::Star,
        AggregatePhase::Single,
        1,
        0,
        |_| {},
    );
    let unrelated = rootless.occurrences();
    assert!(matches!(
        run(
            input(&f, &r, &unrelated, &e, &p),
            &f.catalog,
            &Control::default()
        ),
        Err(PhysicalAggregateOccurrenceError::Relational(
            PhysicalRelationalEffectsError::InvalidSource(_)
        ))
    ));
    prefixes(
        |c| run(input(&f, &r, &unrelated, &e, &p), &f.catalog, c),
        false,
        false,
    );
    let reference = SemanticParameterRef {
        id: SemanticParameterId::new(7),
        expected_key: SemanticParameterKey::TimeZone,
    };
    let refs = [reference];
    let ep = SemanticParameters::try_new([(
        reference.id,
        SemanticParameterValue::TimeZone("UTC".into()),
    )])
    .unwrap();
    prefixes(
        |c| {
            let mut i = input(&f, &r, &o, &e, &ep);
            i.environment = &refs;
            run(i, &f.catalog, c)
        },
        false,
        false,
    );
    prefixes(
        |c| {
            let mut i = input(&f, &r, &o, &e, &p);
            i.proof_scope = CallProofScope::Region(o.root_uses.flow().uses()[&id].context.region);
            run(i, &f.catalog, c)
        },
        false,
        false,
    );
}
#[test]
fn aggregate_occurrences_state_format_drift_and_nominal_utf8_keep_selected_owner_gate() {
    for name in ["min", "max"] {
        let t = FunctionValueType::try_with_logical_type(
            DataType::Utf8,
            true,
            novarocks_type_contract::ValueLogicalType::Json,
        )
        .unwrap();
        let f = Fixture::new(
            name,
            t.clone(),
            Source::Value,
            AggregatePhase::Partial {
                sequence: AggregateSequenceId::new(0),
            },
            1,
            0,
            |_| {},
        );
        let o = f.occurrences();
        let e = summaries(&o);
        let p = SemanticParameters::try_new([]).unwrap();
        let r = request(&f, 0, &Control::default()).unwrap();
        let result = run(input(&f, &r, &o, &e, &p), &f.catalog, &Control::default()).unwrap();
        let PreparedPureKernel::Aggregate(k) = result.preparation.prepared() else {
            panic!("aggregate")
        };
        assert_eq!(k.contract().intermediate_type(), &t);
        assert_eq!(k.contract().final_type(), &t);
    }
    let f = Fixture::new(
        "count",
        ty(DataType::Int64, true),
        Source::Value,
        AggregatePhase::Single,
        1,
        0,
        |b| b.state_format = AggregateStateFormatId::try_new("foreign/state").unwrap(),
    );
    let o = f.occurrences();
    let e = summaries(&o);
    let p = SemanticParameters::try_new([]).unwrap();
    let r = request(&f, 0, &Control::default()).unwrap();
    prefixes(
        |c| run(input(&f, &r, &o, &e, &p), &f.catalog, c),
        false,
        false,
    );
}
#[test]
fn aggregate_occurrences_wide_actual_channels_and_contexts_observe_real_quantum() {
    let fields: Vec<_> = (0..320)
        .map(|n| {
            Arc::new(
                Field::new(format!("source-{n}"), DataType::Int64, true).with_metadata(
                    std::collections::HashMap::from([("source.key".into(), n.to_string())]),
                ),
            )
        })
        .collect();
    let t = ty(DataType::Struct(fields.into()), true);
    let f = Fixture::new(
        "count",
        t.clone(),
        Source::Value,
        AggregatePhase::Single,
        1,
        0,
        |_| {},
    );
    let o = f.occurrences();
    let e = summaries(&o);
    let p = SemanticParameters::try_new([]).unwrap();
    let r = request(&f, 0, &Control::default()).unwrap();
    assert!(
        matches!(&r.request().arguments[0],FunctionArgument::Value{value_type,constant:None}if value_type==&t)
    );
    let novarocks_functions::FunctionArgumentType::Value(selected) =
        &r.selected().argument_types[0]
    else {
        panic!("selected value")
    };
    let (DataType::Struct(original), DataType::Struct(actual)) =
        (&t.data_type(), &selected.data_type())
    else {
        panic!("complete source")
    };
    assert_eq!(original.len(), 320);
    for (a, b) in original.iter().zip(actual) {
        assert!(Arc::ptr_eq(a, b));
        assert_eq!(a.metadata(), b.metadata());
    }
    prefixes(
        |c| run(input(&f, &r, &o, &e, &p), &f.catalog, c),
        true,
        true,
    );
    let many = Fixture::new(
        "count",
        ty(DataType::Int64, true),
        Source::Star,
        AggregatePhase::Single,
        320,
        0,
        |_| {},
    );
    let o = many.occurrences();
    assert_eq!(o.relational_contexts.len(), 320);
    let e = summaries(&o);
    let r = request(&many, 319, &Control::default()).unwrap();
    prefixes(
        |c| run(input(&many, &r, &o, &e, &p), &many.catalog, c),
        true,
        true,
    );
}

// This fixture proves the original fragment phase/port contract. It does not
// claim the full Plan's exactly-one-final aggregate/top-N sequence closure.
fn merge_fixture(phase: AggregatePhase, topn: bool) -> Fixture {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let mut b = FragmentBuilder::new(FragmentId::new(71));
    let leaf = NodeId::new(7);
    let partial = NodeId::new(17);
    let owner = NodeId::new(901);
    let key_type = ty(DataType::Int64, false);
    let seed = b.constant(leaf, key_type.clone(), checked::i64(3));
    let key = b
        .add_value(
            key_type.clone(),
            ValueOrigin::NodeOutput {
                node: leaf,
                output_ordinal: 0,
            },
        )
        .unwrap();
    b.add_values(leaf, Box::from([Box::from([seed])]), Box::from([key]))
        .unwrap();
    let key_expr = b
        .add_expression(partial, key_type.clone(), ExprKind::Value(key))
        .unwrap();
    let partial_phase = AggregatePhase::Partial {
        sequence: AggregateSequenceId::new(u32::MAX),
    };
    let binding = binding(&catalog, "count", None, 0, partial_phase);
    let partial_id = AggregateCallId::new(0);
    let state = b
        .add_value(
            binding.intermediate_type.clone(),
            ValueOrigin::AggregateState {
                call: partial_id,
                phase: partial_phase,
            },
        )
        .unwrap();
    b.insert_node_unchecked(PhysicalNode {
        id: partial,
        inputs: Box::from([leaf]),
        required_inputs: Box::from([props()]),
        output_properties: props(),
        output: OutputPort {
            node: partial,
            columns: Box::from([key, state]),
        },
        kind: NodeKind::Aggregate {
            group_by: Box::from([(key_expr, key)]),
            calls: Box::from([AggregateCall {
                id: partial_id,
                binding: binding.clone(),
                arguments: Box::default(),
                distinct: false,
                order_by: Box::default(),
                output: state,
            }]),
            grouping: AggregateGrouping::Complete,
        },
    })
    .unwrap();
    let state_expr = b
        .add_expression(
            owner,
            binding.intermediate_type.clone(),
            ExprKind::Value(state),
        )
        .unwrap();
    let mut binding = binding;
    binding.phase = phase;
    let call_id = AggregateCallId::new(u32::MAX);
    let (out_type, origin) = if phase.produces_final_result() {
        (
            binding.function.result_type.clone(),
            ValueOrigin::AggregateResult { call: call_id },
        )
    } else {
        (
            binding.intermediate_type.clone(),
            ValueOrigin::AggregateState {
                call: call_id,
                phase,
            },
        )
    };
    let output = b.add_value(out_type, origin).unwrap();
    let call = AggregateCall {
        id: call_id,
        binding,
        arguments: Box::from([state_expr]),
        distinct: false,
        order_by: Box::default(),
        output,
    };
    let (kind, columns): (NodeKind, Box<[ValueId]>) = if topn {
        let key_expr = b
            .add_expression(owner, key_type, ExprKind::Value(key))
            .unwrap();
        (
            NodeKind::TopN {
                reduction: TopNReduction::GroupedStates {
                    group_by: Box::from([(key_expr, key)]),
                    calls: Box::from([call]),
                    comparator: OrderedComparisonAlgorithm::NativeScalarOrderV1,
                },
                order_by: Box::from([SortExpr {
                    expr: key_expr,
                    direction: SortDirection::Ascending,
                    null_ordering: NullOrdering::Last,
                }]),
                limit: 3,
                offset: 0,
                phase: TopNPhase::Partial {
                    sequence: TopNSequenceId::new(u32::MAX),
                },
            },
            Box::from([key, output]),
        )
    } else {
        (
            NodeKind::Aggregate {
                group_by: Box::default(),
                calls: Box::from([call]),
                grouping: AggregateGrouping::Complete,
            },
            Box::from([output]),
        )
    };
    b.insert_node_unchecked(PhysicalNode {
        id: owner,
        inputs: Box::from([partial]),
        required_inputs: Box::from([props()]),
        output_properties: props(),
        output: OutputPort {
            node: owner,
            columns,
        },
        kind,
    })
    .unwrap();
    let fragment = finish(b, owner);
    let pools = fragment.constants().clone();
    Fixture {
        fragment,
        pools,
        catalog,
        owner,
    }
}

#[test]
fn aggregate_occurrences_actual_merge_and_grouped_topn_require_missing_logical_source_before_expansion()
 {
    for phase in [
        AggregatePhase::Intermediate {
            sequence: AggregateSequenceId::new(u32::MAX),
        },
        AggregatePhase::Final {
            sequence: AggregateSequenceId::new(u32::MAX),
        },
    ] {
        let f = merge_fixture(phase, false);
        assert!(
            matches!(request(&f, 0, &Control::default()), Err(PhysicalAggregateRequestError::MissingLogicalSource(actual)) if actual == phase)
        );
        request_prefixes(|c| request(&f, 0, c), false);
    }
    let phase = AggregatePhase::Intermediate {
        sequence: AggregateSequenceId::new(u32::MAX),
    };
    let f = merge_fixture(phase, true);
    let NodeKind::TopN {
        reduction: TopNReduction::GroupedStates { calls, .. },
        phase: TopNPhase::Partial { .. },
        ..
    } = &f.node().kind
    else {
        panic!("actual grouped TopN")
    };
    let site = PhysicalCallSite::TopNState {
        node: f.owner,
        call: 0,
    };
    let o = f.occurrences();
    assert!(o.root_uses.bindings().contains_key(&ExpressionRootSite {
        node: f.owner,
        role: ExpressionRootRole::TopNStateArgument {
            call: 0,
            argument: 0
        }
    }));
    assert!(
        matches!(request_at(&calls[0], f.node(), site, &f, &Control::default()), Err(PhysicalAggregateRequestError::MissingLogicalSource(actual)) if actual == phase)
    );
    request_prefixes(|c| request_at(&calls[0], f.node(), site, &f, c), false);
}

fn fragment_scopes<'a>(
    f: &'a Fixture,
    o: &AuthoredPhysicalOccurrences,
) -> BTreeMap<
    PhysicalCallSite,
    super::super::physical_fragment_effects::PhysicalRelationalCallSourceScope<'a>,
> {
    o.relational_contexts
        .iter()
        .map(|&(site, context)| {
            let node = match site {
                PhysicalCallSite::Aggregate { node, .. }
                | PhysicalCallSite::TopNState { node, .. } => node,
                _ => panic!("aggregate fixture site"),
            };
            (
                site,
                super::super::physical_fragment_effects::PhysicalRelationalCallSourceScope {
                    source: &f.fragment.nodes()[&node],
                    decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                    environment: &[],
                    proof_scope: CallProofScope::Region(context.region),
                },
            )
        })
        .collect()
}
fn fragment_run(
    f: &Fixture,
    o: &AuthoredPhysicalOccurrences,
    p: &SemanticParameters,
    scopes: &BTreeMap<
        PhysicalCallSite,
        super::super::physical_fragment_effects::PhysicalRelationalCallSourceScope<'_>,
    >,
    c: &Control,
) -> Result<
    super::super::physical_fragment_effects::AuthoredPhysicalFragmentEffects,
    super::super::physical_fragment_effects::PhysicalFragmentEffectsError,
> {
    super::super::physical_fragment_effects::author_physical_fragment_effects_observed(
        super::super::physical_fragment_effects::PhysicalFragmentEffectsInput {
            fragment: &f.fragment,
            occurrences: o,
            constants: &f.pools,
            parameters: p,
            literal_policy: policy(),
            expression_scopes: &BTreeMap::new(),
            relational_scopes: scopes,
        },
        &f.catalog,
        c,
    )
}
fn fragment_prefixes(
    mut invoke: impl FnMut(
        &Control,
    ) -> Result<
        super::super::physical_fragment_effects::AuthoredPhysicalFragmentEffects,
        super::super::physical_fragment_effects::PhysicalFragmentEffectsError,
    >,
    success: bool,
    wide: bool,
) {
    use super::super::physical_fragment_effects::PhysicalFragmentEffectsError;
    let c = Control::default();
    let outcome = invoke(&c);
    assert_eq!(outcome.is_ok(), success, "{outcome:?}");
    let trace = c.trace();
    assert_eq!(trace.first(), Some(&(PHASE, 0)));
    if success {
        assert!(trace.len() > 1);
        assert!(trace.iter().any(|(_, units)| *units > 0));
    } else {
        let original = outcome.unwrap_err();
        if matches!(
            &original,
            PhysicalFragmentEffectsError::MissingScope(_)
                | PhysicalFragmentEffectsError::InvalidSource(
                    "relational scope does not loan this actual source call"
                        | "relational source scopes contain an extra call site"
                )
        ) {
            // These small scope checks reject before the first bounded flush.
            // A completed semantic refusal does not start an outer footer.
            assert_eq!(trace, [(PHASE, 0)]);
        }
        for cause in CAUSES {
            let c = Control {
                trace: Mutex::default(),
                refusal: Some((trace.len(), cause)),
            };
            let actual = invoke(&c).unwrap_err();
            match (&actual, &original) {
                (
                    PhysicalFragmentEffectsError::MissingScope(actual),
                    PhysicalFragmentEffectsError::MissingScope(original),
                ) => assert_eq!(actual, original),
                (
                    PhysicalFragmentEffectsError::InvalidSource(actual),
                    PhysicalFragmentEffectsError::InvalidSource(original),
                ) => assert_eq!(actual, original),
                (
                    PhysicalFragmentEffectsError::AggregateRequest(
                        PhysicalAggregateRequestError::MissingLogicalSource(actual),
                    ),
                    PhysicalFragmentEffectsError::AggregateRequest(
                        PhysicalAggregateRequestError::MissingLogicalSource(original),
                    ),
                ) => assert_eq!(actual, original),
                _ => panic!(
                    "different original aggregate composition refusal: {actual:?}; expected {original:?}"
                ),
            }
            assert_eq!(c.trace(), trace);
        }
    }
    let positions: Vec<_> = if wide {
        assert!(trace.iter().any(|(_, n)| *n == 256));
        (0..trace.len())
            .filter(|&at| at == 0 || at + 1 == trace.len() || trace[at].1 == 256)
            .collect()
    } else {
        (0..trace.len()).collect()
    };
    for at in positions {
        for cause in CAUSES {
            let c = Control {
                trace: Mutex::default(),
                refusal: Some((at, cause.clone())),
            };
            assert!(
                matches!(invoke(&c),Err(PhysicalFragmentEffectsError::Control(actual)) if actual==cause),
                "at {at}: {cause:?}"
            );
            assert_eq!(c.trace(), trace[..=at]);
        }
    }
}

#[test]
fn aggregate_fragment_composer_single_partial_complete_coverage_keeps_real_child_error_and_lifecycle()
 {
    for name in ["count", "min", "max"] {
        for phase in [
            AggregatePhase::Single,
            AggregatePhase::Partial {
                sequence: AggregateSequenceId::new(11),
            },
        ] {
            let f = Fixture::new(
                name,
                ty(DataType::Int64, true),
                Source::ChildError,
                phase,
                2,
                0,
                |_| {},
            );
            let o = f.occurrences();
            let scopes = fragment_scopes(&f, &o);
            let p = SemanticParameters::try_new([(
                allow_ref().id,
                SemanticParameterValue::AllowThrowException(false),
            )])
            .unwrap();
            let result = fragment_run(&f, &o, &p, &scopes, &Control::default()).unwrap();
            assert_eq!(result.calls.entries().len(), 2);
            for ordinal in 0..2 {
                let call = &result.calls.entries()[&f.site(ordinal)];
                assert_eq!(call.context, context(&o, f.site(ordinal)));
                assert_eq!(
                    call.effects.instance_state,
                    FunctionInstanceState::AggregateInstance
                );
                assert_eq!(
                    call.effects.own_row_error,
                    FunctionIntrinsicRowError::NotRowEvaluated
                );
                let id = child(&o, &f, ordinal, 0);
                assert!(
                    result.summaries[&id]
                        .for_use(o.root_uses.flow().uses()[&id].context)
                        .unwrap()
                        .may_raise_row_error
                );
            }
            result
                .calls
                .validate_fragment(&f.fragment, &o.root_uses, &Control::default())
                .unwrap();
            fragment_prefixes(|c| fragment_run(&f, &o, &p, &scopes, c), true, false);
        }
    }
}

#[test]
fn aggregate_fragment_composer_missing_extra_and_same_shape_foreign_source_scopes_refuse() {
    use super::super::physical_fragment_effects::{
        PhysicalFragmentEffectsError, PhysicalRelationalCallSourceScope,
    };
    let f = Fixture::new(
        "count",
        ty(DataType::Int64, true),
        Source::Star,
        AggregatePhase::Single,
        1,
        0,
        |_| {},
    );
    let o = f.occurrences();
    let p = SemanticParameters::try_new([]).unwrap();
    let missing = BTreeMap::new();
    assert!(
        matches!(fragment_run(&f,&o,&p,&missing,&Control::default()),Err(PhysicalFragmentEffectsError::MissingScope(site)) if site==f.site(0))
    );
    fragment_prefixes(|c| fragment_run(&f, &o, &p, &missing, c), false, false);
    let foreign = f.node().clone();
    let mut scopes = fragment_scopes(&f, &o);
    scopes.get_mut(&f.site(0)).unwrap().source = &foreign;
    assert!(matches!(
        fragment_run(&f, &o, &p, &scopes, &Control::default()),
        Err(PhysicalFragmentEffectsError::InvalidSource(
            "relational scope does not loan this actual source call"
        ))
    ));
    fragment_prefixes(|c| fragment_run(&f, &o, &p, &scopes, c), false, false);
    let mut scopes = fragment_scopes(&f, &o);
    scopes.insert(
        f.site(1),
        PhysicalRelationalCallSourceScope {
            source: f.node(),
            decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
            environment: &[],
            proof_scope: CallProofScope::Region(context(&o, f.site(0)).region),
        },
    );
    assert!(matches!(
        fragment_run(&f, &o, &p, &scopes, &Control::default()),
        Err(PhysicalFragmentEffectsError::InvalidSource(
            "relational source scopes contain an extra call site"
        ))
    ));
    fragment_prefixes(|c| fragment_run(&f, &o, &p, &scopes, c), false, false);
}

#[test]
fn aggregate_fragment_composer_merge_and_grouped_topn_refuse_missing_logical_source_without_partial_publication()
 {
    use super::super::physical_fragment_effects::PhysicalFragmentEffectsError;
    for topn in [false, true] {
        let phase = if topn {
            AggregatePhase::Intermediate {
                sequence: AggregateSequenceId::new(71),
            }
        } else {
            AggregatePhase::Final {
                sequence: AggregateSequenceId::new(71),
            }
        };
        let f = merge_fixture(phase, topn);
        let o = f.occurrences();
        let scopes = fragment_scopes(&f, &o);
        let p = SemanticParameters::try_new([]).unwrap();
        assert!(
            matches!(fragment_run(&f,&o,&p,&scopes,&Control::default()),Err(PhysicalFragmentEffectsError::AggregateRequest(PhysicalAggregateRequestError::MissingLogicalSource(actual))) if actual==phase)
        );
        fragment_prefixes(|c| fragment_run(&f, &o, &p, &scopes, c), false, false);
    }
}

#[test]
fn aggregate_fragment_composer_320_actual_calls_observe_real_quantum_and_keep_exact_call_coverage()
{
    let f = Fixture::new(
        "count",
        ty(DataType::Int64, true),
        Source::Star,
        AggregatePhase::Single,
        320,
        0,
        |_| {},
    );
    let o = f.occurrences();
    let p = SemanticParameters::try_new([]).unwrap();
    let scopes = fragment_scopes(&f, &o);
    let result = fragment_run(&f, &o, &p, &scopes, &Control::default()).unwrap();
    assert_eq!(result.calls.entries().len(), 320);
    fragment_prefixes(|c| fragment_run(&f, &o, &p, &scopes, c), true, true);
}
