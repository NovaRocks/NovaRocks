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
    expression_occurrences::{AuthoredPhysicalOccurrences, author_physical_occurrences_observed},
    physical_table_requests::{
        AuthoredPhysicalTableRequest, PhysicalTableRequestError,
        author_physical_table_request_observed,
    },
};
use super::*;
use crate::functions::build_builtin_engine_function_catalog;
use crate::test_constants::{self as checked, Fragment, FragmentBuilder};
use arrow::{
    array::{Array, Int64Array, ListArray},
    buffer::OffsetBuffer,
    datatypes::{DataType, Field},
};
use novarocks_functions::{
    ConstantPool, ConstantValidation, EngineFunctionCatalog, FunctionArgument, FunctionResultType,
    PreparedPureKernel, ScopedExpressionEffects,
};
use novarocks_type_contract::{
    CompilePhase, EvaluationDemand, ExpressionEffectContext, FunctionInstanceState,
    FunctionIntrinsicRowError, FunctionNullBehavior, FunctionValueType, PureCompileControl,
    RelationEffectContext, SemanticParameterId, SemanticParameterKey, SemanticParameterValue,
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use {
    novarocks_expression_contract::{BinaryOperator, ExprKind, RootOrdinal, RootScope},
    novarocks_physical_plan::{
        BoundTableFunction, ConstantPoolId, ConstantPools, ConstantReference, Distribution, ExprId,
        FragmentId, FragmentSink, NodeId, NodeKind, PipelineDopDomain, PlanLimits,
        TableFunctionOutput, ValueOrigin,
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
fn ty(data_type: DataType, nullable: bool) -> FunctionValueType {
    FunctionValueType::new(data_type, nullable)
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
fn list_type() -> FunctionValueType {
    ty(
        DataType::List(Arc::new(Field::new("original-item", DataType::Int64, true))),
        true,
    )
}
#[derive(Clone, Copy)]
enum Source {
    Literal,
    Pool,
    Value,
    Again(usize),
    ChildError,
}
struct Fixture {
    fragment: Fragment,
    pools: ConstantPools,
    owner: NodeId,
    args: Vec<ExprId>,
    catalog: EngineFunctionCatalog,
    effects: BTreeMap<NodeId, NodeExpressionEffects>,
}
impl Fixture {
    fn new(types: &[FunctionValueType], sources: &[Source], left: bool, bad_result: bool) -> Self {
        assert_eq!(types.len(), sources.len());
        let catalog = build_builtin_engine_function_catalog().unwrap();
        let mut pools = ConstantPools::empty();
        if sources.iter().any(|s| matches!(s, Source::Pool)) {
            let DataType::List(field) = &types[0].data_type() else {
                panic!("list")
            };
            let array = ListArray::new(
                field.clone(),
                OffsetBuffer::new(vec![0i32, 1, 3, 3].into()),
                Arc::new(Int64Array::from(vec![99, 7, 8])),
                None,
            );
            let pool = ConstantPool::try_new(
                Arc::new(Field::new("whole-pool", array.data_type().clone(), true)),
                types[0].clone(),
                array.to_data(),
                policy(),
                CompilePhase::Validate,
                &Control::default(),
            )
            .unwrap();
            pools.insert(ConstantPoolId::new(u32::MAX), pool).unwrap();
        }
        let mut builder = FragmentBuilder::new(FragmentId::new(71));
        let leaf = NodeId::new(7);
        let owner = NodeId::new(901);
        let input_type = types[0].clone();
        let initial = constant(
            &mut builder,
            &mut pools,
            leaf,
            input_type.clone(),
            checked::null(),
        );
        let initial_root = root(&mut builder, leaf, initial, RootScope::AllPorts);
        let value = builder
            .add_value(
                input_type,
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
        let mut args = Vec::new();
        for (at, (&source, value_type)) in sources.iter().zip(types).enumerate() {
            if let Source::Again(index) = source {
                assert!(index < at);
                args.push(args[index]);
                continue;
            }
            let kind = match source {
                Source::Literal => constant_kind(&mut pools, value_type, checked::null()),
                Source::Pool => ExprKind::Constant(ConstantReference {
                    pool: ConstantPoolId::new(u32::MAX),
                    ordinal: 1,
                }),
                Source::Value => ExprKind::input(value),
                Source::ChildError => {
                    let a = constant(
                        &mut builder,
                        &mut pools,
                        owner,
                        ty(DataType::Int64, false),
                        checked::i64(i64::MAX),
                    );
                    let b = constant(
                        &mut builder,
                        &mut pools,
                        owner,
                        ty(DataType::Int64, false),
                        checked::i64(1),
                    );
                    let add = builder
                        .add_expression(
                            owner,
                            ty(DataType::Int64, true),
                            ExprKind::Binary {
                                op: BinaryOperator::Add,
                                left: a,
                                right: b,
                                decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                                allow_throw_exception: Some(allow_ref()),
                            },
                        )
                        .unwrap();
                    let test = builder
                        .add_expression(
                            owner,
                            ty(DataType::Boolean, false),
                            ExprKind::IsNull {
                                expr: add,
                                negated: false,
                            },
                        )
                        .unwrap();
                    let then = constant(
                        &mut builder,
                        &mut pools,
                        owner,
                        value_type.clone(),
                        checked::null(),
                    );
                    let otherwise = constant(
                        &mut builder,
                        &mut pools,
                        owner,
                        value_type.clone(),
                        checked::null(),
                    );
                    ExprKind::Case {
                        ieee_float_equality: false,
                        operand: None,
                        when_then: Box::from([(test, then)]),
                        else_expr: Some(otherwise),
                    }
                }
                Source::Again(_) => unreachable!(),
            };
            args.push(
                builder
                    .add_expression(owner, value_type.clone(), kind)
                    .unwrap(),
            );
        }
        let request: Vec<_> = types
            .iter()
            .cloned()
            .map(|value_type| FunctionArgument::Value {
                value_type,
                constant: None,
            })
            .collect();
        let resolved = catalog
            .resolve_table_binding("unnest", &request, &Control::default())
            .unwrap();
        let FunctionResultType::Relation(mut result_types) = resolved.selected.result_type else {
            panic!("relation")
        };
        if bad_result {
            let mut changed = result_types.to_vec();
            changed[0] = ty(DataType::Int32, true);
            result_types = novarocks_type_contract::TypedFactSlice::from_pure_boxed(
                changed.into_boxed_slice(),
            );
        }
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
        // Two repeated outer occurrences precede the relation columns. They
        // belong to the host layout, never the selected function relation.
        let mut output = vec![value, value];
        let mut outputs = vec![
            TableFunctionOutput::PassThrough(value),
            TableFunctionOutput::PassThrough(value),
        ];
        for (at, result) in result_types.iter().enumerate() {
            let result = builder
                .add_value(
                    result.clone(),
                    ValueOrigin::NodeOutput {
                        node: owner,
                        output_ordinal: u32::try_from(at + 2).unwrap(),
                    },
                )
                .unwrap();
            output.push(result);
            outputs.push(TableFunctionOutput::FunctionResult {
                result_ordinal: u32::try_from(at).unwrap(),
                value: result,
            });
        }
        let argument_roots = args
            .iter()
            .map(|&id| root(&mut builder, owner, id, RootScope::Port(0)))
            .collect::<Box<[_]>>();
        builder
            .add_row_expanding(
                owner,
                leaf,
                Distribution::Singleton,
                output.into_boxed_slice(),
                NodeKind::TableFunction {
                    function,
                    arguments: argument_roots,
                    outputs: outputs.into_boxed_slice(),
                    left_outer: left,
                },
            )
            .unwrap();
        let effects = seal_fixture_units(&mut builder, &pools, &catalog);
        let fragment = builder
            .finish_structure(
                owner,
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
        checked::merge(&mut pools, fragment.constants());
        Self {
            fragment,
            pools,
            owner,
            args,
            catalog,
            effects,
        }
    }
    fn source(&self) -> &PhysicalNode {
        &self.fragment.nodes()[&self.owner]
    }
    fn occurrences(&self) -> AuthoredPhysicalOccurrences<'_> {
        author_physical_occurrences_observed(&self.fragment, &self.catalog, &Control::default())
            .unwrap()
    }
}

fn constant_kind(
    pools: &mut ConstantPools,
    ty: &FunctionValueType,
    source: checked::Source,
) -> ExprKind {
    checked::insert(pools, source(ty))
}
fn constant(
    builder: &mut FragmentBuilder,
    pools: &mut ConstantPools,
    node: NodeId,
    ty: FunctionValueType,
    source: checked::Source,
) -> ExprId {
    let kind = constant_kind(pools, &ty, source);
    builder.add_expression(node, ty, kind).unwrap()
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
fn seal_fixture_units(
    builder: &mut FragmentBuilder,
    pools: &ConstantPools,
    _catalog: &EngineFunctionCatalog,
) -> BTreeMap<NodeId, NodeExpressionEffects> {
    // Fixed structural fixtures retain the original scalar author's summaries
    // beside exactly the Arc installed in their real node. No SQL journal is
    // minted and no scalar control/effect derivation is duplicated here.
    let nodes = builder
        .construction_nodes()
        .map(|(id, _)| *id)
        .collect::<Vec<_>>();
    let parameters = SemanticParameters::try_new([(
        allow_ref().id,
        SemanticParameterValue::AllowThrowException(false),
    )])
    .unwrap();
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
            pools,
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
    effects
}
fn summaries(fixture: &Fixture) -> BTreeMap<NodeId, NodeExpressionEffects> {
    fixture
        .effects
        .iter()
        .map(|(&node, effects)| {
            let unit = fixture.fragment.nodes()[&node]
                .expressions
                .as_ref()
                .unwrap()
                .clone();
            (
                node,
                node_effects_with_test_summaries(unit, effects.summaries().clone()),
            )
        })
        .collect()
}

fn allow_ref() -> SemanticParameterRef {
    SemanticParameterRef {
        id: SemanticParameterId::new(71),
        expected_key: SemanticParameterKey::AllowThrowException,
    }
}
fn request<'a>(
    source: &'a PhysicalNode,
    fixture: &Fixture,
    control: &Control,
) -> Result<AuthoredPhysicalTableRequest<'a>, PhysicalTableRequestError> {
    let mut work = CompileCheckpoints::try_new(control, PHASE)?;
    let result = author_physical_table_request_observed(
        source,
        &fixture.fragment,
        &fixture.pools,
        policy(),
        &mut work,
    );
    if matches!(&result, Err(PhysicalTableRequestError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn copy_occurrences<'a>(
    source: &AuthoredPhysicalOccurrences<'a>,
) -> AuthoredPhysicalOccurrences<'a> {
    source.clone()
}
fn context(occurrences: &AuthoredPhysicalOccurrences, owner: NodeId) -> RelationEffectContext {
    occurrences
        .relational_contexts
        .iter()
        .find(|(site, _)| *site == PhysicalCallSite::Table { node: owner })
        .unwrap()
        .1
}
fn child(fixture: &Fixture, ordinal: u32) -> ExpressionUseId {
    let NodeKind::TableFunction { arguments, .. } = &fixture.source().kind else {
        panic!("table")
    };
    let unit = fixture.source().expressions.as_ref().unwrap();
    unit.root_use_ids()[arguments[ordinal as usize].get() as usize]
}

fn input<'a>(
    fixture: &'a Fixture,
    req: &'a AuthoredPhysicalTableRequest<'a>,
    occ: &'a AuthoredPhysicalOccurrences<'a>,
    effects: &'a BTreeMap<NodeId, NodeExpressionEffects>,
    params: &'a SemanticParameters,
) -> PhysicalTableOccurrenceInput<'a> {
    PhysicalTableOccurrenceInput {
        fragment: &fixture.fragment,
        source: fixture.source(),
        request: req,
        occurrences: occ,
        child_effects: effects.get(&fixture.owner),
        parameters: params,
        environment: &[],
        decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
        proof_scope: CallProofScope::Relation(context(occ, fixture.owner).identity),
    }
}
fn run(
    input: PhysicalTableOccurrenceInput<'_>,
    catalog: &dyn SqlFunctionCatalog,
    control: &Control,
) -> Result<FreshPhysicalTableOccurrence, PhysicalTableOccurrenceError> {
    let mut work = CompileCheckpoints::try_new(control, PHASE)?;
    let result = prepare_physical_table_occurrence_observed(input, catalog, &mut work);
    if matches!(&result, Err(PhysicalTableOccurrenceError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn prefixes(
    mut invoke: impl FnMut(
        &Control,
    ) -> Result<FreshPhysicalTableOccurrence, PhysicalTableOccurrenceError>,
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
                matches!(invoke(&c),Err(PhysicalTableOccurrenceError::Control(actual)) if actual==cause),
                "at {at}: {cause:?}"
            );
            assert_eq!(c.trace(), trace[..=at]);
        }
    }
}

fn request_prefixes(source: &PhysicalNode, fixture: &Fixture, success: bool) {
    let baseline = Control::default();
    assert_eq!(request(source, fixture, &baseline).is_ok(), success);
    let trace = baseline.trace();
    assert!(trace.len() > 1);
    for at in 0..trace.len() {
        for cause in CAUSES {
            let control = Control {
                trace: Mutex::default(),
                refusal: Some((at, cause.clone())),
            };
            assert!(
                matches!(request(source, fixture, &control), Err(PhysicalTableRequestError::Control(actual)) if actual == cause)
            );
            assert_eq!(control.trace(), trace[..=at]);
        }
    }
}

#[test]
fn table_occurrences_static_request_keeps_literal_cv_ordinal_and_materialized_value_distinct() {
    let t = list_type();
    let f = Fixture::new(
        &[t.clone(), t.clone(), t.clone()],
        &[Source::Literal, Source::Pool, Source::Value],
        false,
        false,
    );
    let req = request(f.source(), &f, &Control::default()).unwrap();
    assert!(std::ptr::eq(req.source(), f.source()));
    assert!(req.request().expected_result_type.is_none());
    let FunctionArgument::Value {
        constant: Some(literal),
        ..
    } = &req.request().arguments[0]
    else {
        panic!("literal")
    };
    assert!(
        literal
            .is_null_observed(PHASE, &Control::default())
            .unwrap()
    );
    let FunctionArgument::Value {
        constant: Some(value),
        ..
    } = &req.request().arguments[1]
    else {
        panic!("CV")
    };
    assert_eq!(value.ordinal(), 1);
    assert!(std::ptr::eq(
        value.pool().data(),
        f.pools.entries()[&ConstantPoolId::new(u32::MAX)].data()
    ));
    assert!(!value.is_null_observed(PHASE, &Control::default()).unwrap());
    assert!(
        matches!(&req.request().arguments[2],FunctionArgument::Value{constant:None,value_type} if value_type==&t)
    );
    let FunctionResultType::Relation(results) = &req.selected().result_type else {
        panic!("relation")
    };
    assert_eq!(results.len(), 3);
    assert!(
        results
            .iter()
            .all(|result| result == &ty(DataType::Int64, true))
    );
    assert!(req.selected().aggregate.is_none());
    let foreign = f.source().clone();
    assert!(matches!(
        request(&foreign, &f, &Control::default()),
        Err(PhysicalTableRequestError::InvalidSource(_))
    ));
    request_prefixes(f.source(), &f, true);
    request_prefixes(&foreign, &f, false);
}
#[test]
fn table_occurrences_installed_unnest_keeps_nested_metadata_relation_and_left_host_layout() {
    let child = Arc::new(
        Field::new("decorated", DataType::Int64, true).with_metadata(
            std::collections::HashMap::from([("provider.key".into(), "value".into())]),
        ),
    );
    let t = ty(
        DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(vec![child.clone()].into()),
            true,
        ))),
        true,
    );
    for left in [false, true] {
        let f = Fixture::new(std::slice::from_ref(&t), &[Source::Value], left, false);
        let occ = f.occurrences();
        let req = request(f.source(), &f, &Control::default()).unwrap();
        let effects = summaries(&f);
        let params = SemanticParameters::try_new([]).unwrap();
        let result = run(
            input(&f, &req, &occ, &effects, &params),
            &f.catalog,
            &Control::default(),
        )
        .unwrap();
        assert_eq!(
            result.frozen.site,
            PhysicalCallSite::Table { node: f.owner }
        );
        assert_eq!(result.frozen.context, context(&occ, f.owner));
        assert!(Arc::ptr_eq(
            req.selected(),
            result.preparation.call_contract().selected_owner()
        ));
        let PreparedPureKernel::Table(kernel) = result.preparation.prepared() else {
            panic!("TableV1")
        };
        assert_eq!(
            kernel.contract().result_types(),
            &[ty(DataType::Struct(vec![child.clone()].into()), true)]
        );
        assert_eq!(
            result.frozen.effects.argument_control,
            ArgumentControl::Table
        );
        assert_eq!(
            result.frozen.effects.instance_state,
            FunctionInstanceState::TableInstance
        );
        assert_eq!(
            result.frozen.effects.own_row_error,
            FunctionIntrinsicRowError::NoRowError
        );
        assert_eq!(
            result.frozen.effects.null_behavior,
            FunctionNullBehavior::CalledOnNull
        );
        let NodeKind::TableFunction {
            outputs,
            left_outer,
            ..
        } = &f.source().kind
        else {
            unreachable!()
        };
        assert_eq!(*left_outer, left);
        assert_eq!(outputs.len(), 3);
        assert_eq!(outputs[0], outputs[1]);
        assert_eq!(kernel.contract().result_types().len(), 1);
    }
}
#[test]
fn table_occurrences_repeated_definition_keeps_independent_roots_and_operator_context() {
    let t = list_type();
    let f = Fixture::new(
        &[t.clone(), t],
        &[Source::Value, Source::Again(0)],
        false,
        false,
    );
    assert_eq!(f.args[0], f.args[1]);
    let occ = f.occurrences();
    let a = child(&f, 0);
    let b = child(&f, 1);
    assert_ne!(a, b);
    let flow = f.source().expressions.as_ref().unwrap().control_flow();
    assert_ne!(
        flow.uses()[&a].context.region,
        flow.uses()[&b].context.region
    );
    let ctx = context(&occ, f.owner);
    assert!(ctx.is_call());
    assert_eq!(
        novarocks_type_contract::CallEffectContext::from(ctx),
        novarocks_type_contract::CallEffectContext::Relation(ctx)
    );
    assert_ne!(
        novarocks_type_contract::CallEffectContext::from(ctx),
        flow.uses()[&a].context.into()
    );
    let req = request(f.source(), &f, &Control::default()).unwrap();
    let effects = summaries(&f);
    let params = SemanticParameters::try_new([]).unwrap();
    prefixes(
        |c| run(input(&f, &req, &occ, &effects, &params), &f.catalog, c),
        true,
        false,
    );
    let mut changed = copy_occurrences(&occ);
    changed.relational_contexts[0].1 = RelationEffectContext::materialized_input(ctx.identity.call);
    assert!(matches!(
        run(
            input(&f, &req, &changed, &effects, &params),
            &f.catalog,
            &Control::default()
        ),
        Err(PhysicalTableOccurrenceError::InvalidSource(_))
    ));
    let mut repeated = copy_occurrences(&occ);
    repeated
        .relational_contexts
        .push(repeated.relational_contexts[0]);
    prefixes(
        |c| run(input(&f, &req, &repeated, &effects, &params), &f.catalog, c),
        false,
        false,
    );
}
#[test]
fn table_occurrences_missing_or_foreign_child_scope_environment_and_proof_refuse_with_original_causes()
 {
    let f = Fixture::new(&[list_type()], &[Source::Value], false, false);
    let occ = f.occurrences();
    let req = request(f.source(), &f, &Control::default()).unwrap();
    let effects = summaries(&f);
    let params = SemanticParameters::try_new([]).unwrap();
    let id = child(&f, 0);
    let mut missing = summaries(&f);
    let mut missing_children = missing[&f.owner].summaries().clone();
    missing_children.remove(&id);
    missing.insert(
        f.owner,
        node_effects_with_test_summaries(
            f.source().expressions.as_ref().unwrap().clone(),
            missing_children,
        ),
    );
    assert!(
        matches!(run(input(&f,&req,&occ,&missing,&params),&f.catalog,&Control::default()),Err(PhysicalTableOccurrenceError::MissingChildEffects(actual)) if actual==id)
    );
    prefixes(
        |c| run(input(&f, &req, &occ, &missing, &params), &f.catalog, c),
        false,
        false,
    );
    let mut foreign = summaries(&f);
    let mut foreign_children = foreign[&f.owner].summaries().clone();
    let actual = f
        .source()
        .expressions
        .as_ref()
        .unwrap()
        .control_flow()
        .uses()[&id]
        .context;
    foreign_children.insert(
        id,
        ScopedExpressionEffects::pure_value(ExpressionEffectContext {
            demand: EvaluationDemand::TruthOnly,
            ..actual
        }),
    );
    foreign.insert(
        f.owner,
        node_effects_with_test_summaries(
            f.source().expressions.as_ref().unwrap().clone(),
            foreign_children,
        ),
    );
    assert!(matches!(
        run(
            input(&f, &req, &occ, &foreign, &params),
            &f.catalog,
            &Control::default()
        ),
        Err(PhysicalTableOccurrenceError::Effects(_))
    ));
    prefixes(
        |c| run(input(&f, &req, &occ, &foreign, &params), &f.catalog, c),
        false,
        false,
    );
    let node = f.source().clone();
    prefixes(
        |c| {
            let mut call = input(&f, &req, &occ, &effects, &params);
            call.source = &node;
            run(call, &f.catalog, c)
        },
        false,
        false,
    );
    let bad_ref = SemanticParameterRef {
        id: SemanticParameterId::new(7),
        expected_key: SemanticParameterKey::TimeZone,
    };
    let environment = [bad_ref];
    let env_params = SemanticParameters::try_new([(
        bad_ref.id,
        SemanticParameterValue::TimeZone(
            novarocks_type_contract::TimeZoneKey::parse("UTC").unwrap(),
        ),
    )])
    .unwrap();
    prefixes(
        |c| {
            let mut call = input(&f, &req, &occ, &effects, &env_params);
            call.environment = &environment;
            run(call, &f.catalog, c)
        },
        false,
        false,
    );
    prefixes(
        |c| {
            let mut call = input(&f, &req, &occ, &effects, &params);
            call.proof_scope = CallProofScope::Region(
                f.source()
                    .expressions
                    .as_ref()
                    .unwrap()
                    .control_flow()
                    .uses()[&id]
                    .context
                    .region,
            );
            run(call, &f.catalog, c)
        },
        false,
        false,
    );
}
#[test]
fn table_occurrences_complete_relation_mismatch_is_refused_by_actual_unnest_owner() {
    let f = Fixture::new(&[list_type()], &[Source::Value], false, true);
    let occ = f.occurrences();
    let req = request(f.source(), &f, &Control::default()).unwrap();
    let effects = summaries(&f);
    let params = SemanticParameters::try_new([]).unwrap();
    assert!(matches!(
        run(
            input(&f, &req, &occ, &effects, &params),
            &f.catalog,
            &Control::default()
        ),
        Err(PhysicalTableOccurrenceError::Function(_))
    ));
    prefixes(
        |c| run(input(&f, &req, &occ, &effects, &params), &f.catalog, c),
        false,
        false,
    );
}
#[test]
fn table_occurrences_genuine_case_arithmetic_child_error_is_not_erased_by_called_on_null_owner() {
    let f = Fixture::new(&[list_type()], &[Source::ChildError], false, false);
    let occ = f.occurrences();
    let params = SemanticParameters::try_new([(
        allow_ref().id,
        SemanticParameterValue::AllowThrowException(false),
    )])
    .unwrap();
    let scoped = summaries(&f);
    let id = child(&f, 0);
    assert!(
        scoped[&f.owner].summaries()[&id]
            .for_use(
                f.source()
                    .expressions
                    .as_ref()
                    .unwrap()
                    .control_flow()
                    .uses()[&id]
                    .context
            )
            .unwrap()
            .may_raise_row_error
    );
    let req = request(f.source(), &f, &Control::default()).unwrap();
    let result = run(
        input(&f, &req, &occ, &scoped, &params),
        &f.catalog,
        &Control::default(),
    )
    .unwrap();
    assert_eq!(
        result.frozen.effects.own_row_error,
        FunctionIntrinsicRowError::NoRowError
    );
    assert!(
        result
            .preparation
            .effects()
            .for_use(context(&occ, f.owner))
            .unwrap()
            .may_raise_row_error
    );
}
#[test]
fn table_occurrences_wide_source_fields_observe_actual_quantum_without_rebinding() {
    let fields: Vec<_> = (0..320)
        .map(|at| {
            Arc::new(
                Field::new(format!("source-{at}"), DataType::Int64, true).with_metadata(
                    std::collections::HashMap::from([("provider.id".into(), at.to_string())]),
                ),
            )
        })
        .collect();
    let t = ty(
        DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(fields.into()),
            true,
        ))),
        true,
    );
    let f = Fixture::new(&[t], &[Source::Value], false, false);
    let occ = f.occurrences();
    let req = request(f.source(), &f, &Control::default()).unwrap();
    let effects = summaries(&f);
    let params = SemanticParameters::try_new([]).unwrap();
    prefixes(
        |c| run(input(&f, &req, &occ, &effects, &params), &f.catalog, c),
        true,
        true,
    );
}

#[test]
fn table_occurrences_missing_installed_table_owner_never_falls_back_to_scalar() {
    let f = Fixture::new(&[list_type()], &[Source::Value], false, false);
    let occ = f.occurrences();
    let req = request(f.source(), &f, &Control::default()).unwrap();
    let effects = summaries(&f);
    let params = SemanticParameters::try_new([]).unwrap();
    let mut builder = novarocks_functions::EngineFunctionCatalogBuilder::new();
    builder
        .register(
            f.catalog
                .definition("abs", FunctionKind::Scalar)
                .unwrap()
                .clone(),
        )
        .unwrap();
    let scalar_only = builder.seal().unwrap();
    assert!(matches!(
        run(
            input(&f, &req, &occ, &effects, &params),
            &scalar_only,
            &Control::default()
        ),
        Err(PhysicalTableOccurrenceError::Function(_))
    ));
    prefixes(
        |c| run(input(&f, &req, &occ, &effects, &params), &scalar_only, c),
        false,
        false,
    );
}

// The complete composition component is tested through the original physical
// source/root fixture. Source/static/global admission remains a separate gate.
fn fragment_run(
    fixture: &Fixture,
    occurrences: &AuthoredPhysicalOccurrences,
    parameters: &SemanticParameters,
    scopes: &BTreeMap<
        PhysicalCallSite,
        super::super::physical_fragment_effects::PhysicalRelationalCallSourceScope<'_>,
    >,
    control: &Control,
) -> Result<
    super::super::physical_fragment_effects::AuthoredPhysicalFragmentEffects,
    super::super::physical_fragment_effects::PhysicalFragmentEffectsError,
> {
    super::super::physical_fragment_effects::author_physical_fragment_effects_observed(
        super::super::physical_fragment_effects::PhysicalFragmentEffectsInput {
            fragment: &fixture.fragment,
            occurrences,
            constants: &fixture.pools,
            parameters,
            literal_policy: policy(),
            node_effects: &summaries(fixture),
            relational_scopes: scopes,
        },
        &fixture.catalog,
        control,
    )
}
fn fragment_scopes<'a>(
    fixture: &'a Fixture,
    occurrences: &AuthoredPhysicalOccurrences,
) -> BTreeMap<
    PhysicalCallSite,
    super::super::physical_fragment_effects::PhysicalRelationalCallSourceScope<'a>,
> {
    BTreeMap::from([(
        PhysicalCallSite::Table {
            node: fixture.owner,
        },
        super::super::physical_fragment_effects::PhysicalRelationalCallSourceScope {
            source: fixture.source(),
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            environment: &[],
            proof_scope: CallProofScope::Relation(context(occurrences, fixture.owner).identity),
        },
    )])
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
    use super::super::physical_call_arguments::PhysicalArgumentError;
    use super::super::physical_fragment_effects::PhysicalFragmentEffectsError;
    use novarocks_functions::ConstantError;
    let baseline = Control::default();
    let outcome = invoke(&baseline);
    assert_eq!(outcome.is_ok(), success, "{outcome:?}");
    let trace = baseline.trace();
    assert_eq!(trace.first(), Some(&(PHASE, 0)));
    if success {
        assert!(trace.len() > 1);
        assert!(trace.iter().any(|(_, units)| *units > 0));
    } else {
        let original = outcome.unwrap_err();
        match &original {
            PhysicalFragmentEffectsError::MissingScope(_)
            | PhysicalFragmentEffectsError::InvalidSource(
                "relational source scopes contain an extra call site"
                | "relational scope does not loan this actual source call",
            ) => {
                // These exact small-fixture scope checks reject before the
                // first bounded flush; the ordinary error starts no footer.
                assert_eq!(trace, [(PHASE, 0)]);
            }
            PhysicalFragmentEffectsError::Control(_)
            | PhysicalFragmentEffectsError::TableRequest(PhysicalTableRequestError::Argument(
                PhysicalArgumentError::Constant(ConstantError::Limit(_)),
            )) => {}
            _ => panic!("unexpected original table composition refusal: {original:?}"),
        }
        for cause in CAUSES {
            let control = Control {
                trace: Mutex::default(),
                refusal: Some((trace.len(), cause)),
            };
            let actual = invoke(&control).unwrap_err();
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
                    PhysicalFragmentEffectsError::Control(actual),
                    PhysicalFragmentEffectsError::Control(original),
                ) => assert_eq!(actual, original),
                (
                    PhysicalFragmentEffectsError::TableRequest(
                        PhysicalTableRequestError::Argument(PhysicalArgumentError::Constant(
                            actual @ ConstantError::Limit(_),
                        )),
                    ),
                    PhysicalFragmentEffectsError::TableRequest(
                        PhysicalTableRequestError::Argument(PhysicalArgumentError::Constant(
                            original @ ConstantError::Limit(_),
                        )),
                    ),
                ) => assert_eq!(actual, original),
                _ => panic!(
                    "different original table composition refusal: {actual:?}; expected {original:?}"
                ),
            }
            assert_eq!(control.trace(), trace);
        }
    }
    let positions: Vec<_> = if wide {
        assert!(trace.iter().any(|(_, units)| *units == 256));
        (0..trace.len())
            .filter(|&at| at == 0 || at + 1 == trace.len() || trace[at].1 == 256)
            .collect()
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
                matches!(invoke(&control), Err(PhysicalFragmentEffectsError::Control(actual)) if actual==cause),
                "at {at}: {cause:?}"
            );
            assert_eq!(control.trace(), trace[..=at]);
        }
    }
}

#[test]
fn table_fragment_composer_keeps_real_child_error_and_publishes_exact_complete_call_coverage() {
    let fixture = Fixture::new(&[list_type()], &[Source::ChildError], true, false);
    let occurrences = fixture.occurrences();
    let parameters = SemanticParameters::try_new([(
        allow_ref().id,
        SemanticParameterValue::AllowThrowException(false),
    )])
    .unwrap();
    let scopes = fragment_scopes(&fixture, &occurrences);
    let result = fragment_run(
        &fixture,
        &occurrences,
        &parameters,
        &scopes,
        &Control::default(),
    )
    .unwrap();
    assert_eq!(result.calls.entries().len(), 1);
    let call = &result.calls.entries()[&PhysicalCallSite::Table {
        node: fixture.owner,
    }];
    assert_eq!(call.context, context(&occurrences, fixture.owner));
    assert_eq!(
        call.effects.instance_state,
        FunctionInstanceState::TableInstance
    );
    assert_eq!(
        call.effects.own_row_error,
        FunctionIntrinsicRowError::NoRowError
    );
    let child = child(&fixture, 0);
    assert!(
        fixture.effects[&fixture.owner].summaries()[&child]
            .for_use(
                fixture
                    .source()
                    .expressions
                    .as_ref()
                    .unwrap()
                    .control_flow()
                    .uses()[&child]
                    .context
            )
            .unwrap()
            .may_raise_row_error
    );
    result
        .calls
        .validate_fragment(&fixture.fragment, &Control::default())
        .unwrap();
    fragment_prefixes(
        |control| fragment_run(&fixture, &occurrences, &parameters, &scopes, control),
        true,
        false,
    );
}

#[test]
fn table_fragment_composer_missing_extra_and_equal_foreign_source_scopes_refuse_before_publication()
{
    use super::super::physical_fragment_effects::{
        PhysicalFragmentEffectsError, PhysicalRelationalCallSourceScope,
    };
    let fixture = Fixture::new(&[list_type()], &[Source::Value], false, false);
    let occurrences = fixture.occurrences();
    let parameters = SemanticParameters::try_new([]).unwrap();
    let site = PhysicalCallSite::Table {
        node: fixture.owner,
    };
    let missing = BTreeMap::new();
    assert!(
        matches!(fragment_run(&fixture, &occurrences, &parameters, &missing, &Control::default()), Err(PhysicalFragmentEffectsError::MissingScope(actual)) if actual==site)
    );
    fragment_prefixes(
        |control| fragment_run(&fixture, &occurrences, &parameters, &missing, control),
        false,
        false,
    );
    let mut extra = fragment_scopes(&fixture, &occurrences);
    extra.insert(
        PhysicalCallSite::Table {
            node: NodeId::new(7),
        },
        PhysicalRelationalCallSourceScope {
            source: fixture.source(),
            decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
            environment: &[],
            proof_scope: CallProofScope::Relation(context(&occurrences, fixture.owner).identity),
        },
    );
    assert!(matches!(
        fragment_run(
            &fixture,
            &occurrences,
            &parameters,
            &extra,
            &Control::default()
        ),
        Err(PhysicalFragmentEffectsError::InvalidSource(
            "relational source scopes contain an extra call site"
        ))
    ));
    fragment_prefixes(
        |control| fragment_run(&fixture, &occurrences, &parameters, &extra, control),
        false,
        false,
    );
    let foreign = fixture.source().clone();
    let mut scopes = fragment_scopes(&fixture, &occurrences);
    scopes.get_mut(&site).unwrap().source = &foreign;
    assert!(matches!(
        fragment_run(
            &fixture,
            &occurrences,
            &parameters,
            &scopes,
            &Control::default()
        ),
        Err(PhysicalFragmentEffectsError::InvalidSource(
            "relational scope does not loan this actual source call"
        ))
    ));
    fragment_prefixes(
        |control| fragment_run(&fixture, &occurrences, &parameters, &scopes, control),
        false,
        false,
    );
}

#[test]
fn table_fragment_composer_320_actual_argument_roots_observe_real_quantum_and_primary_causes() {
    let types = vec![list_type(); 320];
    let sources = vec![Source::Value; 320];
    let fixture = Fixture::new(&types, &sources, true, false);
    let occurrences = fixture.occurrences();
    let parameters = SemanticParameters::try_new([]).unwrap();
    let scopes = fragment_scopes(&fixture, &occurrences);
    fragment_prefixes(
        |control| fragment_run(&fixture, &occurrences, &parameters, &scopes, control),
        true,
        true,
    );
}
