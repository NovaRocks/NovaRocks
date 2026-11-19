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

use super::super::expression_component_test_fixture::{
    ComponentSealError, FixtureBuilder, UnitFixture,
};
use super::*;
use crate::test_constants as checked;
use arrow::datatypes::DataType;
use novarocks_expression_contract::{
    BoundFunction, ExprId, ExprKind, RootOrdinal, UnaryOperator, UnitError,
};
use novarocks_functions::{
    EngineFunctionCatalog, FunctionArgument, FunctionBindingError, FunctionKind,
    FunctionResultType, FunctionValueType,
    builtin::catalogue::build_builtin_engine_function_catalog,
};
use novarocks_physical_plan::{
    ConstantPools, FragmentId, FragmentSink, NodeId, NodeKind, PipelineDopDomain, PlanLimits,
    ValueOrigin, WindowBound, WindowCallSpec, WindowFrame, WindowFrameExclusion, WindowFrameUnits,
    WindowSpec,
};
use novarocks_type_contract::{
    ControlShape, EvaluationDemand, ExpressionControlFlowError, ExpressionInvocation,
    ExpressionUseId, FunctionArgumentEvaluation, GuardKind, RegionGuard,
};
use std::sync::Mutex;
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
fn run(
    builder: FixtureBuilder,
    functions: &novarocks_functions::PureEngineFunctionCatalog,
    control: &Control,
) -> Result<UnitFixture, ComponentSealError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
    let result = builder.seal_observed(functions, checked::policy(), &mut work);
    work.finish_result(result, ComponentSealError::Control)
}
fn prefixes(
    mut source: impl FnMut() -> FixtureBuilder,
    functions: &novarocks_functions::PureEngineFunctionCatalog,
    success: bool,
    wide: bool,
) -> Vec<(CompilePhase, u32)> {
    let baseline = Control::default();
    assert_eq!(run(source(), functions, &baseline).is_ok(), success);
    let trace = baseline.trace.lock().unwrap().clone();
    assert_eq!(trace.first(), Some(&(CompilePhase::Validate, 0)));
    assert!(
        trace.len() > 1,
        "ordinary exits must preserve their actual author trace"
    );
    if wide {
        assert!(trace.contains(&(CompilePhase::Validate, 256)));
    }
    for at in 0..trace.len() {
        for cause in CAUSES {
            let control = Control {
                trace: Mutex::default(),
                refusal: Some((at, cause.clone())),
            };
            assert!(
                matches!(run(source(), functions, &control), Err(ComponentSealError::Control(actual)) if actual==cause),
                "at {at}: {cause:?}"
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
    trace
}
fn ty(data_type: DataType, nullable: bool) -> FunctionValueType {
    FunctionValueType::new(data_type, nullable)
}
fn literal(
    builder: &mut FixtureBuilder,
    value_type: FunctionValueType,
    value: checked::Source,
) -> ExprId {
    let kind = checked::insert(&mut builder.constants, value(&value_type));
    builder.add(value_type, kind)
}
fn boolean(builder: &mut FixtureBuilder, value: bool) -> ExprId {
    literal(
        builder,
        ty(DataType::Boolean, false),
        checked::boolean(value),
    )
}
// These fields are actual typed roots on an unfinished physical Project node.
// The SDK remains the only author of the unit's expanded invocations.
fn project(builder: &mut FixtureBuilder, definitions: &[ExprId]) -> Vec<RootOrdinal> {
    let input = NodeId::new(7);
    builder
        .fragment
        .add_values(
            input,
            Box::from([Box::<[RootOrdinal]>::default()]),
            Box::default(),
        )
        .unwrap();
    let mut expressions = Vec::new();
    let mut outputs = Vec::new();
    let mut roots = Vec::new();
    for &definition in definitions {
        let root = builder.root(definition, EvaluationDemand::Value);
        let value_type = builder.definition(definition).ty.clone();
        let value = builder
            .fragment
            .add_value(
                value_type,
                ValueOrigin::Expr {
                    node: builder.node,
                    root,
                },
            )
            .unwrap();
        expressions.push((root, value));
        outputs.push(value);
        roots.push(root);
    }
    builder
        .fragment
        .add_project(
            builder.node,
            input,
            expressions.into_boxed_slice(),
            outputs.into_boxed_slice(),
        )
        .unwrap();
    roots
}
fn binding(
    catalog: &EngineFunctionCatalog,
    name: &str,
    types: &[FunctionValueType],
    kind: FunctionKind,
) -> BoundFunction {
    let arguments = types
        .iter()
        .map(|ty| FunctionArgument::Value {
            value_type: ty.clone(),
            constant: None,
        })
        .collect::<Vec<_>>();
    let control = Control::default();
    let resolved = match kind {
        FunctionKind::Scalar => {
            SqlFunctionCatalog::resolve_scalar_binding(catalog, name, &arguments, &control).unwrap()
        }
        FunctionKind::Window => {
            SqlFunctionCatalog::resolve_window_binding(catalog, name, &arguments, &control).unwrap()
        }
        _ => unreachable!(),
    };
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
fn invocation(uses: &UnitFixture, id: ExpressionUseId) -> &ExpressionInvocation<ExprId> {
    &uses.flow().uses()[&id]
}
fn child(
    uses: &UnitFixture,
    parent: ExpressionUseId,
    ordinal: usize,
) -> &ExpressionInvocation<ExprId> {
    invocation(uses, invocation(uses, parent).arguments[ordinal])
}
fn assert_guard(
    uses: &UnitFixture,
    parent: ExpressionUseId,
    ordinal: usize,
    expected: GuardKind,
    demand: EvaluationDemand,
) {
    let owner = invocation(uses, parent);
    let item = child(uses, parent, ordinal);
    assert_eq!(item.context.demand, demand);
    let domain = uses.flow().regions()[&item.context.region];
    assert_eq!(domain.parent, Some(owner.context.region));
    assert_eq!(
        domain.guard,
        Some(RegionGuard {
            owner: parent,
            kind: expected
        })
    );
}

#[test]
fn occurrence_shared_sparse_if_definition_keeps_value_truth_domains_and_ignores_legacy_control() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let source = || {
        let mut builder = FixtureBuilder::new();
        let a = boolean(&mut builder, true);
        let b = boolean(&mut builder, false);
        let mut function = binding(
            &catalog,
            "if",
            &std::array::from_fn::<_, 3, _>(|_| ty(DataType::Boolean, false)),
            FunctionKind::Scalar,
        );
        function
            .legacy_metadata
            .as_mut()
            .expect("actual legacy fixture")
            .argument_evaluation = FunctionArgumentEvaluation::Eager;
        let root = builder.add(
            function.result_type.clone(),
            ExprKind::FunctionCall {
                function,
                args: Box::from([a, b, a]),
            },
        );
        let predicate = builder.root(root, EvaluationDemand::TruthOnly);
        let assignment = builder.root(root, EvaluationDemand::Value);
        // Real operator fields name the two independently declared roots.
        let input = NodeId::new(7);
        builder
            .fragment
            .add_values(
                input,
                Box::from([Box::<[RootOrdinal]>::default()]),
                Box::default(),
            )
            .unwrap();
        let value = builder
            .fragment
            .add_value(
                ty(DataType::Boolean, false),
                ValueOrigin::NodeOutput {
                    node: builder.node,
                    output_ordinal: 0,
                },
            )
            .unwrap();
        let effect = builder
            .fragment
            .add_value(
                ty(DataType::Int8, false),
                ValueOrigin::NodeOutput {
                    node: builder.node,
                    output_ordinal: 1,
                },
            )
            .unwrap();
        builder
            .fragment
            .add_row_rewriting(
                builder.node,
                input,
                Some(&std::collections::BTreeMap::new()),
                Box::from([value, effect]),
                NodeKind::ChangeEventExpand {
                    events: Box::from([novarocks_physical_plan::ChangeEventSpec {
                        predicate: Some(predicate),
                        effect: novarocks_connector_contract::ConnectorRowMutationEffect::Insert,
                        assignments: Box::from([novarocks_physical_plan::ChangeAssignment::exact(
                            value,
                            Some(assignment),
                        )]),
                    }]),
                    effect_output: effect,
                },
            )
            .unwrap();
        (builder, root, predicate, assignment)
    };
    let (builder, root, predicate, assignment) = source();
    let uses = run(
        builder,
        &novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
        &Control::default(),
    )
    .unwrap();
    assert_eq!(uses.sealed.roots().len(), 2);
    let predicate = uses.root_use(predicate);
    let value_use = uses.root_use(assignment);
    assert_ne!(predicate, value_use);
    for (id, demand) in [
        (predicate, EvaluationDemand::TruthOnly),
        (value_use, EvaluationDemand::Value),
    ] {
        let use_ = invocation(&uses, id);
        assert_eq!(use_.definition, root);
        assert_eq!(use_.control, ControlShape::If);
        assert_eq!(use_.context.demand, demand);
        assert_eq!(child(&uses, id, 0).context.region, use_.context.region);
        assert_eq!(
            child(&uses, id, 0).context.demand,
            EvaluationDemand::TruthOnly
        );
        assert_guard(&uses, id, 1, GuardKind::IfThen, demand);
        assert_guard(&uses, id, 2, GuardKind::IfElse, demand);
        assert_ne!(
            child(&uses, id, 0).context.use_id,
            child(&uses, id, 2).context.use_id
        );
    }
    assert_ne!(
        invocation(&uses, predicate).context.region,
        invocation(&uses, value_use).context.region
    );
    prefixes(
        || source().0,
        &novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
        true,
        false,
    );
}
#[test]
fn occurrence_installed_coalesce_and_lower_have_exact_guarded_and_eager_edges() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let source = || {
        let mut b = FixtureBuilder::new();
        let text = ty(DataType::Utf8, true);
        let first = literal(&mut b, text.clone(), checked::null());
        let second = literal(&mut b, text.clone(), checked::utf8("Selected".into()));
        let lower = binding(
            &catalog,
            "lower",
            std::slice::from_ref(&text),
            FunctionKind::Scalar,
        );
        let lower_id = b.add(
            lower.result_type.clone(),
            ExprKind::FunctionCall {
                function: lower,
                args: Box::from([second]),
            },
        );
        let function = binding(
            &catalog,
            "coalesce",
            &[text.clone(), text],
            FunctionKind::Scalar,
        );
        let root = b.add(
            function.result_type.clone(),
            ExprKind::FunctionCall {
                function,
                args: Box::from([first, lower_id]),
            },
        );
        let ordinal = project(&mut b, &[root])[0];
        (b, ordinal)
    };
    let (builder, ordinal) = source();
    let uses = run(
        builder,
        &novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
        &Control::default(),
    )
    .unwrap();
    let root = uses.root_use(ordinal);
    assert_eq!(invocation(&uses, root).control, ControlShape::Coalesce);
    assert_eq!(
        child(&uses, root, 0).context.region,
        invocation(&uses, root).context.region
    );
    assert_guard(
        &uses,
        root,
        1,
        GuardKind::CoalesceAfterNull { ordinal: 1 },
        EvaluationDemand::Value,
    );
    let lower = child(&uses, root, 1);
    assert_eq!(lower.control, ControlShape::Eager);
    assert_eq!(
        child(&uses, lower.context.use_id, 0).context.region,
        lower.context.region
    );
    let trace = prefixes(
        || source().0,
        &novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
        true,
        false,
    );
    assert!(
        trace
            .iter()
            .any(|(phase, _)| *phase == CompilePhase::FunctionSpecialization)
    );
}
#[test]
fn occurrence_intrinsic_boolean_and_both_case_shapes_preserve_order_and_hand_guards() {
    let source = || {
        let mut b = FixtureBuilder::new();
        let a = boolean(&mut b, true);
        let false_ = boolean(&mut b, false);
        let not = b.add(
            ty(DataType::Boolean, false),
            ExprKind::Unary {
                op: UnaryOperator::Not,
                expr: a,
            },
        );
        let or = b.add(
            ty(DataType::Boolean, false),
            ExprKind::Disjunction {
                args: Box::from([a, not]),
            },
        );
        let and = b.add(
            ty(DataType::Boolean, false),
            ExprKind::Conjunction {
                args: Box::from([or, false_]),
            },
        );
        let mut definitions = Vec::new();
        for operand in [None, Some(a)] {
            definitions.push(b.add(
                ty(DataType::Boolean, false),
                ExprKind::Case {
                    operand,
                    when_then: Box::from([(and, a), (false_, not)]),
                    else_expr: Some(false_),
                },
            ));
        }
        let roots = project(&mut b, &definitions);
        (b, and, roots)
    };
    let (builder, and, roots) = source();
    let uses = run(
        builder,
        &novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
        &Control::default(),
    )
    .unwrap();
    for (position, ordinal) in roots.into_iter().enumerate() {
        let id = uses.root_use(ordinal);
        let simple = position == 1;
        let root = invocation(&uses, id);
        assert_eq!(
            root.control,
            ControlShape::Case {
                simple,
                arms: 2,
                has_else: true
            }
        );
        let first = usize::from(simple);
        if simple {
            assert_eq!(child(&uses, id, 0).context.region, root.context.region);
        }
        for arm in 0..2 {
            assert_guard(
                &uses,
                id,
                first + arm * 2,
                GuardKind::CaseWhen { arm: arm as u32 },
                if simple {
                    EvaluationDemand::Value
                } else {
                    EvaluationDemand::TruthOnly
                },
            );
            assert_guard(
                &uses,
                id,
                first + arm * 2 + 1,
                GuardKind::CaseThen { arm: arm as u32 },
                EvaluationDemand::Value,
            );
        }
        assert_guard(
            &uses,
            id,
            first + 4,
            GuardKind::CaseElse,
            EvaluationDemand::Value,
        );
        let conjunction = child(&uses, id, first);
        assert_eq!(conjunction.definition, and);
        assert_eq!(conjunction.control, ControlShape::Conjunction);
        let disjunction = child(&uses, conjunction.context.use_id, 0);
        assert_eq!(disjunction.control, ControlShape::Disjunction);
        assert_eq!(
            child(&uses, disjunction.context.use_id, 1).control,
            ControlShape::Eager
        );
        assert_eq!(
            child(&uses, conjunction.context.use_id, 1).context.region,
            conjunction.context.region
        );
    }
    prefixes(
        || source().0,
        &novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
        true,
        false,
    );
}
#[test]
fn occurrence_window_optional_frame_offsets_are_eager_topology_not_scalar_lifecycle() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    for with_frame in [false, true] {
        let mut builder = novarocks_physical_plan::FragmentBuilder::new(FragmentId::new(71));
        let input = NodeId::new(7);
        let owner = NodeId::new(901);
        builder
            .add_values(
                input,
                Box::from([Box::<[RootOrdinal]>::default()]),
                Box::default(),
            )
            .unwrap();
        let function = binding(&catalog, "rank", &[], FunctionKind::Window);
        let mut pools = ConstantPools::empty();
        let offset = with_frame.then(|| {
            let ExprKind::Constant(reference) =
                checked::insert(&mut pools, checked::i64(1)(&ty(DataType::Int64, false)))
            else {
                unreachable!()
            };
            reference
        });
        let frame = offset.map(|offset| WindowFrame {
            units: WindowFrameUnits::Rows,
            start: WindowBound::Preceding(offset),
            end: WindowBound::CurrentRow,
            exclusion: WindowFrameExclusion::NoOthers,
        });
        let output = builder
            .add_value(
                function.result_type.clone(),
                ValueOrigin::NodeOutput {
                    node: owner,
                    output_ordinal: 0,
                },
            )
            .unwrap();
        builder
            .add_row_widening(
                owner,
                input,
                Box::from([output]),
                NodeKind::Window(WindowSpec {
                    partition_by: Box::default(),
                    order_by: Box::default(),
                    calls: Box::from([WindowCallSpec {
                        binding: function.clone(),
                        distinct: false,
                        arguments: Box::default(),
                        order_by: Box::default(),
                        frame,
                        ignore_nulls: false,
                        aggregate_binding: None,
                        output,
                    }]),
                }),
            )
            .unwrap();
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
        assert!(
            fragment
                .nodes()
                .values()
                .all(|node| node.expressions.is_none())
        );
        let run =
            |control: &Control| author_physical_occurrences_observed(&fragment, &catalog, control);
        let authored = run(&Control::default()).unwrap();
        assert_eq!(authored.relational_contexts.len(), 1);
        let (site, context) = authored.relational_contexts[0];
        assert_eq!(
            site,
            novarocks_physical_plan::PhysicalCallSite::Window {
                node: owner,
                call: 0
            }
        );
        assert!(context.is_call());
        assert_eq!(context.demand, EvaluationDemand::Value);
        let NodeKind::Window(spec) = &fragment.nodes()[&owner].kind else {
            unreachable!()
        };
        assert!(spec.calls[0].arguments.is_empty());
        assert_eq!(spec.calls[0].frame, frame);
        if let Some(offset) = offset {
            let value = pools.entries()[&offset.pool].value(offset.ordinal).unwrap();
            assert_eq!(value.value_type(), &ty(DataType::Int64, false));
            assert_eq!(
                value
                    .int64_observed(CompilePhase::Validate, &Control::default())
                    .unwrap(),
                Some(1)
            );
        }
        let base = catalog
            .pure_overload_declaration_observed(
                &function.function_id,
                function.kind,
                &function.overload,
                &Control::default(),
            )
            .unwrap();
        assert_eq!(base.effects().argument_control, ArgumentControl::Window);
        let baseline = Control::default();
        run(&baseline).unwrap();
        let trace = baseline.trace.lock().unwrap().clone();
        assert_eq!(trace.first(), Some(&(CompilePhase::Validate, 0)));
        assert!(trace.len() > 1);
        for at in 0..trace.len() {
            for cause in CAUSES {
                let control = Control {
                    trace: Mutex::default(),
                    refusal: Some((at, cause.clone())),
                };
                assert!(
                    matches!(run(&control),Err(ExpressionOccurrenceError::Control(actual)) if actual==cause)
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
}
#[test]
fn occurrence_typeof_missing_owner_is_not_an_installed_type_only_capability() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    // TYPEOF is a legacy lowering declaration, excluded from the installed
    // Engine catalogue. Do not fabricate a physical installed owner for it.
    let control = Control::default();
    assert!(matches!(
        SqlFunctionCatalog::resolve_scalar_binding(&catalog, "typeof", &[], &control),
        Err(FunctionBindingError::UnknownFunction)
    ));
    let trace = control.trace.lock().unwrap().clone();
    assert_eq!(trace, [(CompilePhase::FunctionSpecialization, 0)]);
    // The missing original owner wins before an armed optional later check.
    for cause in CAUSES {
        let late = Control {
            trace: Mutex::default(),
            refusal: Some((trace.len(), cause)),
        };
        assert!(matches!(
            SqlFunctionCatalog::resolve_scalar_binding(&catalog, "typeof", &[], &late),
            Err(FunctionBindingError::UnknownFunction)
        ));
        assert_eq!(*late.trace.lock().unwrap(), trace);
    }
    for at in 0..trace.len() {
        for cause in CAUSES {
            let control = Control {
                trace: Mutex::default(),
                refusal: Some((at, cause.clone())),
            };
            assert!(
                matches!(SqlFunctionCatalog::resolve_scalar_binding(&catalog, "typeof", &[], &control),
                Err(FunctionBindingError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
    assert_eq!(
        ExprKind::Lambda {
            parameter_types: Box::default(),
            body: ExprId::new(0)
        }
        .intrinsic_control_shape()
        .unwrap(),
        Some(ControlShape::LambdaBody)
    );
}

#[test]
fn occurrence_metadata_only_refusal_and_nested_typed_controls_never_fallback_or_replay() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let source = || {
        let mut b = FixtureBuilder::new();
        let arg = literal(&mut b, ty(DataType::Utf8, false), checked::utf8("Ä".into()));
        let function = binding(
            &catalog,
            "lower",
            &[ty(DataType::Utf8, false)],
            FunctionKind::Scalar,
        );
        let root = b.add(
            function.result_type.clone(),
            ExprKind::FunctionCall {
                function,
                args: Box::from([arg]),
            },
        );
        project(&mut b, &[root]);
        b
    };
    let subset = novarocks_functions::testkit::rng_subset();
    assert!(matches!(
        run(source(), &subset, &Control::default()),
        Err(ComponentSealError::Author(
            novarocks_expression::CallAuthorError::Capability(_)
        ))
    ));
    prefixes(source, &subset, false, false);
    for cause in CAUSES {
        assert!(
            matches!(ExpressionOccurrenceError::function(FunctionSpecializationFailure::Binding(FunctionBindingError::Control(cause.clone()))),ExpressionOccurrenceError::Control(actual) if actual==cause)
        );
        let kernel = novarocks_functions::KernelFailure::CompileControl(cause.clone());
        assert!(
            matches!(ExpressionOccurrenceError::function(FunctionSpecializationFailure::Kernel(kernel)),ExpressionOccurrenceError::Control(actual) if actual==cause)
        );
    }
}
#[test]
fn occurrence_real_320_values_rows_expand_shared_definition_and_observe_actual_quantums() {
    let source = || {
        let mut b = FixtureBuilder::new();
        let root = boolean(&mut b, true);
        let mut rows = Vec::new();
        for _ in 0..320 {
            rows.push(Box::from([b.root(root, EvaluationDemand::Value)]));
        }
        let value = b
            .fragment
            .add_value(
                ty(DataType::Boolean, false),
                ValueOrigin::NodeOutput {
                    node: b.node,
                    output_ordinal: 0,
                },
            )
            .unwrap();
        b.fragment
            .add_values(b.node, rows.into_boxed_slice(), Box::from([value]))
            .unwrap();
        let NodeKind::Values { rows, .. } = &b.fragment.construction_node(b.node).unwrap().kind
        else {
            unreachable!()
        };
        assert_eq!(rows.len(), 320);
        for row in rows {
            assert_eq!(row.len(), 1);
            assert_eq!(
                b.fragment.construction_unit(b.node).unwrap().roots()[row[0].get() as usize]
                    .definition,
                root
            );
        }
        (b, root)
    };
    let (builder, root) = source();
    let uses = run(
        builder,
        &novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
        &Control::default(),
    )
    .unwrap();
    assert_eq!(uses.sealed.roots().len(), 320);
    assert_eq!(uses.flow().uses().len(), 320);
    assert_eq!(uses.flow().regions().len(), 320);
    for (ordinal, declaration) in uses.sealed.roots().iter().enumerate() {
        let use_ = invocation(
            &uses,
            uses.root_use(RootOrdinal::new(u32::try_from(ordinal).unwrap())),
        );
        assert_eq!(declaration.definition, root);
        assert_eq!(use_.definition, root);
        assert_eq!(use_.context.demand, EvaluationDemand::Value);
        assert!(uses.flow().regions()[&use_.context.region].parent.is_none());
    }
    prefixes(
        || source().0,
        &novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
        true,
        true,
    );
}
#[test]
fn occurrence_shared_and_dag_reference_limit_counts_expanded_uses_before_publication() {
    fn source(levels: usize) -> FixtureBuilder {
        let mut b = FixtureBuilder::new();
        let mut root = boolean(&mut b, true);
        for _ in 0..levels {
            root = b.add(
                ty(DataType::Boolean, false),
                ExprKind::Conjunction {
                    args: Box::from([root, root]),
                },
            );
        }
        project(&mut b, &[root]);
        b
    }
    assert_eq!(MAX_CONTROL_USE_REFERENCES, 65_536);
    let near = source(14);
    assert_eq!(near.definitions().len(), 15);
    let near_control = Control::default();
    let near_uses = run(
        near,
        &novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
        &near_control,
    )
    .unwrap();
    assert_eq!(near_uses.sealed.roots().len(), 1);
    assert_eq!(near_uses.flow().uses().len(), 32_767);
    assert_eq!(near_uses.flow().regions().len(), 1);
    let edges: usize = near_uses
        .flow()
        .uses()
        .values()
        .map(|u| u.arguments.len())
        .sum();
    assert_eq!(edges, 32_766);
    assert_eq!(near_uses.flow().uses().len() + edges, 65_533);
    // Successful expansion reaches the SDK's original completed-flow
    // preflight: one real step per captured use before the map operations.
    // The over-limit branch below refuses before entering that stage.
    let near_trace = near_control.trace.lock().unwrap().clone();
    assert_eq!(near_trace.first(), Some(&(CompilePhase::Validate, 0)));
    let quantum = near_trace
        .iter()
        .position(|item| *item == (CompilePhase::Validate, 256))
        .expect("the original completed-flow preflight reaches a real quantum");
    assert!(
        near_trace
            .iter()
            .all(|(phase, units)| *phase == CompilePhase::Validate && *units <= 256)
    );
    let observed: u64 = near_trace.iter().map(|(_, units)| u64::from(*units)).sum();
    assert!(observed >= u64::try_from(near_uses.flow().use_reference_count()).unwrap());
    for at in [0, quantum, near_trace.len() - 1] {
        for cause in CAUSES {
            let control = Control {
                trace: Mutex::default(),
                refusal: Some((at, cause.clone())),
            };
            assert!(
                matches!(run(source(14), &novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(), &control), Err(ComponentSealError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), near_trace[..=at]);
        }
    }
    let over = source(15);
    assert_eq!(over.definitions().len(), 16);
    let control = Control::default();
    assert!(matches!(
        run(
            over,
            &novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
            &control
        ),
        Err(ComponentSealError::Unit(UnitError::Flow(
            ExpressionControlFlowError::TooManyItems
        )))
    ));
    let trace = control.trace.lock().unwrap();
    assert_eq!(trace.first(), Some(&(CompilePhase::Validate, 0)));
    assert!(
        trace
            .iter()
            .all(|(phase, units)| *phase == CompilePhase::Validate && *units <= 256)
    );
    // Every admitted reference contributes a real step. On the ordinary
    // refusal at most the original checkpoint budget can remain unobserved.
    let observed: u64 = trace.iter().map(|(_, units)| u64::from(*units)).sum();
    let minimum =
        MAX_CONTROL_USE_REFERENCES - novarocks_type_contract::MAX_UNOBSERVED_COMPILE_WORK as usize;
    assert!(observed >= u64::try_from(minimum).unwrap());
    assert!(trace.len() > 2);
    // The sole SDK expansion author refuses before producing SealedUnit.
}
