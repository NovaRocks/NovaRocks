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
use arrow::datatypes::DataType;
use novarocks_functions::{
    EngineFunctionCatalog, FunctionArgument, FunctionBindingError, FunctionKind,
    FunctionResolutionError, FunctionResultType, FunctionValueType, ResolvedAggregateSignature,
    builtin::catalogue::build_builtin_engine_function_catalog,
};
use novarocks_physical_plan::{
    BoundFunction, ChangeEventSpec, ExprNode, ExpressionRootRole, FragmentBuilder, FragmentId,
    FragmentSink, LiteralValue, NodeId, NodeKind, PipelineDopDomain, PlanLimits, UnaryOperator,
    ValueOrigin, WindowBound, WindowExpression, WindowFrame, WindowFrameExclusion,
    WindowFrameUnits, WindowSpec,
};
use novarocks_type_contract::{EvaluationDemand, FunctionArgumentEvaluation, GuardKind};
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
fn run(
    fragment: &Fragment,
    catalog: &dyn SqlFunctionCatalog,
    control: &Control,
) -> Result<PhysicalRootUses, ExpressionOccurrenceError> {
    author_expression_occurrences_observed(fragment, catalog, control)
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
    for at in 0..trace.len() {
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
fn ty(data_type: DataType, nullable: bool) -> FunctionValueType {
    FunctionValueType::new(data_type, nullable)
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
fn boolean(builder: &mut FragmentBuilder, owner: NodeId, value: bool) -> ExprId {
    builder
        .add_expression(
            owner,
            ty(DataType::Boolean, false),
            ExprKind::Literal(LiteralValue::Boolean(value)),
        )
        .unwrap()
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
        legacy_metadata: Some(novarocks_physical_plan::LegacyBindingMetadata {
            volatility: resolved.semantics.volatility,
            argument_evaluation: resolved.semantics.argument_evaluation,
            failure_behavior: resolved.semantics.failure_behavior,
            intrinsic_row_error: resolved.semantics.intrinsic_row_error,
            semantic_parameters: Box::default(),
        }),
    }
}
fn project(builder: &mut FragmentBuilder, owner: NodeId, input: NodeId, roots: &[ExprId]) {
    let mut expressions = Vec::new();
    let mut output = Vec::new();
    for expr in roots {
        let ty = builder.expressions().get(*expr).unwrap().ty.clone();
        let value = builder
            .add_value(
                ty,
                ValueOrigin::Expr {
                    node: owner,
                    expr: *expr,
                },
            )
            .unwrap();
        expressions.push((*expr, value));
        output.push(value);
    }
    builder
        .add_project(
            owner,
            input,
            expressions.into_boxed_slice(),
            output.into_boxed_slice(),
        )
        .unwrap();
}
fn invocation(uses: &PhysicalRootUses, id: ExpressionUseId) -> &ExpressionInvocation<ExprId> {
    &uses.flow().uses()[&id]
}
fn child(
    uses: &PhysicalRootUses,
    parent: ExpressionUseId,
    ordinal: usize,
) -> &ExpressionInvocation<ExprId> {
    invocation(uses, invocation(uses, parent).arguments[ordinal])
}
fn assert_guard(
    uses: &PhysicalRootUses,
    parent: ExpressionUseId,
    ordinal: usize,
    expected: GuardKind,
    demand: EvaluationDemand,
) {
    let owner = invocation(uses, parent);
    let item = child(uses, parent, ordinal);
    assert_eq!(item.context.demand, demand);
    let domain = uses.flow().domains()[&item.context.domain];
    assert_eq!(domain.parent, Some(owner.context.domain));
    assert_eq!(
        domain.guard,
        Some(DomainGuard {
            owner: parent,
            kind: expected
        })
    );
}

#[test]
fn occurrence_shared_sparse_if_definition_keeps_value_truth_domains_and_ignores_legacy_control() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let (mut builder, input) = empty();
    let owner = NodeId::new(901);
    let a = boolean(&mut builder, owner, true);
    let b = boolean(&mut builder, owner, false);
    let mut function = binding(
        &catalog,
        "if",
        &std::array::from_fn::<_, 3, _>(|_| ty(DataType::Boolean, false)),
        FunctionKind::Scalar,
    );
    // Deliberately stale legacy metadata cannot select the new control protocol.
    function
        .legacy_metadata
        .as_mut()
        .expect("actual legacy fixture")
        .argument_evaluation = FunctionArgumentEvaluation::Eager;
    let root = ExprId::new(u32::MAX - 1);
    builder
        .insert_expression(ExprNode {
            id: root,
            owner,
            lambda_scope: None,
            ty: function.result_type.clone(),
            kind: ExprKind::FunctionCall {
                function,
                args: Box::from([a, b, a]),
            },
        })
        .unwrap();
    let value = builder
        .add_value(
            ty(DataType::Boolean, false),
            ValueOrigin::NodeOutput {
                node: owner,
                output_ordinal: 0,
            },
        )
        .unwrap();
    let effect = builder
        .add_value(
            ty(DataType::Int8, false),
            ValueOrigin::NodeOutput {
                node: owner,
                output_ordinal: 1,
            },
        )
        .unwrap();
    let passthrough = std::collections::BTreeMap::new();
    builder
        .add_row_rewriting(
            owner,
            input,
            Some(&passthrough),
            Box::from([value, effect]),
            NodeKind::ChangeEventExpand {
                events: Box::from([ChangeEventSpec {
                    predicate: Some(root),
                    effect: novarocks_connector_contract::ConnectorRowMutationEffect::Insert,
                    assignments: Box::from([novarocks_physical_plan::ChangeAssignment::exact(
                        value,
                        Some(root),
                    )]),
                }]),
                effect_output: effect,
            },
        )
        .unwrap();
    let fragment = finish(builder, owner);
    let uses = run(&fragment, &catalog, &Control::default()).unwrap();
    assert_eq!(uses.bindings().len(), 2);
    let predicate = uses.bindings()[&novarocks_physical_plan::ExpressionRootSite {
        node: owner,
        role: ExpressionRootRole::ChangePredicate { event: 0 },
    }];
    let value_use = uses.bindings()[&novarocks_physical_plan::ExpressionRootSite {
        node: owner,
        role: ExpressionRootRole::ChangeAssignment {
            event: 0,
            assignment: 0,
        },
    }];
    assert_ne!(predicate, value_use);
    for (id, demand) in [
        (predicate, EvaluationDemand::TruthOnly),
        (value_use, EvaluationDemand::Value),
    ] {
        let use_ = invocation(&uses, id);
        assert_eq!(use_.definition, root);
        assert_eq!(use_.control, ControlShape::If);
        assert_eq!(use_.context.demand, demand);
        assert_eq!(child(&uses, id, 0).context.domain, use_.context.domain);
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
        invocation(&uses, predicate).context.domain,
        invocation(&uses, value_use).context.domain
    );
    prefixes(&fragment, &catalog, true, false);
}

#[test]
fn occurrence_installed_coalesce_and_lower_have_exact_guarded_and_eager_edges() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let (mut builder, input) = empty();
    let owner = NodeId::new(901);
    let text = ty(DataType::Utf8, true);
    let first = builder
        .add_expression(owner, text.clone(), ExprKind::Literal(LiteralValue::Null))
        .unwrap();
    let second = builder
        .add_expression(
            owner,
            text.clone(),
            ExprKind::Literal(LiteralValue::Utf8("Selected".into())),
        )
        .unwrap();
    let lower = binding(
        &catalog,
        "lower",
        std::slice::from_ref(&text),
        FunctionKind::Scalar,
    );
    let lower_id = builder
        .add_expression(
            owner,
            lower.result_type.clone(),
            ExprKind::FunctionCall {
                function: lower,
                args: Box::from([second]),
            },
        )
        .unwrap();
    let coalesce = binding(
        &catalog,
        "coalesce",
        &[text.clone(), text],
        FunctionKind::Scalar,
    );
    let root = builder
        .add_expression(
            owner,
            coalesce.result_type.clone(),
            ExprKind::FunctionCall {
                function: coalesce,
                args: Box::from([first, lower_id]),
            },
        )
        .unwrap();
    project(&mut builder, owner, input, &[root]);
    let fragment = finish(builder, owner);
    let uses = run(&fragment, &catalog, &Control::default()).unwrap();
    let root = *uses.bindings().values().next().unwrap();
    assert_eq!(invocation(&uses, root).control, ControlShape::Coalesce);
    assert_eq!(
        child(&uses, root, 0).context.domain,
        invocation(&uses, root).context.domain
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
        child(&uses, lower.context.use_id, 0).context.domain,
        lower.context.domain
    );
    let trace = prefixes(&fragment, &catalog, true, false);
    assert!(
        trace
            .iter()
            .any(|(phase, _)| *phase == CompilePhase::FunctionSpecialization)
    );
}

#[test]
fn occurrence_intrinsic_boolean_and_both_case_shapes_preserve_order_and_hand_guards() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let (mut builder, input) = empty();
    let owner = NodeId::new(901);
    let a = boolean(&mut builder, owner, true);
    let b = boolean(&mut builder, owner, false);
    let not = builder
        .add_expression(
            owner,
            ty(DataType::Boolean, false),
            ExprKind::Unary {
                op: UnaryOperator::Not,
                expr: a,
            },
        )
        .unwrap();
    let or = builder
        .add_expression(
            owner,
            ty(DataType::Boolean, false),
            ExprKind::Disjunction {
                args: Box::from([a, not]),
            },
        )
        .unwrap();
    let and = builder
        .add_expression(
            owner,
            ty(DataType::Boolean, false),
            ExprKind::Conjunction {
                args: Box::from([or, b]),
            },
        )
        .unwrap();
    let mut roots = Vec::new();
    for operand in [None, Some(a)] {
        roots.push(
            builder
                .add_expression(
                    owner,
                    ty(DataType::Boolean, false),
                    ExprKind::Case {
                        operand,
                        when_then: Box::from([(and, a), (b, not)]),
                        else_expr: Some(b),
                    },
                )
                .unwrap(),
        );
    }
    project(&mut builder, owner, input, &roots);
    let fragment = finish(builder, owner);
    let uses = run(&fragment, &catalog, &Control::default()).unwrap();
    for (position, id) in uses.bindings().values().copied().enumerate() {
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
            assert_eq!(child(&uses, id, 0).context.domain, root.context.domain);
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
            child(&uses, conjunction.context.use_id, 1).context.domain,
            conjunction.context.domain
        );
    }
    prefixes(&fragment, &catalog, true, false);
}

#[test]
fn occurrence_window_optional_frame_offsets_are_eager_topology_not_scalar_lifecycle() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    for with_frame in [false, true] {
        let (mut builder, input) = empty();
        let owner = NodeId::new(901);
        let function = binding(&catalog, "rank", &[], FunctionKind::Window);
        let offset = with_frame.then(|| {
            builder
                .add_expression(
                    owner,
                    ty(DataType::Int64, false),
                    ExprKind::Literal(LiteralValue::Int64(1)),
                )
                .unwrap()
        });
        let frame = offset.map(|offset| WindowFrame {
            units: WindowFrameUnits::Rows,
            start: WindowBound::Preceding(offset),
            end: WindowBound::CurrentRow,
            exclusion: WindowFrameExclusion::NoOthers,
        });
        let root = builder
            .add_expression(
                owner,
                function.result_type.clone(),
                ExprKind::WindowCall {
                    function: function.clone(),
                    distinct: false,
                    args: Box::default(),
                    function_order_by: Box::default(),
                    frame,
                    ignore_nulls: false,
                    aggregate_binding: None,
                },
            )
            .unwrap();
        let output = builder
            .add_value(
                function.result_type.clone(),
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
                Box::from([output]),
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
        let fragment = finish(builder, owner);
        let uses = run(&fragment, &catalog, &Control::default()).unwrap();
        let id = *uses.bindings().values().next().unwrap();
        assert_eq!(invocation(&uses, id).control, ControlShape::Eager);
        assert_eq!(
            invocation(&uses, id).arguments.len(),
            usize::from(with_frame)
        );
        if let Some(offset) = offset {
            assert_eq!(child(&uses, id, 0).definition, offset);
            assert_eq!(child(&uses, id, 0).context.demand, EvaluationDemand::Value);
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
        prefixes(&fragment, &catalog, true, false);
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
    // These assert closed protocol projection only, not an installed owner.
    assert_eq!(
        scalar_shape(ArgumentControl::TypeOnly, 2),
        Some(ControlShape::TypeOnly)
    );
    assert_eq!(
        scalar_shape(
            ArgumentControl::HigherOrder {
                body_ordinal: 1,
                body_demand: EvaluationDemand::TruthOnly
            },
            2
        ),
        Some(ControlShape::HigherOrder {
            body_ordinal: 1,
            body_demand: EvaluationDemand::TruthOnly
        })
    );
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
fn occurrence_metadata_only_refusal_and_nested_typed_controls_never_fallback_or_replay() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let (mut builder, input) = empty();
    let owner = NodeId::new(901);
    let arg = builder
        .add_expression(
            owner,
            ty(DataType::Utf8, false),
            ExprKind::Literal(LiteralValue::Utf8("Ä".into())),
        )
        .unwrap();
    let function = binding(
        &catalog,
        "lower",
        &[ty(DataType::Utf8, false)],
        FunctionKind::Scalar,
    );
    let root = builder
        .add_expression(
            owner,
            function.result_type.clone(),
            ExprKind::FunctionCall {
                function,
                args: Box::from([arg]),
            },
        )
        .unwrap();
    project(&mut builder, owner, input, &[root]);
    let fragment = finish(builder, owner);
    assert!(matches!(
        run(&fragment, &MetadataOnly, &Control::default()),
        Err(ExpressionOccurrenceError::Function(
            FunctionSpecializationFailure::InvalidInput(
                "SQL function snapshot has no installed pure overload declaration owner"
            )
        ))
    ));
    prefixes(&fragment, &MetadataOnly, false, false);
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
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let mut builder = FragmentBuilder::new(FragmentId::new(71));
    let owner = NodeId::new(901);
    let root = boolean(&mut builder, owner, true);
    let value = builder
        .add_value(
            ty(DataType::Boolean, false),
            ValueOrigin::NodeOutput {
                node: owner,
                output_ordinal: 0,
            },
        )
        .unwrap();
    builder
        .add_values(
            owner,
            (0..320)
                .map(|_| Box::from([root]))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            Box::from([value]),
        )
        .unwrap();
    let fragment = finish(builder, owner);
    let uses = run(&fragment, &catalog, &Control::default()).unwrap();
    assert_eq!(uses.bindings().len(), 320);
    assert_eq!(uses.flow().uses().len(), 320);
    assert_eq!(uses.flow().domains().len(), 320);
    for (site, id) in uses.bindings() {
        assert!(matches!(
            site.role,
            ExpressionRootRole::ValuesCell { column: 0, .. }
        ));
        let use_ = invocation(&uses, *id);
        assert_eq!(use_.definition, root);
        assert_eq!(use_.context.demand, EvaluationDemand::Value);
        assert!(uses.flow().domains()[&use_.context.domain].parent.is_none());
    }
    prefixes(&fragment, &catalog, true, true);
}

#[test]
fn occurrence_shared_and_dag_reference_limit_counts_expanded_uses_before_publication() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    // At depth d, the independently expanded binary tree has 2^(d+1)-1
    // uses and one fewer edges. Shared definitions do not erase occurrences.
    fn source(levels: usize) -> Fragment {
        let (mut builder, input) = empty();
        let owner = NodeId::new(901);
        let mut root = boolean(&mut builder, owner, true);
        for _ in 0..levels {
            root = builder
                .add_expression(
                    owner,
                    ty(DataType::Boolean, false),
                    ExprKind::Conjunction {
                        args: Box::from([root, root]),
                    },
                )
                .unwrap();
        }
        project(&mut builder, owner, input, &[root]);
        finish(builder, owner)
    }
    assert_eq!(MAX_CONTROL_USE_REFERENCES, 65_536);
    let near = source(14);
    assert_eq!(near.expressions().len(), 15);
    let near_control = Control::default();
    let near_uses = run(&near, &catalog, &near_control).unwrap();
    assert_eq!(near_uses.bindings().len(), 1);
    assert_eq!(near_uses.flow().uses().len(), 32_767);
    assert_eq!(near_uses.flow().domains().len(), 1);
    let edges: usize = near_uses
        .flow()
        .uses()
        .values()
        .map(|u| u.arguments.len())
        .sum();
    assert_eq!(edges, 32_766);
    assert_eq!(near_uses.flow().uses().len() + edges, 65_533);
    let over = source(15);
    assert_eq!(over.expressions().len(), 16);
    let control = Control::default();
    assert!(matches!(
        run(&over, &catalog, &control),
        Err(ExpressionOccurrenceError::TooManyItems)
    ));
    let trace = control.trace.lock().unwrap();
    assert_eq!(trace.first(), Some(&(CompilePhase::Validate, 0)));
    assert!(trace.contains(&(CompilePhase::Validate, 256)));
    assert!(trace.len() > 2);
    // The sole author rejects before returning a PhysicalRootUses owner; this
    // case proves the actual expansion cap, not a synthetic charge counter.
}
