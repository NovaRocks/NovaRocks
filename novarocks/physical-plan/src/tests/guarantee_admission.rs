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

use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};

use novarocks_type_contract::{
    ArgumentControl, CallEffects, CallProofScope, CompileControlError, CompilePhase, ControlShape,
    DecimalOverflowPolicy, EvaluationDemand, EvaluationRegionId, ExpressionControlFlow,
    ExpressionEffectContext, ExpressionEvaluationRegion, ExpressionInvocation, ExpressionUseId,
    FunctionInstanceState, FunctionNullBehavior, ObservableEffects, PureCompileControl,
};

use super::*;

const MISSING_PROOF: &str = "predicate guarantee has no exact function proof";

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}

// These ceilings admit only the small, fully authored fixtures below. They
// are an explicit source invoice, not a deployable default or a MEM grant.
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

#[derive(Clone, Copy)]
enum Predicate {
    Primitive,
    Function(FunctionVolatility),
    IntrinsicDescendant,
}

fn function(volatility: FunctionVolatility) -> BoundFunction {
    BoundFunction {
        function_id: FunctionId::try_new("fixture/guarantee/boolean").unwrap(),
        overload: FunctionOverloadId::try_new("fixture/guarantee/boolean/zero").unwrap(),
        kind: FunctionKind::Scalar,
        argument_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(Box::default()),
        result_type: ty(DataType::Boolean, false),
        legacy_metadata: Some(crate::LegacyBindingMetadata {
            volatility,
            argument_evaluation: FunctionArgumentEvaluation::Eager,
            failure_behavior: FunctionFailureBehavior::Propagate,
            intrinsic_row_error: FunctionIntrinsicRowError::NoRowError,
            semantic_parameters: Box::default(),
        }),
    }
}

// This explicit fixture owner declares a zero-argument, non-NULL Boolean
// runtime call. Its occurrence facts are not a provider guarantee proof and
// do not authenticate an installed production implementation.
fn occurrence_effects(volatility: FunctionVolatility) -> CallEffects {
    CallEffects {
        value_stability: volatility,
        own_row_error: FunctionIntrinsicRowError::NoRowError,
        failure_behavior: FunctionFailureBehavior::Propagate,
        null_behavior: FunctionNullBehavior::CalledOnNull,
        argument_control: ArgumentControl::Eager,
        instance_state: FunctionInstanceState::None,
        observable_effects: ObservableEffects::NONE,
        environment: Box::default(),
        proof_scope: CallProofScope::Unconditional,
    }
}

fn add_invocation(
    fragment: &Fragment,
    expression: ExprId,
    demand: EvaluationDemand,
    invocations: &mut Vec<ExpressionInvocation<ExprId>>,
) -> ExpressionUseId {
    let id = ExpressionUseId::new(u32::try_from(invocations.len()).unwrap());
    let definition = fragment.expressions().get(expression).unwrap();
    invocations.push(ExpressionInvocation {
        context: ExpressionEffectContext {
            use_id: id,
            region: EvaluationRegionId::new(0),
            demand,
        },
        definition: expression,
        control: ControlShape::Eager,
        arguments: Box::default(),
    });
    let mut arguments = Vec::new();
    definition
        .kind
        .expression_references_observed::<std::convert::Infallible>(|child| {
            arguments.push(add_invocation(
                fragment,
                child,
                EvaluationDemand::Value,
                invocations,
            ));
            Ok(())
        })
        .unwrap();
    invocations[id.get() as usize].arguments = arguments.into_boxed_slice();
    id
}

fn fixture_uses_and_calls(fragment: &Fragment) -> (PhysicalRootUses, FrozenFragmentCalls) {
    let roots = PhysicalExpressionRoots::try_new(fragment, &Control).unwrap();
    let mut invocations = Vec::new();
    let mut bindings = Vec::new();
    for (site, root) in roots.sites() {
        bindings.push((
            *site,
            add_invocation(fragment, root.expr, root.demand, &mut invocations),
        ));
    }
    let flow = ExpressionControlFlow::try_new(
        vec![ExpressionEvaluationRegion {
            id: EvaluationRegionId::new(0),
            parent: None,
            guard: None,
        }],
        invocations,
        fragment.expressions(),
        CompilePhase::Validate,
        &Control,
    )
    .unwrap();
    let uses = PhysicalRootUses::try_new(fragment, flow, bindings, &Control).unwrap();
    let calls = uses
        .flow()
        .uses()
        .iter()
        .filter_map(|(id, invocation)| {
            let ExprKind::FunctionCall { function, .. } = &fragment
                .expressions()
                .get(invocation.definition)
                .unwrap()
                .kind
            else {
                return None;
            };
            assert_eq!(function.function_id.as_str(), "fixture/guarantee/boolean");
            Some(FrozenPhysicalCall {
                materialized_writer_input: None,
                regexp_count_pattern_source: None,
                to_base64_byte_source: None,
                temporal_source: None,
                site: PhysicalCallSite::Expression(*id),
                context: invocation.context,
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                effects: occurrence_effects(function.legacy_metadata.as_ref().unwrap().volatility),
            })
        })
        .collect();
    let calls = FrozenFragmentCalls::try_new(fragment, &uses, calls, &Control).unwrap();
    (uses, calls)
}

fn fixture(
    predicate: Predicate,
    guarantee: Option<PredicateGuaranteeKind>,
    residual: bool,
    derived: bool,
) -> FragmentPackageInput {
    fixture_with_guarantee_count(predicate, guarantee, residual, derived, 1, false)
}

fn fixture_with_guarantee_count(
    predicate: Predicate,
    guarantee: Option<PredicateGuaranteeKind>,
    residual: bool,
    derived: bool,
    guarantee_count: usize,
    sparse_last: bool,
) -> FragmentPackageInput {
    let binding = connector_binding();
    let column = ProviderColumnReference {
        column_payload: encoded(&binding, ConnectorCodecCategory::ReadColumn, 33),
    };
    let mut relation = metadata_relation(&binding, column.clone());
    let mut builder = FragmentBuilder::new(FragmentId::new(92));
    let scan = builder.reserve_node_id().unwrap();
    let source = builder
        .add_value(
            relation.schema()[0].ty.clone(),
            ValueOrigin::ProviderField {
                scan_node: scan,
                field: column.clone(),
            },
        )
        .unwrap();
    let predicate = match predicate {
        Predicate::Primitive => {
            let value = builder
                .add_expression(scan, ty(DataType::Int64, false), ExprKind::Value(source))
                .unwrap();
            builder
                .add_expression(
                    scan,
                    ty(DataType::Boolean, false),
                    ExprKind::IsNull {
                        expr: value,
                        negated: false,
                    },
                )
                .unwrap()
        }
        Predicate::Function(volatility) => builder
            .add_expression(
                scan,
                ty(DataType::Boolean, false),
                ExprKind::FunctionCall {
                    function: function(volatility),
                    args: Box::default(),
                },
            )
            .unwrap(),
        Predicate::IntrinsicDescendant => {
            let call = builder
                .add_expression(
                    scan,
                    ty(DataType::Boolean, false),
                    ExprKind::FunctionCall {
                        function: function(FunctionVolatility::Immutable),
                        args: Box::default(),
                    },
                )
                .unwrap();
            let negation = builder
                .add_expression(
                    scan,
                    ty(DataType::Boolean, false),
                    ExprKind::Unary {
                        op: UnaryOperator::Not,
                        expr: call,
                    },
                )
                .unwrap();
            builder
                .add_expression(
                    scan,
                    ty(DataType::Boolean, false),
                    ExprKind::IsNull {
                        expr: negation,
                        negated: true,
                    },
                )
                .unwrap()
        }
    };
    if let Some(kind) = guarantee {
        let mut guarantees = vec![PredicateGuarantee { predicate, kind }];
        // Distinct guarantee roots share the same primitive subtree. The
        // source has no duplicate guarantee and no fabricated runtime use.
        for _ in 1..guarantee_count {
            let expression = builder
                .add_expression(
                    scan,
                    ty(DataType::Boolean, false),
                    ExprKind::IsTruthValue {
                        expr: predicate,
                        value: true,
                        negated: false,
                    },
                )
                .unwrap();
            guarantees.push(PredicateGuarantee {
                predicate: expression,
                kind,
            });
        }
        let Relation::Metadata(metadata) = &mut relation else {
            unreachable!()
        };
        metadata.predicate_guarantees = guarantees.into_boxed_slice();
    }
    let mut columns = vec![source];
    let derived_values = if derived {
        let value = builder
            .add_value(
                ty(DataType::Boolean, false),
                ValueOrigin::Expr {
                    node: scan,
                    expr: predicate,
                },
            )
            .unwrap();
        columns.push(value);
        Box::from([value])
    } else {
        Box::default()
    };
    builder
        .insert_node_unchecked(PhysicalNode {
            id: scan,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: unconstrained(),
            output: OutputPort {
                node: scan,
                columns: columns.into_boxed_slice(),
            },
            kind: NodeKind::Scan {
                occurrence: ProviderReadOccurrenceId::new(0),
                relation: Box::new(relation),
                read_budget: scan_budget(),
                provider_outputs: Box::from([(column, source)]),
                residuals: if residual {
                    Box::from([predicate])
                } else {
                    Box::default()
                },
                derived_values,
            },
        })
        .unwrap();
    let mut fragment = builder
        .finish_structure(
            scan,
            FragmentSink::Noop,
            dop(),
            PlanLimits::FROZEN,
            &Control,
            crate::StagePlacementPolicy::Singleton,
        )
        .expect("the real construction stage must accept the source structure");
    if sparse_last {
        // Renumber a leaf root via the sole sparse arena ownership API. No
        // type, child, source owner, or guarantee occurrence is replaced.
        let mut parts = fragment.into_parts();
        let NodeKind::Scan { relation, .. } = &mut parts.nodes.get_mut(&scan).unwrap().kind else {
            unreachable!()
        };
        let Relation::Metadata(metadata) = relation.as_mut() else {
            unreachable!()
        };
        let last = metadata.predicate_guarantees.last_mut().unwrap();
        let old = last.predicate;
        last.predicate = ExprId::new(u32::MAX);
        let definitions = parts
            .expressions
            .iter()
            .map(|(_, definition)| {
                let mut definition = definition.clone();
                if definition.id == old {
                    definition.id = ExprId::new(u32::MAX);
                }
                definition
            })
            .collect::<Vec<_>>();
        parts.expressions = ExprArena::try_from_definitions_observed(
            definitions.into_iter(),
            &PlanLimits::FROZEN,
            &Control,
        )
        .unwrap();
        fragment = Fragment::from(parts);
    }
    // Each FunctionCall definition publishes its own original request: Value
    // arguments of its exact signature, none of which is a constant.
    let requests = fragment
        .expressions()
        .iter()
        .filter_map(|(id, definition)| match &definition.kind {
            ExprKind::FunctionCall { function, .. } => Some((
                crate::PhysicalCallDefinition::Expression(*id),
                crate::PhysicalCallRequest {
                    arguments: function
                        .argument_types
                        .iter()
                        .map(|argument| match argument {
                            novarocks_type_contract::FunctionArgumentType::Value(value_type) => {
                                crate::StaticFunctionArgument::Value {
                                    value_type: value_type.clone(),
                                    constant: None,
                                }
                            }
                            _ => panic!("guarantee fixture calls take Value arguments"),
                        })
                        .collect(),
                    logical_argument_count: function.argument_types.len(),
                    expected_result_type: None,
                    constant_policy: crate::ConstantPolicy {
                        max_rows: 16,
                        max_array_nodes: 32,
                        max_logical_elements: 128,
                        max_retained_buffer_bytes: 64 * 1024,
                        max_type_depth: 16,
                        max_type_nodes: 128,
                        max_dictionary_depth: 8,
                        max_metadata_bytes: 4096,
                        max_library_validation_work: 1_000_000,
                        max_library_validation_bytes: 1_000_000,
                    },
                },
            )),
            _ => None,
        })
        .collect::<Vec<_>>();
    let fragment = fragment
        .with_call_requests_observed(requests, &Control)
        .unwrap();
    let read = package_contract::frozen_scan(&fragment);
    let (uses, calls) = fixture_uses_and_calls(&fragment);
    let mut input = package_contract::package_input_with_controls(fragment, uses, calls);
    input.scans.insert(scan, read);
    input
}

#[test]
fn guarantee_shared_sparse_dag_preserves_exact_combined_projection_ceilings() {
    let input = fixture_with_guarantee_count(
        Predicate::Primitive,
        Some(PredicateGuaranteeKind::Exact),
        false,
        false,
        320,
        true,
    );
    assert!(
        input
            .fragment
            .expressions()
            .get(ExprId::new(u32::MAX))
            .is_some()
    );
    assert!(input.expression_uses.flow().uses().is_empty());
    assert!(input.calls.entries().is_empty());
    let admitted = admission();
    let facts = validate_fragment_output_properties_observed(
        &input.fragment,
        &input.expression_uses,
        &input.calls,
        admitted.plan_limits,
        admitted.source_retained_bytes,
        admitted.property_projection_limits,
        &Control,
    )
    .unwrap();
    let exact = FragmentPackageAdmission {
        property_projection_limits: PropertyProofProjectionLimits {
            max_request_bytes: facts.request_bytes,
            max_coexisting_bytes: facts.coexisting_bytes,
            max_projection_work: facts.projection_work,
        },
        ..admitted
    };
    let baseline = Trace::new(None, CompileControlError::Cancelled);
    FragmentPackage::try_new(input.clone(), exact, &baseline).unwrap();
    let expected = baseline.events.into_inner().unwrap();
    let quantum = expected
        .iter()
        .position(|(_, units)| *units == 256)
        .expect("the actual shared-DAG walk must reach a completed-work quantum");
    for dimension in 0..3 {
        let mut under = exact;
        match dimension {
            0 => under.property_projection_limits.max_request_bytes -= 1,
            1 => under.property_projection_limits.max_coexisting_bytes -= 1,
            _ => under.property_projection_limits.max_projection_work -= 1,
        }
        let error = FragmentPackage::try_new(input.clone(), under, &Control).unwrap_err();
        let FragmentPackageError::Structure(errors) = error else {
            panic!("combined guarantee projection must reject its own under-bound: {error:?}");
        };
        assert!(errors.errors().iter().any(|error| {
            error.category() == ValidationErrorCategory::ResourceLimit
                && error.path() == "fragment.guarantees.resources"
        }));
    }
    for failure in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in [0, quantum, expected.len() - 1] {
            let control = Trace::new(Some(at), failure.clone());
            assert_eq!(
                FragmentPackage::try_new(input.clone(), exact, &control).unwrap_err(),
                FragmentPackageError::Control(failure.clone())
            );
            assert_eq!(*control.events.lock().unwrap(), expected[..=at]);
        }
    }
}

fn assert_missing_proof(input: FragmentPackageInput) {
    let scan = input.fragment.root();
    let error = FragmentPackage::try_new(input, admission(), &Control).unwrap_err();
    let FragmentPackageError::Structure(errors) = error else {
        panic!("expected the exact structural guarantee rejection, got {error:?}");
    };
    assert!(errors.errors().iter().any(|error| {
        error.message() == MISSING_PROOF
            && error.path().contains(&format!("nodes[{}]", scan.get()))
            && error.category() == ValidationErrorCategory::StructuralInvariant
    }));
}

#[test]
fn guarantee_only_function_refuses_missing_proof_independent_of_legacy_stability() {
    for volatility in [FunctionVolatility::Immutable, FunctionVolatility::Volatile] {
        let input = fixture(
            Predicate::Function(volatility),
            Some(PredicateGuaranteeKind::Exact),
            false,
            false,
        );
        assert!(input.expression_uses.flow().uses().is_empty());
        assert!(input.calls.entries().is_empty());
        assert_missing_proof(input);
    }
}

#[test]
fn guarantee_intrinsic_descendants_and_actual_row_calls_cannot_supply_own_proof() {
    assert_missing_proof(fixture(
        Predicate::IntrinsicDescendant,
        Some(PredicateGuaranteeKind::Exact),
        false,
        false,
    ));
    let derived = fixture(
        Predicate::Function(FunctionVolatility::Immutable),
        Some(PredicateGuaranteeKind::Exact),
        false,
        true,
    );
    assert_eq!(derived.calls.entries().len(), 1);
    assert!(
        derived
            .expression_uses
            .roots()
            .sites()
            .keys()
            .any(|site| { matches!(site.role, ExpressionRootRole::ScanDerived { derived: 0 }) })
    );
    assert_missing_proof(derived);
    let pruning = fixture(
        Predicate::Function(FunctionVolatility::Immutable),
        Some(PredicateGuaranteeKind::PruningOnly),
        true,
        false,
    );
    assert_eq!(pruning.calls.entries().len(), 1);
    assert_missing_proof(pruning);
}

#[test]
fn primitive_guarantees_and_ordinary_function_residuals_keep_actual_package_capability() {
    for (kind, residual) in [
        (PredicateGuaranteeKind::Exact, false),
        (PredicateGuaranteeKind::Exact, true),
        (PredicateGuaranteeKind::PruningOnly, true),
    ] {
        let input = fixture(Predicate::Primitive, Some(kind), residual, false);
        assert!(input.calls.entries().is_empty());
        FragmentPackage::try_new(input, admission(), &Control).unwrap();
    }
    for volatility in [FunctionVolatility::Immutable, FunctionVolatility::Volatile] {
        let input = fixture(Predicate::Function(volatility), None, true, false);
        assert_eq!(input.calls.entries().len(), 1);
        let package = FragmentPackage::try_new(input, admission(), &Control).unwrap();
        assert!(matches!(
            package.calls().entries().values().next().unwrap().effects.value_stability,
            stability if stability == volatility
        ));
    }
}

struct Trace {
    events: Mutex<Vec<(CompilePhase, u32)>>,
    fail_at: Option<usize>,
    failure: CompileControlError,
    refused: AtomicBool,
}
impl Trace {
    fn new(fail_at: Option<usize>, failure: CompileControlError) -> Self {
        Self {
            events: Mutex::new(Vec::new()),
            fail_at,
            failure,
            refused: AtomicBool::new(false),
        }
    }
}
impl PureCompileControl for Trace {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(
            !self.refused.load(Ordering::SeqCst),
            "a primary refusal must not be checked again"
        );
        let mut events = self.events.lock().unwrap();
        let at = events.len();
        events.push((phase, units));
        if self.fail_at == Some(at) {
            self.refused.store(true, Ordering::SeqCst);
            Err(self.failure.clone())
        } else {
            Ok(())
        }
    }
}

#[test]
fn guarantee_admission_original_control_prefix_includes_success_and_ordinary_failure_tails() {
    for input in [
        fixture(
            Predicate::Primitive,
            Some(PredicateGuaranteeKind::Exact),
            false,
            false,
        ),
        fixture(
            Predicate::Function(FunctionVolatility::Immutable),
            None,
            true,
            false,
        ),
        fixture(
            Predicate::Function(FunctionVolatility::Immutable),
            Some(PredicateGuaranteeKind::Exact),
            false,
            false,
        ),
        fixture(
            Predicate::IntrinsicDescendant,
            Some(PredicateGuaranteeKind::Exact),
            false,
            false,
        ),
        fixture(
            Predicate::Function(FunctionVolatility::Immutable),
            Some(PredicateGuaranteeKind::Exact),
            false,
            true,
        ),
        fixture(
            Predicate::Function(FunctionVolatility::Immutable),
            Some(PredicateGuaranteeKind::PruningOnly),
            true,
            false,
        ),
    ] {
        let trace = Trace::new(None, CompileControlError::Cancelled);
        let ordinary = FragmentPackage::try_new(input.clone(), admission(), &trace);
        if let Err(error) = &ordinary {
            assert!(matches!(error, FragmentPackageError::Structure(_)));
            assert!(error.to_string().contains(MISSING_PROOF));
        }
        let expected = trace.events.into_inner().unwrap();
        assert!(!expected.is_empty());
        assert!(expected.iter().any(|(_, units)| *units > 0));
        // The final actual callback is tested too: refusal supersedes the
        // ordinary rejection, while a primary control cause gets no tail.
        for failure in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for at in 0..expected.len() {
                let control = Trace::new(Some(at), failure.clone());
                assert_eq!(
                    FragmentPackage::try_new(input.clone(), admission(), &control).unwrap_err(),
                    FragmentPackageError::Control(failure.clone())
                );
                assert_eq!(*control.events.lock().unwrap(), expected[..=at]);
                assert!(control.refused.load(Ordering::SeqCst));
            }
        }
    }
}
