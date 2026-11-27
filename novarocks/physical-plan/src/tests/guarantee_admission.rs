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

#[cfg(test)]
use crate::tests::unit_fixture::DefinitionFixture;
use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};

use novarocks_type_contract::{
    ArgumentControl, CallEffects, CallProofScope, CompileCheckpoints, CompileControlError,
    CompilePhase, ControlShape, DecimalOverflowPolicy, EvaluationDemand, EvaluationRegionId,
    ExpressionControlFlow, ExpressionDefinitionMembership, ExpressionUseId, FunctionInstanceState,
    FunctionNullBehavior, ObservableEffects, PureCompileControl,
};

use super::*;
use novarocks_expression_contract::{
    BoundExprSet, DefinitionControl, ExpressionCallClaim, PhysicalCallRequest, RootScope,
    UnitLimits,
};
use std::{collections::BTreeMap, sync::Arc};

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
    PrimitiveWithIndependentVolatileResidual,
    Function(FunctionVolatility),
    IntrinsicDescendant(FunctionVolatility),
    ClosedComparison {
        wrong_overload: bool,
        column_argument: bool,
    },
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

fn define_call(
    builder: &mut FragmentBuilder,
    scan: NodeId,
    volatility: FunctionVolatility,
) -> ExprId {
    builder
        .define(
            scan,
            ty(DataType::Boolean, false),
            ExprKind::FunctionCall {
                function: function(volatility),
                args: Box::default(),
            },
        )
        .unwrap()
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

// This fixture author declares the exact zero-argument call facts before
// seal. The resulting same-use claims are real expression facts, never a
// dedicated provider guarantee proof or an installed implementation receipt.
fn install_unit(builder: &mut FragmentBuilder, node: NodeId) {
    let ports = builder.node_ports(node).unwrap();
    let mut draft = builder.take_unit_builder(node).unwrap();
    let functions = draft
        .definitions()
        .filter_map(|definition| {
            if let ExprKind::FunctionCall { args, .. } = &definition.kind {
                let arguments = args
                    .iter()
                    .map(|argument| {
                        let source = draft.definition(*argument).unwrap();
                        crate::StaticFunctionArgument::Value {
                            value_type: source.ty.clone(),
                            constant: match source.kind {
                                ExprKind::Constant(reference) => Some(reference),
                                _ => None,
                            },
                        }
                    })
                    .collect::<Vec<_>>()
                    .into_boxed_slice();
                Some((definition.id, arguments))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    for (id, arguments) in functions {
        draft
            .request(
                id,
                PhysicalCallRequest {
                    logical_argument_count: arguments.len(),
                    arguments,
                    expected_result_type: None,
                },
            )
            .unwrap();
        draft
            .definition_control(
                id,
                DefinitionControl {
                    shape: ControlShape::Eager,
                    temporal_sources: None,
                },
            )
            .unwrap();
    }
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Validate).unwrap();
    let sealed = draft.seal(ports, &mut work).unwrap();
    let claims = sealed
        .uses()
        .iter()
        .filter_map(|(id, invocation)| {
            let ExprKind::FunctionCall { function, .. } =
                &sealed.definition(invocation.definition).unwrap().kind
            else {
                return None;
            };
            let mut effects =
                occurrence_effects(function.legacy_metadata.as_ref().unwrap().volatility);
            if novarocks_type_contract::OperatorFunction::from_function_id(&function.function_id)
                .is_some()
            {
                effects.null_behavior = FunctionNullBehavior::Strict;
                effects.proof_scope = CallProofScope::Region(invocation.context.region);
            }
            Some((
                *id,
                ExpressionCallClaim {
                    context: invocation.context,
                    effects,
                    decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                    temporal_source: None,
                },
            ))
        })
        .collect();
    let unit = sealed.finish(claims, &mut work).unwrap();
    work.finish().unwrap();
    builder.install_unit(node, Arc::new(unit)).unwrap();
}
struct Membership(usize);
impl ExpressionDefinitionMembership<ExprId> for Membership {
    fn definition_count(&self) -> usize {
        self.0
    }
    fn contains_definition(&self, id: ExprId) -> bool {
        (id.get() as usize) < self.0
    }
}
// Definitions are dense. Use/region IDs are opaque; exercise sparse keys at
// the neutral checked constructor rather than restoring a global sparse arena.
fn sparsify_unit(fragment: Fragment, node: NodeId) -> Fragment {
    let mut fragment_parts = fragment.into_parts();
    let source = fragment_parts
        .nodes
        .get_mut(&node)
        .unwrap()
        .expressions
        .take()
        .unwrap();
    let mut parts = Arc::try_unwrap(source)
        .expect("fixture uniquely owns its installed unit")
        .into_parts();
    assert!(parts.claims.is_empty());
    let map = parts
        .control
        .uses()
        .keys()
        .enumerate()
        .map(|(ordinal, old)| {
            (
                *old,
                ExpressionUseId::new(u32::MAX - u32::try_from(ordinal).unwrap()),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let regions_map = parts
        .control
        .regions()
        .keys()
        .enumerate()
        .map(|(ordinal, old)| {
            (
                *old,
                EvaluationRegionId::new(u32::MAX - u32::try_from(ordinal).unwrap()),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let regions = parts
        .control
        .regions()
        .values()
        .map(|region| {
            let mut region = *region;
            region.id = regions_map[&region.id];
            region.parent = region.parent.map(|id| regions_map[&id]);
            if let Some(guard) = &mut region.guard {
                guard.owner = map[&guard.owner];
            }
            region
        })
        .collect();
    let uses = parts
        .control
        .uses()
        .values()
        .map(|invocation| {
            let mut invocation = invocation.clone();
            invocation.context.use_id = map[&invocation.context.use_id];
            invocation.context.region = regions_map[&invocation.context.region];
            invocation.arguments = invocation.arguments.iter().map(|id| map[id]).collect();
            invocation
        })
        .collect();
    parts.root_uses = parts.root_uses.iter().map(|id| map[id]).collect();
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Validate).unwrap();
    parts.control = ExpressionControlFlow::try_new_in(
        regions,
        uses,
        &Membership(parts.definitions.len()),
        &mut |_| Ok(()),
        &mut work,
    )
    .unwrap();
    let unit = BoundExprSet::try_from_parts_in(
        parts,
        UnitLimits {
            expression_semantic_depth: PlanLimits::FROZEN.expression_semantic_depth,
        },
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    fragment_parts.nodes.get_mut(&node).unwrap().expressions = Some(Arc::new(unit));
    Fragment::from(fragment_parts)
}
fn unit(input: &FragmentPackageInput) -> &BoundExprSet {
    input.fragment.unit(input.fragment.root()).unwrap()
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
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
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
    let predicate_definition = match predicate {
        Predicate::Primitive | Predicate::PrimitiveWithIndependentVolatileResidual => {
            let value = builder
                .define(scan, ty(DataType::Int64, false), ExprKind::Value(source))
                .unwrap();
            builder
                .define(
                    scan,
                    ty(DataType::Boolean, false),
                    ExprKind::IsNull {
                        expr: value,
                        negated: false,
                    },
                )
                .unwrap()
        }
        Predicate::Function(volatility) => define_call(&mut builder, scan, volatility),
        Predicate::ClosedComparison {
            wrong_overload,
            column_argument,
        } => {
            use novarocks_type_contract::{FunctionArgumentType, OperatorFunction, TypedFactSlice};
            let operand = ty(DataType::Int64, false);
            let result = ty(DataType::Boolean, false);
            let left = builder
                .define(scan, operand.clone(), ExprKind::Value(source))
                .unwrap();
            let right = if column_argument {
                left
            } else {
                builder
                    .define(scan, operand.clone(), fixture_constants.i64(7, &operand))
                    .unwrap()
            };
            let operator = OperatorFunction::Equal;
            let overload = if wrong_overload {
                "fixture/guarantee/other-comparison"
            } else {
                operator.comparison_overload_text().unwrap()
            };
            let function = BoundFunction {
                function_id: operator.function_id(),
                overload: FunctionOverloadId::try_new(overload).unwrap(),
                kind: FunctionKind::Scalar,
                argument_types: TypedFactSlice::from_pure_boxed(Box::new([
                    FunctionArgumentType::Value(operand.clone()),
                    FunctionArgumentType::Value(operand),
                ])),
                result_type: result.clone(),
                legacy_metadata: function(FunctionVolatility::Immutable).legacy_metadata,
            };
            builder
                .define(
                    scan,
                    result,
                    ExprKind::FunctionCall {
                        function,
                        args: Box::new([left, right]),
                    },
                )
                .unwrap()
        }
        Predicate::IntrinsicDescendant(volatility) => {
            let call = define_call(&mut builder, scan, volatility);
            let negation = builder
                .define(
                    scan,
                    ty(DataType::Boolean, false),
                    ExprKind::Unary {
                        op: UnaryOperator::Not,
                        expr: call,
                    },
                )
                .unwrap();
            builder
                .define(
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
    let predicate_root = (guarantee.is_some() || residual).then(|| {
        builder.root_definition(
            scan,
            predicate_definition,
            EvaluationDemand::TruthOnly,
            RootScope::Port(0),
        )
    });
    if let Some(kind) = guarantee {
        let predicate = predicate_root.unwrap();
        let mut guarantees = vec![PredicateGuarantee { predicate, kind }];
        // Distinct guarantee roots share the same primitive subtree. The
        // source has no duplicate guarantee and no fabricated runtime use.
        for _ in 1..guarantee_count {
            let expression = builder
                .define(
                    scan,
                    ty(DataType::Boolean, false),
                    ExprKind::IsTruthValue {
                        expr: predicate_definition,
                        value: true,
                        negated: false,
                    },
                )
                .unwrap();
            let expression = builder.root_definition(
                scan,
                expression,
                EvaluationDemand::TruthOnly,
                RootScope::Port(0),
            );
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
    let mut residuals = Vec::new();
    if residual {
        residuals.push(predicate_root.unwrap());
    }
    if matches!(
        predicate,
        Predicate::PrimitiveWithIndependentVolatileResidual
    ) {
        let call = define_call(&mut builder, scan, FunctionVolatility::Volatile);
        residuals.push(builder.root_definition(
            scan,
            call,
            EvaluationDemand::TruthOnly,
            RootScope::Port(0),
        ));
    }
    let mut columns = vec![source];
    let derived_values = if derived {
        let derived_root = builder.root_definition(
            scan,
            predicate_definition,
            EvaluationDemand::Value,
            RootScope::Port(0),
        );
        let value = builder
            .add_value(
                ty(DataType::Boolean, false),
                ValueOrigin::Expr {
                    node: scan,
                    root: derived_root,
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
            expressions: None,
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
                provider_outputs: Box::from([(column, source)]),
                residuals: residuals.into_boxed_slice(),
                derived_values,
            },
        })
        .unwrap();
    install_unit(&mut builder, scan);
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
        fragment = sparsify_unit(fragment, scan);
    }
    // Scalar claims are in the installed unit. This fixture has no relational
    // call and therefore publishes a genuinely empty fragment call table.
    let fragment = fragment.with_calls_observed(Vec::new(), &Control).unwrap();
    let read = package_contract::frozen_scan(&fragment);
    let mut input = FragmentPackageInput {
        constants: fixture_constants.for_fragment(&fragment),

        version: version(),
        required: RequiredContracts::default(),
        pruning: FrozenFragmentPruning::try_new(fragment.id(), Vec::new(), &Control).unwrap(),
        fragment,
        cuts: FragmentCuts::default(),
        result: None,
        parameters: novarocks_type_contract::SemanticParameters::default(),
        scans: BTreeMap::new(),
        writes: BTreeMap::new(),
        layouts: Default::default(),
        annotations: Box::default(),
    };
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
    let source = unit(&input);
    assert!(
        source
            .definitions()
            .iter()
            .enumerate()
            .all(|(ordinal, definition)| definition.id.get() as usize == ordinal)
    );
    assert!(
        source
            .control_flow()
            .uses()
            .contains_key(&ExpressionUseId::new(u32::MAX))
    );
    assert!(
        source
            .control_flow()
            .regions()
            .contains_key(&EvaluationRegionId::new(u32::MAX))
    );
    assert_eq!(source.roots().len(), 320);
    assert!(source.claims().is_empty());
    assert!(input.fragment.calls().entries().is_empty());
    let admitted = admission();
    let facts = validate_fragment_output_properties_observed(
        &input.fragment,
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
        assert_eq!(unit(&input).claims().len(), 1);
        assert!(input.fragment.calls().entries().is_empty());
        assert_missing_proof(input);
    }
}

#[test]
fn guarantee_intrinsic_descendants_and_actual_row_calls_cannot_supply_own_proof() {
    assert_missing_proof(fixture(
        Predicate::IntrinsicDescendant(FunctionVolatility::Immutable),
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
    let source = unit(&derived);
    assert_eq!(source.claims().len(), 2);
    let NodeKind::Scan {
        derived_values,
        relation,
        ..
    } = &derived.fragment.nodes()[&derived.fragment.root()].kind
    else {
        unreachable!()
    };
    let ValueOrigin::Expr { node, root } = &derived.fragment.values()[&derived_values[0]].origin
    else {
        unreachable!()
    };
    assert_eq!(*node, derived.fragment.root());
    assert_eq!(source.root(*root).unwrap().demand, EvaluationDemand::Value);
    let guaranteed = relation.predicate_guarantees()[0].predicate;
    assert_eq!(
        source.root(guaranteed).unwrap().demand,
        EvaluationDemand::TruthOnly
    );
    assert_eq!(
        source.root(*root).unwrap().definition,
        source.root(guaranteed).unwrap().definition
    );
    assert_ne!(source.root_use(*root), source.root_use(guaranteed));
    assert_missing_proof(derived);
    let pruning = fixture(
        Predicate::Function(FunctionVolatility::Immutable),
        Some(PredicateGuaranteeKind::PruningOnly),
        true,
        false,
    );
    assert_eq!(unit(&pruning).claims().len(), 1);
    assert_missing_proof(pruning);
}

#[test]
fn accepts_only_the_exact_basic_provider_call_with_its_original_column_and_constant() {
    for (kind, residual) in [
        (PredicateGuaranteeKind::Exact, false),
        (PredicateGuaranteeKind::PruningOnly, true),
    ] {
        FragmentPackage::try_new(
            fixture(
                Predicate::ClosedComparison {
                    wrong_overload: false,
                    column_argument: false,
                },
                Some(kind),
                residual,
                false,
            ),
            admission(),
            &Control,
        )
        .unwrap();

        for (wrong_overload, column_argument) in [(true, false), (false, true)] {
            assert_missing_proof(fixture(
                Predicate::ClosedComparison {
                    wrong_overload,
                    column_argument,
                },
                Some(kind),
                residual,
                false,
            ));
        }
    }
}

#[test]
fn primitive_guarantees_and_ordinary_function_residuals_keep_actual_package_capability() {
    for (kind, residual) in [
        (PredicateGuaranteeKind::Exact, false),
        (PredicateGuaranteeKind::Exact, true),
        (PredicateGuaranteeKind::PruningOnly, true),
    ] {
        let input = fixture(Predicate::Primitive, Some(kind), residual, false);
        assert!(unit(&input).claims().is_empty());
        FragmentPackage::try_new(input, admission(), &Control).unwrap();
    }
    for volatility in [FunctionVolatility::Immutable, FunctionVolatility::Volatile] {
        let input = fixture(Predicate::Function(volatility), None, true, false);
        assert_eq!(unit(&input).claims().len(), 1);
        let package = FragmentPackage::try_new(input, admission(), &Control).unwrap();
        assert!(matches!(
            package.fragment().unit(package.fragment().root()).unwrap()
                .claims().values().next().unwrap().effects.value_stability,
            stability if stability == volatility
        ));
    }
}

#[test]
fn scan_guarantee_checks_its_own_root_with_independent_volatile_residual() {
    let accepted = fixture(
        Predicate::PrimitiveWithIndependentVolatileResidual,
        Some(PredicateGuaranteeKind::PruningOnly),
        true,
        false,
    );
    let scan = accepted.fragment.root();
    let NodeKind::Scan {
        relation,
        residuals,
        ..
    } = &accepted.fragment.nodes()[&scan].kind
    else {
        unreachable!()
    };
    let guaranteed = relation.predicate_guarantees()[0].predicate;
    assert_eq!(residuals.len(), 2);
    assert_eq!(residuals[0], guaranteed);
    let source = unit(&accepted);
    let guarantee_use = source.root_use(guaranteed).unwrap();
    let volatile_use = source.root_use(residuals[1]).unwrap();
    assert_ne!(guarantee_use, volatile_use);
    assert_eq!(source.claims().len(), 1);
    assert_eq!(
        source.claims()[&volatile_use].effects.value_stability,
        FunctionVolatility::Volatile,
    );
    crate::validate_fragment(&accepted.fragment, &accepted.cuts).unwrap();
    FragmentPackage::try_new(accepted, admission(), &Control).unwrap();

    let rejected = fixture(
        Predicate::IntrinsicDescendant(FunctionVolatility::Volatile),
        Some(PredicateGuaranteeKind::Exact),
        false,
        false,
    );
    let errors = crate::validate_fragment(&rejected.fragment, &rejected.cuts).unwrap_err();
    assert!(errors.errors().iter().any(|error| {
        error.category() == ValidationErrorCategory::StructuralInvariant
            && error
                .path()
                .contains(&format!("nodes[{}]", rejected.fragment.root().get()))
            && error.message() == "relation predicate guarantee must be replica deterministic"
    }));
    assert_missing_proof(rejected);
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
            Predicate::IntrinsicDescendant(FunctionVolatility::Immutable),
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
