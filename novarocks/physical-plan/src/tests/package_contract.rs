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
use std::collections::BTreeMap;
use std::sync::Arc;

use novarocks_connector_contract::{
    ConnectorReadRelationRecipeDraft, ConnectorValueType, FrozenConnectorRead, FrozenConnectorScan,
    ScanColumnId, StaticScanAssignment, TupleDomain,
};
use novarocks_type_contract::{
    ArgumentControl, CallEffects, CallProofScope, CompileControlError, CompilePhase, ControlShape,
    EvaluationDemand, FunctionInstanceState, FunctionNullBehavior, ObservableEffects,
    PureCompileControl, SemanticParameterId, SemanticParameterKey, SemanticParameterRef,
    SemanticParameterValue, SemanticParameters,
};

use super::*;

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}

// The selected test.parameter.zero fixture reads its exact frozen timezone
// reference and returns its bounded byte length as a non-NULL Int64. These
// are explicit fixture-owner
// facts, not effects inferred from a general function's legacy four fields.
fn parameter_fixture_effects(reference: SemanticParameterRef) -> CallEffects {
    assert_eq!(reference.expected_key, SemanticParameterKey::TimeZone);
    CallEffects {
        value_stability: FunctionVolatility::Stable,
        own_row_error: FunctionIntrinsicRowError::NoRowError,
        failure_behavior: FunctionFailureBehavior::Propagate,
        null_behavior: FunctionNullBehavior::CalledOnNull,
        argument_control: ArgumentControl::Eager,
        instance_state: FunctionInstanceState::None,
        observable_effects: ObservableEffects::NONE,
        environment: Box::from([reference]),
        proof_scope: CallProofScope::Unconditional,
    }
}

// Each test fragment explicitly declares no derived-domain witnesses.
// This fixture table does not classify or authorize its provider predicates.
fn fixture_pruning_tables(plan: &PhysicalPlan) -> BTreeMap<FragmentId, FrozenFragmentPruning> {
    plan.fragments()
        .keys()
        .map(|id| {
            (
                *id,
                FrozenFragmentPruning::try_new(*id, Vec::new(), &Control).unwrap(),
            )
        })
        .collect()
}

fn intrinsic_reference(id: u32) -> SemanticParameterRef {
    SemanticParameterRef {
        id: SemanticParameterId::new(id),
        expected_key: SemanticParameterKey::AllowThrowException,
    }
}

// Tests that deliberately corrupt the frozen table pass this actual source
// to extraction. This helper performs no alternate validation or inference.
fn fixture_parameter_input(plan: &PhysicalPlan, parameters: SemanticParameters) -> PhysicalPlan {
    crate::plan::PhysicalPlanParts {
        constants: plan.constants().clone(),
        parameters,
        statement_constant_policy: plan.statement_constant_policy(),
        version: plan.version(),
        fragments: plan.fragments().clone(),
        edges: plan.edges().clone(),
        runtime_filters: plan.runtime_filters().clone(),
        result_port: plan.result_port().cloned(),
        required: plan.required(),
        annotations: plan.annotations().to_vec().into_boxed_slice(),
        write_layouts: Default::default(),
    }
    .into()
}
fn fixture_plan_parameters(plan: &PhysicalPlan, parameters: SemanticParameters) -> PhysicalPlan {
    let plan = fixture_parameter_input(plan, parameters);
    validate_plan(&plan).unwrap();
    plan
}

fn extract(
    plan: &PhysicalPlan,
    scans: &BTreeMap<ProviderReadOccurrenceId, FrozenConnectorRead>,
    parameters: &SemanticParameters,
    writes: &BTreeMap<WriteTargetOrdinal, novarocks_connector_contract::ConnectorWriteRecipeDraft>,
) -> Result<BTreeMap<FragmentId, FragmentPackage>, FragmentPackageExtractionError> {
    let plan = fixture_plan_parameters(plan, parameters.clone());
    extract_fragment_packages(
        &plan,
        scans,
        writes,
        &fixture_pruning_tables(&plan),
        &package_admissions(&plan),
        &Control,
    )
}

fn package_input(
    fragment: Fragment,
    fixture_constants: &mut crate::tests::constant_fixture::ConstantFixture,
) -> FragmentPackageInput {
    let pruning = FrozenFragmentPruning::try_new(fragment.id(), Vec::new(), &Control).unwrap();
    let parameters = SemanticParameters::try_new(
        fragment
            .nodes()
            .values()
            .filter_map(|node| node.expressions.as_deref())
            .flat_map(|unit| unit.definitions())
            .flat_map(|definition| definition.kind.intrinsic_parameter_references())
            .map(|reference| {
                (
                    reference.id,
                    SemanticParameterValue::AllowThrowException(false),
                )
            })
            .collect::<BTreeMap<_, _>>(),
    )
    .unwrap();
    FragmentPackageInput {
        constants: fixture_constants.for_fragment(&fragment),
        statement_constant_policy: crate::tests::constant_fixture::policy(),
        version: version(),
        required: RequiredContracts::default(),
        pruning,
        fragment,
        cuts: FragmentCuts::default(),
        result: None,
        parameters,
        scans: BTreeMap::new(),
        writes: BTreeMap::new(),
        annotations: Box::default(),
        layouts: Default::default(),
    }
}

#[test]
fn extraction_requires_exact_expression_control_fragment_coverage() {
    let mut constants = crate::tests::constant_fixture::ConstantFixture::new();
    let (fragment, _) = literal_fragment(
        FragmentId::new(81),
        FragmentSink::Noop,
        false,
        &mut constants,
    );
    let mut builder = PlanBuilder::new(version(), crate::tests::constant_fixture::policy());
    builder.add_fragment(fragment.clone()).unwrap();
    let plan = crate::tests::constant_fixture::publish_plan(builder, &constants).unwrap();
    let packages = extract(
        &plan,
        &BTreeMap::new(),
        &SemanticParameters::default(),
        &BTreeMap::new(),
    )
    .unwrap();
    let package = &packages[&fragment.id()];
    assert_eq!(package.fragment(), &fragment);
    assert!(Arc::ptr_eq(
        package.fragment().nodes()[&fragment.root()]
            .expressions
            .as_ref()
            .unwrap(),
        fragment.nodes()[&fragment.root()]
            .expressions
            .as_ref()
            .unwrap(),
    ));
    // Coverage now belongs to each actual node; no parallel fragment-keyed table exists.
    let mut parts = fragment.clone().into_parts();
    parts.nodes.get_mut(&fragment.root()).unwrap().expressions = None;
    let mut input = package_input(fragment, &mut constants);
    input.fragment = Fragment::from(parts);
    let error = FragmentPackage::try_new(input, package_admission(), &Control).unwrap_err();
    assert!(
        matches!(&error, FragmentPackageError::Structure(_)),
        "{error:?}"
    );
    assert!(
        error.to_string().contains("requires an installed unit"),
        "{error}"
    );
}

#[test]
fn extraction_requires_explicit_pruning_table_for_every_fragment() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let (fragment, _) = literal_fragment(
        FragmentId::new(81),
        FragmentSink::Noop,
        false,
        &mut fixture_constants,
    );
    let mut builder = PlanBuilder::new(version(), crate::tests::constant_fixture::policy());
    builder.add_fragment(fragment.clone()).unwrap();
    let plan = crate::tests::constant_fixture::publish_plan(builder, &fixture_constants).unwrap();
    let parameters = SemanticParameters::default();
    let pruning = fixture_pruning_tables(&plan);
    let extract_with = |pruning: &BTreeMap<FragmentId, FrozenFragmentPruning>| {
        extract_fragment_packages(
            &fixture_plan_parameters(&plan, parameters.clone()),
            &BTreeMap::new(),
            &BTreeMap::new(),
            pruning,
            &package_admissions(&fixture_plan_parameters(&plan, parameters.clone())),
            &Control,
        )
    };
    let packages = extract_with(&pruning).unwrap();
    let package = &packages[&fragment.id()];
    assert_eq!(package.pruning(), &pruning[&fragment.id()]);
    assert_eq!(package.pruning().fragment(), fragment.id());
    assert!(package.pruning().witnesses().is_empty());
    assert_eq!(
        extract_with(&BTreeMap::new()).unwrap_err(),
        FragmentPackageExtractionError::MissingPruning(fragment.id())
    );
    let mut extra = pruning;
    let extra_id = FragmentId::new(82);
    extra.insert(
        extra_id,
        FrozenFragmentPruning::try_new(extra_id, Vec::new(), &Control).unwrap(),
    );
    assert_eq!(
        extract_with(&extra).unwrap_err(),
        FragmentPackageExtractionError::UnusedPruning
    );
}

#[test]
fn package_and_extraction_reject_pruning_table_fragment_identity_mismatch() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let (fragment, _) = literal_fragment(
        FragmentId::new(81),
        FragmentSink::Noop,
        false,
        &mut fixture_constants,
    );
    let wrong = FrozenFragmentPruning::try_new(FragmentId::new(82), Vec::new(), &Control).unwrap();
    let mut input = package_input(fragment.clone(), &mut fixture_constants);
    input.pruning = wrong.clone();
    assert_eq!(
        FragmentPackage::try_new(input, package_admission(), &Control).unwrap_err(),
        FragmentPackageError::Pruning(FrozenPruningError::WrongFragment)
    );
    let mut builder = PlanBuilder::new(version(), crate::tests::constant_fixture::policy());
    builder.add_fragment(fragment.clone()).unwrap();
    let plan = crate::tests::constant_fixture::publish_plan(builder, &fixture_constants).unwrap();
    // The map key is correct; the table's own exact fragment identity is not.
    assert_eq!(
        extract_fragment_packages(
            &fixture_plan_parameters(&plan, SemanticParameters::default().clone()),
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::from([(fragment.id(), wrong)]),
            &package_admissions(&fixture_plan_parameters(
                &plan,
                SemanticParameters::default().clone()
            )),
            &Control,
        )
        .unwrap_err(),
        FragmentPackageExtractionError::Local(FragmentPackageError::Pruning(
            FrozenPruningError::WrongFragment
        ))
    );
}

fn binary_control_fixture(
    fixture_constants: &mut crate::tests::constant_fixture::ConstantFixture,
) -> (Fragment, ExprId, ExprId, ExprId) {
    let mut builder = FragmentBuilder::new(FragmentId::new(83));
    let node = builder.reserve_node_id().unwrap();
    let left = builder
        .define(
            node,
            (ty(DataType::Int64, false)).clone(),
            fixture_constants.i64(11, &(ty(DataType::Int64, false))),
        )
        .unwrap();
    let right = builder
        .define(
            node,
            (ty(DataType::Int64, false)).clone(),
            fixture_constants.i64(22, &(ty(DataType::Int64, false))),
        )
        .unwrap();
    let root = builder
        .define(
            node,
            ty(DataType::Int64, false),
            ExprKind::Binary {
                left,
                allow_throw_exception: Some(intrinsic_reference(0)),
                op: BinaryOperator::Subtract,
                right,
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
        )
        .unwrap();
    let outputs = (0..2)
        .map(|ordinal| {
            builder
                .add_value(
                    ty(DataType::Int64, false),
                    ValueOrigin::NodeOutput {
                        node,
                        output_ordinal: ordinal,
                    },
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    let value_field_root_0 = builder.root_definition(
        node,
        root,
        novarocks_type_contract::EvaluationDemand::Value,
        crate::RootScope::AllPorts,
    );
    let value_field_root_1 = builder.root_definition(
        node,
        left,
        novarocks_type_contract::EvaluationDemand::Value,
        crate::RootScope::AllPorts,
    );
    builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: singleton(),
            output: OutputPort {
                node,
                columns: outputs.into_boxed_slice(),
            },
            kind: NodeKind::Values {
                rows: Box::from([Box::from([value_field_root_0, value_field_root_1])]),
            },
        })
        .unwrap();
    let fragment = builder
        .finish_fixture_definition(
            node,
            FragmentSink::Noop,
            dop(),
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    (fragment, root, left, right)
}

#[test]
fn package_rechecks_ordered_children_intrinsic_control_and_roots_with_reused_ids() {
    let mut constants = crate::tests::constant_fixture::ConstantFixture::new();
    let (original, root, left, right) = binary_control_fixture(&mut constants);
    let checked = FragmentPackage::try_new(
        package_input(original.clone(), &mut constants),
        package_admission(),
        &Control,
    )
    .unwrap();
    assert_eq!(checked.fragment(), &original);
    // Both malformed definition/control snapshots are refused by the sole unit author.
    // The independent flow is retained unchanged; dense IDs alone prove no correspondence.
    for mutation in 0..2 {
        let error = candidate_fixture_unit(&original, original.root(), |parts| {
            if mutation == 0 {
                let ExprKind::Binary {
                    left: a, right: b, ..
                } = &mut parts.definitions[root.get() as usize].kind
                else {
                    unreachable!()
                };
                std::mem::swap(a, b);
            } else {
                parts.definitions[left.get() as usize].ty = ty(DataType::Boolean, false);
                parts.definitions[left.get() as usize].kind =
                    constants.boolean(true, &ty(DataType::Boolean, false));
                parts.definitions[root.get() as usize].ty = ty(DataType::Int64, true);
                parts.definitions[root.get() as usize].kind = ExprKind::Case {
                    operand: None,
                    when_then: Box::from([(left, right)]),
                    else_expr: None,
                };
            }
        })
        .unwrap_err();
        let expected = if mutation == 0 {
            "different definition occurrence"
        } else {
            "intrinsic control differs"
        };
        assert!(matches!(
            error,
            novarocks_expression_contract::UnitError::Invalid { .. }
        ));
        assert!(error.to_string().contains(expected), "{error}");
    }
    // An out-of-range node field remains a package-level error even with a valid unit.
    let mut parts = original.clone().into_parts();
    let NodeKind::Values { rows } = &mut parts.nodes.get_mut(&original.root()).unwrap().kind else {
        unreachable!()
    };
    rows[0][0] = novarocks_expression_contract::RootOrdinal::new(2);
    let mut input = package_input(original, &mut constants);
    input.fragment = Fragment::from(parts);
    let error = FragmentPackage::try_new(input, package_admission(), &Control).unwrap_err();
    assert!(
        matches!(&error, FragmentPackageError::Structure(_)),
        "{error:?}"
    );
    assert!(error.to_string().contains("root"), "{error}");
}

struct FailureControl {
    failure: CompileControlError,
    positive_only: bool,
    work: std::sync::Mutex<Vec<u32>>,
}
impl PureCompileControl for FailureControl {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= novarocks_type_contract::MAX_UNOBSERVED_COMPILE_WORK);
        self.work.lock().unwrap().push(units);
        if !self.positive_only || units > 0 {
            Err(self.failure.clone())
        } else {
            Ok(())
        }
    }
}

#[test]
fn package_and_extraction_keep_typed_control_failures_before_and_during_validation() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let (fragment, _) = literal_fragment(
        FragmentId::new(81),
        FragmentSink::Noop,
        false,
        &mut fixture_constants,
    );
    let input = package_input(fragment.clone(), &mut fixture_constants);
    let mut builder = PlanBuilder::new(version(), crate::tests::constant_fixture::policy());
    builder.add_fragment(fragment).unwrap();
    let plan = crate::tests::constant_fixture::publish_plan(builder, &fixture_constants).unwrap();
    for failure in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for positive_only in [false, true] {
            let control = FailureControl {
                failure: failure.clone(),
                positive_only,
                work: std::sync::Mutex::default(),
            };
            assert_eq!(
                FragmentPackage::try_new(input.clone(), package_admission(), &control).unwrap_err(),
                FragmentPackageError::Control(failure.clone())
            );
            if positive_only {
                assert!(control.work.lock().unwrap().iter().any(|units| *units > 0));
            }
            let control = FailureControl {
                failure: failure.clone(),
                positive_only,
                work: std::sync::Mutex::default(),
            };
            let expected = FragmentPackageExtractionError::Control(failure.clone());
            assert_eq!(
                extract_fragment_packages(
                    &fixture_plan_parameters(&plan, SemanticParameters::default().clone()),
                    &BTreeMap::new(),
                    &BTreeMap::new(),
                    &fixture_pruning_tables(&plan),
                    &package_admissions(&fixture_plan_parameters(
                        &plan,
                        SemanticParameters::default().clone()
                    )),
                    &control,
                )
                .unwrap_err(),
                expected
            );
            if positive_only {
                assert!(control.work.lock().unwrap().iter().any(|units| *units > 0));
            }
        }
    }
}

#[test]
fn package_control_checks_correspondence_without_claiming_literal_content_identity() {
    let mut constants = crate::tests::constant_fixture::ConstantFixture::new();
    let (original, _) = literal_fragment(
        FragmentId::new(84),
        FragmentSink::Noop,
        false,
        &mut constants,
    );
    let unit = parameter_unit(&original);
    let definition = unit.roots()[0].definition;
    let literal = &unit.definitions()[definition.get() as usize];
    let ExprKind::Constant(reference) = literal.kind else {
        unreachable!()
    };
    let mut work =
        novarocks_type_contract::CompileCheckpoints::try_new(&Control, CompilePhase::Validate)
            .unwrap();
    assert_eq!(
        constants
            .pools()
            .resolve_observed(reference, &literal.ty, &mut work)
            .unwrap()
            .try_i64()
            .unwrap(),
        Some(11)
    );
    work.finish().unwrap();
    let changed = replace_fixture_unit(original.clone(), original.root(), |parts| {
        let literal = &mut parts.definitions[definition.get() as usize];
        literal.kind = constants.i64(22, &literal.ty);
    })
    .unwrap();
    validate_fragment_definition(&changed).unwrap();
    assert_eq!(
        parameter_unit(&original).control_flow(),
        parameter_unit(&changed).control_flow()
    );
    let package = FragmentPackage::try_new(
        package_input(changed.clone(), &mut constants),
        package_admission(),
        &Control,
    )
    .unwrap();
    assert_eq!(package.fragment(), &changed);
    let ExprKind::Constant(reference) =
        parameter_unit(package.fragment()).definitions()[definition.get() as usize].kind
    else {
        unreachable!()
    };
    let mut work =
        novarocks_type_contract::CompileCheckpoints::try_new(&Control, CompilePhase::Validate)
            .unwrap();
    assert_eq!(
        package
            .constants()
            .resolve_observed(reference, &literal.ty, &mut work)
            .unwrap()
            .try_i64()
            .unwrap(),
        Some(22)
    );
    work.finish().unwrap();
    // Source correspondence is checked against the actual node, not a detached use table.
    let mut foreign = changed.into_parts();
    foreign.id = FragmentId::new(85);
    let mut input = package_input(original, &mut constants);
    input.fragment = Fragment::from(foreign);
    let error = FragmentPackage::try_new(input, package_admission(), &Control).unwrap_err();
    assert!(
        matches!(
            error,
            FragmentPackageError::Calls(FrozenCallError::WrongFragment)
                | FragmentPackageError::Pruning(FrozenPruningError::WrongFragment)
        ),
        "{error:?}"
    );
}

#[test]
fn package_preserves_actual_case_branch_guards_and_root_occurrences() {
    use novarocks_type_contract::GuardKind;
    let mut constants = crate::tests::constant_fixture::ConstantFixture::new();
    let mut builder = FragmentBuilder::new(FragmentId::new(83));
    let node = builder.reserve_node_id().unwrap();
    let condition = builder
        .define(
            node,
            ty(DataType::Boolean, false),
            constants.boolean(true, &ty(DataType::Boolean, false)),
        )
        .unwrap();
    let value = builder
        .define(
            node,
            ty(DataType::Int64, false),
            constants.i64(22, &ty(DataType::Int64, false)),
        )
        .unwrap();
    let case = builder
        .define(
            node,
            ty(DataType::Int64, true),
            ExprKind::Case {
                operand: None,
                when_then: Box::from([(condition, value)]),
                else_expr: None,
            },
        )
        .unwrap();
    let case_root = builder.root_definition(
        node,
        case,
        EvaluationDemand::Value,
        crate::RootScope::AllPorts,
    );
    let condition_root = builder.root_definition(
        node,
        condition,
        EvaluationDemand::Value,
        crate::RootScope::AllPorts,
    );
    let output = [ty(DataType::Int64, true), ty(DataType::Boolean, false)]
        .into_iter()
        .enumerate()
        .map(|(ordinal, ty)| {
            builder
                .add_value(
                    ty,
                    ValueOrigin::NodeOutput {
                        node,
                        output_ordinal: ordinal as u32,
                    },
                )
                .unwrap()
        })
        .collect();
    builder
        .add_values(
            node,
            Box::from([Box::from([case_root, condition_root])]),
            output,
        )
        .unwrap();
    let fragment = builder
        .finish_fixture_definition(
            node,
            FragmentSink::Noop,
            dop(),
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let source = fragment.nodes()[&node]
        .expressions
        .as_ref()
        .unwrap()
        .clone();
    let package = FragmentPackage::try_new(
        package_input(fragment, &mut constants),
        package_admission(),
        &Control,
    )
    .unwrap();
    let unit = package.fragment().unit(node).unwrap();
    assert!(std::ptr::eq(unit, source.as_ref()));
    let flow = unit.control_flow();
    assert_eq!(unit.root_use_ids().len(), 2);
    let owner = unit.root_use_ids()[0];
    let independent = unit.root_use_ids()[1];
    let invocation = &flow.uses()[&owner];
    assert_eq!(invocation.definition, case);
    assert_eq!(
        invocation.control,
        ControlShape::Case {
            simple: false,
            arms: 1,
            has_else: false
        }
    );
    assert_eq!(invocation.arguments.len(), 2);
    for (use_id, definition, demand, guard) in [
        (
            invocation.arguments[0],
            condition,
            EvaluationDemand::TruthOnly,
            GuardKind::CaseWhen { arm: 0 },
        ),
        (
            invocation.arguments[1],
            value,
            EvaluationDemand::Value,
            GuardKind::CaseThen { arm: 0 },
        ),
    ] {
        let child = &flow.uses()[&use_id];
        assert_eq!(child.definition, definition);
        assert_eq!(child.context.demand, demand);
        let region = &flow.regions()[&child.context.region];
        assert_eq!(region.parent, Some(invocation.context.region));
        assert_eq!(
            region.guard,
            Some(novarocks_type_contract::RegionGuard { owner, kind: guard })
        );
    }
    assert_eq!(flow.uses()[&independent].definition, condition);
    assert_eq!(
        flow.uses()[&independent].context.demand,
        EvaluationDemand::Value
    );
    // Each builder-authored root keeps its own unguarded region. The
    // independent condition is not the CASE's guarded WHEN occurrence.
    assert_ne!(
        flow.uses()[&independent].context.region,
        invocation.context.region
    );
    for root in [owner, independent] {
        let region = &flow.regions()[&flow.uses()[&root].context.region];
        assert_eq!(region.parent, None);
        assert_eq!(region.guard, None);
    }
    assert_ne!(
        flow.uses()[&independent].context.region,
        flow.uses()[&invocation.arguments[0]].context.region
    );
    assert_ne!(independent, invocation.arguments[0]);
}

pub(super) fn frozen_scan(fragment: &Fragment) -> FrozenConnectorRead {
    let NodeKind::Scan { relation, .. } = &fragment.nodes()[&fragment.root()].kind else {
        unreachable!()
    };
    let read = relation.read();
    let draft = ConnectorReadRelationRecipeDraft::try_new(
        read.binding.clone(),
        read.relation.clone(),
        relation
            .schema()
            .iter()
            .map(|field| field.column.column_payload.clone())
            .collect(),
    )
    .unwrap();
    let scan = FrozenConnectorScan::try_new(
        draft,
        vec![StaticScanAssignment::new(
            Arc::from("v"),
            ConnectorValueType::BigInt,
        )],
        TupleDomain::all(),
        TupleDomain::all(),
        None,
        vec![],
    )
    .unwrap();
    public_read(fragment, fragment.root(), scan).unwrap()
}

fn public_read(
    fragment: &Fragment,
    scan_id: NodeId,
    scan: FrozenConnectorScan,
) -> Result<FrozenConnectorRead, novarocks_connector_contract::ConnectorError> {
    use novarocks_connector_contract::*;
    let NodeKind::Scan {
        relation,
        provider_outputs,
        ..
    } = &fragment.nodes()[&scan_id].kind
    else {
        unreachable!()
    };
    let properties = relation.provided_properties();
    let distribution = match properties.distribution {
        Distribution::Unconstrained => ConnectorReadDistribution::Unconstrained,
        Distribution::Singleton => ConnectorReadDistribution::Singleton,
        Distribution::RoundRobin => ConnectorReadDistribution::RoundRobin,
        _ => unreachable!("fixture uses no provider partitioned guarantee"),
    };
    let ordering = properties
        .ordering
        .iter()
        .map(|key| {
            ConnectorReadOrderingKey::new(
                ScanColumnId::new(
                    provider_outputs
                        .iter()
                        .position(|(_, value)| *value == key.value)
                        .unwrap(),
                ),
                match key.direction {
                    SortDirection::Ascending => ConnectorReadSortDirection::Ascending,
                    SortDirection::Descending => ConnectorReadSortDirection::Descending,
                },
                match key.null_ordering {
                    NullOrdering::First => ConnectorReadNullOrdering::First,
                    NullOrdering::Last => ConnectorReadNullOrdering::Last,
                },
            )
        })
        .collect::<Vec<_>>();
    let (kind, coverage) = match relation.as_ref() {
        Relation::Data(_) => (None, vec![]),
        Relation::Metadata(metadata) => (
            Some(ConnectorReadMetadataKind::try_new(metadata.kind.as_str())?),
            metadata.coverage_evidence.to_vec(),
        ),
    };
    let facts = ConnectorReadStaticFacts::try_new(
        relation.read().input_version.clone(),
        relation.selection_digest(),
        ConnectorReadProperties::try_new(distribution, ordering)?,
        ConnectorReadArtifactCoverage::NoArtifactInputs,
        coverage,
    )?;
    let schema = arrow_schema::Schema::new(
        relation
            .schema()
            .iter()
            .enumerate()
            .map(|(ordinal, field)| {
                arrow_schema::Field::new(
                    format!("v{ordinal}"),
                    field.ty.data_type().clone(),
                    field.ty.nullable(),
                )
            })
            .collect::<Vec<_>>(),
    );
    let public = ConnectorReadPublicFacts::try_new(
        facts,
        kind,
        schema,
        relation
            .schema()
            .iter()
            .map(|field| field.ty.logical_type())
            .collect(),
    )?;
    FrozenConnectorRead::try_new(scan, public)
}

#[test]
fn package_extracts_metadata_without_losing_public_scan_facts() {
    let fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let binding = connector_binding();
    let column = ProviderColumnReference {
        column_payload: encoded(&binding, ConnectorCodecCategory::ReadColumn, 7),
    };
    let fragment = finish_scan_relation(metadata_relation(&binding, column)).unwrap();
    let scan = frozen_scan(&fragment);
    let fragment_id = fragment.id();
    let node_id = fragment.root();
    let mut builder = PlanBuilder::new(version(), crate::tests::constant_fixture::policy());
    builder.add_fragment(fragment.clone()).unwrap();
    builder.add_annotation(PlanAnnotation {
        subject: AnnotationSubject::Node(fragment_id, node_id),
        key: "statistics.row_count".into(),
        value: "17".into(),
    });
    let plan = crate::tests::constant_fixture::publish_plan(builder, &fixture_constants).unwrap();
    let parameters = SemanticParameters::default();
    let scans = BTreeMap::from([(ProviderReadOccurrenceId::new(0), scan.clone())]);
    let packages = extract(&plan, &scans, &parameters, &BTreeMap::new()).unwrap();
    let package = &packages[&fragment_id];
    assert_eq!(package.fragment(), &fragment);
    assert!(package.parameters().entries().is_empty());
    assert_eq!(package.scans()[&node_id], scan);
    assert_eq!(package.annotations(), plan.annotations());
    assert!(
        matches!(&package.fragment().nodes()[&node_id].kind, NodeKind::Scan { relation, .. } if matches!(relation.as_ref(), Relation::Metadata(_)))
    );
    assert!(matches!(
        extract(&plan, &BTreeMap::new(), &parameters, &BTreeMap::new()),
        Err(FragmentPackageExtractionError::MissingScan(_))
    ));
    let mut extra = scans;
    extra.insert(ProviderReadOccurrenceId::new(1), scan);
    assert!(matches!(
        extract(&plan, &extra, &parameters, &BTreeMap::new()),
        Err(FragmentPackageExtractionError::UnusedScan)
    ));
}

#[test]
fn explicit_empty_pruning_table_keeps_actual_scan_and_provider_domains() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    use novarocks_connector_contract::{ConnectorValue, Domain};
    let binding = connector_binding();
    let column = ProviderColumnReference {
        column_payload: encoded(&binding, ConnectorCodecCategory::ReadColumn, 7),
    };
    let fragment = finish_scan_relation(metadata_relation(&binding, column)).unwrap();
    let original = frozen_scan(&fragment);
    let scan = original.scan();
    let enforced = TupleDomain::with_column_domains(BTreeMap::from([(
        ScanColumnId::new(0),
        Domain::single_value(ConnectorValue::BigInt(7)).unwrap(),
    )]))
    .unwrap();
    let unenforced = TupleDomain::with_column_domains(BTreeMap::from([(
        ScanColumnId::new(0),
        Domain::single_value(ConnectorValue::BigInt(11)).unwrap(),
    )]))
    .unwrap();
    let frozen = FrozenConnectorScan::try_new(
        scan.recipe().clone(),
        scan.assignments().to_vec(),
        enforced.clone(),
        unenforced.clone(),
        None,
        vec![],
    )
    .unwrap();
    let read = public_read(&fragment, fragment.root(), frozen).unwrap();
    let mut input = package_input(fragment.clone(), &mut fixture_constants);
    assert!(input.pruning.witnesses().is_empty());
    input.scans.insert(fragment.root(), read.clone());
    let package = FragmentPackage::try_new(input, package_admission(), &Control).unwrap();
    assert!(package.pruning().witnesses().is_empty());
    assert_eq!(package.fragment(), &fragment);
    assert_eq!(package.calls(), fragment.calls());
    assert_eq!(package.scans().len(), 1);
    let retained = &package.scans()[&fragment.root()];
    assert_eq!(retained, &read);
    assert_eq!(retained.scan().enforced_predicate(), &enforced);
    assert_eq!(retained.scan().unenforced_predicate(), &unenforced);
    // Empty structural declarations retain these actual owner-frozen facts;
    // they establish neither implication nor runtime pruning permission.
    let mut builder = PlanBuilder::new(version(), crate::tests::constant_fixture::policy());
    builder.add_fragment(fragment.clone()).unwrap();
    let plan = crate::tests::constant_fixture::publish_plan(builder, &fixture_constants).unwrap();
    let packages = extract(
        &plan,
        &BTreeMap::from([(ProviderReadOccurrenceId::new(0), read)]),
        &SemanticParameters::default(),
        &BTreeMap::new(),
    )
    .unwrap();
    let extracted = &packages[&fragment.id()];
    assert!(extracted.pruning().witnesses().is_empty());
    assert_eq!(extracted.fragment(), &fragment);
    assert_eq!(extracted.calls(), fragment.calls());
    assert_eq!(
        extracted.scans()[&fragment.root()]
            .scan()
            .enforced_predicate(),
        &enforced
    );
    assert_eq!(
        extracted.scans()[&fragment.root()]
            .scan()
            .unenforced_predicate(),
        &unenforced
    );
}

#[test]
fn package_refuses_missing_or_wrong_scan_node_facts() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let binding = connector_binding();
    let column = ProviderColumnReference {
        column_payload: encoded(&binding, ConnectorCodecCategory::ReadColumn, 7),
    };
    let fragment = finish_scan_relation(metadata_relation(&binding, column)).unwrap();
    let mut input = package_input(fragment.clone(), &mut fixture_constants);
    assert!(
        FragmentPackage::try_new(input.clone(), package_admission(), &Control)
            .unwrap_err()
            .to_string()
            .contains("no complete frozen public facts")
    );
    input.scans.insert(fragment.root(), frozen_scan(&fragment));
    FragmentPackage::try_new(input.clone(), package_admission(), &Control).unwrap();
    input
        .scans
        .insert(NodeId::new(u32::MAX), frozen_scan(&fragment));
    assert!(
        FragmentPackage::try_new(input, package_admission(), &Control)
            .unwrap_err()
            .to_string()
            .contains("missing or non-scan node")
    );
}

#[test]
fn package_refuses_complete_public_source_drift() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    use novarocks_connector_contract::*;
    let binding = connector_binding();
    let column = ProviderColumnReference {
        column_payload: encoded(&binding, ConnectorCodecCategory::ReadColumn, 7),
    };
    let fragment = finish_scan_relation(metadata_relation(&binding, column)).unwrap();
    let original = frozen_scan(&fragment);
    for mutation in 0..7 {
        let public = original.public_facts();
        let source = public.source();
        let properties = if mutation == 4 {
            ConnectorReadProperties::try_new(ConnectorReadDistribution::RoundRobin, vec![]).unwrap()
        } else if mutation == 5 {
            ConnectorReadProperties::try_new(
                source.properties().distribution().clone(),
                vec![ConnectorReadOrderingKey::new(
                    ScanColumnId::new(0),
                    ConnectorReadSortDirection::Ascending,
                    ConnectorReadNullOrdering::First,
                )],
            )
            .unwrap()
        } else {
            source.properties().clone()
        };
        let source = ConnectorReadStaticFacts::try_new(
            if mutation == 0 {
                {
                    let mut bytes = source.input_version().as_bytes().to_vec();
                    bytes[0] ^= 1;
                    ConnectorReadInputVersion::try_new(bytes).unwrap()
                }
            } else {
                source.input_version().clone()
            },
            if mutation == 1 {
                let mut digest = source.selection_digest();
                digest[0] ^= 1;
                digest
            } else {
                source.selection_digest()
            },
            properties,
            source.artifact_coverage().clone(),
            if mutation == 3 {
                let mut bytes = source.coverage_evidence().to_vec();
                bytes[0] ^= 1;
                bytes
            } else {
                source.coverage_evidence().to_vec()
            },
        )
        .unwrap();
        let schema = if mutation == 6 {
            arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "v0",
                DataType::Int64,
                !public.schema().field(0).is_nullable(),
            )])
        } else {
            public.schema().clone()
        };
        let public = ConnectorReadPublicFacts::try_new(
            source,
            if mutation == 2 {
                Some(ConnectorReadMetadataKind::try_new("other").unwrap())
            } else {
                public.metadata_kind().cloned()
            },
            schema,
            public.logical_types().to_vec(),
        )
        .unwrap();
        let scan = FrozenConnectorRead::try_new(original.scan().clone(), public).unwrap();
        let mut input = package_input(fragment.clone(), &mut fixture_constants);
        input.scans.insert(fragment.root(), scan);
        assert!(
            FragmentPackage::try_new(input, package_admission(), &Control).is_err(),
            "accepted public source drift {mutation}"
        );
    }
}

#[test]
fn package_refuses_exact_relation_and_batch_contract_drift() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let binding = connector_binding();
    let column = ProviderColumnReference {
        column_payload: encoded(&binding, ConnectorCodecCategory::ReadColumn, 7),
    };
    let fragment = finish_scan_relation(metadata_relation(&binding, column)).unwrap();
    let frozen = frozen_scan(&fragment);
    let scan = frozen.scan();
    let recipe = scan.recipe();
    for mutation in [0, 2] {
        let mut columns = recipe.columns().to_vec();
        if mutation == 0 {
            columns[0] = encoded(&binding, ConnectorCodecCategory::ReadColumn, 8);
        }
        let draft = ConnectorReadRelationRecipeDraft::try_new(
            recipe.binding().clone(),
            recipe.relation().clone(),
            columns,
        )
        .unwrap();
        let malformed = FrozenConnectorScan::try_new(
            draft,
            if mutation == 2 {
                vec![StaticScanAssignment::new(
                    Arc::from("v"),
                    ConnectorValueType::Varchar,
                )]
            } else {
                scan.assignments().to_vec()
            },
            TupleDomain::all(),
            TupleDomain::all(),
            None,
            vec![],
        )
        .unwrap();
        let malformed = public_read(&fragment, fragment.root(), malformed);
        if mutation == 2 {
            assert!(malformed.is_err());
            continue;
        }
        let mut input = package_input(fragment.clone(), &mut fixture_constants);
        input.scans.insert(fragment.root(), malformed.unwrap());
        assert!(
            FragmentPackage::try_new(input, package_admission(), &Control).is_err(),
            "mutation {mutation}"
        );
    }
}

#[test]
fn result_and_annotation_facts_are_checked_at_the_local_boundary() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let id = FragmentId::new(81);
    let (fragment, value) =
        literal_fragment(id, FragmentSink::Result, false, &mut fixture_constants);
    let field = ResultField {
        domain: crate::ResultValueDomain::Plain,
        name: "v".into(),
        alias: None,
        value,
        ty: fragment.values()[&value].ty.clone(),
    };
    let mut input = package_input(fragment.clone(), &mut fixture_constants);
    assert!(FragmentPackage::try_new(input.clone(), package_admission(), &Control).is_err());
    input.result = Some(ResultPort {
        scalar_schema: None,
        fragment: id,
        output: fragment.nodes()[&fragment.root()].output.clone(),
        fields: Box::from([field]),
    });
    FragmentPackage::try_new(input.clone(), package_admission(), &Control).unwrap();
    let mut wrong_result = input.clone();
    wrong_result.result.as_mut().unwrap().fields[0].ty =
        wrong_result.result.as_ref().unwrap().fields[0]
            .ty
            .with_nullable(true)
            .unwrap();
    assert!(
        FragmentPackage::try_new(wrong_result, package_admission(), &Control)
            .unwrap_err()
            .to_string()
            .contains("result type differs")
    );
    input.annotations = Box::from([PlanAnnotation {
        subject: AnnotationSubject::Node(FragmentId::new(82), fragment.root()),
        key: "statistics.row_count".into(),
        value: "17".into(),
    }]);
    assert!(
        FragmentPackage::try_new(input, package_admission(), &Control)
            .unwrap_err()
            .to_string()
            .contains("subject this plan does not have")
    );
}

// Compare every owned fact and each original checked backing identity.
// Equal addresses alone do not establish equality across different pools.
fn assert_no_constant_package_equal(left: &FragmentPackage, right: &FragmentPackage) {
    assert_eq!(
        left.constants().entries().keys().collect::<Vec<_>>(),
        right.constants().entries().keys().collect::<Vec<_>>()
    );
    for (id, pool) in left.constants().entries() {
        let other = &right.constants().entries()[id];
        assert_eq!(pool.backing_identity(), other.backing_identity());
        assert_eq!(pool.value_type(), other.value_type());
    }
    assert_eq!(left.version(), right.version());
    assert_eq!(left.required(), right.required());
    assert_eq!(
        left.statement_constant_policy(),
        right.statement_constant_policy()
    );
    assert_eq!(left.fragment(), right.fragment());
    assert_eq!(left.calls(), right.calls());
    assert_eq!(left.pruning(), right.pruning());
    assert_eq!(left.cuts(), right.cuts());
    assert_eq!(left.result(), right.result());
    assert_eq!(left.parameters(), right.parameters());
    assert_eq!(left.scans(), right.scans());
    assert_eq!(left.writes(), right.writes());
    assert_eq!(left.annotations(), right.annotations());
}

#[test]
fn remote_plan_statistics_do_not_grow_a_fragment_package() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let (fragment, _) = literal_fragment(
        FragmentId::new(81),
        FragmentSink::Noop,
        false,
        &mut fixture_constants,
    );
    let mut builder = PlanBuilder::new(version(), crate::tests::constant_fixture::policy());
    builder.add_fragment(fragment.clone()).unwrap();
    let before = crate::tests::constant_fixture::publish_plan(builder, &fixture_constants).unwrap();
    let mut builder = PlanBuilder::new(version(), crate::tests::constant_fixture::policy());
    builder.add_fragment(fragment).unwrap();
    builder.add_annotation(PlanAnnotation {
        subject: AnnotationSubject::Plan,
        key: "optimizer.table_statistics".into(),
        value: "remote table statistics".repeat(100).into(),
    });
    let after = crate::tests::constant_fixture::publish_plan(builder, &fixture_constants).unwrap();
    let parameters = SemanticParameters::default();
    let before_packages =
        extract(&before, &BTreeMap::new(), &parameters, &BTreeMap::new()).unwrap();
    let after_packages = extract(&after, &BTreeMap::new(), &parameters, &BTreeMap::new()).unwrap();
    assert_eq!(
        before_packages.keys().collect::<Vec<_>>(),
        after_packages.keys().collect::<Vec<_>>()
    );
    for (id, before) in &before_packages {
        assert_no_constant_package_equal(before, &after_packages[id]);
    }
    assert_eq!(after.annotations().len(), 1);
}

#[test]
fn duplicate_scan_payloads_keep_runtime_filter_assignment_occurrences() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let (fragment, filter, second) = super::contract_regressions::scan_lineage_filter(
        false,
        false,
        false,
        true,
        &mut fixture_constants,
    );
    let scan_id = filter.consumers[0].endpoint.node;
    let second = second.unwrap();
    let mut nodes = fragment.nodes().clone();
    let NodeKind::Scan {
        relation,
        provider_outputs,
        ..
    } = &mut nodes.get_mut(&scan_id).unwrap().kind
    else {
        unreachable!()
    };
    let first_field = provider_outputs[0].0.clone();
    provider_outputs[1].0 = first_field.clone();
    let schema = Box::from([relation.schema()[0].clone(), relation.schema()[0].clone()]);
    match relation.as_mut() {
        Relation::Data(relation) => relation.schema = schema,
        Relation::Metadata(relation) => relation.schema = schema,
    }
    let mut values = fragment.values().clone();
    values.get_mut(&second).unwrap().origin = ValueOrigin::ProviderField {
        scan_node: scan_id,
        field: first_field,
    };
    let fragment = Fragment::from(crate::plan::FragmentParts {
        summaries: Box::default(),
        stage_placement: fragment.stage_placement().clone(),
        id: fragment.id(),
        root: fragment.root(),
        nodes,
        values,

        sink: fragment.sink().clone(),
        dop_domain: fragment.dop_domain(),
        runtime_filters: fragment.runtime_filters().into(),
        call_requests: fragment.call_requests().clone(),
        calls: fragment.calls().clone(),
    });
    let NodeKind::Scan { relation, .. } = &fragment.nodes()[&scan_id].kind else {
        unreachable!()
    };
    let read = relation.read();
    let recipe = ConnectorReadRelationRecipeDraft::try_new(
        read.binding.clone(),
        read.relation.clone(),
        relation
            .schema()
            .iter()
            .map(|field| field.column.column_payload.clone())
            .collect(),
    )
    .unwrap();
    let mut input = package_input(fragment.clone(), &mut fixture_constants);
    input.cuts = FragmentCuts {
        runtime_filter_bindings: single_fragment_runtime_filter_bindings(
            fragment.id(),
            std::slice::from_ref(&filter),
        ),
        runtime_filters: Box::from([filter]),
        ..FragmentCuts::default()
    };
    // The consumer targets the first ValueId. The second assignment has an
    // identical private payload but is a different output occurrence.
    for (variable, accepted) in [("v0", true), ("v1", false)] {
        let scan = FrozenConnectorScan::try_new(
            recipe.clone(),
            vec![
                StaticScanAssignment::new(Arc::from("v0"), ConnectorValueType::BigInt),
                StaticScanAssignment::new(Arc::from("v1"), ConnectorValueType::BigInt),
            ],
            TupleDomain::all(),
            TupleDomain::all(),
            None,
            vec![novarocks_connector_contract::StaticScanDynamicFilter::new(
                101,
                Arc::from(variable),
            )],
        )
        .unwrap();
        input
            .scans
            .insert(scan_id, public_read(&fragment, scan_id, scan).unwrap());
        let result = FragmentPackage::try_new(input.clone(), package_admission(), &Control);
        assert_eq!(result.is_ok(), accepted, "{variable}: {result:?}");
    }
}

#[test]
fn package_refuses_a_runtime_filter_binding_table_other_than_its_plan_slice() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let plan = super::runtime_filter_binding_contract::numbered_plan();
    let fragment = plan.fragments()[&FragmentId::new(412)].clone();
    let mut input = package_input(fragment.clone(), &mut fixture_constants);
    input.cuts = fragment_cuts(&plan, fragment.id()).unwrap();
    let mut constant_work =
        novarocks_type_contract::CompileCheckpoints::try_new(&Control, CompilePhase::Validate)
            .unwrap();
    input.constants =
        crate::project_fragment_constants_observed(plan.constants(), &fragment, &mut constant_work)
            .unwrap();
    constant_work.finish().unwrap();
    let package = FragmentPackage::try_new(input.clone(), package_admission(), &Control).unwrap();
    assert_eq!(
        package.cuts().runtime_filter_bindings,
        input.cuts.runtime_filter_bindings
    );
    let refusal = |input: FragmentPackageInput| match FragmentPackage::try_new(
        input,
        package_admission(),
        &Control,
    ) {
        Err(FragmentPackageError::Structure(errors)) => errors,
        other => panic!("tampered binding table was not refused: {other:?}"),
    };
    let mut swapped = input.clone();
    swapped.cuts.runtime_filter_bindings.swap(0, 1);
    let errors = refusal(swapped);
    assert!(
        errors.errors().iter().any(|error| error.path()
            == "fragments[412].cuts.runtime_filter_bindings[0]"
            && error
                .message()
                .starts_with("runtime-filter binding is out of numbering order")),
        "{errors}"
    );
    let mut missing = input;
    missing.cuts.runtime_filter_bindings = Box::default();
    let errors = refusal(missing);
    assert!(
        errors.errors().iter().any(|error| error.path()
            == "fragments[412].cuts.runtime_filter_bindings"
            && error.message() == "local producer 0 of runtime filter 410 has no binding"),
        "{errors}"
    );
}

fn parameter_fragment(reference: novarocks_type_contract::SemanticParameterRef) -> Fragment {
    parameter_occurrences_fragment(reference, 1)
}

// Proposal only: paste into package_contract.rs after reviewing the migration map.
// This fixed fixture owns exactly test.parameter.zero. It is not a function dispatcher.
#[derive(Clone, Copy)]
struct ParameterClaimSpec {
    reference: SemanticParameterRef,
    decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy,
}

fn parameter_occurrences_fragment(reference: SemanticParameterRef, occurrences: u32) -> Fragment {
    let claims = vec![
        ParameterClaimSpec {
            reference,
            decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        };
        occurrences as usize
    ];
    parameter_fragment_with_claims(reference, &claims, true)
}

fn parameter_fragment_with_claims(
    definition_reference: SemanticParameterRef,
    claims: &[ParameterClaimSpec],
    keep_legacy_metadata: bool,
) -> Fragment {
    use novarocks_expression_contract::{DefinitionControl, ExpressionCallClaim, RootScope};
    use novarocks_type_contract::CompileCheckpoints;
    assert!(!claims.is_empty());
    let mut builder = FragmentBuilder::new(FragmentId::new(81));
    let node = builder.reserve_node_id().unwrap();
    let value_type = ty(DataType::Int64, false);
    let legacy_metadata = keep_legacy_metadata.then(|| crate::LegacyBindingMetadata {
        volatility: FunctionVolatility::Stable,
        argument_evaluation: FunctionArgumentEvaluation::Eager,
        failure_behavior: FunctionFailureBehavior::Propagate,
        intrinsic_row_error: FunctionIntrinsicRowError::NoRowError,
        semantic_parameters: Box::from([definition_reference]),
    });
    let definition = builder
        .unit(node)
        .unwrap()
        .add(
            None,
            value_type.clone(),
            ExprKind::FunctionCall {
                function: BoundFunction {
                    function_id: FunctionId::try_new("test.parameter").unwrap(),
                    overload: FunctionOverloadId::try_new("test.parameter.zero").unwrap(),
                    kind: FunctionKind::Scalar,
                    argument_types: novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(
                        Box::default(),
                    ),
                    result_type: value_type.clone(),
                    legacy_metadata,
                },
                args: Box::default(),
            },
        )
        .unwrap();
    builder
        .unit(node)
        .unwrap()
        .definition_control(
            definition,
            DefinitionControl {
                shape: ControlShape::Eager,
                temporal_sources: None,
            },
        )
        .unwrap();
    // The original zero-argument request remains authored before sealing.
    // Keep its original source policy independently of the statement policy.
    builder
        .unit(node)
        .unwrap()
        .request(
            definition,
            novarocks_expression_contract::PhysicalCallRequest {
                arguments: Box::default(),
                logical_argument_count: 0,
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
                    max_library_validation_bytes: 1024 * 1024,
                },
            },
        )
        .unwrap();
    let mut roots = Vec::new();
    let mut outputs = Vec::new();
    for (ordinal, _) in claims.iter().enumerate() {
        roots.push(
            builder
                .unit(node)
                .unwrap()
                .root(definition, EvaluationDemand::Value, RootScope::AllPorts)
                .unwrap(),
        );
        outputs.push(
            builder
                .add_value(
                    value_type.clone(),
                    ValueOrigin::NodeOutput {
                        node,
                        output_ordinal: u32::try_from(ordinal).unwrap(),
                    },
                )
                .unwrap(),
        );
    }
    builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: singleton(),
            output: OutputPort {
                node,
                columns: outputs.into_boxed_slice(),
            },
            kind: NodeKind::Values {
                rows: Box::from([roots.into_boxed_slice()]),
            },
        })
        .unwrap();
    let ports = builder.node_ports(node).unwrap();
    let draft = builder.take_unit_builder(node).unwrap();
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Validate).unwrap();
    let sealed = draft.seal(ports, &mut work).unwrap();
    assert_eq!(sealed.root_use_ids().len(), claims.len());
    let authored = sealed
        .root_use_ids()
        .iter()
        .zip(claims)
        .map(|(&id, spec)| {
            let invocation = &sealed.uses()[&id];
            assert_eq!(invocation.definition, definition);
            assert!(invocation.arguments.is_empty());
            (
                id,
                ExpressionCallClaim {
                    context: invocation.context,
                    effects: parameter_fixture_effects(spec.reference),
                    decimal_overflow_policy: spec.decimal_overflow_policy,
                    temporal_source: None,
                    regexp_count_pattern_source: None,
                    to_base64_byte_source: None,
                },
            )
        })
        .collect();
    let unit = sealed.finish(authored, &mut work).unwrap();
    work.finish().unwrap();
    builder.install_unit(node, Arc::new(unit)).unwrap();
    builder
        .finish_definition(
            node,
            FragmentSink::Noop,
            dop(),
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap()
}

fn parameter_unit(fragment: &Fragment) -> &novarocks_expression_contract::BoundExprSet {
    fragment.nodes()[&fragment.root()]
        .expressions
        .as_deref()
        .unwrap()
}

// For valid claim/type changes, retain the actual previous unit as source custody.
// UnitParts revalidation remains mandatory; this helper cannot publish a malformed unit.
struct FixtureUnitSource {
    _source: Arc<novarocks_expression_contract::BoundExprSet>,
}
impl novarocks_type_contract::ValueTypeStorageHold for FixtureUnitSource {}

fn replace_fixture_unit(
    fragment: Fragment,
    node: NodeId,
    change: impl FnOnce(&mut novarocks_expression_contract::UnitParts),
) -> Result<Fragment, novarocks_expression_contract::UnitError> {
    let unit = candidate_fixture_unit(&fragment, node, change)?;
    let mut fragment = fragment.into_parts();
    fragment.nodes.get_mut(&node).unwrap().expressions = Some(Arc::new(unit));
    Ok(Fragment::from(fragment))
}

fn candidate_fixture_unit(
    fragment: &Fragment,
    node: NodeId,
    change: impl FnOnce(&mut novarocks_expression_contract::UnitParts),
) -> Result<novarocks_expression_contract::BoundExprSet, novarocks_expression_contract::UnitError> {
    use novarocks_expression_contract::{BoundExprSet, UnitLimits, UnitParts};
    use novarocks_type_contract::{CompileCheckpoints, ValueTypeStorageLifetime};
    let source = fragment.nodes()[&node]
        .expressions
        .as_ref()
        .unwrap()
        .clone();
    let mut parts = UnitParts::new(
        source.ports().to_vec().into_boxed_slice(),
        source.definitions().to_vec().into_boxed_slice(),
        source.roots().to_vec().into_boxed_slice(),
        source.root_use_ids().to_vec().into_boxed_slice(),
        source.control_flow().clone(),
        source.claims().clone(),
        source.requests().clone(),
    )
    .with_storage_lifetime(ValueTypeStorageLifetime::held(Arc::new(
        FixtureUnitSource { _source: source },
    )))?;
    change(&mut parts);
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Validate)?;
    let unit = BoundExprSet::try_from_parts_in(
        parts,
        UnitLimits {
            expression_semantic_depth: crate::PlanLimits::FROZEN.expression_semantic_depth,
        },
        &mut work,
    )?;
    work.finish()?;
    Ok(unit)
}

#[test]
fn package_and_observed_plan_require_original_static_requests_and_keep_first_control_cause() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let source = package_window_fragment();
    let definition = *source.call_requests().entries().keys().next().unwrap();
    let mut parts = source.into_parts();
    parts.call_requests = crate::FragmentCallRequests::unpublished_empty(parts.id);
    let fragment = Fragment::from(parts);
    let input = package_input(fragment.clone(), &mut fixture_constants);
    let expected = crate::CallRequestError::MissingDefinition(definition);
    assert_eq!(
        FragmentPackage::try_new(input.clone(), package_admission(), &Control).unwrap_err(),
        FragmentPackageError::Requests(expected.clone())
    );
    let observed_plan = |control: &dyn PureCompileControl| {
        let mut builder = PlanBuilder::new(version(), crate::tests::constant_fixture::policy());
        builder.add_fragment(fragment.clone()).unwrap();
        builder.finish_observed(control)
    };
    assert_eq!(
        observed_plan(&Control).unwrap_err(),
        crate::PlanConstructionError::Requests(expected)
    );
    let mut builder = PlanBuilder::new(version(), crate::tests::constant_fixture::policy());
    builder.add_fragment(fragment.clone()).unwrap();
    assert!(builder.finish().is_err());

    #[derive(Default)]
    struct Trace {
        calls: std::sync::Mutex<Vec<(CompilePhase, u32)>>,
        refusal: Option<(usize, CompileControlError)>,
    }
    impl PureCompileControl for Trace {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            let mut calls = self.calls.lock().unwrap();
            if let Some((at, _)) = self.refusal {
                assert!(calls.len() < at, "callback after the originating refusal");
            }
            calls.push((phase, units));
            match self.refusal.clone() {
                Some((at, cause)) if calls.len() == at => Err(cause),
                _ => Ok(()),
            }
        }
    }
    for package in [false, true] {
        let run = |control: &dyn PureCompileControl| {
            if package {
                match FragmentPackage::try_new(input.clone(), package_admission(), control) {
                    Err(FragmentPackageError::Control(cause)) => Some(cause),
                    Err(FragmentPackageError::Requests(_)) => None,
                    other => panic!("unexpected package verdict: {other:?}"),
                }
            } else {
                match observed_plan(control) {
                    Err(crate::PlanConstructionError::Constants(
                        crate::ConstantReferenceError::Control(cause),
                    )) => Some(cause),
                    Err(crate::PlanConstructionError::Requests(_)) => None,
                    other => panic!("unexpected plan verdict: {other:?}"),
                }
            }
        };
        let baseline = Trace::default();
        assert_eq!(run(&baseline), None);
        let calls = baseline.calls.into_inner().unwrap();
        for at in 1..=calls.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = Trace {
                    calls: Default::default(),
                    refusal: Some((at, cause.clone())),
                };
                assert_eq!(run(&control), Some(cause));
                assert_eq!(*control.calls.lock().unwrap(), calls[..at]);
            }
        }
    }
}

#[test]
fn extraction_requires_frozen_call_table_for_every_fragment_even_without_calls() {
    let mut constants = crate::tests::constant_fixture::ConstantFixture::new();
    for fragment in [
        literal_fragment(
            FragmentId::new(81),
            FragmentSink::Noop,
            false,
            &mut constants,
        )
        .0,
        package_window_fragment(),
    ] {
        let mut builder = PlanBuilder::new(version(), crate::tests::constant_fixture::policy());
        builder.add_fragment(fragment.clone()).unwrap();
        let plan = crate::tests::constant_fixture::publish_plan(builder, &constants).unwrap();
        let packages = extract(
            &plan,
            &BTreeMap::new(),
            &SemanticParameters::default(),
            &BTreeMap::new(),
        )
        .unwrap();
        assert_eq!(packages[&fragment.id()].calls(), fragment.calls());
        assert_eq!(
            packages[&fragment.id()].calls().entries().len(),
            usize::from(matches!(
                fragment.nodes()[&fragment.root()].kind,
                NodeKind::Window(_)
            ))
        );
    }
    // A detached foreign table cannot be installed even when both tables are empty.
    let (first, _) = literal_fragment(
        FragmentId::new(81),
        FragmentSink::Noop,
        false,
        &mut constants,
    );
    let (second, _) = literal_fragment(
        FragmentId::new(82),
        FragmentSink::Noop,
        false,
        &mut constants,
    );
    let mut parts = first.clone().into_parts();
    parts.calls = second.calls().clone();
    let mut input = package_input(first, &mut constants);
    input.fragment = Fragment::from(parts);
    assert_eq!(
        FragmentPackage::try_new(input, package_admission(), &Control).unwrap_err(),
        FragmentPackageError::Calls(FrozenCallError::WrongFragment)
    );
}

#[test]
fn package_empty_noncall_table_cannot_substitute_for_an_actual_call_occurrence() {
    let mut constants = crate::tests::constant_fixture::ConstantFixture::new();
    let fragment = package_window_fragment();
    let site = PhysicalCallSite::Window {
        node: fragment.root(),
        call: 0,
    };
    let (literal, _) = literal_fragment(fragment.id(), FragmentSink::Noop, false, &mut constants);
    let empty = FragmentPackage::try_new(
        package_input(literal.clone(), &mut constants),
        package_admission(),
        &Control,
    )
    .unwrap();
    assert!(empty.calls().entries().is_empty());
    let mut parts = fragment.clone().into_parts();
    parts.calls = literal.calls().clone();
    let mut input = package_input(fragment, &mut constants);
    input.fragment = Fragment::from(parts);
    assert_eq!(
        FragmentPackage::try_new(input, package_admission(), &Control).unwrap_err(),
        FragmentPackageError::Calls(FrozenCallError::MissingSite(site))
    );
}

#[test]
fn package_preserves_same_key_different_lexical_references_for_shared_definition_uses() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let first = SemanticParameterRef {
        id: SemanticParameterId::new(7),
        expected_key: SemanticParameterKey::TimeZone,
    };
    let second = SemanticParameterRef {
        id: SemanticParameterId::new(u32::MAX),
        expected_key: SemanticParameterKey::TimeZone,
    };
    let fragment = parameter_fragment_with_claims(
        first,
        &[
            ParameterClaimSpec {
                reference: first,
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            ParameterClaimSpec {
                reference: second,
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
        ],
        true,
    );
    let unit = parameter_unit(&fragment);
    let contexts = unit.control_flow().uses().values().collect::<Vec<_>>();
    assert_eq!(contexts[0].definition, contexts[1].definition);
    assert_ne!(contexts[0].context.use_id, contexts[1].context.use_id);
    let mut input = package_input(fragment.clone(), &mut fixture_constants);
    input.parameters = SemanticParameters::try_new([
        (
            first.id,
            SemanticParameterValue::TimeZone(
                novarocks_type_contract::TimeZoneKey::parse("UTC").unwrap(),
            ),
        ),
        (
            second.id,
            SemanticParameterValue::TimeZone(
                novarocks_type_contract::TimeZoneKey::parse("Asia/Shanghai").unwrap(),
            ),
        ),
    ])
    .unwrap();
    let checked = FragmentPackage::try_new(input.clone(), package_admission(), &Control).unwrap();
    assert_eq!(checked.parameters().entries().len(), 2);
    for (id, reference) in [first, second].into_iter().enumerate() {
        assert_eq!(
            parameter_unit(checked.fragment()).claims()
                [&parameter_unit(checked.fragment()).root_use_ids()[id]]
                .effects
                .environment
                .as_ref(),
            &[reference]
        );
    }
    let mut builder = PlanBuilder::new(version(), crate::tests::constant_fixture::policy())
        .with_semantic_parameters(input.parameters.clone());
    builder.add_fragment(fragment).unwrap();
    let plan = builder.finish_observed(&Control).unwrap();
    let packages = extract_fragment_packages(
        &fixture_plan_parameters(&plan, input.parameters.clone()),
        &BTreeMap::new(),
        &BTreeMap::new(),
        &fixture_pruning_tables(&plan),
        &package_admissions(&fixture_plan_parameters(&plan, input.parameters.clone())),
        &Control,
    )
    .unwrap();
    assert_eq!(
        packages[&FragmentId::new(81)].parameters(),
        checked.parameters()
    );

    let mut missing = input.clone();
    missing.parameters = SemanticParameters::try_new([(
        first.id,
        SemanticParameterValue::TimeZone(
            novarocks_type_contract::TimeZoneKey::parse("UTC").unwrap(),
        ),
    )])
    .unwrap();
    assert!(
        FragmentPackage::try_new(missing, package_admission(), &Control)
            .unwrap_err()
            .to_string()
            .contains("missing semantic parameter ID")
    );
    let mut wrong_key = input.clone();
    wrong_key.parameters = SemanticParameters::try_new([
        (
            first.id,
            SemanticParameterValue::TimeZone(
                novarocks_type_contract::TimeZoneKey::parse("UTC").unwrap(),
            ),
        ),
        (second.id, SemanticParameterValue::AllowThrowException(true)),
    ])
    .unwrap();
    assert!(
        FragmentPackage::try_new(wrong_key, package_admission(), &Control)
            .unwrap_err()
            .to_string()
            .contains("expected key")
    );
    input.parameters = SemanticParameters::try_new([
        (
            first.id,
            SemanticParameterValue::TimeZone(
                novarocks_type_contract::TimeZoneKey::parse("UTC").unwrap(),
            ),
        ),
        (
            second.id,
            SemanticParameterValue::TimeZone(
                novarocks_type_contract::TimeZoneKey::parse("Asia/Shanghai").unwrap(),
            ),
        ),
        (
            SemanticParameterId::new(8),
            SemanticParameterValue::TimeZone(
                novarocks_type_contract::TimeZoneKey::parse("Europe/London").unwrap(),
            ),
        ),
    ])
    .unwrap();
    assert!(
        FragmentPackage::try_new(input, package_admission(), &Control)
            .unwrap_err()
            .to_string()
            .contains("unused definitions")
    );
}

#[test]
fn frozen_call_environment_is_the_only_package_dependency_authority() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let legacy = SemanticParameterRef {
        id: SemanticParameterId::new(7),
        expected_key: SemanticParameterKey::TimeZone,
    };
    let active = SemanticParameterRef {
        id: SemanticParameterId::new(u32::MAX),
        expected_key: SemanticParameterKey::TimeZone,
    };
    let fragment = parameter_fragment_with_claims(
        legacy,
        &[ParameterClaimSpec {
            reference: active,
            decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        }],
        true,
    );
    let mut input = package_input(fragment.clone(), &mut fixture_constants);
    input.parameters = SemanticParameters::try_new([(
        active.id,
        SemanticParameterValue::TimeZone(
            novarocks_type_contract::TimeZoneKey::parse("Asia/Shanghai").unwrap(),
        ),
    )])
    .unwrap();
    let checked = FragmentPackage::try_new(input.clone(), package_admission(), &Control).unwrap();
    assert!(checked.parameters().entries().get(&legacy.id).is_none());
    let unit = parameter_unit(checked.fragment());
    let definition = &unit.definitions()[unit.control_flow().uses()[&unit.root_use_ids()[0]]
        .definition
        .get() as usize]
        .kind;
    let ExprKind::FunctionCall { function, .. } = definition else {
        unreachable!();
    };
    assert_eq!(
        function
            .legacy_metadata
            .as_ref()
            .unwrap()
            .semantic_parameters
            .as_ref(),
        &[legacy]
    );
    assert_eq!(
        unit.claims()[&unit.root_use_ids()[0]]
            .effects
            .environment
            .as_ref(),
        &[active]
    );

    let mut builder = PlanBuilder::new(version(), crate::tests::constant_fixture::policy())
        .with_semantic_parameters(input.parameters.clone());
    builder.add_fragment(fragment).unwrap();
    let plan = builder.finish_observed(&Control).unwrap();
    let snapshot = SemanticParameters::try_new([(
        active.id,
        SemanticParameterValue::TimeZone(
            novarocks_type_contract::TimeZoneKey::parse("Asia/Shanghai").unwrap(),
        ),
    )])
    .unwrap();
    let packages = extract_fragment_packages(
        &fixture_plan_parameters(&plan, snapshot.clone()),
        &BTreeMap::new(),
        &BTreeMap::new(),
        &fixture_pruning_tables(&plan),
        &package_admissions(&fixture_plan_parameters(&plan, snapshot.clone())),
        &Control,
    )
    .unwrap();
    assert_eq!(
        packages[&FragmentId::new(81)].parameters(),
        checked.parameters()
    );
    input.parameters = SemanticParameters::try_new([(
        legacy.id,
        SemanticParameterValue::TimeZone(
            novarocks_type_contract::TimeZoneKey::parse("UTC").unwrap(),
        ),
    )])
    .unwrap();
    assert!(
        FragmentPackage::try_new(input.clone(), package_admission(), &Control)
            .unwrap_err()
            .to_string()
            .contains("missing semantic parameter ID")
    );
    // Preserve the original negative extraction boundary: the raw source
    // table is deliberately wrong, so an earlier fixture unwrap cannot mask
    // the actual extraction owner's typed rejection.
    let malformed = fixture_parameter_input(&plan, input.parameters.clone());
    assert!(matches!(
        extract_fragment_packages(
            &malformed,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &fixture_pruning_tables(&malformed),
            &package_admissions(&malformed),
            &Control,
        ),
        Err(FragmentPackageExtractionError::Parameter(_))
    ));
}

#[test]
fn package_parameters_are_the_exact_call_dependency_closure() {
    use novarocks_type_contract::{SemanticParameterKey, SemanticParameterRef};
    let reference = SemanticParameterRef {
        id: SemanticParameterId::new(u32::MAX),
        expected_key: SemanticParameterKey::TimeZone,
    };
    let fragment = parameter_fragment(reference);
    let parameters = SemanticParameters::try_new([(
        reference.id,
        SemanticParameterValue::TimeZone(
            novarocks_type_contract::TimeZoneKey::parse("Asia/Shanghai").unwrap(),
        ),
    )])
    .unwrap();
    let mut builder = PlanBuilder::new(version(), crate::tests::constant_fixture::policy())
        .with_semantic_parameters(parameters.clone());
    builder.add_fragment(fragment).unwrap();
    let plan = builder.finish_observed(&Control).unwrap();
    let packages = extract(&plan, &BTreeMap::new(), &parameters, &BTreeMap::new()).unwrap();
    let mut input = packages[&FragmentId::new(81)].clone().into_input();
    assert_eq!(input.parameters.entries().len(), 1);
    assert_eq!(
        input.parameters.require(reference).unwrap(),
        &SemanticParameterValue::TimeZone(
            novarocks_type_contract::TimeZoneKey::parse("Asia/Shanghai").unwrap()
        )
    );
    input.parameters = SemanticParameters::try_new([
        (
            reference.id,
            SemanticParameterValue::TimeZone(
                novarocks_type_contract::TimeZoneKey::parse("Asia/Shanghai").unwrap(),
            ),
        ),
        (
            SemanticParameterId::new(1),
            SemanticParameterValue::TimeZone(
                novarocks_type_contract::TimeZoneKey::parse("UTC").unwrap(),
            ),
        ),
    ])
    .unwrap();
    let malformed = fixture_parameter_input(&plan, input.parameters.clone());
    assert!(matches!(
        extract_fragment_packages(
            &malformed,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &fixture_pruning_tables(&malformed),
            &package_admissions(&malformed),
            &Control,
        ),
        Err(FragmentPackageExtractionError::UnusedParameters)
    ));
    assert_eq!(
        FragmentPackage::try_new(input.clone(), package_admission(), &Control).unwrap_err(),
        FragmentPackageError::UnusedParameters
    );
    input.parameters = SemanticParameters::default();
    assert!(
        FragmentPackage::try_new(input.clone(), package_admission(), &Control)
            .unwrap_err()
            .to_string()
            .contains("missing semantic parameter ID")
    );
    input.parameters = SemanticParameters::try_new([(
        reference.id,
        SemanticParameterValue::AllowThrowException(true),
    )])
    .unwrap();
    assert!(
        FragmentPackage::try_new(input, package_admission(), &Control)
            .unwrap_err()
            .to_string()
            .contains("expected key")
    );
}

#[test]
fn package_cannot_carry_whole_plan_display_annotations() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let (fragment, _) = literal_fragment(
        FragmentId::new(81),
        FragmentSink::Noop,
        false,
        &mut fixture_constants,
    );
    let mut input = package_input(fragment, &mut fixture_constants);
    input.annotations = Box::from([PlanAnnotation {
        subject: AnnotationSubject::Plan,
        key: "optimizer.table_statistics".into(),
        value: "peer facts".into(),
    }]);
    assert!(FragmentPackage::try_new(input, package_admission(), &Control).is_err());
}

#[test]
fn writer_package_requires_the_exact_public_input_recipe() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    use novarocks_connector_contract::{
        ConnectorWriteBinding, ConnectorWriteFieldBinding, ConnectorWriteFieldToken,
        ConnectorWriteInputShape, ConnectorWriteRecipeDraft,
    };
    let plan = super::sink_contract::finish_router_writer_plan(
        super::sink_contract::RouterWriterShape::Valid,
        &mut fixture_constants,
    )
    .unwrap();
    let fragment = plan.fragments()[&FragmentId::new(722)].clone();
    let writer_id = fragment.root();
    let NodeKind::TableWriter { target } = &fragment.nodes()[&writer_id].kind else {
        unreachable!()
    };
    let read_binding = connector_binding();
    let binding = ConnectorWriteBinding::new(
        read_binding.descriptor().clone(),
        read_binding.catalog_handle().clone(),
    );
    let fields = target
        .target_fields
        .iter()
        .map(|field| {
            ConnectorWriteFieldBinding::new(
                field.token,
                arrow_schema::Field::new(
                    field.provider_name.as_ref(),
                    field.ty.data_type().clone(),
                    field.ty.nullable(),
                ),
            )
        })
        .collect::<Vec<_>>();
    let write = ConnectorWriteRecipeDraft::try_new(
        binding.clone(),
        target.handle.clone(),
        ConnectorWriteInputShape::Data {
            fields: fields.clone(),
        },
    )
    .unwrap();
    let mut input = package_input(fragment.clone(), &mut fixture_constants);
    input.cuts = fragment_cuts(&plan, fragment.id()).unwrap();
    assert!(
        FragmentPackage::try_new(input.clone(), package_admission(), &Control)
            .unwrap_err()
            .to_string()
            .contains("no complete frozen public facts")
    );
    input.writes.insert(writer_id, write.clone());
    FragmentPackage::try_new(input.clone(), package_admission(), &Control).unwrap();
    let mut wrong_node = input.clone();
    wrong_node.writes.insert(NodeId::new(u32::MAX), write);
    assert!(
        FragmentPackage::try_new(wrong_node, package_admission(), &Control)
            .unwrap_err()
            .to_string()
            .contains("missing or non-writer node")
    );
    for mutation in 0..3 {
        let field = &fields[0];
        let malformed = match mutation {
            0 => ConnectorWriteFieldBinding::new(
                ConnectorWriteFieldToken::from_bytes([8; 32]),
                field.field().clone(),
            ),
            1 => ConnectorWriteFieldBinding::new(
                field.token(),
                arrow_schema::Field::new("different", DataType::Int64, false),
            ),
            _ => ConnectorWriteFieldBinding::new(
                field.token(),
                arrow_schema::Field::new("v", DataType::Utf8, false),
            ),
        };
        let write = ConnectorWriteRecipeDraft::try_new(
            binding.clone(),
            target.handle.clone(),
            ConnectorWriteInputShape::Data {
                fields: vec![malformed],
            },
        )
        .unwrap();
        input.writes.insert(writer_id, write);
        assert!(
            FragmentPackage::try_new(input.clone(), package_admission(), &Control)
                .unwrap_err()
                .to_string()
                .contains("input field occurrence")
        );
    }
}

#[test]
fn writer_package_separates_declared_value_binding_from_provider_dictionary_facts() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    use novarocks_connector_contract::{
        ConnectorWriteBinding, ConnectorWriteFieldBinding, ConnectorWriteInputShape,
        ConnectorWriteRecipeDraft,
    };
    #[allow(deprecated)]
    let nested_type = |dictionary_id| {
        DataType::Struct(
            vec![arrow_schema::Field::new_dict(
                "dictionary",
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                false,
                dictionary_id,
                false,
            )]
            .into(),
        )
    };
    let plan = super::sink_contract::finish_router_writer_plan(
        super::sink_contract::RouterWriterShape::Valid,
        &mut fixture_constants,
    )
    .unwrap();
    let original = &plan.fragments()[&FragmentId::new(722)];
    let mut nodes = original.nodes().clone();
    let NodeKind::TableWriter { target } = &mut nodes.get_mut(&original.root()).unwrap().kind
    else {
        unreachable!()
    };
    let input_value = target.target_fields[0].input;
    let projected_value = target.target_fields[0].projected;
    target.target_fields[0].ty =
        ValueType::new(nested_type(1), target.target_fields[0].ty.nullable());
    let target = target.clone();
    let mut values = original.values().clone();
    let value = values.get_mut(&input_value).unwrap();
    value.ty = ValueType::new(nested_type(1), value.ty.nullable());
    let projected = values.get_mut(&projected_value).unwrap();
    projected.ty = target.target_fields[0].ty.clone();
    let fragment = Fragment::from(crate::plan::FragmentParts {
        summaries: Box::default(),
        stage_placement: crate::StagePlacementPolicy::Singleton,
        id: original.id(),
        root: original.root(),
        values,

        nodes,
        sink: original.sink().clone(),
        dop_domain: original.dop_domain(),
        runtime_filters: original.runtime_filters().into(),
        call_requests: original.call_requests().clone(),
        calls: original.calls().clone(),
    });
    let mut input = package_input(fragment, &mut fixture_constants);
    input.cuts = fragment_cuts(&plan, original.id()).unwrap();
    for cut in &mut input.cuts.inbound {
        for import in &mut cut.imports {
            if import.destination == input_value {
                import.source.ty = ValueType::new(nested_type(1), import.source.ty.nullable());
            }
        }
    }
    let read_binding = connector_binding();
    let binding = ConnectorWriteBinding::new(
        read_binding.descriptor().clone(),
        read_binding.catalog_handle().clone(),
    );
    for id in [1, 2] {
        let fields = target
            .target_fields
            .iter()
            .map(|field| {
                ConnectorWriteFieldBinding::try_new_typed(
                    field.token,
                    arrow_schema::Field::new(
                        field.provider_name.as_ref(),
                        nested_type(id),
                        field.ty.nullable(),
                    ),
                    field.ty.clone(),
                )
                .unwrap()
            })
            .collect();
        let recipe = ConnectorWriteRecipeDraft::try_new(
            binding.clone(),
            target.handle.clone(),
            ConnectorWriteInputShape::Data { fields },
        )
        .unwrap();
        input.writes.insert(original.root(), recipe);
        let result = FragmentPackage::try_new(input.clone(), package_admission(), &Control);
        result.unwrap();
    }
}

#[test]
fn extraction_preserves_per_occurrence_decimal_policy_and_package_receipt_identity() {
    use novarocks_type_contract::DecimalOverflowPolicy::{OutputNull, ReportError};
    let mut constants = crate::tests::constant_fixture::ConstantFixture::new();
    let reference = SemanticParameterRef {
        id: SemanticParameterId::new(7),
        expected_key: SemanticParameterKey::TimeZone,
    };
    let fragment = parameter_fragment_with_claims(
        reference,
        &[
            ParameterClaimSpec {
                reference,
                decimal_overflow_policy: ReportError,
            },
            ParameterClaimSpec {
                reference,
                decimal_overflow_policy: OutputNull,
            },
        ],
        true,
    );
    let parameters = SemanticParameters::try_new([(
        reference.id,
        SemanticParameterValue::TimeZone(
            novarocks_type_contract::TimeZoneKey::parse("UTC").unwrap(),
        ),
    )])
    .unwrap();
    let mut input = package_input(fragment.clone(), &mut constants);
    input.parameters = parameters.clone();
    let checked = FragmentPackage::try_new(input.clone(), package_admission(), &Control).unwrap();
    let unit = parameter_unit(checked.fragment());
    let first = unit.root_use_ids()[0];
    let second = unit.root_use_ids()[1];
    assert_eq!(unit.claims()[&first].decimal_overflow_policy, ReportError);
    assert_eq!(unit.claims()[&second].decimal_overflow_policy, OutputNull);
    assert_eq!(
        unit.control_flow().uses()[&first].definition,
        unit.control_flow().uses()[&second].definition
    );
    let changed_fragment = parameter_occurrences_fragment(reference, 2);
    let mut changed = package_input(changed_fragment, &mut constants);
    changed.parameters = parameters.clone();
    let changed = FragmentPackage::try_new(changed, package_admission(), &Control).unwrap();
    assert_eq!(
        unit.control_flow(),
        parameter_unit(changed.fragment()).control_flow()
    );
    assert_eq!(
        unit.claims()[&first].effects,
        parameter_unit(changed.fragment()).claims()[&first].effects
    );
    assert_ne!(unit.claims(), parameter_unit(changed.fragment()).claims());
    assert_ne!(checked.fragment(), changed.fragment());
    let mut builder = PlanBuilder::new(version(), crate::tests::constant_fixture::policy())
        .with_semantic_parameters(parameters.clone());
    builder.add_fragment(fragment.clone()).unwrap();
    let plan = builder.finish_observed(&Control).unwrap();
    let packages = extract(&plan, &BTreeMap::new(), &parameters, &BTreeMap::new()).unwrap();
    assert_eq!(
        parameter_unit(packages[&fragment.id()].fragment()).claims(),
        unit.claims()
    );
    assert_no_constant_package_equal(&packages[&fragment.id()], &checked);
}

fn intrinsic_package_fixture(
    fixture_constants: &mut crate::tests::constant_fixture::ConstantFixture,
) -> FragmentPackageInput {
    let mut builder = FragmentBuilder::new(FragmentId::new(91));
    let node = builder.reserve_node_id().unwrap();
    let left = builder
        .define(
            node,
            (ty(DataType::Int64, false)).clone(),
            fixture_constants.i64(4, &(ty(DataType::Int64, false))),
        )
        .unwrap();
    let right = builder
        .define(
            node,
            (ty(DataType::Int64, false)).clone(),
            fixture_constants.i64(2, &(ty(DataType::Int64, false))),
        )
        .unwrap();
    let arithmetic = builder
        .define(
            node,
            ty(DataType::Int64, false),
            ExprKind::Binary {
                left,
                op: BinaryOperator::Add,
                right,
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                allow_throw_exception: Some(intrinsic_reference(0)),
            },
        )
        .unwrap();
    let cast = builder
        .define(
            node,
            ty(DataType::Int64, false),
            ExprKind::Cast {
                expr: arithmetic,
                target: DataType::Int64,
                decimal_overflow_policy:
                    novarocks_type_contract::DecimalOverflowPolicy::ReportError,
                allow_throw_exception: intrinsic_reference(u32::MAX),
                array_failure_mode: None,
            },
        )
        .unwrap();
    let output = builder
        .add_value(
            ty(DataType::Int64, false),
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 0,
            },
        )
        .unwrap();
    let root = builder.root_definition(
        node,
        cast,
        EvaluationDemand::Value,
        crate::RootScope::AllPorts,
    );
    builder
        .add_values(node, Box::from([Box::from([root])]), Box::from([output]))
        .unwrap();
    let fragment = builder
        .finish_fixture_definition(
            node,
            FragmentSink::Noop,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let mut input = package_input(fragment, fixture_constants);
    input.parameters = SemanticParameters::try_new([
        (
            SemanticParameterId::new(0),
            SemanticParameterValue::AllowThrowException(false),
        ),
        (
            SemanticParameterId::new(u32::MAX),
            SemanticParameterValue::AllowThrowException(true),
        ),
    ])
    .unwrap();
    input
}

#[test]
fn intrinsic_arithmetic_and_cast_use_the_only_plan_table_with_sparse_false_true_scopes() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let input = intrinsic_package_fixture(&mut fixture_constants);
    let checked = FragmentPackage::try_new(input.clone(), package_admission(), &Control).unwrap();
    assert_eq!(
        checked
            .parameters()
            .require(intrinsic_reference(0))
            .unwrap(),
        &SemanticParameterValue::AllowThrowException(false)
    );
    assert_eq!(
        checked
            .parameters()
            .require(intrinsic_reference(u32::MAX))
            .unwrap(),
        &SemanticParameterValue::AllowThrowException(true)
    );
    let mut builder = PlanBuilder::new(version(), crate::tests::constant_fixture::policy())
        .with_semantic_parameters(input.parameters.clone());
    builder.add_fragment(input.fragment).unwrap();
    let plan = crate::tests::constant_fixture::publish_plan(builder, &fixture_constants).unwrap();
    let packages = extract_fragment_packages(
        &plan,
        &BTreeMap::new(),
        &BTreeMap::new(),
        &fixture_pruning_tables(&plan),
        &package_admissions(&plan),
        &Control,
    )
    .unwrap();
    assert_no_constant_package_equal(&packages[&checked.fragment().id()], &checked);
}

#[test]
fn intrinsic_reference_closure_rejects_missing_wrong_key_and_unconsumed_values() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let input = intrinsic_package_fixture(&mut fixture_constants);
    let mut missing = input.clone();
    missing.parameters = SemanticParameters::default();
    assert_eq!(
        FragmentPackage::try_new(missing, package_admission(), &Control).unwrap_err(),
        FragmentPackageError::Parameter(
            novarocks_type_contract::SemanticParameterError::MissingId(SemanticParameterId::new(0))
        )
    );
    let mut wrong = input.clone();
    wrong.parameters = SemanticParameters::try_new([
        (
            SemanticParameterId::new(0),
            SemanticParameterValue::AllowThrowException(false),
        ),
        (
            SemanticParameterId::new(u32::MAX),
            SemanticParameterValue::GroupConcatLegacy(true),
        ),
    ])
    .unwrap();
    assert_eq!(
        FragmentPackage::try_new(wrong, package_admission(), &Control).unwrap_err(),
        FragmentPackageError::Parameter(
            novarocks_type_contract::SemanticParameterError::KeyMismatch(intrinsic_reference(
                u32::MAX
            ))
        )
    );
    let mut extra = input.clone();
    extra.parameters = SemanticParameters::try_new(
        input
            .parameters
            .entries()
            .iter()
            .map(|(id, value)| (*id, value.clone()))
            .chain([(
                SemanticParameterId::new(7),
                SemanticParameterValue::AllowThrowException(false),
            )]),
    )
    .unwrap();
    assert_eq!(
        FragmentPackage::try_new(extra.clone(), package_admission(), &Control).unwrap_err(),
        FragmentPackageError::UnusedParameters
    );
    let mut builder = PlanBuilder::new(version(), crate::tests::constant_fixture::policy())
        .with_semantic_parameters(extra.parameters);
    builder.add_fragment(extra.fragment).unwrap();
    let plan = crate::tests::constant_fixture::publish_plan(builder, &fixture_constants).unwrap();
    assert_eq!(
        extract_fragment_packages(
            &plan,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &fixture_pruning_tables(&plan),
            &package_admissions(&plan),
            &Control,
        )
        .unwrap_err(),
        FragmentPackageExtractionError::UnusedParameters
    );
    // Whole-plan publication cannot leave even an unused-runtime definition's
    // intrinsic reference dangling. No external extraction table can repair it.
    let mut missing_plan = PlanBuilder::new(version(), crate::tests::constant_fixture::policy());
    missing_plan.add_fragment(input.fragment).unwrap();
    assert!(
        crate::tests::constant_fixture::publish_plan(missing_plan, &fixture_constants)
            .unwrap_err()
            .errors()
            .iter()
            .any(|error| error.message().contains("missing semantic parameter ID"))
    );
}

#[test]
fn intrinsic_operator_profiles_refuse_missing_extra_or_foreign_key_refs() {
    let mut constants = crate::tests::constant_fixture::ConstantFixture::new();
    let input = intrinsic_package_fixture(&mut constants);
    for mutation in 0..3 {
        let error = candidate_fixture_unit(&input.fragment, input.fragment.root(), |parts| {
            let expression = parts
                .definitions
                .iter_mut()
                .find(|expression| {
                    if mutation == 2 {
                        matches!(expression.kind, ExprKind::Cast { .. })
                    } else {
                        matches!(expression.kind, ExprKind::Binary { .. })
                    }
                })
                .unwrap();
            match &mut expression.kind {
                ExprKind::Binary {
                    allow_throw_exception,
                    ..
                } => match mutation {
                    0 => *allow_throw_exception = None,
                    _ => {
                        *allow_throw_exception = Some(SemanticParameterRef {
                            id: SemanticParameterId::new(0),
                            expected_key: SemanticParameterKey::TimeZone,
                        })
                    }
                },
                ExprKind::Cast {
                    allow_throw_exception,
                    ..
                } => allow_throw_exception.expected_key = SemanticParameterKey::TimeZone,
                _ => unreachable!(),
            }
        })
        .unwrap_err();
        assert!(
            matches!(
                error,
                novarocks_expression_contract::UnitError::Invalid { .. }
            ),
            "{error:?}"
        );
        assert!(
            error.to_string().contains("ALLOW_THROW_EXCEPTION"),
            "{error}"
        );
    }
}

#[test]
fn parameter_free_actual_definitions_keep_original_control_at_every_package_boundary() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let mut builder = FragmentBuilder::new(FragmentId::new(92));
    let node = builder.reserve_node_id().unwrap();
    let mut row = Vec::new();
    let mut outputs = Vec::new();
    for ordinal in 0..320 {
        let definition = builder
            .define(
                node,
                ty(DataType::Int64, false),
                fixture_constants.i64(ordinal, &ty(DataType::Int64, false)),
            )
            .unwrap();
        row.push(builder.root_definition(
            node,
            definition,
            EvaluationDemand::Value,
            crate::RootScope::AllPorts,
        ));
        outputs.push(
            builder
                .add_value(
                    ty(DataType::Int64, false),
                    ValueOrigin::NodeOutput {
                        node,
                        output_ordinal: ordinal as u32,
                    },
                )
                .unwrap(),
        );
    }
    builder
        .add_values(
            node,
            Box::from([row.into_boxed_slice()]),
            outputs.into_boxed_slice(),
        )
        .unwrap();
    let fragment = builder
        .finish_fixture_definition(
            node,
            FragmentSink::Noop,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let input = package_input(fragment, &mut fixture_constants);
    assert_eq!(parameter_unit(&input.fragment).definitions().len(), 320);
    assert!(input.parameters.entries().is_empty());
    struct Trace {
        calls: std::sync::Mutex<Vec<u32>>,
        refusal: Option<(usize, CompileControlError)>,
    }
    impl PureCompileControl for Trace {
        fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            assert!(units <= novarocks_type_contract::MAX_UNOBSERVED_COMPILE_WORK);
            let mut calls = self.calls.lock().unwrap();
            calls.push(units);
            match self.refusal.clone() {
                Some((index, error)) if calls.len() == index + 1 => Err(error),
                _ => Ok(()),
            }
        }
    }
    let good = Trace {
        calls: Default::default(),
        refusal: None,
    };
    FragmentPackage::try_new(input.clone(), package_admission(), &good).unwrap();
    let trace = good.calls.into_inner().unwrap();
    // The original entry is observed before the constant/resource and call
    // passes. Those independently observed passes may add earlier callbacks;
    // their positions are not a contract for the parameter count pass.
    assert_eq!(trace.first(), Some(&0));
    // Both actual 320-definition walks remain observed. Node/unit bookkeeping
    // can move their tails; a complete 256-unit quantum is still bounded and visible.
    assert!(
        trace
            .iter()
            .filter(|&&units| units == novarocks_type_contract::MAX_UNOBSERVED_COMPILE_WORK)
            .count()
            >= 2
    );
    for index in 0..trace.len() {
        for error in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Trace {
                calls: Default::default(),
                refusal: Some((index, error.clone())),
            };
            assert_eq!(
                FragmentPackage::try_new(input.clone(), package_admission(), &control).unwrap_err(),
                FragmentPackageError::Control(error)
            );
            assert_eq!(*control.calls.lock().unwrap(), trace[..=index]);
        }
    }
}

#[test]
fn extraction_reference_preallocation_uses_the_existing_fragment_profile_boundary() {
    let fragment = parameter_fragment(SemanticParameterRef {
        id: SemanticParameterId::new(0),
        expected_key: SemanticParameterKey::TimeZone,
    });
    let mut errors = crate::validation::ValidationContext::new();
    let mut usage = crate::resource::CutResourcePreflight::new();
    usage.add_fragment(&fragment, &mut errors);
    let actual = usage.validate("fixture.resources", &mut errors);
    assert!(errors.is_empty());
    let remaining = crate::MAX_FRAGMENT_DYNAMIC_ITEMS - actual.items;
    // Counts come from the allocation-free observed walk. Test the real
    // preallocation gate without allocating millions of repeated references.
    crate::validation::validate_fragment_parameter_resource_usage(
        &fragment,
        PlanLimits::FROZEN,
        remaining,
    )
    .unwrap();
    for count in [remaining + 1, usize::MAX] {
        let errors = crate::validation::validate_fragment_parameter_resource_usage(
            &fragment,
            PlanLimits::FROZEN,
            count,
        )
        .unwrap_err();
        assert!(errors.errors().iter().any(|error| {
            error.path() == "package.resources" && error.message().contains("dynamic items")
        }));
    }
}

// Explicit small-fixture source invoice and independent property projection
// ceilings. These are test inputs, not a production default or MEM grant.
fn package_admission() -> crate::FragmentPackageAdmission {
    crate::FragmentPackageAdmission {
        plan_limits: crate::PlanLimits::FROZEN,
        source_retained_bytes: 64 * 1024 * 1024,
        property_projection_limits: crate::PropertyProofProjectionLimits {
            max_request_bytes: 16 * 1024 * 1024,
            max_coexisting_bytes: 256 * 1024 * 1024,
            max_projection_work: 16 * 1024 * 1024,
        },
    }
}

fn package_admissions(
    plan: &crate::PhysicalPlan,
) -> std::collections::BTreeMap<crate::FragmentId, crate::FragmentPackageAdmission> {
    plan.fragments()
        .keys()
        .map(|id| (*id, package_admission()))
        .collect()
}

#[test]
fn exact_signature_without_legacy_metadata_uses_fresh_frozen_package_authority() {
    let mut constants = crate::tests::constant_fixture::ConstantFixture::new();
    let reference = SemanticParameterRef {
        id: SemanticParameterId::new(7),
        expected_key: SemanticParameterKey::TimeZone,
    };
    let fragment = parameter_fragment_with_claims(
        reference,
        &[ParameterClaimSpec {
            reference,
            decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        }],
        false,
    );
    let unit = parameter_unit(&fragment);
    let definition = &unit.definitions()[0];
    let ExprKind::FunctionCall { function, .. } = &definition.kind else {
        unreachable!()
    };
    assert_eq!(
        function.require_legacy_metadata(),
        Err(MissingLegacyBindingMetadata)
    );
    assert_eq!(unit.requests().len(), 1);
    assert_eq!(unit.claims().len(), 1);
    validate_fragment(&fragment, &FragmentCuts::default()).unwrap();
    let mut input = package_input(fragment, &mut constants);
    input.parameters = SemanticParameters::try_new([(
        reference.id,
        SemanticParameterValue::TimeZone(
            novarocks_type_contract::TimeZoneKey::parse("UTC").unwrap(),
        ),
    )])
    .unwrap();
    let checked = FragmentPackage::try_new(input.clone(), package_admission(), &Control).unwrap();
    assert_eq!(parameter_unit(checked.fragment()).claims().len(), 1);
    assert!(checked.calls().entries().is_empty());
    assert_eq!(checked.parameters().entries().len(), 1);
    let ExprKind::FunctionCall { function, .. } =
        &parameter_unit(checked.fragment()).definitions()[0].kind
    else {
        unreachable!()
    };
    assert!(function.legacy_metadata.is_none());
    let error = candidate_fixture_unit(&input.fragment, input.fragment.root(), |parts| {
        parts
            .claims
            .values_mut()
            .next()
            .unwrap()
            .effects
            .own_row_error = FunctionIntrinsicRowError::NotRowEvaluated;
    })
    .unwrap_err();
    assert!(
        matches!(error, novarocks_expression_contract::UnitError::Effects(_)),
        "{error:?}"
    );
}

mod borrowed_constructor_tests {
    include!("package_contract/borrowed_constructor_tests.rs");
}

fn package_window_fragment() -> Fragment {
    use novarocks_type_contract::{RelationCallId, RelationEffectContext};
    let mut builder = FragmentBuilder::new(FragmentId::new(81));
    let input = builder.reserve_node_id().unwrap();
    builder
        .add_values(input, Box::default(), Box::default())
        .unwrap();
    let node = builder.reserve_node_id().unwrap();
    let value_type = ty(DataType::Int64, false);
    let output = builder
        .add_value(
            value_type.clone(),
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 0,
            },
        )
        .unwrap();
    let binding = BoundFunction::from_exact_signature(
        FunctionId::try_new("test.package.window").unwrap(),
        FunctionOverloadId::try_new("test.package.window.zero").unwrap(),
        FunctionKind::Window,
        novarocks_type_contract::TypedFactSlice::from_pure_boxed(Box::default()),
        value_type,
    );
    let properties = builder.node_output_properties(input).unwrap().clone();
    builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: node,
            inputs: Box::from([input]),
            required_inputs: Box::from([properties.clone()]),
            output_properties: properties,
            output: OutputPort {
                node,
                columns: Box::from([output]),
            },
            kind: NodeKind::Window(WindowSpec {
                partition_by: Box::default(),
                order_by: Box::default(),
                calls: Box::from([WindowCallSpec {
                    binding,
                    distinct: false,
                    arguments: Box::default(),
                    order_by: Box::default(),
                    frame: None,
                    ignore_nulls: false,
                    aggregate_binding: None,
                    output,
                }]),
            }),
        })
        .unwrap();
    let fragment = builder
        .finish_structure(
            node,
            FragmentSink::Noop,
            dop(),
            PlanLimits::FROZEN,
            &Control,
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let site = PhysicalCallSite::Window { node, call: 0 };
    let context = RelationEffectContext::call(RelationCallId::new(71));
    let fragment = fragment
        .with_call_requests_observed(
            vec![(
                site,
                crate::PhysicalCallRequest {
                    arguments: Box::default(),
                    logical_argument_count: 0,
                    expected_result_type: None,
                    constant_policy: crate::tests::constant_fixture::policy(),
                },
            )],
            &Control,
        )
        .unwrap();
    fragment
        .with_calls_observed(
            vec![FrozenPhysicalCall {
                site,
                context,
                materialized_writer_input: None,
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                effects: CallEffects {
                    value_stability: FunctionVolatility::Immutable,
                    own_row_error: FunctionIntrinsicRowError::NotRowEvaluated,
                    failure_behavior: FunctionFailureBehavior::Propagate,
                    null_behavior: FunctionNullBehavior::CalledOnNull,
                    argument_control: ArgumentControl::Window,
                    instance_state: FunctionInstanceState::WindowPartition,
                    observable_effects: ObservableEffects::NONE,
                    environment: Box::default(),
                    proof_scope: CallProofScope::Relation(context.identity),
                },
            }],
            &Control,
        )
        .unwrap()
}
