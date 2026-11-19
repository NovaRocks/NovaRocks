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

use super::all_property_derivation_tests::{
    exchange, frozen_fixture, replace_properties, single_copy_dag, unsafe_filter,
};
use super::*;
use arrow_schema::Field;
use novarocks_type_contract::{SemanticParameterValue, SemanticParameters};

const SOURCE: usize = 4 * 1024 * 1024;
const PROJECTION: PropertyProofProjectionLimits = PropertyProofProjectionLimits {
    max_request_bytes: 1024 * 1024,
    max_coexisting_bytes: 8 * 1024 * 1024,
    max_projection_work: 1024 * 1024,
};
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct FinalControl {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for FinalControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Validate);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop, "callback after original finalization refusal");
        }
        trace.push(units);
        match self.refusal.clone() {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}
fn admission() -> FragmentPackageAdmission {
    FragmentPackageAdmission {
        plan_limits: PlanLimits::FROZEN,
        source_retained_bytes: SOURCE,
        property_projection_limits: PROJECTION,
    }
}
fn input(fixture: Fixture, cuts: FragmentCuts) -> FragmentPackageInput {
    fixture.checked().unwrap();
    let pruning =
        FrozenFragmentPruning::try_new(fixture.fragment.id(), vec![], &Control::default()).unwrap();
    FragmentPackageInput {
        constants: ConstantPools::empty(),
        statement_constant_policy: policy(),
        version: PlanVersionId::try_new([17; 16]).unwrap(),
        required: RequiredContracts::default(),
        fragment: fixture.fragment,
        pruning,
        cuts,
        result: None,
        parameters: SemanticParameters::default(),
        scans: BTreeMap::new(),
        writes: BTreeMap::new(),
        annotations: Box::default(),
        layouts: Default::default(),
    }
}
fn finish(
    input: &FragmentPackageInput,
    control: &FinalControl,
) -> Result<FragmentPackage, FragmentPackageError> {
    // Replay fixture clones are created before the tested constructor sees its
    // original control. The production constructor consumes this source once.
    FragmentPackage::try_new_with_derived_properties(input.clone(), admission(), control)
}
fn derive(input: &FragmentPackageInput) {
    derive_fragment_output_properties_observed(
        &input.fragment,
        &input.cuts,
        PlanLimits::FROZEN,
        SOURCE,
        PROJECTION,
        &Control::default(),
    )
    .unwrap();
}
fn prefixes(input: &FragmentPackageInput, succeeds: bool, all: bool) {
    let baseline = FinalControl::default();
    assert_eq!(finish(input, &baseline).is_ok(), succeeds);
    let trace = baseline.trace.lock().unwrap().clone();
    assert_eq!(trace.first(), Some(&0));
    assert!(trace.len() > 1);
    for (at, units) in trace.iter().enumerate() {
        if !all && at != 0 && at + 1 != trace.len() && *units != 256 {
            continue;
        }
        for cause in CAUSES {
            let control = FinalControl {
                trace: Mutex::new(vec![]),
                refusal: Some((at, cause.clone())),
            };
            assert!(
                matches!(finish(input, &control), Err(FragmentPackageError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
}
fn broadcast() -> PhysicalProperties {
    PhysicalProperties {
        distribution: Distribution::Broadcast,
        row_multiplicity: RowMultiplicity::Replicated,
        ordering: Box::default(),
    }
}
fn unconstrained(multiplicity: RowMultiplicity) -> PhysicalProperties {
    PhysicalProperties {
        distribution: Distribution::Unconstrained,
        row_multiplicity: multiplicity,
        ordering: Box::default(),
    }
}

#[test]
fn owned_finalization_repairs_filter_project_repeat_keeps_source_calls_dop_and_fresh_proof() {
    let source = input(unsafe_filter(), FragmentCuts::default());
    assert!(
        matches!(FragmentPackage::try_new(source.clone(),admission(),&Control::default()),Err(FragmentPackageError::Calls(FrozenCallError::ReplicaExpression{node,invocation})) if node==NodeId::new(0)&&invocation==ExpressionUseId::new(u32::MAX))
    );
    let original_calls = source.fragment.calls().entries().clone();
    let original_dop = source.fragment.dop_domain();
    let original_unit = Arc::clone(
        source.fragment.nodes()[&NodeId::new(0)]
            .expressions
            .as_ref()
            .unwrap(),
    );
    let original_roots = original_unit.root_use_ids().to_vec();
    let original_exprs = original_unit.definitions().to_vec();
    let original_claims = original_unit.claims().clone();
    let original_requests = original_unit.requests().clone();
    let package = finish(&source, &FinalControl::default()).unwrap();
    assert_eq!(package.fragment().dop_domain(), original_dop);
    assert_eq!(package.calls().entries(), &original_calls);
    let current_unit = package.fragment().nodes()[&NodeId::new(0)]
        .expressions
        .as_ref()
        .unwrap();
    // The transformed property snapshot retains the original immutable unit Arc,
    // including its actual construction lifetime and recursive type/CV sources.
    assert!(Arc::ptr_eq(current_unit, &original_unit));
    assert_eq!(current_unit.root_use_ids(), original_roots.as_slice());
    assert_eq!(current_unit.claims(), &original_claims);
    assert_eq!(current_unit.requests(), &original_requests);
    assert_eq!(
        package.fragment().nodes()[&NodeId::new(u32::MAX)].output_properties,
        broadcast()
    );
    for id in [0, 7, 1] {
        assert_eq!(
            package.fragment().nodes()[&NodeId::new(id)].output_properties,
            unconstrained(RowMultiplicity::Replicated)
        );
        assert_eq!(
            source.fragment.nodes()[&NodeId::new(id)].output_properties,
            broadcast()
        );
    }
    for original in original_exprs {
        let current = current_unit.definition(original.id).unwrap();
        assert_eq!(current.id, original.id);
        assert_eq!(current.ty, original.ty);
        assert_eq!(current.lambda_scope, original.lambda_scope);
        let (
            ExprKind::FunctionCall {
                function: original, ..
            },
            ExprKind::FunctionCall {
                function: current, ..
            },
        ) = (&original.kind, &current.kind)
        else {
            unreachable!()
        };
        assert_eq!(current, original);
    }
    let control = FinalControl::default();
    let old_proof = source
        .fragment
        .calls()
        .property_proof(
            &source.fragment,
            &PlanLimits::FROZEN,
            SOURCE,
            PROJECTION,
            &control,
        )
        .unwrap();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    assert_eq!(
        old_proof.require_fragment(package.fragment(), &mut work),
        Err(FrozenCallError::WrongFragment)
    );
    work.finish().unwrap();
    let fresh = package
        .calls()
        .property_proof(
            package.fragment(),
            &PlanLimits::FROZEN,
            SOURCE,
            PROJECTION,
            &control,
        )
        .unwrap();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    fresh
        .require_fragment(package.fragment(), &mut work)
        .unwrap();
    assert!(!fresh.replica_safe(NodeId::new(0), &mut work).unwrap());
    assert!(
        fresh
            .replica_safe(NodeId::new(u32::MAX), &mut work)
            .unwrap()
    );
    work.finish().unwrap();
    prefixes(&source, true, true);
}

#[test]
fn owned_finalization_shared_sparse_union_hash_join_stronger_candidate_is_fully_admitted() {
    let source = input(single_copy_dag(), FragmentCuts::default());
    let package = finish(&source, &FinalControl::default()).unwrap();
    assert_eq!(package.fragment().nodes().len(), 5);
    for id in [u32::MAX, 0, 7, 1, 2] {
        assert_eq!(
            package.fragment().nodes()[&NodeId::new(id)].output_properties,
            properties()
        );
    }
    assert_eq!(
        source.fragment.nodes()[&NodeId::new(0)].output_properties,
        unconstrained(RowMultiplicity::SingleCopy)
    );
    assert_eq!(
        package.fragment().nodes()[&NodeId::new(1)].inputs.as_ref(),
        &[NodeId::new(0), NodeId::new(7), NodeId::new(0)]
    );
    assert_eq!(package.calls().entries(), source.fragment.calls().entries());
    prefixes(&source, true, true);
}

#[test]
fn owned_finalization_exchange_cut_anchors_new_snapshot_and_remains_unchanged() {
    let (fixture, cuts) = exchange();
    let source = input(fixture, cuts);
    let package = finish(&source, &FinalControl::default()).unwrap();
    assert_eq!(
        package.fragment().nodes()[&NodeId::new(u32::MAX)].output_properties,
        PhysicalProperties {
            distribution: Distribution::RoundRobin,
            row_multiplicity: RowMultiplicity::SingleCopy,
            ordering: Box::default()
        }
    );
    assert_eq!(
        source.fragment.nodes()[&NodeId::new(u32::MAX)].output_properties,
        properties()
    );
    assert_eq!(package.cuts(), &source.cuts);
    prefixes(&source, true, true);
}

#[test]
fn owned_finalization_required_broadcast_refusal_preserves_original_typed_tail() {
    let mut fixture = unsafe_filter();
    replace_properties(&mut fixture, |parts| {
        parts
            .nodes
            .get_mut(&NodeId::new(7))
            .unwrap()
            .required_inputs[0] = broadcast();
    });
    let source = input(fixture, FragmentCuts::default());
    assert!(matches!(
        finish(&source, &FinalControl::default()),
        Err(FragmentPackageError::Structure(_))
    ));
    assert_eq!(
        source.fragment.nodes()[&NodeId::new(0)].output_properties,
        broadcast()
    );
    prefixes(&source, false, true);
}

fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 1024,
        max_array_nodes: 4096,
        max_logical_elements: 1_000_000,
        max_retained_buffer_bytes: 16_777_216,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 16,
        max_metadata_bytes: 1_048_576,
        max_library_validation_work: 67_108_864,
        max_library_validation_bytes: 67_108_864,
    }
}

#[test]
fn owned_finalization_runs_final_package_constants_parameters_pruning_contract_and_cuts() {
    for fault in 0..5 {
        let mut source = input(unsafe_filter(), FragmentCuts::default());
        match fault {
            0 => {
                let value = ConstantValue::from_i64(
                    Arc::new(Field::new("unused", DataType::Int64, false)),
                    integer(),
                    31,
                    policy(),
                    CompilePhase::Validate,
                    &Control::default(),
                )
                .unwrap();
                source
                    .constants
                    .insert(ConstantPoolId::new(u32::MAX), value.pool().clone())
                    .unwrap();
            }
            1 => {
                source.parameters = SemanticParameters::try_new([(
                    SemanticParameterId::new(u32::MAX),
                    SemanticParameterValue::TimeZone("UTC".into()),
                )])
                .unwrap()
            }
            2 => {
                source.pruning =
                    FrozenFragmentPruning::try_new(FragmentId::new(42), vec![], &Control::default())
                        .unwrap()
            }
            3 => source.required.plan_contract_revision = PLAN_CONTRACT_REVISION + 1,
            4 => {
                source.cuts.outbound = Box::from([OutboundFragmentCut {
                    edge: EdgeId::new(u32::MAX),
                    kind: EdgeKind::Stream,
                    destination_fragment: FragmentId::new(42),
                    destination_node: NodeId::new(42),
                    projection: Box::default(),
                    destination_imports: Box::default(),
                    partitioning: EdgePartitioning {
                        source: Distribution::Unconstrained,
                        source_multiplicity: RowMultiplicity::Replicated,
                        destination: Distribution::Unconstrained,
                        destination_multiplicity: RowMultiplicity::Replicated,
                        routing_only: Box::default(),
                    },
                    source_port: crate::FragmentOutputPort::Root,
                    delivery: crate::DeliveryRequirement::DemandDriven,
                    payload: crate::ExchangePayloadContract::Relational,
                }])
            }
            _ => unreachable!(),
        }
        derive(&source);
        let error = finish(&source, &FinalControl::default()).unwrap_err();
        match fault {
            0 => assert!(matches!(
                error,
                FragmentPackageError::Constant(ConstantReferenceError::UnusedPools)
            )),
            1 => assert!(matches!(error, FragmentPackageError::UnusedParameters)),
            2 => assert!(matches!(
                error,
                FragmentPackageError::Pruning(FrozenPruningError::WrongFragment)
            )),
            3 | 4 => assert!(matches!(error, FragmentPackageError::Structure(_))),
            _ => unreachable!(),
        }
        prefixes(&source, false, true);
    }
}

#[test]
fn owned_finalization_real_wide_project_source_preserves_all_definitions_and_quantum() {
    let mut builder = FragmentBuilder::new(FragmentId::new(81));
    let mut child = NodeId::new(u32::MAX);
    builder
        .add_values(child, Box::from([Box::default()]), Box::default())
        .unwrap();
    for id in 0..319 {
        let node = NodeId::new(id);
        builder
            .add_project(node, child, Box::default(), Box::default())
            .unwrap();
        child = node;
    }
    let mut fixture = frozen_fixture(
        builder
            .finish_definition(
                child,
                FragmentSink::Noop,
                dop(),
                crate::StagePlacementPolicy::Singleton,
            )
            .unwrap(),
    );
    replace_properties(&mut fixture, |parts| {
        for node in parts.nodes.values_mut() {
            if !node.inputs.is_empty() {
                node.output_properties = unconstrained(RowMultiplicity::SingleCopy);
                node.required_inputs[0] = unconstrained(RowMultiplicity::SingleCopy);
            }
        }
    });
    let source = input(fixture, FragmentCuts::default());
    let control = FinalControl::default();
    let package = finish(&source, &control).unwrap();
    assert_eq!(package.fragment().nodes().len(), 320);
    assert!(
        package
            .fragment()
            .nodes()
            .values()
            .all(|node| node.output_properties == properties())
    );
    assert_eq!(
        package.fragment().dop_domain(),
        source.fragment.dop_domain()
    );
    assert!(control.trace.lock().unwrap().contains(&256));
    prefixes(&source, true, false);
}
