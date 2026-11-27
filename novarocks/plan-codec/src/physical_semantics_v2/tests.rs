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
use novarocks_physical_plan::{
    Distribution, FragmentBuilder, FragmentCuts, FragmentId, FragmentPackageError,
    FragmentPackageInput, FragmentSink, NodeId, NodeKind, OutputPort, PhysicalNode,
    PhysicalProperties, PipelineDopDomain, PlanVersionId, RequiredContracts, RowMultiplicity,
};
use novarocks_type_contract::{SemanticParameterId, SemanticParameterValue};
use std::{collections::BTreeMap, sync::Mutex};

#[derive(Default)]
struct Control {
    observations: Mutex<Vec<(CompilePhase, u32)>>,
    failure: Option<(CompilePhase, usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= novarocks_type_contract::MAX_UNOBSERVED_COMPILE_WORK);
        let mut observed = self.observations.lock().unwrap();
        let ordinal = observed.iter().filter(|(seen, _)| *seen == phase).count();
        observed.push((phase, units));
        match self.failure.clone() {
            Some((target, at, cause)) if target == phase && ordinal == at => Err(cause),
            _ => Ok(()),
        }
    }
}

fn input() -> FragmentPackageInput {
    let control = Control::default();
    let mut builder = FragmentBuilder::new(FragmentId::new(u32::MAX));
    let node = NodeId::new(0);
    builder
        .insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output: OutputPort {
                node,
                columns: Box::default(),
            },
            output_properties: PhysicalProperties {
                distribution: Distribution::Singleton,
                row_multiplicity: RowMultiplicity::SingleCopy,
                ordering: Box::default(),
            },
            expressions: None,
            kind: NodeKind::Values {
                rows: Box::from([Box::default()]),
            },
        })
        .unwrap();
    let fragment = builder
        .finish_definition(
            node,
            FragmentSink::Noop,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let fragment = fragment
        .with_call_requests_observed(vec![], &control)
        .unwrap()
        .with_calls_observed(vec![], &control)
        .unwrap();
    let pruning = FrozenFragmentPruning::try_new(fragment.id(), vec![], &control).unwrap();
    FragmentPackageInput {
        version: PlanVersionId::try_new([1; 16]).unwrap(),
        required: RequiredContracts::default(),
        fragment,
        pruning,
        cuts: FragmentCuts::default(),
        result: None,
        parameters: SemanticParameters::default(),
        constants: novarocks_physical_plan::ConstantPools::empty(),
        scans: BTreeMap::new(),
        writes: BTreeMap::new(),
        annotations: Box::default(),
        layouts: Default::default(),
    }
}

#[test]
fn public_semantics_projection_preserves_explicit_empty_components_and_same_package() {
    let original = input();
    let package =
        FragmentPackage::try_new(original.clone(), package_admission(), &Control::default())
            .unwrap();
    let encoded = encode_fragment_semantics(&package, &Control::default()).unwrap();
    assert!(encoded.parameters.entries.is_empty());
    assert!(encoded.calls.entries.is_empty());
    assert!(encoded.pruning.witnesses.is_empty());
    let decoded =
        decode_fragment_semantics(package.fragment(), &encoded, &Control::default()).unwrap();
    let (parameters, calls, pruning) = decoded.into_parts();
    let fragment = original
        .fragment
        .clone()
        .with_calls_observed(
            calls.entries().values().cloned().collect(),
            &Control::default(),
        )
        .unwrap();
    let reconstructed = FragmentPackage::try_new(
        FragmentPackageInput {
            parameters,
            fragment,
            pruning,
            ..original
        },
        package_admission(),
        &Control::default(),
    )
    .unwrap();
    assert_eq!(reconstructed.version(), package.version());
    assert_eq!(reconstructed.required(), package.required());
    assert_eq!(reconstructed.fragment(), package.fragment());
    assert_eq!(reconstructed.parameters(), package.parameters());
    assert_eq!(reconstructed.cuts(), package.cuts());
    assert_eq!(reconstructed.result(), package.result());
    assert!(reconstructed.calls().entries().is_empty());
    assert!(reconstructed.pruning().witnesses().is_empty());
    assert!(reconstructed.constants().entries().is_empty());
    assert!(reconstructed.scans().is_empty());
    assert!(reconstructed.writes().is_empty());
    assert!(reconstructed.annotations().is_empty());
}

#[test]
fn decoded_semantic_parts_require_actual_package_parameter_closure() {
    let original = input();
    let package =
        FragmentPackage::try_new(original.clone(), package_admission(), &Control::default())
            .unwrap();
    let mut encoded = encode_fragment_semantics(&package, &Control::default()).unwrap();
    encoded.parameters.entries.push(wire::SemanticParameter {
        id: u32::MAX,
        value: Some(wire::semantic_parameter::Value::AllowThrowException(false)),
    });
    let (parameters, calls, pruning) =
        decode_fragment_semantics(package.fragment(), &encoded, &Control::default())
            .unwrap()
            .into_parts();
    let fragment = original
        .fragment
        .clone()
        .with_calls_observed(
            calls.entries().values().cloned().collect(),
            &Control::default(),
        )
        .unwrap();
    assert_eq!(
        parameters.get(SemanticParameterId::new(u32::MAX)),
        Some(&SemanticParameterValue::AllowThrowException(false))
    );
    assert!(matches!(
        FragmentPackage::try_new(
            FragmentPackageInput {
                parameters,
                fragment,
                pruning,
                ..original
            },
            package_admission(),
            &Control::default()
        ),
        Err(FragmentPackageError::UnusedParameters)
    ));
}

#[test]
fn public_semantics_control_refusal_precedes_result_publication_in_both_directions() {
    let package =
        FragmentPackage::try_new(input(), package_admission(), &Control::default()).unwrap();
    let encoded = encode_fragment_semantics(&package, &Control::default()).unwrap();
    for error in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for phase in [CompilePhase::Encode, CompilePhase::Decode] {
            for at in [0, 1] {
                let control = Control {
                    failure: Some((phase, at, error.clone())),
                    ..Default::default()
                };
                let result = if phase == CompilePhase::Encode {
                    encode_fragment_semantics(&package, &control).map(|_| ())
                } else {
                    decode_fragment_semantics(package.fragment(), &encoded, &control).map(|_| ())
                };
                assert_eq!(result, Err(SemanticsCodecError::Control(error.clone())));
                assert_eq!(
                    control
                        .observations
                        .lock()
                        .unwrap()
                        .iter()
                        .filter(|(seen, _)| *seen == phase)
                        .count(),
                    at + 1
                );
            }
        }
    }
}

#[test]
fn ordinary_decode_failure_observes_completed_tail_and_preserves_control_category() {
    struct PositiveRefusal(CompileControlError);
    impl PureCompileControl for PositiveRefusal {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            if phase == CompilePhase::Decode && units > 0 {
                Err(self.0.clone())
            } else {
                Ok(())
            }
        }
    }
    let package =
        FragmentPackage::try_new(input(), package_admission(), &Control::default()).unwrap();
    let encoded = encode_fragment_semantics(&package, &Control::default()).unwrap();
    for malformed_parameters in [false, true] {
        let mut bad = encoded.clone();
        if malformed_parameters {
            bad.parameters
                .entries
                .push(wire::SemanticParameter { id: 0, value: None });
        } else {
            bad.pruning.witnesses.push(wire::PruningDomainWitness {
                target: None,
                sources: vec![],
            });
        }
        assert!(matches!(
            decode_fragment_semantics(package.fragment(), &bad, &Control::default()),
            Err(SemanticsCodecError::InvalidShape(_))
        ));
        for error in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let result = decode_fragment_semantics(
                package.fragment(),
                &bad,
                &PositiveRefusal(error.clone()),
            )
            .map(|_| ());
            assert_eq!(result, Err(SemanticsCodecError::Control(error)));
        }
    }
}

#[test]
fn nested_owner_control_failures_keep_the_original_semantics_codec_category() {
    for error in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for projected in [
            SemanticsCodecError::from(FrozenCallError::Control(error.clone())),
            SemanticsCodecError::from(ControlCodecError::Flow(
                novarocks_type_contract::ExpressionControlFlowError::Control(error.clone()),
            )),
            SemanticsCodecError::from(FrozenPruningError::Structure(
                PruningStructureError::Source(PredicateSourceError::Control(error.clone())),
            )),
        ] {
            assert_eq!(projected, SemanticsCodecError::Control(error.clone()));
        }
    }
}

// Explicit small-fixture source invoice and independent property projection
// ceilings. These are test inputs, not a production default or MEM grant.
fn package_admission() -> novarocks_physical_plan::FragmentPackageAdmission {
    novarocks_physical_plan::FragmentPackageAdmission {
        plan_limits: novarocks_physical_plan::PlanLimits::FROZEN,
        source_retained_bytes: 64 * 1024 * 1024,
        property_projection_limits: novarocks_physical_plan::PropertyProofProjectionLimits {
            max_request_bytes: 16 * 1024 * 1024,
            max_coexisting_bytes: 256 * 1024 * 1024,
            max_projection_work: 16 * 1024 * 1024,
        },
    }
}
