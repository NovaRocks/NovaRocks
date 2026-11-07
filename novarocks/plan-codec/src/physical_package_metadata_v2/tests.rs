// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use crate::physical_properties_v2::PhysicalPropertyProjectionLimits;
use novarocks_type_contract::{
    CompilePhase, ExpressionControlFlow, PureCompileControl, SemanticParameters,
};
use std::{alloc::Layout, collections::BTreeMap, sync::Mutex};
const SOURCE: usize = 1 << 20;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    events: Mutex<Vec<(CompilePhase, u32)>>,
    stop: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut events = self.events.lock().unwrap();
        let at = events.len();
        if let Some((stop, _)) = self.stop {
            assert!(at <= stop, "callback after refusal");
        }
        events.push((phase, units));
        match self.stop.clone() {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}
impl Control {
    fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.events.lock().unwrap().clone()
    }
}
fn limits() -> PackageMetadataProjectionLimits {
    PackageMetadataProjectionLimits {
        max_input_nodes: 0,
        max_value_references: 0,
        max_list_items: 1024,
        max_allocation_requests: 8192,
        max_allocation_request_bytes: SOURCE,
        max_coexisting_source_and_request_bytes: 4 * SOURCE,
        max_work: 64 * SOURCE,
        properties: PhysicalPropertyProjectionLimits {
            max_value_references: 0,
            max_allocation_requests: 0,
            max_allocation_request_bytes: 0,
            max_coexisting_source_and_request_bytes: SOURCE,
            max_work: 1024,
        },
    }
}
fn admission() -> p::FragmentPackageAdmission {
    p::FragmentPackageAdmission {
        plan_limits: p::PlanLimits::FROZEN,
        source_retained_bytes: SOURCE,
        property_projection_limits: p::PropertyProofProjectionLimits {
            max_request_bytes: SOURCE,
            max_coexisting_bytes: 4 * SOURCE,
            max_projection_work: 64 * SOURCE,
        },
    }
}
fn package(annotations: Vec<p::PlanAnnotation>) -> p::FragmentPackage {
    let control = Control::default();
    let node = p::NodeId::new(u32::MAX);
    let fragment = p::Fragment::try_from_structure_observed(
        p::FragmentStructureInput {
            summaries: Box::default(),
            stage_placement: novarocks_physical_plan::StagePlacementPolicy::Singleton,
            id: p::FragmentId::new(0),
            root: node,
            values: BTreeMap::new(),
            expressions: p::ExprArena::default(),
            nodes: BTreeMap::from([(
                node,
                p::PhysicalNode {
                    id: node,
                    inputs: Box::default(),
                    required_inputs: Box::default(),
                    output_properties: p::PhysicalProperties {
                        distribution: p::Distribution::Singleton,
                        row_multiplicity: p::RowMultiplicity::SingleCopy,
                        ordering: Box::default(),
                    },
                    output: p::OutputPort {
                        node,
                        columns: Box::default(),
                    },
                    kind: p::NodeKind::Values {
                        rows: Box::from([Box::default()]),
                    },
                },
            )]),
            sink: p::FragmentSink::Noop,
            dop_domain: p::PipelineDopDomain {
                min: 2,
                max: 8,
                requires_power_of_two: true,
            },
            runtime_filters: Box::default(),
        },
        p::PlanLimits::FROZEN,
        &control,
    )
    .unwrap()
    .with_call_requests_observed(vec![], &control)
    .unwrap();
    let flow = ExpressionControlFlow::try_new(
        vec![],
        vec![],
        fragment.expressions(),
        CompilePhase::Validate,
        &control,
    )
    .unwrap();
    let uses = p::PhysicalRootUses::try_new(&fragment, flow, vec![], &control).unwrap();
    let calls = p::FrozenFragmentCalls::try_new(&fragment, &uses, vec![], &control).unwrap();
    let pruning = p::FrozenFragmentPruning::try_new(fragment.id(), vec![], &control).unwrap();
    p::FragmentPackage::try_new(
        p::FragmentPackageInput {
            constants: p::ConstantPools::empty(),
            version: p::PlanVersionId::try_new([19; 16]).unwrap(),
            required: p::RequiredContracts::default(),
            fragment,
            expression_uses: uses,
            calls,
            pruning,
            cuts: p::FragmentCuts::default(),
            result: None,
            parameters: SemanticParameters::default(),
            scans: BTreeMap::new(),
            writes: BTreeMap::new(),
            annotations: annotations.into_boxed_slice(),
        },
        admission(),
        &control,
    )
    .unwrap()
}
fn raw() -> wire::FragmentPackage {
    wire::FragmentPackage {
        plan_version: vec![19; 16],
        required: Some(wire::RequiredContracts {
            plan_contract_revision: 73,
        }),
        annotations: vec![wire::PlanAnnotation {
            subject: Some(wire::AnnotationSubject {
                kind: Some(wire::annotation_subject::Kind::Node(
                    wire::FragmentNodeSubject {
                        fragment_id: Some(0),
                        node_id: Some(u32::MAX),
                    },
                )),
            }),
            key: "计量".into(),
            value: "é".into(),
        }],
        ..Default::default()
    }
}
type Run<T> = (
    Result<(T, PackageMetadataProjectionFacts), Error>,
    Vec<(CompilePhase, u32)>,
);
fn decode(
    input: &wire::FragmentPackage,
    c: &Control,
    l: PackageMetadataProjectionLimits,
) -> Run<DecodedPackageMetadata> {
    let result = (|| {
        let mut work = CompileCheckpoints::try_new(c, CompilePhase::Decode)?;
        let result = decode_package_metadata_observed(input, SOURCE, l, &mut |_| Ok(()), &mut work);
        if matches!(&result, Err(Error::Control(_))) {
            return result;
        }
        work.finish()?;
        result
    })();
    (result, c.trace())
}
fn encode(
    input: &p::FragmentPackage,
    c: &Control,
    l: PackageMetadataProjectionLimits,
) -> Run<EncodedPackageMetadata> {
    let result = (|| {
        let mut work = CompileCheckpoints::try_new(c, CompilePhase::Encode)?;
        let result = encode_package_metadata_observed(input, SOURCE, l, &mut |_| Ok(()), &mut work);
        if matches!(&result, Err(Error::Control(_))) {
            return result;
        }
        work.finish()?;
        result
    })();
    (result, c.trace())
}

#[test]
fn metadata_real_package_and_hand_wire_preserve_local_subjects_and_detach_text() {
    let source = package(vec![
        p::PlanAnnotation {
            subject: p::AnnotationSubject::Fragment(p::FragmentId::new(0)),
            key: "fragment".into(),
            value: "".into(),
        },
        p::PlanAnnotation {
            subject: p::AnnotationSubject::Node(p::FragmentId::new(0), p::NodeId::new(u32::MAX)),
            key: "计量".into(),
            value: "é".into(),
        },
    ]);
    let (out, _) = encode(&source, &Control::default(), limits());
    let (out, _) = out.unwrap();
    assert_eq!(out.plan_version, [19; 16]);
    assert_eq!(
        out.required.plan_contract_revision,
        p::PLAN_CONTRACT_REVISION
    );
    assert_eq!(
        out.annotations[0].subject.as_ref().unwrap().kind,
        Some(wire::annotation_subject::Kind::FragmentId(0))
    );
    assert_eq!(out.annotations[1], raw().annotations[0]);
    assert_eq!(out.annotations[0].key, "fragment");
    drop(source);
    assert_eq!(out.annotations[1].value, "é");
    let mut wire = raw();
    wire.annotations.push(wire::PlanAnnotation {
        subject: Some(wire::AnnotationSubject {
            kind: Some(wire::annotation_subject::Kind::Value(
                wire::FragmentValueSubject {
                    fragment_id: Some(u32::MAX),
                    value_id: Some(0),
                },
            )),
        }),
        key: "".into(),
        value: "value".into(),
    });
    let decoded = decode(&wire, &Control::default(), limits()).0.unwrap().0;
    assert_eq!(decoded.version.as_bytes(), &[19; 16]);
    assert_eq!(decoded.required.plan_contract_revision, 73);
    assert_eq!(
        decoded.annotations[1].subject,
        p::AnnotationSubject::Value(p::FragmentId::new(u32::MAX), p::ValueId::new(0))
    );
    assert_ne!(
        decoded.annotations[0].key.as_ptr(),
        wire.annotations[0].key.as_ptr()
    );
    drop(wire);
    assert_eq!(&*decoded.annotations[0].key, "计量");
}
#[test]
fn metadata_malformed_presence_and_zero_version_retain_typed_errors() {
    let cases: [fn(&mut wire::FragmentPackage); 7] = [
        |v| v.required = None,
        |v| v.plan_version.pop().map(drop).unwrap(),
        |v| v.plan_version = vec![0; 16],
        |v| v.annotations[0].subject = None,
        |v| v.annotations[0].subject.as_mut().unwrap().kind = None,
        |v| {
            v.annotations[0].subject.as_mut().unwrap().kind = Some(
                wire::annotation_subject::Kind::Node(wire::FragmentNodeSubject {
                    fragment_id: None,
                    node_id: Some(0),
                }),
            )
        },
        |v| {
            v.annotations[0].subject.as_mut().unwrap().kind = Some(
                wire::annotation_subject::Kind::Value(wire::FragmentValueSubject {
                    fragment_id: Some(0),
                    value_id: None,
                }),
            )
        },
    ];
    for (at, mutate) in cases.into_iter().enumerate() {
        let mut input = raw();
        mutate(&mut input);
        let error = decode(&input, &Control::default(), limits())
            .0
            .err()
            .unwrap();
        if at == 2 {
            assert!(matches!(
                error,
                Error::Identity(p::IdentityError::ZeroPlanVersion)
            ));
        } else {
            assert!(matches!(error, Error::InvalidShape(_)));
        }
    }
}
#[test]
fn metadata_publication_rechecks_annotation_references_and_revision() {
    let input = package(vec![]).into_input();
    let mut wrong = input.clone();
    wrong.annotations = vec![p::PlanAnnotation {
        subject: p::AnnotationSubject::Plan,
        key: "k".into(),
        value: "v".into(),
    }]
    .into_boxed_slice();
    assert!(p::FragmentPackage::try_new(wrong, admission(), &Control::default()).is_err());
    let decoded = decode(&raw(), &Control::default(), limits()).0.unwrap().0;
    let mut fresh = input.clone();
    fresh.version = decoded.version;
    fresh.required = decoded.required;
    fresh.annotations = decoded.annotations;
    assert!(p::FragmentPackage::try_new(fresh, admission(), &Control::default()).is_err());
    let mut missing = input;
    missing.annotations = vec![p::PlanAnnotation {
        subject: p::AnnotationSubject::Value(p::FragmentId::new(0), p::ValueId::new(0)),
        key: "k".into(),
        value: "v".into(),
    }]
    .into_boxed_slice();
    assert!(p::FragmentPackage::try_new(missing, admission(), &Control::default()).is_err());
}
#[test]
fn metadata_hand_layout_inventory_and_each_nonzero_axis_have_exact_and_under_gates() {
    let input = raw();
    let facts = decode(&input, &Control::default(), limits()).0.unwrap().1;
    let expected =
        2 * Layout::array::<p::PlanAnnotation>(1).unwrap().size() + 2 * ("计量".len() + "é".len());
    assert_eq!(facts.allocation_requests_upper_bound, 6);
    assert_eq!(facts.allocation_request_bytes_upper_bound, expected);
    assert_eq!(
        facts.coexisting_source_and_request_bytes_upper_bound,
        SOURCE + expected
    );
    assert_eq!(facts.list_item_count, 1);
    assert_eq!(facts.cumulative_work_upper_bound, 256 + 32 + 4 * expected);
    let mut exact = limits();
    exact.max_list_items = 1;
    exact.max_allocation_requests = 6;
    exact.max_allocation_request_bytes = expected;
    exact.max_coexisting_source_and_request_bytes = SOURCE + expected;
    exact.max_work = facts.cumulative_work_upper_bound;
    assert!(decode(&input, &Control::default(), exact).0.is_ok());
    for axis in 0..5 {
        let mut under = exact;
        match axis {
            0 => under.max_list_items -= 1,
            1 => under.max_allocation_requests -= 1,
            2 => under.max_allocation_request_bytes -= 1,
            3 => under.max_coexisting_source_and_request_bytes -= 1,
            _ => under.max_work -= 1,
        };
        assert!(matches!(
            decode(&input, &Control::default(), under).0,
            Err(Error::Control(CompileControlError::ResourceExhausted))
        ));
    }
    let source = package(vec![p::PlanAnnotation {
        subject: p::AnnotationSubject::Node(p::FragmentId::new(0), p::NodeId::new(u32::MAX)),
        key: "计量".into(),
        value: "é".into(),
    }]);
    let facts = encode(&source, &Control::default(), limits()).0.unwrap().1;
    assert_eq!(facts.allocation_requests_upper_bound, 4);
    assert_eq!(
        facts.allocation_request_bytes_upper_bound,
        16 + size_of::<wire::PlanAnnotation>() + 8
    );
}
#[test]
fn metadata_all_actual_small_success_and_ordinary_control_prefixes_keep_three_causes() {
    let input = raw();
    let good = decode(&input, &Control::default(), limits());
    assert!(good.0.is_ok());
    let mut bad = raw();
    bad.annotations[0].subject = None;
    let ordinary = decode(&bad, &Control::default(), limits());
    assert!(matches!(ordinary.0, Err(Error::InvalidShape(_))));
    for (input, baseline) in [(&input, good.1), (&bad, ordinary.1)] {
        for at in 0..baseline.len() {
            for cause in CAUSES {
                let c = Control {
                    events: Mutex::default(),
                    stop: Some((at, cause.clone())),
                };
                let (result, trace) = decode(input, &c, limits());
                assert!(matches!(result,Err(Error::Control(found)) if found==cause));
                assert_eq!(trace, baseline[..=at]);
            }
        }
    }
    let source = package(vec![p::PlanAnnotation {
        subject: p::AnnotationSubject::Fragment(p::FragmentId::new(0)),
        key: "a".into(),
        value: "b".into(),
    }]);
    let good = encode(&source, &Control::default(), limits());
    assert!(good.0.is_ok());
    for at in 0..good.1.len() {
        for cause in CAUSES {
            let c = Control {
                events: Mutex::default(),
                stop: Some((at, cause.clone())),
            };
            let (result, trace) = encode(&source, &c, limits());
            assert!(matches!(result,Err(Error::Control(found)) if found==cause));
            assert_eq!(trace, good.1[..=at]);
        }
    }
}
#[test]
fn metadata_known_text_parent_numeric_refusal_precedes_pending_255_late_control() {
    let input = raw();
    let source = package(vec![p::PlanAnnotation {
        subject: p::AnnotationSubject::Fragment(p::FragmentId::new(0)),
        key: "known".into(),
        value: "text".into(),
    }]);
    for cause in CAUSES {
        for decode_direction in [false, true] {
            let c = Control {
                events: Mutex::default(),
                stop: Some((1, cause.clone())),
            };
            let mut work = CompileCheckpoints::try_new(
                &c,
                if decode_direction {
                    CompilePhase::Decode
                } else {
                    CompilePhase::Encode
                },
            )
            .unwrap();
            for _ in 0..255 {
                work.step().unwrap();
            }
            let mut cap = limits();
            cap.max_allocation_request_bytes = if decode_direction {
                2 * size_of::<p::PlanAnnotation>()
            } else {
                16 + size_of::<wire::PlanAnnotation>()
            };
            let result = if decode_direction {
                decode_package_metadata_observed(&input, SOURCE, cap, &mut |_| Ok(()), &mut work)
                    .map(|_| ())
            } else {
                encode_package_metadata_observed(&source, SOURCE, cap, &mut |_| Ok(()), &mut work)
                    .map(|_| ())
            };
            assert!(matches!(
                result,
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
            assert_eq!(c.trace().len(), 1);
        }
    }
    let c = Control::default();
    let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
    let mut largest = 0;
    let error = decode_package_metadata_observed(
        &input,
        SOURCE,
        limits(),
        &mut |f| {
            largest = largest.max(f.allocation_request_bytes_upper_bound);
            if f.allocation_requests_upper_bound > 2 {
                Err(CompileControlError::ResourceExhausted)
            } else {
                Ok(())
            }
        },
        &mut work,
    )
    .err()
    .unwrap();
    assert!(largest > 2 * size_of::<p::PlanAnnotation>());
    assert!(matches!(
        error,
        Error::Control(CompileControlError::ResourceExhausted)
    ));
    assert_eq!(c.trace(), vec![(CompilePhase::Decode, 0)]);
}

#[test]
fn metadata_source_capacity_floor_and_arithmetic_overflow_are_checked_before_copy() {
    let mut input = raw();
    input.annotations[0].key = String::with_capacity(10_000);
    input.annotations[0].key.push('k');
    let known = size_of::<wire::FragmentPackage>()
        + input.plan_version.capacity()
        + input.annotations.capacity() * size_of::<wire::PlanAnnotation>()
        + input.annotations[0].key.capacity()
        + input.annotations[0].value.capacity();
    for (invoice, accepted) in [(known, true), (known - 1, false)] {
        let c = Control::default();
        let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
        let result =
            decode_package_metadata_observed(&input, invoice, limits(), &mut |_| Ok(()), &mut work);
        if accepted {
            let (decoded, facts) = result.unwrap();
            assert_eq!(&*decoded.annotations[0].key, "k");
            assert_eq!(
                facts.coexisting_source_and_request_bytes_upper_bound,
                invoice + 2 * size_of::<p::PlanAnnotation>() + 6
            );
        } else {
            assert!(matches!(
                result,
                Err(Error::InvalidShape(
                    "package metadata source invoice is understated"
                ))
            ));
            assert_eq!(c.trace(), vec![(CompilePhase::Decode, 0)]);
        }
    }
    for cause in CAUSES {
        let c = Control {
            events: Mutex::default(),
            stop: Some((1, cause)),
        };
        let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
        for _ in 0..255 {
            work.step().unwrap();
        }
        let result = decode_package_metadata_observed(
            &input,
            usize::MAX,
            limits(),
            &mut |_| Ok(()),
            &mut work,
        );
        assert!(matches!(
            result,
            Err(Error::Control(CompileControlError::ResourceExhausted))
        ));
        assert_eq!(c.trace(), vec![(CompilePhase::Decode, 0)]);
    }
}
