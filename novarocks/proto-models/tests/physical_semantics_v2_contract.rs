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

use std::{
    collections::BTreeSet,
    fmt::Debug,
    mem::{align_of, size_of},
};

use novarocks_proto_models::{
    FILE_DESCRIPTOR_SET, generated_resource_layouts, physical_control_v2 as control,
    physical_semantics_v2 as v2,
    resource_layout::{GeneratedResourceLayout, ObjectKind},
};
use prost::Message;
use prost_reflect::{DescriptorPool, Kind, MessageDescriptor};

fn roundtrip<M: Message + Default + PartialEq + Debug>(value: &M) {
    assert_eq!(M::decode(value.encode_to_vec().as_slice()).unwrap(), *value);
}
fn pool() -> DescriptorPool {
    DescriptorPool::decode(FILE_DESCRIPTOR_SET).unwrap()
}
fn effects(scope: v2::proof_scope::Kind) -> v2::CallEffects {
    v2::CallEffects {
        value_stability: v2::ValueStability::Volatile as i32,
        own_row_error: v2::OwnRowError::MayRaise as i32,
        failure_behavior: v2::FailureBehavior::ReturnsNull as i32,
        null_behavior: v2::NullBehavior::CalledOnNull as i32,
        argument_control: Some(v2::ArgumentControl {
            kind: Some(v2::argument_control::Kind::HigherOrder(
                control::HigherOrderControl {
                    body_ordinal: u32::MAX,
                    body_demand: control::EvaluationDemand::TruthOnly as i32,
                },
            )),
        }),
        instance_state: v2::InstanceState::ScalarInstance as i32,
        observable_effects: Some(v2::ObservableEffects {
            rng_sampling: true,
            warnings: true,
            controlled_wait: true,
        }),
        environment: vec![
            v2::SemanticParameterRef {
                id: Some(0),
                expected_key: v2::SemanticParameterKey::TimeZone as i32,
            },
            v2::SemanticParameterRef {
                id: Some(u32::MAX),
                expected_key: v2::SemanticParameterKey::StatementStartUtc as i32,
            },
        ],
        proof_scope: Some(v2::ProofScope { kind: Some(scope) }),
        strict_argument_ordinals: Vec::new(),
    }
}

#[test]
fn all_parameter_variants_preserve_raw_values_and_oneof_presence() {
    use v2::semantic_parameter::Value;
    let values = [
        Value::StatementStartUtc(
            novarocks_proto_models::physical_semantics_v2::StatementStartUtc {
                seconds: Some(i64::MIN),
                nanos: Some(7),
            },
        ),
        Value::TimeZone("+08:00".into()),
        Value::StatementStartUtc(v2::StatementStartUtc {
            seconds: Some(0),
            nanos: Some(0),
        }),
    ];
    let parameters = v2::SemanticParameters {
        entries: values
            .into_iter()
            .enumerate()
            .map(|(ordinal, value)| v2::SemanticParameter {
                id: if ordinal == 2 {
                    u32::MAX
                } else {
                    ordinal as u32
                },
                value: Some(value),
            })
            .collect(),
    };
    roundtrip(&parameters);
    let pool = pool();
    let descriptor = pool
        .get_message_by_name("novarocks.physical_semantics_v2.SemanticParameter")
        .unwrap();
    assert_eq!(
        descriptor
            .oneofs()
            .find(|oneof| oneof.name() == "value")
            .unwrap()
            .fields()
            .count(),
        3
    );
    for entry in &parameters.entries {
        let absent = v2::SemanticParameter {
            id: entry.id,
            value: None,
        };
        assert_ne!(entry.encode_to_vec(), absent.encode_to_vec());
    }
    let zero = v2::SemanticParameter {
        id: 0,
        value: Some(Value::StatementStartUtc(v2::StatementStartUtc {
            seconds: Some(0),
            nanos: Some(0),
        })),
    };
    roundtrip(&zero);
    assert_ne!(
        zero.encode_to_vec(),
        v2::SemanticParameter::default().encode_to_vec()
    );
}

#[test]
fn all_nine_effect_fields_higher_order_channels_and_both_proof_scopes_survive() {
    let value = effects(v2::proof_scope::Kind::RegionId(0));
    roundtrip(&value);
    let unconditional = effects(v2::proof_scope::Kind::Unconditional(control::Empty {}));
    roundtrip(&unconditional);
    assert_ne!(value.encode_to_vec(), unconditional.encode_to_vec());
    let relation = effects(v2::proof_scope::Kind::Relation(v2::RelationEffectId {
        call_id: Some(0),
        role: v2::RelationEffectRole::Call as i32,
    }));
    roundtrip(&relation);
    assert_ne!(value.encode_to_vec(), relation.encode_to_vec());
    let max_region = effects(v2::proof_scope::Kind::RegionId(u32::MAX));
    roundtrip(&max_region);
    assert_ne!(value.encode_to_vec(), max_region.encode_to_vec());
    let descriptor = pool()
        .get_message_by_name("novarocks.physical_semantics_v2.CallEffects")
        .unwrap();
    assert_eq!(
        descriptor
            .fields()
            .map(|field| (field.number(), field.name().to_owned()))
            .collect::<Vec<_>>(),
        vec![
            (1, "value_stability".into()),
            (2, "own_row_error".into()),
            (3, "failure_behavior".into()),
            (4, "null_behavior".into()),
            (5, "argument_control".into()),
            (6, "instance_state".into()),
            (7, "observable_effects".into()),
            (8, "environment".into()),
            (9, "proof_scope".into()),
        ]
    );
    for simple in [
        v2::SimpleArgumentControl::Eager,
        v2::SimpleArgumentControl::TypeOnly,
        v2::SimpleArgumentControl::If,
        v2::SimpleArgumentControl::Coalesce,
        v2::SimpleArgumentControl::SimpleCase,
        v2::SimpleArgumentControl::SearchedCase,
        v2::SimpleArgumentControl::Aggregate,
        v2::SimpleArgumentControl::Window,
        v2::SimpleArgumentControl::Table,
        v2::SimpleArgumentControl::NoArguments,
    ] {
        roundtrip(&v2::ArgumentControl {
            kind: Some(v2::argument_control::Kind::Simple(simple as i32)),
        });
    }
    roundtrip(&v2::ArgumentControl {
        kind: Some(v2::argument_control::Kind::HigherOrder(
            control::HigherOrderControl {
                body_ordinal: 0,
                body_demand: control::EvaluationDemand::Value as i32,
            },
        )),
    });
    // These tests cover representation, not valid owner declarations or
    // application admission of the deliberately independent field values.
}

#[test]
fn six_typed_sites_and_context_references_preserve_zero_max_and_absence() {
    use v2::call_site::Kind;
    let node = v2::NodeCallSite {
        node_id: Some(0),
        call: u32::MAX,
    };
    let sites = [
        Kind::Window(node),
        Kind::Aggregate(node),
        Kind::TopNState(node),
        Kind::WriterPartial(node),
        Kind::WriterFinal(node),
        Kind::TableNodeId(u32::MAX),
    ];
    let calls = v2::FrozenCalls {
        entries: sites
            .into_iter()
            .enumerate()
            .map(|(ordinal, site)| {
                let identity = v2::RelationEffectId {
                    call_id: Some(if ordinal == 0 { 0 } else { u32::MAX }),
                    role: v2::RelationEffectRole::Call as i32,
                };
                v2::FrozenCall {
                    site: Some(v2::CallSite { kind: Some(site) }),
                    relation_context: Some(v2::RelationEffectContext {
                        identity: Some(identity),
                        demand: control::EvaluationDemand::Value as i32,
                    }),
                    materialized_input: matches!(ordinal, 3 | 4).then_some(
                        v2::RelationEffectContext {
                            identity: Some(v2::RelationEffectId {
                                role: v2::RelationEffectRole::MaterializedInput as i32,
                                ..identity
                            }),
                            demand: control::EvaluationDemand::Value as i32,
                        },
                    ),
                    effects: Some(effects(v2::proof_scope::Kind::Relation(identity))),
                    decimal_overflow_policy: Some(if ordinal % 2 == 0 {
                        v2::DecimalOverflowPolicy::OutputNull
                    } else {
                        v2::DecimalOverflowPolicy::ReportError
                    } as i32),
                }
            })
            .collect(),
    };
    roundtrip(&calls);
    let descriptor = pool()
        .get_message_by_name("novarocks.physical_semantics_v2.CallSite")
        .unwrap();
    assert_eq!(
        descriptor
            .oneofs()
            .find(|oneof| oneof.name() == "kind")
            .unwrap()
            .fields()
            .count(),
        6
    );
    assert!(descriptor.reserved_ranges().any(|range| range.contains(&1)));
    for id in [0, u32::MAX] {
        let reference = v2::SemanticParameterRef {
            id: Some(id),
            expected_key: v2::SemanticParameterKey::TimeZone as i32,
        };
        roundtrip(&reference);
        assert_ne!(
            reference.encode_to_vec(),
            v2::SemanticParameterRef {
                id: None,
                ..reference
            }
            .encode_to_vec()
        );
        let context = v2::ExpressionEffectContext {
            use_id: Some(id),
            region_id: Some(id),
            demand: control::EvaluationDemand::Value as i32,
        };
        roundtrip(&context);
        for absent in [
            v2::ExpressionEffectContext {
                use_id: None,
                ..context
            },
            v2::ExpressionEffectContext {
                region_id: None,
                ..context
            },
        ] {
            roundtrip(&absent);
            assert_ne!(context.encode_to_vec(), absent.encode_to_vec());
        }
        let node = v2::NodeCallSite {
            node_id: Some(id),
            call: 0,
        };
        roundtrip(&node);
        assert_ne!(
            node.encode_to_vec(),
            v2::NodeCallSite {
                node_id: None,
                ..node
            }
            .encode_to_vec()
        );
        for kind in [Kind::Window(node), Kind::TableNodeId(id)] {
            let site = v2::CallSite { kind: Some(kind) };
            roundtrip(&site);
            assert_ne!(
                site.encode_to_vec(),
                v2::CallSite::default().encode_to_vec()
            );
        }
        for role in [
            v2::RelationEffectRole::Call,
            v2::RelationEffectRole::MaterializedInput,
        ] {
            let identity = v2::RelationEffectId {
                call_id: Some(id),
                role: role as i32,
            };
            roundtrip(&identity);
            assert_ne!(
                identity.encode_to_vec(),
                v2::RelationEffectId {
                    call_id: None,
                    ..identity
                }
                .encode_to_vec()
            );
        }
    }
    for name in [
        "SemanticParameterRef",
        "ExpressionEffectContext",
        "NodeCallSite",
        "RelationEffectId",
    ] {
        let descriptor = pool()
            .get_message_by_name(&format!("novarocks.physical_semantics_v2.{name}"))
            .unwrap();
        for field in descriptor
            .fields()
            .filter(|field| field.name() == "id" || field.name().ends_with("_id"))
        {
            assert!(field.supports_presence(), "{}", field.full_name());
        }
    }
}

#[test]
fn semantics_descriptor_is_nonrecursive_and_every_actual_layout_is_registered() {
    fn depth(message: MessageDescriptor, path: &mut BTreeSet<String>) -> usize {
        assert!(
            path.insert(message.full_name().to_owned()),
            "recursive carrier: {}",
            message.full_name()
        );
        let mut maximum = 1;
        for field in message.fields() {
            if let Kind::Message(child) = field.kind() {
                assert!(matches!(
                    child.package_name(),
                    "novarocks.physical_semantics_v2" | "novarocks.physical_control_v2"
                ));
                maximum = maximum.max(1 + depth(child, path));
            }
        }
        path.remove(message.full_name());
        maximum
    }
    let pool = pool();
    assert_eq!(
        depth(
            pool.get_message_by_name("novarocks.physical_semantics_v2.FrozenCalls")
                .unwrap(),
            &mut BTreeSet::new()
        ),
        5
    );
    assert_eq!(
        depth(
            pool.get_message_by_name("novarocks.physical_semantics_v2.ExpressionClaims")
                .unwrap(),
            &mut BTreeSet::new()
        ),
        5
    );
    assert_eq!(
        depth(
            pool.get_message_by_name("novarocks.physical_semantics_v2.SemanticParameters")
                .unwrap(),
            &mut BTreeSet::new()
        ),
        3
    );
    assert_eq!(
        depth(
            pool.get_message_by_name("novarocks.physical_semantics_v2.FrozenPruning")
                .unwrap(),
            &mut BTreeSet::new()
        ),
        4
    );
    let layouts = generated_resource_layouts()
        .filter(|layout| {
            layout
                .schema_id
                .starts_with("novarocks.physical_semantics_v2.")
        })
        .collect::<Vec<_>>();
    let actual = layouts
        .iter()
        .map(|layout| layout.schema_id.to_owned())
        .collect::<BTreeSet<_>>();
    assert_eq!(actual.len(), layouts.len());
    let mut expected = BTreeSet::new();
    for message in pool
        .all_messages()
        .filter(|message| message.package_name() == "novarocks.physical_semantics_v2")
    {
        expected.insert(message.full_name().to_owned());
        let layout = layouts
            .iter()
            .find(|layout| layout.schema_id == message.full_name())
            .unwrap();
        assert_eq!(layout.kind, ObjectKind::Message);
        let tags = layout
            .fields
            .iter()
            .flat_map(|field| field.wire.iter().map(|wire| wire.number))
            .collect::<Vec<_>>();
        let distinct = tags.iter().copied().collect::<BTreeSet<_>>();
        assert_eq!(tags.len(), distinct.len());
        assert_eq!(
            distinct,
            message.fields().map(|field| field.number()).collect()
        );
        for oneof in message.oneofs().filter(|oneof| {
            !oneof
                .fields()
                .any(|field| field.field_descriptor_proto().proto3_optional == Some(true))
        }) {
            expected.insert(oneof.full_name().to_owned());
            let layout = layouts
                .iter()
                .find(|layout| layout.schema_id == oneof.full_name())
                .unwrap();
            assert_eq!(layout.kind, ObjectKind::Oneof);
            assert_eq!(
                layout
                    .fields
                    .iter()
                    .flat_map(|field| field.wire.iter().map(|wire| wire.number))
                    .collect::<BTreeSet<_>>(),
                oneof.fields().map(|field| field.number()).collect()
            );
        }
    }
    assert_eq!(actual, expected);
    fn dimensions<T: GeneratedResourceLayout>() {
        assert_eq!(T::RESOURCE_LAYOUT.size, size_of::<T>());
        assert_eq!(T::RESOURCE_LAYOUT.alignment, align_of::<T>());
    }
    dimensions::<v2::SemanticParameters>();
    dimensions::<v2::SemanticParameter>();
    dimensions::<v2::semantic_parameter::Value>();
    dimensions::<v2::FrozenCalls>();
    dimensions::<v2::FrozenCall>();
    dimensions::<v2::ExpressionClaims>();
    dimensions::<v2::ExpressionCallClaim>();
    dimensions::<v2::ExpressionEffectContext>();
    dimensions::<v2::RelationEffectContext>();
    dimensions::<v2::RelationEffectId>();
    dimensions::<v2::CallEffects>();
    dimensions::<v2::CallSite>();
    dimensions::<v2::call_site::Kind>();
    dimensions::<v2::ArgumentControl>();
    dimensions::<v2::argument_control::Kind>();
    dimensions::<v2::ProofScope>();
    dimensions::<v2::proof_scope::Kind>();
    dimensions::<v2::FrozenPruning>();
    dimensions::<v2::PruningDomainWitness>();
    dimensions::<v2::PruningDomainSite>();
    dimensions::<v2::PredicateResponsibilityRef>();
    dimensions::<v2::PruningSourceWitness>();
    dimensions::<v2::PruningInputEdge>();
    dimensions::<v2::PruningColumnTrace>();
    // Actual Rust layout coverage is an input to later resource modelling;
    // it does not prove bounded decoding allocations or a complete codec.
}

fn pruning_witness(id: u32, field: v2::PruningDomainField) -> v2::PruningDomainWitness {
    v2::PruningDomainWitness {
        target: Some(v2::PruningDomainSite {
            fragment_id: Some(id),
            scan_id: Some(id),
            occurrence_id: Some(id),
            field: field as i32,
        }),
        sources: vec![v2::PruningSourceWitness {
            responsibility: Some(v2::PredicateResponsibilityRef {
                fragment_id: Some(id),
                node_id: Some(id),
                root_ordinal: Some(id),
                use_id: Some(id),
            }),
            context: Some(v2::ExpressionEffectContext {
                use_id: Some(id),
                region_id: Some(id),
                demand: control::EvaluationDemand::TruthOnly as i32,
            }),
            conjunct_path: vec![0, 2, 0],
            input_path: vec![
                v2::PruningInputEdge {
                    consumer_id: Some(id),
                    input_ordinal: 0,
                    producer_id: Some(0),
                },
                v2::PruningInputEdge {
                    consumer_id: Some(0),
                    input_ordinal: 1,
                    producer_id: Some(u32::MAX),
                },
            ],
            columns: vec![
                v2::PruningColumnTrace {
                    column_ordinal: 0,
                    value_ids: vec![0, u32::MAX, 0],
                },
                v2::PruningColumnTrace {
                    column_ordinal: 1,
                    value_ids: vec![u32::MAX, u32::MAX, 0],
                },
            ],
        }],
    }
}

#[test]
fn pruning_retains_complete_typed_roots_context_and_ordered_paths() {
    let pruning = v2::FrozenPruning {
        witnesses: vec![
            pruning_witness(0, v2::PruningDomainField::Enforced),
            pruning_witness(u32::MAX, v2::PruningDomainField::Unenforced),
        ],
    };
    roundtrip(&pruning);
    roundtrip(&v2::FrozenPruning::default());
    let source = &pruning.witnesses[0].sources[0];
    let mut reordered = source.clone();
    reordered.input_path.reverse();
    assert_ne!(source.encode_to_vec(), reordered.encode_to_vec());
    let mut reordered = source.clone();
    reordered.columns.reverse();
    assert_ne!(source.encode_to_vec(), reordered.encode_to_vec());
    let mut changed = source.clone();
    changed.conjunct_path[1] = 0;
    assert_ne!(source.encode_to_vec(), changed.encode_to_vec());
    let mut changed = source.clone();
    changed.columns[0].value_ids.pop();
    assert_ne!(source.encode_to_vec(), changed.encode_to_vec());
    let mut changed = source.clone();
    changed.responsibility.as_mut().unwrap().root_ordinal = Some(1);
    assert_ne!(source.encode_to_vec(), changed.encode_to_vec());
    let mut changed = source.clone();
    changed.context.as_mut().unwrap().demand = control::EvaluationDemand::Value as i32;
    assert_ne!(source.encode_to_vec(), changed.encode_to_vec());
    // This is a representation fixture; sparse references and a path alone
    // do not prove that these sources, domains or transforms are admissible.
}

#[test]
fn pruning_reference_presence_and_unspecified_defaults_cannot_be_confused() {
    for id in [0, u32::MAX] {
        let witness = pruning_witness(id, v2::PruningDomainField::Enforced);
        let target = witness.target.unwrap();
        roundtrip(&target);
        for absent in [
            v2::PruningDomainSite {
                fragment_id: None,
                ..target
            },
            v2::PruningDomainSite {
                scan_id: None,
                ..target
            },
            v2::PruningDomainSite {
                occurrence_id: None,
                ..target
            },
        ] {
            roundtrip(&absent);
            assert_ne!(target.encode_to_vec(), absent.encode_to_vec());
        }
        let reference = witness.sources[0].responsibility.as_ref().unwrap();
        for absent in [
            v2::PredicateResponsibilityRef {
                fragment_id: None,
                ..*reference
            },
            v2::PredicateResponsibilityRef {
                use_id: None,
                ..*reference
            },
            v2::PredicateResponsibilityRef {
                node_id: None,
                ..*reference
            },
            v2::PredicateResponsibilityRef {
                root_ordinal: None,
                ..*reference
            },
        ] {
            roundtrip(&absent);
            assert_ne!(reference.encode_to_vec(), absent.encode_to_vec());
        }
        let edge = v2::PruningInputEdge {
            consumer_id: Some(id),
            input_ordinal: 0,
            producer_id: Some(id),
        };
        for absent in [
            v2::PruningInputEdge {
                consumer_id: None,
                ..edge
            },
            v2::PruningInputEdge {
                producer_id: None,
                ..edge
            },
        ] {
            roundtrip(&absent);
            assert_ne!(edge.encode_to_vec(), absent.encode_to_vec());
        }
    }
    let default = v2::PruningDomainSite::default();
    roundtrip(&default);
    assert_eq!(default.field, v2::PruningDomainField::Unspecified as i32);
    assert!(
        default.fragment_id.is_none()
            && default.scan_id.is_none()
            && default.occurrence_id.is_none()
    );
    assert_ne!(default.field, v2::PruningDomainField::Enforced as i32);
    assert_ne!(default.field, v2::PruningDomainField::Unenforced as i32);
    assert!(v2::PruningDomainField::try_from(99).is_err());
    let unknown = v2::PruningDomainSite {
        field: 99,
        ..default
    };
    roundtrip(&unknown);
    // Prost preserves missing references and unknown enum numbers. The later
    // typed decoder must reject them; no defaulting or admission is tested here.
}

#[test]
fn pruning_descriptor_exposes_only_exact_references_and_flat_path_fields() {
    let pool = pool();
    for (message, fields) in [
        (
            "PruningDomainSite",
            &["fragment_id", "scan_id", "occurrence_id"][..],
        ),
        (
            "PredicateResponsibilityRef",
            &["fragment_id", "node_id", "root_ordinal", "use_id"][..],
        ),
        ("PruningSourceWitness", &["responsibility", "context"][..]),
        ("PruningInputEdge", &["consumer_id", "producer_id"][..]),
    ] {
        let descriptor = pool
            .get_message_by_name(&format!("novarocks.physical_semantics_v2.{message}"))
            .unwrap();
        for name in fields {
            let field = descriptor.get_field_by_name(name).unwrap();
            assert!(field.supports_presence(), "{}", field.full_name());
        }
    }
    for name in [
        "FrozenPruning",
        "PruningDomainWitness",
        "PruningDomainSite",
        "PredicateResponsibilityRef",
        "PruningSourceWitness",
        "PruningInputEdge",
        "PruningColumnTrace",
    ] {
        let message = pool
            .get_message_by_name(&format!("novarocks.physical_semantics_v2.{name}"))
            .unwrap();
        assert!(
            !message
                .fields()
                .any(|field| matches!(field.kind(), Kind::Bool))
        );
        assert!(
            !message
                .fields()
                .any(|field| matches!(field.name(), "safe" | "implication" | "legal_transform"))
        );
    }
    for (message, field) in [
        ("PruningSourceWitness", "conjunct_path"),
        ("PruningColumnTrace", "value_ids"),
    ] {
        let descriptor = pool
            .get_message_by_name(&format!("novarocks.physical_semantics_v2.{message}"))
            .unwrap();
        let field = descriptor.get_field_by_name(field).unwrap();
        assert_eq!(field.cardinality(), prost_reflect::Cardinality::Repeated);
        assert!(matches!(field.kind(), Kind::Uint32));
        assert!(field.is_packed());
    }
    let site = pool
        .get_message_by_name("novarocks.physical_semantics_v2.PruningDomainSite")
        .unwrap();
    let Kind::Enum(field) = site.get_field_by_name("field").unwrap().kind() else {
        panic!("target field requires a closed enum")
    };
    assert_eq!(
        field
            .values()
            .map(|value| (value.number(), value.name().to_owned()))
            .collect::<Vec<_>>(),
        vec![
            (0, "PRUNING_DOMAIN_FIELD_UNSPECIFIED".into()),
            (1, "PRUNING_DOMAIN_FIELD_ENFORCED".into()),
            (2, "PRUNING_DOMAIN_FIELD_UNENFORCED".into()),
        ]
    );
}

#[test]
fn call_policy_presence_preserves_absent_unspecified_and_unknown_for_typed_decode() {
    // Independent canonical wire fixtures: both call owners use field 4 varints.
    // Prost intentionally retains all distinctions for the later typed codec.
    assert_eq!(
        v2::ExpressionCallClaim::decode(&b""[..])
            .unwrap()
            .decimal_overflow_policy,
        None
    );
    for (bytes, value) in [
        ([0x20, 0x00], 0),
        ([0x20, 0x01], 1),
        ([0x20, 0x02], 2),
        ([0x20, 0x63], 99),
    ] {
        let decoded = v2::ExpressionCallClaim::decode(bytes.as_slice()).unwrap();
        assert_eq!(decoded.decimal_overflow_policy, Some(value));
        assert_eq!(decoded.encode_to_vec(), bytes);
        let relation = v2::FrozenCall::decode(bytes.as_slice()).unwrap();
        assert_eq!(relation.decimal_overflow_policy, Some(value));
        assert_eq!(relation.encode_to_vec(), bytes);
    }
    assert_eq!(v2::DecimalOverflowPolicy::OutputNull as i32, 1);
    assert_eq!(v2::DecimalOverflowPolicy::ReportError as i32, 2);
    assert!(v2::DecimalOverflowPolicy::try_from(99).is_err());
    // These are representation checks. None/0/99 must be refused by the future
    // typed semantic decoder; this test does not claim that decoder exists.
}

#[test]
fn calls_and_binary_share_one_semantic_policy_with_unchanged_numeric_wire_values() {
    let pool = pool();
    for (message, tag, presence) in [
        ("novarocks.physical_semantics_v2.FrozenCall", 4, true),
        (
            "novarocks.physical_semantics_v2.ExpressionCallClaim",
            4,
            true,
        ),
    ] {
        let descriptor = pool.get_message_by_name(message).unwrap();
        let field = descriptor
            .get_field_by_name("decimal_overflow_policy")
            .unwrap();
        assert_eq!(field.number(), tag);
        assert_eq!(field.supports_presence(), presence);
        let Kind::Enum(policy) = field.kind() else {
            panic!("policy is one closed semantic enum")
        };
        assert_eq!(
            policy.full_name(),
            "novarocks.physical_semantics_v2.DecimalOverflowPolicy"
        );
        assert_eq!(
            policy.values().map(|v| v.number()).collect::<Vec<_>>(),
            [0, 1, 2]
        );
    }
}

#[test]
fn retired_scalar_source_claim_fields_are_reserved() {
    let pool = pool();
    let message = pool
        .get_message_by_name("novarocks.physical_semantics_v2.ExpressionCallClaim")
        .unwrap();
    for (number, name) in [
        (6, "regexp_count_pattern_source"),
        (7, "to_base64_byte_source"),
    ] {
        assert!(
            message
                .reserved_ranges()
                .any(|range| range.contains(&number))
        );
        assert!(message.reserved_names().any(|reserved| reserved == name));
        assert!(message.get_field_by_name(name).is_none());
    }
    let decoded = v2::ExpressionCallClaim::decode(&[0x30, 0, 0x38, 1][..]).unwrap();
    assert_eq!(decoded, v2::ExpressionCallClaim::default());
}
