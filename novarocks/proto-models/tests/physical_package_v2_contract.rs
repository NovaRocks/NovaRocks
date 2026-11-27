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

//! Carrier shape and protobuf representation checks. These are not typed
//! package validation, IPC allocation bounds, or bidirectional codec receipts.

use novarocks_proto_models::FILE_DESCRIPTOR_SET;
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, Kind, MessageDescriptor, Value};
use std::collections::{BTreeMap, BTreeSet};

fn pool() -> DescriptorPool {
    DescriptorPool::decode(FILE_DESCRIPTOR_SET).expect("canonical native descriptor set")
}

#[test]
fn aggregate_state_argument_contract_has_explicit_nonzero_values_and_tag_seven() {
    use novarocks_proto_models::common::AggregateStateArgumentContract;
    use novarocks_proto_models::physical_package_v2::AggregateBindingDefinition;
    assert_eq!(AggregateStateArgumentContract::ExactSignature as i32, 1);
    assert_eq!(
        AggregateStateArgumentContract::ValueRootNullabilityIndependent as i32,
        2
    );
    let descriptor = pool()
        .get_message_by_name("novarocks.physical_package_v2.AggregateBindingDefinition")
        .unwrap();
    let field = descriptor
        .get_field_by_name("state_argument_contract")
        .unwrap();
    assert_eq!(field.number(), 7);
    assert!(matches!(field.kind(), Kind::Enum(_)));
    for code in [1, 2] {
        let value = AggregateBindingDefinition {
            state_argument_contract: code,
            ..Default::default()
        };
        // Independently authored tag/type/value bytes, not an encoder roundtrip.
        assert_eq!(value.encode_to_vec(), vec![0x38, code as u8]);
        assert_eq!(
            AggregateBindingDefinition::decode([0x38, code as u8].as_slice())
                .unwrap()
                .state_argument_contract,
            code,
        );
    }
    // DTO decode retains omission/unknown; the receiving owner rejects them.
    assert_eq!(
        AggregateBindingDefinition::decode([].as_slice())
            .unwrap()
            .state_argument_contract,
        0
    );
    assert_eq!(
        AggregateBindingDefinition::decode([0x38, 3].as_slice())
            .unwrap()
            .state_argument_contract,
        3
    );
}

fn depth(
    message: MessageDescriptor,
    path: &mut BTreeSet<String>,
    memo: &mut BTreeMap<String, usize>,
) -> usize {
    if let Some(depth) = memo.get(message.full_name()) {
        return *depth;
    }
    assert!(
        path.insert(message.full_name().to_owned()),
        "recursive carrier at {}",
        message.full_name()
    );
    let result = message
        .fields()
        .filter_map(|field| match field.kind() {
            Kind::Message(child) => Some(1 + depth(child, path, memo)),
            _ => None,
        })
        .max()
        .unwrap_or(1);
    path.remove(message.full_name());
    memo.insert(message.full_name().to_owned(), result);
    result
}

#[test]
fn complete_carrier_has_fixed_message_depth_and_explicit_duplicate_tables() {
    let pool = pool();
    let mut memo = BTreeMap::new();
    let messages = pool
        .all_messages()
        .filter(|message| message.package_name() == "novarocks.physical_package_v2")
        .collect::<Vec<_>>();
    assert!(!messages.is_empty());
    for message in messages {
        // Every reference follows a finite descriptor path, even if actual
        // semantic definition/value graphs share nodes or have sparse IDs.
        depth(message.clone(), &mut BTreeSet::new(), &mut memo);
        for field in message.fields() {
            assert!(
                !field.is_map(),
                "{} silently overwrites duplicate entries",
                field.full_name()
            );
        }
    }
    println!(
        "complete fragment protobuf message depth: {}",
        memo["novarocks.physical_package_v2.FragmentPackage"]
    );
}

#[test]
fn singular_numeric_references_preserve_zero_maximum_and_absence() {
    let pool = pool();
    let mut references = 0;
    for message in pool
        .all_messages()
        .filter(|message| message.package_name() == "novarocks.physical_package_v2")
    {
        for field in message.fields().filter(|field| {
            !field.is_list()
                && field.name().ends_with("_id")
                && matches!(field.kind(), Kind::Uint32)
        }) {
            assert!(
                field.supports_presence(),
                "{} uses a missing reference as zero",
                field.full_name()
            );
            references += 1;
            let missing = DynamicMessage::new(message.clone());
            for id in [0, u32::MAX] {
                let mut value = DynamicMessage::new(message.clone());
                value.set_field(&field, Value::U32(id));
                assert!(value.has_field(&field));
                let encoded = value.encode_to_vec();
                assert_ne!(encoded, missing.encode_to_vec());
                let decoded = DynamicMessage::decode(message.clone(), encoded.as_slice()).unwrap();
                assert!(decoded.has_field(&field));
                assert_eq!(decoded.get_field(&field).as_ref(), &Value::U32(id));
            }
        }
    }
    assert!(references > 0);
    let public = pool
        .get_message_by_name("novarocks.physical_package_v2.ConnectorReadPublicFacts")
        .unwrap();
    for (number, name) in [(3, "schema_id"), (4, "logical_types")] {
        assert!(public.get_field(number).is_none());
        assert!(public.get_field_by_name(name).is_none());
        assert!(
            public
                .reserved_ranges()
                .any(|range| range.contains(&number))
        );
        assert!(public.reserved_names().any(|reserved| reserved == name));
    }
    for (number, name) in [(5, "execution_schema_id"), (6, "source_schema_id")] {
        let field = public.get_field_by_name(name).unwrap();
        assert_eq!(field.number(), number);
        assert!(field.supports_presence());
    }
    use novarocks_proto_models::physical_package_v2::ConnectorReadPublicFacts;
    // Retired tags cannot create an implicit zero or author meaning. The typed
    // codec separately rejects the missing pair; this is representation only.
    let retired = ConnectorReadPublicFacts::decode(&[0x18, 1, 0x22, 1, 0][..]).unwrap();
    assert!(retired.execution_schema_id.is_none() && retired.source_schema_id.is_none());
    assert_eq!(retired, ConnectorReadPublicFacts::default());
    let explicit = ConnectorReadPublicFacts::decode(&[0x28, 0, 0x30, 0][..]).unwrap();
    assert_eq!(explicit.execution_schema_id, Some(0));
    assert_eq!(explicit.source_schema_id, Some(0));
    assert_eq!(explicit.encode_to_vec(), [0x28, 0, 0x30, 0]);
}

#[test]
fn execution_enums_preserve_unspecified_or_explicit_plain_zero_and_unknown_wire_values() {
    let pool = pool();
    let mut enums = BTreeSet::new();
    for message in pool
        .all_messages()
        .filter(|message| message.package_name() == "novarocks.physical_package_v2")
    {
        for field in message.fields() {
            let Kind::Enum(enumeration) = field.kind() else {
                continue;
            };
            if enumeration.package_name() != "novarocks.physical_package_v2" {
                continue;
            }
            enums.insert(enumeration.full_name().to_owned());
            let zero = enumeration.get_value(0).unwrap();
            if enumeration.full_name() == "novarocks.physical_package_v2.ResultValueDomain" {
                // This optional field distinguishes absent domain from explicit PLAIN.
                assert_eq!(zero.name(), "RESULT_VALUE_DOMAIN_PLAIN");
                assert_eq!(
                    field.full_name(),
                    "novarocks.physical_package_v2.ResultField.domain"
                );
                assert!(field.supports_presence());
                let absent = DynamicMessage::new(message.clone());
                let mut plain = DynamicMessage::new(message.clone());
                plain.set_field(&field, Value::EnumNumber(0));
                assert!(!absent.has_field(&field));
                assert!(plain.has_field(&field));
                let encoded = plain.encode_to_vec();
                assert_ne!(encoded, absent.encode_to_vec());
                let decoded = DynamicMessage::decode(message.clone(), encoded.as_slice()).unwrap();
                assert!(decoded.has_field(&field));
                assert_eq!(decoded.get_field(&field).as_ref(), &Value::EnumNumber(0));
            } else {
                assert!(
                    zero.name().ends_with("UNSPECIFIED"),
                    "{}",
                    enumeration.full_name()
                );
            }
            if field.is_list() {
                continue;
            }
            // Prost owns wire legality; unknown execution vocabulary survives
            // decoding so the typed ingress can reject it, rather than choose
            // an apparently valid kernel/default policy.
            let mut value = DynamicMessage::new(message.clone());
            value.set_field(&field, Value::EnumNumber(i32::MAX));
            let encoded = value.encode_to_vec();
            let decoded = DynamicMessage::decode(message.clone(), encoded.as_slice()).unwrap();
            assert_eq!(
                decoded.get_field(&field).as_ref(),
                &Value::EnumNumber(i32::MAX)
            );
        }
    }
    assert!(!enums.is_empty());
}

#[test]
fn typed_pool_references_preserve_sharing_order_and_zero_ordinals() {
    use novarocks_proto_models::{physical_package_v2 as v2, physical_type_v2 as ty};
    // This checks the representation only. IPC validation is the codec's
    // obligation; these opaque bytes are deliberately not an IPC fixture.
    let pool = v2::IpcConstantPool {
        id: u32::MAX,
        value_type_id: Some(0),
        source_field_id: Some(u32::MAX),
        compression: v2::IpcCompression::Uncompressed as i32,
        arrow_ipc: vec![0, 255, 0, 128],
    };
    let reference = v2::ConstantReference {
        pool_id: Some(u32::MAX),
        row_ordinal: 0,
    };
    let definitions = [u32::MAX, 0, 71]
        .into_iter()
        .map(|id| v2::ExpressionDefinition {
            id,
            lambda_scope_expr_id: None,
            value_type_id: Some(0),
            kind: Some(v2::expression_definition::Kind::Constant(reference)),
        })
        .collect::<Vec<_>>();
    let package = v2::FragmentPackage {
        constants: vec![pool.clone()],
        types: Some(ty::TypeTable {
            logical_types: vec![novarocks_proto_models::value_type::LogicalTypeDefinition {
                id: Some(0),
                nullable: true,
                kind: Some(
                    novarocks_proto_models::value_type::logical_type_definition::Kind::Nominal(
                        novarocks_proto_models::value_type::NominalValue::Json as i32,
                    ),
                ),
            }],
            carriers: vec![ty::CarrierTypeDefinition {
                id: 0,
                kind: Some(ty::carrier_type_definition::Kind::Primitive(
                    novarocks_proto_models::plan::ArrowPrimitiveType::Utf8 as i32,
                )),
            }],
            value_types: vec![ty::ValueTypeDefinition {
                id: 0,
                carrier_type_id: Some(0),
                logical_type_id: Some(0),
            }],
            ..Default::default()
        }),
        fragment: Some(v2::Fragment {
            id: 0,
            root_node_id: Some(u32::MAX),
            nodes: vec![v2::PhysicalNode {
                id: u32::MAX,
                expressions: Some(v2::ExpressionUnit {
                    definitions: definitions.clone(),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        }),
        ..Default::default()
    };
    let descriptor = self::pool()
        .get_message_by_name("novarocks.physical_package_v2.ExpressionDefinition")
        .unwrap();
    assert_eq!(
        descriptor.get_field_by_name("constant").unwrap().number(),
        7
    );
    assert!(descriptor.get_field_by_name("literal").is_none());
    assert!(descriptor.reserved_names().any(|name| name == "literal"));
    let bytes = package.encode_to_vec();
    let decoded = v2::FragmentPackage::decode(bytes.as_slice()).unwrap();
    assert_eq!(decoded, package);
    assert_eq!(decoded.constants, [pool]);
    assert_eq!(
        decoded.fragment.unwrap().nodes[0]
            .expressions
            .as_ref()
            .unwrap()
            .definitions,
        definitions
    );
    assert_ne!(
        reference.encode_to_vec(),
        v2::ConstantReference::default().encode_to_vec()
    );
}

#[test]
fn intrinsic_allow_throw_references_use_dedicated_v2_messages_and_presence() {
    let pool = pool();
    let descriptor = pool
        .get_message_by_name("novarocks.physical_package_v2.BinaryExpression")
        .unwrap();
    let field = descriptor
        .get_field_by_name("allow_throw_exception")
        .unwrap();
    assert_eq!(field.number(), 5);
    assert!(field.supports_presence());
    assert!(!field.is_list());
    let Kind::Message(reference) = field.kind() else {
        panic!("intrinsic uses a typed reference");
    };
    assert_eq!(
        reference.full_name(),
        "novarocks.physical_semantics_v2.SemanticParameterRef"
    );
    assert_eq!(
        reference.get_field_by_name("id").unwrap().kind(),
        Kind::Uint32
    );
    assert!(
        reference
            .get_field_by_name("id")
            .unwrap()
            .supports_presence()
    );
}

#[test]
fn cast_mode_has_presence_and_retired_conversion_fields_remain_reserved() {
    use novarocks_proto_models::physical_package_v2 as v2;
    let descriptor = pool()
        .get_message_by_name("novarocks.physical_package_v2.CastExpression")
        .unwrap();
    let mode = descriptor.get_field_by_name("cast_mode").unwrap();
    assert_eq!(mode.number(), 6);
    assert!(mode.supports_presence());
    for tag in [3, 4, 5] {
        assert!(
            descriptor
                .reserved_ranges()
                .any(|range| range.contains(&tag))
        );
    }
    for name in ["decimal_overflow_policy", "allow_throw_exception"] {
        assert!(descriptor.reserved_names().any(|reserved| reserved == name));
    }

    let cast = v2::CastExpression::decode(&[0x18, 2, 0x22, 0, 0x30, 3][..]).unwrap();
    assert_eq!(cast.cast_mode, Some(v2::TrinoCastMode::Implicit as i32));
    assert_eq!(cast.encode_to_vec(), [0x30, 3]);
    assert!(v2::CastExpression::default().cast_mode.is_none());
}

#[test]
fn intrinsic_reference_raw_wire_distinguishes_absence_zero_max_and_unknown_key() {
    use novarocks_proto_models::{physical_package_v2 as v2, physical_semantics_v2 as semantics};
    // Binary field 5 wraps an explicit zero ID and the closed ALLOW key (3).
    let binary = v2::BinaryExpression::decode(&[0x2a, 4, 8, 0, 16, 3][..]).unwrap();
    let zero = semantics::SemanticParameterRef {
        id: Some(0),
        expected_key: semantics::SemanticParameterKey::AllowThrowException as i32,
    };
    assert_eq!(binary.allow_throw_exception, Some(zero));
    assert_eq!(binary.encode_to_vec(), [0x2a, 4, 8, 0, 16, 3]);
    assert!(
        v2::BinaryExpression::decode(&[][..])
            .unwrap()
            .allow_throw_exception
            .is_none()
    );
    let empty_binary = v2::BinaryExpression::decode(&[0x2a, 0][..]).unwrap();
    assert_eq!(
        empty_binary.allow_throw_exception,
        Some(semantics::SemanticParameterRef::default())
    );
    assert_ne!(
        empty_binary.encode_to_vec(),
        v2::BinaryExpression::default().encode_to_vec()
    );
    for id in [0, u32::MAX] {
        for expected_key in [
            semantics::SemanticParameterKey::AllowThrowException as i32,
            0,
            i32::MAX,
        ] {
            let reference = semantics::SemanticParameterRef {
                id: Some(id),
                expected_key,
            };
            let binary = v2::BinaryExpression {
                allow_throw_exception: Some(reference),
                ..Default::default()
            };
            let cast = v2::CastExpression {
                ..Default::default()
            };
            assert_eq!(
                v2::BinaryExpression::decode(binary.encode_to_vec().as_slice()).unwrap(),
                binary
            );
            assert_eq!(
                v2::CastExpression::decode(cast.encode_to_vec().as_slice()).unwrap(),
                cast
            );
        }
    }
    // This DTO preserves absent/unknown facts for typed rejection; it does not
    // itself prove arithmetic presence, key legality or parameter closure.
}

#[test]
fn intrinsic_scoped_refs_and_false_true_parameters_survive_one_package_carrier() {
    use novarocks_proto_models::{physical_package_v2 as v2, physical_semantics_v2 as semantics};
    let reference = |id| semantics::SemanticParameterRef {
        id: Some(id),
        expected_key: semantics::SemanticParameterKey::AllowThrowException as i32,
    };
    let expressions = vec![
        v2::ExpressionDefinition {
            id: 0,
            kind: Some(v2::expression_definition::Kind::Binary(
                v2::BinaryExpression {
                    left_expr_id: Some(0),
                    right_expr_id: Some(u32::MAX),
                    op: v2::BinaryOperator::Multiply as i32,
                    decimal_overflow_policy: semantics::DecimalOverflowPolicy::OutputNull as i32,
                    allow_throw_exception: Some(reference(0)),
                },
            )),
            ..Default::default()
        },
        v2::ExpressionDefinition {
            id: u32::MAX,
            kind: Some(v2::expression_definition::Kind::Cast(v2::CastExpression {
                expr_id: Some(0),
                target_carrier_type_id: Some(u32::MAX),
                cast_mode: Some(v2::TrinoCastMode::Implicit as i32),
            })),
            ..Default::default()
        },
    ];
    let parameters = semantics::SemanticParameters {
        entries: vec![
            semantics::SemanticParameter {
                id: 0,
                value: Some(semantics::semantic_parameter::Value::AllowThrowException(
                    false,
                )),
            },
            semantics::SemanticParameter {
                id: u32::MAX,
                value: Some(semantics::semantic_parameter::Value::AllowThrowException(
                    true,
                )),
            },
        ],
    };
    let package = v2::FragmentPackage {
        fragment: Some(v2::Fragment {
            nodes: vec![v2::PhysicalNode {
                expressions: Some(v2::ExpressionUnit {
                    definitions: expressions.clone(),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        }),
        parameters: Some(parameters.clone()),
        ..Default::default()
    };
    let decoded = v2::FragmentPackage::decode(package.encode_to_vec().as_slice()).unwrap();
    assert_eq!(
        decoded.fragment.unwrap().nodes[0]
            .expressions
            .as_ref()
            .unwrap()
            .definitions,
        expressions
    );
    assert_eq!(decoded.parameters, Some(parameters));
    // Deliberately carrier-only: no fake full FragmentPackage validation claim.
}

#[test]
fn original_requests_keep_missing_distinct_from_explicit_empty_and_present_zero() {
    use novarocks_proto_models::physical_package_v2 as v2;
    // Independent bytes: Fragment.call_requests tag 9, explicit empty message.
    let missing = v2::Fragment::decode([].as_slice()).unwrap();
    let empty = v2::Fragment::decode([0x4a, 0x00].as_slice()).unwrap();
    assert!(missing.call_requests.is_none());
    assert!(empty.call_requests.as_ref().unwrap().entries.is_empty());
    assert_eq!(empty.encode_to_vec(), [0x4a, 0x00]);
    let reserved_policy = v2::FragmentPackage::decode([0xda, 0x01, 0x00].as_slice()).unwrap();
    assert!(reserved_policy.encode_to_vec().is_empty());
    let reserved_request_policy = v2::OriginalCallRequest::decode([0x2a, 0x00].as_slice()).unwrap();
    assert!(reserved_request_policy.encode_to_vec().is_empty());
    let request = v2::OriginalCallRequest::decode([0x18, 0, 0x20, 0].as_slice()).unwrap();
    assert_eq!(request.logical_argument_count, Some(0));
    assert_eq!(request.expected_result_value_type_id, Some(0));
    let no_constraint = v2::OriginalCallRequest::decode([0x18, 0].as_slice()).unwrap();
    assert_eq!(no_constraint.logical_argument_count, Some(0));
    assert_eq!(no_constraint.expected_result_value_type_id, None);
}

#[test]
fn original_request_definition_does_not_alias_expression_occurrence_namespace() {
    use novarocks_proto_models::{
        call_request, physical_package_v2 as v2, physical_semantics_v2 as sem,
    };
    // Independent bytes: a unit-local definition key and an actual relation
    // node key both preserve explicit zero in their distinct message grammar.
    let definition = call_request::UnitCallRequest::decode([0x08, 0].as_slice()).unwrap();
    let occurrence = v2::CallRequestDefinition::decode([0x12, 2, 0x30, 0].as_slice()).unwrap();
    assert_eq!(definition.definition_id, Some(0));
    assert_eq!(
        occurrence.kind,
        Some(v2::call_request_definition::Kind::Relational(
            sem::CallSite {
                kind: Some(sem::call_site::Kind::TableNodeId(0))
            }
        ))
    );
    assert_ne!(
        definition.encode_to_vec(),
        call_request::UnitCallRequest::default().encode_to_vec()
    );
    assert_eq!(definition.encode_to_vec(), [0x08, 0]);
    assert_eq!(occurrence.encode_to_vec(), [0x12, 2, 0x30, 0]);
    // A retired expression-definition key is not reinterpreted as a relation.
    let retired = v2::CallRequestDefinition::decode([0x08, 0].as_slice()).unwrap();
    assert!(retired.kind.is_none());
    let descriptor = pool()
        .get_message_by_name("novarocks.call_request.CallRequestDefinition")
        .unwrap();
    assert!(descriptor.reserved_ranges().any(|range| range.contains(&1)));
    // DTO retention/omission is not the preflight's retired-field rejection.
}

#[test]
fn original_call_requests_reserve_retired_identity_and_policy_fields() {
    let pool = pool();
    let request = pool
        .get_message_by_name("novarocks.call_request.OriginalCallRequest")
        .unwrap();
    for (name, number) in [("constant_policy", 5), ("request_id", 6)] {
        assert!(request.get_field_by_name(name).is_none());
        assert!(
            request
                .reserved_ranges()
                .any(|range| range.contains(&number))
        );
        assert!(request.reserved_names().any(|reserved| reserved == name));
    }
    for (name, number) in [
        ("definition", 1),
        ("arguments", 2),
        ("logical_argument_count", 3),
        ("expected_result_value_type_id", 4),
    ] {
        assert_eq!(request.get_field_by_name(name).unwrap().number(), number);
    }
}

#[test]
fn node_unit_roots_ports_claims_and_requests_preserve_actual_optional_references() {
    use novarocks_proto_models::{
        call_request, physical_control_v2 as control, physical_package_v2 as v2,
        physical_semantics_v2 as sem,
    };
    for id in [0, u32::MAX] {
        let root = v2::ExpressionRoot {
            definition_id: Some(id),
            use_id: Some(id),
            demand: control::EvaluationDemand::Value as i32,
            scope: Some(v2::RootScope {
                kind: Some(v2::root_scope::Kind::Port(0)),
            }),
        };
        for absent in [
            v2::ExpressionRoot {
                definition_id: None,
                ..root
            },
            v2::ExpressionRoot {
                use_id: None,
                ..root
            },
            v2::ExpressionRoot {
                scope: None,
                ..root
            },
        ] {
            assert_ne!(root.encode_to_vec(), absent.encode_to_vec());
            assert_eq!(
                v2::ExpressionRoot::decode(absent.encode_to_vec().as_slice()).unwrap(),
                absent
            );
        }
        let input = v2::ExpressionInput {
            value_id: Some(id),
            value_type_id: Some(id),
        };
        for absent in [
            v2::ExpressionInput {
                value_id: None,
                ..input
            },
            v2::ExpressionInput {
                value_type_id: None,
                ..input
            },
        ] {
            assert_ne!(input.encode_to_vec(), absent.encode_to_vec());
        }
        let unit = v2::ExpressionUnit {
            ports: vec![v2::ExpressionPort {
                values: vec![input, input],
            }],
            roots: vec![
                root,
                v2::ExpressionRoot {
                    scope: Some(v2::RootScope {
                        kind: Some(v2::root_scope::Kind::AllPorts(control::Empty {})),
                    }),
                    ..root
                },
            ],
            control: Some(control::ExpressionControl {
                regions: vec![control::EvaluationRegion {
                    id,
                    parent_region_id: Some(0),
                    guard: None,
                }],
                uses: vec![control::ExpressionUse {
                    id,
                    definition_id: Some(id),
                    region_id: Some(id),
                    argument_use_ids: vec![0, id, 0],
                    ..Default::default()
                }],
            }),
            claims: Some(sem::ExpressionClaims {
                entries: vec![sem::ExpressionCallClaim {
                    use_id: Some(id),
                    context: Some(sem::ExpressionEffectContext {
                        use_id: Some(id),
                        region_id: Some(id),
                        demand: control::EvaluationDemand::TruthOnly as i32,
                    }),
                    ..Default::default()
                }],
            }),
            requests: vec![call_request::UnitCallRequest {
                definition_id: Some(id),
                logical_argument_count: Some(0),
                expected_result_value_type_id: Some(id),
                ..Default::default()
            }],
            ..Default::default()
        };
        let node = v2::PhysicalNode {
            id,
            expressions: Some(unit.clone()),
            ..Default::default()
        };
        let decoded = v2::PhysicalNode::decode(node.encode_to_vec().as_slice()).unwrap();
        assert_eq!(decoded, node);
        assert_eq!(decoded.expressions.unwrap(), unit);
        // Carrier fidelity permits arbitrary numeric references. The unit's
        // structural, ownership and mandatory-claim checks belong to its decoder.
    }
    let descriptor = pool()
        .get_message_by_name("novarocks.physical_package_v2.PhysicalNode")
        .unwrap();
    assert_eq!(
        descriptor
            .get_field_by_name("expressions")
            .unwrap()
            .number(),
        30
    );
    let fragment = pool()
        .get_message_by_name("novarocks.physical_package_v2.Fragment")
        .unwrap();
    assert!(fragment.get_field_by_name("expressions").is_none());
    assert!(fragment.reserved_ranges().any(|range| range.contains(&4)));
}
