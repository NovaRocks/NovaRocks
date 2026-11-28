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

use super::codec::*;
use super::identity::*;
use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorExactSemanticRevision, ConnectorProviderId, ConnectorTableObjectId,
    IcebergSourcePrimitive, IcebergSourceType, IcebergSourceTypeNode, ProviderSourceType,
};

macro_rules! identity_fixture {
    ($name:ident, $kind:ty) => {
        fn $name(value: u8) -> $kind {
            <$kind>::try_new(vec![value]).expect("valid fixture identity")
        }
    };
}

identity_fixture!(object_id, ObjectIdentity);
identity_fixture!(field_id, FieldIdentity);
identity_fixture!(schema_version, SchemaVersion);
identity_fixture!(partition_spec_version, PartitionSpecVersion);

identity_fixture!(state_slot_id, StateSlotIdentity);
identity_fixture!(aggregate_id, AggregateIdentity);
identity_fixture!(branch_id, BranchIdentity);
identity_fixture!(apply_key_id, ApplyKeyIdentity);

pub fn fixture_type(signature: &str) -> ProviderSourceType {
    let primitive = match signature {
        "bigint" | "long" => IcebergSourcePrimitive::Long,
        "integer" | "int" => IcebergSourcePrimitive::Int,
        "varchar" | "string" => IcebergSourcePrimitive::String,
        "binary" | "varbinary" => IcebergSourcePrimitive::Binary,
        "double" => IcebergSourcePrimitive::Double,
        "decimal(18,2)" => IcebergSourcePrimitive::Decimal {
            precision: 18,
            scale: 2,
        },
        "decimal(38,2)" => IcebergSourcePrimitive::Decimal {
            precision: 38,
            scale: 2,
        },
        _ => panic!("unsupported fixture type {signature}"),
    };
    ProviderSourceType::Iceberg(
        IcebergSourceType::try_new(vec![IcebergSourceTypeNode::Primitive(primitive)]).unwrap(),
    )
}

fn field(id: u8, name: &str, signature: &str, nullable: bool) -> BoundSourcePath {
    BoundSourcePath {
        nodes: vec![BoundSourceNode {
            field_id: field_id(id),
            name_at_binding: name.to_owned(),
            required: !nullable,
            kind: SourceNodeKind::Scalar,
        }],
        leaf_type: fixture_type(signature),
        whole_subtree: false,
    }
}

/// A source object identity as CREATE records it: the canonical exact-fact
/// envelope around the provider's own object value, not the bare value.
pub fn source_object_identity(object_value: u8) -> ObjectIdentity {
    source_object_identity_bytes(&[object_value])
}

/// A persisted source object identity, encoded the way the documents encode
/// one: the provider's identity inside the application's own fact envelope.
///
/// Fixtures that store the bare identity instead compare equal to a bare probe
/// and unequal to nothing, which is how a dependency guard that never matched
/// went unnoticed.
pub fn source_object_identity_bytes(object: &[u8]) -> ObjectIdentity {
    let object = ConnectorTableObjectId::try_new(Bytes::copy_from_slice(object))
        .expect("fixture source object");
    let revision =
        novarocks_spi::connector::ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
            novarocks_spi::connector::ConnectorProviderId::parse("iceberg")
                .expect("fixture provider"),
            &object,
            Some(9),
        )
        .expect("fixture source revision");
    crate::persistence::exact_revision::persist_exact_connector_revision(&revision)
        .expect("fixture persisted source identity")
        .0
}

pub fn relation(occurrence_id: u32, object_value: u8, qualifier: &str) -> BindingOccurrence {
    let object_id = source_object_identity(object_value);
    let raw = super::exact_revision::restore_persisted_object(&object_id).unwrap();
    let exact = ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
        ConnectorProviderId::parse("iceberg").unwrap(),
        &raw,
        Some(9),
    )
    .unwrap();
    BindingOccurrence {
        occurrence_id,
        catalog_at_binding: "ice".to_string(),
        namespace_at_binding: "sales".to_string(),
        relation_at_binding: "orders".to_string(),
        qualifier_at_binding: qualifier.to_string(),
        selection: ReadSelection::Current,
        data_version: keyed::DataVersionFormat::from_issued_fact(exact.data_version()).unwrap(),
        facts: SourceBindingFacts {
            occurrence: Bytes::copy_from_slice(&occurrence_id.to_be_bytes()),
            object: Bytes::copy_from_slice(object_id.as_bytes()),
            field_paths: Bytes::from_static(b"fixture-paths"),
            field_types: Bytes::from_static(b"fixture-types"),
            read_declarations: Bytes::from_static(b"fixture-declarations"),
        },
        object_id,
        paths: vec![
            field(1, "order_id", "bigint", false),
            field(2, "amount", "decimal(18,2)", true),
        ],
    }
}

pub(crate) fn sample_definition() -> DefinitionDocument {
    build_definition(
        1_700_000_000_000,
        QuerySource {
            effective_sql: "SELECT AVG(o.amount) AS average_amount FROM ice.sales.orders o UNION ALL SELECT AVG(o2.amount) AS average_amount FROM ice.sales.orders o2".to_string(),
            dialect: QueryDialect::Trino,
            resolution: ResolutionContext {
                default_catalog: "ice".to_string(),
                default_namespace: "sales".to_string(),
            },
        },
    )
    .expect("definition")
}

fn output_id() -> OutputIdentity {
    super::codec::output_identity(0, b"fixture-decimal18-2")
}

fn sample_binding() -> BindingDocument {
    BindingDocument {
        occurrences: vec![relation(7, 11, "o"), relation(8, 11, "o2")],
        outputs: vec![BoundOutput {
            ordinal: 0,
            name: "average_amount".to_string(),
            logical_type_identity: Bytes::from_static(b"fixture-decimal18-2"),
            source_fields: vec![
                SourceFieldReference {
                    occurrence_id: 7,
                    field_id: field_id(2),
                },
                SourceFieldReference {
                    occurrence_id: 8,
                    field_id: field_id(2),
                },
            ],
        }],
        dependencies: Vec::new(),
    }
}

pub fn fixture_mapping(
    binding: &BindingDocument,
    interpretation: &InterpretationDocument,
) -> StorageMappingDocument {
    let columns = binding
        .outputs
        .iter()
        .map(|output| {
            let interpreted = interpretation
                .outputs
                .iter()
                .find(|o| o.output_id == output.identity())
                .unwrap();
            StorageColumnMapping {
                output_ordinal: output.ordinal,
                storage_field_id: interpreted.target_field_id.clone(),
                logical_type_identity: output.logical_type_identity.clone(),
                storage_type: interpreted.source_type.clone(),
                storage_nullable: interpreted.nullable,
                provider_storage_declaration: Bytes::from_static(b"fixture-storage"),
                storage_format_version: 1,
                encode_identity: Bytes::from_static(b"fixture-encode"),
                decode_identity: Bytes::from_static(b"fixture-decode"),
                content_contract: Bytes::from_static(b"fixture-content"),
                pushdown: MappingPushdownClass::Identity,
            }
        })
        .collect();
    StorageMappingDocument { columns }
}

pub(crate) fn aggregate_contract(name: &str) -> novarocks_functions::PersistentAggregateContract {
    use novarocks_functions::{
        AggregateStateFormatIdentity, FunctionId, FunctionOverloadId, PersistentAggregateContract,
    };
    PersistentAggregateContract {
        function: FunctionId::try_new(format!("builtin.aggregate/{name}/v1")).unwrap(),
        overload: FunctionOverloadId::try_new("fixture-overload-v1").unwrap(),
        state_format: AggregateStateFormatIdentity::try_new("fixture-state-v1").unwrap(),
    }
}

pub(crate) fn physical(
    logical_identity: PhysicalFieldLogicalIdentity,
    target_field_value: u8,
    type_signature: &str,
    nullable: bool,
) -> PhysicalFieldBinding {
    PhysicalFieldBinding {
        logical_identity,
        target_field_id: field_id(target_field_value),
        source_type: fixture_type(type_signature),
        nullable,
    }
}

fn sample_interpretation(definition: &DefinitionDocument) -> InterpretationDocument {
    InterpretationDocument {
        definition_revision: super::keyed_documents::definition_revision(definition)
            .expect("fixture D semantic revision"),
        outputs: vec![OutputBinding {
            output_id: output_id(),
            target_field_id: field_id(31),
            source_type: fixture_type("decimal(18,2)"),
            nullable: true,
        }],
        state_slots: vec![
            StateSlot {
                slot_id: state_slot_id(41),
                target_field_id: field_id(33),
                source_type: fixture_type("bigint"),
                nullable: false,
                role: StateRole::AvgCount,
                encoding: StateEncoding::NativeColumnV1,
            },
            StateSlot {
                slot_id: state_slot_id(42),
                target_field_id: field_id(32),
                source_type: fixture_type("decimal(38,2)"),
                nullable: true,
                role: StateRole::AvgSum,
                encoding: StateEncoding::NativeColumnV1,
            },
        ],
        apply_key: Some(ApplyKey {
            kind: ApplyKeyKind::GroupRowId,
            components: vec![ApplyKeyComponent {
                logical_id: apply_key_id(44),
                target_field_id: field_id(34),
            }],
        }),
        aggregates: vec![AggregateInterpretation {
            aggregate_id: aggregate_id(51),
            function_identity: "avg".to_string(),
            semantic_contract: Some(aggregate_contract("avg")),
            binding_signature: Some(Bytes::from_static(b"fixture-avg")),
            source_fields: vec![SourceFieldReference {
                occurrence_id: 7,
                field_id: field_id(2),
            }],
            state_slot_ids: vec![state_slot_id(42), state_slot_id(41)],
            // The fixture declares branches, so its aggregate names the branch
            // it computes.
            branch_id: Some(branch_id(61)),
        }],
        branches: vec![
            BranchInterpretation {
                branch_id: branch_id(61),
                relation_occurrence_ids: vec![7],
                output_ids: vec![output_id()],
            },
            BranchInterpretation {
                branch_id: branch_id(62),
                relation_occurrence_ids: vec![8],
                output_ids: vec![output_id()],
            },
        ],
        target: TargetBinding {
            object_id: object_id(71),
            schema_version: schema_version(3),
            partition_spec_version: partition_spec_version(4),
            fields: vec![
                physical(
                    PhysicalFieldLogicalIdentity::Output(output_id()),
                    31,
                    "decimal(18,2)",
                    true,
                ),
                physical(
                    PhysicalFieldLogicalIdentity::State(state_slot_id(41)),
                    33,
                    "bigint",
                    false,
                ),
                physical(
                    PhysicalFieldLogicalIdentity::State(state_slot_id(42)),
                    32,
                    "decimal(38,2)",
                    true,
                ),
                physical(
                    PhysicalFieldLogicalIdentity::ApplyKey(apply_key_id(44)),
                    34,
                    "binary",
                    false,
                ),
                physical(
                    PhysicalFieldLogicalIdentity::Branch(branch_id(61)),
                    35,
                    "integer",
                    false,
                ),
                physical(
                    PhysicalFieldLogicalIdentity::Branch(branch_id(62)),
                    35,
                    "integer",
                    false,
                ),
            ],
            partition_fields: Vec::new(),
        },
    }
}

fn retraction_count_interpretation(definition: &DefinitionDocument) -> InterpretationDocument {
    let mut interpretation = sample_interpretation(definition);
    interpretation.state_slots = vec![
        StateSlot {
            slot_id: state_slot_id(41),
            target_field_id: field_id(32),
            source_type: fixture_type("binary"),
            nullable: false,
            role: StateRole::Single,
            encoding: StateEncoding::NativeColumnV1,
        },
        StateSlot {
            slot_id: state_slot_id(42),
            target_field_id: field_id(33),
            source_type: fixture_type("bigint"),
            nullable: false,
            role: StateRole::RetractionCount,
            encoding: StateEncoding::NativeColumnV1,
        },
    ];
    interpretation.aggregates = vec![
        AggregateInterpretation {
            aggregate_id: aggregate_id(51),
            function_identity: "sum".to_string(),
            semantic_contract: Some(aggregate_contract("sum")),
            binding_signature: Some(Bytes::from_static(b"fixture-sum")),
            source_fields: vec![SourceFieldReference {
                occurrence_id: 7,
                field_id: field_id(2),
            }],
            state_slot_ids: vec![state_slot_id(41)],
            // The fixture declares branches, so its aggregate names the one it
            // computes: two branches can share a state column, and the branch
            // is what tells the two stored states apart.
            branch_id: Some(branch_id(61)),
        },
        AggregateInterpretation {
            aggregate_id: internal_retraction_count_aggregate_identity(),
            function_identity: INTERNAL_RETRACTION_COUNT_FUNCTION_IDENTITY.to_string(),
            semantic_contract: None,
            binding_signature: None,
            source_fields: Vec::new(),
            state_slot_ids: vec![state_slot_id(42)],
            branch_id: None,
        },
    ];
    interpretation.target.fields = vec![
        physical(
            PhysicalFieldLogicalIdentity::Output(output_id()),
            31,
            "decimal(18,2)",
            true,
        ),
        physical(
            PhysicalFieldLogicalIdentity::State(state_slot_id(41)),
            32,
            "binary",
            false,
        ),
        physical(
            PhysicalFieldLogicalIdentity::State(state_slot_id(42)),
            33,
            "bigint",
            false,
        ),
        physical(
            PhysicalFieldLogicalIdentity::ApplyKey(apply_key_id(44)),
            34,
            "binary",
            false,
        ),
        physical(
            PhysicalFieldLogicalIdentity::Branch(branch_id(61)),
            35,
            "integer",
            false,
        ),
        physical(
            PhysicalFieldLogicalIdentity::Branch(branch_id(62)),
            35,
            "integer",
            false,
        ),
    ];
    interpretation
}

fn sample_configuration() -> ConfigurationDocument {
    ConfigurationDocument {
        refresh_policy: RefreshPolicy::AsyncInterval,
        paused: false,
        refresh_interval_ms: Some(60_000),
        max_staleness_ms: Some(120_000),
    }
}

/// Pure D/L/C shapes with exact provider object facts; no publication is made.
/// The target object is a provider identity, separate from D's source facts.
pub struct MvDocumentFixture {
    pub object_id: ConnectorTableObjectId,
    pub definition: DefinitionDocument,
    pub binding: BindingDocument,
    pub mapping: StorageMappingDocument,
    pub interpretation: InterpretationDocument,
    pub configuration: ConfigurationDocument,
}

impl MvDocumentFixture {
    pub fn new() -> Self {
        let definition = sample_definition();
        let interpretation = sample_interpretation(&definition);
        let binding = sample_binding();
        let mapping = fixture_mapping(&binding, &interpretation);
        Self {
            object_id: ConnectorTableObjectId::try_new(Bytes::from_static(&[71]))
                .expect("fixture target object"),
            definition,
            binding,
            mapping,
            interpretation,
            configuration: sample_configuration(),
        }
    }

    pub fn with_retraction_count(mut self) -> Self {
        self.interpretation = retraction_count_interpretation(&self.definition);
        self
    }
}

impl Default for MvDocumentFixture {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod pure_document_fixture_tests {
    use super::super::codec::keyed::DataVersionFormat;
    use super::super::keyed_documents::{build_keyed_versions, definition_revision};
    use super::super::validation::validate_binding_interpretation;
    use super::*;
    use novarocks_spi::connector::{ConnectorExactSemanticRevision, ConnectorProviderId};

    #[test]
    fn pure_shapes_keep_full_definition_identity_and_avg_runtime_layout() {
        let f = MvDocumentFixture::new();
        let revision = definition_revision(&f.definition).unwrap();
        validate_binding_interpretation(&f.binding, revision, &f.interpretation, &f.mapping)
            .unwrap();
        assert_ne!(
            revision,
            encode_definition(&f.definition).unwrap().revision()
        );
        let bytes =
            super::super::codec::keyed::encode_keyed_interpretation(&f.interpretation).unwrap();
        let decoded =
            super::super::codec::keyed::decode_keyed_interpretation(bytes.as_bytes()).unwrap();
        assert_eq!(decoded, f.interpretation);
        assert_eq!(
            decoded.aggregates[0].state_slot_ids,
            vec![state_slot_id(42), state_slot_id(41)]
        );
        assert_eq!(
            decoded
                .state_slots
                .iter()
                .find(|s| s.slot_id == state_slot_id(41))
                .unwrap()
                .role,
            StateRole::AvgCount
        );
        assert_eq!(
            decoded
                .state_slots
                .iter()
                .find(|s| s.slot_id == state_slot_id(42))
                .unwrap()
                .role,
            StateRole::AvgSum
        );
        assert_eq!(
            decoded
                .target
                .fields
                .iter()
                .filter(|f| f.target_field_id == field_id(35))
                .count(),
            2
        );
    }

    #[test]
    fn pure_shapes_retain_exact_repeated_source_facts_and_binding_bound() {
        let f = MvDocumentFixture::new();
        assert_eq!(
            f.binding
                .occurrences
                .iter()
                .map(|r| r.occurrence_id)
                .collect::<Vec<_>>(),
            vec![7, 8]
        );
        assert_eq!(
            f.binding.occurrences[0].object_id,
            f.binding.occurrences[1].object_id
        );
        f.binding.occurrences.iter().for_each(|occurrence| {
            let raw = super::super::exact_revision::restore_persisted_object(&occurrence.object_id)
                .unwrap();
            assert_eq!(raw.as_bytes().as_ref(), &[11]);
            let exact = ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
                ConnectorProviderId::parse("iceberg").unwrap(),
                &raw,
                Some(9),
            )
            .unwrap();
            let (persisted, _) =
                super::super::exact_revision::persist_exact_connector_revision(&exact).unwrap();
            assert_eq!(persisted, occurrence.object_id);
            let format = DataVersionFormat::from_issued_fact(exact.data_version()).unwrap();
            assert_eq!(format.max_value_bytes, 9);
            assert_eq!(format, occurrence.data_version);
        });
        let versions =
            build_keyed_versions(&f.definition, &f.interpretation, f.binding, f.mapping).unwrap();
        assert_eq!(
            versions.definition_revision(),
            f.interpretation.definition_revision
        );
        assert_eq!(
            versions.definition_revision(),
            definition_revision(&f.definition).unwrap()
        );
        assert_eq!(
            versions.interpretation.references().get("definition"),
            Some(&versions.definition.identity())
        );
    }

    #[test]
    fn pure_retraction_shape_keeps_internal_owner_and_nonnull_count() {
        let f = MvDocumentFixture::new().with_retraction_count();
        validate_binding_interpretation(
            &f.binding,
            definition_revision(&f.definition).unwrap(),
            &f.interpretation,
            &f.mapping,
        )
        .unwrap();
        let internal = f
            .interpretation
            .aggregates
            .iter()
            .find(|a| a.function_identity == INTERNAL_RETRACTION_COUNT_FUNCTION_IDENTITY)
            .unwrap();
        assert_eq!(
            internal.aggregate_id,
            internal_retraction_count_aggregate_identity()
        );
        assert_eq!(internal.state_slot_ids, vec![state_slot_id(42)]);
        assert!(internal.source_fields.is_empty());
        assert_eq!(internal.branch_id, None);
        let count = f
            .interpretation
            .state_slots
            .iter()
            .find(|s| s.slot_id == state_slot_id(42))
            .unwrap();
        assert_eq!(count.role, StateRole::RetractionCount);
        assert_eq!(count.source_type, fixture_type("bigint"));
        assert!(!count.nullable);
    }
}

#[path = "keyed_fixture.rs"]
mod keyed_fixture;
pub use keyed_fixture::{KeyedProjectionFixture, observed_current_keyed, sample_keyed_projection};

/// Coherent model fixture for a balanced UNION ALL over one source column.
///
/// Each branch has its own occurrence and shares the physical branch field.
/// The first branch's final output concatenates an owned literal to `k`.
/// These are real document/Current codec inputs, not a native CREATE execution.
pub fn union_all_keyed_projection_fixture(
    target: crate::product::MvTarget,
    snapshot: Option<i64>,
    branches: usize,
    outputs: usize,
    literal_bytes: usize,
) -> KeyedProjectionFixture {
    use novarocks_spi::connector::{ConnectorExactSemanticRevision, ConnectorProviderId};

    // These assertions describe this fixture's shape, not product capacities.
    assert!(branches >= 2);
    assert!(outputs >= 2);
    let source_object = ConnectorTableObjectId::try_new(Bytes::from_static(b"capacity-fact"))
        .expect("fixture source object");
    let source = ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
        ConnectorProviderId::parse("iceberg").unwrap(),
        &source_object,
        Some(17),
    )
    .expect("fixture source revision");
    let (source_object, source_version) =
        super::exact_revision::persist_exact_connector_revision(&source).unwrap();
    let source_field = FieldIdentity::try_new(vec![1]).unwrap();
    let names = (0..outputs)
        .map(|index| {
            if index == 0 {
                "k".into()
            } else if index + 1 == outputs {
                "wide_value".into()
            } else {
                format!("c{index:02}")
            }
        })
        .collect::<Vec<String>>();
    let literal = "x".repeat(literal_bytes);
    let selects = (0..branches)
        .map(|branch| {
            let projection = names
                .iter()
                .enumerate()
                .map(|(output, name)| {
                    if branch == 0 && output + 1 == outputs {
                        format!("CONCAT(k, '{literal}') AS {name}")
                    } else {
                        format!("k AS {name}")
                    }
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("SELECT {projection} FROM fact")
        })
        .collect::<Vec<_>>();
    fn balanced_union(selects: &[String]) -> String {
        match selects {
            [select] => select.clone(),
            _ => {
                let middle = selects.len() / 2;
                format!(
                    "({} UNION ALL {})",
                    balanced_union(&selects[..middle]),
                    balanced_union(&selects[middle..])
                )
            }
        }
    }
    let occurrences = (0..branches)
        .map(|index| {
            let mut occurrence = relation(u32::try_from(index + 1).unwrap(), 11, "fact");
            occurrence.relation_at_binding = "fact".into();
            occurrence.object_id = source_object.clone();
            occurrence.facts.object = Bytes::copy_from_slice(source_object.as_bytes());
            occurrence.paths = vec![field(1, "k", "varchar", true)];
            occurrence
        })
        .collect::<Vec<_>>();
    let definition = build_definition(
        1_700_000_000_000,
        QuerySource {
            effective_sql: balanced_union(&selects),
            dialect: QueryDialect::Trino,
            resolution: ResolutionContext {
                default_catalog: "ice".into(),
                default_namespace: "sales".into(),
            },
        },
    )
    .expect("coherent UNION definition");
    let binding = BindingDocument {
        occurrences: occurrences.clone(),
        outputs: names
            .iter()
            .enumerate()
            .map(|(index, name)| BoundOutput {
                ordinal: u32::try_from(index).unwrap(),
                name: name.clone(),
                logical_type_identity: Bytes::from_static(b"fixture-varchar"),
                source_fields: occurrences
                    .iter()
                    .map(|occurrence| SourceFieldReference {
                        occurrence_id: occurrence.occurrence_id,
                        field_id: source_field.clone(),
                    })
                    .collect(),
            })
            .collect(),
        dependencies: Vec::new(),
    };
    let output_ids = binding
        .outputs
        .iter()
        .map(BoundOutput::identity)
        .collect::<Vec<_>>();
    let apply_id = apply_key_id(1);
    let apply_field = FieldIdentity::try_new(vec![0xff, 1]).unwrap();
    let branch_field = FieldIdentity::try_new(vec![0xff, 2]).unwrap();
    let branch_ids = (0..branches)
        .map(|index| {
            BranchIdentity::try_new(u32::try_from(index + 1).unwrap().to_be_bytes().to_vec())
                .unwrap()
        })
        .collect::<Vec<_>>();
    let physical_outputs = output_ids
        .iter()
        .enumerate()
        .map(|(index, id)| PhysicalFieldBinding {
            logical_identity: PhysicalFieldLogicalIdentity::Output(id.clone()),
            target_field_id: FieldIdentity::try_new(
                u32::try_from(index + 1).unwrap().to_be_bytes().to_vec(),
            )
            .unwrap(),
            source_type: fixture_type("varchar"),
            nullable: true,
        })
        .collect::<Vec<_>>();
    let mut fixture = KeyedProjectionFixture::new(target, snapshot);
    let mut physical_fields = physical_outputs.clone();
    physical_fields.push(PhysicalFieldBinding {
        logical_identity: PhysicalFieldLogicalIdentity::ApplyKey(apply_id.clone()),
        target_field_id: apply_field.clone(),
        source_type: fixture_type("binary"),
        nullable: false,
    });
    physical_fields.extend(branch_ids.iter().map(|id| PhysicalFieldBinding {
        logical_identity: PhysicalFieldLogicalIdentity::Branch(id.clone()),
        target_field_id: branch_field.clone(),
        source_type: fixture_type("integer"),
        nullable: false,
    }));
    fixture.interpretation = InterpretationDocument {
        definition_revision: super::keyed_documents::definition_revision(&definition).unwrap(),
        outputs: output_ids
            .iter()
            .zip(&physical_outputs)
            .map(|(id, field)| OutputBinding {
                output_id: id.clone(),
                target_field_id: field.target_field_id.clone(),
                source_type: field.source_type.clone(),
                nullable: field.nullable,
            })
            .collect(),
        state_slots: Vec::new(),
        apply_key: Some(ApplyKey {
            kind: ApplyKeyKind::BaseRowId,
            components: vec![ApplyKeyComponent {
                logical_id: apply_id,
                target_field_id: apply_field,
            }],
        }),
        aggregates: Vec::new(),
        branches: branch_ids
            .into_iter()
            .zip(&occurrences)
            .map(|(branch_id, occurrence)| BranchInterpretation {
                branch_id,
                relation_occurrence_ids: vec![occurrence.occurrence_id],
                output_ids: output_ids.clone(),
            })
            .collect(),
        target: TargetBinding {
            object_id: ObjectIdentity::try_new(fixture.object_id.as_bytes().to_vec()).unwrap(),
            schema_version: schema_version(3),
            partition_spec_version: partition_spec_version(4),
            fields: physical_fields,
            partition_fields: Vec::new(),
        },
    };
    fixture.definition = definition;
    fixture.mapping = fixture_mapping(&binding, &fixture.interpretation);
    fixture.binding = binding;
    fixture.configuration = ConfigurationDocument {
        refresh_policy: RefreshPolicy::Manual,
        paused: false,
        refresh_interval_ms: None,
        max_staleness_ms: None,
    };
    if let Some(publication) = fixture.publication.as_mut() {
        publication.inputs = occurrences
            .into_iter()
            .map(|occurrence| PublicationInput {
                relation_occurrence_id: occurrence.occurrence_id,
                object_id: occurrence.object_id,
                native_data_version: source_version.clone(),
            })
            .collect();
        publication.logical_result_rows = Some(u64::try_from(branches).unwrap() * 2);
    }
    fixture
}
