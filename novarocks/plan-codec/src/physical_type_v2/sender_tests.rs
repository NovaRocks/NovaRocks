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
use arrow::datatypes::TimeUnit;
use novarocks_connector_contract as c;
use novarocks_physical_plan as p;
use novarocks_proto_models::plan;
use novarocks_type_contract::{
    ControlShape, EvaluationDomainId, ExpressionControlFlow, ExpressionEffectContext,
    ExpressionEvaluationDomain, ExpressionInvocation, ExpressionUseId,
};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Mutex,
};
use wire::carrier_type_definition::Kind;

// A conservative invoice for these bounded, freshly constructed fixture owners,
// not a measurement of HashMap tombstones or private allocator capacity.
const SOURCE: usize = 128 * 1024 * 1024;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    stop: Option<(usize, CompileControlError)>,
    events: Mutex<Vec<u32>>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Encode);
        assert!(units <= 256);
        let mut events = self.events.lock().unwrap();
        let at = events.len();
        if let Some((stop, _)) = self.stop {
            assert!(at <= stop, "callback after refusal");
        }
        events.push(units);
        match self.stop.clone() {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}
impl Control {
    fn trace(&self) -> Vec<u32> {
        self.events.lock().unwrap().clone()
    }
}
pub(super) struct Setup;
impl PureCompileControl for Setup {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
fn limits() -> PackageTypeProjectionLimits {
    PackageTypeProjectionLimits {
        max_definitions: 100_000,
        max_expanded_nodes: 1_000_000,
        max_string_bytes: 64 * 1024 * 1024,
        max_allocation_requests: 1_000_000,
        max_allocation_request_bytes: 512 * 1024 * 1024,
        max_coexisting_source_and_request_bytes: 1024 * 1024 * 1024,
        max_work: usize::MAX / 4,
    }
}
fn strict_limits() -> TypeProjectionLimits {
    TypeProjectionLimits {
        max_definitions: 100_000,
        max_expanded_nodes: 1_000_000,
        max_string_bytes: 64 * 1024 * 1024,
    }
}
pub(super) fn draft(input: c::ConnectorWriteInputShape) -> c::ConnectorWriteRecipeDraft {
    let instance = c::ConnectorInstanceId::try_from_canonical("lake").unwrap();
    let catalog = c::CatalogHandle::new(instance.clone(), c::CatalogVersion::from_bytes([7; 32]));
    let provider = c::ConnectorProviderId::parse("iceberg").unwrap();
    let binding = c::ConnectorWriteBinding::new(
        c::ConnectorInstanceDescriptor {
            provider_id: provider.clone(),
            instance_id: instance,
        },
        catalog.clone(),
    );
    let payload = c::ConnectorEncodedPayload::new(
        c::ConnectorEnvelopeHeader::new(
            provider,
            catalog,
            c::ConnectorCodecCategory::WriteHandle,
            c::ConnectorCodecRevision::try_new(1).unwrap(),
        ),
        vec![7u8].into(),
    );
    c::ConnectorWriteRecipeDraft::try_new(binding, payload, input).unwrap()
}
fn field(token: u8, field: Field) -> c::ConnectorWriteFieldBinding {
    c::ConnectorWriteFieldBinding::new(c::ConnectorWriteFieldToken::from_bytes([token; 32]), field)
}
fn data(field: Field) -> c::ConnectorWriteRecipeDraft {
    draft(c::ConnectorWriteInputShape::Data {
        fields: vec![self::field(1, field)],
    })
}
fn metadata() -> HashMap<String, String> {
    HashMap::from([
        ("z".into(), "last\0value".into()),
        ("a".into(), "雪".repeat(6826) + "ab"),
    ])
}
// Explicit source component projection. Real source/type emissions each use
// their sole codec body. Source-only numerical gates retain their original
// priority; this fixture adapter is not a full-package admission claim.
pub(super) fn projected_roots_in<'s>(
    values: ValueRootSources<'s>,
    source_fields: FieldRootSources<'s>,
    writers: &'s [WriterTypeSource<'s>],
    source: usize,
    limits: PackageTypeProjectionLimits,
    admit: &mut impl FnMut(&PackageTypeProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<EncodedTypeTable<'s>, TypeCodecError> {
    let mut completed = PackageTypeProjectionFacts::default();
    let has_sources = source_fields.len() != 0 || !writers.is_empty();
    let source_table = if has_sources {
        let borrowed_fields: Vec<_> = source_fields.iter().collect();
        Some(super::encode::encode_source_writer_sources(
            &borrowed_fields,
            writers,
            source,
            limits,
            &mut |facts| {
                validate_namespace_facts(facts, limits)?;
                admit(facts)?;
                completed = *facts;
                Ok(())
            },
            work,
        )?)
    } else {
        None
    };
    let table = if values.len() != 0 || !has_sources {
        super::encode::encode_writer_sources_with_host(
            values,
            FieldRootSources::Owned(&[]),
            &[],
            source,
            limits,
            &mut |facts| {
                let combined = if has_sources {
                    combine_namespace_facts(completed, *facts, source)
                        .map_err(crate::host_projection_v2::AdmissionRefusal::Host)?
                } else {
                    *facts
                };
                validate_namespace_facts(&combined, limits)
                    .map_err(crate::host_projection_v2::AdmissionRefusal::Control)?;
                admit(&combined).map_err(crate::host_projection_v2::AdmissionRefusal::Control)
            },
            work,
        )
        .map_err(|failure| match failure {
            crate::host_projection_v2::ProjectionFailure::Codec(error)
            | crate::host_projection_v2::ProjectionFailure::Host(error) => error,
        })?
    } else {
        wire::TypeTable::default()
    };
    Ok(EncodedTypeTable {
        table,
        source_table,
        values,
        fields: FieldRootSources::Owned(&[]),
        source_fields,
        writers,
    })
}
pub(super) fn projected_in<'s>(
    values: &'s [(u32, FunctionValueType)],
    fields: &'s [(u32, Arc<Field>)],
    writers: &'s [WriterTypeSource<'s>],
    source: usize,
    limits: PackageTypeProjectionLimits,
    admit: &mut impl FnMut(&PackageTypeProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<EncodedTypeTable<'s>, TypeCodecError> {
    projected_roots_in(
        ValueRootSources::Owned(values),
        FieldRootSources::Owned(fields),
        writers,
        source,
        limits,
        admit,
        work,
    )
}
fn projected<'s>(
    values: &'s [(u32, FunctionValueType)],
    fields: &'s [(u32, Arc<Field>)],
    writers: &'s [WriterTypeSource<'s>],
    control: &Control,
) -> Result<EncodedTypeTable<'s>, TypeCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = projected_in(
        values,
        fields,
        writers,
        SOURCE,
        limits(),
        &mut |_| Ok(()),
        &mut work,
    );
    if matches!(result, Err(TypeCodecError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn source_shape<'a>(encoded: &'a EncodedTypeTable<'_>) -> &'a wire::TypeTable {
    encoded.as_source_wire().unwrap().shape.as_ref().unwrap()
}
fn wire_field(id: u32, carrier: u32, name: &str, nullable: bool) -> wire::FieldDefinition {
    wire::FieldDefinition {
        id,
        name: name.into(),
        nullable,
        carrier_type_id: Some(carrier),
        logical_type_id: None,

        value_binding_id: None,
    }
}
fn carrier(id: u32, kind: Kind) -> wire::CarrierTypeDefinition {
    wire::CarrierTypeDefinition {
        id,
        kind: Some(kind),
    }
}
fn prefixes(run: impl Fn(&Control) -> Result<(), TypeCodecError>) {
    let success = Control::default();
    let baseline = run(&success);
    let trace = success.trace();
    assert!(!trace.is_empty());
    for stop in 0..trace.len() {
        for cause in CAUSES {
            let control = Control {
                stop: Some((stop, cause.clone())),
                ..Default::default()
            };
            assert!(
                matches!(run(&control), Err(TypeCodecError::Control(actual)) if actual == cause)
            );
            assert_eq!(control.trace(), trace[..=stop]);
        }
    }
    assert!(!matches!(baseline, Err(TypeCodecError::Control(_))));
}

// This fixture observes source laws through the actual recipe's role and
// ordered IDs. It does not publish a compiled writer or author execution types.
pub(super) fn original_source_package(
    recipe: &c::ConnectorWriteRecipeDraft,
    ids: &[u32],
    source: wire::SourceFieldTable,
) -> novarocks_proto_models::physical_package_v2::FragmentPackage {
    use novarocks_proto_models::physical_package_v2 as raw;
    use raw::connector_write_input_shape::Kind;
    let mut next = ids.iter().copied();
    let mut group = |fields: &[c::ConnectorWriteFieldBinding]| {
        fields
            .iter()
            .map(|field| raw::ConnectorWriteFieldBinding {
                field_token: field.token().to_bytes().to_vec(),
                source_field_id: Some(next.next().expect("original writer source ordinal")),
                value_type_id: Some(0),
            })
            .collect::<Vec<_>>()
    };
    let kind = match recipe.input() {
        c::ConnectorWriteInputShape::Data { fields } => Kind::Data(raw::ConnectorWriteDataInput {
            fields: group(fields),
        }),
        c::ConnectorWriteInputShape::RowLineage {
            data_fields,
            row_identity_fields,
        } => Kind::RowLineage(raw::ConnectorWriteRowLineageInput {
            data_fields: group(data_fields),
            row_identity_fields: group(row_identity_fields),
        }),
        c::ConnectorWriteInputShape::PositionDelete {
            identity_fields,
            partition_source_fields,
        } => Kind::PositionDelete(raw::ConnectorWritePositionDeleteInput {
            identity_fields: group(identity_fields),
            partition_source_fields: group(partition_source_fields),
        }),
        c::ConnectorWriteInputShape::DeletionVector {
            identity_fields,
            partition_source_fields,
        } => Kind::DeletionVector(raw::ConnectorWriteDeletionVectorInput {
            identity_fields: group(identity_fields),
            partition_source_fields: group(partition_source_fields),
        }),
        c::ConnectorWriteInputShape::EqualityDelete { equality_fields } => {
            Kind::EqualityDelete(raw::ConnectorWriteEqualityDeleteInput {
                equality_fields: group(equality_fields),
            })
        }
    };
    assert!(next.next().is_none());
    raw::FragmentPackage {
        types: Some(
            encode_type_table(
                &[(0, FunctionValueType::new(DataType::Int64, false))],
                strict_limits(),
                &Setup,
            )
            .expect("independent source-role fixture execution channel"),
        ),
        source_fields: Some(source),
        writes: vec![raw::FrozenWriterRecipe {
            input: Some(raw::ConnectorWriteInputShape { kind: Some(kind) }),
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn original_source_wire(recipe: &c::ConnectorWriteRecipeDraft, id: u32) -> wire::SourceFieldTable {
    // Borrow the actual checked Writer recipe: its permissive source grammar
    // is never replaced by a copied strict Field or execution declaration.
    let ids: Vec<_> = recipe
        .input()
        .fields_iter()
        .enumerate()
        .map(|(ordinal, _)| {
            if ordinal == 0 {
                id
            } else if id == u32::MAX {
                u32::try_from(ordinal - 1).unwrap()
            } else {
                u32::try_from(ordinal).unwrap()
            }
        })
        .collect();
    let writers = [WriterTypeSource::new(recipe, &ids)];
    let control = Control::default();
    let encoded = projected(&[], &[], &writers, &control).unwrap();
    let wire = encoded.as_source_wire().unwrap().clone();
    let mut request = |_: std::alloc::Layout| Ok::<(), CompileControlError>(());
    let mut opaque = |_: usize| Ok::<(), CompileControlError>(());
    let mut creation = novarocks_type_contract::ValueSchemaCreation::new(
        novarocks_type_contract::ValueTypeStorageLifetime::pure(),
        &mut request,
        &mut opaque,
    );
    // Explicit pure fixture creator, not a BE account. The same receiving
    // parser must retain every original attribute/descriptor and source row.
    let mut receiving =
        CompileCheckpoints::try_new(&super::sender_tests::Setup, CompilePhase::Decode).unwrap();
    let package = super::sender_tests::original_source_package(recipe, &ids, wire.clone());
    let decoded = decode_package_type_table_with_creator_in(
        &package,
        SOURCE,
        limits(),
        &mut creation,
        &mut receiving,
    )
    .unwrap();
    for (field_id, binding) in ids.iter().zip(recipe.input().fields_iter()) {
        let actual = decoded
            .source_field_observed(*field_id, &mut receiving)
            .unwrap()
            .unwrap();
        assert!(novarocks_type_contract::arrow_fields_exact(
            actual,
            binding.field()
        ));
    }
    receiving.finish().unwrap();
    wire
}

#[test]
fn sender_writer_metadata_uses_original_source_field_and_exact_canonical_wire() {
    let recipe = data(Field::new("根\0writer", DataType::Int64, false).with_metadata(metadata()));
    assert_eq!(
        recipe
            .input()
            .fields_iter()
            .next()
            .unwrap()
            .field()
            .metadata()["a"]
            .len(),
        20 * 1024
    );
    let ids = [u32::MAX];
    let writers = [WriterTypeSource::new(&recipe, &ids)];
    let control = Control::default();
    let encoded = projected(&[], &[], &writers, &control).unwrap();
    let expected_field = wire_field(u32::MAX, 0, "根\0writer", false);
    let source_wire = original_source_wire(&recipe, u32::MAX);
    assert_eq!(
        source_wire.field_facts[0].metadata,
        vec![
            plan::ArrowFieldMetadataEntry {
                key: "a".into(),
                value: "雪".repeat(6826) + "ab",
            },
            plan::ArrowFieldMetadataEntry {
                key: "z".into(),
                value: "last\0value".into(),
            },
        ]
    );
    assert_eq!(
        source_shape(&encoded),
        &wire::TypeTable {
            logical_types: Vec::new(),
            carriers: vec![carrier(
                0,
                Kind::Primitive(plan::ArrowPrimitiveType::Int64 as i32)
            )],
            fields: vec![expected_field],
            value_types: vec![]
        }
    );
    let original = recipe.input().fields_iter().next().unwrap().field();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    assert!(std::ptr::eq(
        encoded
            .source_field_source_observed(u32::MAX, &mut work)
            .unwrap()
            .unwrap(),
        original
    ));
    assert!(
        encoded
            .field_observed(u32::MAX, &mut work)
            .unwrap()
            .is_none()
    );
    assert!(
        encoded
            .source_field_source_observed(0, &mut work)
            .unwrap()
            .is_none()
    );
    work.finish().unwrap();
    let strict_fields = [(u32::MAX, Arc::new(original.clone()))];
    assert!(matches!(
        encode_type_table_sources(&[], &strict_fields, strict_limits(), &Control::default()),
        Err(TypeCodecError::InvalidShape(_))
    ));
    prefixes(|control| projected(&[], &[], &writers, control).map(|_| ()));
}

#[test]
fn sender_writer_struct_5000_and_timezone_2000_preserve_complete_original_occurrences() {
    let wide = data(Field::new(
        "root",
        DataType::Struct(
            (0..5000)
                .map(|n| Field::new(format!("c{n}"), DataType::Int64, false))
                .collect::<Vec<_>>()
                .into(),
        ),
        false,
    ));
    let ids = [u32::MAX];
    let writers = [WriterTypeSource::new(&wide, &ids)];
    let encoded = projected(&[], &[], &writers, &Control::default()).unwrap();
    let mut expected_carriers = vec![carrier(
        0,
        Kind::StructType(wire::StructFields {
            field_ids: (0..5000).collect(),
        }),
    )];
    expected_carriers.extend(
        (1..=5000).map(|id| carrier(id, Kind::Primitive(plan::ArrowPrimitiveType::Int64 as i32))),
    );
    let mut expected_fields = (0..5000)
        .map(|n| wire_field(n, n + 1, &format!("c{n}"), false))
        .collect::<Vec<_>>();
    expected_fields.push(wire_field(u32::MAX, 0, "root", false));
    assert_eq!(
        source_shape(&encoded),
        &wire::TypeTable {
            logical_types: Vec::new(),
            carriers: expected_carriers,
            fields: expected_fields,
            value_types: vec![]
        }
    );
    let original = wide.input().fields_iter().next().unwrap().field();
    assert!(
        FunctionValueType::try_with_logical_type(
            original.data_type().clone(),
            false,
            ValueLogicalType::Physical
        )
        .is_err()
    );
    let strict_fields = [(0, Arc::new(original.clone()))];
    assert!(matches!(
        encode_type_table_sources(&[], &strict_fields, strict_limits(), &Control::default()),
        Err(TypeCodecError::ValueType(_))
    ));

    let zone = "UTC".repeat(666) + "ab";
    let timed = data(Field::new(
        "time",
        DataType::Timestamp(TimeUnit::Nanosecond, Some(zone.clone().into())),
        true,
    ));
    let timed_writers = [WriterTypeSource::new(&timed, &ids)];
    let encoded = projected(&[], &[], &timed_writers, &Control::default()).unwrap();
    assert_eq!(
        source_shape(&encoded),
        &wire::TypeTable {
            logical_types: Vec::new(),
            carriers: vec![carrier(
                0,
                Kind::Timestamp(plan::ArrowTimestampType {
                    unit: plan::ArrowTimeUnit::Nanosecond as i32,
                    timezone: Some(zone)
                })
            )],
            fields: vec![wire_field(u32::MAX, 0, "time", true)],
            value_types: vec![]
        }
    );
    let original = timed.input().fields_iter().next().unwrap().field();
    assert!(
        FunctionValueType::try_with_logical_type(
            original.data_type().clone(),
            true,
            ValueLogicalType::Physical
        )
        .is_err()
    );
    let strict_fields = [(0, Arc::new(original.clone()))];
    assert!(matches!(
        encode_type_table_sources(&[], &strict_fields, strict_limits(), &Control::default()),
        Err(TypeCodecError::InvalidShape(
            "Arrow timestamp zone exceeds its owner bound"
        ))
    ));
    // Sample actual callbacks only; wide field count is not a promise that an
    // opaque operation contains a cooperative 256-unit quantum.
    let control = Control::default();
    projected(&[], &[], &writers, &control).unwrap();
    let trace = control.trace();
    let mut samples = vec![0, trace.len() / 2, trace.len() - 1];
    if let Some(at) = trace.iter().position(|units| *units == 256) {
        samples.push(at);
    }
    samples.sort_unstable();
    samples.dedup();
    for stop in samples {
        for cause in CAUSES {
            let control = Control {
                stop: Some((stop, cause.clone())),
                ..Default::default()
            };
            assert!(
                matches!(projected(&[], &[], &writers, &control), Err(TypeCodecError::Control(actual)) if actual == cause)
            );
            assert_eq!(control.trace(), trace[..=stop]);
        }
    }
}

#[test]
fn sender_sparse_ids_dictionary_attributes_and_role_order_are_original_source_facts() {
    #[allow(deprecated)]
    let dictionary = Field::new_dict(
        "dictionary",
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
        true,
        17,
        false,
    )
    .with_metadata(HashMap::from([("note".into(), "原\0值".into())]));
    let recipe = draft(c::ConnectorWriteInputShape::RowLineage {
        data_fields: vec![field(1, dictionary)],
        row_identity_fields: vec![field(2, Field::new("identity", DataType::Int64, false))],
    });
    let ids = [u32::MAX, 0];
    let writers = [WriterTypeSource::new(&recipe, &ids)];
    let strict = [(91, Arc::new(Field::new("strict", DataType::Boolean, false)))];
    let values = [(u32::MAX, FunctionValueType::new(DataType::Int32, true))];
    let control = Control::default();
    let encoded = projected(&values, &strict, &writers, &control).unwrap();
    let source_wire = original_source_wire(&recipe, u32::MAX);
    assert_eq!(source_wire.field_facts[0].dictionary_id, Some(17));
    assert_eq!(
        source_wire.field_facts[0].dictionary_is_ordered,
        Some(false)
    );
    assert_eq!(
        source_wire.field_facts[0].metadata,
        vec![plan::ArrowFieldMetadataEntry {
            key: "note".into(),
            value: "原\0值".into(),
        }]
    );
    assert_eq!(encoded.as_wire().value_types.len(), 1);
    assert_eq!(encoded.as_wire().value_types[0].id, u32::MAX);
    assert_eq!(
        encoded.as_wire().carriers,
        vec![carrier(
            0,
            Kind::Primitive(plan::ArrowPrimitiveType::Int32 as i32)
        )]
    );
    assert!(encoded.as_wire().fields.is_empty());
    assert_eq!(
        source_shape(&encoded),
        &wire::TypeTable {
            logical_types: vec![],
            value_types: vec![],
            carriers: vec![
                carrier(0, Kind::Primitive(plan::ArrowPrimitiveType::Boolean as i32)),
                carrier(
                    1,
                    Kind::Dictionary(wire::DictionaryTypes {
                        key_type_id: Some(2),
                        value_type_id: Some(3)
                    })
                ),
                carrier(2, Kind::Primitive(plan::ArrowPrimitiveType::Int8 as i32)),
                carrier(3, Kind::Primitive(plan::ArrowPrimitiveType::Utf8 as i32)),
                carrier(4, Kind::Primitive(plan::ArrowPrimitiveType::Int64 as i32)),
            ],
            fields: vec![
                wire_field(91, 0, "strict", false),
                wire_field(u32::MAX, 1, "dictionary", true),
                wire_field(0, 4, "identity", false)
            ],
        }
    );
    let originals = recipe.input().fields_iter().collect::<Vec<_>>();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    for (id, original) in ids.iter().zip(originals) {
        assert!(std::ptr::eq(
            encoded
                .source_field_source_observed(*id, &mut work)
                .unwrap()
                .unwrap(),
            original.field()
        ));
    }
    assert!(Arc::ptr_eq(
        encoded
            .source_field_observed(91, &mut work)
            .unwrap()
            .unwrap(),
        &strict[0].1
    ));
    work.finish().unwrap();
}

#[test]
fn sender_id_count_and_duplicate_root_sources_are_refused_with_ordinary_control_tails() {
    let recipe = data(Field::new("v", DataType::Int64, false));
    let short = [];
    let valid = [7];
    let long = [7, 8];
    let strict = [(7, Arc::new(Field::new("strict", DataType::Boolean, true)))];
    let repeated = draft(c::ConnectorWriteInputShape::Data {
        fields: vec![
            field(1, Field::new("a", DataType::Int64, false)),
            field(2, Field::new("b", DataType::Int64, false)),
        ],
    });
    let duplicate = [7, 7];
    for case in 0..5 {
        prefixes(|control| {
            let writers = match case {
                0 => vec![WriterTypeSource::new(&recipe, &short)],
                1 => vec![WriterTypeSource::new(&recipe, &valid)],
                2 => vec![
                    WriterTypeSource::new(&recipe, &valid),
                    WriterTypeSource::new(&recipe, &valid),
                ],
                3 => vec![WriterTypeSource::new(&repeated, &duplicate)],
                _ => vec![WriterTypeSource::new(&recipe, &long)],
            };
            let fields = if case == 1 { strict.as_slice() } else { &[] };
            assert!(matches!(
                projected(&[], fields, &writers, &Control::default()),
                Err(TypeCodecError::InvalidShape(_))
            ));
            projected(&[], fields, &writers, control).map(|_| ())
        });
    }
}

fn property(distribution: p::Distribution) -> p::PhysicalProperties {
    p::PhysicalProperties {
        distribution,
        row_multiplicity: p::RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    }
}
fn dop() -> p::PipelineDopDomain {
    p::PipelineDopDomain {
        min: 1,
        max: 8,
        requires_power_of_two: false,
    }
}
fn relation_fields(
    builder: &mut p::FragmentBuilder,
    owner: p::NodeId,
    root: bool,
) -> Box<[p::WriterRelationField]> {
    let specs = if root {
        vec![
            (
                "kind",
                DataType::Int8,
                false,
                p::WriterRelationFieldRole::Kind,
                p::WriterDerivedKind::RelationKind,
            ),
            (
                "write_target_ordinal",
                DataType::Int32,
                true,
                p::WriterRelationFieldRole::TargetOrdinal,
                p::WriterDerivedKind::WriteTargetOrdinal,
            ),
            (
                "row_count",
                DataType::Int64,
                true,
                p::WriterRelationFieldRole::RowCount,
                p::WriterDerivedKind::AffectedRows,
            ),
            (
                "commit_fragment",
                DataType::Binary,
                true,
                p::WriterRelationFieldRole::CommitFragment,
                p::WriterDerivedKind::CommitFragment,
            ),
            (
                "input_fields",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, false))),
                true,
                p::WriterRelationFieldRole::Auxiliary,
                p::WriterDerivedKind::RelationAuxiliary,
            ),
            (
                "blob_type",
                DataType::Utf8,
                true,
                p::WriterRelationFieldRole::Auxiliary,
                p::WriterDerivedKind::RelationAuxiliary,
            ),
            (
                "body",
                DataType::Binary,
                true,
                p::WriterRelationFieldRole::Auxiliary,
                p::WriterDerivedKind::RelationAuxiliary,
            ),
            (
                "properties",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(
                            vec![
                                Field::new("key", DataType::Utf8, false),
                                Field::new("value", DataType::Utf8, false),
                            ]
                            .into(),
                        ),
                        false,
                    )),
                    false,
                ),
                true,
                p::WriterRelationFieldRole::Auxiliary,
                p::WriterDerivedKind::RelationAuxiliary,
            ),
            (
                "requested",
                DataType::Int64,
                true,
                p::WriterRelationFieldRole::Auxiliary,
                p::WriterDerivedKind::RelationAuxiliary,
            ),
            (
                "matched",
                DataType::Int64,
                true,
                p::WriterRelationFieldRole::Auxiliary,
                p::WriterDerivedKind::RelationAuxiliary,
            ),
            (
                "insufficient_keys",
                DataType::Int64,
                true,
                p::WriterRelationFieldRole::Auxiliary,
                p::WriterDerivedKind::RelationAuxiliary,
            ),
            (
                "insufficient_fingerprint",
                DataType::FixedSizeBinary(16),
                true,
                p::WriterRelationFieldRole::Auxiliary,
                p::WriterDerivedKind::RelationAuxiliary,
            ),
        ]
    } else {
        vec![
            (
                "kind",
                DataType::Int8,
                false,
                p::WriterRelationFieldRole::Kind,
                p::WriterDerivedKind::RelationKind,
            ),
            (
                "write_target_ordinal",
                DataType::Int32,
                false,
                p::WriterRelationFieldRole::TargetOrdinal,
                p::WriterDerivedKind::WriteTargetOrdinal,
            ),
            (
                "row_count",
                DataType::Int64,
                true,
                p::WriterRelationFieldRole::RowCount,
                p::WriterDerivedKind::AffectedRows,
            ),
            (
                "commit_fragment",
                DataType::Binary,
                true,
                p::WriterRelationFieldRole::CommitFragment,
                p::WriterDerivedKind::CommitFragment,
            ),
        ]
    };
    specs
        .into_iter()
        .map(|(name, carrier, nullable, role, kind)| {
            let ty = FunctionValueType::new(carrier, nullable);
            let value = builder
                .add_value(
                    ty.clone(),
                    p::ValueOrigin::WriterDerived {
                        writer_node: owner,
                        kind,
                    },
                )
                .unwrap();
            p::WriterRelationField {
                value,
                name: name.into(),
                ty,
                role,
            }
        })
        .collect()
}
pub(crate) fn checked_writer_package(recipe: c::ConnectorWriteRecipeDraft) -> p::FragmentPackage {
    checked_writer_package_with(recipe, false)
}

/// The same producer, with its Values row authored either as a legacy literal
/// or as an actual constant pool reference (the publishable v2 form).
pub(crate) fn checked_writer_package_with(
    recipe: c::ConnectorWriteRecipeDraft,
    constant_row: bool,
) -> p::FragmentPackage {
    checked_writer_plan_packages(recipe, constant_row)
        .remove(&p::FragmentId::new(1))
        .unwrap()
}

/// The finisher of the same checked plan: an ExchangeSource importing the
/// writer's multiplexed result relation into a TableFinish.
pub(crate) fn checked_finish_package_with(
    recipe: c::ConnectorWriteRecipeDraft,
    constant_row: bool,
) -> p::FragmentPackage {
    checked_writer_plan_packages(recipe, constant_row)
        .remove(&p::FragmentId::new(2))
        .unwrap()
}

fn change_stream_source(
    recipe: &c::ConnectorWriteRecipeDraft,
    pool: p::ConstantPoolId,
    route_edge: p::EdgeId,
    summary_edge: p::EdgeId,
) -> (p::Fragment, p::ValueId, p::SummaryOutput) {
    let mut builder = p::FragmentBuilder::new(p::FragmentId::new(3));
    let input = builder.reserve_node_id().unwrap();
    let expression = builder
        .add_expression(
            input,
            FunctionValueType::new(DataType::Int64, false),
            p::ExprKind::Constant(p::ConstantReference { pool, ordinal: 0 }),
        )
        .unwrap();
    let value = builder
        .add_value(
            FunctionValueType::new(DataType::Int64, false),
            p::ValueOrigin::NodeOutput {
                node: input,
                output_ordinal: 0,
            },
        )
        .unwrap();
    builder
        .add_values(
            input,
            Box::from([Box::from([expression])]),
            Box::from([value]),
        )
        .unwrap();
    let expand = builder.reserve_node_id().unwrap();
    let assignment = builder
        .add_expression(
            expand,
            FunctionValueType::new(DataType::Int64, false),
            p::ExprKind::Value(value),
        )
        .unwrap();
    let effect = builder
        .add_value(
            FunctionValueType::new(DataType::Int8, false),
            p::ValueOrigin::NodeOutput {
                node: expand,
                output_ordinal: 0,
            },
        )
        .unwrap();
    let routed = builder
        .add_value(
            FunctionValueType::new(DataType::Int64, false),
            p::ValueOrigin::NodeOutput {
                node: expand,
                output_ordinal: 1,
            },
        )
        .unwrap();
    builder
        .insert_node_unchecked(p::PhysicalNode {
            id: expand,
            inputs: Box::from([input]),
            required_inputs: Box::from([property(p::Distribution::Unconstrained)]),
            output_properties: property(p::Distribution::Unconstrained),
            output: p::OutputPort {
                node: expand,
                columns: Box::from([effect, routed]),
            },
            kind: p::NodeKind::ChangeEventExpand {
                events: Box::from([p::ChangeEventSpec {
                    predicate: None,
                    effect: c::ConnectorRowMutationEffect::Insert,
                    assignments: Box::from([(routed, Some(assignment))]),
                }]),
                effect_output: effect,
            },
        })
        .unwrap();
    let mut add_count = |field| {
        builder
            .add_value(
                FunctionValueType::new(DataType::Int64, false),
                p::ValueOrigin::Summary { field },
            )
            .unwrap()
    };
    let summary = p::SummaryOutput {
        edge: summary_edge,
        columns: p::SummaryColumns::write_counts(
            add_count(p::SummaryField::Inserted),
            add_count(p::SummaryField::Replaced),
            add_count(p::SummaryField::Deleted),
        ),
        author: p::SummaryAuthor::RouterSink,
    };
    builder.set_summaries(Box::new([summary]));
    let fragment = builder
        .finish_definition(
            expand,
            p::FragmentSink::Router {
                effect,
                routes: Box::from([p::ChangeStreamRoute {
                    route_id: c::ConnectorWriteRouteId::from_bytes([9; 32]),
                    write_target_ordinal: c::WriteTargetOrdinal::try_new(0).unwrap(),
                    accepted_effects: Box::from([c::ConnectorRowMutationEffect::Insert]),
                    input_mapping: recipe
                        .input()
                        .fields_iter()
                        .map(|field| (field.token(), routed))
                        .collect(),
                    requirements: p::WriteRouteRequirements {
                        colocation: p::WriteColocation::Singleton,
                        layout: p::WriteLayoutPreference::ClusterBy(Box::from([routed])),
                    },
                    edge: route_edge,
                }]),
            },
            dop(),
            p::StagePlacementPolicy::Spread {
                max_instances: std::num::NonZeroU32::new(3),
            },
        )
        .unwrap();
    (fragment, routed, summary)
}

/// All packages of one independently authored Router/writer/summary/finish graph.
pub(crate) fn checked_change_stream_packages(
    recipe: c::ConnectorWriteRecipeDraft,
) -> BTreeMap<p::FragmentId, p::FragmentPackage> {
    checked_writer_plan_packages_inner(recipe, true, true)
}

/// Both checked packages of one producer/stream/finisher plan.
fn checked_writer_plan_packages(
    recipe: c::ConnectorWriteRecipeDraft,
    constant_row: bool,
) -> BTreeMap<p::FragmentId, p::FragmentPackage> {
    checked_writer_plan_packages_inner(recipe, constant_row, false)
}

fn checked_writer_plan_packages_inner(
    recipe: c::ConnectorWriteRecipeDraft,
    constant_row: bool,
    change_stream: bool,
) -> BTreeMap<p::FragmentId, p::FragmentPackage> {
    // A real producer/stream/finisher plan, with original cut derivation and
    // full Package publication. There are no aggregate or function calls.
    let pool = p::ConstantPoolId::new(5);
    let ordinal = c::WriteTargetOrdinal::try_new(0).unwrap();
    let edge = p::EdgeId::new(7);
    let route_edge = p::EdgeId::new(8);
    let summary_edge = p::EdgeId::new(9);
    let router =
        change_stream.then(|| change_stream_source(&recipe, pool, route_edge, summary_edge));
    let mut builder = p::FragmentBuilder::new(p::FragmentId::new(1));
    let (source, values) = if let Some((_, route_value, _)) = &router {
        let source = builder.reserve_node_id().unwrap();
        // Each projected occurrence has its own receiver declaration, even
        // when the Router reads the same original value twice.
        let values: Vec<_> = recipe
            .input()
            .fields_iter()
            .map(|_| {
                builder
                    .add_value(
                        FunctionValueType::new(DataType::Int64, false),
                        p::ValueOrigin::ExchangeImport {
                            edge: route_edge,
                            source_value: *route_value,
                        },
                    )
                    .unwrap()
            })
            .collect();
        builder
            .add_exchange_source(
                source,
                route_edge,
                values.iter().map(|value| (*route_value, *value)).collect(),
                values.clone().into_boxed_slice(),
                p::Distribution::Singleton,
                p::RowMultiplicity::SingleCopy,
            )
            .unwrap();
        (source, values)
    } else {
        let source = builder.reserve_node_id().unwrap();
        let expression = builder
            .add_expression(
                source,
                FunctionValueType::new(DataType::Int64, false),
                if constant_row {
                    p::ExprKind::Constant(p::ConstantReference { pool, ordinal: 0 })
                } else {
                    p::ExprKind::Literal(p::LiteralValue::Int64(42))
                },
            )
            .unwrap();
        let value = builder
            .add_value(
                FunctionValueType::new(DataType::Int64, false),
                p::ValueOrigin::NodeOutput {
                    node: source,
                    output_ordinal: 0,
                },
            )
            .unwrap();
        builder
            .insert_node_unchecked(p::PhysicalNode {
                id: source,
                inputs: Box::default(),
                required_inputs: Box::default(),
                output_properties: property(p::Distribution::Singleton),
                output: p::OutputPort {
                    node: source,
                    columns: Box::from([value]),
                },
                kind: p::NodeKind::Values {
                    rows: Box::from([Box::from([expression])]),
                },
            })
            .unwrap();
        (source, vec![value])
    };
    let writer = builder.reserve_node_id().unwrap();
    let fields = relation_fields(&mut builder, writer, false);
    builder
        .insert_node_unchecked(p::PhysicalNode {
            id: writer,
            inputs: Box::from([source]),
            required_inputs: Box::from([property(p::Distribution::Singleton)]),
            output_properties: property(p::Distribution::Unconstrained),
            output: p::OutputPort {
                node: writer,
                columns: fields.iter().map(|field| field.value).collect(),
            },
            kind: p::NodeKind::TableWriter {
                target: p::WriterTarget {
                    handle: recipe.payload().clone(),
                    write_target_ordinal: ordinal,
                    input: values.clone().into_boxed_slice(),
                    required_distribution: p::Distribution::Singleton,
                    target_fields: recipe
                        .input()
                        .fields_iter()
                        .zip(&values)
                        .map(|(field, value)| p::WriterTargetField {
                            provider_name: field.field().name().clone().into(),
                            token: field.token(),
                            input: *value,
                            ty: FunctionValueType::new(DataType::Int64, false),
                            hidden: false,
                        })
                        .collect(),
                    output_schema: p::WriterRelationSchema {
                        revision: p::WRITER_MULTIPLEX_SCHEMA_REVISION,
                        fields: fields.clone(),
                    },
                    partial_aggregates: Box::default(),
                },
            },
        })
        .unwrap();
    let producer = builder
        .finish_definition(
            writer,
            p::FragmentSink::Stream { edge },
            dop(),
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let mut builder = p::FragmentBuilder::new(p::FragmentId::new(2));
    let exchange = builder.reserve_node_id().unwrap();
    let imported = fields
        .iter()
        .map(|field| {
            let value = builder
                .add_value(
                    field.ty.clone(),
                    p::ValueOrigin::ExchangeImport {
                        edge,
                        source_value: field.value,
                    },
                )
                .unwrap();
            p::WriterRelationField {
                value,
                name: field.name.clone(),
                ty: field.ty.clone(),
                role: field.role,
            }
        })
        .collect::<Box<[_]>>();
    builder
        .insert_node_unchecked(p::PhysicalNode {
            id: exchange,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: property(p::Distribution::Singleton),
            output: p::OutputPort {
                node: exchange,
                columns: imported.iter().map(|field| field.value).collect(),
            },
            kind: p::NodeKind::ExchangeSource {
                edge,
                imports: fields
                    .iter()
                    .zip(&imported)
                    .map(|(a, b)| (a.value, b.value))
                    .collect(),
            },
        })
        .unwrap();
    let summary_input = router.as_ref().map(|(_, _, summary)| {
        let node = builder.reserve_node_id().unwrap();
        let original = [
            summary.columns.values()[0],
            summary.columns.values()[1],
            summary.columns.values()[2],
        ];
        let mut imports = Vec::new();
        for source_value in original {
            let value = builder
                .add_value(
                    FunctionValueType::new(DataType::Int64, false),
                    p::ValueOrigin::ExchangeImport {
                        edge: summary_edge,
                        source_value,
                    },
                )
                .unwrap();
            imports.push((source_value, value));
        }
        builder
            .add_exchange_source(
                node,
                summary_edge,
                imports.clone().into_boxed_slice(),
                imports.iter().map(|(_, value)| *value).collect(),
                p::Distribution::Singleton,
                p::RowMultiplicity::SingleCopy,
            )
            .unwrap();
        (
            node,
            p::SummaryColumns::write_counts(imports[0].1, imports[1].1, imports[2].1),
        )
    });
    let finish = builder.reserve_node_id().unwrap();
    let outputs = relation_fields(&mut builder, finish, true);
    builder
        .insert_node_unchecked(p::PhysicalNode {
            id: finish,
            inputs: std::iter::once(exchange)
                .chain(summary_input.map(|(node, _)| node))
                .collect(),
            required_inputs: std::iter::repeat_n(
                property(p::Distribution::Singleton),
                1 + usize::from(summary_input.is_some()),
            )
            .collect(),
            output_properties: property(p::Distribution::Singleton),
            output: p::OutputPort {
                node: finish,
                columns: outputs.iter().map(|field| field.value).collect(),
            },
            kind: p::NodeKind::TableFinish(p::WriterFinishSpec {
                count_mode: if change_stream {
                    p::FinishCountMode::LogicalChanges
                } else {
                    p::FinishCountMode::WriterReported
                },
                inputs: novarocks_physical_plan::FinishInputs {
                    writer_results: 0,
                    summaries: novarocks_physical_plan::PerKind {
                        write_counts: summary_input.map(|_| 1),
                        delete_verification: None,
                    },
                },
                summaries: p::PerKind {
                    write_counts: summary_input.map(|(_, columns)| columns),
                    delete_verification: None,
                },
                expected_target_ordinals: Box::from([ordinal]),
                input_schema: p::WriterRelationSchema {
                    revision: p::WRITER_MULTIPLEX_SCHEMA_REVISION,
                    fields: imported.clone(),
                },
                output_schema: p::WriterRelationSchema {
                    revision: p::ROOT_WRITE_RESULT_SCHEMA_REVISION,
                    fields: outputs,
                },
                final_aggregates: Box::default(),
                grouped_unpivot: None,
            }),
        })
        .unwrap();
    let consumer = builder
        .finish_definition(
            finish,
            p::FragmentSink::Noop,
            dop(),
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let mut plan = p::PlanBuilder::new(p::PlanVersionId::try_new([1; 16]).unwrap());
    if constant_row {
        plan.insert_constant_pool(
            pool,
            novarocks_constant_contract::ConstantPool::try_new(
                Arc::new(Field::new("row", DataType::Int64, false)),
                FunctionValueType::new(DataType::Int64, false),
                arrow::array::Array::to_data(&arrow::array::Int64Array::from(vec![42])),
                p::ConstantPolicy {
                    max_rows: 16,
                    max_array_nodes: 32,
                    max_logical_elements: 128,
                    max_retained_buffer_bytes: 65536,
                    max_type_depth: 16,
                    max_type_nodes: 128,
                    max_dictionary_depth: 8,
                    max_metadata_bytes: 4096,
                    max_library_validation_work: 1_000_000,
                    max_library_validation_bytes: 1_000_000,
                },
                CompilePhase::Validate,
                &Setup,
            )
            .unwrap(),
        )
        .unwrap();
    }
    if let Some((router_fragment, route_value, summary)) = router {
        let source_id = router_fragment.id();
        plan.add_fragment(router_fragment).unwrap();
        plan.add_edge(p::Edge {
            role: p::OutputRole::ProviderRoute { target: ordinal },
            delivery: p::DeliveryRequirement::MustDrain,
            id: route_edge,
            kind: p::EdgeKind::ChangeStreamRouter,
            source: p::EdgeSource {
                port: p::FragmentOutputPort::Root,
                fragment: source_id,
                projection: values.iter().map(|_| route_value).collect(),
            },
            destination: p::EdgeDestination {
                fragment: producer.id(),
                node: source,
                receive_mapping: values.iter().map(|value| (route_value, *value)).collect(),
            },
            partitioning: p::EdgePartitioning {
                source: p::Distribution::Singleton,
                source_multiplicity: p::RowMultiplicity::SingleCopy,
                destination: p::Distribution::Singleton,
                destination_multiplicity: p::RowMultiplicity::SingleCopy,
            },
        })
        .unwrap();
        let (summary_node, imported) =
            summary_input.expect("an actual Router always has its exact summary input");
        plan.add_edge(p::Edge {
            role: p::OutputRole::Summary(p::SummaryKind::WriteCounts),
            delivery: p::DeliveryRequirement::MustDrain,
            id: summary_edge,
            kind: p::EdgeKind::Stream,
            source: p::EdgeSource {
                port: p::FragmentOutputPort::Summary(p::SummaryKind::WriteCounts),
                fragment: source_id,
                projection: Box::from([
                    summary.columns.values()[0],
                    summary.columns.values()[1],
                    summary.columns.values()[2],
                ]),
            },
            destination: p::EdgeDestination {
                fragment: consumer.id(),
                node: summary_node,
                receive_mapping: Box::from([
                    (summary.columns.values()[0], imported.values()[0]),
                    (summary.columns.values()[1], imported.values()[1]),
                    (summary.columns.values()[2], imported.values()[2]),
                ]),
            },
            partitioning: p::EdgePartitioning {
                source: p::Distribution::Singleton,
                source_multiplicity: p::RowMultiplicity::SingleCopy,
                destination: p::Distribution::Singleton,
                destination_multiplicity: p::RowMultiplicity::SingleCopy,
            },
        })
        .unwrap();
    }
    plan.add_fragment(producer).unwrap();
    plan.add_fragment(consumer).unwrap();
    plan.add_edge(p::Edge {
        role: p::OutputRole::WriterResults,
        delivery: p::DeliveryRequirement::MustDrain,
        id: edge,
        kind: p::EdgeKind::Stream,
        source: p::EdgeSource {
            port: p::FragmentOutputPort::Root,
            fragment: p::FragmentId::new(1),
            projection: fields.iter().map(|field| field.value).collect(),
        },
        destination: p::EdgeDestination {
            fragment: p::FragmentId::new(2),
            node: exchange,
            receive_mapping: fields
                .iter()
                .zip(&imported)
                .map(|(a, b)| (a.value, b.value))
                .collect(),
        },
        partitioning: p::EdgePartitioning {
            source: p::Distribution::Singleton,
            source_multiplicity: p::RowMultiplicity::SingleCopy,
            destination: p::Distribution::Singleton,
            destination_multiplicity: p::RowMultiplicity::SingleCopy,
        },
    })
    .unwrap();
    let plan = plan.finish_observed(&Setup).unwrap();
    let mut uses = BTreeMap::new();
    let mut calls = BTreeMap::new();
    let mut pruning = BTreeMap::new();
    let mut admissions = BTreeMap::new();
    for (id, fragment) in plan.fragments() {
        let roots = p::PhysicalExpressionRoots::try_new(fragment, &Setup).unwrap();
        let bindings = roots
            .sites()
            .iter()
            .enumerate()
            .map(|(ordinal, (site, _))| (*site, ExpressionUseId::new(ordinal as u32)))
            .collect::<Vec<_>>();
        let invocations = roots
            .sites()
            .iter()
            .enumerate()
            .map(|(ordinal, (_, root))| {
                assert!(matches!(
                    fragment.expressions().get(root.expr).unwrap().kind,
                    p::ExprKind::Literal(_) | p::ExprKind::Constant(_) | p::ExprKind::Value(_)
                ));
                ExpressionInvocation {
                    context: ExpressionEffectContext {
                        use_id: ExpressionUseId::new(ordinal as u32),
                        domain: EvaluationDomainId::new(0),
                        demand: root.demand,
                    },
                    definition: root.expr,
                    control: ControlShape::Eager,
                    arguments: Box::default(),
                }
            })
            .collect::<Vec<_>>();
        let domains = if invocations.is_empty() {
            vec![]
        } else {
            vec![ExpressionEvaluationDomain {
                id: EvaluationDomainId::new(0),
                parent: None,
                guard: None,
            }]
        };
        let flow = ExpressionControlFlow::try_new(
            domains,
            invocations,
            fragment.expressions(),
            CompilePhase::Validate,
            &Setup,
        )
        .unwrap();
        let actual = p::PhysicalRootUses::try_new(fragment, flow, bindings, &Setup).unwrap();
        calls.insert(
            *id,
            p::FrozenFragmentCalls::try_new(fragment, &actual, vec![], &Setup).unwrap(),
        );
        uses.insert(*id, actual);
        pruning.insert(
            *id,
            p::FrozenFragmentPruning::try_new(*id, vec![], &Setup).unwrap(),
        );
        admissions.insert(
            *id,
            p::FragmentPackageAdmission {
                plan_limits: p::PlanLimits::FROZEN,
                source_retained_bytes: SOURCE,
                property_projection_limits: p::PropertyProofProjectionLimits {
                    max_request_bytes: 64 * 1024 * 1024,
                    max_coexisting_bytes: 512 * 1024 * 1024,
                    max_projection_work: 128 * 1024 * 1024,
                },
            },
        );
    }
    p::extract_fragment_packages(
        &plan,
        &BTreeMap::new(),
        &BTreeMap::from([(ordinal, recipe)]),
        &uses,
        &calls,
        &pruning,
        &admissions,
        &Setup,
    )
    .unwrap()
}

#[test]
fn sender_checked_writer_package_retains_large_top_level_metadata_without_widening_value_law() {
    let recipe = data(Field::new("v", DataType::Int64, false).with_metadata(metadata()));
    let original = recipe.clone();
    let package = checked_writer_package(recipe);
    let (node, source) = package.writes().iter().next().unwrap();
    let p::NodeKind::TableWriter { target } = &package.fragment().nodes()[node].kind else {
        panic!("actual writer expected")
    };
    assert_eq!(
        target.target_fields[0].ty,
        FunctionValueType::new(DataType::Int64, false)
    );
    let ids = [u32::MAX];
    let writers = [WriterTypeSource::new(source, &ids)];
    let control = Control::default();
    let encoded = projected(&[], &[], &writers, &control).unwrap();
    let actual = source.input().fields_iter().next().unwrap().field();
    let original = original.input().fields_iter().next().unwrap().field();
    assert!(
        std::ptr::eq(actual, original),
        "original draft clone retains the actual input owner"
    );
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    assert!(std::ptr::eq(
        encoded
            .source_field_source_observed(u32::MAX, &mut work)
            .unwrap()
            .unwrap(),
        actual
    ));
    work.finish().unwrap();
    assert_eq!(
        original_source_wire(source, u32::MAX).field_facts[0].metadata,
        vec![
            plan::ArrowFieldMetadataEntry {
                key: "a".into(),
                value: "雪".repeat(6826) + "ab"
            },
            plan::ArrowFieldMetadataEntry {
                key: "z".into(),
                value: "last\0value".into()
            }
        ]
    );
    assert_eq!(package.writes().len(), 1);
    // These tests prove pure source/component projection, not provider runtime
    // capabilities or acceptance of wide Value carriers in a checked package.
}

#[test]
fn sender_all_five_input_shapes_keep_order_and_clone_loan_without_equal_foreign_substitution() {
    for role in 0..5 {
        let a = field(1, Field::new("first", DataType::Int64, false));
        let b = field(2, Field::new("second", DataType::Utf8, true));
        let input = match role {
            0 => c::ConnectorWriteInputShape::Data { fields: vec![a, b] },
            1 => c::ConnectorWriteInputShape::RowLineage {
                data_fields: vec![a],
                row_identity_fields: vec![b],
            },
            2 => c::ConnectorWriteInputShape::PositionDelete {
                identity_fields: vec![a],
                partition_source_fields: vec![b],
            },
            3 => c::ConnectorWriteInputShape::DeletionVector {
                identity_fields: vec![a],
                partition_source_fields: vec![b],
            },
            _ => c::ConnectorWriteInputShape::EqualityDelete {
                equality_fields: vec![a, b],
            },
        };
        let recipe = draft(input.clone());
        let cloned = recipe.clone();
        let foreign = draft(input);
        let ids = [u32::MAX, 0];
        let writers = [WriterTypeSource::new(&cloned, &ids)];
        let control = Control::default();
        let encoded = projected(&[], &[], &writers, &control).unwrap();
        assert_eq!(
            source_shape(&encoded),
            &wire::TypeTable {
                logical_types: Vec::new(),
                carriers: vec![
                    carrier(0, Kind::Primitive(plan::ArrowPrimitiveType::Int64 as i32)),
                    carrier(1, Kind::Primitive(plan::ArrowPrimitiveType::Utf8 as i32))
                ],
                fields: vec![
                    wire_field(u32::MAX, 0, "first", false),
                    wire_field(0, 1, "second", true)
                ],
                value_types: vec![],
            }
        );
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
        for ((id, original), equal_foreign) in ids
            .iter()
            .zip(recipe.input().fields_iter())
            .zip(foreign.input().fields_iter())
        {
            let actual = encoded
                .source_field_source_observed(*id, &mut work)
                .unwrap()
                .unwrap();
            assert!(std::ptr::eq(actual, original.field()));
            assert!(!std::ptr::eq(actual, equal_foreign.field()));
            assert!(novarocks_type_contract::arrow_fields_exact(
                actual,
                equal_foreign.field()
            ));
        }
        work.finish().unwrap();
    }
}

#[test]
fn sender_metadata_next_known_string_refusal_precedes_its_actual_late_callback() {
    let recipe = data(
        Field::new("v", DataType::Int64, false)
            .with_metadata(HashMap::from([("k".into(), "雪".repeat(6826) + "ab")])),
    );
    let ids = [7];
    let writers = [WriterTypeSource::new(&recipe, &ids)];
    let control = Control::default();
    let mut gates = Vec::new();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    projected_in(
        &[],
        &[],
        &writers,
        SOURCE,
        limits(),
        &mut |facts| {
            gates.push((*facts, control.trace().len()));
            Ok(())
        },
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    let baseline = control.trace();
    // Independent source bytes: root name1 + the sole entry key1 + value20480.
    // Identify the first original numerical gate at that actual iterator result,
    // not a simulated quantum or a callback ordinal copied from implementation.
    let known_string_bytes = 1 + 1 + 20 * 1024;
    let (facts, next_callback) = gates
        .iter()
        .find(|(facts, _)| facts.string_bytes == known_string_bytes)
        .unwrap();
    assert_eq!(facts.string_bytes, known_string_bytes);
    assert!(*next_callback < baseline.len());
    let mut tight = limits();
    tight.max_string_bytes = known_string_bytes - 1;
    for cause in CAUSES {
        let control = Control {
            stop: Some((*next_callback, cause)),
            ..Default::default()
        };
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
        let result = projected_in(
            &[],
            &[],
            &writers,
            SOURCE,
            tight,
            &mut |_| Ok(()),
            &mut work,
        );
        assert!(matches!(
            result,
            Err(TypeCodecError::Control(
                CompileControlError::ResourceExhausted
            ))
        ));
        assert_eq!(control.trace(), baseline[..*next_callback]);
        // Numeric Resource owns the result. No caller footer or opaque-exit
        // observation may invoke the armed later refusal.
    }
}

#[test]
fn sender_zero_source_invoice_refuses_before_pending_source_callbacks() {
    let recipe = data(Field::new("v", DataType::Int64, false));
    let ids = [0];
    let writers = [WriterTypeSource::new(&recipe, &ids)];
    for pending in [0, 254, 255] {
        for cause in CAUSES {
            let control = Control {
                stop: Some((1, cause)),
                ..Default::default()
            };
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
            for _ in 0..pending {
                work.step().unwrap();
            }
            assert_eq!(control.trace(), vec![0]);
            let result = projected_in(&[], &[], &writers, 0, limits(), &mut |_| Ok(()), &mut work);
            assert!(matches!(
                result,
                Err(TypeCodecError::Control(
                    CompileControlError::ResourceExhausted
                ))
            ));
            assert_eq!(control.trace(), vec![0]);
        }
    }
}

#[test]
fn sender_actual_projection_admits_complete_request_inventory_and_refuses_each_axis() {
    let recipe = data(
        Field::new(
            "v",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("z".repeat(2000).into())),
            true,
        )
        .with_metadata(HashMap::from([("m".into(), "x".into())])),
    );
    let ids = [u32::MAX];
    let writers = [WriterTypeSource::new(&recipe, &ids)];
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    let mut last = None;
    let encoded = projected_in(
        &[],
        &[],
        &writers,
        SOURCE,
        limits(),
        &mut |facts| {
            last = Some(*facts);
            Ok(())
        },
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    let facts = last.unwrap();
    assert_eq!(facts.definition_count, 2);
    assert_eq!(facts.expanded_node_count, 2);
    assert_eq!(facts.string_bytes, 2003);
    assert_eq!(source_shape(&encoded).carriers.len(), 1);
    assert_eq!(source_shape(&encoded).fields.len(), 1);
    // One reserved root Set upper, one bounded terminal diagnostic, four
    // nonempty Strings, two metadata Vecs, and two nonempty namespace Vecs.
    assert!(facts.allocation_requests_upper_bound >= 1 + 128 + 4 + 2 + 2);
    let pointer = std::mem::size_of::<usize>();
    let align = std::mem::align_of::<usize>().max(std::mem::align_of::<u32>());
    let tree_raw = pointer + 4 + 11 * 4 + 5 * (align - 1) + 12 * pointer + align - 1;
    let tree_bytes = tree_raw.div_ceil(align) * align;
    let expected_bytes = tree_bytes
        + 512
        + 2003
        + std::mem::size_of::<(&str, &str)>()
        + std::mem::size_of::<plan::ArrowFieldMetadataEntry>()
        + std::mem::size_of::<wire::CarrierTypeDefinition>()
        + std::mem::size_of::<wire::FieldDefinition>();
    assert!(facts.allocation_request_bytes_upper_bound >= expected_bytes);
    assert_eq!(
        facts.coexisting_source_and_request_bytes_upper_bound,
        SOURCE + facts.allocation_request_bytes_upper_bound
    );
    for axis in 0..7 {
        let mut under = limits();
        match axis {
            0 => under.max_definitions = facts.definition_count - 1,
            1 => under.max_expanded_nodes = facts.expanded_node_count - 1,
            2 => under.max_string_bytes = facts.string_bytes - 1,
            3 => under.max_allocation_requests = facts.allocation_requests_upper_bound - 1,
            4 => {
                under.max_allocation_request_bytes = facts.allocation_request_bytes_upper_bound - 1
            }
            5 => {
                under.max_coexisting_source_and_request_bytes =
                    facts.coexisting_source_and_request_bytes_upper_bound - 1
            }
            _ => under.max_work = facts.cumulative_work_upper_bound - 1,
        }
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
        let result = projected_in(
            &[],
            &[],
            &writers,
            SOURCE,
            under,
            &mut |_| Ok(()),
            &mut work,
        );
        assert!(
            matches!(
                result,
                Err(TypeCodecError::Control(
                    CompileControlError::ResourceExhausted
                ))
            ),
            "axis {axis}"
        );
        // Control errors own no success or ordinary footer. The only caller
        // entry remains part of the real trace; no fabricated test quantum.
        assert_eq!(control.trace().first(), Some(&0));
    }
}

fn strict_value_facts(values: &[(u32, FunctionValueType)]) -> PackageTypeProjectionFacts {
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    let mut last = None;
    encode_type_table_writer_sources_observed(
        values,
        &[],
        &[],
        SOURCE,
        limits(),
        &mut |facts| {
            last = Some(*facts);
            Ok(())
        },
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    last.unwrap()
}

/// Each strict root admits its validator from its own actual node count. A
/// thousand scalar roots fit the request ceiling that the former per-type
/// 4096-node charge exhausted at 245 roots; every List level adds exactly
/// its validator node and its item Field name, so a deeper type charges more
/// while staying far below the per-type maximum.
#[test]
fn sender_strict_roots_admit_their_validator_from_actual_type_size() {
    use novarocks_type_contract::{MAX_VALUE_TYPE_DEPTH, MAX_VALUE_TYPE_NODES};
    assert!(245 * MAX_VALUE_TYPE_NODES > limits().max_allocation_requests);
    let scalars = (0..1000)
        .map(|id| (id, FunctionValueType::new(DataType::Int64, true)))
        .collect::<Vec<_>>();
    let wide = strict_value_facts(&scalars);
    assert_eq!(wide.definition_count, 3000);
    // The complete three-definition scalar projection remains linear in
    // source root count, far below one MAX_VALUE_TYPE_NODES scratch per root.
    assert!(wide.allocation_requests_upper_bound < 3 * 1000, "{wide:?}");
    let one = strict_value_facts(&scalars[..1]);
    let tuple = std::mem::size_of::<novarocks_type_contract::TypeTraversalFrame<'_>>();
    assert!(one.allocation_request_bytes_upper_bound < MAX_VALUE_TYPE_NODES * tuple);

    let nested = |depth: usize| {
        let ty = (0..depth).fold(DataType::Int64, |ty, _| {
            DataType::List(Arc::new(Field::new_list_field(ty, true)))
        });
        [(0, FunctionValueType::new(ty, true))]
    };
    let mut previous = strict_value_facts(&nested(1));
    assert!(previous.allocation_requests_upper_bound > one.allocation_requests_upper_bound);
    for depth in 2..MAX_VALUE_TYPE_DEPTH {
        let facts = strict_value_facts(&nested(depth));
        assert_eq!(
            facts.allocation_requests_upper_bound - previous.allocation_requests_upper_bound,
            2,
            "depth {depth}"
        );
        assert!(
            facts.allocation_request_bytes_upper_bound
                - previous.allocation_request_bytes_upper_bound
                >= 4 * tuple,
            "depth {depth}"
        );
        assert!(facts.cumulative_work_upper_bound > previous.cumulative_work_upper_bound);
        previous = facts;
    }
    assert!(previous.allocation_requests_upper_bound < MAX_VALUE_TYPE_NODES);
}

/// The admitted children of every visited carrier are exactly the original
/// walker's pushes, so a valid strict root admits one node per visit.
#[test]
fn strict_validator_admission_matches_every_original_walker_visit() {
    use arrow::datatypes::{UnionFields, UnionMode};
    use novarocks_type_contract::{ValueTypeError, validate_value_type_structure_observed};
    let item = |ty: DataType| Arc::new(Field::new("item", ty, true));
    let entries = Field::new(
        "entries",
        DataType::Struct(
            vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int64, true),
            ]
            .into(),
        ),
        false,
    );
    let union = UnionFields::try_new(
        [0, 3],
        [
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::List(item(DataType::Utf8)), true),
        ],
    )
    .unwrap();
    let types = [
        DataType::Int64,
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        DataType::List(item(DataType::Int64)),
        DataType::LargeList(item(DataType::Utf8)),
        DataType::ListView(item(DataType::Int8)),
        DataType::LargeListView(item(DataType::Int16)),
        DataType::FixedSizeList(item(DataType::Float64), 3),
        DataType::Map(Arc::new(entries), false),
        DataType::Struct(Vec::<Field>::new().into()),
        DataType::Struct(
            vec![
                Field::new("x", DataType::Int64, true),
                Field::new("y", DataType::List(item(DataType::Boolean)), true),
                Field::new(
                    "z",
                    DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                    true,
                ),
            ]
            .into(),
        ),
        DataType::Union(union, UnionMode::Dense),
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Binary)),
        DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int32, false)),
            Arc::new(Field::new("values", DataType::Utf8, true)),
        ),
    ];
    for ty in &types {
        let (mut visits, mut admitted) = (0usize, 1usize);
        validate_value_type_structure_observed::<ValueTypeError>(ty, |visit| {
            if let ValueTypeVisit::TypeNode(node) = visit {
                visits += 1;
                admitted += super::encode::strict_children(node);
            }
            Ok(())
        })
        .unwrap();
        assert_eq!(admitted, visits, "{ty}");
    }
}
