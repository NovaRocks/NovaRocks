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
use crate::physical_connector_payload_v2::{
    ConnectorPayloadProjectionLimits, decode_connector_payloads, encode_connector_payloads,
};
use crate::physical_node_v2::NodeProjectionLimits;
use crate::physical_properties_v2::PhysicalPropertyProjectionLimits;
use crate::physical_provider_binding_v2::{
    ProviderBindingProjectionLimits, decode_provider_bindings, encode_provider_bindings,
};
use crate::physical_public_source_schema_v2::{
    SourceSchemaFactsSource, decode_public_source_schemas_pure_in,
    encode_public_source_schemas_pure_in,
};
use crate::physical_schema_v2::{SchemaSource, prepare_schemas_decode, prepare_schemas_encode};
use crate::physical_type_v2::{
    BorrowedFieldTypeRoot, TypeProjectionLimits, decode_type_table,
    encode_borrowed_type_table_associated_sources_pure_in,
};
use arrow::datatypes::{DataType, Field, Schema};
use novarocks_type_contract::{
    CompilePhase, FunctionValueType, PureCompileControl, ValueField, ValueKind, ValueLayout,
    ValueTypeRef, ValueTypeStorageLifetime,
};
use std::{collections::HashMap, sync::Mutex};
const SOURCE: usize = 64 << 20;
const NAMESPACE_SOURCE: usize = 1 << 20;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    stop: Mutex<Option<(usize, CompileControlError)>>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut t = self.trace.lock().unwrap();
        let at = t.len();
        let stop = self.stop.lock().unwrap().clone();
        if let Some((stop, _)) = stop {
            assert!(at <= stop, "callback after primary refusal");
        }
        t.push(units);
        match stop {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
impl Control {
    fn arm(&self, stop: Option<(usize, CompileControlError)>) {
        self.trace.lock().unwrap().clear();
        *self.stop.lock().unwrap() = stop;
    }
    fn trace(&self) -> Vec<u32> {
        self.trace.lock().unwrap().clone()
    }
}
fn limits() -> ReadScanProjectionLimits {
    ReadScanProjectionLimits {
        max_scans: 1000,
        max_items: 1_000_000,
        max_scalar_bytes: 64 << 20,
        max_allocation_requests: 1_000_000,
        max_allocation_request_bytes: 512 << 20,
        max_coexisting_source_and_request_bytes: 1 << 30,
        max_work: usize::MAX / 4,
    }
}
fn provider_limits() -> ProviderBindingProjectionLimits {
    ProviderBindingProjectionLimits {
        max_definitions: 1000,
        max_allocation_requests: 100_000,
        max_allocation_request_bytes: 64 << 20,
        max_coexisting_source_and_request_bytes: 128 << 20,
        max_work: usize::MAX / 4,
    }
}
fn payload_limits() -> ConnectorPayloadProjectionLimits {
    ConnectorPayloadProjectionLimits {
        max_definitions: 8192,
        max_payload_bytes: 64 << 20,
        max_allocation_requests: 100_000,
        max_allocation_request_bytes: 64 << 20,
        max_coexisting_source_and_request_bytes: 128 << 20,
        max_work: usize::MAX / 4,
    }
}
fn type_limits() -> TypeProjectionLimits {
    TypeProjectionLimits {
        max_definitions: 10_000,
        max_expanded_nodes: 100_000,
        max_string_bytes: 64 << 20,
    }
}
fn schema_limits() -> NodeProjectionLimits {
    NodeProjectionLimits {
        max_input_nodes: 0,
        max_value_references: 10_000,
        max_list_items: 10_000,
        max_allocation_requests: 100_000,
        max_allocation_request_bytes: 64 << 20,
        max_coexisting_source_and_request_bytes: 128 << 20,
        max_work: usize::MAX / 4,
        properties: PhysicalPropertyProjectionLimits {
            max_value_references: 0,
            max_allocation_requests: 0,
            max_allocation_request_bytes: 0,
            max_coexisting_source_and_request_bytes: 128 << 20,
            max_work: usize::MAX / 4,
        },
    }
}
fn source_attributes(original: &arrow::datatypes::Metadata) -> (HashMap<String, String>, usize) {
    // The fixture author creates this table in one explicit reserve. Its
    // source work proof includes actual copied String backing, not a floor.
    let table = novarocks_type_contract::owned_resources::hashmap::fresh_table_layout::<
        String,
        String,
    >(original.len())
    .unwrap();
    let mut output = HashMap::with_capacity(original.len());
    let mut invoice = table.request_bytes_upper_bound;
    for (key, value) in original {
        let key = key.clone();
        let value = value.clone();
        invoice += key.capacity() + value.capacity();
        output.insert(key, value);
    }
    (output, invoice)
}
fn typed_public(
    source: c::ConnectorReadStaticFacts<c::ScanColumnId>,
    metadata: Option<c::ConnectorReadMetadataKind>,
    original: Schema,
    bindings: Vec<FunctionValueType>,
) -> c::ConnectorReadPublicFacts {
    let pure = ValueTypeStorageLifetime::pure();
    let fields = original
        .fields()
        .iter()
        .map(|field| {
            Arc::new(Field::new(
                field.name(),
                field.data_type().clone(),
                field.is_nullable(),
            ))
        })
        .collect::<Vec<_>>();
    let execution = c::ConnectorReadExecutionSchema::try_new(
        Arc::new(Schema::new(fields.clone())),
        bindings.into_boxed_slice(),
        pure.clone(),
    )
    .unwrap();
    let shape = c::ConnectorSourceSchemaShape::try_new(Arc::new(Schema::new(fields)), pure.clone())
        .unwrap();
    let mut facts = Vec::new();
    for (ordinal, field) in original.fields().iter().enumerate() {
        let (attributes, invoice) = source_attributes(field.metadata());
        facts.push(
            c::ConnectorSourceFieldFacts::try_new_observed(
                vec![ordinal as u32].into(),
                None,
                None,
                attributes,
                &[],
                pure.clone(),
                invoice,
                |_| Ok::<_, c::ConnectorError>(()),
                |_| Ok(()),
            )
            .unwrap(),
        );
    }
    let (attributes, invoice) = source_attributes(original.metadata());
    let source_facts = c::ConnectorSourceSchemaFacts::try_new_observed_with_source_invoice(
        shape,
        attributes,
        facts.into_boxed_slice(),
        pure.clone(),
        invoice,
        |_| Ok::<_, c::ConnectorError>(()),
        |_| Ok(()),
    )
    .unwrap();
    c::ConnectorReadPublicFacts::try_new_typed_with_control(
        source,
        metadata,
        &execution,
        &source_facts,
        &Control::default(),
    )
    .unwrap()
}

fn read(count: usize, metadata: bool, residual: bool) -> c::FrozenConnectorRead {
    let provider = c::ConnectorProviderId::parse("iceberg").unwrap();
    let instance = c::ConnectorInstanceId::try_from_canonical("lake").unwrap();
    let catalog = c::CatalogHandle::new(instance.clone(), c::CatalogVersion::from_bytes([7; 32]));
    let binding = c::ConnectorReadBinding::new(
        c::ConnectorInstanceDescriptor {
            provider_id: provider.clone(),
            instance_id: instance,
        },
        catalog.clone(),
    );
    let payload = |category| {
        c::ConnectorEncodedPayload::new(
            c::ConnectorEnvelopeHeader::new(
                provider.clone(),
                catalog.clone(),
                category,
                c::ConnectorCodecRevision::try_new(1).unwrap(),
            ),
            vec![9, 8, 7].into(),
        )
    };
    let recipe = c::ConnectorReadRelationRecipeDraft::try_new(
        binding,
        c::ConnectorReadRelationPayload::new(
            if metadata {
                c::ConnectorReadRelationKind::SystemTable
            } else {
                c::ConnectorReadRelationKind::Table
            },
            payload(c::ConnectorCodecCategory::ReadTable),
            payload(c::ConnectorCodecCategory::ReadView),
        ),
        (0..count)
            .map(|_| payload(c::ConnectorCodecCategory::ReadColumn))
            .collect(),
    )
    .unwrap();
    let mut domains = BTreeMap::new();
    domains.insert(
        c::ScanColumnId::new(0),
        c::Domain::new(
            c::ValueSet::of_ranges(
                c::ConnectorValueType::BigInt,
                vec![
                    c::Range::try_new(
                        c::ConnectorValueType::BigInt,
                        c::Bound::Inclusive(c::ConnectorValue::BigInt(5)),
                        c::Bound::Unbounded,
                    )
                    .unwrap(),
                ],
            )
            .unwrap(),
            true,
        ),
    );
    let expression = residual.then(|| c::ConnectorExpression::Call {
        function: c::ConnectorFunctionName::try_new("$equal").unwrap(),
        value_type: c::ConnectorValueType::Boolean,
        arguments: vec![
            c::ConnectorExpression::Variable {
                name: Arc::from("v0"),
                value_type: c::ConnectorValueType::BigInt,
            },
            c::ConnectorExpression::Constant {
                value: None,
                value_type: c::ConnectorValueType::BigInt,
            },
        ],
    });
    let scan = c::FrozenConnectorScan::try_new(
        recipe,
        (0..count)
            .map(|i| {
                c::StaticScanAssignment::new(
                    Arc::from(format!("v{i}")),
                    c::ConnectorValueType::BigInt,
                )
            })
            .collect(),
        c::TupleDomain::with_column_domains(domains).unwrap(),
        c::TupleDomain::none(),
        expression,
        vec![
            c::StaticScanDynamicFilter::new(0, Arc::from("v0")),
            c::StaticScanDynamicFilter::new(u32::MAX, Arc::from("v0")),
        ],
        NonZeroU64::new(123).unwrap(),
        NonZeroU64::new(4097).unwrap(),
    )
    .unwrap();
    let properties = c::ConnectorReadProperties::try_new(
        c::ConnectorReadDistribution::BucketShuffle {
            keys: Arc::from([c::ScanColumnId::new(0)]),
            partition_space: [2; 32],
            bucket_count: 8,
            hash: c::ConnectorReadPartitionHash::Murmur3X64_128,
            layout: c::ConnectorReadBucketLayout::JumpConsistent,
            ordinal_domain_evidence: [3; 32],
        },
        vec![c::ConnectorReadOrderingKey::new(
            c::ScanColumnId::new(0),
            c::ConnectorReadSortDirection::Descending,
            c::ConnectorReadNullOrdering::First,
        )],
    )
    .unwrap();
    let source = c::ConnectorReadStaticFacts::try_new(
        c::ConnectorReadInputVersion::try_new(vec![1, 2, 3]).unwrap(),
        [4; 32],
        properties,
        c::ConnectorReadArtifactCoverage::exact([5; 32], [6; 32], vec![7, 8]).unwrap(),
        vec![9, 10],
    )
    .unwrap();
    let fields = (0..count)
        .map(|i| {
            Arc::new(
                Field::new(format!("col{i}"), DataType::Int64, i % 2 == 0).with_metadata(
                    std::collections::HashMap::<String, String>::from([(
                        "ordinal".into(),
                        i.to_string(),
                    )]),
                ),
            )
        })
        .collect::<Vec<_>>();
    let schema = Schema::new_with_metadata(
        fields,
        HashMap::from([(
            "source".into(),
            "雪".repeat(if metadata { 4096 } else { 1 }),
        )]),
    );
    let public = typed_public(
        source,
        metadata.then(|| c::ConnectorReadMetadataKind::try_new("snapshots").unwrap()),
        schema,
        (0..count)
            .map(|i| FunctionValueType::new(DataType::Int64, i % 2 == 0))
            .collect(),
    );
    c::FrozenConnectorRead::try_new(scan, public).unwrap()
}
fn with_encoded<T>(
    read: &c::FrozenConnectorRead,
    control: &dyn PureCompileControl,
    run: impl FnOnce(ReadScanEncodeContext<'_, '_, '_>) -> T,
) -> T {
    let r = read.scan().recipe();
    let providers = [(u32::MAX, r.binding())];
    let mut inputs = vec![(0, r.relation().table()), (u32::MAX, r.relation().view())];
    inputs.extend(
        r.columns()
            .iter()
            .enumerate()
            .map(|(i, p)| (i as u32 + 1, p)),
    );
    let bindings =
        encode_provider_bindings(&providers, NAMESPACE_SOURCE, provider_limits(), control).unwrap();
    let payloads =
        encode_connector_payloads(&inputs, NAMESPACE_SOURCE, payload_limits(), control).unwrap();
    let public = read.public_facts();
    let execution = public.execution_schema().unwrap();
    let source_facts = public.source_schema_facts().unwrap();
    let roots = execution
        .bindings()
        .iter()
        .enumerate()
        .map(|(i, ty)| (i as u32, ty))
        .collect::<Vec<_>>();
    let count = roots.len() as u32;
    let field_ids = (0..count).collect::<Vec<_>>();
    let shape_field_ids = (count..count * 2).collect::<Vec<_>>();
    let mut fields = execution
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| BorrowedFieldTypeRoot {
            id: i as u32,
            field,
            value_type_id: Some(i as u32),
        })
        .collect::<Vec<_>>();
    fields.extend(
        source_facts
            .shape()
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(i, field)| BorrowedFieldTypeRoot {
                id: count + i as u32,
                field,
                value_type_id: None,
            }),
    );
    let mut type_work = CompileCheckpoints::try_new(control, CompilePhase::Encode).unwrap();
    let types = encode_borrowed_type_table_associated_sources_pure_in(
        &roots,
        &fields,
        type_limits(),
        &mut type_work,
    )
    .unwrap();
    type_work.finish().unwrap();
    let sources = [
        SchemaSource {
            id: u32::MAX,
            source: execution.schema(),
            field_ids: &field_ids,
        },
        SchemaSource {
            id: u32::MAX - 1,
            source: source_facts.shape().schema(),
            field_ids: &shape_field_ids,
        },
    ];
    let schemas =
        prepare_schemas_encode(&sources, &types, NAMESPACE_SOURCE, schema_limits(), control)
            .unwrap()
            .emit()
            .unwrap();
    let source_schemas = [SourceSchemaFactsSource {
        id: 17,
        source_shape_schema_id: u32::MAX - 1,
        facts: source_facts,
    }];
    run(ReadScanEncodeContext {
        bindings: &bindings,
        payloads: &payloads,
        schemas: &schemas,
        source_schemas: &source_schemas,
    })
}
#[derive(Clone)]
struct Raw {
    scans: Vec<w::FrozenReadScan>,
    expressions: Vec<w::ConnectorExpressionDefinition>,
    providers: Vec<w::ProviderBindingDefinition>,
    payloads: Vec<w::ConnectorPayloadDefinition>,
    schemas: Vec<w::SchemaDefinition>,
    types: novarocks_proto_models::physical_type_v2::TypeTable,
    source_schemas: Vec<w::PublicSourceSchemaFactsDefinition>,
}
fn raw(read: &c::FrozenConnectorRead) -> Raw {
    let c = Control::default();
    with_encoded(read, &c, |context| {
        let mut work =
            CompileCheckpoints::try_new(context.bindings.original_control(), CompilePhase::Encode)
                .unwrap();
        let ids = if read.scan().remaining_expression().is_some() {
            vec![u32::MAX, 0, 17]
        } else {
            vec![]
        };
        let (s, e, _) = encode_read_scans_observed(
            &[ReadScanSource {
                node: NodeId::new(u32::MAX),
                read,
                schema_id: u32::MAX,
                source_schema_id: 17,
                expression_ids: &ids,
            }],
            context,
            SOURCE,
            limits(),
            &mut |_| Ok(()),
            &mut work,
        )
        .unwrap();
        let source_schemas =
            encode_public_source_schemas_pure_in(context.source_schemas, &mut work).unwrap();
        work.finish().unwrap();
        Raw {
            scans: s,
            expressions: e,
            providers: context.bindings.as_wire().to_vec(),
            payloads: context.payloads.as_wire().to_vec(),
            schemas: context.schemas.as_wire().to_vec(),
            types: context.schemas.types().as_wire().clone(),
            source_schemas,
        }
    })
}
type EncodedRead = Result<
    (
        Vec<w::FrozenReadScan>,
        Vec<w::ConnectorExpressionDefinition>,
        ReadScanProjectionFacts,
    ),
    E,
>;
type DecodedRead = Result<
    (
        Vec<(NodeId, c::FrozenConnectorRead)>,
        ReadScanProjectionFacts,
    ),
    E,
>;
fn encode_run(
    read: &c::FrozenConnectorRead,
    ids: &[u32],
    l: ReadScanProjectionLimits,
    stop: Option<(usize, CompileControlError)>,
) -> (EncodedRead, Vec<u32>) {
    let c = Control::default();
    let out = with_encoded(read, &c, |context| {
        c.arm(stop);
        let mut work =
            CompileCheckpoints::try_new(context.bindings.original_control(), CompilePhase::Encode)?;
        let out = encode_read_scans_observed(
            &[ReadScanSource {
                node: NodeId::new(u32::MAX),
                read,
                schema_id: u32::MAX,
                source_schema_id: 17,
                expression_ids: ids,
            }],
            context,
            SOURCE,
            l,
            &mut |_| Ok(()),
            &mut work,
        );
        if !matches!(&out, Err(E::Control(_))) {
            work.finish()?;
        }
        out
    });
    (out, c.trace())
}
fn decode_run(
    raw: &Raw,
    l: ReadScanProjectionLimits,
    stop: Option<(usize, CompileControlError)>,
) -> (DecodedRead, Vec<u32>) {
    let c = Control::default();
    let original: &dyn PureCompileControl = &c;
    let bindings = decode_provider_bindings(
        &raw.providers,
        NAMESPACE_SOURCE,
        provider_limits(),
        original,
    )
    .unwrap();
    let payloads =
        decode_connector_payloads(&raw.payloads, NAMESPACE_SOURCE, payload_limits(), original)
            .unwrap();
    let types = decode_type_table(&raw.types, type_limits(), original).unwrap();
    let schemas = prepare_schemas_decode(
        &raw.schemas,
        &types,
        NAMESPACE_SOURCE,
        schema_limits(),
        original,
    )
    .unwrap()
    .emit()
    .unwrap();
    let mut source_work = CompileCheckpoints::try_new(original, CompilePhase::Decode).unwrap();
    let source_schemas = decode_public_source_schemas_pure_in(
        &raw.source_schemas,
        &schemas,
        NAMESPACE_SOURCE,
        schema_limits(),
        &mut source_work,
    )
    .unwrap();
    source_work.finish().unwrap();
    c.arm(stop);
    let out = (|| {
        let mut work =
            CompileCheckpoints::try_new(bindings.original_control(), CompilePhase::Decode)?;
        let out = decode_read_scans_observed(
            &raw.scans,
            &raw.expressions,
            ReadScanDecodeContext {
                bindings: &bindings,
                payloads: &payloads,
                schemas: &schemas,
                source_schemas: Some(&source_schemas),
            },
            SOURCE,
            l,
            &mut |_| Ok(()),
            &mut work,
        );
        if !matches!(&out, Err(E::Control(_))) {
            work.finish()?;
        }
        out
    })();
    (out, c.trace())
}
fn ordinary<T>(out: Result<T, E>) {
    assert!(out.is_err());
    assert!(!matches!(out, Err(E::Control(_))));
}

#[test]
fn complete_data_and_metadata_scans_have_independent_sparse_wire_oracles() {
    for metadata in [false, true] {
        let read = read(2, metadata, true);
        let raw = raw(&read);
        let scan = &raw.scans[0];
        assert_eq!(scan.node_id, Some(u32::MAX));
        let r = scan.recipe.as_ref().unwrap();
        assert_eq!(r.provider_binding_id, Some(u32::MAX));
        assert_eq!(r.kind, if metadata { 4 } else { 1 });
        assert_eq!(r.table_payload_id, Some(0));
        assert_eq!(r.view_payload_id, Some(u32::MAX));
        assert_eq!(r.column_payload_ids, [1, 2]);
        let f = scan.facts.as_ref().unwrap();
        assert_eq!(
            f.assignments
                .iter()
                .map(|v| v.variable.as_str())
                .collect::<Vec<_>>(),
            ["v0", "v1"]
        );
        assert_eq!((f.max_batch_rows, f.max_batch_bytes), (123, 4097));
        assert_eq!(f.remaining_connector_expression_id, Some(u32::MAX));
        assert_eq!(
            f.dynamic_filters
                .iter()
                .map(|d| d.filter_id)
                .collect::<Vec<_>>(),
            [Some(0), Some(u32::MAX)]
        );
        let Some(w::scan_tuple_domain::Kind::Columns(columns)) =
            &f.enforced_predicate.as_ref().unwrap().kind
        else {
            panic!("typed column predicate")
        };
        assert_eq!(columns.entries.len(), 1);
        assert_eq!(columns.entries[0].column_ordinal, 0);
        let domain = columns.entries[0].domain.as_ref().unwrap();
        assert!(domain.null_allowed);
        let range = &domain.values.as_ref().unwrap().ranges[0];
        assert_eq!(
            range.low.as_ref().unwrap().kind,
            cv::BoundKind::Inclusive as i32
        );
        assert_eq!(
            range.low.as_ref().unwrap().value.as_ref().unwrap().value,
            Some(cv::value::Value::BigInt(5))
        );
        assert_eq!(
            range.high.as_ref().unwrap().kind,
            cv::BoundKind::Unbounded as i32
        );
        assert!(matches!(
            f.unenforced_predicate.as_ref().unwrap().kind,
            Some(w::scan_tuple_domain::Kind::None(_))
        ));
        assert_eq!(
            raw.expressions.iter().map(|d| d.id).collect::<Vec<_>>(),
            [0, 17, u32::MAX]
        );
        let Some(w::connector_expression_definition::Kind::Call(call)) = &raw.expressions[2].kind
        else {
            panic!("actual call")
        };
        assert_eq!(call.function_name, "$equal");
        assert_eq!(call.argument_connector_expression_ids, [0, 17]);
        let Some(w::connector_expression_definition::Kind::Constant(constant)) =
            &raw.expressions[1].kind
        else {
            panic!("actual typed null")
        };
        assert!(constant.value.is_none());
        assert_eq!(
            constant.value_type.as_ref().unwrap().kind,
            cv::ValueTypeKind::BigInt as i32
        );
        let p = scan.public_facts.as_ref().unwrap();
        assert_eq!(p.execution_schema_id, Some(u32::MAX));
        assert_eq!(p.source_schema_id, Some(17));
        assert_eq!(p.metadata_kind.as_deref(), metadata.then_some("snapshots"));
        let source = p.source.as_ref().unwrap();
        assert_eq!(source.input_version, [1, 2, 3]);
        assert_eq!(source.selection_digest, [4; 32]);
        assert_eq!(source.coverage_evidence, [9, 10]);
        let Some(w::connector_read_artifact_coverage::Kind::Exact(e)) =
            &source.artifact_coverage.as_ref().unwrap().kind
        else {
            panic!("exact evidence")
        };
        assert_eq!(e.source_selection_digest, [5; 32]);
        assert_eq!(e.content_digest, [6; 32]);
        assert_eq!(e.evidence, [7, 8]);
        let Some(w::connector_read_distribution::Kind::BucketShuffle(b)) = &source
            .properties
            .as_ref()
            .unwrap()
            .distribution
            .as_ref()
            .unwrap()
            .kind
        else {
            panic!("bucket")
        };
        assert_eq!((b.bucket_count, b.hash, b.layout), (8, 2, 2));
        assert_eq!(b.partition_space, [2; 32]);
        assert_eq!(b.ordinal_domain_evidence, [3; 32]);
        assert_eq!(
            source.properties.as_ref().unwrap().ordering[0],
            w::ConnectorReadOrderingKey {
                column_ordinal: 0,
                direction: 2,
                null_ordering: 1
            }
        );
        let (decoded, _) = decode_run(&raw, limits(), None);
        let (out, facts) = decoded.unwrap();
        assert_eq!(out[0], (NodeId::new(u32::MAX), read));
        assert_eq!(facts.scan_count, 1);
        assert!(facts.allocation_requests_upper_bound > 10);
        assert_eq!(
            out[0]
                .1
                .public_facts()
                .source_schema_facts()
                .unwrap()
                .schema_attributes()["source"]
                .len(),
            if metadata { 12288 } else { 3 }
        );
        assert_eq!(
            out[0]
                .1
                .public_facts()
                .source_schema_facts()
                .unwrap()
                .field_facts()[1]
                .attributes()["ordinal"],
            "1"
        );
    }
}

#[test]
fn residual_field_variable_value_and_typed_null_use_the_original_algebra() {
    let original = read(1, false, false);
    let recipe = original.scan().recipe().clone();
    let expression = c::ConnectorExpression::Call {
        function: c::ConnectorFunctionName::try_new("$opaque_provider_test").unwrap(),
        value_type: c::ConnectorValueType::Boolean,
        arguments: vec![
            c::ConnectorExpression::FieldDereference {
                target: Box::new(c::ConnectorExpression::Variable {
                    name: Arc::from("object"),
                    value_type: c::ConnectorValueType::NonComparable,
                }),
                field_index: 0,
                value_type: c::ConnectorValueType::BigInt,
            },
            c::ConnectorExpression::Constant {
                value: Some(c::ConnectorValue::Varbinary(Arc::from([0, 255, 7]))),
                value_type: c::ConnectorValueType::Varbinary,
            },
            c::ConnectorExpression::Constant {
                value: None,
                value_type: c::ConnectorValueType::BigInt,
            },
        ],
    };
    let scan = c::FrozenConnectorScan::try_new(
        recipe,
        vec![c::StaticScanAssignment::new(
            Arc::from("object"),
            c::ConnectorValueType::NonComparable,
        )],
        c::TupleDomain::all(),
        c::TupleDomain::all(),
        Some(expression.clone()),
        vec![],
        NonZeroU64::new(1).unwrap(),
        NonZeroU64::new(2).unwrap(),
    )
    .unwrap();
    let schema = Schema::new(vec![Field::new(
        "object",
        DataType::Struct(vec![Arc::new(Field::new("member", DataType::Int64, true))].into()),
        true,
    )]);
    let member = FunctionValueType::new(DataType::Int64, true);
    let value = ValueTypeRef::try_new(
        ValueKind::Struct(
            vec![ValueField::new(
                "member",
                member.value_type().clone(),
                ValueTypeStorageLifetime::pure(),
            )]
            .into(),
        ),
        true,
        ValueTypeStorageLifetime::pure(),
    )
    .unwrap();
    let binding = FunctionValueType::try_from_value(
        value,
        ValueLayout::try_new(
            schema.field(0).data_type(),
            ValueTypeStorageLifetime::pure(),
        )
        .unwrap(),
    )
    .unwrap();
    let public = typed_public(
        original.public_facts().source().clone(),
        None,
        schema,
        vec![binding],
    );
    let read = c::FrozenConnectorRead::try_new(scan, public).unwrap();
    let c = Control::default();
    let ids = [u32::MAX, 0, 7, 9, 17];
    let mut raw = with_encoded(&read, &c, |context| {
        let mut work =
            CompileCheckpoints::try_new(context.bindings.original_control(), CompilePhase::Encode)
                .unwrap();
        let (scans, expressions, _) = encode_read_scans_observed(
            &[ReadScanSource {
                node: NodeId::new(0),
                read: &read,
                schema_id: u32::MAX,
                source_schema_id: 17,
                expression_ids: &ids,
            }],
            context,
            SOURCE,
            limits(),
            &mut |_| Ok(()),
            &mut work,
        )
        .unwrap();
        let source_schemas =
            encode_public_source_schemas_pure_in(context.source_schemas, &mut work).unwrap();
        work.finish().unwrap();
        Raw {
            scans,
            expressions,
            providers: context.bindings.as_wire().to_vec(),
            payloads: context.payloads.as_wire().to_vec(),
            schemas: context.schemas.as_wire().to_vec(),
            types: context.schemas.types().as_wire().clone(),
            source_schemas,
        }
    });
    assert_eq!(
        raw.expressions.iter().map(|e| e.id).collect::<Vec<_>>(),
        [7, 0, 9, 17, u32::MAX]
    );
    let Some(w::connector_expression_definition::Kind::FieldDereference(f)) =
        &raw.expressions[1].kind
    else {
        panic!("field")
    };
    assert_eq!(
        (f.target_connector_expression_id, f.field_index),
        (Some(7), 0)
    );
    let Some(w::connector_expression_definition::Kind::Constant(v)) = &raw.expressions[2].kind
    else {
        panic!("binary")
    };
    assert_eq!(
        v.value.as_ref().unwrap().value,
        Some(cv::value::Value::Varbinary(vec![0, 255, 7]))
    );
    assert_eq!(
        decode_run(&raw, limits(), None).0.unwrap().0[0]
            .1
            .scan()
            .remaining_expression(),
        Some(&expression)
    );
    let Some(w::connector_expression_definition::Kind::Constant(v)) = &mut raw.expressions[2].kind
    else {
        unreachable!()
    };
    v.value_type = Some(novarocks_proto_codec::connector_read::encode_value_type(
        c::ConnectorValueType::BigInt,
    ));
    ordinary(decode_run(&raw, limits(), None).0);
}

#[test]
fn mandatory_presence_order_and_original_constructor_laws_fail_closed() {
    let raw = raw(&read(2, true, true));
    let mut cases = vec![];
    let mut x = raw.clone();
    x.scans[0].node_id = None;
    cases.push(x);
    let mut x = raw.clone();
    x.scans[0].recipe = None;
    cases.push(x);
    let mut x = raw.clone();
    x.scans[0].facts.as_mut().unwrap().assignments[0].value_type = None;
    cases.push(x);
    let mut x = raw.clone();
    x.scans[0].facts.as_mut().unwrap().max_batch_rows = 0;
    cases.push(x);
    let mut x = raw.clone();
    x.scans[0]
        .public_facts
        .as_mut()
        .unwrap()
        .source
        .as_mut()
        .unwrap()
        .selection_digest
        .pop();
    cases.push(x);
    let mut x = raw.clone();
    x.scans[0].public_facts.as_mut().unwrap().metadata_kind = None;
    cases.push(x);
    let mut x = raw.clone();
    x.scans[0]
        .public_facts
        .as_mut()
        .unwrap()
        .execution_schema_id = None;
    cases.push(x);
    let mut x = raw.clone();
    x.scans[0].public_facts.as_mut().unwrap().source_schema_id = None;
    cases.push(x);
    let mut x = raw.clone();
    x.scans[0].recipe.as_mut().unwrap().provider_binding_id = Some(7);
    cases.push(x);
    let mut x = raw.clone();
    let Some(w::scan_tuple_domain::Kind::Columns(c)) = &mut x.scans[0]
        .facts
        .as_mut()
        .unwrap()
        .enforced_predicate
        .as_mut()
        .unwrap()
        .kind
    else {
        unreachable!()
    };
    c.entries.push(c.entries[0].clone());
    cases.push(x);
    for case in cases {
        ordinary(decode_run(&case, limits(), None).0);
    }
    let mut wrong = raw.clone();
    wrong.scans[0].facts.as_mut().unwrap().assignments[0].variable = "".into();
    assert!(matches!(
        decode_run(&wrong, limits(), None).0,
        Err(E::Scan(c::StaticConnectorScanError::InvalidVariable))
    ));
    let mut wrong = raw;
    wrong.scans[0]
        .public_facts
        .as_mut()
        .unwrap()
        .source
        .as_mut()
        .unwrap()
        .selection_digest = vec![0; 32];
    assert!(matches!(
        decode_run(&wrong, limits(), None).0,
        Err(E::Provider(_))
    ));
}

#[test]
fn exact_source_and_control_loans_are_not_equal_metadata_authority() {
    let a = read(1, false, true);
    let b = read(1, false, true);
    assert_eq!(a, b);
    let c = Control::default();
    with_encoded(&a, &c, |context| {
        let mut work =
            CompileCheckpoints::try_new(context.bindings.original_control(), CompilePhase::Encode)
                .unwrap();
        ordinary(encode_read_scans_observed(
            &[ReadScanSource {
                node: NodeId::new(0),
                read: &b,
                schema_id: u32::MAX,
                source_schema_id: 17,
                expression_ids: &[0, 1, 2],
            }],
            context,
            SOURCE,
            limits(),
            &mut |_| Ok(()),
            &mut work,
        ));
        work.finish().unwrap();
    });
    let c = Control::default();
    let foreign = Control::default();
    with_encoded(&a, &c, |context| {
        let mut work = CompileCheckpoints::try_new(&foreign, CompilePhase::Encode).unwrap();
        ordinary(encode_read_scans_observed(
            &[ReadScanSource {
                node: NodeId::new(0),
                read: &a,
                schema_id: u32::MAX,
                source_schema_id: 17,
                expression_ids: &[0, 1, 2],
            }],
            context,
            SOURCE,
            limits(),
            &mut |_| Ok(()),
            &mut work,
        ));
        work.finish().unwrap();
    });
}

#[test]
fn residual_dag_occurrences_expand_independently_and_unused_cycles_refuse() {
    let raw = raw(&read(1, false, true));
    let mut shared = raw.clone();
    shared.expressions.retain(|e| e.id != 17);
    let Some(w::connector_expression_definition::Kind::Call(c)) = &mut shared.expressions[1].kind
    else {
        panic!("root")
    };
    c.argument_connector_expression_ids = vec![0, 0];
    let out = decode_run(&shared, limits(), None).0.unwrap().0;
    let Some(c::ConnectorExpression::Call { arguments, .. }) =
        out[0].1.scan().remaining_expression()
    else {
        panic!("call")
    };
    assert_eq!(arguments.len(), 2);
    assert_eq!(arguments[0], arguments[1]);
    let mut cycle = raw.clone();
    let Some(w::connector_expression_definition::Kind::Call(c)) = &mut cycle.expressions[2].kind
    else {
        unreachable!()
    };
    c.argument_connector_expression_ids = vec![u32::MAX];
    ordinary(decode_run(&cycle, limits(), None).0);
    let mut extra = raw.clone();
    let mut unused = extra.expressions[0].clone();
    unused.id = 99;
    extra.expressions.push(unused);
    ordinary(decode_run(&extra, limits(), None).0);
    let mut duplicate = raw.clone();
    duplicate.expressions.push(duplicate.expressions[0].clone());
    ordinary(decode_run(&duplicate, limits(), None).0);
    let mut missing = raw;
    missing.scans[0]
        .facts
        .as_mut()
        .unwrap()
        .remaining_connector_expression_id = Some(13);
    ordinary(decode_run(&missing, limits(), None).0);
}
fn prefixes<T>(
    mut run: impl FnMut(Option<(usize, CompileControlError)>) -> (Result<T, E>, Vec<u32>),
    success: bool,
) {
    let (out, trace) = run(None);
    assert_eq!(out.is_ok(), success);
    assert!(!trace.is_empty());
    for i in 0..trace.len() {
        for cause in CAUSES {
            let (out, actual) = run(Some((i, cause.clone())));
            assert!(matches!(out,Err(E::Control(c))if c==cause));
            assert_eq!(actual, &trace[..=i]);
        }
    }
}
#[test]
fn all_actual_success_and_ordinary_callbacks_keep_the_first_typed_cause() {
    let r = read(1, false, true);
    prefixes(
        |stop| encode_run(&r, &[u32::MAX, 0, 17], limits(), stop),
        true,
    );
    prefixes(|stop| encode_run(&r, &[0], limits(), stop), false);
    let raw = raw(&r);
    prefixes(|stop| decode_run(&raw, limits(), stop), true);
    let mut wrong = raw;
    wrong.scans[0].facts.as_mut().unwrap().max_batch_rows = 0;
    prefixes(|stop| decode_run(&wrong, limits(), stop), false);
}
#[test]
fn actual_seven_axis_invoices_refuse_underbounds_without_publication() {
    let read = read(1, false, true);
    let golden = encode_run(&read, &[u32::MAX, 0, 17], limits(), None)
        .0
        .unwrap()
        .2;
    // Independent lower invoices: one outer scan Vec, one outer expression
    // Vec and the source IDs and both sorting indices have distinct backing.
    let lower = Layout::array::<w::FrozenReadScan>(1).unwrap().size()
        + Layout::array::<w::ConnectorExpressionDefinition>(3)
            .unwrap()
            .size()
        + Layout::array::<u32>(3).unwrap().size()
        + Layout::array::<usize>(4).unwrap().size();
    assert!(golden.allocation_request_bytes_upper_bound >= lower);
    assert!(golden.allocation_requests_upper_bound >= 5);
    assert!(golden.scalar_bytes >= 3 + 32 + 2 + 64 + 2 + 64 + 2 + 6);
    for axis in 0..7 {
        let mut l = limits();
        match axis {
            0 => l.max_scans = golden.scan_count - 1,
            1 => l.max_items = golden.item_count - 1,
            2 => l.max_scalar_bytes = golden.scalar_bytes - 1,
            3 => l.max_allocation_requests = golden.allocation_requests_upper_bound - 1,
            4 => l.max_allocation_request_bytes = golden.allocation_request_bytes_upper_bound - 1,
            5 => {
                l.max_coexisting_source_and_request_bytes =
                    golden.coexisting_source_and_request_bytes_upper_bound - 1
            }
            6 => l.max_work = golden.cumulative_work_upper_bound - 1,
            _ => unreachable!(),
        };
        assert!(matches!(
            encode_run(&read, &[u32::MAX, 0, 17], l, None).0,
            Err(E::Control(CompileControlError::ResourceExhausted))
        ));
    }
    let original_raw = raw(&read);
    let receiver = decode_run(&original_raw, limits(), None).0.unwrap().1;
    for axis in 0..7 {
        let mut l = limits();
        match axis {
            0 => l.max_scans = receiver.scan_count - 1,
            1 => l.max_items = receiver.item_count - 1,
            2 => l.max_scalar_bytes = receiver.scalar_bytes - 1,
            3 => l.max_allocation_requests = receiver.allocation_requests_upper_bound - 1,
            4 => l.max_allocation_request_bytes = receiver.allocation_request_bytes_upper_bound - 1,
            5 => {
                l.max_coexisting_source_and_request_bytes =
                    receiver.coexisting_source_and_request_bytes_upper_bound - 1
            }
            6 => l.max_work = receiver.cumulative_work_upper_bound - 1,
            _ => unreachable!(),
        }
        assert!(matches!(
            decode_run(&original_raw, l, None).0,
            Err(E::Control(CompileControlError::ResourceExhausted))
        ));
    }
    // A raw scalar header has a known request/byte invoice before any ID or
    // namespace observer. Its ordinary absent node ID cannot hide Resource.
    let mut malformed = original_raw.clone();
    malformed.scans[0].node_id = None;
    let mut low = limits();
    low.max_scalar_bytes = 0;
    for cause in CAUSES {
        let (out, trace) = decode_run(&malformed, low, Some((1, cause)));
        assert!(matches!(
            out,
            Err(E::Control(CompileControlError::ResourceExhausted))
        ));
        assert_eq!(trace, [0]);
    }
    for pending in [0, 254, 255] {
        for cause in CAUSES {
            let c = Control::default();
            with_encoded(&read, &c, |context| {
                c.arm(None);
                let mut work = CompileCheckpoints::try_new(
                    context.bindings.original_control(),
                    CompilePhase::Encode,
                )
                .unwrap();
                for _ in 0..pending {
                    work.step().unwrap();
                }
                let before = c.trace();
                *c.stop.lock().unwrap() = Some((before.len(), cause));
                let mut low = limits();
                low.max_scans = 0;
                assert!(matches!(
                    encode_read_scans_observed(
                        &[ReadScanSource {
                            node: NodeId::new(0),
                            read: &read,
                            schema_id: u32::MAX,
                            source_schema_id: 17,
                            expression_ids: &[0, 1, 2]
                        }],
                        context,
                        SOURCE,
                        low,
                        &mut |_| Ok(()),
                        &mut work
                    ),
                    Err(E::Control(CompileControlError::ResourceExhausted))
                ));
                assert_eq!(c.trace(), before);
            });
        }
    }
}
#[test]
fn wide_real_assignments_and_columns_preserve_order_and_actual_sampled_controls() {
    let read = read(320, false, false);
    let encoded = encode_run(&read, &[], limits(), None);
    let (scans, expressions, facts) = encoded.0.unwrap();
    assert!(expressions.is_empty());
    assert_eq!(
        scans[0].recipe.as_ref().unwrap().column_payload_ids,
        (1..=320).collect::<Vec<_>>()
    );
    assert_eq!(
        scans[0].facts.as_ref().unwrap().assignments[319].variable,
        "v319"
    );
    assert!(facts.item_count >= 640);
    assert!(encoded.1.iter().map(|u| *u as usize).sum::<usize>() >= 320);
    let raw = raw(&read);
    let (out, trace) = decode_run(&raw, limits(), None);
    assert_eq!(out.unwrap().0[0].1, read);
    let quantum = trace
        .iter()
        .position(|units| *units == 256)
        .expect("wide actual read construction observes a full work quantum");
    for at in [0, quantum, trace.len() / 2, trace.len() - 1] {
        for cause in CAUSES {
            let (out, actual) = decode_run(&raw, limits(), Some((at, cause.clone())));
            assert!(matches!(out,Err(E::Control(c))if c==cause));
            assert_eq!(actual, &trace[..=at]);
        }
    }
}

#[test]
fn read_public_schema_presence_has_no_pure_legacy_route_or_logical_label_parser() {
    use prost::Message;
    let raw = raw(&read(2, true, true));
    let mut absent = raw.clone();
    let public = absent.scans[0].public_facts.as_mut().unwrap();
    public.execution_schema_id = None;
    public.source_schema_id = None;
    assert!(matches!(
        decode_run(&absent, limits(), None).0,
        Err(E::InvalidShape("read execution schema ID absent"))
    ));
    // Independent retired tags: uint32 schema_id(3), packed root labels(4).
    // Prost discards unknown/reserved fields. They cannot supply either real
    // modern reference, even when every other original source fact is present.
    for retired in [
        &[0x18, 0xff, 0xff, 0xff, 0xff, 0x0f][..],
        &[0x22, 0x05, 0xff, 0xff, 0xff, 0xff, 0x07][..],
    ] {
        let mut bytes = absent.scans[0]
            .public_facts
            .as_ref()
            .unwrap()
            .encode_to_vec();
        bytes.extend_from_slice(retired);
        let decoded = w::ConnectorReadPublicFacts::decode(bytes.as_slice()).unwrap();
        assert!(decoded.execution_schema_id.is_none());
        assert!(decoded.source_schema_id.is_none());
        let mut legacy = absent.clone();
        legacy.scans[0].public_facts = Some(decoded);
        assert!(matches!(
            decode_run(&legacy, limits(), None).0,
            Err(E::InvalidShape("read execution schema ID absent"))
        ));
    }
    let mut only_execution = raw.clone();
    only_execution.scans[0]
        .public_facts
        .as_mut()
        .unwrap()
        .source_schema_id = None;
    assert!(matches!(
        decode_run(&only_execution, limits(), None).0,
        Err(E::InvalidShape("read source schema facts ID absent"))
    ));
    let mut only_source = raw;
    only_source.scans[0]
        .public_facts
        .as_mut()
        .unwrap()
        .execution_schema_id = None;
    assert!(matches!(
        decode_run(&only_source, limits(), None).0,
        Err(E::InvalidShape("read execution schema ID absent"))
    ));
}

#[test]
fn read_public_execution_and_source_namespaces_accept_zero_ids_without_changing_facts() {
    for metadata in [false, true] {
        let original = read(2, metadata, true);
        let mut raw = raw(&original);
        raw.schemas
            .iter_mut()
            .find(|schema| schema.id == u32::MAX)
            .unwrap()
            .id = 0;
        raw.source_schemas
            .iter_mut()
            .find(|source| source.id == Some(17))
            .unwrap()
            .id = Some(0);
        let public = raw.scans[0].public_facts.as_mut().unwrap();
        public.execution_schema_id = Some(0);
        public.source_schema_id = Some(0);
        let (decoded, _) = decode_run(&raw, limits(), None);
        let (out, facts) = decoded.unwrap();
        assert_eq!(out[0], (NodeId::new(u32::MAX), original));
        assert_eq!(facts.scan_count, 1);
        let public = out[0].1.public_facts();
        assert!(
            public
                .execution_schema()
                .unwrap()
                .schema()
                .metadata()
                .as_arc()
                .is_none()
        );
        assert_eq!(
            public.source_schema_facts().unwrap().field_facts()[1].attributes()["ordinal"],
            "1"
        );
        assert_eq!(
            public.source_schema_facts().unwrap().schema_attributes()["source"].len(),
            if metadata { 12288 } else { 3 }
        );
    }
}
