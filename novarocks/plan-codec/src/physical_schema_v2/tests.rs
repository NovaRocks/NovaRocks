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
use crate::{
    physical_properties_v2::PhysicalPropertyProjectionLimits,
    physical_type_v2::{TypeProjectionLimits, decode_type_table, encode_type_table_sources},
};
use arrow::datatypes::DataType;
use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
use std::{collections::HashMap, sync::Mutex};
const SOURCE: usize = 1 << 20;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    stop: Mutex<Option<(usize, CompileControlError)>>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        let stop = self.stop.lock().unwrap().clone();
        if let Some((stop, _)) = stop {
            assert!(at <= stop, "callback after primary refusal");
        }
        trace.push((phase, units));
        match stop {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}
impl Control {
    fn arm(&self, stop: Option<(usize, CompileControlError)>) {
        self.trace.lock().unwrap().clear();
        *self.stop.lock().unwrap() = stop;
    }
    fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.trace.lock().unwrap().clone()
    }
}
fn limits() -> NodeProjectionLimits {
    NodeProjectionLimits {
        max_input_nodes: 0,
        max_value_references: 8192,
        max_list_items: 8192,
        max_allocation_requests: 8192,
        max_allocation_request_bytes: 8 << 20,
        max_coexisting_source_and_request_bytes: 16 << 20,
        max_work: 64 << 30,
        properties: PhysicalPropertyProjectionLimits {
            max_value_references: 0,
            max_allocation_requests: 0,
            max_allocation_request_bytes: 0,
            max_coexisting_source_and_request_bytes: 16 << 20,
            max_work: 64 << 30,
        },
    }
}
fn type_limits() -> TypeProjectionLimits {
    TypeProjectionLimits {
        max_definitions: 128,
        max_expanded_nodes: 8192,
        max_string_bytes: 1 << 20,
    }
}
fn field() -> Arc<Field> {
    Arc::new(Field::new("original", DataType::Int64, true))
}
fn original_source_field() -> Arc<Field> {
    Arc::new(Field::new("original", DataType::Int64, true).with_metadata(
        std::collections::HashMap::<String, String>::from([(
            "field.tag".into(),
            "unmodified".into(),
        )]),
    ))
}
fn metadata() -> HashMap<String, String> {
    [
        ("".into(), "".into()),
        ("a\0".into(), "雪\0".into()),
        ("雪".into(), "value".into()),
    ]
    .into()
}
fn run_encode<'loan, 'source, 'control>(
    sources: &'loan [SchemaSource<'source>],
    types: &'loan EncodedTypeTable<'source>,
    control: &'control Control,
) -> Result<EncodedSchemas<'loan, 'source, 'control>, Error> {
    prepare_schemas_encode(sources, types, SOURCE, limits(), control)?.emit()
}
fn run_decode<'loan, 'control>(
    wire: &'loan [wire::SchemaDefinition],
    types: &'loan DecodedTypeTable,
    control: &'control Control,
) -> Result<DecodedSchemas<'loan, 'control>, Error> {
    prepare_schemas_decode(wire, types, SOURCE, limits(), control)?.emit()
}
fn empty_types(control: &Control) -> DecodedTypeTable {
    let wire = encode_type_table_sources(&[], &[], type_limits(), control).unwrap();
    decode_type_table(wire.as_wire(), type_limits(), control).unwrap()
}
fn ordinary(result: Result<(), Error>) {
    assert!(result.is_err());
    assert!(!matches!(result, Err(Error::Control(_))));
}

#[test]
fn schemas_preserve_zero_max_ordered_repeated_fields_and_empty_root() {
    let control = Control::default();
    let original = field();
    let roots = [(0, original.clone()), (u32::MAX, original.clone())];
    let types = encode_type_table_sources(&[], &roots, type_limits(), &control).unwrap();
    let read = decode_type_table(types.as_wire(), type_limits(), &control).unwrap();
    let schema = Schema::new(vec![original.clone(), original.clone(), original]);
    let empty = Schema::empty();
    let sources = [
        SchemaSource {
            id: u32::MAX,
            source: &schema,
            field_ids: &[u32::MAX, 0, u32::MAX],
        },
        SchemaSource {
            id: 0,
            source: &empty,
            field_ids: &[],
        },
    ];
    let encoded = run_encode(&sources, &types, &control).unwrap();
    assert_eq!(
        encoded.as_wire(),
        [
            wire::SchemaDefinition {
                id: u32::MAX,
                field_ids: vec![u32::MAX, 0, u32::MAX],
            },
            wire::SchemaDefinition {
                id: 0,
                field_ids: vec![],
            }
        ]
    );
    let decoded = run_decode(encoded.as_wire(), &read, &control).unwrap();
    assert_eq!(decoded.definitions()[0].0, u32::MAX);
    assert_eq!(decoded.definitions()[1].0, 0);
    assert!(decoded.definitions()[1].1.schema().fields().is_empty());
    let output = decoded.definitions()[0].1.schema().fields();
    assert!(Arc::ptr_eq(&output[0], read.field(u32::MAX).unwrap()));
    assert!(Arc::ptr_eq(&output[1], read.field(0).unwrap()));
    assert!(Arc::ptr_eq(&output[2], read.field(u32::MAX).unwrap()));
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    assert!(decoded.schema_observed(7, &mut work).unwrap().is_none());
    assert!(std::ptr::eq(
        decoded.schema_observed(0, &mut work).unwrap().unwrap(),
        decoded.definitions()[1].1.schema()
    ));
    work.finish().unwrap();
    let foreign = Control::default();
    let mut work = CompileCheckpoints::try_new(&foreign, CompilePhase::Decode).unwrap();
    ordinary(decoded.schema_observed(0, &mut work).map(|_| ()));
}
#[test]
#[allow(deprecated)]
fn schema_metadata_hand_oracle_is_independent_of_complete_field_metadata() {
    use crate::physical_public_source_schema_v2::{
        SourceSchemaFactsSource, decode_public_source_schemas_pure_in,
        encode_public_source_schemas_pure_in,
    };
    use crate::physical_type_v2::{
        decode_source_field_table_input_derived_with_creator, encode_source_field_table_pure_in,
    };
    use novarocks_connector_contract::{ConnectorSourceSchemaFacts, ConnectorSourceSchemaShape};
    use novarocks_type_contract::{ValueSchemaCreation, ValueTypeStorageLifetime};
    let control = Control::default();
    let declared = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        false,
        ValueLogicalType::LargeInt,
    )
    .unwrap();
    let nested = Arc::new(declared.try_to_field("large").unwrap());
    let dictionary = Arc::new(
        Field::new_dict(
            "dict",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            true,
            77,
            true,
        )
        .with_metadata(std::collections::HashMap::<String, String>::from([(
            "unknown.field".into(),
            "雪".into(),
        )])),
    );
    let structure = Arc::new(Field::new(
        "nested",
        DataType::Struct(vec![nested].into()),
        false,
    ));
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    let original = original_source_field();
    let source_fields = encode_source_field_table_pure_in(
        &[(0, &dictionary), (1, &original), (u32::MAX, &structure)],
        type_limits(),
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    let mut request = |_: Layout| Ok::<(), ()>(());
    let mut opaque = |_: usize| Ok::<(), ()>(());
    let mut creation =
        ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut request, &mut opaque);
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let decoded_fields = decode_source_field_table_input_derived_with_creator(
        &source_fields,
        SOURCE,
        &mut creation,
        &mut work,
    )
    .unwrap();
    let actual = decoded_fields
        .field_observed(0, &mut work)
        .unwrap()
        .unwrap();
    assert_eq!(actual.dict_id(), Some(77));
    assert_eq!(actual.dict_is_ordered(), Some(true));
    assert_eq!(actual.metadata()["unknown.field"], "雪");
    assert_eq!(
        decoded_fields
            .field_observed(1, &mut work)
            .unwrap()
            .unwrap()
            .metadata()["field.tag"],
        "unmodified"
    );
    let nested_actual = decoded_fields
        .field_observed(u32::MAX, &mut work)
        .unwrap()
        .unwrap();
    assert!(!nested_actual.is_nullable());
    let DataType::Struct(children) = nested_actual.data_type() else {
        panic!()
    };
    assert_eq!(children[0].data_type(), &DataType::FixedSizeBinary(16));
    // Execution identity is the explicit author beside source attributes.
    assert_eq!(declared.logical_type(), ValueLogicalType::LargeInt);
    work.finish().unwrap();
    let roots = [];
    let types = encode_type_table_sources(&[], &roots, type_limits(), &control).unwrap();
    let read = decode_type_table(types.as_wire(), type_limits(), &control).unwrap();
    let shape = Schema::empty();
    let sources = [SchemaSource {
        id: 7,
        source: &shape,
        field_ids: &[],
    }];
    let encoded = run_encode(&sources, &types, &control).unwrap();
    let schemas = run_decode(encoded.as_wire(), &read, &control).unwrap();
    let facts = ConnectorSourceSchemaFacts::try_new(
        ConnectorSourceSchemaShape::try_new(
            Arc::new(Schema::empty()),
            ValueTypeStorageLifetime::pure(),
        )
        .unwrap(),
        metadata(),
        Box::new([]),
        ValueTypeStorageLifetime::pure(),
    )
    .unwrap();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    let wire = encode_public_source_schemas_pure_in(
        &[SourceSchemaFactsSource {
            id: 0,
            source_shape_schema_id: 7,
            facts: &facts,
        }],
        &mut work,
    )
    .unwrap();
    assert_eq!(
        wire[0].schema_attributes,
        vec![
            novarocks_proto_models::arrow_type::ArrowFieldMetadataEntry {
                key: "".into(),
                value: "".into()
            },
            novarocks_proto_models::arrow_type::ArrowFieldMetadataEntry {
                key: "a\0".into(),
                value: "雪\0".into()
            },
            novarocks_proto_models::arrow_type::ArrowFieldMetadataEntry {
                key: "雪".into(),
                value: "value".into()
            },
        ]
    );
    work.finish().unwrap();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let decoded =
        decode_public_source_schemas_pure_in(&wire, &schemas, SOURCE, limits(), &mut work).unwrap();
    assert_eq!(
        decoded
            .lookup_observed(0, &mut work)
            .unwrap()
            .unwrap()
            .schema_attributes(),
        &metadata()
    );
    assert!(
        schemas.definitions()[0]
            .1
            .schema()
            .metadata()
            .as_arc()
            .is_none()
    );
    work.finish().unwrap();
    // The original large Unicode annotation remains a source fact. It is
    // carried through the actual independent codec, never an execution shape.
    let text = "雪".repeat(320);
    let wide = ConnectorSourceSchemaFacts::try_new(
        ConnectorSourceSchemaShape::try_new(
            Arc::new(Schema::empty()),
            ValueTypeStorageLifetime::pure(),
        )
        .unwrap(),
        HashMap::from([("key".into(), text.clone())]),
        Box::new([]),
        ValueTypeStorageLifetime::pure(),
    )
    .unwrap();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    let wire = encode_public_source_schemas_pure_in(
        &[SourceSchemaFactsSource {
            id: u32::MAX,
            source_shape_schema_id: 7,
            facts: &wide,
        }],
        &mut work,
    )
    .unwrap();
    assert_eq!(wire[0].schema_attributes[0].value, text);
    work.finish().unwrap();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let decoded =
        decode_public_source_schemas_pure_in(&wire, &schemas, SOURCE, limits(), &mut work).unwrap();
    assert_eq!(
        decoded
            .lookup_observed(u32::MAX, &mut work)
            .unwrap()
            .unwrap()
            .schema_attributes()["key"],
        text
    );
    work.finish().unwrap();
}
#[test]
fn schema_field_namespace_requires_original_arc_and_real_connector_schema() {
    use novarocks_connector_contract::{
        ConnectorReadArtifactCoverage, ConnectorReadDistribution, ConnectorReadInputVersion,
        ConnectorReadProperties, ConnectorReadPublicFacts, ConnectorReadStaticFacts, ScanColumnId,
    };
    let control = Control::default();
    let source = ConnectorReadStaticFacts::try_new(
        ConnectorReadInputVersion::try_new(vec![1]).unwrap(),
        [1; 32],
        ConnectorReadProperties::<ScanColumnId>::try_new(
            ConnectorReadDistribution::Unconstrained,
            Vec::new(),
        )
        .unwrap(),
        ConnectorReadArtifactCoverage::NoArtifactInputs,
        Vec::<u8>::new(),
    )
    .unwrap();
    let facts = ConnectorReadPublicFacts::try_new(
        source,
        None,
        Schema::new(vec![field()]),
        vec![ValueLogicalType::Physical],
    )
    .unwrap();
    let roots = [(u32::MAX, facts.schema().fields()[0].clone())];
    let types = encode_type_table_sources(&[], &roots, type_limits(), &control).unwrap();
    let read = decode_type_table(types.as_wire(), type_limits(), &control).unwrap();
    let sources = [SchemaSource {
        id: 0,
        source: facts.schema(),
        field_ids: &[u32::MAX],
    }];
    let encoded = run_encode(&sources, &types, &control).unwrap();
    assert_eq!(encoded.as_wire()[0].field_ids, [u32::MAX]);
    let wrong_count = [SchemaSource {
        id: 0,
        source: facts.schema(),
        field_ids: &[],
    }];
    ordinary(run_encode(&wrong_count, &types, &control).map(|_| ()));
    let foreign = Schema::new(vec![Arc::new(roots[0].1.as_ref().clone())]);
    let sources = [SchemaSource {
        id: 0,
        source: &foreign,
        field_ids: &[u32::MAX],
    }];
    ordinary(run_encode(&sources, &types, &control).map(|_| ()));
    let bad = [wire::SchemaDefinition {
        id: 0,
        field_ids: vec![7],
    }];
    ordinary(run_decode(&bad, &read, &control).map(|_| ()));
}
#[test]
fn schema_metadata_duplicate_unsorted_and_duplicate_ids_never_normalize() {
    use crate::physical_public_source_schema_v2::decode_public_source_schemas_pure_in;
    let control = Control::default();
    let types = empty_types(&control);
    let defs = [wire::SchemaDefinition {
        id: 0,
        field_ids: vec![],
    }];
    let schemas = run_decode(&defs, &types, &control).unwrap();
    for keys in [["a", "a"], ["b", "a"]] {
        let source = [wire::PublicSourceSchemaFactsDefinition {
            id: Some(0),
            source_shape_schema_id: Some(0),
            schema_attributes: keys
                .into_iter()
                .map(
                    |key| novarocks_proto_models::arrow_type::ArrowFieldMetadataEntry {
                        key: key.into(),
                        value: "".into(),
                    },
                )
                .collect(),
            field_facts: vec![],
        }];
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        assert!(
            decode_public_source_schemas_pure_in(&source, &schemas, SOURCE, limits(), &mut work)
                .is_err()
        );
    }
    let duplicates = [
        wire::SchemaDefinition {
            id: u32::MAX,
            field_ids: vec![],
        },
        wire::SchemaDefinition {
            id: u32::MAX,
            field_ids: vec![],
        },
    ];
    assert!(matches!(
        run_decode(&duplicates, &types, &control),
        Err(Error::Binding(BindingCodecError::InvalidShape(_)))
    ));
    let empty = Schema::empty();
    let types = encode_type_table_sources(&[], &[], type_limits(), &control).unwrap();
    let sources = [
        SchemaSource {
            id: 0,
            source: &empty,
            field_ids: &[],
        },
        SchemaSource {
            id: 0,
            source: &empty,
            field_ids: &[],
        },
    ];
    ordinary(run_encode(&sources, &types, &control).map(|_| ()));
}
fn tighten(f: NodeProjectionFacts) -> NodeProjectionLimits {
    let mut l = limits();
    l.max_value_references = f.value_reference_count;
    l.max_list_items = f.list_item_count;
    l.max_allocation_requests = f.allocation_requests_upper_bound;
    l.max_allocation_request_bytes = f.allocation_request_bytes_upper_bound;
    l.max_coexisting_source_and_request_bytes = f.coexisting_source_and_request_bytes_upper_bound;
    l.max_work = f.cumulative_work_upper_bound;
    l
}
#[test]
fn schema_request_layout_golden_includes_empty_fields_arc_and_persistent_index() {
    let control = Control::default();
    let original = field();
    let roots = [(0, original.clone())];
    let types = encode_type_table_sources(&[], &roots, type_limits(), &control).unwrap();
    let read = decode_type_table(types.as_wire(), type_limits(), &control).unwrap();
    let schema = Schema::new(vec![original.clone(), original]);
    let sources = [SchemaSource {
        id: 0,
        source: &schema,
        field_ids: &[0, 0],
    }];
    let encoded = run_encode(&sources, &types, &control).unwrap();
    let decoded = run_decode(encoded.as_wire(), &read, &control).unwrap();
    let sent = encoded.facts();
    let send_bytes = Layout::array::<usize>(1).unwrap().size()
        + Layout::array::<wire::SchemaDefinition>(1).unwrap().size()
        + Layout::array::<u32>(2).unwrap().size();
    assert_eq!(sent.allocation_requests_upper_bound, 3);
    assert_eq!(sent.allocation_request_bytes_upper_bound, send_bytes);
    let arc = Layout::new::<[usize; 2]>()
        .extend(Layout::array::<Arc<Field>>(2).unwrap())
        .unwrap()
        .0
        .pad_to_align()
        .size();
    let receive_bytes = Layout::array::<usize>(1).unwrap().size()
        + 2 * Layout::array::<(u32, MaterializedSchema)>(1)
            .unwrap()
            .size()
        + Layout::array::<Arc<Field>>(2).unwrap().size()
        + arc;
    assert_eq!(decoded.facts().allocation_requests_upper_bound, 5);
    assert_eq!(
        decoded.facts().allocation_request_bytes_upper_bound,
        receive_bytes
    );
    let empty = [wire::SchemaDefinition {
        id: 0,
        field_ids: vec![],
    }];
    let empty_decoded = run_decode(&empty, &read, &control).unwrap();
    assert_eq!(empty_decoded.facts().allocation_requests_upper_bound, 4);
    assert_eq!(
        empty_decoded.facts().allocation_request_bytes_upper_bound,
        Layout::array::<usize>(1).unwrap().size()
            + 2 * Layout::array::<(u32, MaterializedSchema)>(1)
                .unwrap()
                .size()
            + Layout::new::<[usize; 2]>().size()
    );
    for receive in [false, true] {
        let facts = if receive { *decoded.facts() } else { *sent };
        let exact = tighten(facts);
        if receive {
            prepare_schemas_decode(encoded.as_wire(), &read, SOURCE, exact, &control)
                .unwrap()
                .emit()
                .unwrap();
        } else {
            prepare_schemas_encode(&sources, &types, SOURCE, exact, &control)
                .unwrap()
                .emit()
                .unwrap();
        }
        for axis in 0..6 {
            let mut l = exact;
            let n = match axis {
                0 => &mut l.max_value_references,
                1 => &mut l.max_list_items,
                2 => &mut l.max_allocation_requests,
                3 => &mut l.max_allocation_request_bytes,
                4 => &mut l.max_coexisting_source_and_request_bytes,
                5 => &mut l.max_work,
                _ => unreachable!(),
            };
            assert!(*n > 0);
            *n -= 1;
            let result = if receive {
                prepare_schemas_decode(encoded.as_wire(), &read, SOURCE, l, &control).map(|_| ())
            } else {
                prepare_schemas_encode(&sources, &types, SOURCE, l, &control).map(|_| ())
            };
            assert!(matches!(
                result,
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
        }
    }
}
#[test]
fn shared_schema_source_union_floor_and_consumed_empty_staging_remain_truthful() {
    let control = Control::default();
    let original = field();
    let roots = [(0, original.clone())];
    let types = encode_type_table_sources(&[], &roots, type_limits(), &control).unwrap();
    let mut deleted = HashMap::new();
    deleted.reserve(4096);
    for id in 0..4096 {
        deleted.insert(id.to_string(), String::new());
    }
    deleted.clear();
    assert!(deleted.capacity() >= 4096);
    let schema = Schema::new_with_metadata(vec![original.clone()], deleted);
    assert!(schema.metadata().as_arc().is_none());
    let ids = [0];
    let sources = [
        SchemaSource {
            id: 0,
            source: &schema,
            field_ids: &ids,
        },
        SchemaSource {
            id: u32::MAX,
            source: &schema,
            field_ids: &ids,
        },
    ];
    let converted = run_encode(&sources, &types, &control).unwrap();
    let empty = Schema::new(vec![original]);
    let clean_sources = [
        SchemaSource {
            id: 0,
            source: &empty,
            field_ids: &ids,
        },
        SchemaSource {
            id: u32::MAX,
            source: &empty,
            field_ids: &ids,
        },
    ];
    let canonical = run_encode(&clean_sources, &types, &control).unwrap();
    assert_eq!(converted.as_wire(), canonical.as_wire());
    assert_eq!(converted.facts(), canonical.facts());
    assert!(schema.metadata().as_arc().is_none());
    let mut input = vec![wire::SchemaDefinition {
        id: 0,
        field_ids: vec![0],
    }];
    input[0].field_ids.reserve(64);
    let read = decode_type_table(types.as_wire(), type_limits(), &control).unwrap();
    let known = bytes::<wire::SchemaDefinition>(1).unwrap()
        + read.necessary_fields_retained_floor().unwrap()
        + bytes::<u32>(input[0].field_ids.capacity()).unwrap();
    ordinary(prepare_schemas_decode(&input, &read, known - 1, limits(), &control).map(|_| ()));
    // Eight exact aliases share one 320-field Fields allocation and one IDs
    // slice. The caller invoice covers their actual union, including the
    // original Field, emitted type DTO and source-root handles. It is larger
    // than the distinct allocations, but smaller than eight invented Fields
    // allocations. A necessary lower floor is not the complete invoice.
    let shared = Schema::new(vec![field(); 320]);
    let shared_ids = vec![0; 320];
    let shared_roots = [(0, shared.fields()[0].clone())];
    let shared_types =
        encode_type_table_sources(&[], &shared_roots, type_limits(), &control).unwrap();
    let aliases = (0..8)
        .map(|id| SchemaSource {
            id,
            source: &shared,
            field_ids: &shared_ids,
        })
        .collect::<Vec<_>>();
    let shared_union_invoice = 16 << 10;
    let one_schema_floor =
        size_of::<Schema>() + fields_layout(320).unwrap().size() + bytes::<u32>(320).unwrap();
    assert!(one_schema_floor * aliases.len() > shared_union_invoice);
    prepare_schemas_encode(
        &aliases,
        &shared_types,
        shared_union_invoice,
        limits(),
        &control,
    )
    .unwrap()
    .emit()
    .unwrap();
}
fn prefixes(control: &Control, mut invoke: impl FnMut() -> Result<(), Error>, success: bool) {
    control.arm(None);
    assert_eq!(invoke().is_ok(), success);
    let trace = control.trace();
    assert!(!trace.is_empty());
    for at in 0..trace.len() {
        for cause in CAUSES {
            control.arm(Some((at, cause.clone())));
            assert!(matches!(invoke(),Err(Error::Control(actual)) if actual==cause));
            assert_eq!(control.trace(), trace[..=at]);
        }
    }
    control.arm(None);
}
#[test]
fn every_small_schema_callback_preserves_three_primary_causes_and_ordinary_tail() {
    let control = Control::default();
    let original = field();
    let roots = [(0, original.clone())];
    let types = encode_type_table_sources(&[], &roots, type_limits(), &control).unwrap();
    let read = decode_type_table(types.as_wire(), type_limits(), &control).unwrap();
    let schema = Schema::new(vec![original]);
    let sources = [SchemaSource {
        id: 0,
        source: &schema,
        field_ids: &[0],
    }];
    let encoded = run_encode(&sources, &types, &control).unwrap();
    prefixes(
        &control,
        || run_encode(&sources, &types, &control).map(|_| ()),
        true,
    );
    prefixes(
        &control,
        || run_decode(encoded.as_wire(), &read, &control).map(|_| ()),
        true,
    );
    let bad = [wire::SchemaDefinition {
        id: 0,
        field_ids: vec![7],
    }];
    control.arm(None);
    ordinary(run_decode(&bad, &read, &control).map(|_| ()));
    assert!(control.trace().last().unwrap().1 > 0);
    prefixes(
        &control,
        || run_decode(&bad, &read, &control).map(|_| ()),
        false,
    );
    let mut l = limits();
    l.max_allocation_requests = 0;
    control.arm(Some((1, CompileControlError::DeadlineExceeded)));
    assert!(matches!(
        prepare_schemas_decode(encoded.as_wire(), &read, SOURCE, l, &control),
        Err(Error::Control(CompileControlError::ResourceExhausted))
    ));
    assert_eq!(control.trace(), [(CompilePhase::Decode, 0)]);
}
#[test]
fn known_shape_field_prefix_resource_refusal_precedes_late_control() {
    let control = Control::default();
    let original = field();
    let roots = [(0, original.clone())];
    let types = encode_type_table_sources(&[], &roots, type_limits(), &control).unwrap();
    let read = decode_type_table(types.as_wire(), type_limits(), &control).unwrap();
    let shape = Schema::new(vec![original]);
    let sources = [SchemaSource {
        id: 0,
        source: &shape,
        field_ids: &[0],
    }];
    let encoded = run_encode(&sources, &types, &control).unwrap();
    let decoded = run_decode(encoded.as_wire(), &read, &control).unwrap();
    for receive in [false, true] {
        let facts = if receive {
            *decoded.facts()
        } else {
            *encoded.facts()
        };
        let mut bound = limits();
        bound.max_allocation_request_bytes = facts.allocation_request_bytes_upper_bound - 1;
        control.arm(None);
        let run = |control: &Control| {
            if receive {
                prepare_schemas_decode(encoded.as_wire(), &read, SOURCE, bound, control).map(|_| ())
            } else {
                prepare_schemas_encode(&sources, &types, SOURCE, bound, control).map(|_| ())
            }
        };
        assert!(matches!(
            run(&control),
            Err(Error::Control(CompileControlError::ResourceExhausted))
        ));
        let prefix = control.trace();
        for cause in CAUSES {
            control.arm(Some((prefix.len(), cause)));
            assert!(matches!(
                run(&control),
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
            assert_eq!(control.trace(), prefix);
        }
    }
    control.arm(None);
}
#[test]
fn wide_schema_copy_has_actual_quantum_without_claiming_hashmap_internal_sampling() {
    let control = Control::default();
    let original = field();
    let roots = [(u32::MAX, original.clone())];
    let types = encode_type_table_sources(&[], &roots, type_limits(), &control).unwrap();
    let read = decode_type_table(types.as_wire(), type_limits(), &control).unwrap();
    let schema = Schema::new(vec![original; 320]);
    let ids = vec![u32::MAX; 320];
    let sources = [SchemaSource {
        id: u32::MAX,
        source: &schema,
        field_ids: &ids,
    }];
    control.arm(None);
    let encoded = run_encode(&sources, &types, &control).unwrap();
    let sent = control.trace();
    assert!(sent.iter().any(|(_, units)| *units == 256));
    assert_eq!(encoded.as_wire()[0].field_ids, ids);

    control.arm(None);
    let decoded = run_decode(encoded.as_wire(), &read, &control).unwrap();
    let received = control.trace();
    // Decode borrows each immutable Field through an opaque lookup and clones
    // its Arc at an observed boundary. The retired metadata byte-copy was the
    // receive operation that accumulated a 256-unit quantum; do not simulate it.
    assert!(
        received
            .iter()
            .map(|(_, units)| u64::from(*units))
            .sum::<u64>()
            >= 320
    );
    assert_eq!(decoded.definitions()[0].1.schema().fields().len(), 320);
    for receive in [false, true] {
        let trace = if receive { &received } else { &sent };
        let middle = if receive {
            trace.len() / 2
        } else {
            trace.iter().position(|(_, units)| *units == 256).unwrap()
        };
        for at in [0, middle, trace.len() - 1] {
            for cause in CAUSES {
                control.arm(Some((at, cause.clone())));
                let result = if receive {
                    run_decode(encoded.as_wire(), &read, &control).map(|_| ())
                } else {
                    run_encode(&sources, &types, &control).map(|_| ())
                };
                assert!(matches!(result,Err(Error::Control(actual)) if actual==cause));
                assert_eq!(control.trace(), trace[..=at]);
            }
        }
    }
}

fn observed_encode<'loan, 'source, 'control>(
    sources: &'loan [SchemaSource<'source>],
    types: &'loan EncodedTypeTable<'source>,
    control: &'control Control,
    admit: &mut Admission<'_>,
) -> Result<EncodedSchemas<'loan, 'source, 'control>, Error> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
    let result = (|| {
        let prepared =
            prepare_schemas_encode_observed_in(sources, types, SOURCE, limits(), admit, &mut work)?;
        prepared.emit_observed_in(admit, &mut work)
    })();
    finish(work, result)
}
fn observed_decode<'loan, 'control>(
    definitions: &'loan [wire::SchemaDefinition],
    types: &'loan DecodedTypeTable,
    control: &'control Control,
    admit: &mut Admission<'_>,
) -> Result<DecodedSchemas<'loan, 'control>, Error> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
    let result = (|| {
        let prepared = prepare_schemas_decode_observed_in(
            definitions,
            types,
            SOURCE,
            limits(),
            admit,
            &mut work,
        )?;
        prepared.emit_observed_in(admit, &mut work)
    })();
    finish(work, result)
}

#[test]
fn observed_schema_composition_keeps_original_loans_and_one_caller_scope() {
    let control = Control::default();
    let original = field();
    let roots = [(0, original.clone()), (u32::MAX, original.clone())];
    let types = encode_type_table_sources(&[], &roots, type_limits(), &control).unwrap();
    let read = decode_type_table(types.as_wire(), type_limits(), &control).unwrap();
    let schema = Schema::new(vec![original.clone(), original]);
    let sources = [SchemaSource {
        id: u32::MAX,
        source: &schema,
        field_ids: &[u32::MAX, 0],
    }];
    let original_encoded = run_encode(&sources, &types, &control).unwrap();
    let original_decoded = run_decode(original_encoded.as_wire(), &read, &control).unwrap();
    control.arm(None);
    let mut contributions = Vec::new();
    let mut admit = |facts: &NodeProjectionFacts| {
        assert_eq!(
            facts.coexisting_source_and_request_bytes_upper_bound,
            SOURCE + facts.allocation_request_bytes_upper_bound,
        );
        contributions.push(*facts);
        Ok(())
    };
    let encoded = observed_encode(&sources, &types, &control, &mut admit).unwrap();
    assert_eq!(encoded.as_wire(), original_encoded.as_wire());
    assert_eq!(encoded.facts(), original_encoded.facts());
    assert!(std::ptr::eq(encoded.types(), &types));
    assert!(std::ptr::eq(encoded.original_control(), &control));
    let trace = control.trace();
    assert!(
        trace
            .iter()
            .all(|(phase, _)| *phase == CompilePhase::Validate)
    );
    assert!(trace.last().unwrap().1 > 0);
    assert_eq!(contributions.last(), Some(encoded.facts()));
    assert!(contributions.windows(2).all(|pair| {
        pair[0].allocation_request_bytes_upper_bound <= pair[1].allocation_request_bytes_upper_bound
            && pair[0].cumulative_work_upper_bound <= pair[1].cumulative_work_upper_bound
    }));
    control.arm(None);
    let decoded = observed_decode(encoded.as_wire(), &read, &control, &mut |_| Ok(())).unwrap();
    assert_eq!(decoded.facts(), original_decoded.facts());
    assert_eq!(decoded.definitions()[0].0, u32::MAX);
    assert!(
        decoded.definitions()[0]
            .1
            .schema()
            .metadata()
            .as_arc()
            .is_none()
    );
    assert!(Arc::ptr_eq(
        &decoded.definitions()[0].1.schema().fields()[0],
        read.field(u32::MAX).unwrap()
    ));
    assert!(Arc::ptr_eq(
        &decoded.definitions()[0].1.schema().fields()[1],
        read.field(0).unwrap()
    ));
    assert!(std::ptr::eq(decoded.types(), &read));
    assert!(
        control
            .trace()
            .iter()
            .all(|(phase, _)| *phase == CompilePhase::Validate)
    );

    // A sealed preparation cannot be emitted under an equal-looking control.
    let foreign = Control::default();
    let prepared =
        prepare_schemas_decode(encoded.as_wire(), &read, SOURCE, limits(), &control).unwrap();
    let mut work = CompileCheckpoints::try_new(&foreign, CompilePhase::Decode).unwrap();
    ordinary(
        prepared
            .emit_observed_in(&mut |_| Ok(()), &mut work)
            .map(|_| ()),
    );
    assert_eq!(foreign.trace(), [(CompilePhase::Decode, 0)]);
}

#[test]
fn observed_schema_parent_numeric_refusal_precedes_pending_quantum_and_output_reserve() {
    let control = Control::default();
    let roots = [];
    let types = encode_type_table_sources(&[], &roots, type_limits(), &control).unwrap();
    let read = decode_type_table(types.as_wire(), type_limits(), &control).unwrap();
    let empty = Schema::empty();
    let sources = [SchemaSource {
        id: 0,
        source: &empty,
        field_ids: &[],
    }];
    let definitions = [wire::SchemaDefinition {
        id: u32::MAX,
        field_ids: vec![],
    }];
    let send_bytes = Layout::array::<usize>(1).unwrap().size()
        + Layout::array::<wire::SchemaDefinition>(1).unwrap().size();
    let receive_bytes = Layout::array::<usize>(1).unwrap().size()
        + 2 * Layout::array::<(u32, MaterializedSchema)>(1)
            .unwrap()
            .size();
    for receive in [false, true] {
        for cause in CAUSES {
            control.arm(Some((1, cause)));
            let phase = if receive {
                CompilePhase::Decode
            } else {
                CompilePhase::Encode
            };
            let mut work = CompileCheckpoints::try_new(&control, phase).unwrap();
            for _ in 0..255 {
                work.step().unwrap();
            }
            let mut calls = 0;
            let mut admit = |facts: &NodeProjectionFacts| {
                calls += 1;
                assert_eq!(
                    facts.allocation_requests_upper_bound,
                    if receive { 3 } else { 2 }
                );
                assert_eq!(
                    facts.allocation_request_bytes_upper_bound,
                    if receive { receive_bytes } else { send_bytes }
                );
                Err(CompileControlError::ResourceExhausted)
            };
            let result = if receive {
                prepare_schemas_decode_observed_in(
                    &definitions,
                    &read,
                    SOURCE,
                    limits(),
                    &mut admit,
                    &mut work,
                )
                .map(|_| ())
            } else {
                prepare_schemas_encode_observed_in(
                    &sources,
                    &types,
                    SOURCE,
                    limits(),
                    &mut admit,
                    &mut work,
                )
                .map(|_| ())
            };
            assert!(matches!(
                result,
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
            assert_eq!(calls, 1);
            assert_eq!(control.trace(), [(phase, 0)]);
        }
    }
    // The full prepared contribution is admitted again before its first owned
    // output request. Preparation and its persistent index are already real.
    control.arm(None);
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let prepared = prepare_schemas_decode_observed_in(
        &definitions,
        &read,
        SOURCE,
        limits(),
        &mut |_| Ok(()),
        &mut work,
    )
    .unwrap();
    work.flush().unwrap();
    let trace = control.trace();
    control
        .stop
        .lock()
        .unwrap()
        .replace((trace.len(), CompileControlError::Cancelled));
    assert!(matches!(
        prepared.emit_observed_in(
            &mut |_| Err(CompileControlError::ResourceExhausted),
            &mut work
        ),
        Err(Error::Control(CompileControlError::ResourceExhausted))
    ));
    assert_eq!(control.trace(), trace);
    control.arm(None);
}

#[test]
fn observed_schema_every_actual_callback_keeps_primary_cause_and_caller_ordinary_footer() {
    let control = Control::default();
    let original = field();
    let roots = [(0, original.clone())];
    let types = encode_type_table_sources(&[], &roots, type_limits(), &control).unwrap();
    let read = decode_type_table(types.as_wire(), type_limits(), &control).unwrap();
    let schema = Schema::new(vec![original]);
    let sources = [SchemaSource {
        id: 0,
        source: &schema,
        field_ids: &[0],
    }];
    let encoded = run_encode(&sources, &types, &control).unwrap();
    prefixes(
        &control,
        || observed_encode(&sources, &types, &control, &mut |_| Ok(())).map(|_| ()),
        true,
    );
    prefixes(
        &control,
        || observed_decode(encoded.as_wire(), &read, &control, &mut |_| Ok(())).map(|_| ()),
        true,
    );
    let bad = [wire::SchemaDefinition {
        id: 0,
        field_ids: vec![u32::MAX],
    }];
    control.arm(None);
    ordinary(observed_decode(&bad, &read, &control, &mut |_| Ok(())).map(|_| ()));
    assert!(control.trace().last().unwrap().1 > 0);
    prefixes(
        &control,
        || observed_decode(&bad, &read, &control, &mut |_| Ok(())).map(|_| ()),
        false,
    );
    let bad_sources = [SchemaSource {
        id: 0,
        source: &schema,
        field_ids: &[],
    }];
    prefixes(
        &control,
        || observed_encode(&bad_sources, &types, &control, &mut |_| Ok(())).map(|_| ()),
        false,
    );
}
