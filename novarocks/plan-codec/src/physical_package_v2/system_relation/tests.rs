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

//! Frozen snapshot carrier regressions. Arrow values and row order are the oracle;
//! no SQL constant-pool, VALUES, batch-count or aggregate wire quota is involved.
use super::*;
use crate::physical_package_v2::{
    FROZEN_PACKAGE_ADMISSION, PackageDecodeLimits, PackageEncodeLimits, decode_fragment_package,
    encode_fragment_package,
};
use crate::resource_preflight_v2::FragmentDecodeResourceModel;
use arrow::array::*;
use novarocks_type_contract::{CompilePhase, PureCompileControl, ValueTypeStorageLifetime};
use prost::Message;
struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
fn package(fields: Arc<[FieldRef]>, batches: Arc<[RecordBatch]>) -> p::FragmentPackage {
    package_data(
        fields,
        p::SystemRelationData::Batches {
            batches,
            storage: ValueTypeStorageLifetime::pure(),
        },
    )
}
fn package_data(fields: Arc<[FieldRef]>, data: p::SystemRelationData) -> p::FragmentPackage {
    let mut builder = p::FragmentBuilder::new(p::FragmentId::new(0));
    let id = p::NodeId::new(0);
    let output = fields
        .iter()
        .enumerate()
        .map(|(ordinal, f)| {
            builder
                .add_value(
                    FunctionValueType::new(f.data_type().clone(), f.is_nullable()),
                    p::ValueOrigin::NodeOutput {
                        node: id,
                        output_ordinal: ordinal as u32,
                    },
                )
                .unwrap()
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    builder
        .add_system_relation(id, output, fields, data)
        .unwrap();
    let fragment = builder
        .finish_definition(
            id,
            p::FragmentSink::Noop,
            p::PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            p::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let flow = novarocks_type_contract::ExpressionControlFlow::try_new(
        vec![],
        vec![],
        fragment.expressions(),
        CompilePhase::Validate,
        &Control,
    )
    .unwrap();
    let uses = p::PhysicalRootUses::try_new(&fragment, flow, vec![], &Control).unwrap();
    let calls = p::FrozenFragmentCalls::try_new(&fragment, &uses, vec![], &Control).unwrap();
    let pruning = p::FrozenFragmentPruning::try_new(fragment.id(), vec![], &Control).unwrap();
    p::FragmentPackage::try_new(
        p::FragmentPackageInput {
            version: p::PlanVersionId::try_new([71; 16]).unwrap(),
            required: p::RequiredContracts::default(),
            fragment,
            expression_uses: uses,
            calls,
            pruning,
            constants: p::ConstantPools::empty(),
            cuts: p::FragmentCuts::default(),
            result: None,
            parameters: novarocks_type_contract::SemanticParameters::try_new([]).unwrap(),
            scans: BTreeMap::new(),
            writes: BTreeMap::new(),
            annotations: Box::default(),
        },
        FROZEN_PACKAGE_ADMISSION,
        &Control,
    )
    .unwrap()
}
fn roundtrip(package: &p::FragmentPackage) -> p::FragmentPackage {
    let wire = encode_fragment_package(package, &PackageEncodeLimits::FROZEN, &Control).unwrap();
    decode_fragment_package(
        &wire.encode_to_vec(),
        &FragmentDecodeResourceModel::try_new(&Control).unwrap(),
        &PackageDecodeLimits::FROZEN,
        &Control,
    )
    .unwrap()
}
fn batches(package: &p::FragmentPackage) -> &[RecordBatch] {
    let p::NodeKind::SystemRelation {
        data: p::SystemRelationData::Batches { batches, .. },
        ..
    } = &package.fragment().nodes()[&p::NodeId::new(0)].kind
    else {
        panic!("snapshot missing")
    };
    batches
}
fn string_snapshot(rows: usize, width: usize) -> p::FragmentPackage {
    let fields: Arc<[FieldRef]> = Arc::from([Arc::new(Field::new("name", DataType::Utf8, false))]);
    let text = "x".repeat(width);
    let values = StringArray::from_iter_values((0..rows).map(|_| text.as_str()));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(fields.to_vec())),
        vec![Arc::new(values)],
    )
    .unwrap();
    package(fields, Arc::from([batch]))
}
#[test]
fn typed_multi_batch_roundtrip_and_empty_snapshot() {
    let mut text_list =
        arrow::array::builder::ListBuilder::new(arrow::array::builder::StringBuilder::new());
    text_list.values().append_value("child");
    text_list.append(true);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec!["view"])),
        Arc::new(Int64Array::from(vec![7])),
        Arc::new(BooleanArray::from(vec![true])),
        Arc::new(Float64Array::from(vec![1.25])),
        Arc::new(TimestampMillisecondArray::from(vec![1234])),
        Arc::new(TimestampNanosecondArray::from(vec![987654321]).with_timezone("UTC")),
        Arc::new(
            Decimal128Array::from(vec![12345])
                .with_precision_and_scale(9, 2)
                .unwrap(),
        ),
        Arc::new(text_list.finish()),
    ];
    let fields: Arc<[FieldRef]> = columns
        .iter()
        .enumerate()
        .map(|(i, c)| Arc::new(Field::new(format!("c{i}"), c.data_type().clone(), false)))
        .collect::<Vec<_>>()
        .into();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields.to_vec())), columns).unwrap();
    let original = package(fields.clone(), Arc::from([batch.clone(), batch]));
    let decoded = roundtrip(&original);
    assert_eq!(batches(&decoded), batches(&original));
    let empty = package(fields, Arc::from([]));
    assert!(batches(&roundtrip(&empty)).is_empty());
}
#[test]
fn data_is_not_subject_to_constant_or_total_wire_quotas() {
    for (rows, width) in [
        (4097, 1),
        (2, 4 * 1024 * 1024 + 1),
        (1, 64 * 1024 * 1024 + 1),
    ] {
        let original = string_snapshot(rows, width);
        let encoded =
            encode_fragment_package(&original, &PackageEncodeLimits::FROZEN, &Control).unwrap();
        if width > 64 * 1024 * 1024 {
            assert!(encoded.encoded_len() > 64 * 1024 * 1024);
        }
        let decoded = decode_fragment_package(
            &encoded.encode_to_vec(),
            &FragmentDecodeResourceModel::try_new(&Control).unwrap(),
            &PackageDecodeLimits::FROZEN,
            &Control,
        )
        .unwrap();
        assert!(
            batches(&decoded) == batches(&original),
            "snapshot values differ"
        );
    }
}
#[test]
fn zero_column_batches_preserve_rows() {
    let batch = RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(5137)),
    )
    .unwrap();
    let original = package(Arc::from([]), Arc::from([batch]));
    let decoded = roundtrip(&original);
    assert_eq!(batches(&decoded)[0].num_rows(), 5137);
    assert_eq!(batches(&decoded)[0].num_columns(), 0);
}
#[test]
fn receiver_rejects_wrong_schema_incomplete_frames_and_geometry() {
    let original = string_snapshot(2, 8);
    let valid = encode_fragment_package(&original, &PackageEncodeLimits::FROZEN, &Control).unwrap();
    let model = FragmentDecodeResourceModel::try_new(&Control).unwrap();
    let fields: Arc<[FieldRef]> = Arc::from([
        Arc::new(Field::new("a", DataType::Int64, false)),
        Arc::new(Field::new("b", DataType::Boolean, false)),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(fields.to_vec())),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(BooleanArray::from(vec![true])),
        ],
    )
    .unwrap();
    let typed = package(fields, Arc::from([batch]));
    let mut mismatched =
        encode_fragment_package(&typed, &PackageEncodeLimits::FROZEN, &Control).unwrap();
    mismatched.system_relation_units[0]
        .output_field_ids
        .swap(0, 1);
    assert!(
        decode_fragment_package(
            &mismatched.encode_to_vec(),
            &model,
            &PackageDecodeLimits::FROZEN,
            &Control
        )
        .is_err()
    );
    let mut wrong = valid.clone();
    wrong.system_relation_units[0].output_field_ids.clear();
    assert!(
        decode_fragment_package(
            &wrong.encode_to_vec(),
            &model,
            &PackageDecodeLimits::FROZEN,
            &Control
        )
        .is_err()
    );
    let mut truncated = valid.clone();
    truncated.system_relation_units[0].units[0].pop();
    assert!(
        decode_fragment_package(
            &truncated.encode_to_vec(),
            &model,
            &PackageDecodeLimits::FROZEN,
            &Control
        )
        .is_err()
    );
    let mut geometry = valid;
    let unit = &mut geometry.system_relation_units[0].units[0];
    let last = unit.len() - 1;
    let _ = last;
    let metadata_bytes = i32::from_le_bytes(unit[41..45].try_into().unwrap()) as usize;
    let body_start = 45 + metadata_bytes;
    // The first UTF8 offset is required to be nonnegative. Framing and the
    // described body length remain complete, so geometry supplies the refusal.
    unit[body_start..body_start + 4].copy_from_slice(&(-1i32).to_le_bytes());
    assert!(
        decode_fragment_package(
            &geometry.encode_to_vec(),
            &model,
            &PackageDecodeLimits::FROZEN,
            &Control
        )
        .is_err()
    );
}
fn unchecked_unit(batch: &RecordBatch) -> Vec<u8> {
    let fields = batch.schema().fields().to_vec();
    let declarations = fields
        .iter()
        .map(|f| FunctionValueType::new(f.data_type().clone(), f.is_nullable()))
        .collect::<Vec<_>>();
    let types = declarations.iter().collect::<Vec<_>>();
    let mut request = |_: Layout| Ok::<_, std::convert::Infallible>(());
    let mut opaque = |_: usize| Ok::<_, std::convert::Infallible>(());
    let mut creator =
        ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut request, &mut opaque);
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Encode).unwrap();
    let recipe = recipe(&fields, &types, &mut creator, &mut work).unwrap();
    encode_unit(
        &recipe,
        batch,
        &mut Observer {
            creation: RefCell::new(&mut creator),
            work: RefCell::new(&mut work),
        },
    )
    .unwrap()
}
#[test]
fn receiver_checks_decimal_precision_and_required_nulls() {
    let model = FragmentDecodeResourceModel::try_new(&Control).unwrap();
    let fields: Arc<[FieldRef]> =
        Arc::from([Arc::new(Field::new("d", DataType::Decimal128(3, 0), false))]);
    let schema = Arc::new(Schema::new(fields.to_vec()));
    let valid = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(
            Decimal128Array::from(vec![123])
                .with_precision_and_scale(3, 0)
                .unwrap(),
        )],
    )
    .unwrap();
    let original = package(fields, Arc::from([valid]));
    let mut wire =
        encode_fragment_package(&original, &PackageEncodeLimits::FROZEN, &Control).unwrap();
    let invalid = RecordBatch::try_new(
        schema,
        vec![Arc::new(
            Decimal128Array::from(vec![1234])
                .with_precision_and_scale(3, 0)
                .unwrap(),
        )],
    )
    .unwrap();
    wire.system_relation_units[0].units[0] = unchecked_unit(&invalid);
    assert!(
        decode_fragment_package(
            &wire.encode_to_vec(),
            &model,
            &PackageDecodeLimits::FROZEN,
            &Control
        )
        .is_err()
    );
    let fields: Arc<[FieldRef]> = Arc::from([Arc::new(Field::new("n", DataType::Int64, false))]);
    let valid = RecordBatch::try_new(
        Arc::new(Schema::new(fields.to_vec())),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )
    .unwrap();
    let original = package(fields, Arc::from([valid]));
    let mut wire =
        encode_fragment_package(&original, &PackageEncodeLimits::FROZEN, &Control).unwrap();
    let nullable = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)])),
        vec![Arc::new(Int64Array::from(vec![None::<i64>]))],
    )
    .unwrap();
    let mut unit = unchecked_unit(&nullable);
    unit[4..36].copy_from_slice(&wire.system_relation_units[0].units[0][4..36]);
    wire.system_relation_units[0].units[0] = unit;
    assert!(
        decode_fragment_package(
            &wire.encode_to_vec(),
            &model,
            &PackageDecodeLimits::FROZEN,
            &Control
        )
        .is_err()
    );
}
#[test]
fn raw_data_is_classified_but_full_heap_is_invoiced() {
    let package = string_snapshot(2, 1024);
    let wire = encode_fragment_package(&package, &PackageEncodeLimits::FROZEN, &Control).unwrap();
    let raw = wire.encode_to_vec();
    let model = FragmentDecodeResourceModel::try_new(&Control).unwrap();
    let projection = model
        .preflight(&raw, PackageDecodeLimits::FROZEN.wire, &Control)
        .unwrap();
    assert!(projection.usage.system_relation_input_bytes >= 2048);
    assert!(projection.usage.system_relation_requested_heap_bytes_upper >= 2048);
    let mut opaque_quotes = Vec::new();
    let mut request = |_: Layout| Ok::<_, std::convert::Infallible>(());
    let mut opaque = |bytes| {
        opaque_quotes.push(bytes);
        Ok::<_, std::convert::Infallible>(())
    };
    let mut creator =
        ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut request, &mut opaque);
    super::super::decode_fragment_package_with_creator(
        &raw,
        &model,
        &PackageDecodeLimits::FROZEN,
        &mut creator,
        &Control,
    )
    .unwrap();
    drop(creator);
    assert!(opaque_quotes.contains(&projection.usage.peak_requested_heap_bytes_upper));
}

#[test]
fn execution_refuses_deferred_and_non_singleton_snapshots() {
    let fields: Arc<[FieldRef]> = Arc::from([Arc::new(Field::new("name", DataType::Utf8, false))]);
    let deferred = package_data(fields, p::SystemRelationData::Deferred);
    assert!(encode_fragment_package(&deferred, &PackageEncodeLimits::FROZEN, &Control).is_err());
    let mut wire = encode_fragment_package(
        &string_snapshot(1, 1),
        &PackageEncodeLimits::FROZEN,
        &Control,
    )
    .unwrap();
    wire.fragment.as_mut().unwrap().nodes[0]
        .output_properties
        .as_mut()
        .unwrap()
        .distribution
        .as_mut()
        .unwrap()
        .kind = Some(wire::distribution::Kind::RoundRobin(
        novarocks_proto_models::physical_control_v2::Empty {},
    ));
    assert!(
        decode_fragment_package(
            &wire.encode_to_vec(),
            &FragmentDecodeResourceModel::try_new(&Control).unwrap(),
            &PackageDecodeLimits::FROZEN,
            &Control
        )
        .is_err()
    );
}
