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

// These tests call the same private constructors used by production admission.
// Small stock preparation/signing/freeze uses no catalog or provider IO.
use super::*;
use crate::commit::write_stack::copy_on_write::{
    IcebergCowBranchInput, IcebergCowBranchRecipe, IcebergCowFreezeInput,
    freeze_copy_on_write_branches,
};
use crate::iceberg::spec::{
    FormatVersion, NestedField, Operation, PartitionSpec, PrimitiveType, Schema, Snapshot,
    SnapshotReference, SnapshotRetention, SortOrder, Summary, TableMetadataBuilder, Type,
};
use arrow::array::{ArrayRef, BooleanArray, Int8Array, Int64Array, StringArray, new_null_array};
use arrow::datatypes::{DataType, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use novarocks_spi::connector::{
    ConnectorInstanceId, ConnectorRowMutationEffect, ConnectorRowMutationIntent,
    ConnectorRowMutationPreparation, ConnectorRowMutationPreparationOutcome,
    ConnectorRowMutationPreparationRequest, ConnectorRowMutationSelection, ConnectorStopOwner,
    ConnectorTableHandle, ConnectorWriteOperationId, ConnectorWriteTargetRef,
};

fn stock_metadata() -> TableMetadata {
    let schema = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::required(
            1,
            "flag",
            Type::Primitive(PrimitiveType::Boolean),
        ))])
        .build()
        .expect("stock schema");
    let metadata = TableMetadataBuilder::new(
        schema,
        PartitionSpec::unpartition_spec(),
        SortOrder::unsorted_order(),
        "memory://warehouse/db/t".to_string(),
        FormatVersion::V3,
        [
            ("write.row-lineage".to_string(), "true".to_string()),
            ("write.update.mode".to_string(), "copy-on-write".to_string()),
        ]
        .into_iter()
        .collect(),
    )
    .expect("stock metadata builder")
    .build()
    .expect("stock metadata")
    .metadata;
    let snapshot = Snapshot::builder()
        .with_snapshot_id(41)
        .with_sequence_number(1)
        .with_timestamp_ms(metadata.last_updated_ms())
        .with_manifest_list("memory://warehouse/db/t/snap-41.avro".to_string())
        .with_summary(Summary {
            operation: Operation::Append,
            additional_properties: Default::default(),
        })
        .with_schema_id(metadata.current_schema_id())
        .with_row_range(0, 0)
        .build();
    metadata
        .into_builder(None)
        .add_snapshot(snapshot)
        .expect("stock snapshot")
        .set_ref(
            "main",
            SnapshotReference::new(
                41,
                SnapshotRetention::Branch {
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            ),
        )
        .expect("stock main ref")
        .build()
        .expect("stock exact generation")
        .metadata
}

fn table(metadata: Arc<TableMetadata>) -> crate::iceberg::table::Table {
    crate::iceberg::table::Table::builder()
        .identifier(crate::iceberg::TableIdent::from_strs(["db", "t"]).expect("identifier"))
        .metadata(metadata)
        .file_io(crate::iceberg::io::FileIO::new_with_memory())
        .disable_cache()
        .build()
        .expect("stock table")
}

#[test]
fn cow_statistics_retains_the_original_generation_after_table_and_admission_exit() {
    let original = Arc::new(stock_metadata());
    let weak = Arc::downgrade(&original);
    let original_address = Arc::as_ptr(&original);
    let uuid = original.uuid();
    let snapshot = original.current_snapshot_id();
    let table = table(Arc::clone(&original));
    let admission = IcebergAdmissionStatisticsMetadata::from_loaded_table(&table, true);
    let statistics = admission
        .for_statistics(None, true)
        .expect("COW statistics handoff");
    assert!(std::ptr::eq(admission.as_ref(), original_address));
    drop(original);
    drop(table);
    drop(admission);
    // This independent statistics owner is the only remaining strong metadata alias.
    assert!(std::ptr::eq(statistics.as_ref(), original_address));
    assert_eq!(statistics.uuid(), uuid);
    assert_eq!(statistics.current_snapshot_id(), snapshot);
    assert_eq!(weak.strong_count(), 1);
    assert!(weak.upgrade().is_some());
    drop(statistics);
    assert!(weak.upgrade().is_none());
}

#[test]
fn ordinary_metadata_keeps_its_owned_copy_and_cow_never_adopts_a_prospective_generation() {
    let original = Arc::new(stock_metadata());
    let table = table(Arc::clone(&original));
    let ordinary = IcebergAdmissionStatisticsMetadata::from_loaded_table(&table, false);
    assert!(!std::ptr::eq(ordinary.as_ref(), original.as_ref()));
    assert_eq!(
        serde_json::to_value(ordinary.as_ref()).expect("ordinary metadata JSON"),
        serde_json::to_value(original.as_ref()).expect("original metadata JSON")
    );
    let statistics = ordinary
        .for_statistics(None, false)
        .expect("ordinary handoff");
    assert!(!std::ptr::eq(statistics.as_ref(), ordinary.as_ref()));
    let shared = IcebergAdmissionStatisticsMetadata::from_loaded_table(&table, true);
    let failure = shared
        .for_statistics(Some(original.as_ref()), true)
        .err()
        .expect("COW rejects prospective metadata");
    assert_eq!(failure.kind(), ConnectorErrorKind::InvalidRequest);
    assert_eq!(
        failure.message(),
        "Iceberg COW admission lost its original immutable metadata owner"
    );
}

fn prepared(
    metadata: &TableMetadata,
    owner: &ConnectorProviderBindingKey,
    context: &ConnectorRequestContext,
) -> ConnectorRowMutationPreparation {
    let payload = crate::metadata::IcebergTablePayload {
        namespace: "db".to_string(),
        table: "t".to_string(),
        metadata_location: None,
        table_info: Some(crate::scan_model::IcebergTableInfo {
            catalog: owner.instance_id.as_str().to_string(),
            namespace: "db".to_string(),
            table: "t".to_string(),
            table_uuid: Some(metadata.uuid().to_string()),
            current_snapshot_id: metadata.current_snapshot_id(),
            schema_id: metadata.current_schema_id(),
            location: metadata.location().to_string(),
            schema: crate::schema_facts::iceberg_schema_def(metadata.current_schema()),
            serialized_metadata: Some(
                serde_json::to_string(metadata).expect("small metadata JSON"),
            ),
            serialized_metadata_rows: None,
        }),
        metadata_columns: ["_file", "_pos", "_row_id", "_last_updated_sequence_number"]
            .into_iter()
            .map(str::to_string)
            .collect(),
        metadata_table_type: None,
        prepared_files: Vec::new(),
        explicit_files: None,
        row_mutation_frozen_source: false,
        logical_type_columns: BTreeMap::new(),
        hidden_columns: Vec::new(),
    };
    let request = ConnectorRowMutationPreparationRequest {
        operation_id: ConnectorWriteOperationId::from_bytes([9; 16]),
        table: ConnectorTableHandle::try_new(
            owner.instance_id.clone(),
            Bytes::from(serde_json::to_vec(&payload).expect("same original handle payload")),
        )
        .expect("handle"),
        target_ref: ConnectorWriteTargetRef::parse("main".to_string()).expect("ref"),
        intent: ConnectorRowMutationIntent::Merge {
            effects: vec![
                ConnectorRowMutationEffect::Replace,
                ConnectorRowMutationEffect::Insert,
            ],
        },
        context: context.clone(),
    };
    match crate::commit::row_mutation_preparation::prepare_row_mutation(request, owner)
        .expect("stock preparation")
    {
        ConnectorRowMutationPreparationOutcome::Prepared(value) => value,
        ConnectorRowMutationPreparationOutcome::Denied(_) => panic!("stock preparation denied"),
    }
}

fn insert_selection(
    preparation: &ConnectorRowMutationPreparation,
) -> ConnectorRowMutationSelection {
    let contract = preparation.match_contract();
    let width = contract.identity_fields().len()
        + contract.before_fields().len()
        + contract.after_fields().len()
        + 1;
    let mut fields = vec![None; width];
    for field in contract.identity_fields() {
        fields[field.source_ordinal() as usize] = Some(field.field().clone());
    }
    for field in contract
        .before_fields()
        .iter()
        .chain(contract.after_fields())
    {
        fields[field.target_ordinal() as usize] = Some(field.field().clone());
    }
    fields[contract.effect_field().target_ordinal() as usize] =
        Some(contract.effect_field().field().clone());
    let schema = Arc::new(ArrowSchema::new(
        fields
            .into_iter()
            .map(|field| field.expect("dense original schema"))
            .collect::<Vec<_>>(),
    ));
    let mut arrays: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .map(|field| {
            if field.is_nullable() {
                return new_null_array(field.data_type(), 2);
            }
            match field.data_type() {
                DataType::Utf8 => Arc::new(StringArray::from(vec!["", ""])) as ArrayRef,
                DataType::Int64 => Arc::new(Int64Array::from(vec![0, 0])) as ArrayRef,
                DataType::Boolean => Arc::new(BooleanArray::from(vec![false, true])) as ArrayRef,
                DataType::Int8 => Arc::new(Int8Array::from(vec![3, 3])) as ArrayRef,
                _ => panic!("unexpected stock scalar"),
            }
        })
        .collect();
    for field in contract.after_fields() {
        arrays[field.target_ordinal() as usize] = Arc::new(BooleanArray::from(vec![false, true]));
    }
    arrays[contract.effect_field().target_ordinal() as usize] =
        Arc::new(Int8Array::from(vec![3, 3]));
    let batch = RecordBatch::try_new(Arc::clone(&schema), arrays).expect("stock insert batch");
    let selection =
        ConnectorRowMutationSelection::try_new(schema, vec![batch], 1024, 64 * 1024 * 1024)
            .expect("small selection");
    contract
        .validate_selection(&selection)
        .expect("same original signed contract");
    selection
}

fn signed_and_frozen(
    metadata: &TableMetadata,
    owner: &ConnectorProviderBindingKey,
    preparation: &ConnectorRowMutationPreparation,
    selection: &ConnectorRowMutationSelection,
    context: &ConnectorRequestContext,
) -> (ConnectorWriteInputShape, Vec<IcebergCowBranchRecipe>) {
    let facts = IcebergWriteTableFacts::try_new(
        metadata.uuid().to_string(),
        "db".to_string(),
        "t".to_string(),
        metadata.location().to_string(),
        iceberg_data_location(metadata),
        "main".to_string(),
        metadata.current_snapshot_id(),
        metadata.last_sequence_number(),
        metadata.current_schema_id(),
        metadata.default_partition_spec_id(),
        format_version_number(metadata),
    )
    .expect("original table facts");
    let contract = preparation.match_contract();
    let fields = contract
        .after_fields()
        .iter()
        .map(|field| ConnectorWriteFieldRequest::new(field.field().clone()))
        .collect::<Vec<_>>();
    let input = ConnectorWriteInputRequest::RowLineage {
        data_fields: crate::commit::write_shared::exact_requested_write_fields(metadata, &fields)
            .expect("original requested fields"),
        row_identity_fields: vec![
            ConnectorWriteFieldRequest::new(arrow::datatypes::Field::new(
                "_row_id",
                DataType::Int64,
                true,
            )),
            ConnectorWriteFieldRequest::new(arrow::datatypes::Field::new(
                "_last_updated_sequence_number",
                DataType::Int64,
                true,
            )),
        ],
    };
    let signed = sign_input_shape(&facts, &input).expect("original stock signer");
    let recipes = freeze_copy_on_write_branches(
        selection,
        contract,
        IcebergCowFreezeInput {
            owner,
            catalog: &owner.instance_id,
            namespace: "db",
            table_name: "t",
            metadata,
            snapshot_id: metadata.current_snapshot_id().expect("original snapshot"),
            base_files: Vec::<crate::manifest::DataFileWithStats>::new(),
            input: &signed,
            base_version_digest: preparation.base_version().digest(),
            max_handle_payload_bytes: context.max_handle_payload_bytes(),
        },
    )
    .expect("original stock freeze");
    (signed, recipes)
}

#[test]
fn original_stock_signer_and_append_freeze_are_identical_for_shared_and_previous_owned_metadata() {
    let original = Arc::new(stock_metadata());
    let table = table(Arc::clone(&original));
    let shared = IcebergAdmissionStatisticsMetadata::from_loaded_table(&table, true);
    let statistics = shared
        .for_statistics(None, true)
        .expect("original COW statistics owner");
    let previous_owned = original.as_ref().clone();
    let stop = ConnectorStopOwner::new();
    let context = ConnectorRequestContext::try_new(
        std::time::Instant::now() + std::time::Duration::from_secs(30),
        stop.view(),
        16 * 1024 * 1024,
        64 * 1024 * 1024,
    )
    .expect("original production payload limits");
    let owner = ConnectorProviderBindingKey {
        instance_id: ConnectorInstanceId::parse("iceberg").expect("instance"),
        incarnation: ProviderBindingEpoch::from_bytes([7; 16]),
    };
    let preparation = prepared(shared.as_ref(), &owner, &context);
    let selection = insert_selection(&preparation);
    let (shared_input, shared_recipes) = signed_and_frozen(
        statistics.as_ref(),
        &owner,
        &preparation,
        &selection,
        &context,
    );
    let (owned_input, owned_recipes) =
        signed_and_frozen(&previous_owned, &owner, &preparation, &selection, &context);
    assert!(matches!(
        shared_input,
        ConnectorWriteInputShape::RowLineage { .. }
    ));
    assert!(matches!(
        owned_input,
        ConnectorWriteInputShape::RowLineage { .. }
    ));
    let shared_fields = shared_input.fields();
    let owned_fields = owned_input.fields();
    assert_eq!(shared_fields.len(), owned_fields.len());
    for (shared, owned) in shared_fields.iter().zip(&owned_fields) {
        assert_eq!(shared.token(), owned.token());
        assert_eq!(shared.field(), owned.field());
    }
    assert_eq!(shared_recipes.len(), 1);
    assert_eq!(owned_recipes.len(), 1);
    assert_eq!(shared_recipes[0].input(), &IcebergCowBranchInput::Append);
    assert_eq!(shared_recipes[0].input(), owned_recipes[0].input());
    assert_eq!(
        shared_recipes[0].selection_digest(),
        owned_recipes[0].selection_digest()
    );
    assert_eq!(
        shared_recipes[0].selection_ordinals(),
        owned_recipes[0].selection_ordinals()
    );
    assert_eq!(shared_recipes[0].selection_ordinals().len(), 2);
    assert!(shared_recipes[0].rewrite_source().is_none());
    assert!(owned_recipes[0].rewrite_source().is_none());
}

#[test]
fn source_metadata_retains_the_exact_shared_generation() {
    let original = Arc::new(stock_metadata());
    let table = table(Arc::clone(&original));
    let source =
        IcebergAdmissionStatisticsMetadata::from_loaded_table(&table, true).into_source_metadata();
    assert!(Arc::ptr_eq(&original, &source));
}
