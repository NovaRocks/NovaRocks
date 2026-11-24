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

//! Exercises preserved lineage from the production writer through compact rewrite publication.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_spi::connector::write_stack::{
    ConnectorOpenWriterRequest, ConnectorWriteExecution, ConnectorWriteQueryIdentity,
    ConnectorWriterPhysicalContext, WriteTargetOrdinal,
};
use novarocks_spi::connector::{
    CatalogHandle, CatalogVersion, ConnectorError, ConnectorInstanceDescriptor,
    ConnectorInstanceId, ConnectorProviderId, ConnectorRequestContext, ConnectorStopOwner,
    ConnectorWriteAdmissionPurpose,
};

use crate::commit::dependency::ValidationInputs;
use crate::commit::model::{
    AddedContent, ArtifactWriter, CommitProtocol, EntryIdentity, FileChanges, ObjectIdentity,
    OperationIntent, OperationIntentParts, RequestShape, SeqField, StartSnapshot,
};
use crate::commit::new_manifests::PendingNewFiles;
use crate::commit::overwrite::preparer_tests::{Fixture, data};
use crate::commit::selected_rewrite::CompactRewritePreparer;
use crate::commit::staging::{StagingBase, StagingEngine};
use crate::commit::write_stack::codec::CompactFragmentCodec;
use crate::commit::write_stack::control::written_file_from_fragment;
use crate::commit::write_stack::domain::{
    IcebergCommitFragment, IcebergDataBranchRecipe, IcebergFrozenRewriteBranchInput,
    IcebergSealedWriteTarget, IcebergSessionFacts, IcebergWriteBranch, IcebergWriteCommitSession,
    IcebergWriteFlavor, IcebergWriteSessionId, IcebergWriteTableFacts, IcebergWriterHandle,
    IcebergWriterOutput,
};
use crate::commit::write_stack::execution::IcebergWriteStackExecution;
use crate::commit::write_stack::runtime::{IcebergWriteAdapter, build_write_adapter};
use crate::commit::write_stack::test_support::{paid_fragments_counted, write_query_identity};
use crate::iceberg::spec::{FormatVersion, Operation, Struct, TableMetadata};
use crate::row_lineage_synth::{
    ICEBERG_LAST_UPDATED_SEQ_COL, ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
    ICEBERG_RESERVED_FIELD_ID_ROW_ID, ICEBERG_ROW_ID_COL,
};

fn table_facts(metadata: &TableMetadata) -> IcebergWriteTableFacts {
    IcebergWriteTableFacts::try_new(
        metadata.uuid().to_string(),
        "db".into(),
        "t".into(),
        metadata.location().into(),
        format!("{}/data", metadata.location()),
        "main".into(),
        metadata.current_snapshot_id(),
        metadata.current_snapshot().unwrap().sequence_number(),
        metadata.current_schema_id(),
        metadata.default_partition_spec_id(),
        3,
    )
    .unwrap()
}

fn rewrite_session(
    source: &TableMetadata,
    data: BTreeSet<EntryIdentity>,
) -> IcebergWriteCommitSession {
    let mut facts =
        IcebergSessionFacts::ordinary(ConnectorWriteAdmissionPurpose::OrdinaryDml, None);
    facts.rewrite_inputs =
        vec![IcebergFrozenRewriteBranchInput::try_new(data, BTreeSet::new()).unwrap()];
    IcebergWriteCommitSession::try_new_sealed(
        IcebergWriteSessionId::new(),
        table_facts(source),
        IcebergWriteFlavor::DistributedRewrite,
        facts,
        vec![IcebergSealedWriteTarget::new(
            WriteTargetOrdinal::try_new(0).unwrap(),
            IcebergWriteBranch::Data,
        )],
    )
    .unwrap()
}

fn begin_rewrite(handle: &IcebergWriteCommitSession) -> ConnectorWriteQueryIdentity {
    let query = write_query_identity();
    handle
        .with_active_receiving(|receiving| {
            receiving.begin_query(
                query,
                &handle.expected_targets(),
                &mut None,
                handle.targets(),
                None,
            )
        })
        .unwrap();
    query
}

fn arrive_rewrite(
    handle: &IcebergWriteCommitSession,
    adapter: &IcebergWriteAdapter,
    query: ConnectorWriteQueryIdentity,
    fragments: Vec<IcebergCommitFragment>,
) -> (Result<(), ConnectorError>, Arc<AtomicUsize>) {
    let (paid, author, live) = paid_fragments_counted(
        adapter,
        fragments
            .into_iter()
            .map(|fragment| adapter.wrap_commit_fragment(fragment))
            .collect(),
    );
    let ordinal = handle.targets()[0].ordinal();
    let result = handle.with_active_receiving(|receiving| {
        receiving.arrive_original(
            handle,
            adapter,
            query,
            paid.into_iter()
                .map(|fragment| (ordinal, fragment))
                .collect(),
            &author,
        )
    });
    (result, live)
}

async fn real_writer_fragment(
    metadata: &TableMetadata,
) -> (IcebergWriteAdapter, IcebergCommitFragment) {
    let descriptor = ConnectorInstanceDescriptor {
        provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
        instance_id: ConnectorInstanceId::parse("lineage").unwrap(),
    };
    let catalog = CatalogHandle::new(
        descriptor.instance_id.clone(),
        CatalogVersion::from_bytes([9; 32]),
    );
    let adapter = build_write_adapter(descriptor, catalog.clone());
    let runtime = tokio::runtime::Handle::current();
    let binding = crate::access_binding::IcebergReadBinding::new(
        None,
        novarocks_fs::FsAccessResolver::new(),
        Arc::new(novarocks_fs::TokioFileIoRuntime::new(runtime.clone())),
        novarocks_fs::TokioRefreshExecutor::shared(runtime.clone()),
        Arc::new(novarocks_fs::TokioFileIoSpawner::new(runtime)),
    );
    let table = table_facts(metadata);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(ICEBERG_ROW_ID_COL, DataType::Int64, true),
        Field::new(ICEBERG_LAST_UPDATED_SEQ_COL, DataType::Int64, true),
    ]));
    let public_input = novarocks_spi::connector::ConnectorWriteInputShape::Data {
        fields: schema
            .fields()
            .iter()
            .enumerate()
            .map(|(ordinal, field)| {
                let mut token = [0; 32];
                token[..8].copy_from_slice(&(ordinal as u64).to_be_bytes());
                novarocks_spi::connector::ConnectorWriteFieldBinding::new(
                    novarocks_spi::connector::ConnectorWriteFieldToken::from_bytes(token),
                    field.as_ref().clone(),
                )
            })
            .collect(),
    };
    let input_identity =
        crate::commit::write_stack::domain::data_write_input_identity(&public_input).unwrap();
    let handle = IcebergWriterHandle::try_new_data(
        table,
        IcebergWriterOutput::for_test(
            crate::delete_file::IcebergFileFormat::Parquet,
            parquet::basic::Compression::SNAPPY,
        ),
        IcebergDataBranchRecipe::try_new(
            Some(serde_json::to_string(metadata.current_schema().as_ref()).unwrap()),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            true,
        )
        .unwrap()
        .with_write_input_identity(input_identity),
    )
    .unwrap();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![10, 20, 30])),
            Arc::new(Int64Array::from(vec![Some(0), None, Some(2)])),
            Arc::new(Int64Array::from(vec![Some(0), None, Some(1)])),
        ],
    )
    .unwrap();
    let stop = ConnectorStopOwner::new();
    let execution = IcebergWriteStackExecution::new(catalog, adapter.clone(), binding);
    let mut writer = execution
        .open_writer(ConnectorOpenWriterRequest {
            resources: Some(crate::commit::write_stack::test_support::writer_resources()),
            handle: adapter.wrap_writer_handle(handle),
            target: WriteTargetOrdinal::try_new(0).unwrap(),
            expected_schema: schema,
            physical: ConnectorWriterPhysicalContext::new([1; 16], 1, [2; 16], 0, 0),
            context: ConnectorRequestContext::try_new(
                Instant::now() + Duration::from_secs(60),
                stop.view(),
                64 * 1024,
                1024 * 1024,
            )
            .unwrap(),
        })
        .await
        .unwrap();
    writer.append(batch).await.unwrap();
    let fragments = writer.finish().await.unwrap();
    assert_eq!(fragments.len(), 1);
    let fragment = adapter.commit_fragment(&fragments[0]).unwrap().clone();
    (adapter, fragment)
}

#[tokio::test]
async fn production_rewrite_preserves_stored_lineage_and_inherits_a_fresh_file_base() {
    let fixture = Fixture::new();
    let initial = fixture.metadata(FormatVersion::V3);
    let old_path = format!("{}/data/source.parquet", initial.location());
    let append = fixture.intent(
        &initial,
        "main",
        vec![AddedContent::new_logical_data(data(&old_path, 3, Struct::empty()), 0).unwrap()],
    );
    let (source, _) = fixture
        .stage(
            initial,
            &append,
            &crate::commit::fast_append::FastAppendPreparer,
        )
        .await;
    assert_eq!(source.next_row_id(), 3);
    let (adapter, fragment) = real_writer_fragment(&source).await;
    let file = written_file_from_fragment(&fragment, &source).unwrap();
    assert_eq!(file.first_row_id, None);
    // Generic conversion intentionally omits reserved fields from user-schema bounds.
    assert!(
        !file
            .lower_bounds
            .contains_key(&ICEBERG_RESERVED_FIELD_ID_ROW_ID)
    );
    let physical = std::fs::File::open(
        fragment
            .path()
            .strip_prefix("file://")
            .unwrap_or(fragment.path()),
    )
    .unwrap();
    let mut reader =
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(physical)
            .unwrap()
            .build()
            .unwrap();
    let batch = reader.next().unwrap().unwrap();
    let row_ids = batch
        .column_by_name(ICEBERG_ROW_ID_COL)
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(row_ids.iter().collect::<Vec<_>>(), [Some(0), None, Some(2)]);
    let last_updated = batch
        .column_by_name(ICEBERG_LAST_UPDATED_SEQ_COL)
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(
        last_updated.iter().collect::<Vec<_>>(),
        [Some(0), None, Some(1)]
    );
    for (column, field_id) in [
        (ICEBERG_ROW_ID_COL, ICEBERG_RESERVED_FIELD_ID_ROW_ID),
        (
            ICEBERG_LAST_UPDATED_SEQ_COL,
            ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
        ),
    ] {
        assert_eq!(
            batch.schema().field_with_name(column).unwrap().metadata()
                [parquet::arrow::PARQUET_FIELD_ID_META_KEY],
            field_id.to_string()
        );
    }
    let attempt = fixture.operation.begin_attempt().unwrap();
    let mut inputs = ValidationInputs::new(&source, source.current_snapshot_id(), &attempt);
    let live = inputs.live_set().await.unwrap().clone();
    let frozen_sequence = source.current_snapshot().unwrap().sequence_number();
    let handle = rewrite_session(&source, live.keys().cloned().collect());
    let query = begin_rewrite(&handle);
    let output_path = fragment.path().to_owned();
    let (arrival, retained) = arrive_rewrite(&handle, &adapter, query, vec![fragment]);
    arrival.unwrap();
    handle
        .with_active_receiving(|receiving| receiving.confirm(query))
        .unwrap();
    let received = handle.begin_commit().unwrap().take_compact_files();
    let object = {
        let mut entries = received.entries();
        let addition = entries.next().unwrap().addition.as_ref().unwrap();
        assert!(entries.next().is_none());
        assert_eq!(addition.object_path.as_ref(), output_path);
        assert_eq!(addition.data_sequence, SeqField::Explicit(frozen_sequence));
        ObjectIdentity::from_shared_path(Arc::clone(&addition.object_path)).unwrap()
    };
    assert!(retained.load(Ordering::SeqCst) > 0);
    fixture.operation.adopt_session_data(object).unwrap();
    fixture
        .operation
        .install_stage_new_files(
            0,
            PendingNewFiles {
                received,
                source: Arc::new(source.clone()),
                codec: CompactFragmentCodec::for_adapter(&adapter),
            },
        )
        .unwrap();
    let intent = OperationIntent::new(OperationIntentParts {
        target: append.target().clone(),
        target_ref: "main".into(),
        start: Some(StartSnapshot {
            snapshot_id: source.current_snapshot_id().unwrap(),
            sequence_number: frozen_sequence,
        }),
        changes: FileChanges::default(),
        dependencies: vec![crate::commit::model::Dependency::RefUnchanged],
        protocol: CommitProtocol::RewriteFiles {
            output_spec_id: source.default_partition_spec_id(),
            replaced: live
                .values()
                .map(|entry| entry.frozen.clone())
                .collect::<Vec<_>>()
                .into(),
        },
        shape: RequestShape::SnapshotProducing,
        summary: BTreeMap::new(),
        token: fixture.operation.token(),
    })
    .unwrap()
    .with_compact_stage(0)
    .unwrap();
    let mut engine = StagingEngine::begin(
        StagingBase::Existing {
            metadata_location: format!("{}/metadata/base.json", source.location()),
            metadata: source,
        },
        &intent,
        &attempt,
    )
    .unwrap();
    engine
        .stage(&CompactRewritePreparer {
            operation: fixture.operation.clone(),
            stage: 0,
        })
        .await
        .unwrap();
    assert_eq!(retained.load(Ordering::SeqCst), 0);
    let after = engine.metadata().clone();
    engine.freeze().unwrap();
    assert_eq!(after.next_row_id(), 6);
    let snapshot = after.current_snapshot().unwrap();
    assert_eq!(snapshot.row_range(), Some((3, 3)));
    assert_eq!(snapshot.summary().operation, Operation::Replace);
    let manifests = crate::manifest::load_manifest_list(snapshot, attempt.file_io(), &after)
        .await
        .unwrap();
    let manifest = manifests
        .entries()
        .iter()
        .find(|manifest| manifest.added_files_count == Some(1))
        .unwrap();
    assert_eq!(manifest.first_row_id, Some(3));
    let raw = crate::manifest::source::ManifestSource::committed(
        snapshot.snapshot_id(),
        snapshot.manifest_list(),
        manifest,
    )
    .load_sized(attempt.file_io())
    .await
    .unwrap();
    assert_eq!(raw.entries()[0].data_file().first_row_id(), None);
    let mut inputs = ValidationInputs::new(&after, Some(snapshot.snapshot_id()), &attempt);
    let live = inputs.live_set().await.unwrap();
    assert_eq!(live.len(), 1);
    let output = &live[&EntryIdentity::DataFile { path: output_path }];
    let facts = output.frozen.facts();
    assert_eq!(facts.first_row_id, Some(3));
    assert_eq!(facts.data_sequence, Some(frozen_sequence));
    assert_eq!(facts.file_sequence, Some(snapshot.sequence_number()));
}
