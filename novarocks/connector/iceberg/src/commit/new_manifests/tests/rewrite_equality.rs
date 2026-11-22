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

use std::collections::BTreeMap;

use crate::commit::dependency::{self, ValidationInputs, Verdict};
use crate::commit::fast_append::FastAppendPreparer;
use crate::commit::model::{
    ArtifactWriter, CommitProtocol, FileChanges, OperationIntent, OperationIntentParts,
    RequestShape, StartSnapshot,
};
use crate::commit::overwrite::preparer_tests::data;
use crate::commit::row_delta::RowDeltaPreparer;
use crate::commit::selected_rewrite::CompactRewritePreparer;
use crate::commit::staging::{StagingBase, StagingEngine};
use crate::commit::write_stack::domain::IcebergWriteTableFacts;
use crate::iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat, Operation, Struct};
use crate::iceberg::table::Table;
use crate::manifest::extract_data_files_with_stats_at;
use crate::scan_model::IcebergDeleteFileContent;

use super::*;

async fn append(fixture: &Fixture, base: TableMetadata, path: &str, rows: u64) -> TableMetadata {
    let file = AddedContent::new_logical_data(data(path, rows, Struct::empty()), 0).unwrap();
    let intent = fixture.intent(&base, "main", vec![file]);
    fixture.stage(base, &intent, &FastAppendPreparer).await.0
}

fn install_rewrite(fixture: &Fixture, source: &TableMetadata, path: &str) -> Arc<AtomicUsize> {
    let window = Arc::new(Window::default());
    let live = window.0.clone();
    let author = ConnectorPayloadRetentionGuard::from_byte_budget(window);
    let adapter = adapter("compact-rewrite-equality", 1);
    let encoder = IcebergWriteFragmentEncoder::new(adapter.clone());
    let decoder = IcebergWriteFragmentDecoder::new(adapter.clone());
    let e = source.current_snapshot().unwrap();
    let facts = IcebergWriteTableFacts::try_new(
        source.uuid().to_string(),
        "db".into(),
        "t".into(),
        source.location().to_owned(),
        format!("{}/data", source.location()),
        "main".into(),
        Some(e.snapshot_id()),
        e.sequence_number(),
        source.current_schema_id(),
        source.default_partition_spec_id(),
        source.format_version() as u8,
    )
    .unwrap();

    let fragment = adapter.wrap_commit_fragment(data_fragment(path));
    let encoded = encoder.encode_commit_fragment_payload(&fragment).unwrap();
    let mut original = Some(
        decoder
            .decode_commit_fragment_payload(&encoded, &author)
            .unwrap(),
    );
    let mut pending = CompactEntry::prepare(
        &adapter,
        original.as_ref().unwrap(),
        0,
        &author,
        Some(&facts),
    )
    .unwrap();
    let entry = pending.finish(&adapter, &mut original).unwrap();
    fixture
        .operation
        .adopt_session_data(
            ObjectIdentity::from_shared_path(entry.addition.as_ref().unwrap().object_path.clone())
                .unwrap(),
        )
        .unwrap();
    let mut received = ReceivedCompactFiles::default();
    received.counts = entry.counts;
    received.push(entry);
    received.author = Some(author);
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
    live
}

#[tokio::test]
async fn b3_compact_rewrite_keeps_a_concurrent_equality_delete_applicable() {
    let fixture = registered::fixture();
    let location = format!("file://{}", fixture.directory.path().display());
    let old = format!("{location}/data/old.parquet");
    let at_start = format!("{location}/data/at-start.parquet");
    let equality = format!("{location}/data/equality.parquet");
    let later = format!("{location}/data/later.parquet");
    let rewritten = format!("{location}/data/rewritten.parquet");

    let first = append(&fixture, fixture.metadata(FormatVersion::V2), &old, 2).await;
    let source = append(&fixture, first, &at_start, 3).await;
    let e = source.current_snapshot().unwrap();
    let start = StartSnapshot {
        snapshot_id: e.snapshot_id(),
        sequence_number: e.sequence_number(),
    };
    let attempt = fixture.operation.begin_attempt().unwrap();
    let replaced: Vec<_> = ValidationInputs::new(&source, Some(start.snapshot_id), &attempt)
        .live_set()
        .await
        .unwrap()
        .values()
        .map(|entry| entry.frozen.clone())
        .collect();
    assert_eq!(replaced.len(), 2);
    assert!(replaced.iter().any(|entry| {
        entry.identity().path() == old
            && entry
                .facts()
                .data_sequence
                .is_some_and(|age| age < start.sequence_number)
    }));

    // The rewrite owns the whole contents of E. Its output arrives before B
    // changes, so the paid compact entry may only derive its age from E.
    let retained = install_rewrite(&fixture, &source, &rewritten);
    assert!(retained.load(Ordering::SeqCst) > 0);

    let delete = DataFileBuilder::default()
        .content(DataContentType::EqualityDeletes)
        .file_path(equality.clone())
        .file_format(DataFileFormat::Parquet)
        .record_count(1)
        .file_size_in_bytes(100)
        .partition_spec_id(0)
        .equality_ids(Some(vec![1]))
        .build()
        .unwrap();
    let delete_intent = fixture.intent(
        &source,
        "main",
        vec![AddedContent::new_logical_data(delete, 0).unwrap()],
    );
    let (after_delete, _) = fixture
        .stage(source, &delete_intent, &RowDeltaPreparer)
        .await;
    let delete_sequence = after_delete.current_snapshot().unwrap().sequence_number();
    let base = append(&fixture, after_delete, &later, 3).await;
    let b = base.current_snapshot().unwrap();
    let base_id = b.snapshot_id();
    let base_sequence = b.sequence_number();
    assert!(start.sequence_number < delete_sequence && delete_sequence < base_sequence);

    let seed_intent = fixture.intent(&base, "main", Vec::new());
    let intent = OperationIntent::new(OperationIntentParts {
        target: seed_intent.target().clone(),
        target_ref: "main".into(),
        start: Some(start),
        changes: FileChanges::default(),
        dependencies: Vec::new(),
        protocol: CommitProtocol::RewriteFiles {
            output_spec_id: 0,
            replaced: replaced.into(),
        },
        shape: RequestShape::SnapshotProducing,
        summary: BTreeMap::new(),
        token: fixture.operation.token(),
    })
    .unwrap()
    .with_compact_stage(0)
    .unwrap();
    assert_eq!(
        dependency::validate(&intent, &base, Some(base_id), &attempt)
            .await
            .unwrap(),
        Verdict::Holds
    );
    let mut engine = StagingEngine::begin(
        StagingBase::Existing {
            metadata: base,
            metadata_location: format!("{location}/metadata/base.json"),
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

    let metadata = engine.metadata().clone();
    engine.freeze().unwrap();
    let committed = metadata.current_snapshot().unwrap();
    assert_eq!(committed.parent_snapshot_id(), Some(base_id));
    assert_eq!(committed.summary().operation, Operation::Replace);
    let snapshot = committed.snapshot_id();
    let files = ValidationInputs::new(&metadata, Some(snapshot), &attempt)
        .live_set()
        .await
        .unwrap()
        .clone();
    let output = files
        .values()
        .find(|entry| entry.frozen.identity().path() == rewritten)
        .unwrap();
    assert_eq!(
        output.frozen.facts().data_sequence,
        Some(start.sequence_number)
    );
    assert_eq!(
        output.frozen.facts().file_sequence,
        Some(committed.sequence_number())
    );

    let table = Table::builder()
        .identifier(intent.target().ident.clone())
        .file_io(attempt.file_io().clone())
        .metadata(metadata)
        .runtime(fixture.operation.runtime().iceberg_runtime().clone())
        .build()
        .unwrap();
    let scan = extract_data_files_with_stats_at(&table, snapshot)
        .await
        .unwrap();
    assert_eq!(scan.len(), 2);
    let rewritten = scan.iter().find(|file| file.path == rewritten).unwrap();
    assert_eq!(rewritten.record_count, Some(5));
    assert_eq!(rewritten.data_sequence_number, Some(start.sequence_number));
    let [delete] = rewritten.delete_files.as_slice() else {
        panic!("Rewritten file must retain its one applicable equality delete");
    };
    assert_eq!(delete.path, equality);
    assert_eq!(delete.file_content, IcebergDeleteFileContent::Equality);
    assert_eq!(delete.sequence_number, Some(delete_sequence));
    assert_eq!(delete.equality_field_ids, vec![1]);

    let later = scan.iter().find(|file| file.path == later).unwrap();
    assert_eq!(later.data_sequence_number, Some(base_sequence));
    assert!(later.delete_files.is_empty());
}
