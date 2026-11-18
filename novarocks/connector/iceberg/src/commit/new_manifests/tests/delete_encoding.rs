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

//! Exercises published Avro encoding and compact coverage for a shared Puffin path.

use super::*;
use crate::commit::model::ArtifactWriter;
use crate::commit::staging::{StagingBase, StagingEngine, write_manifest_list};
use crate::commit::write_stack::test_support::sample_metrics;
use crate::commit::write_stack::{
    IcebergArtifactPartition, IcebergCommitFragment, IcebergContentRange,
    IcebergDeletionVectorArtifact,
};
use crate::iceberg::spec::{
    Literal, NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder, Struct,
    TableMetadataBuilder, Transform, Type,
};
use crate::manifest::source::ManifestSource;

#[tokio::test]
async fn shared_puffin_entries_in_independent_manifests_keep_full_coverage() {
    let fixture = Fixture::new();
    let schema = |value| {
        Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(value),
            ))])
            .build()
            .unwrap()
    };
    let original = schema(PrimitiveType::Int);
    let spec = PartitionSpec::builder(Arc::new(original.clone()))
        .add_partition_field("id", "id_part", Transform::Identity)
        .unwrap()
        .build()
        .unwrap();
    let initial = TableMetadataBuilder::new(
        original,
        spec,
        SortOrder::unsorted_order(),
        fixture.metadata(FormatVersion::V3).location().into(),
        FormatVersion::V3,
        std::collections::HashMap::from([
            ("commit.manifest.target-size-bytes".into(), "1".into()),
            ("commit.manifest-merge.enabled".into(), "false".into()),
        ]),
    )
    .unwrap()
    .build()
    .unwrap()
    .metadata;
    let original_schema = initial.current_schema().clone();
    let base = initial
        .into_builder(None)
        .add_schema(schema(PrimitiveType::Long))
        .unwrap()
        .set_current_schema(-1)
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    let spec_id = base.default_partition_spec_id();
    let historical = crate::read_snapshot::frozen_partition_types(&base, &original_schema).unwrap();
    let tuple = Struct::from_iter([Some(Literal::int(10))]);
    let partition = IcebergArtifactPartition::try_new(
        "id_part=10".into(),
        String::new(),
        spec_id,
        crate::write_descriptor::encode_partition_descriptor_with_type(
            &tuple,
            &historical[&spec_id],
        )
        .unwrap(),
    )
    .unwrap();
    let adapter = adapter("compact-delete-encoding", 1);
    let encoder = IcebergWriteFragmentEncoder::new(adapter.clone());
    let decoder = IcebergWriteFragmentDecoder::new(adapter.clone());
    let window = Arc::new(Window::default());
    let live = window.0.clone();
    let author = ConnectorPayloadRetentionGuard::from_byte_budget(window);
    let mut received = ReceivedCompactFiles::default();
    let shared = format!("{}/data/shared.puffin", base.location());
    let expected_refs = ["s3://b/data/a.parquet", "s3://b/data/b.parquet"];
    for (path, offset) in expected_refs.into_iter().zip([16, 64]) {
        let vector = IcebergDeletionVectorArtifact::try_new(
            shared.clone(),
            partition.clone(),
            sample_metrics(2, 1024),
            path.into(),
            IcebergContentRange::try_new(offset, 32).unwrap(),
            2,
            Vec::new(),
        )
        .unwrap();
        let fragment = adapter.wrap_commit_fragment(IcebergCommitFragment::deletion_vector(vector));
        let encoded = encoder.encode_commit_fragment_payload(&fragment).unwrap();
        let mut incoming = Some(
            decoder
                .decode_commit_fragment_payload(&encoded, &author)
                .unwrap(),
        );
        let entry = CompactEntry::from_arrival(&adapter, &mut incoming, 0, &author).unwrap();
        received.counts = received.counts.checked_add(entry.counts).unwrap();
        received.push(entry);
    }
    fixture
        .operation
        .adopt_session_data(ObjectIdentity::new(&shared).unwrap())
        .unwrap();
    received.author = Some(author);
    fixture
        .operation
        .install_stage_new_files(
            0,
            PendingNewFiles {
                received,
                source: Arc::new(base.clone()),
                codec: CompactFragmentCodec::for_adapter(&adapter),
            },
        )
        .unwrap();
    let intent = fixture
        .intent(&base, "main", Vec::new())
        .with_compact_stage(0)
        .unwrap();
    let attempt = fixture.operation.begin_attempt().unwrap();
    let engine = StagingEngine::begin(
        StagingBase::Existing {
            metadata: base.clone(),
            metadata_location: "file:///table/00000.metadata.json".into(),
        },
        &intent,
        &attempt,
    )
    .unwrap();
    let view = engine.view();
    let snapshot = view.stage_snapshot_id().unwrap();
    let (compact_manifests, counts) =
        prepare_added_manifests(&fixture.operation, &view, 0, snapshot, Some(&historical))
            .await
            .unwrap();
    assert_eq!(counts.position_deletes.files, 2);
    assert_eq!(counts.position_deletes.bytes, 64);
    assert_eq!(live.load(Ordering::SeqCst), 0);
    assert_eq!(compact_manifests.len(), 2);
    let list = write_manifest_list(
        &attempt,
        &base,
        snapshot,
        None,
        compact_manifests.iter().cloned(),
    )
    .await
    .unwrap();
    let coverage = attempt
        .snapshot_artifacts(std::slice::from_ref(&list.object), &[])
        .unwrap();
    assert_eq!(
        coverage
            .references()
            .iter()
            .filter(|object| object.path() == shared)
            .count(),
        1
    );
    let mut indexed = std::collections::BTreeSet::new();
    for descriptor in &compact_manifests {
        let source = ManifestSource::pending(snapshot, descriptor);
        let raw = source.load_sized(attempt.file_io()).await.unwrap();
        source.check_header(raw.metadata()).unwrap();
        assert_eq!(descriptor.content, ManifestContentType::Deletes);
        let expected = &historical[&descriptor.partition_spec_id];
        assert_eq!(
            raw.metadata()
                .partition_spec()
                .partition_type(raw.metadata().schema())
                .unwrap(),
            *expected
        );
        assert_eq!(
            raw.metadata().schema.schema_id(),
            original_schema.schema_id()
        );
        assert_eq!(raw.entries().len(), 1);
        let data = raw.entries()[0].data_file();
        assert_eq!(data.partition(), &tuple);
        assert_eq!(data.file_path(), shared);
        indexed.insert(data.referenced_data_file().unwrap());
    }
    assert_eq!(
        indexed,
        expected_refs.map(str::to_owned).into_iter().collect()
    );
}
