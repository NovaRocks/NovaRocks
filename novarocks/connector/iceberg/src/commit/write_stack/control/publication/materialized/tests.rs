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

//! Tests E-to-B entry binding, including actual manifest ordinals and delete identities.

use super::*;
use crate::commit::fast_append::FastAppendPreparer;
use crate::commit::model::AddedContent;
use crate::commit::overwrite::preparer_tests::{Fixture, data};
use crate::commit::row_delta::RowDeltaPreparer;
use crate::commit::staging::{StagingBase, StagingEngine};
use crate::commit::truncate::TruncatePreparer;
use crate::commit::write_stack::test_support::sample_partition;
use crate::iceberg::spec::{DataFileBuilder, DataFileFormat, FormatVersion, Struct};
use novarocks_spi::connector::ConnectorMutationFailure;

const TARGET: &str = "s3://b/data.parquet";
const CONCURRENT_DV: &str = "s3://b/concurrent-delete.puffin";

async fn initial(fixture: &Fixture) -> TableMetadata {
    let base = fixture.metadata(FormatVersion::V3);
    let intent = fixture.intent(
        &base,
        "main",
        vec![AddedContent::new_logical_data(data(TARGET, 3, Struct::empty()), 0).unwrap()],
    );
    fixture.stage(base, &intent, &FastAppendPreparer).await.0
}

async fn remove_target(
    fixture: &Fixture,
    source: &TableMetadata,
    io: &dyn ArtifactWriter,
) -> Arc<MaterializedDeleteStep> {
    let id = source.current_snapshot_id().unwrap();
    let mut scope = ManifestReadScope::new(Arc::new(source.clone()), id).unwrap();
    let endpoint = scope.domain().endpoint();
    let partition = TypedPartition::bind_type(
        source.default_partition_spec(),
        &endpoint.partition_type(0).unwrap(),
        &Struct::empty(),
    )
    .unwrap();
    let observed = scope
        .observe(
            BTreeMap::from([(TARGET.to_owned(), partition)]),
            io.file_io(),
            || io.check_active(),
        )
        .await
        .unwrap();
    let facts = &observed.files[TARGET];
    let locator = facts.data_locator(&observed.completion).unwrap();
    let old_exclusive_deletes = exclusive_positions(facts)
        .unwrap()
        .into_keys()
        .cloned()
        .collect();
    let partition = sample_partition();
    let effect = IcebergFinalDeleteEffect::try_new(
        locator,
        3,
        IcebergFinalDeleteKind::RemoveDataFile {
            partition: partition.clone(),
            old_exclusive_deletes,
        },
    )
    .unwrap();
    let step = Arc::new(MaterializedDeleteStep::new(BTreeMap::from([(
        TARGET.to_owned(),
        Target {
            partition,
            final_effect: Some(effect),
        },
    )])));
    step.observe(
        Arc::new(source.clone()),
        id,
        io,
        fixture.operation.request_context(),
    )
    .await
    .unwrap();
    step
}

async fn add_dv(fixture: &Fixture, base: TableMetadata) -> TableMetadata {
    let delete = DataFileBuilder::default()
        .content(DataContentType::PositionDeletes)
        .file_format(DataFileFormat::Puffin)
        .file_path(CONCURRENT_DV.into())
        .partition(Struct::empty())
        .record_count(1)
        .file_size_in_bytes(1000)
        .referenced_data_file(Some(TARGET.into()))
        .content_offset(Some(4))
        .content_size_in_bytes(Some(16))
        .build()
        .unwrap();
    let intent = fixture.intent(
        &base,
        "main",
        vec![AddedContent::new_logical_data(delete, 0).unwrap()],
    );
    fixture.stage(base, &intent, &RowDeltaPreparer).await.0
}

#[tokio::test]
async fn rebound_full_file_removal_also_deletes_a_concurrent_dv() {
    let fixture = Fixture::new();
    let source = initial(&fixture).await;
    let io = fixture.operation.begin_attempt().unwrap();
    let step = remove_target(&fixture, &source, &io).await;
    let (original, spec, file) = step.observed_data_file(TARGET).unwrap();
    assert_eq!(original.current_snapshot_id(), source.current_snapshot_id());
    assert_eq!(spec, 0);
    assert_eq!(file.file_path(), TARGET);

    let base = add_dv(&fixture, source.clone()).await;
    let intent = fixture.intent(&source, "main", Vec::new());
    let mut staging = StagingEngine::begin(
        StagingBase::Existing {
            metadata: base,
            metadata_location: "file:///base.json".into(),
        },
        &intent,
        &io,
    )
    .unwrap();
    staging.stage(&step).await.unwrap();
    let after = staging.metadata();
    let summary = &after
        .current_snapshot()
        .unwrap()
        .summary()
        .additional_properties;
    assert_eq!(summary["deleted-data-files"], "1");
    assert_eq!(summary["removed-position-delete-files"], "1");
    let sources = staging.view();
    let (_, live) = sources.observe_live_sources().await.unwrap();
    assert!(live.is_empty());
}

#[tokio::test]
async fn rebound_missing_target_reports_validation_conflict() {
    use std::error::Error as _;

    let fixture = Fixture::new();
    let source = initial(&fixture).await;
    let io = fixture.operation.begin_attempt().unwrap();
    let step = remove_target(&fixture, &source, &io).await;
    let intent = fixture.intent(&source, "main", Vec::new());
    let base = fixture.stage(source, &intent, &TruncatePreparer).await.0;
    let mut staging = StagingEngine::begin(
        StagingBase::Existing {
            metadata: base,
            metadata_location: "file:///base.json".into(),
        },
        &intent,
        &io,
    )
    .unwrap();
    let error = staging.stage(&step).await.unwrap_err();
    let source = error
        .source()
        .and_then(|source| source.downcast_ref::<ConnectorMutationFailure>())
        .unwrap();
    assert_eq!(
        source.message(),
        format!("Missing required files to delete: {TARGET}")
    );
}

struct RewriteCurrentManifests {
    replacement_delete: Option<AddedContent>,
}

#[async_trait::async_trait]
impl Preparer for RewriteCurrentManifests {
    async fn prepare(
        &self,
        view: &StagedView<'_>,
        intent: &OperationIntent,
    ) -> Result<PreparedChange> {
        use crate::commit::manifest_encoding::{ManifestWriteSource, SourcedManifestEntry};
        use crate::commit::row_delta_dv_metadata::write_added_entry_groups;
        use crate::commit::staging::{ManifestEntryWrite, write_sourced_manifest};
        use crate::iceberg::spec::ManifestContentType;

        let sources = view.target_sources().await?.unwrap();
        let data_source = sources
            .sources()
            .find(|source| source.descriptor.content == ManifestContentType::Data)
            .unwrap();
        let raw = data_source.load_sized(view.artifacts().file_io()).await?;
        let author = Arc::new(ManifestWriteSource::from_header(raw.metadata())?);
        let entries = data_source
            .bind(&raw)?
            .map(|entry| {
                let entry = entry?;
                Ok(SourcedManifestEntry {
                    source: Arc::clone(&author),
                    entry: ManifestEntryWrite::Existing {
                        file: entry.raw.data_file().clone(),
                        frozen: FrozenEntry::from_bound_entry(&entry)?,
                    },
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let snapshot = view.stage_snapshot_id()?;
        let moved = write_sourced_manifest(
            view.artifacts(),
            view.metadata(),
            snapshot,
            view.metadata().default_partition_spec(),
            ManifestContentType::Data,
            entries,
            std::slice::from_ref(&data_source.descriptor.manifest_path),
        )
        .await?;
        let mut manifests = vec![moved];
        if let Some(replacement) = &self.replacement_delete {
            manifests
                .extend(write_added_entry_groups(view, snapshot, vec![replacement.clone()]).await?);
        }
        crate::commit::overwrite::prepare_snapshot_change(
            view,
            intent,
            snapshot,
            Operation::Replace,
            manifests,
            HashMap::from([("total-records".into(), "3".into())]),
            false,
        )
        .await
    }
}

#[tokio::test]
async fn locates_the_original_entry_after_a_manifest_rewrite() {
    let fixture = Fixture::new();
    let source = initial(&fixture).await;
    let io = fixture.operation.begin_attempt().unwrap();
    let step = remove_target(&fixture, &source, &io).await;
    let intent = fixture.intent(&source, "main", Vec::new());
    let base = fixture
        .stage(
            source,
            &intent,
            &RewriteCurrentManifests {
                replacement_delete: None,
            },
        )
        .await
        .0;
    let mut staging = StagingEngine::begin(
        StagingBase::Existing {
            metadata: base,
            metadata_location: "file:///base.json".into(),
        },
        &intent,
        &io,
    )
    .unwrap();
    staging.stage(&step).await.unwrap();
    let view = staging.view();
    let (_, live) = view.observe_live_sources().await.unwrap();
    assert!(live.is_empty());
}

#[tokio::test]
async fn replacing_an_already_replaced_dv_reports_a_missing_required_delete() {
    use crate::commit::write_stack::domain::{IcebergContentRange, IcebergDeletionVectorArtifact};
    use crate::commit::write_stack::test_support::sample_metrics;
    use std::error::Error as _;

    let fixture = Fixture::new();
    let start = initial(&fixture).await;
    let source = add_dv(&fixture, start).await;
    let io = fixture.operation.begin_attempt().unwrap();
    let id = source.current_snapshot_id().unwrap();
    let mut scope = ManifestReadScope::new(Arc::new(source.clone()), id).unwrap();
    let ty = scope.domain().endpoint().partition_type(0).unwrap();
    let typed =
        TypedPartition::bind_type(source.default_partition_spec(), &ty, &Struct::empty()).unwrap();
    let observation = scope
        .observe(
            BTreeMap::from([(TARGET.to_owned(), typed)]),
            io.file_io(),
            || io.check_active(),
        )
        .await
        .unwrap();
    let facts = &observation.files[TARGET];
    let locator = facts.data_locator(&observation.completion).unwrap();
    let merged = exclusive_positions(facts)
        .unwrap()
        .into_keys()
        .cloned()
        .collect();
    let vector = IcebergDeletionVectorArtifact::try_new(
        "s3://b/new-output.puffin".into(),
        sample_partition(),
        sample_metrics(2, 1024),
        TARGET.into(),
        IcebergContentRange::try_new(4, 16).unwrap(),
        2,
        merged,
    )
    .unwrap();
    let effect = IcebergFinalDeleteEffect::try_new(
        locator,
        1,
        IcebergFinalDeleteKind::ReplacePositions(Box::new(vector)),
    )
    .unwrap();
    let step = Arc::new(MaterializedDeleteStep::new(BTreeMap::from([(
        TARGET.into(),
        Target {
            partition: sample_partition(),
            final_effect: Some(effect),
        },
    )])));
    step.observe(
        Arc::new(source.clone()),
        id,
        &io,
        fixture.operation.request_context(),
    )
    .await
    .unwrap();
    let replacement = DataFileBuilder::default()
        .content(DataContentType::PositionDeletes)
        .file_format(DataFileFormat::Puffin)
        .file_path("s3://b/other-replacement.puffin".into())
        .partition(Struct::empty())
        .record_count(2)
        .file_size_in_bytes(1000)
        .referenced_data_file(Some(TARGET.into()))
        .content_offset(Some(4))
        .content_size_in_bytes(Some(16))
        .build()
        .unwrap();
    let intent = fixture.intent(&source, "main", Vec::new());
    let base = fixture
        .stage(
            source,
            &intent,
            &RewriteCurrentManifests {
                replacement_delete: Some(AddedContent::new_logical_data(replacement, 0).unwrap()),
            },
        )
        .await
        .0;
    let mut staging = StagingEngine::begin(
        StagingBase::Existing {
            metadata: base,
            metadata_location: "file:///base.json".into(),
        },
        &intent,
        &io,
    )
    .unwrap();
    let error = staging.stage(&step).await.unwrap_err();
    let failure = error
        .source()
        .and_then(|source| source.downcast_ref::<ConnectorMutationFailure>())
        .unwrap();
    assert_eq!(
        failure.message(),
        format!("Missing required files to delete: {CONCURRENT_DV}")
    );
}
