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
use crate::commit::filter_delete::FilterDeletePreparer;
use crate::commit::model::{AddedContent, ObjectIdentity};
use crate::commit::new_manifests::PendingNewFiles;
use crate::commit::overwrite::preparer_tests::{Fixture, data};
use crate::commit::row_delta::RowDeltaPreparer;
use crate::commit::staging::{StagingBase, StagingEngine};
use crate::commit::write_stack::codec::CompactFragmentCodec;
use crate::commit::write_stack::domain::{
    IcebergCommitFragment, IcebergContentRange, IcebergCreationReceipt, IcebergCreationScope,
    IcebergDataFileArtifact, IcebergDeletionVectorArtifact, IcebergFinalDeleteEffect,
    IcebergFinalDeleteKind,
};
use crate::commit::write_stack::receiving::IcebergReceiving;
use crate::commit::write_stack::test_support::{paid_fragments, sample_metrics, sample_partition};
use crate::commit::write_stack::tests::dv_session;
use crate::delete_file::IcebergFileFormat;
use crate::iceberg::spec::{DataFileBuilder, DataFileFormat, FormatVersion, Struct};

use novarocks_spi::connector::write_stack::{
    ConnectorWriteProductionProof, ConnectorWriteQueryIdentity, ConnectorWriterPhysicalContext,
    WriteTargetOrdinal,
};
use novarocks_spi::connector::{ConnectorError, ConnectorMutationFailure};
use sha2::{Digest, Sha256};

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

async fn observe_target(
    source: Arc<TableMetadata>,
    io: &dyn ArtifactWriter,
    path: &str,
) -> Result<TargetObservation> {
    let id = source
        .current_snapshot_id()
        .ok_or_else(|| invalid("fixture source is empty"))?;
    let mut scope = ManifestReadScope::new(source, id)?;
    let partition = TypedPartition::bind_type(
        scope.metadata().default_partition_spec(),
        &scope.domain().endpoint().partition_type(0).unwrap(),
        &Struct::empty(),
    )
    .unwrap();
    scope
        .observe(
            BTreeMap::from([(path.to_owned(), partition)]),
            io.file_io(),
            || io.check_active(),
        )
        .await
}

pub(crate) async fn observed_materialized_staging_fixture(
    metadata: Arc<TableMetadata>,
    operation: &IcebergCommitOperation,
    io: &dyn ArtifactWriter,
    additions: Vec<DataFile>,
) -> Result<impl Preparer> {
    let mut fragments = Vec::with_capacity(additions.len());
    for file in additions {
        let metrics = sample_metrics(file.record_count(), file.file_size_in_bytes());
        match file.content_type() {
            DataContentType::Data => fragments.push(IcebergCommitFragment::data_file(
                IcebergDataFileArtifact::try_new(
                    file.file_path().to_owned(),
                    IcebergFileFormat::Parquet,
                    sample_partition(),
                    metrics,
                    file.first_row_id(),
                )
                .unwrap(),
            )),
            DataContentType::PositionDeletes => {
                let path = file.referenced_data_file().unwrap();
                let observed = observe_target(Arc::clone(&metadata), io, &path).await?;
                let facts = &observed.files[&path];
                let locator = facts.data_locator(&observed.completion)?;
                let merged = exclusive_positions(facts)?.into_keys().cloned().collect();
                let vector = IcebergDeletionVectorArtifact::try_new(
                    file.file_path().to_owned(),
                    sample_partition(),
                    metrics,
                    path,
                    IcebergContentRange::try_new(
                        file.content_offset().unwrap(),
                        file.content_size_in_bytes().unwrap(),
                    )
                    .unwrap(),
                    file.record_count(),
                    merged,
                )
                .unwrap();
                let effect = IcebergFinalDeleteEffect::try_new(
                    locator,
                    1,
                    IcebergFinalDeleteKind::ReplacePositions(Box::new(vector)),
                )
                .unwrap();
                fragments.push(IcebergCommitFragment::final_delete(effect));
            }
            _ => panic!("unexpected materialized fixture content"),
        }
    }
    observed_compact_step(metadata, operation, io, fragments).await
}

struct FixtureWriterCohort(BTreeMap<WriteTargetOrdinal, ConnectorWriterPhysicalContext>);

impl ConnectorWriteProductionProof for FixtureWriterCohort {
    fn admits(
        &self,
        target: WriteTargetOrdinal,
        physical: &ConnectorWriterPhysicalContext,
    ) -> std::result::Result<bool, ConnectorError> {
        Ok(self.0.get(&target) == Some(physical))
    }
}

async fn fixture_content_digest(
    io: &dyn ArtifactWriter,
    path: &str,
    size: u64,
) -> Result<[u8; 32]> {
    if path.starts_with("file://") {
        let file = io.file_io().new_input(path)?;
        if file.exists().await? {
            let bytes = file.read().await?;
            assert_eq!(bytes.len() as u64, size, "{path}");
            return Ok(Sha256::digest(bytes.as_ref()).into());
        }
    }
    // Some staging tests address manifest-only files without a storage body.
    let mut digest = Sha256::new();
    digest.update(path);
    digest.update(size.to_be_bytes());
    Ok(digest.finalize().into())
}

async fn observed_compact_step(
    metadata: Arc<TableMetadata>,
    operation: &IcebergCommitOperation,
    io: &dyn ArtifactWriter,
    fragments: Vec<IcebergCommitFragment>,
) -> Result<Arc<MaterializedDeleteStep>> {
    let id = metadata.current_snapshot_id().unwrap();
    let (handle, adapter) = dv_session();
    let scope = IcebergCreationScope::try_new(
        handle.session_id(),
        *uuid::Uuid::now_v7().as_bytes(),
        crate::read_snapshot::metadata_identity_digest(&metadata).map_err(invalid)?,
    )
    .unwrap();
    let handle = handle.try_with_creation_scope(scope).unwrap();
    let execution = *uuid::Uuid::now_v7().as_bytes();
    let query = ConnectorWriteQueryIdentity::try_new(execution, 1, [2; 16]).unwrap();
    let writers = FixtureWriterCohort(
        handle
            .expected_targets()
            .into_iter()
            .map(|ordinal| {
                let physical =
                    ConnectorWriterPhysicalContext::new(execution, 1, [4; 16], 0, ordinal.get());
                (ordinal, physical)
            })
            .collect(),
    );
    let mut targets = Vec::with_capacity(fragments.len());
    let mut witnessed = Vec::with_capacity(fragments.len());
    for mut fragment in fragments {
        let target = handle
            .targets()
            .iter()
            .find(|target| target.branch() == fragment.branch())
            .unwrap()
            .ordinal();
        if let Some(path) = fragment.new_object_path() {
            let physical = writers.0[&target];
            let format = if fragment.referenced_data_file().is_some() {
                IcebergFileFormat::Puffin
            } else {
                IcebergFileFormat::Parquet
            };
            let identity = scope.identity(physical, path, format).unwrap();
            let size = fragment.metrics().unwrap().file_size_in_bytes();
            let digest = fixture_content_digest(io, path, size).await?;
            let receipt = IcebergCreationReceipt::try_new(
                physical,
                *uuid::Uuid::now_v7().as_bytes(),
                identity,
                size,
                digest,
            )
            .unwrap();
            fragment = fragment.try_with_creation(receipt).unwrap();
        }
        targets.push(target);
        witnessed.push(adapter.wrap_commit_fragment(fragment));
    }
    let (fragments, author) = paid_fragments(&adapter, witnessed);
    let mut receiving = IcebergReceiving::default();
    receiving
        .begin_query(
            query,
            &handle.expected_targets(),
            &mut Some(Arc::new(writers)),
            handle.targets(),
            None,
        )
        .unwrap();
    receiving
        .arrive_original(
            &handle,
            &adapter,
            query,
            targets.into_iter().zip(fragments).collect(),
            &author,
        )
        .map_err(|error| invalid(error.message()))?;
    receiving.confirm(query).unwrap();
    receiving.require_complete().unwrap();
    let received = receiving.take_compact_files();
    for addition in received
        .entries()
        .filter_map(|entry| entry.addition.as_ref())
    {
        operation.adopt_session_data(ObjectIdentity::from_shared_path(Arc::clone(
            &addition.object_path,
        ))?)?;
    }
    let counts = received.counts;
    operation.install_stage_new_files(
        0,
        PendingNewFiles {
            received,
            source: Arc::clone(&metadata),
            codec: CompactFragmentCodec::for_adapter(&adapter),
        },
    )?;
    let step = Arc::new(MaterializedDeleteStep::compact(
        operation.clone(),
        0,
        counts,
    ));
    step.observe(metadata, id, io, operation.request_context())
        .await?;
    Ok(step)
}

async fn remove_target(
    fixture: &Fixture,
    source: &TableMetadata,
    io: &dyn ArtifactWriter,
) -> Arc<MaterializedDeleteStep> {
    let source = Arc::new(source.clone());
    let observed = observe_target(Arc::clone(&source), io, TARGET)
        .await
        .unwrap();
    let facts = &observed.files[TARGET];
    let locator = facts.data_locator(&observed.completion).unwrap();
    let old_exclusive_deletes = exclusive_positions(facts)
        .unwrap()
        .into_keys()
        .cloned()
        .collect();
    let effect = IcebergFinalDeleteEffect::try_new(
        locator,
        3,
        IcebergFinalDeleteKind::RemoveDataFile {
            partition: sample_partition(),
            old_exclusive_deletes,
        },
    )
    .unwrap();
    observed_compact_step(
        source,
        &fixture.operation,
        io,
        vec![IcebergCommitFragment::final_delete(effect)],
    )
    .await
    .unwrap()
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
    let intent = intent.with_compact_stage(0).unwrap();
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
    let base = fixture
        .stage(source, &intent, &FilterDeletePreparer::always_true())
        .await
        .0;
    let intent = intent.with_compact_stage(0).unwrap();
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
    let intent = intent.with_compact_stage(0).unwrap();
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
    use std::error::Error as _;

    let fixture = Fixture::new();
    let start = initial(&fixture).await;
    let source = add_dv(&fixture, start).await;
    let io = fixture.operation.begin_attempt().unwrap();
    let observation = observe_target(Arc::new(source.clone()), &io, TARGET)
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
    let step = observed_compact_step(
        Arc::new(source.clone()),
        &fixture.operation,
        &io,
        vec![IcebergCommitFragment::final_delete(effect)],
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
    let intent = intent.with_compact_stage(0).unwrap();
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
