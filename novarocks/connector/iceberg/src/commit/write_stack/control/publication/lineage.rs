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

//! Freeze stored lineage from exact writer footer facts before preparing a rewrite.

use super::*;
use crate::row_lineage_synth::{
    ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER, ICEBERG_RESERVED_FIELD_ID_ROW_ID,
};

fn long_bound(bounds: &BTreeMap<i32, Vec<u8>>, field: i32) -> Result<i64, ConnectorError> {
    let bytes = bounds
        .get(&field)
        .ok_or_else(|| corrupt("Preserved rewrite is missing a stored lineage bound"))?;
    let bytes: [u8; 8] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| corrupt("Preserved rewrite stored lineage bound is not an Iceberg Long"))?;
    let value = i64::from_le_bytes(bytes);
    if value < 0 {
        return Err(corrupt(
            "Preserved rewrite stored lineage bound is negative",
        ));
    }
    Ok(value)
}

pub(super) fn freeze_preserved_row_ids(
    validated: &[ValidatedFragment<'_>],
    files: &mut [WrittenFile],
) -> Result<(), ConnectorError> {
    let mut fragments = BTreeMap::new();
    for entry in validated {
        if fragments
            .insert(entry.fragment.path(), entry.fragment)
            .is_some()
        {
            return Err(corrupt("Preserved rewrite has duplicate fragment paths"));
        }
    }
    if fragments.len() != files.len() {
        return Err(corrupt(
            "Preserved rewrite fragments do not exactly cover frozen outputs",
        ));
    }
    // Validate the entire set before changing any frozen output fact.
    let mut markers = Vec::with_capacity(files.len());
    for file in files.iter() {
        let fragment = fragments
            .remove(file.path.as_str())
            .ok_or_else(|| corrupt("Preserved rewrite output has no exact validated fragment"))?;
        if file.content != DataContentType::Data
            || !matches!(fragment.artifact(), IcebergCommitArtifact::DataFile(_))
            || file.record_count != fragment.metrics().record_count()
        {
            return Err(corrupt(
                "Preserved rewrite fragment differs from its frozen data output",
            ));
        }
        if file.record_count == 0 {
            markers.push(file.first_row_id);
            continue;
        }
        let stats = fragment.metrics().column_stats().ok_or_else(|| {
            corrupt("Preserved rewrite output is missing physical row-id statistics")
        })?;
        // A partial stored column would inherit IDs from a physical position
        // after compaction, which cannot preserve the source row identities.
        if stats
            .null_value_counts
            .get(&ICEBERG_RESERVED_FIELD_ID_ROW_ID)
            != Some(&0)
        {
            return Err(corrupt(
                "Preserved rewrite requires a proven non-null stored row-id column",
            ));
        }
        let first_row_id = long_bound(&stats.lower_bounds, ICEBERG_RESERVED_FIELD_ID_ROW_ID)?;
        if stats
            .null_value_counts
            .get(&ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER)
            != Some(&0)
        {
            return Err(corrupt(
                "Preserved rewrite requires proven non-null stored last-updated sequences",
            ));
        }
        let last_updated_min = long_bound(
            &stats.lower_bounds,
            ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
        )?;
        let last_updated_max = long_bound(
            &stats.upper_bounds,
            ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
        )?;
        if last_updated_min > last_updated_max {
            return Err(corrupt(
                "Preserved rewrite stored last-updated sequence bounds are reversed",
            ));
        }
        // Stored sequence values remain per-row facts. A physical rewrite may
        // combine several source ages; it never replaces them with its S0 age.
        if file
            .first_row_id
            .is_some_and(|marker| marker != first_row_id)
        {
            return Err(corrupt(
                "Preserved rewrite first-row-id marker differs from the physical row-id minimum",
            ));
        }
        markers.push(Some(first_row_id));
    }
    for (file, marker) in files.iter_mut().zip(markers) {
        file.first_row_id = marker;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::dependency::ValidationInputs;
    use crate::commit::overwrite::preparer_tests::{Fixture, data};
    use crate::commit::selected_rewrite::{
        SelectedRewriteFiles, SelectedRewriteKind, SelectedRewritePreparer,
    };
    use crate::commit::write_stack::domain::{
        IcebergArtifactMetrics, IcebergDataBranchRecipe, IcebergDataFileArtifact,
        IcebergWriterHandle, IcebergWriterOutput,
    };
    use crate::commit::write_stack::execution::IcebergWriteStackExecution;
    use crate::commit::write_stack::runtime::build_write_adapter;
    use crate::iceberg::spec::{FormatVersion, Operation, Struct};
    use crate::row_lineage_synth::{ICEBERG_LAST_UPDATED_SEQ_COL, ICEBERG_ROW_ID_COL};
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_spi::connector::write_stack::{
        ConnectorOpenWriterRequest, ConnectorWriteExecution, ConnectorWriterPhysicalContext,
    };
    use novarocks_spi::connector::{
        CatalogVersion, ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorProviderId,
        ConnectorStopOwner,
    };
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    async fn real_writer_fragment(metadata: &TableMetadata) -> IcebergCommitFragment {
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
            Arc::new(novarocks_fs::TokioFileTaskSpawner::new(runtime)),
        );
        let table = crate::commit::write_stack::domain::IcebergWriteTableFacts::try_new(
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
        .unwrap();
        let handle = IcebergWriterHandle::try_new_data(
            table,
            IcebergWriterOutput::try_new(
                crate::delete_file::IcebergFileFormat::Parquet,
                parquet::basic::Compression::SNAPPY,
                None,
            )
            .unwrap(),
            IcebergDataBranchRecipe::try_new(
                Some(crate::schema_facts::iceberg_schema_def(
                    metadata.current_schema(),
                )),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                true,
            )
            .unwrap(),
        )
        .unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(ICEBERG_ROW_ID_COL, DataType::Int64, false),
            Field::new(ICEBERG_LAST_UPDATED_SEQ_COL, DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![10, 30])),
                Arc::new(Int64Array::from(vec![0, 2])),
                Arc::new(Int64Array::from(vec![0, 1])),
            ],
        )
        .unwrap();
        let stop = ConnectorStopOwner::new();
        let execution = IcebergWriteStackExecution::new(catalog, adapter.clone(), binding);
        let mut writer = execution
            .open_writer(ConnectorOpenWriterRequest {
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
        adapter.commit_fragment(&fragments[0]).unwrap().clone()
    }

    #[tokio::test]
    async fn production_writer_preserves_source_ids_through_canonical_selected_rewrite() {
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
        let fragment = real_writer_fragment(&source).await;
        let mut files = vec![written_file_from_fragment(&fragment, &source).unwrap()];
        assert_eq!(files[0].first_row_id, None);
        // Generic conversion intentionally omits reserved fields from user-schema bounds.
        assert!(
            !files[0]
                .lower_bounds
                .contains_key(&ICEBERG_RESERVED_FIELD_ID_ROW_ID)
        );
        let validated = [ValidatedFragment {
            ordinal: WriteTargetOrdinal::try_new(0).unwrap(),
            fragment: &fragment,
        }];
        freeze_preserved_row_ids(&validated, &mut files).unwrap();
        assert_eq!(files[0].first_row_id, Some(0));
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
        assert_eq!(row_ids.values().as_ref(), &[0, 2]);
        let last_updated = batch
            .column_by_name(ICEBERG_LAST_UPDATED_SEQ_COL)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(last_updated.values().as_ref(), &[0, 1]);
        let attempt = fixture.operation.begin_attempt().unwrap();
        let mut inputs = ValidationInputs::new(&source, source.current_snapshot_id(), &attempt);
        let live = inputs.live_set().await.unwrap().clone();
        let data_paths = live.keys().cloned().collect();
        let frozen_sequence = source.current_snapshot().unwrap().sequence_number();
        let added = files
            .iter()
            .map(|file| {
                AddedContent::rewritten_data(
                    crate::commit::data_file::from_written_file(file).unwrap(),
                    file.partition_spec_id,
                    frozen_sequence,
                )
                .unwrap()
            })
            .collect();
        let intent = OperationIntent::new(OperationIntentParts {
            target: append.target().clone(),
            target_ref: "main".into(),
            start: Some(StartSnapshot {
                snapshot_id: source.current_snapshot_id().unwrap(),
                sequence_number: frozen_sequence,
            }),
            changes: FileChanges {
                added,
                removed: live.values().map(|e| e.frozen.clone()).collect(),
            },
            dependencies: vec![crate::commit::model::Dependency::RefUnchanged],
            isolation: IsolationLevel::Snapshot,
            shape: RequestShape::SnapshotProducing,
            summary: BTreeMap::new(),
            token: fixture.operation.token(),
        })
        .unwrap();
        let (after, _) = fixture
            .stage(
                source,
                &intent,
                &SelectedRewritePreparer {
                    files: SelectedRewriteFiles {
                        kind: SelectedRewriteKind::Data,
                        data_paths,
                        delete_paths: BTreeSet::new(),
                    },
                },
            )
            .await;
        assert_eq!(after.next_row_id(), 3);
        let snapshot = after.current_snapshot().unwrap();
        assert_eq!(snapshot.row_range(), Some((3, 0)));
        assert_eq!(snapshot.summary().operation, Operation::Replace);
        let mut inputs = ValidationInputs::new(&after, Some(snapshot.snapshot_id()), &attempt);
        let live = inputs.live_set().await.unwrap();
        assert_eq!(live.len(), 1);
        let facts = live.values().next().unwrap().frozen.facts();
        assert_eq!(facts.first_row_id, Some(0));
        assert_eq!(facts.data_sequence, Some(frozen_sequence));
        assert_eq!(facts.file_sequence, Some(snapshot.sequence_number()));
    }

    #[tokio::test]
    async fn preserved_row_ids_reject_untrusted_footer_and_inconsistent_explicit_markers() {
        let fixture = Fixture::new();
        let metadata = fixture.metadata(FormatVersion::V3);
        let append = fixture.intent(
            &metadata,
            "main",
            vec![
                AddedContent::new_logical_data(
                    data("file:///source.parquet", 3, Struct::empty()),
                    0,
                )
                .unwrap(),
            ],
        );
        let (source, _) = fixture
            .stage(
                metadata,
                &append,
                &crate::commit::fast_append::FastAppendPreparer,
            )
            .await;
        let original = real_writer_fragment(&source).await;
        for invalid in 0..15 {
            let mut stats = original.metrics().column_stats().unwrap().clone();
            match invalid {
                0 => {
                    stats.lower_bounds.remove(&ICEBERG_RESERVED_FIELD_ID_ROW_ID);
                }
                1 => {
                    stats.lower_bounds.insert(
                        ICEBERG_RESERVED_FIELD_ID_ROW_ID,
                        0i32.to_le_bytes().to_vec(),
                    );
                }
                2 => {
                    stats.lower_bounds.insert(
                        ICEBERG_RESERVED_FIELD_ID_ROW_ID,
                        (-1i64).to_le_bytes().to_vec(),
                    );
                }
                3 => {
                    stats
                        .null_value_counts
                        .insert(ICEBERG_RESERVED_FIELD_ID_ROW_ID, 1);
                }
                4 => {
                    stats
                        .null_value_counts
                        .remove(&ICEBERG_RESERVED_FIELD_ID_ROW_ID);
                }
                6 => {
                    stats
                        .null_value_counts
                        .insert(ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER, 1);
                }
                7 => {
                    stats
                        .null_value_counts
                        .remove(&ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER);
                }
                8 => {
                    stats
                        .lower_bounds
                        .remove(&ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER);
                }
                9 => {
                    stats.lower_bounds.insert(
                        ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
                        0i32.to_le_bytes().to_vec(),
                    );
                }
                10 => {
                    stats.lower_bounds.insert(
                        ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
                        (-1i64).to_le_bytes().to_vec(),
                    );
                }
                11 => {
                    stats
                        .upper_bounds
                        .remove(&ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER);
                }
                12 => {
                    stats.upper_bounds.insert(
                        ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
                        (-1i64).to_le_bytes().to_vec(),
                    );
                }
                13 => {
                    stats.lower_bounds.insert(
                        ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
                        2i64.to_le_bytes().to_vec(),
                    );
                }
                14 => {
                    stats.upper_bounds.insert(
                        ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
                        1i32.to_le_bytes().to_vec(),
                    );
                }
                _ => {}
            }
            let fragment = IcebergCommitFragment::data_file(
                IcebergDataFileArtifact::try_new(
                    original.path().into(),
                    crate::delete_file::IcebergFileFormat::Parquet,
                    original.partition().clone(),
                    IcebergArtifactMetrics::try_new(
                        2,
                        original.metrics().file_size_in_bytes(),
                        original.metrics().split_offsets().to_vec(),
                        Some(stats),
                    )
                    .unwrap(),
                    None,
                )
                .unwrap(),
            );
            let mut files = vec![written_file_from_fragment(&fragment, &source).unwrap()];
            if invalid == 5 {
                files[0].first_row_id = Some(99);
            }
            let before = files.clone();
            let validated = [ValidatedFragment {
                ordinal: WriteTargetOrdinal::try_new(0).unwrap(),
                fragment: &fragment,
            }];
            assert!(
                freeze_preserved_row_ids(&validated, &mut files).is_err(),
                "case {invalid}"
            );
            assert_eq!(files, before);
        }
        let validated = [ValidatedFragment {
            ordinal: WriteTargetOrdinal::try_new(0).unwrap(),
            fragment: &original,
        }];
        let mut files = vec![written_file_from_fragment(&original, &source).unwrap()];
        files[0].path.push_str("-different");
        assert!(freeze_preserved_row_ids(&validated, &mut files).is_err());
        let mut files = vec![written_file_from_fragment(&original, &source).unwrap()];
        files[0].first_row_id = Some(0);
        freeze_preserved_row_ids(&validated, &mut files).unwrap();
        assert_eq!(files[0].first_row_id, Some(0));
    }
}
