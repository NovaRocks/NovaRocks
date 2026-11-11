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

//! Exact data and position-delete facts for a declared target set.

#![allow(
    dead_code,
    reason = "RWM-6 consumes this scoped facts interface after switching the writer inputs."
)]

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::commit::manifest_sources::{LocatedEntry, SnapshotSources, duplicate_identity};
use crate::delete_semantics::{
    ConstructionFailure, DataFileFact, DataSequenceNumber, DeleteCandidateIndex, DeleteObservation,
    DeleteSemanticsError, FileMetrics, POSITION_FILE_PATH_FIELD_ID, ReadDomain, TypedPartition,
    validate_partition_binding,
};
use crate::iceberg::io::FileIO;
use crate::iceberg::spec::{DataContentType, ManifestContentType, ManifestFile, TableMetadata};
use crate::iceberg::{Error, ErrorKind, Result};
use crate::read_snapshot::{
    manifest_metrics_for_metadata, mint_read_domain, observed_raw_delete_entry,
};

use super::partition_pruning::may_contain;

/// The exact observation whose processed descriptor indexes locate the result.
/// T declares position semantics only. It has no equality completeness claim.
#[derive(Debug)]
pub(crate) struct RangeCompletion {
    pub(crate) domain: Arc<ReadDomain>,
    pub(crate) manifest_list: String,
    pub(crate) targets: BTreeMap<String, TypedPartition>,
    pub(crate) processed: Vec<ManifestFile>,
}

#[derive(Debug)]
pub(crate) struct TargetFacts {
    pub(crate) data: LocatedEntry,
    pub(crate) positions: Vec<LocatedEntry>,
}

#[derive(Debug)]
pub(crate) struct TargetObservation {
    pub(crate) files: BTreeMap<String, TargetFacts>,
    pub(crate) completion: RangeCompletion,
}

/// Resolve T using only a pinned metadata generation and admitted read IO.
/// Missing targets, mismatched facts and incomplete I/O all fail the observation.
pub(crate) async fn observe_targets(
    metadata: &TableMetadata,
    snapshot_id: i64,
    targets: BTreeMap<String, TypedPartition>,
    io: &FileIO,
    active: impl Fn() -> Result<()>,
) -> Result<TargetObservation> {
    active()?;
    let snapshot = metadata.snapshot_by_id(snapshot_id).ok_or_else(|| {
        invalid(format!(
            "Target snapshot {snapshot_id} is absent from metadata"
        ))
    })?;
    let schema = snapshot.schema(metadata)?;
    let domain = mint_read_domain(metadata, snapshot_id, &schema).map_err(invalid)?;
    let partition_bindings = domain
        .endpoint()
        .bind_partition_specs()
        .map_err(delete_error)?;
    let mut partitions_by_spec: BTreeMap<i32, Vec<&TypedPartition>> = BTreeMap::new();
    for partition in targets.values() {
        active()?;
        validate_partition_binding(&partition_bindings, partition).map_err(delete_error)?;
        partitions_by_spec
            .entry(partition.spec_id())
            .or_default()
            .push(partition);
    }
    let mut processed = Vec::new();
    let mut data = BTreeMap::new();
    let mut delete_entries = Vec::new();
    let mut delete_sources = BTreeMap::new();
    let mut delete_locations = BTreeMap::new();
    if !targets.is_empty() {
        let sources = SnapshotSources::load(metadata, snapshot, io, &active).await?;
        for source in sources.sources() {
            active()?;
            let descriptor = source.descriptor;
            let spec_id = descriptor.partition_spec_id;
            let Some(partitions) = partitions_by_spec.get(&spec_id) else {
                continue;
            };
            let spec = metadata
                .partition_spec_by_id(spec_id)
                .ok_or_else(|| invalid(format!("Unknown target partition spec {spec_id}")))?;
            if !partitions
                .iter()
                .any(|partition| may_contain(descriptor, spec, partition))
            {
                continue;
            }
            let partition_type = domain
                .endpoint()
                .partition_type(spec_id)
                .map_err(delete_error)?;
            let source_index = processed.len();
            let raw = source.load(io).await?;
            active()?;
            if raw.metadata().partition_spec != **spec {
                return Err(invalid(format!(
                    "Manifest {} partition spec differs from pinned metadata",
                    descriptor.manifest_path,
                )));
            }
            let mut positions = Vec::new();
            for bound in source.bind(&raw)? {
                active()?;
                let entry = bound?;
                if !entry.raw.is_alive() {
                    continue;
                }
                let file = entry.raw.data_file();
                match (descriptor.content, file.content_type()) {
                    (ManifestContentType::Data, DataContentType::Data) => {
                        let Some(expected_partition) = targets.get(file.file_path()) else {
                            continue;
                        };
                        let partition =
                            TypedPartition::bind_type(spec, &partition_type, file.partition())
                                .map_err(delete_error)?;
                        if &partition != expected_partition {
                            return Err(invalid(format!(
                                "Target data file {} differs from its pinned typed partition",
                                file.file_path(),
                            )));
                        }
                        let located = LocatedEntry::from_bound(source_index, &entry)?;
                        if data.contains_key(file.file_path()) {
                            return Err(duplicate_identity(located.frozen.identity()));
                        }
                        let sequence = entry.sequences.data_sequence.ok_or_else(|| {
                            invalid("Target data file has no resolved data sequence")
                        })?;
                        let fact = DataFileFact::try_new(
                            file.file_path(),
                            DataSequenceNumber::try_new(sequence).map_err(delete_error)?,
                            partition,
                            file.record_count(),
                            FileMetrics::default(),
                        )
                        .map_err(delete_error)?;
                        data.insert(file.file_path().to_owned(), (fact, located));
                    }
                    (ManifestContentType::Deletes, DataContentType::PositionDeletes) => {
                        let partition =
                            TypedPartition::bind_type(spec, &partition_type, file.partition())
                                .map_err(delete_error)?;
                        // Keep only the metrics that can infer an exact position target.
                        // Sequence/partition membership never uses other column statistics.
                        let metrics = manifest_metrics_for_metadata(file, &schema, &active)?;
                        let path_metrics = metrics
                            .fields()
                            .get(&POSITION_FILE_PATH_FIELD_ID)
                            .map(|field| (POSITION_FILE_PATH_FIELD_ID, field.clone()))
                            .into_iter()
                            .collect();
                        let raw_entry = observed_raw_delete_entry(
                            &entry,
                            raw.metadata().format_version,
                            &schema,
                            partition,
                            FileMetrics::new(path_metrics),
                        )
                        .map_err(invalid)?;
                        positions.push((Some(entry.ordinal), raw_entry));
                        delete_locations.insert(
                            (source_index, entry.ordinal),
                            LocatedEntry::from_bound(source_index, &entry)?,
                        );
                    }
                    (ManifestContentType::Deletes, DataContentType::EqualityDeletes) => {}
                    _ => {
                        return Err(invalid(format!(
                            "Manifest {} contains an entry of the wrong content type",
                            descriptor.manifest_path,
                        )));
                    }
                }
            }
            if !positions.is_empty() {
                let path: Arc<str> = Arc::from(descriptor.manifest_path.as_str());
                delete_sources.insert(Arc::clone(&path), source_index);
                delete_entries.push((path, positions));
            }
            processed.push(descriptor.clone());
        }
    }
    let observation = DeleteObservation::from_source_entries_checked(delete_entries, &active)
        .map_err(construction_error)?;
    let index = DeleteCandidateIndex::try_new_checked(domain.clone(), observation, &active)
        .map_err(construction_error)?;
    let mut files = BTreeMap::new();
    for path in targets.keys() {
        active()?;
        let (fact, data) = data.remove(path).ok_or_else(|| {
            invalid(format!(
                "Target data file {path} is absent from the declared snapshot range"
            ))
        })?;
        let set = index.for_data(&fact).map_err(delete_error)?;
        let positions = set
            .members()
            .map(|delete| {
                let provenance = delete
                    .provenance()
                    .ok_or_else(|| invalid("Observed position delete has no source path"))?;
                let source_index = *delete_sources
                    .get(&provenance.manifest_path)
                    .ok_or_else(|| invalid("Observed position delete source was not processed"))?;
                let ordinal = delete.physical_ordinal().ok_or_else(|| {
                    invalid("Observed position delete has no physical entry ordinal")
                })?;
                delete_locations
                    .get(&(source_index, ordinal))
                    .cloned()
                    .ok_or_else(|| invalid("Observed position delete has no frozen source entry"))
            })
            .collect::<Result<Vec<_>>>()?;
        files.insert(path.clone(), TargetFacts { data, positions });
    }
    active()?;
    Ok(TargetObservation {
        files,
        completion: RangeCompletion {
            domain,
            manifest_list: snapshot.manifest_list().to_owned(),
            targets,
            processed,
        },
    })
}

fn delete_error(error: DeleteSemanticsError) -> Error {
    invalid(error.message)
}

fn construction_error(error: ConstructionFailure<Error>) -> Error {
    match error {
        ConstructionFailure::Original(error) => error,
        ConstructionFailure::Semantic(error) => delete_error(error),
    }
}

fn invalid(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::DataInvalid, message)
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::iceberg::spec::{
        DataFile, DataFileBuilder, DataFileFormat, Datum, FieldSummary, FormatVersion, Literal,
        ManifestListWriter, ManifestWriterBuilder, NestedField, Operation, PartitionSpec,
        PrimitiveType, Schema, Snapshot, SortOrder, Struct, Summary, TableMetadataBuilder,
        Transform, Type,
    };

    const SNAPSHOT_ID: i64 = 21;
    const LIST_PATH: &str = "memory:///list.avro";
    const DATA_PATH: &str = "memory:///data.avro";
    const DELETE_PATH: &str = "memory:///delete.avro";
    const TARGET: &str = "memory:///target.parquet";

    #[derive(Clone, Copy)]
    enum Positions {
        Matching,
        WrongPartition,
        DuplicateDv,
    }

    fn partition(value: i32) -> Struct {
        Struct::from_iter([Some(Literal::int(value))])
    }

    fn data(path: &str, partition_value: i32) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.into())
            .file_format(DataFileFormat::Parquet)
            .partition(partition(partition_value))
            .record_count(5)
            .file_size_in_bytes(123)
            .build()
            .unwrap()
    }

    fn position(path: &str, target: &str, value: i32, dv_offset: Option<i64>) -> DataFile {
        let mut builder = DataFileBuilder::default();
        builder
            .content(DataContentType::PositionDeletes)
            .file_path(path.into())
            .partition(partition(value))
            .record_count(2)
            .file_size_in_bytes(111)
            .referenced_data_file(Some(target.into()));
        match dv_offset {
            Some(offset) => {
                builder
                    .file_format(DataFileFormat::Puffin)
                    .content_offset(Some(offset))
                    .content_size_in_bytes(Some(9));
            }
            None => {
                builder.file_format(DataFileFormat::Parquet);
            }
        }
        builder.build().unwrap()
    }

    async fn fixture(positions: Positions, unknown_summary: bool) -> (TableMetadata, FileIO) {
        let io = FileIO::new_with_memory();
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "p",
                Type::Primitive(PrimitiveType::Int),
            ))])
            .build()
            .unwrap();
        let spec = PartitionSpec::builder(Arc::new(schema.clone()))
            .with_spec_id(0)
            .add_partition_field("p", "p_key", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();
        let metadata = TableMetadataBuilder::new(
            schema,
            spec.into_unbound(),
            SortOrder::unsorted_order(),
            "memory:///table".into(),
            FormatVersion::V3,
            Default::default(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        let spec = metadata.default_partition_spec().as_ref().clone();
        let other_spec = PartitionSpec::builder(metadata.current_schema().clone())
            .with_spec_id(1)
            .add_partition_field("p", "p_bucket", Transform::Bucket(8))
            .unwrap()
            .build()
            .unwrap();
        let metadata = metadata
            .into_builder(None)
            .add_partition_spec(other_spec.into_unbound())
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let writer = |path| {
            ManifestWriterBuilder::new(
                io.new_output(path).unwrap(),
                Some(SNAPSHOT_ID),
                metadata.current_schema().clone(),
                spec.clone(),
            )
        };

        let mut data_writer = writer(DATA_PATH).build_v3_data();
        data_writer
            .add_file(data("memory:///unrequested.parquet", 10), 3)
            .unwrap();
        data_writer.add_file(data(TARGET, 10), 3).unwrap();
        let data = data_writer.write_manifest_file().await.unwrap();

        let mut delete_writer = writer(DELETE_PATH).build_v3_deletes();
        delete_writer
            .add_file(position("memory:///other.pos", "other", 10, None), 4)
            .unwrap();
        match positions {
            Positions::Matching => delete_writer
                .add_file(position("memory:///target.pos", TARGET, 10, None), 4)
                .unwrap(),
            Positions::WrongPartition => delete_writer
                .add_file(position("memory:///target.pos", TARGET, 11, None), 4)
                .unwrap(),
            Positions::DuplicateDv => {
                delete_writer
                    .add_file(position("memory:///dv.puffin", TARGET, 10, Some(4)), 4)
                    .unwrap();
                delete_writer
                    .add_file(position("memory:///dv.puffin", TARGET, 10, Some(40)), 4)
                    .unwrap();
            }
        }
        let delete = delete_writer.write_manifest_file().await.unwrap();

        // These paths are absent in FileIO. Successful target observation
        // therefore proves the exact spec and inclusive summary eliminated them.
        let bound = Datum::int(99).to_bytes().unwrap();
        let missing_partition = ManifestFile {
            manifest_path: "memory:///summary-pruned.avro".into(),
            partitions: (!unknown_summary).then(|| {
                vec![FieldSummary {
                    contains_null: false,
                    contains_nan: Some(false),
                    lower_bound: Some(bound.clone()),
                    upper_bound: Some(bound),
                }]
            }),
            ..data.clone()
        };
        let missing_spec = ManifestFile {
            manifest_path: "memory:///spec-pruned.avro".into(),
            partition_spec_id: 1,
            ..delete.clone()
        };
        let mut list = ManifestListWriter::v3(
            io.new_output(LIST_PATH).unwrap().writer().await.unwrap(),
            SNAPSHOT_ID,
            None,
            4,
            Some(0),
        );
        list.add_manifests([data, missing_partition, missing_spec, delete].into_iter())
            .unwrap();
        let added = list.next_row_id().unwrap();
        list.close().await.unwrap();
        let snapshot = Snapshot::builder()
            .with_snapshot_id(SNAPSHOT_ID)
            .with_sequence_number(4)
            .with_timestamp_ms(metadata.last_updated_ms() + 1)
            .with_manifest_list(LIST_PATH)
            .with_summary(Summary {
                operation: Operation::Overwrite,
                additional_properties: Default::default(),
            })
            .with_schema_id(metadata.current_schema_id())
            .with_row_range(0, added)
            .build();
        (
            metadata
                .into_builder(None)
                .add_snapshot(snapshot)
                .unwrap()
                .build()
                .unwrap()
                .metadata,
            io,
        )
    }

    async fn query(metadata: &TableMetadata, io: &FileIO) -> Result<TargetObservation> {
        let partition = TypedPartition::bind(
            metadata.partition_spec_by_id(0).unwrap(),
            metadata.current_schema(),
            &partition(10),
        )
        .unwrap();
        observe_targets(
            metadata,
            SNAPSHOT_ID,
            BTreeMap::from([(TARGET.to_owned(), partition)]),
            io,
            || Ok(()),
        )
        .await
    }

    #[tokio::test]
    async fn completion_only_covers_processed_sources_and_keeps_original_ordinals() {
        let (metadata, io) = fixture(Positions::Matching, false).await;
        let result = query(&metadata, &io).await.unwrap();
        assert_eq!(
            result.completion.domain.endpoint().snapshot_id(),
            SNAPSHOT_ID
        );
        assert_eq!(result.completion.manifest_list, LIST_PATH);
        assert_eq!(
            result
                .completion
                .targets
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            [TARGET]
        );
        assert_eq!(
            result
                .completion
                .processed
                .iter()
                .map(|m| m.manifest_path.as_str())
                .collect::<Vec<_>>(),
            [DATA_PATH, DELETE_PATH],
        );
        let target = &result.files[TARGET];
        assert_eq!(target.data.ordinal, 1);
        assert_eq!(target.data.source_index, 0);
        assert_eq!(target.data.frozen.identity().path(), TARGET);
        assert_eq!(target.data.frozen.facts().record_count, 5);
        assert_eq!(target.data.frozen.facts().data_sequence, Some(3));
        assert_eq!(target.data.frozen.facts().first_row_id, Some(5));
        assert_eq!(
            target
                .positions
                .iter()
                .map(|f| (f.source_index, f.ordinal, f.frozen.identity().path()))
                .collect::<Vec<_>>(),
            [(1, 1, "memory:///target.pos")],
        );
    }

    #[tokio::test]
    async fn unknown_summary_does_not_hide_an_unreadable_candidate() {
        let (metadata, io) = fixture(Positions::Matching, true).await;
        assert!(query(&metadata, &io).await.is_err());
    }

    #[tokio::test]
    async fn observed_exact_partition_contradiction_and_duplicate_dv_fail_the_scope() {
        for (positions, expected) in [
            (Positions::WrongPartition, "partition differs from target"),
            (Positions::DuplicateDv, "multiple live deletion vector"),
        ] {
            let (metadata, io) = fixture(positions, false).await;
            let error = query(&metadata, &io).await.unwrap_err();
            assert_eq!(error.kind(), ErrorKind::DataInvalid);
            assert!(error.to_string().contains(expected), "{error}");
        }
    }
}
