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

use super::*;
use crate::iceberg::spec::{
    DataFile, DataFileBuilder, DataFileFormat, Datum, FieldSummary, FormatVersion, Literal,
    ManifestListWriter, ManifestWriterBuilder, NestedField, Operation, PartitionSpec,
    PrimitiveType, Schema, Snapshot, SortOrder, Struct, Summary, TableMetadataBuilder, Transform,
    Type,
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
    DuplicateDvAcrossSources,
    DvOverParquet,
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
        Positions::Matching | Positions::DvOverParquet => delete_writer
            .add_file(position("memory:///target.pos", TARGET, 10, None), 4)
            .unwrap(),
        Positions::WrongPartition => delete_writer
            .add_file(position("memory:///target.pos", TARGET, 11, None), 4)
            .unwrap(),
        Positions::DuplicateDvAcrossSources => delete_writer
            .add_file(position("memory:///dv.puffin", TARGET, 10, Some(4)), 4)
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
    let extra_delete = match positions {
        Positions::DuplicateDvAcrossSources | Positions::DvOverParquet => {
            let mut writer = writer("memory:///extra-delete.avro").build_v3_deletes();
            writer
                .add_file(position("memory:///dv.puffin", TARGET, 10, Some(40)), 4)
                .unwrap();
            Some(writer.write_manifest_file().await.unwrap())
        }
        _ => None,
    };

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
    list.add_manifests(
        [data, missing_partition, missing_spec, delete]
            .into_iter()
            .chain(extra_delete),
    )
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
            .values()
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
        [(3, 1, "memory:///target.pos")],
    );
}

struct AccountedBytes(Arc<std::sync::atomic::AtomicU64>);

struct Receipt {
    used: Arc<std::sync::atomic::AtomicU64>,
    bytes: u64,
}

impl novarocks_spi::connector::ConnectorResourceLedger for AccountedBytes {
    fn checkpoint(
        &self,
    ) -> std::result::Result<
        novarocks_spi::connector::ConnectorResourceCheckpoint,
        novarocks_spi::connector::ConnectorError,
    > {
        Ok(novarocks_spi::connector::ConnectorResourceCheckpoint::new(
            0,
        ))
    }

    fn try_reserve(
        &self,
        class: ConnectorResourceClass,
        bytes: u64,
    ) -> std::result::Result<
        Box<dyn novarocks_spi::connector::ConnectorReservationBackend>,
        novarocks_spi::connector::ConnectorError,
    > {
        assert_eq!(class, ConnectorResourceClass::WriterState);
        self.0
            .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
        Ok(Box::new(Receipt {
            used: self.0.clone(),
            bytes,
        }))
    }
}

impl novarocks_spi::connector::ConnectorReservationBackend for Receipt {
    fn bytes(&self) -> u64 {
        self.bytes
    }

    fn try_grow(
        &mut self,
        additional: u64,
    ) -> std::result::Result<(), novarocks_spi::connector::ConnectorError> {
        self.used
            .fetch_add(additional, std::sync::atomic::Ordering::Relaxed);
        self.bytes += additional;
        Ok(())
    }

    fn shrink_to(
        &mut self,
        bytes: u64,
    ) -> std::result::Result<(), novarocks_spi::connector::ConnectorError> {
        self.used
            .fetch_sub(self.bytes - bytes, std::sync::atomic::Ordering::Relaxed);
        self.bytes = bytes;
        Ok(())
    }
}

impl Drop for Receipt {
    fn drop(&mut self) {
        self.used
            .fetch_sub(self.bytes, std::sync::atomic::Ordering::Relaxed);
    }
}

#[tokio::test]
async fn position_composite_prefers_dv_from_an_independent_source() {
    let (metadata, io) = fixture(Positions::DvOverParquet, false).await;
    let metadata = Arc::new(metadata);
    let typed = TypedPartition::bind(
        metadata.partition_spec_by_id(0).unwrap(),
        metadata.current_schema(),
        &partition(10),
    )
    .unwrap();
    let mut scope = ManifestReadScope::new(metadata, SNAPSHOT_ID).unwrap();
    let observation = scope
        .observe(BTreeMap::from([(TARGET.to_owned(), typed)]), &io, || Ok(()))
        .await
        .unwrap();
    let physical = scope
        .live_position_entries()
        .map(|entry| entry.frozen.identity())
        .collect::<Vec<_>>();
    assert!(physical.iter().any(|identity| matches!(
        identity,
        crate::commit::model::EntryIdentity::DeleteFile { .. }
    )));
    assert!(physical.iter().any(|identity| matches!(
        identity,
        crate::commit::model::EntryIdentity::DeletionVector { .. }
    )));
    let [position] = observation.files[TARGET].positions.as_slice() else {
        panic!("DV must suppress the independent positional source");
    };
    assert_eq!(position.entry.source_index, 4);
    assert!(matches!(
        position.delete().address(),
        crate::delete_semantics::DeleteContentAddress::PuffinBlob { offset: 40, .. }
    ));
}

#[tokio::test]
async fn warm_scope_reuses_bound_and_composite_facts_but_rebind_keeps_only_raw() {
    let (metadata, io) = fixture(Positions::Matching, false).await;
    let metadata = Arc::new(metadata);
    let typed = TypedPartition::bind(
        metadata.partition_spec_by_id(0).unwrap(),
        metadata.current_schema(),
        &partition(10),
    )
    .unwrap();
    let targets = || BTreeMap::from([(TARGET.to_owned(), typed.clone())]);
    let mut scope = ManifestReadScope::new(metadata.clone(), SNAPSHOT_ID).unwrap();
    let used = Arc::new(std::sync::atomic::AtomicU64::new(0));
    scope.resources = Some(ConnectorExecutionResources::from_admitted_ledger(Arc::new(
        AccountedBytes(used.clone()),
    )));
    let first = scope.observe(targets(), &io, || Ok(())).await.unwrap();
    let warm = scope.observe(targets(), &io, || Ok(())).await.unwrap();
    assert!(Arc::ptr_eq(
        &first.completion.domain,
        &warm.completion.domain
    ));
    assert_eq!(scope.composites.len(), 1);
    let first_source = &first.files[TARGET].positions[0]._source;
    let warm_source = &warm.files[TARGET].positions[0]._source;
    assert!(Arc::ptr_eq(first_source, warm_source));

    let mut rebound_scope = scope.rebind(metadata, SNAPSHOT_ID).unwrap();
    let rebound = rebound_scope
        .observe(targets(), &io, || Ok(()))
        .await
        .unwrap();
    let rebound_source = &rebound.files[TARGET].positions[0]._source;
    assert!(!Arc::ptr_eq(
        &first.completion.domain,
        &rebound.completion.domain
    ));
    assert!(!Arc::ptr_eq(first_source, rebound_source));
    assert!(Arc::ptr_eq(&first_source.raw, &rebound_source.raw));

    let locator = first.files[TARGET].data_locator(&first.completion).unwrap();
    let previous = Arc::downgrade(&first_source.raw);
    let previous_domain = first.completion.domain.clone();
    scope.evict();
    assert!(previous.upgrade().is_some());
    assert!(used.load(std::sync::atomic::Ordering::Relaxed) > 0);
    drop(first);
    drop(warm);
    drop(rebound);
    drop(rebound_scope);
    scope.evict();
    assert!(previous.upgrade().is_none());
    assert_eq!(used.load(std::sync::atomic::Ordering::Relaxed), 0);
    let cold = scope.observe(targets(), &io, || Ok(())).await.unwrap();
    assert!(Arc::ptr_eq(&previous_domain, &cold.completion.domain));
    assert_eq!(
        cold.files[TARGET].data_locator(&cold.completion).unwrap(),
        locator
    );
    let sources = scope.snapshot_sources().unwrap();
    assert_eq!(sources.source_index_for_list_ordinal(3), Some(3));
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
        (
            Positions::DuplicateDvAcrossSources,
            "multiple DVs for one target",
        ),
    ] {
        let (metadata, io) = fixture(positions, false).await;
        let error = query(&metadata, &io).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert!(error.to_string().contains(expected), "{error}");
    }
}
