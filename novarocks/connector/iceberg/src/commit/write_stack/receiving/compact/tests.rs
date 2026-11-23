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

//! The original provider codec's representative artifacts preserve both the
//! exact decoded payment and the frozen manifest projection.

use super::*;
use crate::commit::report::IcebergColumnStats;
use crate::commit::write_stack::codec::{
    CompactFragmentCodec, IcebergWriteFragmentDecoder, IcebergWriteFragmentEncoder,
};
use crate::commit::write_stack::control::publication::written_file_with_historical_partition;
use crate::commit::write_stack::domain::{
    IcebergArtifactMetrics, IcebergContentRange, IcebergCreationReceipt, IcebergDataFileArtifact,
    IcebergDeletionVectorArtifact, IcebergEqualityDeleteFileArtifact,
    IcebergPositionDeleteFileArtifact,
};
use crate::commit::write_stack::runtime::build_write_adapter;
use crate::commit::write_stack::test_support::staged_table_metadata;
use crate::delete_file::IcebergFileFormat;
use crate::write_descriptor::IcebergPartitionDescriptor;
use novarocks_spi::connector::{
    CatalogHandle, CatalogVersion, ConnectorInstanceDescriptor, ConnectorInstanceId,
    ConnectorPayloadByteBudget, ConnectorProviderId, ConnectorWriteFragmentWireDecoder,
    ConnectorWriteFragmentWireEncoder,
};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Window {
    live: usize,
    limit: usize,
    reservations: Vec<usize>,
    refunds: Vec<usize>,
}

struct Receipt(Arc<Mutex<Window>>, usize);

impl Drop for Receipt {
    fn drop(&mut self) {
        let mut window = self.0.lock().unwrap();
        window.live -= self.1;
        window.refunds.push(self.1);
    }
}

struct Author(Arc<Mutex<Window>>);

impl ConnectorPayloadByteBudget for Author {
    fn reserve_bytes(
        &self,
        bytes: usize,
    ) -> Result<ConnectorPayloadRetentionGuard, ConnectorError> {
        let mut window = self.0.lock().unwrap();
        let Some(live) = window
            .live
            .checked_add(bytes)
            .filter(|live| *live <= window.limit)
        else {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "original fixture result window is full",
            ));
        };
        window.live = live;
        window.reservations.push(bytes);
        Ok(ConnectorPayloadRetentionGuard::new(Receipt(
            Arc::clone(&self.0),
            bytes,
        )))
    }
}

fn window() -> (Arc<Mutex<Window>>, ConnectorPayloadRetentionGuard) {
    let window = Arc::new(Mutex::new(Window {
        limit: usize::MAX,
        ..Window::default()
    }));
    let author = ConnectorPayloadRetentionGuard::from_byte_budget(Arc::new(Author(window.clone())));
    (window, author)
}

fn adapter(version: u8) -> IcebergWriteAdapter {
    let instance = ConnectorInstanceId::parse("compact-iceberg").unwrap();
    build_write_adapter(
        ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse(crate::PROVIDER_ID).unwrap(),
            instance_id: instance.clone(),
        },
        CatalogHandle::new(instance, CatalogVersion::from_bytes([version; 32])),
    )
}

fn partition() -> IcebergArtifactPartition {
    IcebergArtifactPartition::try_new(
        String::new(),
        String::new(),
        0,
        IcebergPartitionDescriptor { values: Vec::new() },
    )
    .unwrap()
}

fn metrics(rows: u64, bytes: u64) -> IcebergArtifactMetrics {
    IcebergArtifactMetrics::try_new(
        rows,
        bytes,
        vec![4, 16],
        Some(IcebergColumnStats {
            column_sizes: BTreeMap::from([(1, 64)]),
            value_counts: BTreeMap::from([(1, rows as i64)]),
            null_value_counts: BTreeMap::from([(1, 0)]),
            lower_bounds: BTreeMap::from([(1, 1_i64.to_le_bytes().to_vec())]),
            upper_bounds: BTreeMap::from([(1, 17_i64.to_le_bytes().to_vec())]),
            ..IcebergColumnStats::default()
        }),
    )
    .unwrap()
}

fn data() -> IcebergCommitFragment {
    IcebergCommitFragment::data_file(
        IcebergDataFileArtifact::try_new(
            "s3://bucket/data/abc.parquet".into(),
            IcebergFileFormat::Parquet,
            partition(),
            metrics(10, 1024),
            None,
        )
        .unwrap(),
    )
}

fn placement() -> ConnectorWriterPhysicalContext {
    ConnectorWriterPhysicalContext::new([3; 16], 7, [1; 16], 2, 0)
}

fn decode(
    adapter: &IcebergWriteAdapter,
    fragment: IcebergCommitFragment,
    author: &ConnectorPayloadRetentionGuard,
) -> ConnectorCommitFragment {
    let bytes = IcebergWriteFragmentEncoder::new(adapter.clone())
        .encode_commit_fragment_payload(&adapter.wrap_commit_fragment(fragment))
        .unwrap();
    IcebergWriteFragmentDecoder::new(adapter.clone())
        .decode_commit_fragment_payload(&bytes, author)
        .unwrap()
}

#[test]
fn original_decode_receipt_moves_to_compact_entry_and_refunds_on_last_use() {
    let adapter = adapter(1);
    let (window, author) = window();
    let mut original = Some(decode(&adapter, data(), &author));
    let original_bytes = window.lock().unwrap().live;
    let entry = CompactEntry::from_arrival(&adapter, &mut original, 0, &author).unwrap();
    assert!(original.is_none());
    assert_eq!(entry.receipt.original_bytes, original_bytes);
    assert_eq!(entry.receipt.additional_bytes, 0);
    {
        let window = window.lock().unwrap();
        assert_eq!(window.reservations[0], original_bytes);
        assert_eq!(window.live, original_bytes);
        assert_eq!(window.refunds, window.reservations[1..]);
    }
    assert!(!entry.addition.as_ref().unwrap().payload.is_empty());
    drop(entry);
    assert_eq!(window.lock().unwrap().live, 0);
}

#[test]
fn compact_top_up_refusal_leaves_original_paid_fragment_available_for_abort() {
    let original_adapter = adapter(1);
    let (window, author) = window();
    let mut original = Some(decode(&original_adapter, data(), &author));
    let original_bytes = window.lock().unwrap().live;

    // Force growth using checked projected index occupancy, and leave the
    // original window one byte short of the exact amount.
    let metered_fragment = original_adapter
        .commit_fragment(original.as_ref().unwrap())
        .unwrap();
    let index_bytes = original_bytes;
    let retained = retained_entry_node_bytes()
        + metered_fragment.compact_payload_bytes().unwrap()
        + object_path_bytes(metered_fragment.path()).unwrap()
        + index_bytes;
    window.lock().unwrap().limit = retained - 1;
    let result = CompactEntry::from_arrival(&original_adapter, &mut original, index_bytes, &author);
    assert_eq!(
        result.err().unwrap().kind(),
        ConnectorErrorKind::ResourceExhausted
    );
    assert!(original.is_some());
    assert_eq!(window.lock().unwrap().live, original_bytes);

    let wrong_generation = adapter(2);
    let result = CompactEntry::from_arrival(&wrong_generation, &mut original, index_bytes, &author);
    assert_eq!(
        result.err().unwrap().kind(),
        ConnectorErrorKind::InvalidRequest
    );
    assert!(original.is_some());
    assert_eq!(window.lock().unwrap().live, original_bytes);

    let peak = original_bytes + retained - retained_entry_node_bytes();
    window.lock().unwrap().limit = peak - 1;
    let result = CompactEntry::from_arrival(&original_adapter, &mut original, index_bytes, &author);
    assert_eq!(
        result.err().unwrap().kind(),
        ConnectorErrorKind::ResourceExhausted
    );
    assert!(original.is_some());
    assert_eq!(window.lock().unwrap().live, original_bytes);
    // Index backing coexists with the original when the borrowed delta
    // becomes owned. Only the retained delta remains paid after the move.
    window.lock().unwrap().limit = peak;
    let compact =
        CompactEntry::from_arrival(&original_adapter, &mut original, index_bytes, &author).unwrap();
    assert!(original.is_none());
    assert_eq!(compact.receipt.additional_bytes, retained - original_bytes);
    assert_eq!(window.lock().unwrap().live, retained);
    drop(compact);
    assert_eq!(window.lock().unwrap().live, 0);
}

#[test]
fn paid_projection_consumes_only_its_exact_original_fragment() {
    let adapter = adapter(1);
    let (window, author) = window();
    let first = decode(&adapter, data(), &author);
    let mut other = Some(decode(&adapter, data(), &author));
    let original = window.lock().unwrap().live;
    let mut preparation = CompactEntry::prepare(&adapter, &first, 0, &author, None).unwrap();
    assert!(window.lock().unwrap().live > original);
    let result = preparation.finish(&adapter, &mut other);
    drop(preparation);
    assert_eq!(
        result.err().unwrap().kind(),
        ConnectorErrorKind::InvalidRequest
    );
    assert!(other.is_some());
    // Only the discarded preparation was refunded. The two observations are
    // still original paid fragments even though every domain field agrees.
    assert_eq!(window.lock().unwrap().live, original);
    drop((first, other));
    assert_eq!(window.lock().unwrap().live, 0);
}

#[test]
fn compact_projection_reuses_null_and_value_partition_backing() {
    use crate::write_descriptor::IcebergPartitionValueDescriptor;
    let adapter = adapter(1);
    let (window, author) = window();
    let expected = IcebergArtifactPartition::try_new(
        "p=42/q=__HIVE_DEFAULT_PARTITION__".into(),
        "q".into(),
        7,
        IcebergPartitionDescriptor {
            values: vec![
                IcebergPartitionValueDescriptor {
                    is_null: false,
                    datum_bytes: Some(42_i64.to_le_bytes().to_vec()),
                },
                IcebergPartitionValueDescriptor {
                    is_null: true,
                    datum_bytes: None,
                },
            ],
        },
    )
    .unwrap();
    let fragment = IcebergCommitFragment::data_file(
        IcebergDataFileArtifact::try_new(
            "s3://bucket/data/partitioned.parquet".into(),
            IcebergFileFormat::Parquet,
            expected.clone(),
            metrics(6, 1024),
            None,
        )
        .unwrap(),
    );
    let mut original = Some(decode(&adapter, fragment, &author));
    let entry = CompactEntry::from_arrival(&adapter, &mut original, 0, &author).unwrap();
    let addition = entry.addition.as_ref().unwrap();
    assert_eq!(addition.partition_spec_id, 7);
    let (decoded, temporary) = CompactFragmentCodec::for_adapter(&adapter)
        .decode(&addition.payload, &author)
        .unwrap();
    assert_eq!(decoded.partition(), Some(&expected));
    drop(decoded);
    drop(temporary);
    drop(entry);
    assert_eq!(window.lock().unwrap().live, 0);
}

#[test]
fn compact_equality_delete_keeps_the_field_ids_and_counts_only_keys() {
    let adapter = adapter(1);
    let (window, author) = window();
    let path = "s3://bucket/data/equality-delete.parquet";
    let fragment = IcebergCommitFragment::equality_delete_file(
        IcebergEqualityDeleteFileArtifact::try_new(
            path.into(),
            partition(),
            metrics(7, 256),
            vec![1, 4],
        )
        .unwrap(),
    );
    let mut original = Some(decode(&adapter, fragment, &author));
    let entry = CompactEntry::from_arrival(&adapter, &mut original, 0, &author).unwrap();
    let addition = entry.addition.as_ref().unwrap();
    assert_eq!(addition.kind, AdditionKind::EqualityDelete);
    assert_eq!(addition.data_sequence, SeqField::Inherit);
    assert!(entry.delete_effect.is_none());
    assert_eq!(
        entry.counts,
        AddedCounters {
            equality_deletes: PhysicalAdditionCounts {
                files: 1,
                rows: 7,
                bytes: 256
            },
            ..AddedCounters::default()
        }
    );
    let (decoded, temporary) = CompactFragmentCodec::for_adapter(&adapter)
        .decode(&addition.payload, &author)
        .unwrap();
    let IcebergCommitArtifact::EqualityDeleteFile(decoded) = decoded.artifact() else {
        panic!("the compact addition must remain an equality delete");
    };
    assert_eq!(decoded.equality_field_ids(), &[1, 4]);
    assert_eq!(decoded.partition(), &partition());
    assert_eq!(decoded.metrics(), &metrics(7, 256));
    drop(temporary);
    drop(entry);
    assert_eq!(window.lock().unwrap().live, 0);
}

#[test]
fn compact_files_use_the_same_frozen_partition_and_statistics_author() {
    let adapter = adapter(1);
    let (window, author) = window();
    let e = staged_table_metadata();
    let codec = CompactFragmentCodec::for_adapter(&adapter);
    let v2 = IcebergCommitFragment::position_delete_file(
        IcebergPositionDeleteFileArtifact::try_new(
            "s3://bucket/data/old-delete.parquet".into(),
            partition(),
            metrics(4, 128),
            "s3://bucket/data/abc.parquet".into(),
            Vec::new(),
        )
        .unwrap(),
    );
    let v3 = IcebergCommitFragment::deletion_vector(
        IcebergDeletionVectorArtifact::try_new(
            "s3://bucket/data/shared.puffin".into(),
            partition(),
            metrics(8, 256),
            "s3://bucket/data/abc.parquet".into(),
            IcebergContentRange::try_new(16, 64).unwrap(),
            8,
            Vec::new(),
        )
        .unwrap(),
    );
    let mut counts = AddedCounters::default();
    for original in [data(), v2, v3] {
        let expected = written_file_with_historical_partition(&original, &e, None).unwrap();
        let expected = crate::commit::data_file::from_written_file(&expected).unwrap();
        let mut neutral = Some(decode(&adapter, original, &author));
        let compact = CompactEntry::from_arrival(&adapter, &mut neutral, 0, &author).unwrap();
        let addition = compact.addition.as_ref().unwrap();
        let (decoded, temporary) = codec.decode(&addition.payload, &author).unwrap();
        assert!(decoded.creation().is_none());
        assert_eq!(addition.object_path.as_ref(), expected.file_path());
        assert_eq!(addition.partition_spec_id, e.default_partition_spec_id());
        let actual = written_file_with_historical_partition(&decoded, &e, None).unwrap();
        let actual = crate::commit::data_file::from_written_file(&actual).unwrap();
        assert_eq!(actual, expected);
        drop(decoded);
        drop(temporary);
        counts = counts.checked_add(compact.counts).unwrap();
    }
    assert_eq!(
        counts.data,
        PhysicalAdditionCounts {
            files: 1,
            rows: 10,
            bytes: 1024
        }
    );
    assert_eq!(
        counts.position_deletes,
        PhysicalAdditionCounts {
            files: 2,
            rows: 12,
            bytes: 192
        }
    );
    assert_eq!(counts.newly_deleted_rows, 0);
    assert_eq!(window.lock().unwrap().live, 0);
}

fn target(path: &str) -> IcebergDeleteTargetLocator {
    IcebergDeleteTargetLocator::try_new(
        "s3://bucket/metadata/snap-61.avro".into(),
        "s3://bucket/metadata/manifest.avro".into(),
        0,
        2,
        61,
        path.into(),
        80,
    )
    .unwrap()
}

#[test]
fn final_delete_keeps_only_the_unshared_effect_and_the_admitted_addition() {
    let adapter = adapter(1);
    let (window, author) = window();
    let target_path = "s3://bucket/data/target.parquet";
    let old = EntryIdentity::DeleteFile {
        path: "s3://bucket/data/old.parquet".into(),
    };
    let vector = IcebergDeletionVectorArtifact::try_new(
        "s3://bucket/data/shared.puffin".into(),
        partition(),
        metrics(8, 256),
        target_path.into(),
        IcebergContentRange::try_new(16, 64).unwrap(),
        8,
        vec![old.clone()],
    )
    .unwrap();
    let fragment = IcebergCommitFragment::final_delete(
        IcebergFinalDeleteEffect::try_new(
            target(target_path),
            3,
            IcebergFinalDeleteKind::ReplacePositions(Box::new(vector)),
        )
        .unwrap(),
    )
    .try_with_creation(
        IcebergCreationReceipt::try_new(placement(), [7; 16], [6; 32], 256, [5; 32]).unwrap(),
    )
    .unwrap();
    let mut original = Some(decode(&adapter, fragment, &author));
    let compact = CompactEntry::from_arrival(&adapter, &mut original, 0, &author).unwrap();
    let addition = compact.addition.as_ref().unwrap();
    assert_eq!(addition.writer, Some(placement()));
    assert_eq!(addition.kind, AdditionKind::PositionDelete);
    let effect = compact.delete_effect.as_ref().unwrap();
    assert_eq!(effect.target.data_file_path(), target_path);
    assert_eq!(effect.newly_deleted_rows, 3);
    let CompactDeleteKind::ReplacePositions {
        merged_old_references,
        ..
    } = &effect.kind
    else {
        panic!("the compact replacement must preserve its logical delete effect");
    };
    assert_eq!(merged_old_references, std::slice::from_ref(&old));
    let (decoded, temporary) = CompactFragmentCodec::for_adapter(&adapter)
        .decode(&addition.payload, &author)
        .unwrap();
    let IcebergCommitArtifact::DeletionVector(vector) = decoded.artifact() else {
        panic!("the compact addition contains only the new DV");
    };
    assert_eq!(vector.referenced_data_file(), target_path);
    assert!(vector.merged_old_references().is_empty());
    assert!(decoded.creation().is_none());
    drop(decoded);
    drop(temporary);

    let removed_path = "s3://bucket/data/removed.parquet";
    let remove = IcebergCommitFragment::final_delete(
        IcebergFinalDeleteEffect::try_new(
            target(removed_path),
            8,
            IcebergFinalDeleteKind::RemoveDataFile {
                partition: partition(),
                old_exclusive_deletes: vec![old.clone()].into_boxed_slice(),
            },
        )
        .unwrap(),
    );
    let mut original = Some(decode(&adapter, remove, &author));
    let removed = CompactEntry::from_arrival(&adapter, &mut original, 0, &author).unwrap();
    assert!(removed.addition.is_none());
    let effect = removed.delete_effect.as_ref().unwrap();
    assert_eq!(effect.target.data_file_path(), removed_path);
    assert_eq!(effect.newly_deleted_rows, 8);
    let CompactDeleteKind::RemoveDataFile {
        old_exclusive_deletes,
        ..
    } = &effect.kind
    else {
        panic!("pure removal must retain its only original effect");
    };
    assert_eq!(old_exclusive_deletes.as_ref(), &[old]);
    let total = compact.counts.checked_add(removed.counts).unwrap();
    assert_eq!(total.position_deletes.files, 1);
    assert_eq!(total.position_deletes.rows, 8);
    assert_eq!(total.position_deletes.bytes, 64);
    assert_eq!(total.newly_deleted_rows, 11);
    drop((compact, removed));
    assert_eq!(window.lock().unwrap().live, 0);
}
