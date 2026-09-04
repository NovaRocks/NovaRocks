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

//! Finds each column's most recent NDV by walking a snapshot's ancestry.
//! Design: ADR-0081 (docs/adr/ADR-0081-statistics-are-versioned-and-may-be-stale.md)
//!
//! Statistics are published against the snapshot they were measured on, and the
//! table keeps moving afterwards, so the newest statistics file is rarely the
//! one that covers the column you asked about. The search is therefore **per
//! column**: each field id independently takes the first ancestor that has a
//! sketch for it.
//!
//! Taking the first ancestor with *any* statistics file — what Trino and
//! StarRocks do — would let a recent single-column ANALYZE mask complete
//! statistics published one snapshot earlier. Every other column would report
//! as missing while its data sat one hop further back.

use std::collections::{BTreeSet, HashMap};

use crate::iceberg::io::FileIO;
use crate::iceberg::spec::TableMetadata;
use crate::stats_loader::StatsLoader;

/// How far back the walk looks before giving up.
///
/// Deliberately bounded even though the walk is metadata-only.
const MAX_STATISTICS_ANCESTRY_STEPS: usize = 64;

/// One column's NDV together with the snapshot it was measured on.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct AncestorNdv {
    pub basis_snapshot_id: i64,
    pub ndv: f64,
}

/// Resolves the nearest NDV for each requested field id.
///
/// Field ids with no sketch anywhere in the walked ancestry are simply absent
/// from the result: an unanalyzed column is missing a statistic, not an error.
/// Corrupt metadata stops the walk conservatively. The ordinary optimizer path
/// never opens a Puffin body; `ndv` is read from registered blob metadata.
pub async fn resolve_ancestor_ndv(
    metadata: &TableMetadata,
    _file_io: &FileIO,
    queried_snapshot: i64,
    wanted: &BTreeSet<i32>,
) -> HashMap<i32, AncestorNdv> {
    let mut resolved = HashMap::new();
    if wanted.is_empty() {
        return resolved;
    }
    let mut outstanding: BTreeSet<i32> = wanted.clone();
    let mut cursor = Some(queried_snapshot);

    for _ in 0..MAX_STATISTICS_ANCESTRY_STEPS {
        let Some(snapshot_id) = cursor else {
            break;
        };
        if let Some(statistics) = metadata.statistics_for_snapshot(snapshot_id) {
            match StatsLoader::load_ndv_from_metadata(statistics) {
                Ok(by_field) => {
                    // Each field id is satisfied by the first ancestor that has
                    // it, independently of the others.
                    outstanding.retain(|field_id| {
                        match by_field.get(field_id).copied().filter(|ndv| {
                            // A negative or non-finite estimate is not a usable
                            // count; keep looking rather than publish nonsense.
                            ndv.is_finite() && *ndv >= 0.0
                        }) {
                            Some(ndv) => {
                                resolved.insert(
                                    *field_id,
                                    AncestorNdv {
                                        basis_snapshot_id: snapshot_id,
                                        ndv,
                                    },
                                );
                                false
                            }
                            None => true,
                        }
                    });
                    if outstanding.is_empty() {
                        break;
                    }
                }
                Err(error) => {
                    tracing::debug!(
                        snapshot_id,
                        error = %error,
                        "iceberg statistics metadata is corrupt; stopping ancestor walk",
                    );
                    break;
                }
            }
        }
        cursor = metadata
            .snapshot_by_id(snapshot_id)
            .and_then(|snapshot| snapshot.parent_snapshot_id());
    }

    resolved
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap as StdHashMap;
    use std::sync::Arc;

    use novarocks_fs::{FsAccessResolver, TokioFileIoRuntime, TokioFileTaskSpawner};

    use crate::iceberg::spec::{
        FormatVersion, NestedField, Operation, PartitionSpec, PrimitiveType, Schema, Snapshot,
        SortOrder, StatisticsFile, Summary, TableMetadataBuilder, Type,
    };

    fn local_test_binding() -> crate::access_binding::IcebergReadBinding {
        let runtime = tokio::runtime::Handle::current();
        crate::access_binding::IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime)),
        )
    }
    use crate::stats_assembler::write_puffin_artifacts;

    use super::*;

    fn snapshot(snapshot_id: i64, parent_snapshot_id: Option<i64>) -> Snapshot {
        Snapshot::builder()
            .with_snapshot_id(snapshot_id)
            .with_parent_snapshot_id(parent_snapshot_id)
            .with_sequence_number(snapshot_id)
            .with_timestamp_ms(1_700_000_000_000 + snapshot_id)
            .with_manifest_list(format!("file:///tmp/manifest-list-{snapshot_id}.avro"))
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: StdHashMap::new(),
            })
            .with_schema_id(0)
            .build()
    }

    /// A linear chain 1..=len, newest last, plus any statistics files given.
    fn chain(len: i64, statistics: Vec<StatisticsFile>) -> TableMetadata {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .expect("schema");
        let mut builder = TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec(),
            SortOrder::unsorted_order(),
            "/tmp/statistics-ancestry-test".to_string(),
            FormatVersion::V2,
            StdHashMap::new(),
        )
        .expect("metadata builder");
        for id in 1..=len {
            builder = builder
                .add_snapshot(snapshot(id, (id > 1).then(|| id - 1)))
                .expect("add snapshot");
        }
        for file in statistics {
            builder = builder.set_statistics(file);
        }
        builder.build().expect("metadata").metadata
    }

    /// Writes a real Puffin holding a Theta sketch for each given field id.
    async fn statistics_file(
        dir: &std::path::Path,
        snapshot_id: i64,
        fields: &[(i32, i64)],
    ) -> StatisticsFile {
        let path = format!(
            "file://{}",
            dir.join(format!("s{snapshot_id}.puffin")).display()
        );
        let file_io = crate::fs_io::build_file_io_for_location(&path, local_test_binding());
        let artifacts = fields
            .iter()
            .map(|(field_id, distinct)| {
                novarocks_spi::connector::StatisticsArtifactDraft::try_new(
                    vec![*field_id],
                    crate::iceberg::puffin::APACHE_DATASKETCHES_THETA_V1,
                    bytes::Bytes::from_static(&[1, 3, 3, 0, 0, 0x1e, 0, 0]),
                    std::collections::BTreeMap::from([(
                        crate::stats_loader::NDV_PROPERTY.to_string(),
                        distinct.to_string(),
                    )]),
                )
                .expect("artifact")
            })
            .collect::<Vec<_>>();
        write_puffin_artifacts(&file_io, &path, snapshot_id, snapshot_id, &artifacts)
            .await
            .expect("write puffin")
            .expect("statistics file")
    }

    #[tokio::test]
    async fn an_empty_request_reads_nothing() {
        let metadata = chain(3, Vec::new());
        let file_io = crate::fs_io::build_file_io_for_location("file:///tmp", local_test_binding());
        assert!(
            resolve_ancestor_ndv(&metadata, &file_io, 3, &BTreeSet::new())
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn a_chain_without_statistics_files_resolves_nothing() {
        let metadata = chain(3, Vec::new());
        let file_io = crate::fs_io::build_file_io_for_location("file:///tmp", local_test_binding());
        assert!(
            resolve_ancestor_ndv(&metadata, &file_io, 3, &BTreeSet::from([1, 2]))
                .await
                .is_empty(),
            "a column nobody analyzed is missing, not an error"
        );
    }

    /// The reason the search is per column: a recent statistics file covering
    /// only field 7 must not hide field 5's statistics one snapshot further
    /// back. First-file-wins would report field 5 as missing.
    #[tokio::test]
    async fn a_recent_partial_file_does_not_mask_an_older_complete_one() {
        let dir = tempfile::tempdir().expect("tempdir");
        let older = statistics_file(dir.path(), 1, &[(5, 400), (7, 100)]).await;
        let newer = statistics_file(dir.path(), 3, &[(7, 900)]).await;
        let metadata = chain(3, vec![older, newer]);
        let file_io = crate::fs_io::build_file_io_for_location(
            &format!("file://{}", dir.path().display()),
            local_test_binding(),
        );

        let resolved = resolve_ancestor_ndv(&metadata, &file_io, 3, &BTreeSet::from([5, 7])).await;

        let field_7 = resolved.get(&7).expect("field 7 resolves");
        assert_eq!(
            field_7.basis_snapshot_id, 3,
            "field 7 takes the nearest file that has it"
        );
        let field_5 = resolved.get(&5).expect("field 5 must not be masked");
        assert_eq!(
            field_5.basis_snapshot_id, 1,
            "field 5 reaches past the newer file that lacks it"
        );
        assert!(
            field_5.ndv > 100.0,
            "the recovered estimate belongs to field 5, not to its neighbour"
        );
    }

    #[tokio::test]
    async fn a_field_absent_from_every_ancestor_stays_unresolved() {
        let dir = tempfile::tempdir().expect("tempdir");
        let only = statistics_file(dir.path(), 2, &[(5, 50)]).await;
        let metadata = chain(3, vec![only]);
        let file_io = crate::fs_io::build_file_io_for_location(
            &format!("file://{}", dir.path().display()),
            local_test_binding(),
        );

        let resolved = resolve_ancestor_ndv(&metadata, &file_io, 3, &BTreeSet::from([5, 9])).await;
        assert!(resolved.contains_key(&5));
        assert!(
            !resolved.contains_key(&9),
            "an unanalyzed column resolves to nothing rather than borrowing a value"
        );
    }

    /// Ordinary optimizer ancestry consumes the standard `ndv` property from
    /// Iceberg metadata. The Puffin body may already have been moved or deleted
    /// without turning a metadata-only planning lookup into object I/O.
    #[tokio::test]
    async fn an_unreadable_body_is_not_opened_by_optimizer_ancestry() {
        let dir = tempfile::tempdir().expect("tempdir");
        let reachable = statistics_file(dir.path(), 3, &[(7, 100)]).await;
        let mut vanished = statistics_file(dir.path(), 1, &[(5, 400)]).await;
        vanished.statistics_path =
            format!("file://{}", dir.path().join("expired.puffin").display());
        let metadata = chain(3, vec![reachable, vanished]);
        let file_io = crate::fs_io::build_file_io_for_location(
            &format!("file://{}", dir.path().display()),
            local_test_binding(),
        );

        StatsLoader::reset_theta_body_reads_for_test();
        let resolved = resolve_ancestor_ndv(&metadata, &file_io, 3, &BTreeSet::from([5, 7])).await;
        assert!(
            resolved.contains_key(&7),
            "current metadata resolves without opening its body"
        );
        assert!(
            resolved.contains_key(&5),
            "ancestor metadata resolves even when its body path is unreadable"
        );
        assert_eq!(StatsLoader::theta_body_reads(), 0);
    }
}
