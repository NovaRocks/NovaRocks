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

#[test]
fn calibration_takes_the_minimum_of_observed_ratios() {
    let mut calibration = Calibration::default();
    assert!(!calibration.reaches(4, 5));
    calibration.observe(15, 5).unwrap();
    assert!(calibration.reaches(4, 12));
    calibration.observe(50, 10).unwrap();
    assert!(!calibration.reaches(4, 13));
    calibration.observe(12, 6).unwrap();
    assert!(!calibration.reaches(4, 9));
    assert!(calibration.reaches(4, 8));
    assert!(calibration.observe(9, 10).is_err());
}

#[tokio::test]
async fn compact_bound_is_below_physical_avro_for_supported_contents_and_widths() {
    use crate::commit::report::IcebergColumnStats;
    use crate::commit::write_stack::test_support::sample_partition;
    use crate::commit::write_stack::{
        IcebergArtifactMetrics, IcebergCommitFragment, IcebergContentRange,
        IcebergDataFileArtifact, IcebergDeletionVectorArtifact, IcebergPositionDeleteFileArtifact,
    };
    use crate::delete_file::IcebergFileFormat;
    use crate::iceberg::spec::{
        NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder, TableMetadataBuilder, Type,
    };

    let adapter = adapter("compact-bounds", 1);
    let encoder = IcebergWriteFragmentEncoder::new(adapter.clone());
    let decoder = IcebergWriteFragmentDecoder::new(adapter.clone());
    let author = ConnectorPayloadRetentionGuard::from_byte_budget(Arc::new(Window::default()));
    let codec = CompactFragmentCodec::for_adapter(&adapter);
    let fixture = Fixture::new();
    let attempt = fixture.operation.begin_attempt().unwrap();
    for version in [FormatVersion::V1, FormatVersion::V2, FormatVersion::V3] {
        for width in [1, 32] {
            let fields = (1..=width)
                .map(|id| {
                    Arc::new(NestedField::optional(
                        id,
                        format!("column_{id}"),
                        Type::Primitive(PrimitiveType::Long),
                    ))
                })
                .collect::<Vec<_>>();
            let schema = Schema::builder().with_fields(fields).build().unwrap();
            let metadata = TableMetadataBuilder::new(
                schema,
                PartitionSpec::unpartition_spec(),
                SortOrder::unsorted_order(),
                fixture.directory.path().to_str().unwrap().into(),
                version,
                Default::default(),
            )
            .unwrap()
            .build()
            .unwrap()
            .metadata;
            let spec = metadata.default_partition_spec();
            for statistics in [false, true] {
                let metrics = || {
                    IcebergArtifactMetrics::try_new(
                        5,
                        1024,
                        vec![4, 128],
                        statistics.then(|| IcebergColumnStats {
                            column_sizes: (1..=width).map(|id| (id, 128)).collect(),
                            value_counts: (1..=width).map(|id| (id, 5)).collect(),
                            null_value_counts: (1..=width).map(|id| (id, 0)).collect(),
                            lower_bounds: (1..=width)
                                .map(|id| (id, 1i64.to_le_bytes().to_vec()))
                                .collect(),
                            upper_bounds: (1..=width)
                                .map(|id| (id, 16i64.to_le_bytes().to_vec()))
                                .collect(),
                            ..IcebergColumnStats::default()
                        }),
                    )
                    .unwrap()
                };
                for content in [ManifestContentType::Data, ManifestContentType::Deletes] {
                    if version == FormatVersion::V1 && content == ManifestContentType::Deletes {
                        continue;
                    }
                    let mut lower = 0;
                    let mut files = Vec::new();
                    for ordinal in 0..16 {
                        let stem =
                            format!("s3://bucket/data/compact-test-with-wide-fields-{ordinal:04}");
                        let data = format!("{stem}.parquet");
                        let fragment = match (version, content) {
                            (_, ManifestContentType::Data) => IcebergCommitFragment::data_file(
                                IcebergDataFileArtifact::try_new(
                                    data,
                                    IcebergFileFormat::Parquet,
                                    sample_partition(),
                                    metrics(),
                                    None,
                                )
                                .unwrap(),
                            ),
                            (FormatVersion::V2, ManifestContentType::Deletes) => {
                                IcebergCommitFragment::position_delete_file(
                                    IcebergPositionDeleteFileArtifact::try_new(
                                        format!("{stem}-deletes.parquet"),
                                        sample_partition(),
                                        metrics(),
                                        data,
                                        Vec::new(),
                                    )
                                    .unwrap(),
                                )
                            }
                            (FormatVersion::V3, ManifestContentType::Deletes) => {
                                IcebergCommitFragment::deletion_vector(
                                    IcebergDeletionVectorArtifact::try_new(
                                        format!("{stem}.puffin"),
                                        sample_partition(),
                                        metrics(),
                                        data,
                                        IcebergContentRange::try_new(128, 64).unwrap(),
                                        5,
                                        Vec::new(),
                                    )
                                    .unwrap(),
                                )
                            }
                            (FormatVersion::V1, ManifestContentType::Deletes) => unreachable!(),
                        };
                        let encoded = encoder
                            .encode_commit_fragment_payload(&adapter.wrap_commit_fragment(fragment))
                            .unwrap();
                        let mut incoming = Some(
                            decoder
                                .decode_commit_fragment_payload(&encoded, &author)
                                .unwrap(),
                        );
                        let compact =
                            CompactEntry::from_arrival(&adapter, &mut incoming, 0, &author)
                                .unwrap();
                        let addition = compact.addition.as_ref().unwrap();
                        lower += addition.encoded_lower_bound;
                        let (value, _receipt) = codec.decode(&addition.payload, &author).unwrap();
                        let file = written_file_with_historical_partition(&value, &metadata, None)
                            .unwrap();
                        let file = crate::commit::data_file::into_written_file(file).unwrap();
                        files.push(ManifestEntryWrite::Added(
                            AddedContent::new_logical_data(file, 0).unwrap(),
                        ));
                    }
                    let manifest = crate::commit::staging::write_manifest(
                        &attempt,
                        version,
                        1,
                        metadata.current_schema().clone(),
                        spec.as_ref().clone(),
                        content,
                        files,
                    )
                    .await
                    .unwrap();
                    assert!(
                        manifest.manifest_length as usize >= lower,
                        "version={version:?} width={width} statistics={statistics} content={content:?}"
                    );
                }
            }
        }
    }
}
