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
use crate::commit::model::{ReusableStatisticsFile, StatisticsReuseBasis};
use crate::commit::overwrite::preparer_tests::Fixture;
use crate::iceberg::puffin::{Blob, CompressionCodec, PuffinReader, PuffinWriter};
use crate::iceberg::spec::{BlobMetadata, FormatVersion, StatisticsFile};
use std::collections::HashMap;

#[tokio::test]
async fn reusable_statistics_survive_round_retirement_and_require_exact_basis() {
    let fixture = Fixture::new();
    let base = fixture.metadata(FormatVersion::V2);
    let operation = &fixture.operation;
    let first = operation.begin_attempt().unwrap();
    let id = first.stage_snapshot_id(0, &base).unwrap();
    let other_id = first.stage_snapshot_id(1, &base).unwrap();
    let source_path = format!(
        "file://{}/source.puffin",
        fixture.directory.path().display()
    );
    let source_io = crate::iceberg::io::FileIO::new_with_fs();
    let source_file = source_io.new_output(&source_path).unwrap();
    let r#type = "test.statistics".to_owned();
    let properties = HashMap::from([("ndv".to_owned(), "2".to_owned())]);
    let blob = Blob::builder()
        .r#type(r#type.clone())
        .fields(vec![1])
        .snapshot_id(7)
        .sequence_number(1)
        .data(vec![1, 2])
        .properties(properties.clone())
        .build();
    let mut source_writer = PuffinWriter::new(&source_file, HashMap::new(), false)
        .await
        .unwrap();
    source_writer
        .add(blob, CompressionCodec::zstd_default())
        .await
        .unwrap();
    let source_close = source_writer.close().await.unwrap();
    let input = first.file_io().new_input(&source_path).unwrap();
    let bytes = input.read().await.unwrap();
    let footer_payload =
        u32::from_le_bytes(bytes[bytes.len() - 12..bytes.len() - 8].try_into().unwrap());
    let source = StatisticsFile {
        snapshot_id: 7,
        statistics_path: source_path,
        file_size_in_bytes: i64::try_from(source_close.size).unwrap(),
        file_footer_size_in_bytes: 16 + i64::from(footer_payload),
        key_metadata: None,
        blob_metadata: vec![BlobMetadata {
            r#type,
            snapshot_id: 7,
            sequence_number: 1,
            fields: vec![1],
            properties: HashMap::new(),
        }],
    };
    let basis = StatisticsReuseBasis {
        stage: 0,
        source: (&source).into(),
        stage_snapshot_id: id,
    };
    let reusable = first.allocate_reusable_statistics().unwrap();
    let ordinary = first.allocate(ArtifactKind::Statistics).unwrap();
    let output = ReusableStatisticsFile {
        snapshot_id: id,
        object: reusable.clone(),
        file_size_in_bytes: bytes.len() as i64,
        file_footer_size_in_bytes: source.file_footer_size_in_bytes,
    };
    assert!(first.find_reusable_statistics(&basis).unwrap().is_none());
    assert!(
        first
            .remember_reusable_statistics(basis.clone(), output.clone())
            .is_err(),
        "an allocated path does not prove write completion"
    );
    for object in [&reusable, &ordinary] {
        let mut writer = first
            .file_io()
            .new_output(object.path())
            .unwrap()
            .writer()
            .await
            .unwrap();
        writer.write(bytes.clone()).await.unwrap();
        let closed = writer.close().await.unwrap();
        assert_eq!(
            closed.size,
            u64::try_from(output.file_size_in_bytes).unwrap()
        );
    }
    let reader = PuffinReader::new(first.file_io().new_input(reusable.path()).unwrap())
        .await
        .unwrap();
    assert_eq!(
        reader.file_metadata().await.unwrap().blobs()[0].properties(),
        &properties
    );
    for invalid_footer in [0, source.file_size_in_bytes - 3] {
        assert!(
            first
                .remember_reusable_statistics(
                    basis.clone(),
                    ReusableStatisticsFile {
                        file_footer_size_in_bytes: invalid_footer,
                        ..output.clone()
                    },
                )
                .is_err()
        );
    }
    assert!(
        first
            .remember_reusable_statistics(
                basis.clone(),
                ReusableStatisticsFile {
                    object: ordinary.clone(),
                    ..output.clone()
                },
            )
            .is_err(),
        "an ordinary statistics file remains round-bound"
    );
    first
        .remember_reusable_statistics(basis.clone(), output.clone())
        .unwrap();
    operation
        .retire_attempt_bound_artifacts(first.attempt_token())
        .unwrap();

    let second = operation.begin_attempt().unwrap();
    assert_eq!(second.stage_snapshot_id(0, &base).unwrap(), id);
    assert_eq!(
        second.find_reusable_statistics(&basis).unwrap(),
        Some(output.clone())
    );
    for other in [
        StatisticsReuseBasis {
            stage: 1,
            ..basis.clone()
        },
        StatisticsReuseBasis {
            stage_snapshot_id: other_id,
            ..basis.clone()
        },
        StatisticsReuseBasis {
            source: (&StatisticsFile {
                statistics_path: "file:///caller-owned/source-b.puffin".into(),
                ..source.clone()
            })
                .into(),
            ..basis.clone()
        },
    ] {
        assert!(second.find_reusable_statistics(&other).unwrap().is_none());
    }
    let mut refreshed_registration = source.clone();
    refreshed_registration.blob_metadata[0].properties = properties;
    let refreshed = StatisticsReuseBasis {
        source: (&refreshed_registration).into(),
        ..basis.clone()
    };
    assert_eq!(
        second.find_reusable_statistics(&refreshed).unwrap(),
        Some(output.clone()),
        "the same physical Puffin can acquire a different registered property subset",
    );
    for changed_source in [
        StatisticsFile {
            file_size_in_bytes: source.file_size_in_bytes + 1,
            ..source.clone()
        },
        StatisticsFile {
            file_footer_size_in_bytes: source.file_footer_size_in_bytes - 1,
            ..source.clone()
        },
        StatisticsFile {
            key_metadata: Some("dGVzdC1rZXk=".into()),
            ..source.clone()
        },
    ] {
        assert!(
            second
                .find_reusable_statistics(&StatisticsReuseBasis {
                    source: (&changed_source).into(),
                    ..basis.clone()
                })
                .unwrap()
                .is_none(),
            "changed same-URI physical registration must be revalidated",
        );
    }
    assert_eq!(
        second.find_reusable_statistics(&basis).unwrap(),
        Some(output)
    );
    let records = operation.artifacts().unwrap();
    for (object, retired) in [(&reusable, false), (&ordinary, true)] {
        let record = records
            .iter()
            .find(|record| &record.object == object)
            .unwrap();
        assert_eq!(record.retired, retired);
        assert_eq!(record.write_state, ArtifactWriteState::Written);
        assert!(second.file_io().exists(object.path()).await.unwrap());
    }
    assert_eq!(
        operation.cleanup(CleanupScope::EntireOperation).await,
        IcebergCleanupReport::Complete { deleted: 2 }
    );
    assert!(second.find_reusable_statistics(&basis).unwrap().is_none());
    assert!(!second.file_io().exists(reusable.path()).await.unwrap());
    assert!(!second.file_io().exists(ordinary.path()).await.unwrap());
}
