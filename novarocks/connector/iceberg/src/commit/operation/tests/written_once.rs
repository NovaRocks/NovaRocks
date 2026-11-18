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

//! Observes custody and release of the original operation stage and its receipts.

use super::*;
use crate::commit::new_manifests::{StageNewFiles, tests::install_appends};
use crate::commit::overwrite::preparer_tests::Fixture;
use crate::iceberg::spec::FormatVersion;

#[tokio::test]
async fn compact_stage_is_claimed_once_and_its_original_receipts_are_released() {
    let fixture = Fixture::new();
    let metadata = fixture.metadata(FormatVersion::V2);
    let live = install_appends(&fixture, &metadata, 4);
    let intent = fixture
        .intent(&metadata, "main", Vec::new())
        .with_compact_stage(0)
        .unwrap();
    assert_eq!(intent.compact_stage(), Some(0));
    assert!(intent.changes().added.is_empty());
    let StageNewFiles::Pending(original) = fixture.operation.take_stage_new_files(0).unwrap()
    else {
        panic!("compact additions were not held in the operation");
    };
    assert_eq!(original.received.entries().count(), 4);
    assert!(matches!(
        fixture.operation.take_stage_new_files(0).unwrap(),
        StageNewFiles::Writing
    ));
    drop(original);
    assert_eq!(live.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn undispatched_cleanup_releases_unclaimed_compact_inputs() {
    let fixture = Fixture::new();
    let metadata = fixture.metadata(FormatVersion::V2);
    let live = install_appends(&fixture, &metadata, 4);
    assert!(live.load(Ordering::SeqCst) > 0);
    fixture
        .operation
        .cleanup(CleanupScope::EntireOperation)
        .await;
    assert_eq!(live.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn wrong_compact_coverage_refuses_before_building_the_publication_list() {
    use crate::commit::fast_append::CompactFastAppendPreparer;
    use crate::commit::new_manifests::tests::install_appends_with_adoption;
    use crate::commit::staging::{StagingBase, StagingEngine};

    for omit_adoption in [false, true] {
        let fixture = Fixture::new();
        let metadata = fixture.metadata(FormatVersion::V2);
        let live = install_appends_with_adoption(&fixture, &metadata, 4, |index| {
            index != 0 || !omit_adoption
        });
        if !omit_adoption {
            fixture
                .operation
                .adopt_session_data(
                    ObjectIdentity::new(format!("{}/data/omitted.parquet", metadata.location()))
                        .unwrap(),
                )
                .unwrap();
        }
        let intent = fixture
            .intent(&metadata, "main", Vec::new())
            .with_compact_stage(0)
            .unwrap();
        let attempt = fixture.operation.begin_attempt().unwrap();
        let mut engine = StagingEngine::begin(
            StagingBase::Existing {
                metadata,
                metadata_location: "file:///table/00000.metadata.json".into(),
            },
            &intent,
            &attempt,
        )
        .unwrap();
        let error = engine
            .stage(&CompactFastAppendPreparer(fixture.operation.clone()))
            .await
            .unwrap_err();
        assert_eq!(
            crate::commit::attempt::before_dispatch_failure(&error).kind(),
            novarocks_spi::connector::ConnectorMutationFailureKind::Internal
        );
        assert_eq!(live.load(Ordering::SeqCst), 0);
        assert!(
            fixture
                .operation
                .artifacts()
                .unwrap()
                .iter()
                .all(|artifact| {
                    !matches!(
                        artifact.origin,
                        crate::commit::model::ArtifactOrigin::Produced {
                            kind: ArtifactKind::ManifestList,
                            ..
                        }
                    )
                })
        );
    }
}
