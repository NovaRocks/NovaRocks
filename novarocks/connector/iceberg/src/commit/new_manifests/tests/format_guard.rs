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

use std::error::Error as _;

use super::*;
use crate::commit::staging::{StagingBase, StagingEngine};
use crate::commit::write_stack::domain::{
    IcebergCommitFragment, IcebergPositionDeleteFileArtifact,
};
use crate::commit::write_stack::test_support::{paid_fragments, sample_metrics, sample_partition};
use novarocks_spi::connector::ConnectorMutationFailure;

#[tokio::test]
async fn v2_delete_manifest_is_rejected_after_upgrade_on_first_write_and_reuse() {
    for previously_written in [false, true] {
        let fixture = Fixture::new();
        let source = fixture.metadata(FormatVersion::V2);
        let upgraded = source
            .clone()
            .into_builder(None)
            .upgrade_format_version(FormatVersion::V3)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let adapter = adapter("position-delete-format-guard", 1);
        let path = format!("{}/received.parquet", source.location());
        let fragment = IcebergCommitFragment::position_delete_file(
            IcebergPositionDeleteFileArtifact::try_new(
                path.clone(),
                sample_partition(),
                sample_metrics(1, 1024),
                "s3://b/target.parquet".into(),
                Vec::new(),
            )
            .unwrap(),
        );
        let (fragments, author) =
            paid_fragments(&adapter, vec![adapter.wrap_commit_fragment(fragment)]);
        let entry =
            CompactEntry::from_arrival(&adapter, &mut fragments.into_iter().next(), 0, &author)
                .unwrap();
        let mut received = ReceivedCompactFiles {
            counts: entry.counts,
            author: Some(author),
            ..Default::default()
        };
        received.push(entry);
        fixture
            .operation
            .adopt_session_data(ObjectIdentity::new(path).unwrap())
            .unwrap();
        fixture
            .operation
            .install_stage_new_files(
                0,
                PendingNewFiles {
                    received,
                    source: Arc::new(source.clone()),
                    codec: CompactFragmentCodec::for_adapter(&adapter),
                },
            )
            .unwrap();
        let intent = fixture
            .intent(&source, "main", Vec::new())
            .with_compact_stage(0)
            .unwrap();
        let historical =
            crate::read_snapshot::frozen_partition_types(&source, source.current_schema()).unwrap();
        if previously_written {
            let attempt = fixture.operation.begin_attempt().unwrap();
            let staging = StagingEngine::begin(
                StagingBase::Existing {
                    metadata: source,
                    metadata_location: "file:///source.json".into(),
                },
                &intent,
                &attempt,
            )
            .unwrap();
            let view = staging.view();
            prepare_added_manifests(
                &fixture.operation,
                &view,
                0,
                view.stage_snapshot_id().unwrap(),
                Some(&historical),
            )
            .await
            .unwrap();
        }
        let attempt = fixture.operation.begin_attempt().unwrap();
        let staging = StagingEngine::begin(
            StagingBase::Existing {
                metadata: upgraded,
                metadata_location: "file:///upgraded.json".into(),
            },
            &intent,
            &attempt,
        )
        .unwrap();
        let view = staging.view();
        let Err(error) = prepare_added_manifests(
            &fixture.operation,
            &view,
            0,
            view.stage_snapshot_id().unwrap(),
            Some(&historical),
        )
        .await
        else {
            panic!("V2 position deletes reached a V3 table");
        };
        let failure = error
            .source()
            .and_then(|source| source.downcast_ref::<ConnectorMutationFailure>())
            .unwrap();
        assert!(
            failure
                .message()
                .starts_with("Must use DVs for position deletes in V3:")
        );
    }
}
