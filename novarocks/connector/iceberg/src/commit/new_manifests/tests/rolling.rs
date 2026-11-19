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
use crate::commit::fast_append::CompactFastAppendPreparer;
use crate::commit::model::ArtifactWriter;
use crate::commit::staging::{StagingBase, StagingEngine};

#[tokio::test]
async fn equal_shape_entries_roll_and_refund_the_original_receipts() {
    let fixture = Fixture::new();
    let target = 8192;
    let metadata = fixture
        .metadata(FormatVersion::V3)
        .into_builder(None)
        .set_properties(std::collections::HashMap::from([
            (
                "commit.manifest.target-size-bytes".into(),
                target.to_string(),
            ),
            ("commit.manifest-merge.enabled".into(), "false".into()),
        ]))
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    let live = install_appends(&fixture, &metadata, 600);
    assert!(live.load(Ordering::SeqCst) > 0);
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
    let trace = Arc::new(std::sync::Mutex::new(Vec::new()));
    crate::fs_io::TEST_STORAGE_REQUESTS
        .scope(trace.clone(), async {
            engine
                .stage(&CompactFastAppendPreparer(fixture.operation.clone()))
                .await
                .unwrap();
        })
        .await;
    assert_eq!(live.load(Ordering::SeqCst), 0);
    let metadata = engine.metadata();
    let list = crate::manifest::load_manifest_list(
        metadata.current_snapshot().unwrap(),
        attempt.file_io(),
        metadata,
    )
    .await
    .unwrap();
    let manifests = list.entries();
    assert!(manifests.len() > 1);
    for manifest in &manifests[..manifests.len() - 1] {
        assert!(
            manifest.manifest_length >= target,
            "{}",
            manifest.manifest_length
        );
    }
    let writes = trace.lock().unwrap();
    let manifest_writes = writes
        .iter()
        .filter(|event| event.operation == "write")
        .filter(|event| {
            manifests
                .iter()
                .any(|manifest| manifest.manifest_path == event.path)
        })
        .count();
    assert_eq!(manifest_writes, manifests.len());
    assert_eq!(
        manifests
            .iter()
            .map(|m| m.added_files_count.unwrap())
            .sum::<u32>(),
        600
    );
    engine.freeze().unwrap();
}
