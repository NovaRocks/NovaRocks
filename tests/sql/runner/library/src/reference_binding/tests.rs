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
use novarocks_cluster_harness::reference_inputs::{ArtifactRef, TrinoImageRef};

fn reference() -> ResolvedReference {
    ResolvedReference {
        trino_image: Some(TrinoImageRef {
            alias: "reference-alias".into(),
            image_id: "image-id".into(),
            version: "declared-version".into(),
        }),
        trino_jdbc_driver: Some(ArtifactRef {
            name: "driver.jar".into(),
            path: "/resolved/driver.jar".into(),
            sha256: "hash".into(),
        }),
    }
}

#[test]
fn only_final_nonempty_selection_requests_the_union_and_retains_per_suite_facts() {
    let bound = bind_selected(
        false,
        [
            ("empty".into(), false, vec![ReferenceInput::TrinoJdbcDriver]),
            ("ordinary".into(), true, vec![]),
            ("first".into(), true, vec![ReferenceInput::TrinoImage]),
            ("second".into(), true, vec![ReferenceInput::TrinoImage]),
        ],
        |needed| {
            assert_eq!(needed, &BTreeSet::from([ReferenceInput::TrinoImage]));
            Ok(reference())
        },
    )
    .unwrap();
    assert_eq!(bound.len(), 2);
    for value in bound.values() {
        assert_eq!(
            serde_json::to_value(value).unwrap(),
            serde_json::json!({"trino_image":{"alias":"reference-alias","image_id":"image-id","version":"declared-version"}})
        );
    }
}

#[test]
fn dry_run_empty_selection_and_ordinary_selection_never_resolve() {
    for (dry_run, selected, inputs) in [
        (true, true, vec![ReferenceInput::TrinoImage]),
        (false, false, vec![ReferenceInput::TrinoImage]),
        (false, true, vec![]),
    ] {
        assert!(
            bind_selected(dry_run, [("suite".into(), selected, inputs)], |_| panic!(
                "must not resolve"
            ))
            .unwrap()
            .is_empty()
        );
    }
}

#[test]
fn builder_failure_prevents_binding_any_suite() {
    let failure = bind_selected(
        false,
        [("suite".into(), true, vec![ReferenceInput::TrinoImage])],
        |_| anyhow::bail!("original builder failure"),
    );
    assert_eq!(failure.unwrap_err().to_string(), "original builder failure");
}
