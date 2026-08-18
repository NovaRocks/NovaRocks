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

use std::collections::BTreeMap;

use novarocks::maintenance::MaintenanceTarget;
use novarocks_frontend::query_execution::maintenance::{
    MaintenanceActionOutcome, MaintenanceActionRequest, OptimizeJobState,
};

#[test]
fn typed_action_variants_cannot_mix_unrelated_options() {
    let target = MaintenanceTarget {
        catalog: "ice".to_owned(),
        namespace: "db".to_owned(),
        table: "t".to_owned(),
    };
    let request = MaintenanceActionRequest::RewriteManifests {
        target,
        use_caching: Some(false),
        spec_id: Some(7),
    };
    assert!(matches!(
        request,
        MaintenanceActionRequest::RewriteManifests {
            use_caching: Some(false),
            spec_id: Some(7),
            ..
        }
    ));
    assert_eq!(OptimizeJobState::Pending.as_str(), "PENDING");
    let _: BTreeMap<String, String> = BTreeMap::new();
}

#[test]
fn rewrite_data_files_outcome_carries_durable_optimize_truth() {
    let outcome = MaintenanceActionOutcome::RewriteDataFiles {
        target_snapshot_id: Some(900),
        rewritten_data_files_count: 4,
        added_data_files_count: 2,
        rewritten_bytes_count: 8192,
        failed_data_files_count: 0,
        removed_delete_files_count: 3,
        output_record_count: 88,
    };

    assert!(matches!(
        outcome,
        MaintenanceActionOutcome::RewriteDataFiles {
            target_snapshot_id: Some(900),
            output_record_count: 88,
            ..
        }
    ));
}
