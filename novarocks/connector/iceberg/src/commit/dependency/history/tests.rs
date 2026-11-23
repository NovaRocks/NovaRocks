// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use crate::commit::dependency::builtin::tests::{append_snapshot, metadata};

#[test]
fn rewrite_files_walk_stops_at_the_exact_parent_pointer() {
    let metadata = append_snapshot(metadata(), 11, None, Operation::Append);
    let metadata = append_snapshot(metadata, 12, Some(11), Operation::Overwrite);
    let metadata = append_snapshot(metadata, 13, Some(12), Operation::Replace);
    let expired = metadata
        .into_builder(None)
        .remove_snapshots(&[11])
        .build()
        .unwrap()
        .metadata;

    assert_eq!(
        history_window(&expired, Some(11), Some(13)).unwrap_err(),
        HistoryGap::StartExpired(11)
    );
    let window = rewrite_files_history_window(&expired, Some(11), Some(13)).unwrap();
    assert_eq!(window.endpoints(), (Some(11), Some(13)));
    assert_eq!(
        window
            .snapshots()
            .iter()
            .map(|snapshot| snapshot.snapshot_id())
            .collect::<Vec<_>>(),
        [13, 12]
    );
    assert!(
        rewrite_files_history_window(&expired, Some(11), Some(11))
            .unwrap()
            .snapshots()
            .is_empty()
    );
}

#[test]
fn rewrite_files_reports_the_last_proved_ancestor() {
    let metadata = append_snapshot(metadata(), 11, None, Operation::Append);
    let metadata = append_snapshot(metadata, 12, Some(11), Operation::Overwrite);
    let metadata = append_snapshot(metadata, 13, Some(12), Operation::Replace);
    let broken = metadata
        .clone()
        .into_builder(None)
        .remove_snapshots(&[12])
        .build()
        .unwrap()
        .metadata;

    assert_eq!(
        rewrite_files_history_window(&broken, Some(11), Some(13)).unwrap_err(),
        Some(13)
    );
    assert_eq!(
        rewrite_files_history_window(&metadata, Some(99), Some(13)).unwrap_err(),
        Some(11)
    );
    assert_eq!(
        rewrite_files_history_window(&metadata, Some(11), Some(99)).unwrap_err(),
        None
    );
    assert_eq!(
        rewrite_files_history_window(&metadata, None, Some(13))
            .unwrap()
            .snapshots()
            .len(),
        3
    );
}
