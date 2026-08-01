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
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use crate::common::types::UniqueId;
use crate::connector::iceberg::stats_assembler::FileSketchSet;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SinkLoadStats {
    pub loaded_rows: i64,
    pub loaded_bytes: i64,
    pub filtered_rows: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TabletCommitInfo {
    pub tablet_id: i64,
    pub backend_id: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TabletFailInfo {
    pub tablet_id: i64,
    pub backend_id: i64,
}

/// Protocol-neutral final-report facts collected by fragment sinks.
///
/// The runtime owns this data; protocol adapters are responsible for encoding
/// it into their respective report wire formats.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SinkCommitReportSnapshot {
    pub connector_staged_report_frames: Vec<novarocks_spi::connector::ConnectorStagedReportFrame>,
    pub tablet_commit_infos: Vec<TabletCommitInfo>,
    pub tablet_fail_infos: Vec<TabletFailInfo>,
    pub load_stats: SinkLoadStats,
}

impl SinkCommitReportSnapshot {
    pub fn with_connector_staged_report_frames(
        mut self,
        frames: Vec<novarocks_spi::connector::ConnectorStagedReportFrame>,
    ) -> Self {
        self.connector_staged_report_frames = frames;
        self
    }
}

struct SinkCommitStore {
    mu: Mutex<HashMap<UniqueId, SinkCommitEntry>>,
}

#[derive(Default)]
struct SinkCommitEntry {
    tablet_commit_infos: Vec<TabletCommitInfo>,
    tablet_fail_infos: Vec<TabletFailInfo>,
    /// Per-file Theta sketch sets produced by the Iceberg sink for Puffin
    /// NDV statistics. These are not Cloneable (the `ThetaSketchHandle`
    /// holds an underlying `ThetaSketch` that does not implement `Clone`),
    /// so callers consume them via `take_sketch_sets` — a destructive
    /// drain — rather than `list_sketch_sets`. The pattern mirrors
    /// `IcebergCommitCollector::take_sketch_sets`.
    sketch_sets: Vec<FileSketchSet>,
    loaded_rows: i64,
    loaded_bytes: i64,
    filtered_rows: i64,
}

static STORE: OnceLock<SinkCommitStore> = OnceLock::new();

fn store() -> &'static SinkCommitStore {
    STORE.get_or_init(|| SinkCommitStore {
        mu: Mutex::new(HashMap::new()),
    })
}

pub(crate) fn register(finst_id: UniqueId) {
    let store = store();
    let mut guard = store.mu.lock().expect("sink commit store lock");
    guard.entry(finst_id).or_default();
}

pub(crate) fn try_register(finst_id: UniqueId) -> bool {
    let store = store();
    let mut guard = store.mu.lock().expect("sink commit store lock");
    if guard.contains_key(&finst_id) {
        return false;
    }
    guard.insert(finst_id, SinkCommitEntry::default());
    true
}

pub fn unregister(finst_id: UniqueId) {
    let store = store();
    let mut guard = store.mu.lock().expect("sink commit store lock");
    guard.remove(&finst_id);
}

pub(crate) fn is_registered(finst_id: UniqueId) -> bool {
    store()
        .mu
        .lock()
        .expect("sink commit store lock")
        .contains_key(&finst_id)
}

/// Push a per-file Theta sketch set produced by the Iceberg sink. Used by
/// the pipeline-driven sink path; the standalone iceberg_writer path uses
/// [`IcebergCommitCollector::inject_sketch_set`] directly.
pub(crate) fn add_sketch_set(finst_id: UniqueId, set: FileSketchSet) {
    let store = store();
    let mut guard = store.mu.lock().expect("sink commit store lock");
    guard.entry(finst_id).or_default().sketch_sets.push(set);
}

/// Destructively drain the per-file sketch sets registered via
/// [`add_sketch_set`]. The sketches cannot be cloned (the underlying
/// `ThetaSketch` from the `datasketches` crate does not implement Clone),
/// so each finst_id can be drained exactly once.
pub(crate) fn take_sketch_sets(finst_id: UniqueId) -> Vec<FileSketchSet> {
    let store = store();
    let mut guard = store.mu.lock().expect("sink commit store lock");
    guard
        .get_mut(&finst_id)
        .map(|entry| std::mem::take(&mut entry.sketch_sets))
        .unwrap_or_default()
}

pub(crate) fn add_tablet_commit_info(finst_id: UniqueId, info: TabletCommitInfo) {
    let store = store();
    let mut guard = store.mu.lock().expect("sink commit store lock");
    let entry = guard.entry(finst_id).or_default();
    let already_exists = entry.tablet_commit_infos.contains(&info);
    if !already_exists {
        entry.tablet_commit_infos.push(info);
    }
}

pub(crate) fn list_tablet_commit_infos(finst_id: UniqueId) -> Vec<TabletCommitInfo> {
    let store = store();
    let guard = store.mu.lock().expect("sink commit store lock");
    guard
        .get(&finst_id)
        .map(|entry| entry.tablet_commit_infos.clone())
        .unwrap_or_default()
}

pub(crate) fn add_tablet_fail_info(finst_id: UniqueId, info: TabletFailInfo) {
    let store = store();
    let mut guard = store.mu.lock().expect("sink commit store lock");
    let entry = guard.entry(finst_id).or_default();
    let already_exists = entry.tablet_fail_infos.contains(&info);
    if !already_exists {
        entry.tablet_fail_infos.push(info);
    }
}

pub(crate) fn list_tablet_fail_infos(finst_id: UniqueId) -> Vec<TabletFailInfo> {
    let store = store();
    let guard = store.mu.lock().expect("sink commit store lock");
    guard
        .get(&finst_id)
        .map(|entry| entry.tablet_fail_infos.clone())
        .unwrap_or_default()
}

pub(crate) fn add_load_stats(
    finst_id: UniqueId,
    loaded_rows: i64,
    loaded_bytes: i64,
    filtered_rows: i64,
) {
    let store = store();
    let mut guard = store.mu.lock().expect("sink commit store lock");
    let entry = guard.entry(finst_id).or_default();
    entry.loaded_rows = entry.loaded_rows.saturating_add(loaded_rows.max(0));
    entry.loaded_bytes = entry.loaded_bytes.saturating_add(loaded_bytes.max(0));
    entry.filtered_rows = entry.filtered_rows.saturating_add(filtered_rows.max(0));
}

pub(crate) fn get_load_counters(finst_id: UniqueId) -> (i64, i64) {
    let stats = get_load_stats(finst_id);
    (stats.loaded_rows, stats.loaded_bytes)
}

pub(crate) fn get_load_stats(finst_id: UniqueId) -> SinkLoadStats {
    let store = store();
    let guard = store.mu.lock().expect("sink commit store lock");
    guard
        .get(&finst_id)
        .map(|entry| SinkLoadStats {
            loaded_rows: entry.loaded_rows,
            loaded_bytes: entry.loaded_bytes,
            filtered_rows: entry.filtered_rows,
        })
        .unwrap_or_default()
}

pub fn report_snapshot(finst_id: UniqueId) -> SinkCommitReportSnapshot {
    let store = store();
    let guard = store.mu.lock().expect("sink commit store lock");
    let Some(entry) = guard.get(&finst_id) else {
        return SinkCommitReportSnapshot::default();
    };
    SinkCommitReportSnapshot {
        connector_staged_report_frames: Vec::new(),
        tablet_commit_infos: entry.tablet_commit_infos.clone(),
        tablet_fail_infos: entry.tablet_fail_infos.clone(),
        load_stats: SinkLoadStats {
            loaded_rows: entry.loaded_rows,
            loaded_bytes: entry.loaded_bytes,
            filtered_rows: entry.filtered_rows,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::{
        TabletCommitInfo, TabletFailInfo, add_tablet_commit_info, add_tablet_fail_info,
        list_tablet_commit_infos, list_tablet_fail_infos, unregister,
    };
    use crate::common::types::UniqueId;

    #[test]
    fn tablet_domain_records_deduplicate_by_tablet_and_backend() {
        let finst_id = UniqueId::new(41, 42);
        unregister(finst_id);

        let commit = TabletCommitInfo {
            tablet_id: 101,
            backend_id: 202,
        };
        add_tablet_commit_info(finst_id, commit);
        add_tablet_commit_info(finst_id, commit);
        add_tablet_commit_info(
            finst_id,
            TabletCommitInfo {
                tablet_id: 101,
                backend_id: 303,
            },
        );

        let fail = TabletFailInfo {
            tablet_id: 404,
            backend_id: 505,
        };
        add_tablet_fail_info(finst_id, fail);
        add_tablet_fail_info(finst_id, fail);

        assert_eq!(
            list_tablet_commit_infos(finst_id),
            vec![
                commit,
                TabletCommitInfo {
                    tablet_id: 101,
                    backend_id: 303,
                },
            ]
        );
        assert_eq!(list_tablet_fail_infos(finst_id), vec![fail]);

        unregister(finst_id);
    }
}
