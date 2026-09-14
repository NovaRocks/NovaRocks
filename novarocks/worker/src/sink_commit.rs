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

//! Worker-owned final sink-report aggregation for native fragments.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use novarocks_execution::runtime::fragment::io::{
    FragmentCommitLease, FragmentCommitPort, FragmentCommitReport, FragmentSinkLoadStats,
    TabletCommitInfo as ExecutionTabletCommitInfo, TabletFailInfo as ExecutionTabletFailInfo,
};
use novarocks_spi::connector::{WriteCommitEvidenceLedger, WriteCommitEvidenceLimits};
use novarocks_types::UniqueId;

const TABLET_TERMINAL_CANONICAL_BYTES: usize = 16;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct SinkLoadStats {
    pub(crate) loaded_rows: i64,
    pub(crate) loaded_bytes: i64,
    pub(crate) filtered_rows: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TabletCommitInfo {
    pub(crate) tablet_id: i64,
    pub(crate) backend_id: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TabletFailInfo {
    pub(crate) tablet_id: i64,
    pub(crate) backend_id: i64,
}

/// Runtime facts collected by Worker fragment sinks before the registry
/// projects them into the Protocol terminal snapshot.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct SinkCommitReportSnapshot {
    pub(crate) tablet_commit_infos: Vec<TabletCommitInfo>,
    pub(crate) tablet_fail_infos: Vec<TabletFailInfo>,
    pub(crate) load_stats: SinkLoadStats,
}

#[derive(Debug, Default)]
pub struct WorkerSinkCommitPort;

impl FragmentCommitPort for WorkerSinkCommitPort {
    fn acquire(
        &self,
        fragment_instance_id: UniqueId,
    ) -> Result<Box<dyn FragmentCommitLease>, String> {
        acquire_worker_sink_commit_lease(fragment_instance_id, WriteCommitEvidenceLimits::default())
    }
}

/// Configured Worker adapter used by production fragment composition.
#[derive(Debug)]
pub struct ConfiguredWorkerSinkCommitPort {
    evidence_limits: WriteCommitEvidenceLimits,
}

impl ConfiguredWorkerSinkCommitPort {
    pub fn new(evidence_limits: WriteCommitEvidenceLimits) -> Self {
        Self { evidence_limits }
    }
}

impl FragmentCommitPort for ConfiguredWorkerSinkCommitPort {
    fn acquire(
        &self,
        fragment_instance_id: UniqueId,
    ) -> Result<Box<dyn FragmentCommitLease>, String> {
        acquire_worker_sink_commit_lease(fragment_instance_id, self.evidence_limits)
    }
}

fn acquire_worker_sink_commit_lease(
    fragment_instance_id: UniqueId,
    evidence_limits: WriteCommitEvidenceLimits,
) -> Result<Box<dyn FragmentCommitLease>, String> {
    if !try_register_with_ledger(
        fragment_instance_id,
        WriteCommitEvidenceLedger::new(evidence_limits),
    ) {
        return Err(format!(
            "sink commit already registered for fragment instance {fragment_instance_id}"
        ));
    }
    Ok(Box::new(WorkerSinkCommitLease {
        fragment_instance_id,
        active: true,
    }))
}

struct WorkerSinkCommitLease {
    fragment_instance_id: UniqueId,
    active: bool,
}

impl WorkerSinkCommitLease {
    fn snapshot(&self) -> FragmentCommitReport {
        let snapshot = report_snapshot(self.fragment_instance_id);
        FragmentCommitReport {
            tablet_commit_infos: snapshot
                .tablet_commit_infos
                .into_iter()
                .map(|info| ExecutionTabletCommitInfo {
                    tablet_id: info.tablet_id,
                    backend_id: info.backend_id,
                })
                .collect(),
            tablet_fail_infos: snapshot
                .tablet_fail_infos
                .into_iter()
                .map(|info| ExecutionTabletFailInfo {
                    tablet_id: info.tablet_id,
                    backend_id: info.backend_id,
                })
                .collect(),
            load_stats: FragmentSinkLoadStats {
                loaded_rows: snapshot.load_stats.loaded_rows,
                loaded_bytes: snapshot.load_stats.loaded_bytes,
                filtered_rows: snapshot.load_stats.filtered_rows,
            },
        }
    }
}

impl FragmentCommitLease for WorkerSinkCommitLease {
    fn add_load_stats(&mut self, stats: FragmentSinkLoadStats) {
        add_load_stats(
            self.fragment_instance_id,
            stats.loaded_rows,
            stats.loaded_bytes,
            stats.filtered_rows,
        );
    }

    fn add_tablet_commit_info(&mut self, info: ExecutionTabletCommitInfo) -> Result<(), String> {
        try_add_tablet_commit_info(
            self.fragment_instance_id,
            TabletCommitInfo {
                tablet_id: info.tablet_id,
                backend_id: info.backend_id,
            },
        )
    }

    fn add_tablet_fail_info(&mut self, info: ExecutionTabletFailInfo) -> Result<(), String> {
        try_add_tablet_fail_info(
            self.fragment_instance_id,
            TabletFailInfo {
                tablet_id: info.tablet_id,
                backend_id: info.backend_id,
            },
        )
    }

    fn finish(mut self: Box<Self>) -> Result<FragmentCommitReport, String> {
        let snapshot = self.snapshot();
        if self.active {
            unregister(self.fragment_instance_id);
            self.active = false;
        }
        Ok(snapshot)
    }

    fn handoff(mut self: Box<Self>) -> Result<(), String> {
        self.active = false;
        Ok(())
    }

    fn rollback(mut self: Box<Self>) -> Result<(), String> {
        if self.active {
            unregister(self.fragment_instance_id);
            self.active = false;
        }
        Ok(())
    }
}

impl Drop for WorkerSinkCommitLease {
    fn drop(&mut self) {
        if self.active {
            unregister(self.fragment_instance_id);
        }
    }
}

struct SinkCommitStore {
    mu: Mutex<HashMap<UniqueId, SinkCommitEntry>>,
}

#[derive(Default)]
struct SinkCommitEntry {
    evidence_ledger: WriteCommitEvidenceLedger,
    tablet_commit_infos: Vec<TabletCommitInfo>,
    tablet_fail_infos: Vec<TabletFailInfo>,
    loaded_rows: i64,
    loaded_bytes: i64,
    filtered_rows: i64,
}

impl SinkCommitEntry {
    fn new(evidence_ledger: WriteCommitEvidenceLedger) -> Self {
        Self {
            evidence_ledger,
            ..Self::default()
        }
    }
}

static STORE: OnceLock<SinkCommitStore> = OnceLock::new();

fn store() -> &'static SinkCommitStore {
    STORE.get_or_init(|| SinkCommitStore {
        mu: Mutex::new(HashMap::new()),
    })
}

fn try_register_with_ledger(
    finst_id: UniqueId,
    evidence_ledger: WriteCommitEvidenceLedger,
) -> bool {
    let store = store();
    let mut guard = store.mu.lock().expect("sink commit store lock");
    if guard.contains_key(&finst_id) {
        return false;
    }
    guard.insert(finst_id, SinkCommitEntry::new(evidence_ledger));
    true
}

fn unregister(finst_id: UniqueId) {
    let store = store();
    let mut guard = store.mu.lock().expect("sink commit store lock");
    guard.remove(&finst_id);
}

fn try_add_tablet_commit_info(finst_id: UniqueId, info: TabletCommitInfo) -> Result<(), String> {
    let store = store();
    let mut guard = store.mu.lock().expect("sink commit store lock");
    let entry = guard
        .entry(finst_id)
        .or_insert_with(|| SinkCommitEntry::new(WriteCommitEvidenceLedger::default()));
    if !entry.tablet_commit_infos.contains(&info) {
        entry
            .evidence_ledger
            .reserve(TABLET_TERMINAL_CANONICAL_BYTES, 1)
            .map_err(|error| format!("reserve tablet commit evidence: {error}"))?;
        entry.tablet_commit_infos.push(info);
    }
    Ok(())
}

fn try_add_tablet_fail_info(finst_id: UniqueId, info: TabletFailInfo) -> Result<(), String> {
    let store = store();
    let mut guard = store.mu.lock().expect("sink commit store lock");
    let entry = guard
        .entry(finst_id)
        .or_insert_with(|| SinkCommitEntry::new(WriteCommitEvidenceLedger::default()));
    if !entry.tablet_fail_infos.contains(&info) {
        entry
            .evidence_ledger
            .reserve(TABLET_TERMINAL_CANONICAL_BYTES, 1)
            .map_err(|error| format!("reserve tablet failure evidence: {error}"))?;
        entry.tablet_fail_infos.push(info);
    }
    Ok(())
}

fn add_load_stats(finst_id: UniqueId, loaded_rows: i64, loaded_bytes: i64, filtered_rows: i64) {
    let store = store();
    let mut guard = store.mu.lock().expect("sink commit store lock");
    let entry = guard.entry(finst_id).or_default();
    entry.loaded_rows = entry.loaded_rows.saturating_add(loaded_rows.max(0));
    entry.loaded_bytes = entry.loaded_bytes.saturating_add(loaded_bytes.max(0));
    entry.filtered_rows = entry.filtered_rows.saturating_add(filtered_rows.max(0));
}

pub(crate) fn report_snapshot(finst_id: UniqueId) -> SinkCommitReportSnapshot {
    let store = store();
    let guard = store.mu.lock().expect("sink commit store lock");
    let Some(entry) = guard.get(&finst_id) else {
        return SinkCommitReportSnapshot::default();
    };
    SinkCommitReportSnapshot {
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
        TabletCommitInfo, TabletFailInfo, try_add_tablet_commit_info, try_add_tablet_fail_info,
        unregister,
    };
    use novarocks_types::UniqueId;

    #[test]
    fn tablet_domain_records_deduplicate_by_tablet_and_backend() {
        let finst_id = UniqueId::new(41, 42);
        unregister(finst_id);

        let commit = TabletCommitInfo {
            tablet_id: 101,
            backend_id: 202,
        };
        try_add_tablet_commit_info(finst_id, commit).expect("first commit fact");
        try_add_tablet_commit_info(finst_id, commit).expect("duplicate commit fact");
        try_add_tablet_commit_info(
            finst_id,
            TabletCommitInfo {
                tablet_id: 101,
                backend_id: 303,
            },
        )
        .expect("second commit fact");

        let fail = TabletFailInfo {
            tablet_id: 404,
            backend_id: 505,
        };
        try_add_tablet_fail_info(finst_id, fail).expect("first failure fact");
        try_add_tablet_fail_info(finst_id, fail).expect("duplicate failure fact");

        let snapshot = super::report_snapshot(finst_id);
        assert_eq!(
            snapshot.tablet_commit_infos,
            vec![
                commit,
                TabletCommitInfo {
                    tablet_id: 101,
                    backend_id: 303,
                },
            ]
        );
        assert_eq!(snapshot.tablet_fail_infos, vec![fail]);

        unregister(finst_id);
    }
}
