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

use novarocks_execution::runtime::task::io::{
    TabletCommitInfo as ExecutionTabletCommitInfo, TabletFailInfo as ExecutionTabletFailInfo,
    TaskCommitPort, TaskCommitRegistration, TaskCommitReport, TaskSinkLoadStats,
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

impl TaskCommitPort for WorkerSinkCommitPort {
    fn acquire(
        &self,
        task_key: UniqueId,
    ) -> novarocks_execution::runtime::task::ExecutionResult<Box<dyn TaskCommitRegistration>> {
        acquire_worker_sink_commit_lease(task_key, WriteCommitEvidenceLimits::default())
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

impl TaskCommitPort for ConfiguredWorkerSinkCommitPort {
    fn acquire(
        &self,
        task_key: UniqueId,
    ) -> novarocks_execution::runtime::task::ExecutionResult<Box<dyn TaskCommitRegistration>> {
        acquire_worker_sink_commit_lease(task_key, self.evidence_limits)
    }
}

fn acquire_worker_sink_commit_lease(
    task_key: UniqueId,
    evidence_limits: WriteCommitEvidenceLimits,
) -> novarocks_execution::runtime::task::ExecutionResult<Box<dyn TaskCommitRegistration>> {
    if !try_register_with_ledger(task_key, WriteCommitEvidenceLedger::new(evidence_limits)) {
        return Err(
            novarocks_execution::runtime::task::ExecutionFailure::from_domain(
                DuplicateCommitRegistration(task_key),
            ),
        );
    }
    Ok(Box::new(WorkerSinkCommitRegistration {
        task_key,
        active: true,
    }))
}

struct WorkerSinkCommitRegistration {
    task_key: UniqueId,
    active: bool,
}

impl WorkerSinkCommitRegistration {
    fn require_registration_permission(
        &self,
        operation: SinkCommitOperation,
    ) -> novarocks_execution::runtime::task::ExecutionResult<()> {
        if !self.active {
            return Err(
                novarocks_execution::runtime::task::ExecutionFailure::from_domain(
                    SinkCommitAfterSettled {
                        task_key: self.task_key,
                        operation,
                    },
                ),
            );
        }
        Ok(())
    }

    fn snapshot(&self) -> TaskCommitReport {
        let snapshot = report_snapshot(self.task_key);
        TaskCommitReport {
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
            load_stats: TaskSinkLoadStats {
                loaded_rows: snapshot.load_stats.loaded_rows,
                loaded_bytes: snapshot.load_stats.loaded_bytes,
                filtered_rows: snapshot.load_stats.filtered_rows,
            },
        }
    }
}

impl TaskCommitRegistration for WorkerSinkCommitRegistration {
    fn add_load_stats(
        &mut self,
        stats: TaskSinkLoadStats,
    ) -> novarocks_execution::runtime::task::ExecutionResult<()> {
        self.require_registration_permission(SinkCommitOperation::AddLoadStats)?;
        add_load_stats(
            self.task_key,
            stats.loaded_rows,
            stats.loaded_bytes,
            stats.filtered_rows,
        );
        Ok(())
    }

    fn add_tablet_commit_info(
        &mut self,
        info: ExecutionTabletCommitInfo,
    ) -> novarocks_execution::runtime::task::ExecutionResult<()> {
        self.require_registration_permission(SinkCommitOperation::AddTabletCommitInfo)?;
        try_add_tablet_commit_info(
            self.task_key,
            TabletCommitInfo {
                tablet_id: info.tablet_id,
                backend_id: info.backend_id,
            },
        )
    }

    fn add_tablet_fail_info(
        &mut self,
        info: ExecutionTabletFailInfo,
    ) -> novarocks_execution::runtime::task::ExecutionResult<()> {
        self.require_registration_permission(SinkCommitOperation::AddTabletFailInfo)?;
        try_add_tablet_fail_info(
            self.task_key,
            TabletFailInfo {
                tablet_id: info.tablet_id,
                backend_id: info.backend_id,
            },
        )
    }

    fn finish(&mut self) -> novarocks_execution::runtime::task::ExecutionResult<TaskCommitReport> {
        self.require_registration_permission(SinkCommitOperation::Finish)?;
        self.active = false;
        let snapshot = self.snapshot();
        unregister(self.task_key);
        Ok(snapshot)
    }

    fn handoff(&mut self) -> novarocks_execution::runtime::task::ExecutionResult<()> {
        self.require_registration_permission(SinkCommitOperation::Handoff)?;
        self.active = false;
        Ok(())
    }

    fn rollback(&mut self) -> novarocks_execution::runtime::task::ExecutionResult<()> {
        self.require_registration_permission(SinkCommitOperation::Rollback)?;
        self.active = false;
        unregister(self.task_key);
        Ok(())
    }
}

impl Drop for WorkerSinkCommitRegistration {
    fn drop(&mut self) {
        if self.active {
            unregister(self.task_key);
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SinkCommitOperation {
    AddLoadStats,
    AddTabletCommitInfo,
    AddTabletFailInfo,
    Finish,
    Handoff,
    Rollback,
}
#[derive(Debug)]
struct SinkCommitAfterSettled {
    task_key: UniqueId,
    operation: SinkCommitOperation,
}
impl std::fmt::Display for SinkCommitAfterSettled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "sink commit lease for fragment instance {} is settled before {:?}",
            self.task_key, self.operation
        )
    }
}
impl std::error::Error for SinkCommitAfterSettled {}
impl novarocks_execution_contract::failure::FailureProjection for SinkCommitAfterSettled {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::{FailureClass, FailureFact};
        FailureFact::new(
            FailureClass::Invariant,
            crate::failure_reason::WorkerFailureReason::SinkCommitAfterSettled.reason(),
        )
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
    task_key: UniqueId,
    evidence_ledger: WriteCommitEvidenceLedger,
) -> bool {
    let store = store();
    let mut guard = store.mu.lock().expect("sink commit store lock");
    if guard.contains_key(&task_key) {
        return false;
    }
    guard.insert(task_key, SinkCommitEntry::new(evidence_ledger));
    true
}

fn unregister(task_key: UniqueId) {
    let store = store();
    let mut guard = store.mu.lock().expect("sink commit store lock");
    guard.remove(&task_key);
}

fn try_add_tablet_commit_info(
    task_key: UniqueId,
    info: TabletCommitInfo,
) -> novarocks_execution::runtime::task::ExecutionResult<()> {
    let store = store();
    let mut guard = store.mu.lock().expect("sink commit store lock");
    let entry = guard
        .entry(task_key)
        .or_insert_with(|| SinkCommitEntry::new(WriteCommitEvidenceLedger::default()));
    if !entry.tablet_commit_infos.contains(&info) {
        entry
            .evidence_ledger
            .reserve(TABLET_TERMINAL_CANONICAL_BYTES, 1)
            .map_err(|source| {
                novarocks_execution::runtime::task::ExecutionFailure::from_domain(
                    CommitEvidenceFailure {
                        operation: CommitEvidenceOperation::Commit,
                        source,
                    },
                )
            })?;
        entry.tablet_commit_infos.push(info);
    }
    Ok(())
}

fn try_add_tablet_fail_info(
    task_key: UniqueId,
    info: TabletFailInfo,
) -> novarocks_execution::runtime::task::ExecutionResult<()> {
    let store = store();
    let mut guard = store.mu.lock().expect("sink commit store lock");
    let entry = guard
        .entry(task_key)
        .or_insert_with(|| SinkCommitEntry::new(WriteCommitEvidenceLedger::default()));
    if !entry.tablet_fail_infos.contains(&info) {
        entry
            .evidence_ledger
            .reserve(TABLET_TERMINAL_CANONICAL_BYTES, 1)
            .map_err(|source| {
                novarocks_execution::runtime::task::ExecutionFailure::from_domain(
                    CommitEvidenceFailure {
                        operation: CommitEvidenceOperation::Failure,
                        source,
                    },
                )
            })?;
        entry.tablet_fail_infos.push(info);
    }
    Ok(())
}

fn add_load_stats(task_key: UniqueId, loaded_rows: i64, loaded_bytes: i64, filtered_rows: i64) {
    let store = store();
    let mut guard = store.mu.lock().expect("sink commit store lock");
    let entry = guard.entry(task_key).or_default();
    entry.loaded_rows = entry.loaded_rows.saturating_add(loaded_rows.max(0));
    entry.loaded_bytes = entry.loaded_bytes.saturating_add(loaded_bytes.max(0));
    entry.filtered_rows = entry.filtered_rows.saturating_add(filtered_rows.max(0));
}

pub(crate) fn report_snapshot(task_key: UniqueId) -> SinkCommitReportSnapshot {
    let store = store();
    let guard = store.mu.lock().expect("sink commit store lock");
    let Some(entry) = guard.get(&task_key) else {
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
        let task_key = UniqueId::new(41, 42);
        unregister(task_key);

        let commit = TabletCommitInfo {
            tablet_id: 101,
            backend_id: 202,
        };
        try_add_tablet_commit_info(task_key, commit).expect("first commit fact");
        try_add_tablet_commit_info(task_key, commit).expect("duplicate commit fact");
        try_add_tablet_commit_info(
            task_key,
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
        try_add_tablet_fail_info(task_key, fail).expect("first failure fact");
        try_add_tablet_fail_info(task_key, fail).expect("duplicate failure fact");

        let snapshot = super::report_snapshot(task_key);
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

        unregister(task_key);
    }
    #[test]
    fn borrowed_commit_settlement_and_box_drop_preserve_the_replacement_epoch() {
        use novarocks_execution::runtime::task::io::TaskCommitPort;
        let port = super::WorkerSinkCommitPort;
        for (ordinal, finish) in [false, true].into_iter().enumerate() {
            let identity = UniqueId::new(44_598, ordinal as i64);
            let mut original = port.acquire(identity).unwrap();
            assert!(
                port.acquire(identity).is_err(),
                "the actual original registration is occupied"
            );
            if finish {
                original.finish().unwrap();
            } else {
                original.rollback().unwrap();
            }
            let mut replacement = port.acquire(identity).unwrap();
            drop(original);
            assert!(
                port.acquire(identity).is_err(),
                "an old Box Drop must not unregister the new epoch"
            );
            replacement.rollback().unwrap();
            let mut final_lease = port.acquire(identity).unwrap();
            final_lease.rollback().unwrap();
        }
        let identity = UniqueId::new(44_598, 2);
        let mut handed_off = port.acquire(identity).unwrap();
        handed_off.handoff().unwrap();
        drop(handed_off);
        assert!(
            port.acquire(identity).is_err(),
            "handoff retains the host registration after lease Box Drop"
        );
        super::unregister(identity);
        let mut replacement = port.acquire(identity).unwrap();
        replacement.rollback().unwrap();
    }

    #[test]
    fn settled_borrowed_lease_rejects_every_operation_without_touching_same_id_replacement() {
        use super::{SinkCommitAfterSettled, SinkCommitOperation};
        use novarocks_execution::runtime::task::{
            ExecutionFailureCause,
            io::{
                TabletCommitInfo as Commit, TabletFailInfo as Failed, TaskCommitPort,
                TaskSinkLoadStats,
            },
        };
        use novarocks_execution_contract::failure::{FailureClass, FailureProjection};
        let port = super::WorkerSinkCommitPort;
        for (ordinal, settlement) in [
            SinkCommitOperation::Finish,
            SinkCommitOperation::Handoff,
            SinkCommitOperation::Rollback,
        ]
        .into_iter()
        .enumerate()
        {
            let identity = UniqueId::new(44_608, ordinal as i64);
            let mut original = port.acquire(identity).unwrap();
            original
                .add_load_stats(TaskSinkLoadStats {
                    loaded_rows: 1,
                    loaded_bytes: 2,
                    filtered_rows: 3,
                })
                .unwrap();
            match settlement {
                SinkCommitOperation::Finish => {
                    let report = original.finish().unwrap();
                    assert_eq!(report.load_stats.loaded_rows, 1);
                }
                SinkCommitOperation::Handoff => {
                    original.handoff().unwrap();
                    assert_eq!(super::report_snapshot(identity).load_stats.loaded_rows, 1);
                    assert!(
                        port.acquire(identity).is_err(),
                        "handoff retains the actual host registration"
                    );
                    super::unregister(identity); // The original host's explicit final retirement.
                }
                SinkCommitOperation::Rollback => original.rollback().unwrap(),
                _ => unreachable!("only settlement authors in this test"),
            }
            let mut replacement = port.acquire(identity).unwrap();
            replacement
                .add_load_stats(TaskSinkLoadStats {
                    loaded_rows: 17,
                    loaded_bytes: 23,
                    filtered_rows: 29,
                })
                .unwrap();
            let commit = Commit {
                tablet_id: 31,
                backend_id: 37,
            };
            replacement.add_tablet_commit_info(commit).unwrap();
            let expected = super::report_snapshot(identity);
            for operation in [
                SinkCommitOperation::AddLoadStats,
                SinkCommitOperation::AddTabletCommitInfo,
                SinkCommitOperation::AddTabletFailInfo,
                SinkCommitOperation::Finish,
                SinkCommitOperation::Handoff,
                SinkCommitOperation::Rollback,
            ] {
                let error = match operation {
                    SinkCommitOperation::AddLoadStats => {
                        original.add_load_stats(TaskSinkLoadStats {
                            loaded_rows: 1000,
                            loaded_bytes: 2000,
                            filtered_rows: 3000,
                        })
                    }
                    SinkCommitOperation::AddTabletCommitInfo => {
                        original.add_tablet_commit_info(Commit {
                            tablet_id: 41,
                            backend_id: 43,
                        })
                    }
                    SinkCommitOperation::AddTabletFailInfo => {
                        original.add_tablet_fail_info(Failed {
                            tablet_id: 47,
                            backend_id: 53,
                        })
                    }
                    SinkCommitOperation::Finish => original.finish().map(|_| ()),
                    SinkCommitOperation::Handoff => original.handoff(),
                    SinkCommitOperation::Rollback => original.rollback(),
                }
                .unwrap_err();
                assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
                assert_eq!(
                    error.failure_fact().reason(),
                    crate::failure_reason::WorkerFailureReason::SinkCommitAfterSettled.reason()
                );
                let ExecutionFailureCause::Domain(source) = error.cause() else {
                    panic!("the real Worker author is retained");
                };
                let source = source.downcast_ref::<SinkCommitAfterSettled>().unwrap();
                assert_eq!(source.task_key, identity);
                assert_eq!(source.operation, operation);
                assert_eq!(
                    super::report_snapshot(identity),
                    expected,
                    "a settled owner cannot mutate or retire a new registration"
                );
            }
            drop(original);
            assert_eq!(
                super::report_snapshot(identity),
                expected,
                "old owner Drop cannot roll back a new registration"
            );
            let report = replacement.finish().unwrap();
            assert_eq!(report.load_stats.loaded_rows, 17);
            assert_eq!(report.load_stats.loaded_bytes, 23);
            assert_eq!(report.load_stats.filtered_rows, 29);
            assert_eq!(report.tablet_commit_infos, vec![commit]);
            assert!(report.tablet_fail_infos.is_empty());
            drop(replacement);
            let mut final_owner = port.acquire(identity).unwrap();
            final_owner.rollback().unwrap();
        }
    }

    #[test]
    fn actual_commit_lease_preserves_evidence_refusal_and_does_not_publish_a_partial_fact() {
        use novarocks_execution::runtime::task::io::{
            TabletCommitInfo as Commit, TabletFailInfo as Failed, TaskCommitPort,
        };
        use novarocks_execution_contract::failure::{
            FailureClass, FailureProjection, ResourceDimension, ResourceScope, domain,
        };
        use std::error::Error;
        let identity = UniqueId::new(41, 43);
        let port = super::ConfiguredWorkerSinkCommitPort::new(
            novarocks_spi::connector::WriteCommitEvidenceLimits::try_new(16, 1).unwrap(),
        );
        let mut lease = port.acquire(identity).unwrap();
        let committed = Commit {
            tablet_id: 8,
            backend_id: 9,
        };
        lease.add_tablet_commit_info(committed).unwrap();
        lease.add_tablet_commit_info(committed).unwrap();
        let failure = lease
            .add_tablet_fail_info(Failed {
                tablet_id: 10,
                backend_id: 11,
            })
            .unwrap_err();
        let fact = failure.failure_fact();
        assert_eq!(fact.class(), FailureClass::Resource);
        assert_eq!(fact.reason().domain(), domain::CONNECTOR.get());
        assert_eq!(fact.reason().code(), 47);
        let resource = fact.resource().unwrap();
        assert_eq!(resource.dimension, ResourceDimension::Bytes);
        assert_eq!(resource.requested, Some(16));
        assert_eq!(resource.committed, Some(16));
        assert_eq!(resource.limit, Some(16));
        assert_eq!(resource.constraint_scope, Some(ResourceScope::Task));
        let mut source = failure.source();
        let mut connector = false;
        while let Some(error) = source {
            if let Some(original) = error.downcast_ref::<novarocks_spi::connector::ConnectorError>()
            {
                assert_eq!(original.failure_fact(), fact);
                assert_eq!(
                    original.message(),
                    "write commit evidence exceeds fragment budget: bytes 32/16 entries 2/1"
                );
                connector = true;
            }
            source = error.source();
        }
        assert!(
            connector,
            "the real SPI source survives the Worker context and Execution envelope"
        );
        let report = lease.finish().unwrap();
        assert_eq!(report.tablet_commit_infos, vec![committed]);
        assert!(report.tablet_fail_infos.is_empty());
        assert_eq!(
            super::report_snapshot(identity),
            super::SinkCommitReportSnapshot::default()
        );
        let mut replacement = port.acquire(identity).unwrap();
        replacement.rollback().unwrap();
    }
}

#[derive(Debug)]
struct DuplicateCommitRegistration(UniqueId);
impl std::fmt::Display for DuplicateCommitRegistration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "sink commit already registered for fragment instance {}",
            self.0
        )
    }
}
impl std::error::Error for DuplicateCommitRegistration {}
impl novarocks_execution_contract::failure::FailureProjection for DuplicateCommitRegistration {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::*;
        FailureFact::new(
            FailureClass::Invariant,
            crate::failure_reason::WorkerFailureReason::SinkCommitAlreadyRegistered.reason(),
        )
    }
}

#[derive(Debug)]
enum CommitEvidenceOperation {
    Commit,
    Failure,
}
#[derive(Debug)]
struct CommitEvidenceFailure {
    operation: CommitEvidenceOperation,
    source: novarocks_spi::connector::ConnectorError,
}
impl std::fmt::Display for CommitEvidenceFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let operation = match self.operation {
            CommitEvidenceOperation::Commit => "commit",
            CommitEvidenceOperation::Failure => "failure",
        };
        write!(f, "reserve tablet {operation} evidence: {}", self.source)
    }
}
impl std::error::Error for CommitEvidenceFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
impl novarocks_execution_contract::failure::FailureProjection for CommitEvidenceFailure {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        novarocks_execution_contract::failure::FailureProjection::failure_fact(&self.source)
    }
}
