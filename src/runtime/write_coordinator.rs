//! Coordinates distributed write reports for one query.
//!
//! Lifecycle: register expected writers, apply final status reports, produce
//! exactly one commit or abort input, then unregister the query.

use std::collections::{BTreeMap, HashMap, hash_map::Entry};
use std::sync::{Arc, Mutex, OnceLock};

use crate::{frontend_service, status, status_code, types};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(crate) struct WriterKey {
    pub(crate) query_id: types::TUniqueId,
    pub(crate) fragment_instance_id: types::TUniqueId,
    pub(crate) backend_num: i32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FragmentExecStatusReport {
    pub(crate) query_id: types::TUniqueId,
    pub(crate) fragment_instance_id: types::TUniqueId,
    pub(crate) backend_num: i32,
    pub(crate) done: bool,
    pub(crate) status: status::TStatus,
    pub(crate) sink_commit_infos: Vec<types::TSinkCommitInfo>,
    pub(crate) tablet_commit_infos: Vec<types::TTabletCommitInfo>,
    pub(crate) tablet_fail_infos: Vec<types::TTabletFailInfo>,
    pub(crate) load_counters: BTreeMap<String, String>,
    pub(crate) loaded_rows: i64,
    pub(crate) loaded_bytes: i64,
    pub(crate) filtered_rows: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ReportOutcome {
    Accepted,
    Duplicate,
    CommitReady,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WriteCommitInput {
    pub(crate) write_id: types::TUniqueId,
    pub(crate) writers: Vec<WriterCommitInput>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WriterCommitInput {
    pub(crate) writer_id: usize,
    pub(crate) writer_key: WriterKey,
    pub(crate) sink_commit_infos: Vec<types::TSinkCommitInfo>,
    pub(crate) tablet_commit_infos: Vec<types::TTabletCommitInfo>,
    pub(crate) tablet_fail_infos: Vec<types::TTabletFailInfo>,
    pub(crate) load_counters: BTreeMap<String, String>,
    pub(crate) loaded_rows: i64,
    pub(crate) loaded_bytes: i64,
    pub(crate) filtered_rows: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WriteAbortInput {
    pub(crate) write_id: types::TUniqueId,
    pub(crate) reason: String,
    pub(crate) completed_writer_outputs: Vec<WriterCommitInput>,
    pub(crate) incomplete_writers: Vec<WriterKey>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum WriterState {
    Pending,
    Running {
        load_counters: BTreeMap<String, String>,
    },
    Finished(WriterCommitInput),
    Failed {
        error: String,
    },
    Canceled {
        reason: String,
    },
}

#[derive(Clone, Debug)]
struct WriterSlot {
    writer_id: usize,
    key: WriterKey,
    state: WriterState,
}

#[derive(Debug)]
pub(crate) struct WriteCoordinator {
    write_id: types::TUniqueId,
    writers: BTreeMap<WriterKey, WriterSlot>,
    failed_reason: Option<String>,
}

impl WriteCoordinator {
    pub(crate) fn new(query_id: types::TUniqueId, writers: Vec<WriterKey>) -> Result<Self, String> {
        let mut slots = BTreeMap::new();
        for (writer_id, key) in writers.into_iter().enumerate() {
            if key.query_id != query_id {
                return Err(format!(
                    "writer key query mismatch: expected {}/{}, got {}",
                    query_id.hi,
                    query_id.lo,
                    format_writer_key(&key)
                ));
            }
            if slots.contains_key(&key) {
                return Err(format!("duplicate writer key: {}", format_writer_key(&key)));
            }
            slots.insert(
                key.clone(),
                WriterSlot {
                    writer_id,
                    key,
                    state: WriterState::Pending,
                },
            );
        }
        Ok(Self {
            write_id: query_id,
            writers: slots,
            failed_reason: None,
        })
    }

    pub(crate) fn apply_report(
        &mut self,
        report: FragmentExecStatusReport,
    ) -> Result<ReportOutcome, String> {
        let key = WriterKey {
            query_id: report.query_id.clone(),
            fragment_instance_id: report.fragment_instance_id.clone(),
            backend_num: report.backend_num,
        };
        let slot = self
            .writers
            .get_mut(&key)
            .ok_or_else(|| format!("unknown writer report: {}", format_writer_key(&key)))?;

        if !report.done {
            match &mut slot.state {
                WriterState::Pending | WriterState::Running { .. } => {
                    slot.state = WriterState::Running {
                        load_counters: report.load_counters,
                    };
                    return Ok(ReportOutcome::Accepted);
                }
                WriterState::Finished(_)
                | WriterState::Failed { .. }
                | WriterState::Canceled { .. } => {
                    return Ok(ReportOutcome::Duplicate);
                }
            }
        }

        if report.status.status_code != status_code::TStatusCode::OK {
            let error = status_message(&report.status);
            match &slot.state {
                WriterState::Failed { error: existing } if existing == &error => {
                    return Ok(ReportOutcome::Duplicate);
                }
                WriterState::Failed { error: existing } => {
                    let reason = format!(
                        "conflicting final report for {}: failed writer reported different error: existing={} new={}",
                        format_writer_key(&key),
                        existing,
                        error
                    );
                    self.latch_failed_reason(reason.clone());
                    return Err(reason);
                }
                WriterState::Finished(_) => {
                    let reason = format!(
                        "conflicting final report for {}: finished writer later reported error: {}",
                        format_writer_key(&key),
                        error
                    );
                    self.latch_failed_reason(reason.clone());
                    return Err(reason);
                }
                _ => {}
            }
            slot.state = WriterState::Failed {
                error: error.clone(),
            };
            self.latch_failed_reason(error);
            return Ok(ReportOutcome::Failed);
        }

        let output = WriterCommitInput {
            writer_id: slot.writer_id,
            writer_key: slot.key.clone(),
            sink_commit_infos: report.sink_commit_infos,
            tablet_commit_infos: report.tablet_commit_infos,
            tablet_fail_infos: report.tablet_fail_infos,
            load_counters: report.load_counters,
            loaded_rows: report.loaded_rows,
            loaded_bytes: report.loaded_bytes,
            filtered_rows: report.filtered_rows,
        };

        match &slot.state {
            WriterState::Finished(existing) if existing == &output => Ok(ReportOutcome::Duplicate),
            WriterState::Finished(_) => {
                let reason = format!(
                    "conflicting final report for {}: commit metadata changed",
                    format_writer_key(&key)
                );
                self.latch_failed_reason(reason.clone());
                Err(reason)
            }
            WriterState::Failed { error } => Err(format!(
                "conflicting final report for {}: failed writer later reported OK after {}",
                format_writer_key(&key),
                error
            )),
            WriterState::Canceled { reason } => {
                tracing::debug!(
                    target: "novarocks::write_coordinator",
                    writer = %format_writer_key(&key),
                    reason = %reason,
                    "ignore late OK report for canceled writer"
                );
                Ok(ReportOutcome::Duplicate)
            }
            WriterState::Pending | WriterState::Running { .. } => {
                slot.state = WriterState::Finished(output);
                if self.failed_reason.is_some() {
                    Ok(ReportOutcome::Failed)
                } else if self.all_finished() {
                    Ok(ReportOutcome::CommitReady)
                } else {
                    Ok(ReportOutcome::Accepted)
                }
            }
        }
    }

    fn latch_failed_reason(&mut self, reason: String) {
        if self.failed_reason.is_none() {
            self.failed_reason = Some(reason);
        }
    }

    pub(crate) fn mark_canceled_except_finished(&mut self, reason: String) {
        for slot in self.writers.values_mut() {
            if matches!(
                slot.state,
                WriterState::Pending | WriterState::Running { .. }
            ) {
                slot.state = WriterState::Canceled {
                    reason: reason.clone(),
                };
            }
        }
    }

    pub(crate) fn has_failed(&self) -> bool {
        self.failed_reason.is_some()
    }

    pub(crate) fn failed_reason(&self) -> Option<String> {
        self.failed_reason.clone()
    }

    pub(crate) fn commit_input(&self) -> Result<WriteCommitInput, String> {
        if let Some(reason) = &self.failed_reason {
            return Err(format!("write failed: {reason}"));
        }
        let mut writers = Vec::with_capacity(self.writers.len());
        for slot in self.writers.values() {
            match &slot.state {
                WriterState::Finished(output) => writers.push(output.clone()),
                _ => {
                    return Err(format!(
                        "missing writer final report: {}",
                        format_writer_key(&slot.key)
                    ));
                }
            }
        }
        writers.sort_by_key(|writer| writer.writer_id);
        Ok(WriteCommitInput {
            write_id: self.write_id.clone(),
            writers,
        })
    }

    pub(crate) fn abort_input(&self) -> Option<WriteAbortInput> {
        let reason = self.failed_reason.clone()?;
        let mut completed_writer_outputs = Vec::new();
        let mut incomplete_writers = Vec::new();
        for slot in self.writers.values() {
            match &slot.state {
                WriterState::Finished(output) => completed_writer_outputs.push(output.clone()),
                _ => incomplete_writers.push((slot.writer_id, slot.key.clone())),
            }
        }
        completed_writer_outputs.sort_by_key(|writer| writer.writer_id);
        incomplete_writers.sort_by_key(|(writer_id, _)| *writer_id);
        let incomplete_writers = incomplete_writers
            .into_iter()
            .map(|(_, writer_key)| writer_key)
            .collect();
        Some(WriteAbortInput {
            write_id: self.write_id.clone(),
            reason,
            completed_writer_outputs,
            incomplete_writers,
        })
    }

    fn all_finished(&self) -> bool {
        self.writers
            .values()
            .all(|slot| matches!(slot.state, WriterState::Finished(_)))
    }
}

pub(crate) fn report_from_thrift(
    params: frontend_service::TReportExecStatusParams,
) -> Result<FragmentExecStatusReport, String> {
    let query_id = params
        .query_id
        .ok_or_else(|| "TReportExecStatusParams missing query_id".to_string())?;
    let fragment_instance_id = params
        .fragment_instance_id
        .ok_or_else(|| "TReportExecStatusParams missing fragment_instance_id".to_string())?;
    let backend_num = params
        .backend_num
        .ok_or_else(|| "TReportExecStatusParams missing backend_num".to_string())?;
    let status = params
        .status
        .ok_or_else(|| "TReportExecStatusParams missing status".to_string())?;
    let done = params
        .done
        .ok_or_else(|| "TReportExecStatusParams missing done".to_string())?;
    Ok(FragmentExecStatusReport {
        query_id,
        fragment_instance_id,
        backend_num,
        done,
        status,
        sink_commit_infos: params.sink_commit_infos.unwrap_or_default(),
        tablet_commit_infos: params.commit_infos.unwrap_or_default(),
        tablet_fail_infos: params.fail_infos.unwrap_or_default(),
        load_counters: params.load_counters.unwrap_or_default(),
        loaded_rows: params.loaded_rows.unwrap_or_default(),
        loaded_bytes: params.sink_load_bytes.unwrap_or_default(),
        filtered_rows: params.filtered_rows.unwrap_or_default(),
    })
}

fn status_message(status: &status::TStatus) -> String {
    status
        .error_msgs
        .as_ref()
        .filter(|msgs| !msgs.is_empty())
        .map(|msgs| msgs.join("; "))
        .unwrap_or_else(|| format!("status={:?}", status.status_code))
}

fn format_writer_key(key: &WriterKey) -> String {
    format!(
        "query={}/{} finst={}/{} backend_num={}",
        key.query_id.hi,
        key.query_id.lo,
        key.fragment_instance_id.hi,
        key.fragment_instance_id.lo,
        key.backend_num
    )
}

#[derive(Default)]
struct WriteCoordinatorRegistry {
    queries: Mutex<HashMap<(i64, i64), Arc<Mutex<WriteCoordinator>>>>,
}

fn registry() -> &'static WriteCoordinatorRegistry {
    static REGISTRY: OnceLock<WriteCoordinatorRegistry> = OnceLock::new();
    REGISTRY.get_or_init(WriteCoordinatorRegistry::default)
}

fn query_key(query_id: &types::TUniqueId) -> (i64, i64) {
    (query_id.hi, query_id.lo)
}

pub(crate) fn register_query(
    query_id: types::TUniqueId,
    writers: Vec<WriterKey>,
) -> Result<Arc<Mutex<WriteCoordinator>>, String> {
    let coord = Arc::new(Mutex::new(WriteCoordinator::new(
        query_id.clone(),
        writers,
    )?));
    let mut queries = registry()
        .queries
        .lock()
        .expect("write coordinator registry lock");
    match queries.entry(query_key(&query_id)) {
        Entry::Occupied(_) => Err(format!(
            "write coordinator already registered for query {}/{}",
            query_id.hi, query_id.lo
        )),
        Entry::Vacant(entry) => {
            entry.insert(Arc::clone(&coord));
            Ok(coord)
        }
    }
}

pub(crate) fn unregister_query(query_id: &types::TUniqueId) {
    registry()
        .queries
        .lock()
        .expect("write coordinator registry lock")
        .remove(&query_key(query_id));
}

pub(crate) fn handle_report_exec_status(
    params: frontend_service::TReportExecStatusParams,
) -> Result<ReportOutcome, String> {
    let report = report_from_thrift(params)?;
    let query_id = report.query_id.clone();
    let coord = registry()
        .queries
        .lock()
        .expect("write coordinator registry lock")
        .get(&query_key(&query_id))
        .cloned()
        .ok_or_else(|| {
            format!(
                "write coordinator not found for query {}/{}",
                query_id.hi, query_id.lo
            )
        })?;
    coord
        .lock()
        .expect("write coordinator lock")
        .apply_report(report)
}

#[cfg(test)]
pub(crate) fn test_clear_registry() {
    registry()
        .queries
        .lock()
        .expect("write coordinator registry lock")
        .clear();
}

#[cfg(test)]
pub(crate) struct WriteRegistryTestGuard {
    _lock: std::sync::MutexGuard<'static, ()>,
    registered_queries: Vec<types::TUniqueId>,
}

#[cfg(test)]
impl WriteRegistryTestGuard {
    pub(crate) fn register_query(
        &mut self,
        query_id: types::TUniqueId,
        writers: Vec<WriterKey>,
    ) -> Result<Arc<Mutex<WriteCoordinator>>, String> {
        let coord = register_query(query_id.clone(), writers)?;
        self.registered_queries.push(query_id);
        Ok(coord)
    }

    pub(crate) fn unregister_query(&mut self, query_id: &types::TUniqueId) {
        unregister_query(query_id);
        self.registered_queries.retain(|id| id != query_id);
    }
}

#[cfg(test)]
impl Drop for WriteRegistryTestGuard {
    fn drop(&mut self) {
        for query_id in self.registered_queries.iter().rev() {
            unregister_query(query_id);
        }
        test_clear_registry();
    }
}

#[cfg(test)]
pub(crate) fn write_registry_test_guard() -> WriteRegistryTestGuard {
    static REGISTRY_TEST_LOCK: Mutex<()> = Mutex::new(());
    let lock = REGISTRY_TEST_LOCK
        .lock()
        .expect("write coordinator registry test lock");
    test_clear_registry();
    WriteRegistryTestGuard {
        _lock: lock,
        registered_queries: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(hi: i64, lo: i64) -> types::TUniqueId {
        types::TUniqueId::new(hi, lo)
    }

    fn key(
        query_hi: i64,
        query_lo: i64,
        finst_hi: i64,
        finst_lo: i64,
        backend_num: i32,
    ) -> WriterKey {
        WriterKey {
            query_id: id(query_hi, query_lo),
            fragment_instance_id: id(finst_hi, finst_lo),
            backend_num,
        }
    }

    fn ok_status() -> status::TStatus {
        status::TStatus::new(status_code::TStatusCode::OK, None)
    }

    fn err_status(msg: &str) -> status::TStatus {
        status::TStatus::new(
            status_code::TStatusCode::INTERNAL_ERROR,
            Some(vec![msg.to_string()]),
        )
    }

    fn coord(query_id: types::TUniqueId, writers: Vec<WriterKey>) -> WriteCoordinator {
        WriteCoordinator::new(query_id, writers).expect("coordinator")
    }

    fn report(
        writer: &WriterKey,
        done: bool,
        status: status::TStatus,
        path: &str,
    ) -> FragmentExecStatusReport {
        let sink_commit_infos = if path.is_empty() {
            Vec::new()
        } else {
            vec![types::TSinkCommitInfo {
                iceberg_data_file: Some(types::TIcebergDataFile {
                    path: Some(path.to_string()),
                    record_count: Some(7),
                    file_size_in_bytes: Some(70),
                    ..Default::default()
                }),
                ..Default::default()
            }]
        };
        FragmentExecStatusReport {
            query_id: writer.query_id.clone(),
            fragment_instance_id: writer.fragment_instance_id.clone(),
            backend_num: writer.backend_num,
            done,
            status,
            sink_commit_infos,
            tablet_commit_infos: Vec::new(),
            tablet_fail_infos: Vec::new(),
            load_counters: std::collections::BTreeMap::from([
                ("dpp.norm.ALL".to_string(), "7".to_string()),
                ("loaded.bytes".to_string(), "70".to_string()),
            ]),
            loaded_rows: 7,
            loaded_bytes: 70,
            filtered_rows: 0,
        }
    }

    fn thrift_params(
        report: FragmentExecStatusReport,
    ) -> frontend_service::TReportExecStatusParams {
        frontend_service::TReportExecStatusParams::new(
            frontend_service::FrontendServiceVersion::V1,
            Some(report.query_id),
            Some(report.backend_num),
            Some(report.fragment_instance_id),
            Some(report.status),
            Some(report.done),
            None,
            Option::<Vec<String>>::None,
            Option::<Vec<String>>::None,
            Some(report.load_counters),
            None,
            Option::<Vec<String>>::None,
            Some(report.tablet_commit_infos),
            Some(report.loaded_rows),
            None,
            Some(report.loaded_bytes),
            None,
            None,
            None,
            Some(report.tablet_fail_infos),
            Some(report.filtered_rows),
            None,
            None,
            Some(report.sink_commit_infos),
            None,
            None,
            None,
        )
    }

    #[test]
    fn all_expected_writers_finish_and_commit_input_is_stable() {
        let query_id = id(10, 20);
        let writer_a = key(10, 20, 101, 201, 0);
        let writer_b = key(10, 20, 102, 202, 1);
        let mut coord = coord(query_id.clone(), vec![writer_a.clone(), writer_b.clone()]);

        assert_eq!(
            coord
                .apply_report(report(&writer_a, true, ok_status(), "s3://w/a.parquet"))
                .expect("writer a report"),
            ReportOutcome::Accepted
        );
        assert_eq!(
            coord
                .apply_report(report(&writer_b, true, ok_status(), "s3://w/b.parquet"))
                .expect("writer b report"),
            ReportOutcome::CommitReady
        );

        let input = coord.commit_input().expect("commit input");
        assert_eq!(input.write_id, query_id);
        assert_eq!(input.writers.len(), 2);
        assert_eq!(input.writers[0].writer_id, 0);
        assert_eq!(input.writers[1].writer_id, 1);
        assert_eq!(
            input.writers[0].sink_commit_infos[0]
                .iceberg_data_file
                .as_ref()
                .and_then(|f| f.path.as_deref()),
            Some("s3://w/a.parquet")
        );
        assert_eq!(
            input.writers[1].sink_commit_infos[0]
                .iceberg_data_file
                .as_ref()
                .and_then(|f| f.path.as_deref()),
            Some("s3://w/b.parquet")
        );
    }

    #[test]
    fn duplicate_identical_final_report_is_idempotent() {
        let query_id = id(11, 21);
        let writer = key(11, 21, 111, 211, 0);
        let mut coord = coord(query_id, vec![writer.clone()]);
        let first = report(&writer, true, ok_status(), "s3://w/dup.parquet");
        let duplicate = first.clone();

        assert_eq!(
            coord.apply_report(first).expect("first"),
            ReportOutcome::CommitReady
        );
        assert_eq!(
            coord.apply_report(duplicate).expect("duplicate"),
            ReportOutcome::Duplicate
        );
    }

    #[test]
    fn conflicting_duplicate_final_report_fails_fast() {
        let query_id = id(12, 22);
        let writer = key(12, 22, 112, 212, 0);
        let mut coord = coord(query_id, vec![writer.clone()]);
        coord
            .apply_report(report(
                &writer,
                true,
                ok_status(),
                "s3://w/original.parquet",
            ))
            .expect("first report");

        let err = coord
            .apply_report(report(
                &writer,
                true,
                ok_status(),
                "s3://w/conflict.parquet",
            ))
            .expect_err("conflicting duplicate must fail");
        assert!(err.contains("conflicting final report"), "{err}");
    }

    #[test]
    fn conflicting_ok_final_report_latches_failed_state() {
        let query_id = id(15, 25);
        let writer = key(15, 25, 117, 217, 0);
        let mut coord = coord(query_id.clone(), vec![writer.clone()]);
        coord
            .apply_report(report(
                &writer,
                true,
                ok_status(),
                "s3://w/original.parquet",
            ))
            .expect("first report");

        let err = coord
            .apply_report(report(
                &writer,
                true,
                ok_status(),
                "s3://w/conflict.parquet",
            ))
            .expect_err("conflicting duplicate must fail");
        assert!(err.contains("conflicting final report"), "{err}");
        let commit_err = coord.commit_input().expect_err("conflict blocks commit");
        assert!(
            commit_err.contains("conflicting final report"),
            "{commit_err}"
        );
        let abort = coord.abort_input().expect("conflict creates abort input");
        assert_eq!(abort.write_id, query_id);
        assert!(abort.reason.contains("conflicting final report"));
    }

    #[test]
    fn latched_conflict_prevents_later_commit_ready_outcome() {
        let query_id = id(23, 33);
        let writer_a = key(23, 33, 123, 223, 0);
        let writer_b = key(23, 33, 124, 224, 1);
        let mut coord = coord(query_id, vec![writer_a.clone(), writer_b.clone()]);
        coord
            .apply_report(report(
                &writer_a,
                true,
                ok_status(),
                "s3://w/original.parquet",
            ))
            .expect("writer a report");
        coord
            .apply_report(report(
                &writer_a,
                true,
                ok_status(),
                "s3://w/conflict.parquet",
            ))
            .expect_err("conflicting duplicate must fail");

        let outcome = coord
            .apply_report(report(&writer_b, true, ok_status(), "s3://w/b.parquet"))
            .expect("writer b report after conflict");
        assert_ne!(outcome, ReportOutcome::CommitReady);
        assert_eq!(outcome, ReportOutcome::Failed);
        let commit_err = coord
            .commit_input()
            .expect_err("latched conflict blocks commit");
        assert!(
            commit_err.contains("conflicting final report"),
            "{commit_err}"
        );
        assert!(coord.abort_input().is_some());
    }

    #[test]
    fn conflicting_failure_report_preserves_authoritative_reason() {
        let query_id = id(16, 26);
        let writer = key(16, 26, 118, 218, 0);
        let mut coord = coord(query_id.clone(), vec![writer.clone()]);
        assert_eq!(
            coord
                .apply_report(report(&writer, true, err_status("first failure"), ""))
                .expect("first failure report"),
            ReportOutcome::Failed
        );

        let err = coord
            .apply_report(report(&writer, true, err_status("second failure"), ""))
            .expect_err("different failure report must conflict");
        assert!(err.contains("conflicting final report"), "{err}");
        let commit_err = coord.commit_input().expect_err("failure blocks commit");
        assert!(commit_err.contains("first failure"), "{commit_err}");
        let abort = coord.abort_input().expect("failure creates abort input");
        assert_eq!(abort.write_id, query_id);
        assert!(abort.reason.contains("first failure"), "{}", abort.reason);
        assert!(!abort.reason.contains("second failure"), "{}", abort.reason);
    }

    #[test]
    fn writer_failure_builds_abort_input_and_blocks_commit() {
        let query_id = id(13, 23);
        let writer_a = key(13, 23, 113, 213, 0);
        let writer_b = key(13, 23, 114, 214, 1);
        let mut coord = coord(query_id.clone(), vec![writer_a.clone(), writer_b.clone()]);

        coord
            .apply_report(report(&writer_a, true, ok_status(), "s3://w/done.parquet"))
            .expect("first writer ok");
        assert_eq!(
            coord
                .apply_report(report(&writer_b, true, err_status("writer failed"), ""))
                .expect("failed writer report"),
            ReportOutcome::Failed
        );

        let err = coord
            .commit_input()
            .expect_err("failed write cannot commit");
        assert!(err.contains("failed"), "{err}");
        let abort = coord.abort_input().expect("abort input");
        assert_eq!(abort.write_id, query_id);
        assert!(abort.reason.contains("writer failed"), "{}", abort.reason);
        assert_eq!(abort.completed_writer_outputs.len(), 1);
        assert_eq!(abort.incomplete_writers.len(), 1);
    }

    #[test]
    fn missing_writer_prevents_commit() {
        let query_id = id(14, 24);
        let writer_a = key(14, 24, 115, 215, 0);
        let writer_b = key(14, 24, 116, 216, 1);
        let mut coord = coord(query_id, vec![writer_a.clone(), writer_b]);
        coord
            .apply_report(report(&writer_a, true, ok_status(), "s3://w/only.parquet"))
            .expect("writer a report");

        let err = coord
            .commit_input()
            .expect_err("missing writer must block commit");
        assert!(err.contains("missing writer"), "{err}");
    }

    #[test]
    fn construction_rejects_duplicate_writers_and_query_mismatches() {
        let query_id = id(17, 27);
        let writer = key(17, 27, 119, 219, 0);
        let duplicate_err =
            WriteCoordinator::new(query_id.clone(), vec![writer.clone(), writer.clone()])
                .expect_err("duplicate writer key must fail");
        assert!(
            duplicate_err.contains("duplicate writer key"),
            "{duplicate_err}"
        );

        let wrong_query_writer = key(99, 99, 120, 220, 1);
        let mismatch_err = WriteCoordinator::new(query_id, vec![wrong_query_writer])
            .expect_err("writer query mismatch must fail");
        assert!(mismatch_err.contains("query mismatch"), "{mismatch_err}");
    }

    #[test]
    fn commit_outputs_follow_writer_registration_order() {
        let query_id = id(18, 28);
        let writer_a = key(18, 28, 220, 320, 0);
        let writer_b = key(18, 28, 120, 220, 1);
        let mut coord = coord(query_id, vec![writer_a.clone(), writer_b.clone()]);

        coord
            .apply_report(report(&writer_b, true, ok_status(), "s3://w/b.parquet"))
            .expect("writer b report");
        coord
            .apply_report(report(&writer_a, true, ok_status(), "s3://w/a.parquet"))
            .expect("writer a report");
        let commit = coord.commit_input().expect("commit input");
        assert_eq!(commit.writers[0].writer_id, 0);
        assert_eq!(commit.writers[0].writer_key, writer_a);
        assert_eq!(commit.writers[1].writer_id, 1);
        assert_eq!(commit.writers[1].writer_key, writer_b);
    }

    #[test]
    fn abort_outputs_follow_writer_registration_order() {
        let query_id = id(22, 32);
        let writer_a = key(22, 32, 420, 520, 0);
        let writer_b = key(22, 32, 120, 220, 1);
        let writer_c = key(22, 32, 320, 420, 2);
        let writer_d = key(22, 32, 20, 120, 3);
        let mut coord = coord(
            query_id,
            vec![
                writer_a.clone(),
                writer_b.clone(),
                writer_c.clone(),
                writer_d.clone(),
            ],
        );

        coord
            .apply_report(report(&writer_c, true, ok_status(), "s3://w/c.parquet"))
            .expect("writer c report");
        coord
            .apply_report(report(&writer_a, true, ok_status(), "s3://w/a.parquet"))
            .expect("writer a report");
        coord
            .apply_report(report(&writer_b, true, err_status("writer b failed"), ""))
            .expect("writer b failure");

        let abort = coord.abort_input().expect("abort input");
        assert_eq!(
            abort
                .completed_writer_outputs
                .iter()
                .map(|writer| writer.writer_id)
                .collect::<Vec<_>>(),
            vec![0, 2]
        );
        assert_eq!(
            abort.incomplete_writers,
            vec![writer_b.clone(), writer_d.clone()]
        );
    }

    #[test]
    fn registry_registers_handles_reports_unregisters_and_rejects_unknown_query() {
        let mut guard = write_registry_test_guard();
        let query_id = id(19, 29);
        let writer = key(19, 29, 121, 221, 0);
        let coord = guard
            .register_query(query_id.clone(), vec![writer.clone()])
            .expect("register write coordinator");

        assert_eq!(
            handle_report_exec_status(thrift_params(report(
                &writer,
                true,
                ok_status(),
                "s3://w/registry.parquet"
            )))
            .expect("handle report"),
            ReportOutcome::CommitReady
        );
        let commit = coord
            .lock()
            .expect("write coordinator lock")
            .commit_input()
            .expect("commit input");
        assert_eq!(commit.write_id, query_id);

        guard.unregister_query(&query_id);
        let err = handle_report_exec_status(thrift_params(report(
            &writer,
            true,
            ok_status(),
            "s3://w/late.parquet",
        )))
        .expect_err("unregistered query must fail");
        assert!(err.contains("not found"), "{err}");
    }

    #[test]
    fn registry_handles_two_writer_reports_and_builds_commit_input() {
        let mut guard = write_registry_test_guard();
        let query_id = id(24, 34);
        let writer_a = key(24, 34, 124, 224, 0);
        let writer_b = key(24, 34, 125, 225, 1);
        let coord = guard
            .register_query(query_id.clone(), vec![writer_a.clone(), writer_b.clone()])
            .expect("register two writer coordinator");

        assert_eq!(
            handle_report_exec_status(thrift_params(report(
                &writer_a,
                true,
                ok_status(),
                "s3://w/registry-a.parquet"
            )))
            .expect("handle writer a report"),
            ReportOutcome::Accepted
        );
        assert_eq!(
            handle_report_exec_status(thrift_params(report(
                &writer_b,
                true,
                ok_status(),
                "s3://w/registry-b.parquet"
            )))
            .expect("handle writer b report"),
            ReportOutcome::CommitReady
        );

        let commit = coord
            .lock()
            .expect("write coordinator lock")
            .commit_input()
            .expect("commit input");
        assert_eq!(commit.write_id, query_id);
        assert_eq!(commit.writers.len(), 2);
        assert_eq!(commit.writers[0].writer_id, 0);
        assert_eq!(commit.writers[1].writer_id, 1);
        assert_eq!(commit.writers[0].writer_key, writer_a);
        assert_eq!(commit.writers[1].writer_key, writer_b);
        assert_eq!(
            commit.writers[0].sink_commit_infos[0]
                .iceberg_data_file
                .as_ref()
                .and_then(|f| f.path.as_deref()),
            Some("s3://w/registry-a.parquet")
        );
        assert_eq!(
            commit.writers[1].sink_commit_infos[0]
                .iceberg_data_file
                .as_ref()
                .and_then(|f| f.path.as_deref()),
            Some("s3://w/registry-b.parquet")
        );
    }

    #[test]
    fn registry_rejects_duplicate_query_registration() {
        let mut guard = write_registry_test_guard();
        let query_id = id(20, 30);
        let writer = key(20, 30, 122, 222, 0);
        guard
            .register_query(query_id.clone(), vec![writer.clone()])
            .expect("first registration");
        let err = register_query(query_id.clone(), vec![writer])
            .expect_err("duplicate query registration must fail");
        assert!(err.contains("already registered"), "{err}");
    }

    #[test]
    fn thrift_report_requires_identity_and_status() {
        let params = frontend_service::TReportExecStatusParams::new(
            frontend_service::FrontendServiceVersion::V1,
            None,
            Some(0),
            None,
            Some(ok_status()),
            Some(true),
            None,
            Option::<Vec<String>>::None,
            Option::<Vec<String>>::None,
            None,
            None,
            Option::<Vec<String>>::None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        let err = report_from_thrift(params).expect_err("missing ids must fail");
        assert!(err.contains("query_id"), "{err}");
    }

    #[test]
    fn thrift_report_maps_commit_and_load_fields() {
        let query_id = id(21, 31);
        let finst_id = id(123, 223);
        let sink_commit = types::TSinkCommitInfo {
            iceberg_data_file: Some(types::TIcebergDataFile {
                path: Some("s3://w/from-thrift.parquet".to_string()),
                record_count: Some(123),
                file_size_in_bytes: Some(456),
                ..Default::default()
            }),
            ..Default::default()
        };
        let tablet_commit =
            types::TTabletCommitInfo::new(1001, 2002, None, Some(vec!["c1".to_string()]), None);
        let tablet_fail = types::TTabletFailInfo::new(Some(3003), Some(4004));
        let load_counters = BTreeMap::from([
            ("dpp.norm.ALL".to_string(), "123".to_string()),
            ("loaded.bytes".to_string(), "456".to_string()),
        ]);
        let params = frontend_service::TReportExecStatusParams::new(
            frontend_service::FrontendServiceVersion::V1,
            Some(query_id.clone()),
            Some(7),
            Some(finst_id.clone()),
            Some(ok_status()),
            Some(true),
            None,
            Option::<Vec<String>>::None,
            Option::<Vec<String>>::None,
            Some(load_counters.clone()),
            None,
            Option::<Vec<String>>::None,
            Some(vec![tablet_commit.clone()]),
            Some(123),
            None,
            Some(456),
            None,
            None,
            None,
            Some(vec![tablet_fail.clone()]),
            Some(5),
            None,
            None,
            Some(vec![sink_commit.clone()]),
            None,
            None,
            None,
        );

        let report = report_from_thrift(params).expect("thrift report");
        assert_eq!(report.query_id, query_id);
        assert_eq!(report.fragment_instance_id, finst_id);
        assert_eq!(report.backend_num, 7);
        assert!(report.done);
        assert_eq!(report.sink_commit_infos, vec![sink_commit]);
        assert_eq!(report.tablet_commit_infos, vec![tablet_commit]);
        assert_eq!(report.tablet_fail_infos, vec![tablet_fail]);
        assert_eq!(report.load_counters, load_counters);
        assert_eq!(report.loaded_rows, 123);
        assert_eq!(report.loaded_bytes, 456);
        assert_eq!(report.filtered_rows, 5);
    }
}
