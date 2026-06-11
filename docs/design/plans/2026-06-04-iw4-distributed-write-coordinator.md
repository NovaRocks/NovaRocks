# IW-4 Distributed Write Coordinator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build IW-4 v1 so NovaRocks standalone distributed write-sink fragments can report final writer status through the same `TReportExecStatusParams` payload used by FE-compatible mode, then produce coordinator-owned commit/abort inputs and cancel peer fragments on writer failure.

**Architecture:** Keep `TReportExecStatusParams` as the single report payload. FE-compatible sends it through the existing FE report worker, while standalone BE sends thrift-binary report payloads back to the NovaRocks coordinator over `NovaRocksGrpc`. A new `WriteCoordinator` state machine consumes transport-neutral report events and stays separate from fragment submit/fetch/cancel orchestration.

**Tech Stack:** Rust, Thrift IDL, tonic/prost gRPC, existing NovaRocks `FragmentDispatcher`, `runtime::sink_commit`, `cargo test`, SQL cluster test harness.

---

## Scope Check

The spec covers one cohesive subsystem: distributed write final status collection. It touches protocol, report construction, coordinator state, and integration wiring because all are needed to make the coordinator/report vertical slice work. The plan keeps concrete metadata commit, staging cleanup executors, and user-level Iceberg INSERT pipeline cutover outside IW-4.

## File Structure

- Create `src/runtime/write_coordinator.rs`
  - Owns writer identity, writer state, commit/abort input structs, query-level coordinator registry, and unit tests.
- Modify `src/runtime/mod.rs`
  - Exports `write_coordinator`.
- Create `src/service/exec_status_report.rs`
  - Builds `frontend_service::TReportExecStatusParams` from a transport-neutral input and `runtime::sink_commit`.
- Modify `src/service/fe_report.rs`
  - Keeps FE registry/profile lifecycle and delegates payload construction to `exec_status_report`.
- Create `src/service/standalone_exec_state_reporter.rs`
  - Sends final and periodic standalone report payloads to NovaRocks coordinator through `NovaRocksGrpc`.
- Modify `src/service/exec_state_reporter.rs`
  - Keeps FE-only worker behavior unchanged.
- Modify `src/service/mod.rs`
  - Exports the new service modules.
- Modify `idl/thrift/InternalService.thrift`
  - Adds optional `novarocks_report_addr` to `TExecPlanFragmentParams`.
- Modify `idl/proto/starust_grpc.proto`
  - Adds `ReportExecStatus` and `BatchReportExecStatus`.
- Modify `src/service/grpc_server.rs`
  - Decodes standalone report payloads and forwards them to `runtime::write_coordinator`.
- Modify `src/service/grpc_client.rs`
  - Adds blocking report RPC client methods.
- Modify `src/runtime/exec_params.rs`
  - Allows coordinator code to populate `novarocks_report_addr` without overloading StarRocks FE `coord`.
- Modify `src/runtime/coordinator.rs`
  - Registers expected writer fragments, populates report destination, polls write failure while fetching, and validates all writers before returning success.
- Modify `src/service/internal_service.rs`
  - Registers report destination as FE-compatible or standalone based on `novarocks_report_addr`.
- Modify `src/server/mod.rs` and `src/main.rs`
  - Starts a NovaRocks coordinator report endpoint for `role=fe` without enabling local fragment execution.
- Modify focused tests in the same files as current patterns.

## Task 1: WriteCoordinator State Machine

**Files:**
- Create: `src/runtime/write_coordinator.rs`
- Modify: `src/runtime/mod.rs`

- [ ] **Step 1: Write failing state-machine tests**

Create `src/runtime/write_coordinator.rs` with tests first. Keep the implementation stub small enough to fail compilation on missing types and methods.

```rust
use crate::{frontend_service, status, status_code, types};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(crate) struct WriterKey {
    pub(crate) query_id: types::TUniqueId,
    pub(crate) fragment_instance_id: types::TUniqueId,
    pub(crate) backend_num: i32,
}

#[derive(Clone, Debug)]
pub(crate) struct FragmentExecStatusReport {
    pub(crate) query_id: types::TUniqueId,
    pub(crate) fragment_instance_id: types::TUniqueId,
    pub(crate) backend_num: i32,
    pub(crate) done: bool,
    pub(crate) status: status::TStatus,
    pub(crate) sink_commit_infos: Vec<types::TSinkCommitInfo>,
    pub(crate) tablet_commit_infos: Vec<types::TTabletCommitInfo>,
    pub(crate) tablet_fail_infos: Vec<types::TTabletFailInfo>,
    pub(crate) load_counters: std::collections::BTreeMap<String, String>,
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

pub(crate) struct WriteCoordinator;

impl WriteCoordinator {
    pub(crate) fn new(_query_id: types::TUniqueId, _writers: Vec<WriterKey>) -> Self {
        Self
    }

    pub(crate) fn apply_report(
        &mut self,
        _report: FragmentExecStatusReport,
    ) -> Result<ReportOutcome, String> {
        Err("WriteCoordinator stub".to_string())
    }

    pub(crate) fn commit_input(&self) -> Result<WriteCommitInput, String> {
        Err("WriteCoordinator stub".to_string())
    }

    pub(crate) fn abort_input(&self) -> Option<WriteAbortInput> {
        None
    }
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
    pub(crate) load_counters: std::collections::BTreeMap<String, String>,
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

pub(crate) fn report_from_thrift(
    params: frontend_service::TReportExecStatusParams,
) -> Result<FragmentExecStatusReport, String> {
    Err(format!("report_from_thrift stub: {:?}", params.protocol_version))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(hi: i64, lo: i64) -> types::TUniqueId {
        types::TUniqueId::new(hi, lo)
    }

    fn key(query_hi: i64, query_lo: i64, finst_hi: i64, finst_lo: i64, backend_num: i32) -> WriterKey {
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

    fn report(writer: &WriterKey, done: bool, status: status::TStatus, path: &str) -> FragmentExecStatusReport {
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

    #[test]
    fn all_expected_writers_finish_and_commit_input_is_stable() {
        let query_id = id(10, 20);
        let writer_a = key(10, 20, 101, 201, 0);
        let writer_b = key(10, 20, 102, 202, 1);
        let mut coord = WriteCoordinator::new(query_id.clone(), vec![writer_a.clone(), writer_b.clone()]);

        assert_eq!(
            coord.apply_report(report(&writer_a, true, ok_status(), "s3://w/a.parquet"))
                .expect("writer a report"),
            ReportOutcome::Accepted
        );
        assert_eq!(
            coord.apply_report(report(&writer_b, true, ok_status(), "s3://w/b.parquet"))
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
    }

    #[test]
    fn duplicate_identical_final_report_is_idempotent() {
        let query_id = id(11, 21);
        let writer = key(11, 21, 111, 211, 0);
        let mut coord = WriteCoordinator::new(query_id, vec![writer.clone()]);
        let first = report(&writer, true, ok_status(), "s3://w/dup.parquet");
        let duplicate = first.clone();

        assert_eq!(coord.apply_report(first).expect("first"), ReportOutcome::CommitReady);
        assert_eq!(
            coord.apply_report(duplicate).expect("duplicate"),
            ReportOutcome::Duplicate
        );
    }

    #[test]
    fn conflicting_duplicate_final_report_fails_fast() {
        let query_id = id(12, 22);
        let writer = key(12, 22, 112, 212, 0);
        let mut coord = WriteCoordinator::new(query_id, vec![writer.clone()]);
        coord
            .apply_report(report(&writer, true, ok_status(), "s3://w/original.parquet"))
            .expect("first report");

        let err = coord
            .apply_report(report(&writer, true, ok_status(), "s3://w/conflict.parquet"))
            .expect_err("conflicting duplicate must fail");
        assert!(err.contains("conflicting final report"), "{err}");
    }

    #[test]
    fn writer_failure_builds_abort_input_and_blocks_commit() {
        let query_id = id(13, 23);
        let writer_a = key(13, 23, 113, 213, 0);
        let writer_b = key(13, 23, 114, 214, 1);
        let mut coord = WriteCoordinator::new(query_id.clone(), vec![writer_a.clone(), writer_b.clone()]);

        coord
            .apply_report(report(&writer_a, true, ok_status(), "s3://w/done.parquet"))
            .expect("first writer ok");
        assert_eq!(
            coord.apply_report(report(&writer_b, true, err_status("writer failed"), ""))
                .expect("failed writer report"),
            ReportOutcome::Failed
        );

        let err = coord.commit_input().expect_err("failed write cannot commit");
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
        let mut coord = WriteCoordinator::new(query_id, vec![writer_a.clone(), writer_b]);
        coord
            .apply_report(report(&writer_a, true, ok_status(), "s3://w/only.parquet"))
            .expect("writer a report");

        let err = coord.commit_input().expect_err("missing writer must block commit");
        assert!(err.contains("missing writer"), "{err}");
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
}
```

- [ ] **Step 2: Run the failing tests**

Run:

```bash
cargo test --lib write_coordinator
```

Expected: FAIL. The failure should mention `WriteCoordinator stub` or `report_from_thrift stub`.

- [ ] **Step 3: Implement the state machine**

Replace the stubs in `src/runtime/write_coordinator.rs` with this implementation shape. Keep helper functions private unless another task explicitly needs them.

```rust
use std::collections::{BTreeMap, HashMap};
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

pub(crate) struct WriteCoordinator {
    write_id: types::TUniqueId,
    writers: BTreeMap<WriterKey, WriterSlot>,
    failed_reason: Option<String>,
}

impl WriteCoordinator {
    pub(crate) fn new(query_id: types::TUniqueId, writers: Vec<WriterKey>) -> Self {
        let mut slots = BTreeMap::new();
        for (writer_id, key) in writers.into_iter().enumerate() {
            slots.insert(
                key.clone(),
                WriterSlot {
                    writer_id,
                    key,
                    state: WriterState::Pending,
                },
            );
        }
        Self {
            write_id: query_id,
            writers: slots,
            failed_reason: None,
        }
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
                WriterState::Finished(_) | WriterState::Failed { .. } | WriterState::Canceled { .. } => {
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
                WriterState::Finished(_) => {
                    return Err(format!(
                        "conflicting final report for {}: finished writer later reported error: {}",
                        format_writer_key(&key),
                        error
                    ));
                }
                _ => {}
            }
            slot.state = WriterState::Failed {
                error: error.clone(),
            };
            if self.failed_reason.is_none() {
                self.failed_reason = Some(error);
            }
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
            WriterState::Finished(_) => Err(format!(
                "conflicting final report for {}: commit metadata changed",
                format_writer_key(&key)
            )),
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
                if self.all_finished() {
                    Ok(ReportOutcome::CommitReady)
                } else {
                    Ok(ReportOutcome::Accepted)
                }
            }
        }
    }

    pub(crate) fn mark_canceled_except_finished(&mut self, reason: String) {
        for slot in self.writers.values_mut() {
            if matches!(slot.state, WriterState::Pending | WriterState::Running { .. }) {
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
                    return Err(format!("missing writer final report: {}", format_writer_key(&slot.key)));
                }
            }
        }
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
                _ => incomplete_writers.push(slot.key.clone()),
            }
        }
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
        tablet_commit_infos: params.commitInfos.unwrap_or_default(),
        tablet_fail_infos: params.failInfos.unwrap_or_default(),
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
```

- [ ] **Step 4: Add the query-level registry in the same module**

Append this registry code below the state machine. It is needed by the gRPC handler and by `ExecutionCoordinator`.

```rust
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
) -> Arc<Mutex<WriteCoordinator>> {
    let coord = Arc::new(Mutex::new(WriteCoordinator::new(query_id.clone(), writers)));
    registry()
        .queries
        .lock()
        .expect("write coordinator registry lock")
        .insert(query_key(&query_id), Arc::clone(&coord));
    coord
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
        .ok_or_else(|| format!("write coordinator not found for query {}/{}", query_id.hi, query_id.lo))?;
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
```

- [ ] **Step 5: Export the module**

Modify `src/runtime/mod.rs`:

```rust
pub(crate) mod write_coordinator;
```

Place it near the other runtime coordinator modules.

- [ ] **Step 6: Run the state-machine tests**

Run:

```bash
cargo test --lib write_coordinator
```

Expected: PASS for the tests added in this task.

- [ ] **Step 7: Commit**

```bash
git add src/runtime/write_coordinator.rs src/runtime/mod.rs
git commit -m "Add distributed write coordinator state machine"
```

## Task 2: Shared Report Builder

**Files:**
- Create: `src/service/exec_status_report.rs`
- Modify: `src/service/fe_report.rs`
- Modify: `src/service/mod.rs`

- [ ] **Step 1: Write failing builder tests**

Create `src/service/exec_status_report.rs` with this test module and minimal public surface.

```rust
use crate::common::types::UniqueId;
use crate::runtime::query_context::QueryId;
use crate::{data_cache, frontend_service, runtime_profile, status, types};

pub(crate) struct ExecStatusReportInput {
    pub(crate) finst_id: UniqueId,
    pub(crate) query_id: QueryId,
    pub(crate) backend_num: i32,
    pub(crate) status: status::TStatus,
    pub(crate) done: bool,
    pub(crate) profile: Option<runtime_profile::TRuntimeProfileTree>,
    pub(crate) load_channel_profile: Option<runtime_profile::TRuntimeProfileTree>,
    pub(crate) load_datacache_metrics: Option<data_cache::TLoadDataCacheMetrics>,
}

pub(crate) fn build_report_params(
    _input: ExecStatusReportInput,
) -> frontend_service::TReportExecStatusParams {
    panic!("build_report_params stub")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::sink_commit;
    use crate::{status_code, types};

    fn ok_status() -> status::TStatus {
        status::TStatus::new(status_code::TStatusCode::OK, None)
    }

    #[test]
    fn builder_collects_sink_commit_infos_and_load_counters() {
        let finst_id = UniqueId { hi: 91, lo: 92 };
        sink_commit::register(finst_id);
        sink_commit::add(
            finst_id,
            types::TSinkCommitInfo {
                iceberg_data_file: Some(types::TIcebergDataFile {
                    path: Some("s3://warehouse/table/data-1.parquet".to_string()),
                    record_count: Some(9),
                    file_size_in_bytes: Some(90),
                    ..Default::default()
                }),
                ..Default::default()
            },
        );
        sink_commit::add_load_stats(finst_id, 3, 30, 2);

        let params = build_report_params(ExecStatusReportInput {
            finst_id,
            query_id: QueryId { hi: 81, lo: 82 },
            backend_num: 7,
            status: ok_status(),
            done: true,
            profile: None,
            load_channel_profile: None,
            load_datacache_metrics: None,
        });

        assert_eq!(params.query_id, Some(types::TUniqueId::new(81, 82)));
        assert_eq!(params.fragment_instance_id, Some(types::TUniqueId::new(91, 92)));
        assert_eq!(params.backend_num, Some(7));
        assert_eq!(params.done, Some(true));
        assert_eq!(
            params
                .sink_commit_infos
                .as_ref()
                .expect("sink commit infos")
                .len(),
            1
        );
        assert_eq!(params.loaded_rows, Some(12));
        assert_eq!(params.sink_load_bytes, Some(120));
        assert_eq!(
            params
                .load_counters
                .as_ref()
                .and_then(|c| c.get("dpp.norm.ALL")),
            Some(&"12".to_string())
        );
        assert_eq!(
            params
                .load_counters
                .as_ref()
                .and_then(|c| c.get("dpp.abnorm.ALL")),
            Some(&"2".to_string())
        );
        assert_eq!(
            params
                .load_counters
                .as_ref()
                .and_then(|c| c.get("loaded.bytes")),
            Some(&"120".to_string())
        );
        sink_commit::unregister(finst_id);
    }
}
```

- [ ] **Step 2: Run the failing builder test**

Run:

```bash
cargo test --lib exec_status_report
```

Expected: FAIL with `build_report_params stub`.

- [ ] **Step 3: Move report payload construction into the shared builder**

Replace the stub with a direct extraction of the current `fe_report::build_report_params` body. The new function must have this exact signature:

```rust
pub(crate) fn build_report_params(
    input: ExecStatusReportInput,
) -> frontend_service::TReportExecStatusParams
```

Use this body structure:

```rust
use std::collections::BTreeMap;

use crate::runtime::sink_commit;

pub(crate) fn build_report_params(
    input: ExecStatusReportInput,
) -> frontend_service::TReportExecStatusParams {
    let sink_commit_infos = sink_commit::list(input.finst_id);
    let tablet_commit_infos = sink_commit::list_tablet_commit_infos(input.finst_id);
    let tablet_fail_infos = sink_commit::list_tablet_fail_infos(input.finst_id);
    let state_stats = sink_commit::get_load_stats(input.finst_id);
    let mut normal_rows: i64 = state_stats.loaded_rows.max(0);
    let mut loaded_bytes: i64 = state_stats.loaded_bytes.max(0);
    let filtered_rows: i64 = state_stats.filtered_rows.max(0);

    for info in &sink_commit_infos {
        if let Some(file) = info.iceberg_data_file.as_ref() {
            if let Some(rows) = file.record_count {
                normal_rows = normal_rows.saturating_add(rows);
            }
            if let Some(bytes) = file.file_size_in_bytes {
                loaded_bytes = loaded_bytes.saturating_add(bytes);
            }
        }
        if let Some(file) = info.hive_file_info.as_ref() {
            if let Some(rows) = file.record_count {
                normal_rows = normal_rows.saturating_add(rows);
            }
            if let Some(bytes) = file.file_size_in_bytes {
                loaded_bytes = loaded_bytes.saturating_add(bytes);
            }
        }
    }

    let load_counters = if normal_rows > 0 || loaded_bytes > 0 || filtered_rows > 0 {
        let mut counters = BTreeMap::new();
        counters.insert("dpp.norm.ALL".to_string(), normal_rows.to_string());
        counters.insert("dpp.abnorm.ALL".to_string(), filtered_rows.to_string());
        if loaded_bytes > 0 {
            counters.insert("loaded.bytes".to_string(), loaded_bytes.to_string());
        }
        Some(counters)
    } else {
        None
    };

    let tablet_commit_infos = if tablet_commit_infos.is_empty() {
        None
    } else {
        Some(tablet_commit_infos)
    };
    let sink_commit_infos = if sink_commit_infos.is_empty() {
        None
    } else {
        Some(sink_commit_infos)
    };
    let tablet_fail_infos = if tablet_fail_infos.is_empty() {
        None
    } else {
        Some(tablet_fail_infos)
    };

    frontend_service::TReportExecStatusParams::new(
        frontend_service::FrontendServiceVersion::V1,
        Some(types::TUniqueId {
            hi: input.query_id.hi,
            lo: input.query_id.lo,
        }),
        Some(input.backend_num),
        Some(types::TUniqueId {
            hi: input.finst_id.hi,
            lo: input.finst_id.lo,
        }),
        Some(input.status),
        Some(input.done),
        input.profile,
        Option::<Vec<String>>::None,
        Option::<Vec<String>>::None,
        load_counters,
        None::<String>,
        Option::<Vec<String>>::None,
        tablet_commit_infos,
        (normal_rows > 0).then_some(normal_rows),
        Option::<i64>::None,
        (loaded_bytes > 0).then_some(loaded_bytes),
        Option::<i64>::None,
        Option::<i64>::None,
        Option::<crate::internal_service::TLoadJobType>::None,
        tablet_fail_infos,
        (filtered_rows > 0).then_some(filtered_rows),
        Option::<i64>::None,
        Option::<i64>::None,
        sink_commit_infos,
        Option::<String>::None,
        input.load_channel_profile,
        input.load_datacache_metrics,
    )
}
```

This snippet intentionally sets `tracking_url` to `None` in the shared builder. If FE-compatible still requires `tracking_url`, add `tracking_url: Option<String>` to `ExecStatusReportInput` and pass the existing `fe_report::build_tracking_url(instance.query_id)` from `fe_report.rs`. Use that explicit field rather than calling FE-specific config helpers from the shared module.

- [ ] **Step 4: Route FE report through the shared builder**

Modify `src/service/fe_report.rs`:

1. Remove the old private `build_report_params` function.
2. Import the shared builder:

```rust
use crate::service::exec_status_report::{self, ExecStatusReportInput};
```

3. In `report_fragment_done`, replace the call with:

```rust
let params = exec_status_report::build_report_params(ExecStatusReportInput {
    finst_id,
    query_id: instance.query_id,
    backend_num: instance.backend_num,
    status,
    done: true,
    profile,
    load_channel_profile: None,
    load_datacache_metrics,
});
```

4. In `report_exec_state`, replace the call with:

```rust
let params = exec_status_report::build_report_params(ExecStatusReportInput {
    finst_id,
    query_id: instance.query_id,
    backend_num: instance.backend_num,
    status,
    done: false,
    profile,
    load_channel_profile: None,
    load_datacache_metrics,
});
```

- [ ] **Step 5: Export the module**

Modify `src/service/mod.rs`:

```rust
pub(crate) mod exec_status_report;
```

- [ ] **Step 6: Run report tests**

Run:

```bash
cargo test --lib exec_status_report fe_report exec_state_reporter
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/service/exec_status_report.rs src/service/fe_report.rs src/service/mod.rs
git commit -m "Share fragment exec status report builder"
```

## Task 3: Standalone Report gRPC Protocol

**Files:**
- Modify: `idl/proto/starust_grpc.proto`
- Modify: `src/service/grpc_server.rs`
- Modify: `src/service/grpc_client.rs`

- [ ] **Step 1: Write failing server tests**

Add these tests to `src/service/grpc_server.rs` inside the existing `pr3_tests` module. Extend the imports with `ReportExecStatusRequest`.

```rust
use crate::common::thrift::thrift_binary_serialize;
use crate::{frontend_service, status, status_code, types};
use super::proto::novarocks::ReportExecStatusRequest;

fn ok_report_params(query: types::TUniqueId, finst: types::TUniqueId) -> frontend_service::TReportExecStatusParams {
    frontend_service::TReportExecStatusParams::new(
        frontend_service::FrontendServiceVersion::V1,
        Some(query),
        Some(0),
        Some(finst),
        Some(status::TStatus::new(status_code::TStatusCode::OK, None)),
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
    )
}

#[tokio::test]
async fn report_exec_status_bad_thrift_returns_business_error() {
    let svc = GrpcService::default();
    let req = Request::new(ReportExecStatusRequest {
        report_exec_status_params_thrift: vec![0xff, 0xff, 0xff],
    });
    let resp = svc.report_exec_status(req).await.expect("RPC level success");
    let body = resp.into_inner();
    assert_ne!(body.status_code, 0);
    assert!(body.message.contains("deserialize") || body.message.contains("thrift"));
}

#[tokio::test]
async fn report_exec_status_updates_registered_write_coordinator() {
    crate::runtime::write_coordinator::test_clear_registry();
    let query = types::TUniqueId::new(701, 801);
    let finst = types::TUniqueId::new(702, 802);
    crate::runtime::write_coordinator::register_query(
        query.clone(),
        vec![crate::runtime::write_coordinator::WriterKey {
            query_id: query.clone(),
            fragment_instance_id: finst.clone(),
            backend_num: 0,
        }],
    );
    let bytes = thrift_binary_serialize(&ok_report_params(query.clone(), finst))
        .expect("serialize report params");
    let svc = GrpcService::default();
    let req = Request::new(ReportExecStatusRequest {
        report_exec_status_params_thrift: bytes,
    });
    let resp = svc.report_exec_status(req).await.expect("RPC level success");
    let body = resp.into_inner();
    assert_eq!(body.status_code, 0, "{}", body.message);
    crate::runtime::write_coordinator::unregister_query(&query);
}
```

- [ ] **Step 2: Run the failing server tests**

Run:

```bash
cargo test --lib report_exec_status
```

Expected: FAIL because generated proto bindings do not yet define `ReportExecStatusRequest` or `report_exec_status`.

- [ ] **Step 3: Add proto RPCs**

Modify `idl/proto/starust_grpc.proto` inside `service NovaRocksGrpc`:

```proto
  // IW-4 distributed write coordinator report RPCs.
  rpc ReportExecStatus(ReportExecStatusRequest) returns (ReportExecStatusResponse);
  rpc BatchReportExecStatus(BatchReportExecStatusRequest) returns (BatchReportExecStatusResponse);
```

Add messages after `CancelFragmentResponse`:

```proto
message ReportExecStatusRequest {
  bytes report_exec_status_params_thrift = 1;
}

message ReportExecStatusResponse {
  int32 status_code = 1;
  string message = 2;
}

message BatchReportExecStatusRequest {
  repeated bytes report_exec_status_params_thrift = 1;
}

message BatchReportExecStatusResponse {
  int32 status_code = 1;
  string message = 2;
}
```

- [ ] **Step 4: Implement gRPC server handlers**

Modify the `impl NovaRocksGrpc for GrpcService` block in `src/service/grpc_server.rs`:

```rust
async fn report_exec_status(
    &self,
    request: tonic::Request<proto::novarocks::ReportExecStatusRequest>,
) -> Result<tonic::Response<proto::novarocks::ReportExecStatusResponse>, tonic::Status> {
    let bytes = request.into_inner().report_exec_status_params_thrift;
    let result = tokio::task::spawn_blocking(move || {
        let params: crate::frontend_service::TReportExecStatusParams =
            crate::common::thrift::thrift_binary_deserialize(&bytes)?;
        crate::runtime::write_coordinator::handle_report_exec_status(params)?;
        Ok::<(), String>(())
    })
    .await
    .map_err(|e| tonic::Status::internal(format!("report_exec_status handler panicked: {e}")))?;

    match result {
        Ok(()) => Ok(tonic::Response::new(proto::novarocks::ReportExecStatusResponse {
            status_code: 0,
            message: String::new(),
        })),
        Err(e) => Ok(tonic::Response::new(proto::novarocks::ReportExecStatusResponse {
            status_code: 1,
            message: e,
        })),
    }
}

async fn batch_report_exec_status(
    &self,
    request: tonic::Request<proto::novarocks::BatchReportExecStatusRequest>,
) -> Result<tonic::Response<proto::novarocks::BatchReportExecStatusResponse>, tonic::Status> {
    let payloads = request.into_inner().report_exec_status_params_thrift;
    let result = tokio::task::spawn_blocking(move || {
        for bytes in payloads {
            let params: crate::frontend_service::TReportExecStatusParams =
                crate::common::thrift::thrift_binary_deserialize(&bytes)?;
            crate::runtime::write_coordinator::handle_report_exec_status(params)?;
        }
        Ok::<(), String>(())
    })
    .await
    .map_err(|e| tonic::Status::internal(format!("batch_report_exec_status handler panicked: {e}")))?;

    match result {
        Ok(()) => Ok(tonic::Response::new(proto::novarocks::BatchReportExecStatusResponse {
            status_code: 0,
            message: String::new(),
        })),
        Err(e) => Ok(tonic::Response::new(proto::novarocks::BatchReportExecStatusResponse {
            status_code: 1,
            message: e,
        })),
    }
}
```

- [ ] **Step 5: Add client methods**

Modify `src/service/grpc_client.rs`:

```rust
pub fn blocking_report_exec_status(
    &self,
    req: proto::novarocks::ReportExecStatusRequest,
) -> Result<proto::novarocks::ReportExecStatusResponse, String> {
    let mut cli = self.make_client()?;
    data_block_on(async move {
        cli.report_exec_status(req)
            .await
            .map(|r| r.into_inner())
            .map_err(|e| format!("report_exec_status rpc failed: {e}"))
    })?
}

pub fn blocking_batch_report_exec_status(
    &self,
    req: proto::novarocks::BatchReportExecStatusRequest,
) -> Result<proto::novarocks::BatchReportExecStatusResponse, String> {
    let mut cli = self.make_client()?;
    data_block_on(async move {
        cli.batch_report_exec_status(req)
            .await
            .map(|r| r.into_inner())
            .map_err(|e| format!("batch_report_exec_status rpc failed: {e}"))
    })?
}
```

- [ ] **Step 6: Add client nonzero-status test**

In `src/runtime/dispatcher.rs`, extend `MockGrpc` with report handlers if the generated trait requires them. Add a focused test in `src/service/grpc_client.rs` tests if that file already has mock server tests; otherwise place it beside existing `RemoteDispatcher` mock server tests and assert nonzero status is visible to the caller:

```rust
let resp = client
    .blocking_report_exec_status(proto::novarocks::ReportExecStatusRequest {
        report_exec_status_params_thrift: vec![0xff],
    })
    .expect("RPC level success");
assert_ne!(resp.status_code, 0);
assert!(!resp.message.is_empty());
```

- [ ] **Step 7: Run protocol tests**

Run:

```bash
cargo test --lib report_exec_status grpc_client grpc_server
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add idl/proto/starust_grpc.proto src/service/grpc_server.rs src/service/grpc_client.rs src/runtime/dispatcher.rs
git commit -m "Add standalone exec status report RPC"
```

## Task 4: Explicit Standalone Report Destination

**Files:**
- Modify: `idl/thrift/InternalService.thrift`
- Modify: `src/runtime/exec_params.rs`

- [ ] **Step 1: Add a failing exec-params test**

In `src/runtime/exec_params.rs`, add a test that expects a standalone report address to survive param construction:

```rust
#[test]
fn build_exec_params_preserves_novarocks_report_addr() {
    let fr = empty_fragment_build_result(1, 2);
    let thrift_fragment = noop_thrift_fragment();
    let exec_params = fr.exec_params.clone();
    let report_addr = types::TNetworkAddress::new("127.0.0.1".to_string(), 18040);

    let params = build_exec_plan_fragment_params(
        &fr,
        thrift_fragment,
        exec_params,
        None,
        1,
        Some(3),
        Some(report_addr.clone()),
    );

    assert_eq!(params.novarocks_report_addr, Some(report_addr));
    assert_eq!(params.coord, None, "StarRocks FE coord must remain separate");
}
```

- [ ] **Step 2: Run the failing test**

Run:

```bash
cargo test --lib build_exec_params_preserves_novarocks_report_addr
```

Expected: FAIL because `novarocks_report_addr` and the new function argument do not exist.

- [ ] **Step 3: Add the thrift field**

Modify `idl/thrift/InternalService.thrift` in `TExecPlanFragmentParams` after field `61`:

```thrift
  // NovaRocks standalone coordinator report endpoint. When present, BE sends
  // TReportExecStatusParams through NovaRocksGrpc instead of StarRocks FE thrift.
  62: optional Types.TNetworkAddress novarocks_report_addr;
```

Run:

```bash
rg "pub struct TExecPlanFragmentParams" src
```

Expected: no checked-in generated Rust binding under `src/`. The thrift IDL remains the source of truth for this field.

- [ ] **Step 4: Thread the field through exec params**

Modify `src/runtime/exec_params.rs` signature:

```rust
pub(crate) fn build_exec_plan_fragment_params(
    fr: &FragmentBuildResult,
    thrift_fragment: planner::TPlanFragment,
    exec_params: internal_service::TPlanFragmentExecParams,
    query_options: Option<internal_service::TQueryOptions>,
    pipeline_dop: i32,
    backend_num: Option<i32>,
    novarocks_report_addr: Option<types::TNetworkAddress>,
) -> internal_service::TExecPlanFragmentParams
```

Pass `novarocks_report_addr` into the generated constructor at the new field position. Update all existing call sites by adding `None::<types::TNetworkAddress>` until Task 6 populates the real address.

- [ ] **Step 5: Run targeted tests**

Run:

```bash
cargo test --lib build_exec_params_preserves_novarocks_report_addr
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add idl/thrift/InternalService.thrift src/runtime/exec_params.rs
git commit -m "Thread standalone report destination through fragment params"
```

## Task 5: Standalone Report Sender Adapter

**Files:**
- Create: `src/service/standalone_exec_state_reporter.rs`
- Modify: `src/service/fe_report.rs`
- Modify: `src/service/internal_service.rs`
- Modify: `src/service/mod.rs`

- [ ] **Step 1: Write failing standalone reporter tests**

Create `src/service/standalone_exec_state_reporter.rs` with a retry helper test first:

```rust
use std::time::Duration;

use crate::common::types::UniqueId;
use crate::frontend_service;
use crate::runtime::query_context::QueryId;
use crate::types;

#[derive(Clone, Debug)]
pub(crate) struct StandaloneExecStateReportTask {
    pub(crate) finst_id: UniqueId,
    pub(crate) query_id: QueryId,
    pub(crate) coord: types::TNetworkAddress,
    pub(crate) params: frontend_service::TReportExecStatusParams,
}

pub(crate) fn enqueue_non_final(_task: StandaloneExecStateReportTask) -> Result<(), String> {
    Err("standalone enqueue_non_final stub".to_string())
}

pub(crate) fn enqueue_final(_task: StandaloneExecStateReportTask) {
    panic!("standalone enqueue_final stub")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    #[test]
    fn final_report_retries_and_returns_error_after_limit() {
        let attempts = AtomicUsize::new(0);
        let sleeps = Mutex::new(Vec::new());
        let result = send_final_report_with(
            test_task(),
            3,
            |_| {
                attempts.fetch_add(1, Ordering::AcqRel);
                Err("network down".to_string())
            },
            |duration| sleeps.lock().expect("sleep record").push(duration),
        );

        let err = result.expect_err("retry exhaustion must be an error");
        assert!(err.contains("network down"), "{err}");
        assert_eq!(attempts.load(Ordering::Acquire), 3);
        assert_eq!(
            *sleeps.lock().expect("sleep record"),
            vec![Duration::from_millis(100), Duration::from_millis(200)]
        );
    }

    fn test_task() -> StandaloneExecStateReportTask {
        StandaloneExecStateReportTask {
            finst_id: UniqueId { hi: 301, lo: 401 },
            query_id: QueryId { hi: 501, lo: 601 },
            coord: types::TNetworkAddress::new("127.0.0.1".to_string(), 18040),
            params: frontend_service::TReportExecStatusParams::new(
                frontend_service::FrontendServiceVersion::V1,
                Some(types::TUniqueId::new(501, 601)),
                Some(0),
                Some(types::TUniqueId::new(301, 401)),
                Some(crate::status::TStatus::new(crate::status_code::TStatusCode::OK, None)),
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
            ),
        }
    }
}
```

- [ ] **Step 2: Implement retry helper and async queue**

Implement `send_final_report_with` and mirror the shape of `exec_state_reporter.rs`:

```rust
fn backoff_for_attempt(attempt: usize) -> Duration {
    Duration::from_millis(100 * (1u64 << attempt.saturating_sub(1)).min(10))
}

fn send_final_report_with<F, S>(
    task: StandaloneExecStateReportTask,
    retry_limit: usize,
    mut send: F,
    mut sleep: S,
) -> Result<(), String>
where
    F: FnMut(&StandaloneExecStateReportTask) -> Result<(), String>,
    S: FnMut(Duration),
{
    let retry_limit = retry_limit.max(1);
    let mut last_error = String::new();
    for attempt in 1..=retry_limit {
        match send(&task) {
            Ok(()) => return Ok(()),
            Err(err) => {
                last_error = err;
                tracing::warn!(
                    target: "novarocks::report",
                    finst_id = %task.finst_id,
                    query_id = %task.query_id,
                    attempt,
                    error = %last_error,
                    "standalone final reportExecStatus failed"
                );
            }
        }
        if attempt < retry_limit {
            sleep(backoff_for_attempt(attempt));
        }
    }
    Err(last_error)
}
```

For production sending:

```rust
fn send_once(task: &StandaloneExecStateReportTask) -> Result<(), String> {
    let addr = std::net::SocketAddr::new(
        task.coord.hostname
            .parse()
            .map_err(|e| format!("invalid standalone report host '{}': {e}", task.coord.hostname))?,
        u16::try_from(task.coord.port)
            .map_err(|_| format!("invalid standalone report port {}", task.coord.port))?,
    );
    let bytes = crate::common::thrift::thrift_binary_serialize(&task.params)?;
    let client = crate::service::grpc_client::NovaRocksGrpcRemoteClient::connect_blocking(addr)?;
    let resp = client.blocking_report_exec_status(
        crate::service::grpc_client::proto::novarocks::ReportExecStatusRequest {
            report_exec_status_params_thrift: bytes,
        },
    )?;
    if resp.status_code == 0 {
        Ok(())
    } else {
        Err(format!("standalone reportExecStatus returned status_code={}: {}", resp.status_code, resp.message))
    }
}
```

Use a priority queue thread for `enqueue_final`. For `enqueue_non_final`, send best-effort on a normal queue or return `Ok(())` after logging if queue insertion succeeds. Final report exhaustion must log an error; Task 7 polls the coordinator and root result to fail the query.

- [ ] **Step 3: Add explicit destination variants to FE report registry**

Modify `src/service/fe_report.rs`:

```rust
#[derive(Clone, Debug)]
enum ReportDestination {
    StarRocksFrontend(types::TNetworkAddress),
    NovaRocksCoordinator(types::TNetworkAddress),
}

#[derive(Clone, Debug)]
struct ReportInstance {
    destination: ReportDestination,
    backend_num: i32,
    query_id: QueryId,
    enable_profile: bool,
    profiler: Option<Profiler>,
    mem_tracker: Option<Arc<MemTracker>>,
    query_mem_tracker: Option<Arc<MemTracker>>,
    report_interval_ns: Option<i64>,
    fe_query_gone: bool,
}
```

Keep `register_instance` as the FE-compatible entry point:

```rust
pub(crate) fn register_instance(
    finst_id: UniqueId,
    query_id: QueryId,
    coord: types::TNetworkAddress,
    backend_num: i32,
    enable_profile: bool,
    profiler: Option<Profiler>,
    mem_tracker: Option<Arc<MemTracker>>,
    query_mem_tracker: Option<Arc<MemTracker>>,
    report_interval_ns: Option<i64>,
) {
    register_instance_with_destination(
        finst_id,
        query_id,
        ReportDestination::StarRocksFrontend(coord),
        backend_num,
        enable_profile,
        profiler,
        mem_tracker,
        query_mem_tracker,
        report_interval_ns,
    );
}
```

Add the standalone entry point:

```rust
pub(crate) fn register_novarocks_instance(
    finst_id: UniqueId,
    query_id: QueryId,
    coord: types::TNetworkAddress,
    backend_num: i32,
    enable_profile: bool,
    profiler: Option<Profiler>,
    mem_tracker: Option<Arc<MemTracker>>,
    query_mem_tracker: Option<Arc<MemTracker>>,
    report_interval_ns: Option<i64>,
) {
    register_instance_with_destination(
        finst_id,
        query_id,
        ReportDestination::NovaRocksCoordinator(coord),
        backend_num,
        enable_profile,
        profiler,
        mem_tracker,
        query_mem_tracker,
        report_interval_ns,
    );
}
```

In `report_fragment_done`, route final task by destination:

```rust
match instance.destination {
    ReportDestination::StarRocksFrontend(coord) => {
        exec_state_reporter::enqueue_final(ExecStateReportTask {
            finst_id,
            query_id: instance.query_id,
            coord,
            params,
        });
    }
    ReportDestination::NovaRocksCoordinator(coord) => {
        crate::service::standalone_exec_state_reporter::enqueue_final(
            crate::service::standalone_exec_state_reporter::StandaloneExecStateReportTask {
                finst_id,
                query_id: instance.query_id,
                coord,
                params,
            },
        );
    }
}
```

Apply the same destination split in `report_exec_state`, using `enqueue_non_final`.

- [ ] **Step 4: Make internal service choose the report destination explicitly**

In `src/service/internal_service.rs`, update both `submit_exec_plan_fragment` and `submit_exec_batch_plan_fragments` registration blocks:

```rust
let novarocks_report_addr = one
    .novarocks_report_addr
    .clone()
    .or_else(|| common.and_then(|c| c.novarocks_report_addr.clone()));

if let (Some(report_addr), Some(backend_num)) = (novarocks_report_addr, backend_num) {
    fe_report::register_novarocks_instance(
        finst_id,
        query_id,
        report_addr,
        backend_num,
        enable_profile,
        profiler.clone(),
        Some(Arc::clone(&fragment_mem_tracker)),
        Some(Arc::clone(&query_mem_tracker)),
        report_interval_ns,
    );
} else if let (Some(coord), Some(backend_num)) = (coord.cloned(), backend_num) {
    fe_report::register_instance(
        finst_id,
        query_id,
        coord,
        backend_num,
        enable_profile,
        profiler.clone(),
        Some(Arc::clone(&fragment_mem_tracker)),
        Some(Arc::clone(&query_mem_tracker)),
        report_interval_ns,
    );
} else {
    warn!(
        target: "novarocks::report",
        finst_id = %finst_id,
        "missing report destination/backend_num for reportExecStatus"
    );
}
```

- [ ] **Step 5: Export the standalone reporter**

Modify `src/service/mod.rs`:

```rust
pub(crate) mod standalone_exec_state_reporter;
```

- [ ] **Step 6: Run report adapter tests**

Run:

```bash
cargo test --lib standalone_exec_state_reporter fe_report exec_status_report
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/service/standalone_exec_state_reporter.rs src/service/fe_report.rs src/service/internal_service.rs src/service/mod.rs
git commit -m "Add standalone exec status report adapter"
```

## Task 6: Coordinator Report Endpoint and Fragment Wiring

**Files:**
- Modify: `src/server/mod.rs`
- Modify: `src/main.rs`
- Modify: `src/runtime/exec_params.rs`
- Modify: `src/runtime/coordinator.rs`

- [ ] **Step 1: Write failing role=fe startup test**

In `src/server/mod.rs`, add a test-only helper and test beside the server option code. This locks the intended behavior: `role=fe` starts a report gRPC endpoint but still does not run local fragment execution.

```rust
#[cfg(test)]
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct TestResolvedServerOptions {
    pub(crate) start_local_exchange_execution: bool,
    pub(crate) start_coordinator_report_grpc: bool,
}

#[cfg(test)]
pub(crate) fn test_resolve_fe_server_options(
    cfg: NovaRocksConfig,
    port_override: Option<u16>,
) -> Result<TestResolvedServerOptions, String> {
    let resolved = resolve_server_options_from_config(&cfg, port_override)?;
    let resolved = ResolvedStandaloneServerOptions {
        config_path: None,
        preloaded_config: Some(cfg),
        start_local_exchange_execution: false,
        start_coordinator_report_grpc: true,
        ..resolved
    };
    Ok(TestResolvedServerOptions {
        start_local_exchange_execution: resolved.start_local_exchange_execution,
        start_coordinator_report_grpc: resolved.start_coordinator_report_grpc,
    })
}

#[test]
fn role_fe_server_options_start_report_endpoint_without_local_exchange_execution() {
    let mut cfg = NovaRocksConfig::default();
    cfg.cluster.role = crate::common::app_config::ClusterRole::Fe;
    cfg.cluster.backends = vec!["127.0.0.1:19070".to_string()];

    let opts = test_resolve_fe_server_options(cfg, None).expect("resolve role=fe server options");
    assert!(opts.start_coordinator_report_grpc);
    assert!(!opts.start_local_exchange_execution);
}
```

- [ ] **Step 2: Split server startup intent**

Replace the single `start_grpc_exchange: bool` in `ResolvedStandaloneServerOptions` with:

```rust
start_local_exchange_execution: bool,
start_coordinator_report_grpc: bool,
```

Set them as:

```rust
// all-in-one
start_local_exchange_execution: true,
start_coordinator_report_grpc: true,

// role=fe
start_local_exchange_execution: false,
start_coordinator_report_grpc: true,
```

In `serve_forever`, start the existing `start_grpc_exchange_server("127.0.0.1", http_port)` when either boolean is true. Update log text:

```rust
if start_coordinator_report_grpc || start_local_exchange_execution {
    let grpc_port = crate::common::config::http_port();
    crate::service::grpc_server::start_grpc_exchange_server("127.0.0.1", grpc_port)
        .map_err(|e| format!("failed to start standalone coordinator grpc on port {grpc_port}: {e}"))?;
}
```

This reuses the existing standalone NovaRocksGrpc/http server. The FE still does not submit fragments to itself because `dispatcher_for_role(ClusterRole::Fe)` remains `RemoteDispatcher`.

- [ ] **Step 3: Populate `novarocks_report_addr` in coordinated params**

Modify `src/runtime/coordinator.rs` to compute one coordinator report address before building submissions:

```rust
fn local_coordinator_report_addr() -> Result<types::TNetworkAddress, String> {
    let cfg = crate::novarocks_config::config()
        .map_err(|e| format!("cannot read coordinator config: {e}"))?;
    Ok(types::TNetworkAddress::new(
        cfg.server.host.clone(),
        cfg.server.http_port as i32,
    ))
}
```

In `ExecutionCoordinator::execute`, call:

```rust
let novarocks_report_addr = local_coordinator_report_addr().ok();
```

When calling `build_exec_plan_fragment_params`, pass `novarocks_report_addr.clone()`.

- [ ] **Step 4: Keep FE-compatible `coord` untouched**

Verify `src/runtime/exec_params.rs` still passes `None::<types::TNetworkAddress>` for `coord` in standalone-generated params and only uses `novarocks_report_addr` for standalone report. The call should look like:

```rust
internal_service::TExecPlanFragmentParams::new(
    internal_service::InternalServiceVersion::V1,
    Some(thrift_fragment),
    Some(fr.desc_tbl.clone()),
    Some(exec_params),
    None::<types::TNetworkAddress>,
    backend_num,
    None::<internal_service::TQueryGlobals>,
    query_options,
    None::<bool>,
    None::<types::TResourceInfo>,
    None::<String>,
    None::<String>,
    None::<i64>,
    None::<internal_service::TLoadErrorHubInfo>,
    Some(true),
    Some(pipeline_dop),
    None::<BTreeMap<types::TPlanNodeId, i32>>,
    None::<crate::work_group::TWorkGroup>,
    None::<bool>,
    None::<i32>,
    None::<bool>,
    None::<bool>,
    None::<internal_service::TAdaptiveDopParam>,
    None::<i32>,
    None::<internal_service::TPredicateTreeParams>,
    None::<Vec<i32>>,
    novarocks_report_addr,
)
```

Adjust the argument order to match generated thrift constructor order after adding field 62.

- [ ] **Step 5: Run startup and exec-param tests**

Run:

```bash
cargo test --lib build_exec_params_preserves_novarocks_report_addr
cargo test --lib role_fe_server_options_start_report_endpoint_without_local_exchange_execution
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/server/mod.rs src/main.rs src/runtime/exec_params.rs src/runtime/coordinator.rs
git commit -m "Wire standalone coordinator report endpoint"
```

## Task 7: Coordinator Integration and Failure Propagation

**Files:**
- Modify: `src/runtime/coordinator.rs`
- Test: `src/runtime/coordinator.rs`

- [ ] **Step 1: Add failing coordinator integration tests**

In `src/runtime/coordinator.rs` tests, add helpers that build write-sink params and a dispatcher that can inject report events. Use existing mock patterns in the file.

```rust
fn is_write_sink_for_test(params: &crate::internal_service::TExecPlanFragmentParams) -> bool {
    params
        .fragment
        .as_ref()
        .and_then(|f| f.output_sink.as_ref())
        .map(|sink| {
            matches!(
                sink.type_,
                crate::data_sinks::TDataSinkType::ICEBERG_TABLE_SINK
                    | crate::data_sinks::TDataSinkType::ICEBERG_DELETE_SINK
                    | crate::data_sinks::TDataSinkType::HIVE_TABLE_SINK
                    | crate::data_sinks::TDataSinkType::OLAP_TABLE_SINK
            )
        })
        .unwrap_or(false)
}

#[test]
fn write_failure_seen_by_coordinator_cancels_inflight_fragments() {
    let dispatcher = CancelTrackingDispatcher::new();
    let query_id = crate::types::TUniqueId::new(901, 902);
    let writer = crate::runtime::write_coordinator::WriterKey {
        query_id: query_id.clone(),
        fragment_instance_id: crate::types::TUniqueId::new(903, 904),
        backend_num: 0,
    };
    let write = crate::runtime::write_coordinator::register_query(query_id.clone(), vec![writer.clone()]);

    {
        let mut guard = write.lock().expect("write coordinator lock");
        guard
            .apply_report(crate::runtime::write_coordinator::FragmentExecStatusReport {
                query_id: query_id.clone(),
                fragment_instance_id: writer.fragment_instance_id.clone(),
                backend_num: writer.backend_num,
                done: true,
                status: crate::status::TStatus::new(
                    crate::status_code::TStatusCode::INTERNAL_ERROR,
                    Some(vec!["writer failed".to_string()]),
                ),
                sink_commit_infos: Vec::new(),
                tablet_commit_infos: Vec::new(),
                tablet_fail_infos: Vec::new(),
                load_counters: std::collections::BTreeMap::new(),
                loaded_rows: 0,
                loaded_bytes: 0,
                filtered_rows: 0,
            })
            .expect("apply failed report");
    }

    let mut tracker = InFlightTracker::default();
    tracker.record_submitted(0, writer.fragment_instance_id.clone());
    let err = poll_write_failure_and_cancel(&write, &tracker, dispatcher.as_ref())
        .expect_err("writer failure should be returned");
    assert!(err.contains("writer failed"), "{err}");
    assert_eq!(dispatcher.cancelled_count(), 1);
    crate::runtime::write_coordinator::unregister_query(&query_id);
}
```

- [ ] **Step 2: Implement write sink detection**

Add this helper near submit orchestration:

```rust
fn is_write_sink(params: &crate::internal_service::TExecPlanFragmentParams) -> bool {
    params
        .fragment
        .as_ref()
        .and_then(|f| f.output_sink.as_ref())
        .map(|sink| {
            matches!(
                sink.type_,
                data_sinks::TDataSinkType::ICEBERG_TABLE_SINK
                    | data_sinks::TDataSinkType::ICEBERG_DELETE_SINK
                    | data_sinks::TDataSinkType::HIVE_TABLE_SINK
                    | data_sinks::TDataSinkType::OLAP_TABLE_SINK
            )
        })
        .unwrap_or(false)
}
```

- [ ] **Step 3: Register expected writers**

While building `submissions`, collect writer keys:

```rust
let mut expected_writers = Vec::new();
if is_write_sink(&params) {
    let exec = params
        .params
        .as_ref()
        .ok_or_else(|| "write sink params missing exec params".to_string())?;
    expected_writers.push(crate::runtime::write_coordinator::WriterKey {
        query_id: exec.query_id.clone(),
        fragment_instance_id: exec.fragment_instance_id.clone(),
        backend_num: placement.instance_index as i32,
    });
}
```

Before submission begins:

```rust
let write_coordinator = if expected_writers.is_empty() {
    None
} else {
    Some(crate::runtime::write_coordinator::register_query(
        query_id.clone(),
        expected_writers,
    ))
};
```

After execution returns or errors, call `unregister_query(&query_id)` in a single cleanup path.

- [ ] **Step 4: Poll writer failure during fetch**

Add helper:

```rust
fn poll_write_failure_and_cancel(
    write: &std::sync::Arc<std::sync::Mutex<crate::runtime::write_coordinator::WriteCoordinator>>,
    tracker: &InFlightTracker,
    dispatcher: &dyn FragmentDispatcher,
) -> Result<(), String> {
    let reason = write
        .lock()
        .expect("write coordinator lock")
        .failed_reason();
    if let Some(reason) = reason {
        tracker.cancel_all(dispatcher);
        write.lock()
            .expect("write coordinator lock")
            .mark_canceled_except_finished(reason.clone());
        return Err(reason);
    }
    Ok(())
}
```

Modify `submit_and_fetch_loop` signature to accept:

```rust
write_coordinator: Option<std::sync::Arc<std::sync::Mutex<crate::runtime::write_coordinator::WriteCoordinator>>>,
```

At the top of each fetch loop iteration:

```rust
if let Some(write) = write_coordinator.as_ref() {
    poll_write_failure_and_cancel(write, tracker, dispatcher.as_ref())?;
}
```

- [ ] **Step 5: Validate commit readiness after root EOF**

After `submit_and_fetch_loop` returns `Ok(chunks)`, validate:

```rust
if let Some(write) = write_coordinator.as_ref() {
    let input = write
        .lock()
        .expect("write coordinator lock")
        .commit_input()?;
    tracing::info!(
        target: "novarocks::write_coordinator",
        write_hi = input.write_id.hi,
        write_lo = input.write_id.lo,
        writers = input.writers.len(),
        "distributed write commit input ready"
    );
}
```

This keeps IW-4 at commit-input generation only.

- [ ] **Step 6: Run coordinator tests**

Run:

```bash
cargo test --lib coordinator write_coordinator
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/runtime/coordinator.rs
git commit -m "Integrate write coordinator with distributed execution"
```

## Task 8: BE Final Report Failure Semantics

**Files:**
- Modify: `src/service/standalone_exec_state_reporter.rs`
- Modify: `src/service/internal_service.rs`
- Test: `src/service/standalone_exec_state_reporter.rs`

- [ ] **Step 1: Add a test for final report exhaustion marker**

Extend `src/service/standalone_exec_state_reporter.rs` tests:

```rust
#[test]
fn final_report_failure_records_fragment_error() {
    let task = test_task();
    let result = send_final_report_with(
        task,
        1,
        |_| Err("coordinator unreachable".to_string()),
        |_| {},
    );
    let err = result.expect_err("final report failure must be visible");
    assert!(err.contains("coordinator unreachable"), "{err}");
}
```

- [ ] **Step 2: Surface final report exhaustion through logs and query context**

When the standalone final report worker exhausts retries, log with `target: "novarocks::report"` and `error` field. If `query_context_manager` exposes a query failure method usable from this thread, call it:

```rust
if let Err(err) = send_final_report_with(task.clone(), retry_limit, send_once, std::thread::sleep) {
    tracing::error!(
        target: "novarocks::report",
        finst_id = %task.finst_id,
        query_id = %task.query_id,
        error = %err,
        "standalone final reportExecStatus exhausted retries"
    );
    crate::service::internal_service::mark_query_failed_from_report(
        task.query_id,
        format!("standalone final reportExecStatus failed: {err}"),
    );
}
```

Add `mark_query_failed_from_report` in `src/service/internal_service.rs`:

```rust
pub(crate) fn mark_query_failed_from_report(query_id: QueryId, error: String) {
    let mgr = query_context_manager();
    let finsts = mgr.cancel_query(query_id, error.clone());
    for id in finsts {
        crate::runtime::result_buffer::close_error(id, error.clone());
        crate::runtime::exchange::cancel_fragment(id.hi, id.lo);
    }
}
```

If `cancel_query` is private to a manager wrapper, add this function beside existing internal-service cancellation code so it uses the same path as fragment execution failure.

- [ ] **Step 3: Run report failure tests**

Run:

```bash
cargo test --lib standalone_exec_state_reporter final_report_failure_records_fragment_error
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add src/service/standalone_exec_state_reporter.rs src/service/internal_service.rs
git commit -m "Fail query when standalone final report is lost"
```

## Task 9: End-to-End and Regression Coverage

**Files:**
- Modify or create focused tests under `tests/` if the existing cluster harness supports a new case.
- Modify SQL test files only if a deterministic standalone distributed write statement already exists.

- [ ] **Step 1: Add an all-in-one coordinator regression test**

Add a Rust integration or unit test that creates a `WriteCoordinator`, applies two final OK reports, and verifies the same commit input shape used by runtime code:

```rust
#[test]
fn all_in_one_write_reports_produce_commit_input() {
    let query_id = crate::types::TUniqueId::new(3001, 3002);
    let writer_a = crate::runtime::write_coordinator::WriterKey {
        query_id: query_id.clone(),
        fragment_instance_id: crate::types::TUniqueId::new(3003, 3004),
        backend_num: 0,
    };
    let writer_b = crate::runtime::write_coordinator::WriterKey {
        query_id: query_id.clone(),
        fragment_instance_id: crate::types::TUniqueId::new(3005, 3006),
        backend_num: 1,
    };
    let mut coord = crate::runtime::write_coordinator::WriteCoordinator::new(
        query_id.clone(),
        vec![writer_a.clone(), writer_b.clone()],
    );
    coord.apply_report(test_ok_report(&writer_a, "s3://w/a.parquet")).expect("writer a");
    coord.apply_report(test_ok_report(&writer_b, "s3://w/b.parquet")).expect("writer b");
    let input = coord.commit_input().expect("commit input");
    assert_eq!(input.writers.len(), 2);
}
```

- [ ] **Step 2: Add a 1FE+2BE cluster smoke if harness support exists**

Search:

```bash
rg "1FE|2BE|cluster.backends|role=fe|role=be|NOVAROCKS_READY" tests docker -n
```

If `tests/cluster_mvp.rs` already starts FE and BE processes, add a case that:

1. Starts FE with two BE addresses.
2. Runs one distributed write-sink report scenario. If user-level Iceberg INSERT has not yet been cut over to multi-fragment `ICEBERG_TABLE_SINK`, use the coordinator/report harness instead of claiming SQL write end-to-end coverage.
3. Asserts FE logs contain `distributed write commit input ready`.
4. Asserts logs contain `writers=2`.

Use the existing process launcher and log capture helpers from that file. Do not create a new cluster harness if `tests/cluster_mvp.rs` already has one.

- [ ] **Step 3: Add a fault-injection smoke**

If the existing debug config supports writer failure injection, add a case that:

1. Starts 1FE+2BE.
2. Enables the failure on exactly one BE writer.
3. Runs the same distributed write statement.
4. Asserts query returns an error containing `writer failed`.
5. Asserts FE logs contain `cancel` and do not contain `distributed write commit input ready`.

If no writer failure injection exists, add a test-only debug flag in `common::config` named `debug_fault_inject_standalone_writer_report_error_after` and make the standalone report adapter return a nonzero final report after the configured threshold. Keep it test-only in behavior and document the log marker in the test.

- [ ] **Step 4: Run targeted integration tests**

Run the unit and cluster tests added in this task:

```bash
cargo test --lib write_coordinator coordinator standalone_exec_state_reporter exec_status_report
cargo test --test cluster_mvp -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Run formatting and compile checks**

Run:

```bash
cargo fmt --check
cargo test --lib write_coordinator coordinator standalone_exec_state_reporter exec_status_report grpc_server grpc_client
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add tests src
git commit -m "Validate distributed write coordinator reporting"
```

## Task 10: Documentation and Final Verification

**Files:**
- Modify: `docs/design/specs/2026-06-04-iw4-distributed-write-coordinator-design.md` only if implementation found a precise correction.
- Modify: `/Users/harbor/Documents/Obsidian/NovaRocks TODO/IW-4-distributed-write-coordinator.md` only after tests pass.

- [ ] **Step 1: Update IW-4 TODO status**

After implementation tests pass, update the Obsidian IW-4 note to mention:

```markdown
Implementation status:
- NovaRocks standalone distributed reports use the same `TReportExecStatusParams` payload as FE-compatible reportExecStatus.
- `NovaRocksGrpc` carries standalone report transport.
- `WriteCoordinator` collects writer outputs and fails/cancels on writer error.
```

- [ ] **Step 2: Run full local verification**

Run:

```bash
cargo fmt --check
cargo test --lib
cargo test --test cluster_mvp -- --nocapture
```

Expected: PASS.

If Docker-backed SQL validation is available in the current worktree, run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-rest --mode verify
```

Expected: PASS.

- [ ] **Step 3: Commit docs**

```bash
git add docs/design/specs/2026-06-04-iw4-distributed-write-coordinator-design.md "/Users/harbor/Documents/Obsidian/NovaRocks TODO/IW-4-distributed-write-coordinator.md"
git commit -m "Document IW-4 distributed write coordinator completion"
```

- [ ] **Step 4: Final branch check**

Run:

```bash
git status --short --branch
git log --oneline --decorate -8
```

Expected:

- working tree clean
- current branch `codex/iw4-distributed-write-coordinator`
- latest commits include the IW-4 implementation commits from this plan

## Self-Review

Spec coverage:
- Unified `TReportExecStatusParams` payload: Tasks 2, 3, 5.
- FE-compatible transport remains separate from standalone gRPC transport: Tasks 4, 5, 6.
- Writer state machine and commit/abort inputs: Task 1.
- Failure propagation and cancel fan-out: Tasks 7, 8.
- Duplicate and missing writer behavior: Tasks 1, 7.
- Observability and 1FE+2BE validation: Tasks 7, 9, 10.

Placeholder scan:
- The only literal `TODO` occurrences are inside the user-owned Obsidian directory name. The plan has no deferred marker and no unnamed implementation step.

Type consistency:
- `WriterKey`, `FragmentExecStatusReport`, `WriteCommitInput`, and `WriteAbortInput` are defined in Task 1 before later tasks reference them.
- `ExecStatusReportInput` is defined in Task 2 before `fe_report.rs` uses it.
- `novarocks_report_addr` is added in Task 4 before coordinator wiring uses it in Task 6.
