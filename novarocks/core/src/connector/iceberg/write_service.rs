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

//! Operation-scoped FE service for Iceberg's connector writer control.
//!
//! The context stays on the control host.  In particular, its sink plan can
//! contain local object-store configuration and its commit executor owns live
//! catalog state; neither is serialized into a writer handle or sent to a BE.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use iceberg::spec::TableMetadata;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorMutationFailure, ConnectorMutationFailureKind,
    ConnectorStagedReport, ConnectorWriteAbortRequest, ConnectorWriteCohortId,
    ConnectorWriteCommitRequest, ConnectorWriteOperationId, ConnectorWritePlanningRequest,
    ExternalMutationFinalization,
};
use serde::Serialize;
use sha2::{Digest, Sha256};

use super::change_stream_routing::ChangeStreamWriterCommitPlan;
use super::commit::{
    CleanupAttempt, CommitOutcome, CommitServiceError, CowUpdateRewriteSet, CowUpdateTouchedFile,
    RecoveryEvidence, RunInput, WrittenFile, run_iceberg_commit,
};
use super::report::IcebergWriterReport;
use super::sink_plan::IcebergSinkPlan;
use super::write_contract::{
    decode_sink_plan_handle_payload, decode_writer_reports, encode_sink_plan_handle_payload,
};
use super::write_control::{
    IcebergFirstRefreshWritePlanPayloadV2, IcebergWriteControlBackend, IcebergWriteControlPlan,
    IcebergWritePlanPayloadV1, IcebergWriteReconcileEvidenceV1,
};
use crate::engine::IcebergWriteCommitExecutor;

/// FE-local operation table for write services created by a DML owner before
/// its durable journal record.  The table is intentionally keyed by the UUID
/// persisted in `attempt_id`; it never exposes a current-generation takeover
/// or a provider payload fallback.
#[derive(Clone, Default)]
pub(crate) struct IcebergWriteServiceRegistry {
    services: Arc<Mutex<HashMap<ConnectorWriteOperationId, IcebergWriteServiceEntry>>>,
}

#[derive(Clone)]
enum IcebergWriteServiceEntry {
    Ready(Arc<dyn IcebergWriteControlBackend>),
    Lazy {
        activation_digest: [u8; 32],
        factory: Arc<
            dyn Fn() -> Result<Arc<dyn IcebergWriteControlBackend>, ConnectorError> + Send + Sync,
        >,
    },
}

impl IcebergWriteServiceRegistry {
    pub(crate) fn register<S>(
        &self,
        operation_id: ConnectorWriteOperationId,
        service: S,
    ) -> Result<(), ConnectorError>
    where
        S: IcebergWriteControlBackend + 'static,
    {
        let mut services = self
            .services
            .lock()
            .map_err(|error| internal(format!("Iceberg write service registry lock: {error}")))?;
        if services
            .insert(
                operation_id,
                IcebergWriteServiceEntry::Ready(Arc::new(service)),
            )
            .is_some()
        {
            return Err(invalid(
                "Iceberg write service already exists for connector operation ID",
            ));
        }
        Ok(())
    }

    /// Reserves an operation after exact-lease admission.  It intentionally
    /// does not construct the provider service: the first SPI `plan` request
    /// creates and caches it under this operation ID.
    pub(crate) fn register_lazy<F>(
        &self,
        operation_id: ConnectorWriteOperationId,
        activation_digest: [u8; 32],
        factory: F,
    ) -> Result<(), ConnectorError>
    where
        F: Fn() -> Result<Arc<dyn IcebergWriteControlBackend>, ConnectorError>
            + Send
            + Sync
            + 'static,
    {
        let mut services = self
            .services
            .lock()
            .map_err(|error| internal(format!("Iceberg write service registry lock: {error}")))?;
        match services.get(&operation_id) {
            Some(IcebergWriteServiceEntry::Lazy {
                activation_digest: existing,
                ..
            }) if existing == &activation_digest => return Ok(()),
            Some(_) => {
                return Err(invalid(
                    "Iceberg write operation already has a conflicting service activation",
                ));
            }
            None => {}
        }
        services.insert(
            operation_id,
            IcebergWriteServiceEntry::Lazy {
                activation_digest,
                factory: Arc::new(factory),
            },
        );
        Ok(())
    }

    fn resolve(
        &self,
        operation_id: ConnectorWriteOperationId,
    ) -> Result<Arc<dyn IcebergWriteControlBackend>, ConnectorError> {
        let mut services = self
            .services
            .lock()
            .map_err(|error| internal(format!("Iceberg write service registry lock: {error}")))?;
        let entry = services.get(&operation_id).cloned().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::NotFound,
                "Iceberg connector write operation has no FE control service",
            )
        })?;
        match entry {
            IcebergWriteServiceEntry::Ready(service) => Ok(service),
            IcebergWriteServiceEntry::Lazy {
                activation_digest: _,
                factory,
            } => {
                let service = factory()?;
                services.insert(
                    operation_id,
                    IcebergWriteServiceEntry::Ready(Arc::clone(&service)),
                );
                Ok(service)
            }
        }
    }
}

/// Stable binding-level dispatch over the operation table.  The outer
/// `IcebergWriteControlAdapter` retains generation, plan, idempotency and
/// evidence rules; this backend only refuses to invent an operation scope.
#[derive(Clone)]
pub(crate) struct RegisteredIcebergWriteControlBackend {
    services: IcebergWriteServiceRegistry,
}

impl RegisteredIcebergWriteControlBackend {
    pub(crate) fn new(services: IcebergWriteServiceRegistry) -> Self {
        Self { services }
    }
}

impl IcebergWriteControlBackend for RegisteredIcebergWriteControlBackend {
    fn plan(
        &self,
        request: &ConnectorWritePlanningRequest,
    ) -> Result<IcebergWriteControlPlan, ConnectorError> {
        self.services.resolve(request.operation_id)?.plan(request)
    }

    fn commit(
        &self,
        request: &ConnectorWriteCommitRequest,
    ) -> Result<CommitOutcome, CommitServiceError> {
        self.services
            .resolve(request.operation_id())
            .map_err(|error| CommitServiceError::invalid_input(error.to_string()))?
            .commit(request)
    }

    fn abort(
        &self,
        request: &ConnectorWriteAbortRequest,
    ) -> Result<ExternalMutationFinalization, ConnectorError> {
        self.services
            .resolve(request.operation_id())?
            .abort(request)
    }

    fn reconcile(
        &self,
        evidence: &IcebergWriteReconcileEvidenceV1,
    ) -> Result<Option<CommitOutcome>, CommitServiceError> {
        let operation_id = evidence
            .operation_id()
            .map_err(|error| CommitServiceError::invalid_input(error.to_string()))?;
        self.services
            .resolve(operation_id)
            .map_err(|error| CommitServiceError::invalid_input(error.to_string()))?
            .reconcile(evidence)
    }
}

/// The narrow provider-private commit seam used by the FE service.  The
/// implementation for [`IcebergWriteCommitExecutor`] is intentionally the
/// only path that turns decoded Iceberg reports into collector input.
pub(crate) trait IcebergWriteReportCommitter: Send + Sync {
    fn table_metadata(&self) -> &TableMetadata;

    /// Commit raw generic staged reports.  The default preserves the ordinary
    /// single-sink behavior; multi-sink committers override it so routing can
    /// use the immutable writer identity before provider reports are merged.
    fn commit_connector_staged_reports(
        &self,
        reports: Vec<ConnectorStagedReport>,
    ) -> Result<CommitOutcome, CommitServiceError> {
        let mut decoded = Vec::new();
        for report in reports {
            report.validate().map_err(|error| {
                CommitServiceError::invalid_input(format!(
                    "validate connector staged report before Iceberg commit: {error}"
                ))
            })?;
            decoded.extend(
                decode_writer_reports(report.payload(), self.table_metadata()).map_err(
                    |error| {
                        CommitServiceError::invalid_input(format!(
                            "decode canonical Iceberg staged report: {error}"
                        ))
                    },
                )?,
            );
        }
        self.commit_iceberg_writer_reports(decoded)
    }

    /// Commit one accepted attempt for every sealed cohort. Provider-private
    /// committers can retain cohort roles and old-file ownership until the
    /// single catalog commit; ordinary single-cohort writes use the default.
    fn commit_connector_operation(
        &self,
        cohorts: Vec<IcebergAcceptedCohortReports>,
    ) -> Result<CommitOutcome, CommitServiceError> {
        self.commit_connector_staged_reports(
            cohorts
                .into_iter()
                .flat_map(|cohort| cohort.reports)
                .collect(),
        )
    }

    fn commit_iceberg_writer_reports(
        &self,
        reports: Vec<IcebergWriterReport>,
    ) -> Result<CommitOutcome, CommitServiceError>;

    fn abort_iceberg_writer_reports(
        &self,
        reports: Vec<IcebergWriterReport>,
    ) -> Result<CleanupAttempt, String>;

    fn recovery_evidence(&self) -> RecoveryEvidence;
}

/// Empty-input policy for a single primary MV staging cohort. First refresh
/// has no existing target data to replace; a full rebuild must instead commit
/// an empty overwrite so the staging ref faithfully represents the rebuild.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IcebergMvPrimaryEmptyInputPolicy {
    AbortWithoutSnapshot,
    CommitEmptyOverwrite,
}

/// Single-primary MV staging committer. It is the only place where the opaque
/// provider payload becomes Iceberg snapshot provenance, and it derives row
/// count from the complete accepted report set rather than any frontend
/// result carrier.
pub(crate) struct IcebergFirstRefreshWriteReportCommitter {
    executor: Arc<IcebergWriteCommitExecutor>,
    provenance_properties: BTreeMap<String, String>,
    empty_input_policy: IcebergMvPrimaryEmptyInputPolicy,
}

impl IcebergFirstRefreshWriteReportCommitter {
    pub(crate) fn new(
        executor: Arc<IcebergWriteCommitExecutor>,
        provenance_properties: BTreeMap<String, String>,
        empty_input_policy: IcebergMvPrimaryEmptyInputPolicy,
    ) -> Result<Self, ConnectorError> {
        if provenance_properties.contains_key("novarocks.mv.refresh.row_count") {
            return Err(invalid(
                "first-refresh provenance template must not predeclare row count",
            ));
        }
        Ok(Self {
            executor,
            provenance_properties,
            empty_input_policy,
        })
    }

    fn decode_primary_reports(
        &self,
        cohorts: Vec<IcebergAcceptedCohortReports>,
    ) -> Result<Vec<IcebergWriterReport>, CommitServiceError> {
        if cohorts.len() != 1 || cohorts[0].role != IcebergWriteCohortRole::Primary {
            return Err(CommitServiceError::invalid_input(
                "first-refresh write must contain exactly one primary append cohort".to_string(),
            ));
        }
        let mut reports = Vec::new();
        for staged in &cohorts[0].reports {
            staged.validate().map_err(|error| {
                CommitServiceError::invalid_input(format!(
                    "validate first-refresh connector staged report: {error}"
                ))
            })?;
            reports.extend(
                decode_writer_reports(staged.payload(), self.executor.table.metadata())
                    .map_err(CommitServiceError::invalid_input)?,
            );
        }
        Ok(reports)
    }
}

impl IcebergWriteReportCommitter for IcebergFirstRefreshWriteReportCommitter {
    fn table_metadata(&self) -> &TableMetadata {
        self.executor.table.metadata()
    }

    fn commit_connector_operation(
        &self,
        cohorts: Vec<IcebergAcceptedCohortReports>,
    ) -> Result<CommitOutcome, CommitServiceError> {
        let reports = self.decode_primary_reports(cohorts)?;
        let row_count = reports.iter().try_fold(0_i64, |total, report| {
            if report.file.record_count < 0 {
                return Err(CommitServiceError::invalid_input(
                    "first-refresh report has a negative row count".to_string(),
                ));
            }
            total.checked_add(report.file.record_count).ok_or_else(|| {
                CommitServiceError::invalid_input("first-refresh row count overflow".to_string())
            })
        })?;
        // A first refresh targets an empty staging ref. Empty input is an
        // explicit no-op. A rebuild, however, must commit its overwrite even
        // when the source is empty so the prior staging contents disappear.
        if row_count == 0
            && self.empty_input_policy == IcebergMvPrimaryEmptyInputPolicy::AbortWithoutSnapshot
        {
            let cleanup = self
                .executor
                .abort_iceberg_writer_reports(reports)
                .unwrap_or_else(|error| CleanupAttempt::completed(vec![error]));
            return Err(CommitServiceError::known_uncommitted(
                "first-refresh produced zero rows; staging was aborted without a snapshot"
                    .to_string(),
                cleanup,
            ));
        }
        let mut provenance = self.provenance_properties.clone();
        provenance.insert(
            "novarocks.mv.refresh.row_count".to_string(),
            row_count.to_string(),
        );
        self.executor
            .commit_iceberg_writer_reports_with_snapshot_properties(reports, provenance)
    }

    fn commit_iceberg_writer_reports(
        &self,
        reports: Vec<IcebergWriterReport>,
    ) -> Result<CommitOutcome, CommitServiceError> {
        self.executor.commit_iceberg_writer_reports(reports)
    }

    fn abort_iceberg_writer_reports(
        &self,
        reports: Vec<IcebergWriterReport>,
    ) -> Result<CleanupAttempt, String> {
        self.executor.abort_iceberg_writer_reports(reports)
    }

    fn recovery_evidence(&self) -> RecoveryEvidence {
        RecoveryEvidence::from_collector(&self.executor.collector)
    }
}

#[derive(Clone)]
pub(crate) struct IcebergAcceptedCohortReports {
    pub cohort_id: ConnectorWriteCohortId,
    pub role: IcebergWriteCohortRole,
    pub reports: Vec<ConnectorStagedReport>,
}

struct IcebergConvertedCohort {
    cohort_id: ConnectorWriteCohortId,
    role: IcebergWriteCohortRole,
    files: Vec<WrittenFile>,
}

impl IcebergWriteReportCommitter for IcebergWriteCommitExecutor {
    fn table_metadata(&self) -> &TableMetadata {
        self.table.metadata()
    }

    fn commit_iceberg_writer_reports(
        &self,
        reports: Vec<IcebergWriterReport>,
    ) -> Result<CommitOutcome, CommitServiceError> {
        self.commit_iceberg_writer_reports(reports)
    }

    fn abort_iceberg_writer_reports(
        &self,
        reports: Vec<IcebergWriterReport>,
    ) -> Result<CleanupAttempt, String> {
        self.abort_iceberg_writer_reports(reports)
    }

    fn recovery_evidence(&self) -> RecoveryEvidence {
        RecoveryEvidence::from_collector(&self.collector)
    }
}

/// Operation-scoped committer for a change stream with several terminal
/// writer fragments.  It retains raw generic reports until the fragment plan
/// has assigned each one to its data/DV/fresh channel.
pub(crate) struct IcebergChangeStreamWriteReportCommitter {
    executor: Arc<IcebergWriteCommitExecutor>,
    plan: ChangeStreamWriterCommitPlan,
}

impl IcebergChangeStreamWriteReportCommitter {
    pub(crate) fn new(
        executor: Arc<IcebergWriteCommitExecutor>,
        plan: ChangeStreamWriterCommitPlan,
    ) -> Self {
        Self { executor, plan }
    }
}

impl IcebergWriteReportCommitter for IcebergChangeStreamWriteReportCommitter {
    fn table_metadata(&self) -> &TableMetadata {
        self.executor.table.metadata()
    }

    fn commit_connector_staged_reports(
        &self,
        reports: Vec<ConnectorStagedReport>,
    ) -> Result<CommitOutcome, CommitServiceError> {
        self.executor
            .commit_change_stream_staged_reports(reports, &self.plan)
    }

    fn commit_iceberg_writer_reports(
        &self,
        _reports: Vec<IcebergWriterReport>,
    ) -> Result<CommitOutcome, CommitServiceError> {
        Err(CommitServiceError::invalid_input(
            "change-stream Iceberg committer requires writer-identified staged reports".to_string(),
        ))
    }

    fn abort_iceberg_writer_reports(
        &self,
        reports: Vec<IcebergWriterReport>,
    ) -> Result<CleanupAttempt, String> {
        self.executor.abort_iceberg_writer_reports(reports)
    }

    fn recovery_evidence(&self) -> RecoveryEvidence {
        RecoveryEvidence::from_collector(&self.executor.collector)
    }
}

/// COW aggregate committer. It keeps each accepted report attached to the
/// registered old-file/append cohort, builds one rewrite set, and invokes the
/// Iceberg catalog commit exactly once.
pub(crate) struct IcebergCowWriteReportCommitter {
    executor: Arc<IcebergWriteCommitExecutor>,
}

impl IcebergCowWriteReportCommitter {
    pub(crate) fn new(executor: Arc<IcebergWriteCommitExecutor>) -> Self {
        Self { executor }
    }

    fn decode_and_convert(
        &self,
        reports: &[ConnectorStagedReport],
    ) -> Result<(Vec<IcebergWriterReport>, Vec<WrittenFile>), CommitServiceError> {
        let mut decoded = Vec::new();
        for staged in reports {
            staged.validate().map_err(|error| {
                CommitServiceError::invalid_input(format!(
                    "validate Iceberg COW staged report: {error}"
                ))
            })?;
            decoded.extend(
                decode_writer_reports(staged.payload(), self.table_metadata())
                    .map_err(CommitServiceError::invalid_input)?,
            );
        }
        let mut files = Vec::with_capacity(decoded.len());
        for report in &decoded {
            let file = self
                .executor
                .collector
                .convert_writer_report(report.clone())
                .map_err(|message| {
                    let cleanup = self
                        .executor
                        .abort_iceberg_writer_reports(decoded.clone())
                        .unwrap_or_else(|error| CleanupAttempt::completed(vec![error]));
                    CommitServiceError::known_uncommitted(message, cleanup)
                })?;
            if file.content != iceberg::spec::DataContentType::Data {
                return Err(CommitServiceError::invalid_input(format!(
                    "Iceberg COW cohort wrote non-data artifact {}",
                    file.path
                )));
            }
            files.push(file);
        }
        Ok((decoded, files))
    }

    fn run_cow_commit(
        &self,
        files: Vec<WrittenFile>,
        rewrite: CowUpdateRewriteSet,
    ) -> Result<CommitOutcome, CommitServiceError> {
        self.executor.collector.inject_written_files(files);
        let input = RunInput {
            collector: Arc::clone(&self.executor.collector),
            catalog: Arc::clone(&self.executor.catalog),
            table: self.executor.table.clone(),
            fs: self.executor.fs.clone(),
            file_io: self.executor.table.file_io().clone(),
            cleanup_path_mapper: self.executor.cleanup_path_mapper.clone(),
            cow_update_rewrite: Some(rewrite),
            target_ref: self.executor.target_ref.clone(),
            snapshot_properties: self.executor.snapshot_properties.clone(),
        };
        match crate::runtime::global_async_runtime::data_block_on(async {
            run_iceberg_commit(input).await
        }) {
            Ok(result) => result,
            Err(message) => Err(CommitServiceError::known_uncommitted(
                message,
                CleanupAttempt::not_attempted(),
            )),
        }
    }
}

impl IcebergWriteReportCommitter for IcebergCowWriteReportCommitter {
    fn table_metadata(&self) -> &TableMetadata {
        self.executor.table.metadata()
    }

    fn commit_connector_operation(
        &self,
        cohorts: Vec<IcebergAcceptedCohortReports>,
    ) -> Result<CommitOutcome, CommitServiceError> {
        let mut converted = Vec::with_capacity(cohorts.len());
        for cohort in cohorts {
            let (_, files) = self.decode_and_convert(&cohort.reports)?;
            converted.push(IcebergConvertedCohort {
                cohort_id: cohort.cohort_id,
                role: cohort.role,
                files,
            });
        }
        let (files, rewrite) =
            build_cow_rewrite_set(self.executor.table.metadata().uuid().to_string(), converted)?;
        self.run_cow_commit(files, rewrite)
    }

    fn commit_iceberg_writer_reports(
        &self,
        _reports: Vec<IcebergWriterReport>,
    ) -> Result<CommitOutcome, CommitServiceError> {
        Err(CommitServiceError::invalid_input(
            "Iceberg COW commit requires cohort-attributed staged reports".to_string(),
        ))
    }

    fn abort_iceberg_writer_reports(
        &self,
        reports: Vec<IcebergWriterReport>,
    ) -> Result<CleanupAttempt, String> {
        self.executor.abort_iceberg_writer_reports(reports)
    }

    fn recovery_evidence(&self) -> RecoveryEvidence {
        RecoveryEvidence::from_collector(&self.executor.collector)
    }
}

fn build_cow_rewrite_set(
    target_table_uuid: String,
    cohorts: Vec<IcebergConvertedCohort>,
) -> Result<(Vec<WrittenFile>, CowUpdateRewriteSet), CommitServiceError> {
    let mut base_snapshot_id = None;
    let mut touched = Vec::new();
    let mut appended_files = Vec::new();
    let mut all_files = Vec::new();
    let mut old_files = HashSet::new();
    let mut output_paths = HashSet::new();
    let mut updated_row_ids = BTreeSet::new();
    let mut append_seen = false;

    for cohort in cohorts {
        if cohort.files.is_empty() {
            return Err(CommitServiceError::invalid_input(format!(
                "Iceberg COW cohort {:?} produced no data files",
                cohort.cohort_id.to_bytes()
            )));
        }
        let paths = cohort
            .files
            .iter()
            .map(|file| file.path.clone())
            .collect::<Vec<_>>();
        for path in &paths {
            if !output_paths.insert(path.clone()) {
                return Err(CommitServiceError::invalid_input(format!(
                    "Iceberg COW replacement or append file belongs to multiple cohorts: {path}"
                )));
            }
        }
        match cohort.role {
            IcebergWriteCohortRole::Primary => {
                return Err(CommitServiceError::invalid_input(
                    "Iceberg COW aggregate contains a primary-role cohort".to_string(),
                ));
            }
            IcebergWriteCohortRole::CowRewrite {
                base_snapshot_id: cohort_base,
                old_file,
                matched_row_ids,
            } => {
                ensure_same_cow_base(&mut base_snapshot_id, cohort_base)?;
                if !old_files.insert(old_file.clone()) {
                    return Err(CommitServiceError::invalid_input(format!(
                        "Iceberg COW old file is registered by multiple cohorts: {old_file}"
                    )));
                }
                for row_id in &matched_row_ids {
                    if !updated_row_ids.insert(*row_id) {
                        return Err(CommitServiceError::invalid_input(format!(
                            "Iceberg COW row ID belongs to multiple rewrite cohorts: {row_id}"
                        )));
                    }
                }
                touched.push(CowUpdateTouchedFile {
                    old_file,
                    new_files: paths,
                    row_ids: matched_row_ids,
                });
            }
            IcebergWriteCohortRole::CowAppend {
                base_snapshot_id: cohort_base,
            } => {
                ensure_same_cow_base(&mut base_snapshot_id, cohort_base)?;
                if append_seen {
                    return Err(CommitServiceError::invalid_input(
                        "Iceberg COW aggregate contains more than one append cohort".to_string(),
                    ));
                }
                append_seen = true;
                appended_files.extend(cohort.files.iter().cloned());
            }
        }
        all_files.extend(cohort.files);
    }
    if touched.is_empty() {
        return Err(CommitServiceError::invalid_input(
            "Iceberg COW aggregate has no rewrite cohorts".to_string(),
        ));
    }
    touched.sort_by(|left, right| left.old_file.cmp(&right.old_file));
    Ok((
        all_files,
        CowUpdateRewriteSet {
            base_snapshot_id: base_snapshot_id.expect("COW rewrite has a base snapshot"),
            target_table_uuid,
            updated_row_ids: updated_row_ids.into_iter().collect(),
            touched_data_files: touched,
            appended_files,
        },
    ))
}

fn ensure_same_cow_base(
    current: &mut Option<i64>,
    candidate: i64,
) -> Result<(), CommitServiceError> {
    match current {
        Some(expected) if *expected != candidate => Err(CommitServiceError::invalid_input(
            format!("Iceberg COW cohorts disagree on base snapshot: {expected} vs {candidate}"),
        )),
        Some(_) => Ok(()),
        None => {
            *current = Some(candidate);
            Ok(())
        }
    }
}

/// Provider-private role retained on the exact-generation FE service. COW row
/// IDs never enter the generic SPI or writer handle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum IcebergWriteCohortRole {
    Primary,
    CowRewrite {
        base_snapshot_id: i64,
        old_file: String,
        matched_row_ids: Vec<i64>,
    },
    CowAppend {
        base_snapshot_id: i64,
    },
}

#[derive(Clone)]
pub(crate) struct IcebergWriteCohortContext {
    writer_handle_payloads: IcebergWriterHandlePayloads,
    control_payload: bytes::Bytes,
    role: IcebergWriteCohortRole,
}

#[derive(Clone)]
enum IcebergWriterHandlePayloads {
    Uniform(bytes::Bytes),
    ByFragment(BTreeMap<i32, bytes::Bytes>),
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct IcebergCowCohortControlPayloadV1<'a> {
    version: u16,
    target: &'a str,
    target_ref: &'a str,
    role: &'static str,
    base_snapshot_id: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    old_file: Option<&'a str>,
    matched_row_count: usize,
    matched_row_digest_base64: String,
}

impl IcebergWriteCohortContext {
    fn primary(
        writer_handle_payloads: IcebergWriterHandlePayloads,
        plan_payload: IcebergWritePlanPayloadV1,
    ) -> Result<Self, ConnectorError> {
        validate_writer_handle_payloads(&writer_handle_payloads)?;
        Ok(Self {
            writer_handle_payloads,
            control_payload: plan_payload.encode()?,
            role: IcebergWriteCohortRole::Primary,
        })
    }

    fn first_refresh_primary(
        writer_handle_payloads: IcebergWriterHandlePayloads,
        plan_payload: IcebergFirstRefreshWritePlanPayloadV2,
    ) -> Result<Self, ConnectorError> {
        validate_writer_handle_payloads(&writer_handle_payloads)?;
        Ok(Self {
            writer_handle_payloads,
            control_payload: plan_payload.encode()?,
            role: IcebergWriteCohortRole::Primary,
        })
    }

    pub(crate) fn cow_rewrite(
        writer_handle_payload: bytes::Bytes,
        plan_payload: &IcebergWritePlanPayloadV1,
        base_snapshot_id: i64,
        old_file: String,
        matched_row_ids: Vec<i64>,
    ) -> Result<Self, ConnectorError> {
        if base_snapshot_id < 0 || old_file.is_empty() || matched_row_ids.is_empty() {
            return Err(invalid("Iceberg COW rewrite cohort context is incomplete"));
        }
        if matched_row_ids.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(invalid(
                "Iceberg COW rewrite row IDs must be sorted and unique",
            ));
        }
        let writer_handle_payloads = IcebergWriterHandlePayloads::Uniform(writer_handle_payload);
        validate_writer_handle_payloads(&writer_handle_payloads)?;
        let control_payload = cow_control_payload(
            plan_payload,
            "cow_rewrite",
            base_snapshot_id,
            Some(&old_file),
            &matched_row_ids,
        )?;
        Ok(Self {
            writer_handle_payloads,
            control_payload,
            role: IcebergWriteCohortRole::CowRewrite {
                base_snapshot_id,
                old_file,
                matched_row_ids,
            },
        })
    }

    pub(crate) fn cow_append(
        writer_handle_payload: bytes::Bytes,
        plan_payload: &IcebergWritePlanPayloadV1,
        base_snapshot_id: i64,
    ) -> Result<Self, ConnectorError> {
        if base_snapshot_id < 0 {
            return Err(invalid("Iceberg COW append base snapshot is invalid"));
        }
        let writer_handle_payloads = IcebergWriterHandlePayloads::Uniform(writer_handle_payload);
        validate_writer_handle_payloads(&writer_handle_payloads)?;
        Ok(Self {
            writer_handle_payloads,
            control_payload: cow_control_payload(
                plan_payload,
                "cow_append",
                base_snapshot_id,
                None,
                &[],
            )?,
            role: IcebergWriteCohortRole::CowAppend { base_snapshot_id },
        })
    }

    pub(crate) fn planning_payload(&self) -> bytes::Bytes {
        self.control_payload.clone()
    }

    fn payload_for_writer(
        &self,
        writer: &novarocks_spi::connector::ConnectorWriterIdentity,
    ) -> Result<bytes::Bytes, ConnectorError> {
        match &self.writer_handle_payloads {
            IcebergWriterHandlePayloads::Uniform(payload) => Ok(payload.clone()),
            IcebergWriterHandlePayloads::ByFragment(payloads) => {
                payloads.get(&writer.fragment_id()).cloned().ok_or_else(|| {
                    invalid(format!(
                        "Iceberg multi-sink write has no handle payload for writer fragment {}",
                        writer.fragment_id()
                    ))
                })
            }
        }
    }

    fn ensure_control_payload(&self, actual: &[u8]) -> Result<(), ConnectorError> {
        if actual != self.control_payload.as_ref() {
            return Err(invalid(
                "Iceberg write request does not match its cohort-scoped canonical control payload",
            ));
        }
        Ok(())
    }
}

/// All private state needed for one frozen Iceberg connector-write operation.
/// The single-cohort template admits only the deterministic primary cohort;
/// multi-cohort COW registers an exact immutable map before service install.
#[derive(Clone)]
pub(crate) struct IcebergWriteControlServiceContext {
    cohorts: IcebergWriteCohortContexts,
    commit_executor: Arc<dyn IcebergWriteReportCommitter>,
}

#[derive(Clone)]
enum IcebergWriteCohortContexts {
    PrimaryTemplate(IcebergWriteCohortContext),
    Sealed(BTreeMap<ConnectorWriteCohortId, IcebergWriteCohortContext>),
}

impl IcebergWriteControlServiceContext {
    pub(crate) fn new(
        sink_plan: Arc<IcebergSinkPlan>,
        plan_payload: IcebergWritePlanPayloadV1,
        commit_executor: Arc<dyn IcebergWriteReportCommitter>,
    ) -> Result<Self, ConnectorError> {
        let writer_handle_payload = encode_sink_plan_handle_payload(&sink_plan)
            .map_err(|error| invalid(format!("encode Iceberg writer handle template: {error}")))?;
        Self::new_with_handle_payload(writer_handle_payload, plan_payload, commit_executor)
    }

    pub(crate) fn new_with_handle_payload(
        writer_handle_payload: bytes::Bytes,
        plan_payload: IcebergWritePlanPayloadV1,
        commit_executor: Arc<dyn IcebergWriteReportCommitter>,
    ) -> Result<Self, ConnectorError> {
        Ok(Self {
            cohorts: IcebergWriteCohortContexts::PrimaryTemplate(
                IcebergWriteCohortContext::primary(
                    IcebergWriterHandlePayloads::Uniform(writer_handle_payload),
                    plan_payload,
                )?,
            ),
            commit_executor,
        })
    }

    pub(crate) fn new_with_first_refresh_handle_payload(
        writer_handle_payload: bytes::Bytes,
        plan_payload: IcebergFirstRefreshWritePlanPayloadV2,
        commit_executor: Arc<dyn IcebergWriteReportCommitter>,
    ) -> Result<Self, ConnectorError> {
        Ok(Self {
            cohorts: IcebergWriteCohortContexts::PrimaryTemplate(
                IcebergWriteCohortContext::first_refresh_primary(
                    IcebergWriterHandlePayloads::Uniform(writer_handle_payload),
                    plan_payload,
                )?,
            ),
            commit_executor,
        })
    }

    pub(crate) fn new_with_fragment_handle_payloads(
        writer_handle_payloads: BTreeMap<i32, bytes::Bytes>,
        plan_payload: IcebergWritePlanPayloadV1,
        commit_executor: Arc<dyn IcebergWriteReportCommitter>,
    ) -> Result<Self, ConnectorError> {
        if writer_handle_payloads.is_empty() {
            return Err(invalid("Iceberg multi-sink writer payload map is empty"));
        }
        Ok(Self {
            cohorts: IcebergWriteCohortContexts::PrimaryTemplate(
                IcebergWriteCohortContext::primary(
                    IcebergWriterHandlePayloads::ByFragment(writer_handle_payloads),
                    plan_payload,
                )?,
            ),
            commit_executor,
        })
    }

    pub(crate) fn new_with_cohorts(
        cohorts: BTreeMap<ConnectorWriteCohortId, IcebergWriteCohortContext>,
        commit_executor: Arc<dyn IcebergWriteReportCommitter>,
    ) -> Result<Self, ConnectorError> {
        if cohorts.is_empty() {
            return Err(invalid("Iceberg write cohort context map is empty"));
        }
        Ok(Self {
            cohorts: IcebergWriteCohortContexts::Sealed(cohorts),
            commit_executor,
        })
    }

    fn cohort(
        &self,
        operation_id: ConnectorWriteOperationId,
        cohort_id: ConnectorWriteCohortId,
    ) -> Result<&IcebergWriteCohortContext, ConnectorError> {
        match &self.cohorts {
            IcebergWriteCohortContexts::PrimaryTemplate(context) => {
                if cohort_id != ConnectorWriteCohortId::primary(operation_id) {
                    return Err(invalid(
                        "Iceberg single-cohort service received a non-primary cohort",
                    ));
                }
                Ok(context)
            }
            IcebergWriteCohortContexts::Sealed(cohorts) => {
                cohorts.get(&cohort_id).ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::NotFound,
                        "Iceberg write operation has no registered context for the cohort",
                    )
                })
            }
        }
    }

    fn validate_sealed(
        &self,
        sealed: &novarocks_spi::connector::ConnectorSealedWriteCohortSet,
    ) -> Result<(), ConnectorError> {
        let expected = match &self.cohorts {
            IcebergWriteCohortContexts::PrimaryTemplate(_) => {
                BTreeSet::from([ConnectorWriteCohortId::primary(sealed.operation_id())])
            }
            IcebergWriteCohortContexts::Sealed(cohorts) => cohorts.keys().copied().collect(),
        };
        let actual = sealed
            .cohorts()
            .iter()
            .map(|cohort| cohort.cohort_id())
            .collect::<BTreeSet<_>>();
        if actual != expected {
            return Err(invalid(
                "Iceberg sealed cohort set does not match the registered operation contexts",
            ));
        }
        Ok(())
    }
}

fn validate_writer_handle_payloads(
    payloads: &IcebergWriterHandlePayloads,
) -> Result<(), ConnectorError> {
    match payloads {
        IcebergWriterHandlePayloads::Uniform(payload) => {
            decode_sink_plan_handle_payload(payload).map_err(|error| {
                invalid(format!("decode Iceberg writer handle template: {error}"))
            })?;
        }
        IcebergWriterHandlePayloads::ByFragment(payloads) => {
            for (fragment_id, payload) in payloads {
                decode_sink_plan_handle_payload(payload).map_err(|error| {
                    invalid(format!(
                        "decode Iceberg writer handle template for fragment {fragment_id}: {error}"
                    ))
                })?;
            }
        }
    }
    Ok(())
}

fn cow_control_payload(
    plan: &IcebergWritePlanPayloadV1,
    role: &'static str,
    base_snapshot_id: i64,
    old_file: Option<&str>,
    matched_row_ids: &[i64],
) -> Result<bytes::Bytes, ConnectorError> {
    plan.encode()?;
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks.iceberg-cow-row-ids.v1\0");
    for row_id in matched_row_ids {
        hasher.update(row_id.to_be_bytes());
    }
    serde_json::to_vec(&IcebergCowCohortControlPayloadV1 {
        version: 1,
        target: &plan.target,
        target_ref: &plan.target_ref,
        role,
        base_snapshot_id,
        old_file,
        matched_row_count: matched_row_ids.len(),
        matched_row_digest_base64: base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            hasher.finalize(),
        ),
    })
    .map(bytes::Bytes::from)
    .map_err(|error| {
        internal(format!(
            "encode Iceberg COW cohort control payload: {error}"
        ))
    })
}

/// Provider-private implementation used by [`super::write_control::IcebergWriteControlAdapter`].
/// It has no generation lookup or current-binding fallback: the adapter owns
/// exact-generation validation and this service only works within its frozen
/// operation scope.
#[derive(Clone)]
pub(crate) struct IcebergWriteControlService {
    context: IcebergWriteControlServiceContext,
}

impl IcebergWriteControlService {
    pub(crate) fn new(context: IcebergWriteControlServiceContext) -> Self {
        Self { context }
    }
}

impl IcebergWriteControlBackend for IcebergWriteControlService {
    fn plan(
        &self,
        request: &ConnectorWritePlanningRequest,
    ) -> Result<IcebergWriteControlPlan, ConnectorError> {
        let cohort = self
            .context
            .cohort(request.operation_id, request.cohort_id)?;
        cohort.ensure_control_payload(&request.provider_payload)?;
        let owner = request
            .expected_writers
            .first()
            .map(|writer| writer.binding_key().clone())
            .ok_or_else(|| invalid("Iceberg write plan has no expected writers"))?;
        let handles = request
            .expected_writers
            .iter()
            .cloned()
            .map(|writer| {
                if writer.binding_key() != &owner {
                    return Err(invalid(
                        "Iceberg write plan contains multiple connector binding generations",
                    ));
                }
                let payload = cohort.payload_for_writer(&writer)?;
                novarocks_spi::connector::ConnectorWriterHandle::try_new(
                    owner.clone(),
                    writer,
                    super::write_contract::ICEBERG_WRITE_PAYLOAD_VERSION,
                    payload,
                )
                .map_err(|error| internal(format!("encode Iceberg writer handle: {error}")))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(IcebergWriteControlPlan {
            handles,
            control_payload: cohort.control_payload.clone(),
        })
    }

    fn commit(
        &self,
        request: &novarocks_spi::connector::ConnectorWriteCommitRequest,
    ) -> Result<CommitOutcome, CommitServiceError> {
        self.context
            .validate_sealed(request.sealed())
            .map_err(|error| CommitServiceError::invalid_input(error.to_string()))?;
        let mut cohorts = Vec::with_capacity(request.cohorts().len());
        for completion in request.cohorts() {
            let accepted = completion.accepted().ok_or_else(|| {
                CommitServiceError::invalid_input(
                    "Iceberg commit is missing an accepted cohort attempt".to_string(),
                )
            })?;
            let context = self
                .context
                .cohort(accepted.operation_id(), completion.cohort_id())
                .map_err(|error| CommitServiceError::invalid_input(error.to_string()))?;
            context
                .ensure_control_payload(accepted.control_payload())
                .map_err(|error| CommitServiceError::invalid_input(error.to_string()))?;
            cohorts.push(IcebergAcceptedCohortReports {
                cohort_id: completion.cohort_id(),
                role: context.role.clone(),
                reports: accepted.reports().to_vec(),
            });
        }
        self.context
            .commit_executor
            .commit_connector_operation(cohorts)
    }

    fn abort(
        &self,
        request: &ConnectorWriteAbortRequest,
    ) -> Result<ExternalMutationFinalization, ConnectorError> {
        self.context.validate_sealed(&request.sealed)?;
        let mut reports = Vec::new();
        for cohort in &request.cohorts {
            let context = self
                .context
                .cohort(request.operation_id(), cohort.cohort_id())?;
            for attempt in cohort.accepted().into_iter().chain(cohort.superseded()) {
                context.ensure_control_payload(attempt.control_payload())?;
                for report in attempt.reports() {
                    report.validate()?;
                    reports.extend(
                        decode_writer_reports(
                            report.payload(),
                            self.context.commit_executor.table_metadata(),
                        )
                        .map_err(|error| {
                            invalid(format!("decode canonical Iceberg staged report: {error}"))
                        })?,
                    );
                }
            }
        }
        let cleanup = self
            .context
            .commit_executor
            .abort_iceberg_writer_reports(reports)
            .map_err(|error| {
                internal(format!("Iceberg staged-file abort cleanup failed: {error}"))
            })?;
        Ok(if cleanup.error_count == 0 {
            ExternalMutationFinalization::Complete
        } else {
            ExternalMutationFinalization::Failed(ConnectorMutationFailure::new(
                ConnectorMutationFailureKind::Internal,
                format!(
                    "Iceberg staged-file cleanup completed with {} deletion error(s): {}",
                    cleanup.error_count,
                    cleanup.error_paths.join(", ")
                ),
            ))
        })
    }

    fn reconcile(
        &self,
        _: &IcebergWriteReconcileEvidenceV1,
    ) -> Result<Option<CommitOutcome>, CommitServiceError> {
        // This operation-scoped service does not try to locate a newer
        // generation, reopen a catalog transaction, or infer a snapshot from
        // another FE incarnation.  The adapter will retain the original
        // evidence as CommitUnknown for explicit later recovery.
        Err(CommitServiceError::unknown(
            "Iceberg connector-write reconciliation is unsupported without the original FE operation scope"
                .to_string(),
            self.context.commit_executor.recovery_evidence(),
        ))
    }
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message.into())
}

fn internal(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Internal, message.into())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use arrow::datatypes::Schema;
    use bytes::Bytes;
    use iceberg::spec::{
        DataContentType, DataFileFormat, FormatVersion, NestedField, PartitionSpec, PrimitiveType,
        Schema as IcebergSchema, TableMetadataBuilder, Type,
    };
    use novarocks_spi::connector::{
        ConnectorCancellation, ConnectorExecutionBindingKey, ConnectorInstanceId,
        ConnectorInstanceIncarnation, ConnectorRequestContext, ConnectorSealedWriteCohortSet,
        ConnectorStagedReportSummary, ConnectorTableHandle, ConnectorWriteAttemptCompletion,
        ConnectorWriteCohortCompletion, ConnectorWriteCohortDescriptor, ConnectorWriteExecutionId,
        ConnectorWriteIntent, ConnectorWriteOperationCompletion, ConnectorWriteOperationId,
        ConnectorWriterIdentity, ConnectorWriterTerminalState,
    };
    use parquet::basic::Compression;

    use super::*;
    use crate::connector::iceberg::delete_file::{IcebergFileContent, IcebergFileFormat};
    use crate::connector::iceberg::report::{IcebergPartitionReport, IcebergWrittenFileReport};
    use crate::connector::iceberg::sink_plan::IcebergSinkPlan;
    use crate::connector::iceberg::write_contract::staged_report_from_iceberg_reports;

    struct NeverCancelled;
    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    struct LazyBackend;

    impl IcebergWriteControlBackend for LazyBackend {
        fn plan(
            &self,
            _: &ConnectorWritePlanningRequest,
        ) -> Result<IcebergWriteControlPlan, ConnectorError> {
            Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "test lazy backend does not plan writes",
            ))
        }

        fn commit(
            &self,
            _: &ConnectorWriteCommitRequest,
        ) -> Result<CommitOutcome, CommitServiceError> {
            Err(CommitServiceError::invalid_input(
                "test lazy backend does not commit writes".to_string(),
            ))
        }

        fn abort(
            &self,
            _: &ConnectorWriteAbortRequest,
        ) -> Result<ExternalMutationFinalization, ConnectorError> {
            Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "test lazy backend does not abort writes",
            ))
        }

        fn reconcile(
            &self,
            _: &IcebergWriteReconcileEvidenceV1,
        ) -> Result<Option<CommitOutcome>, CommitServiceError> {
            Err(CommitServiceError::invalid_input(
                "test lazy backend does not reconcile writes".to_string(),
            ))
        }
    }

    struct FakeCommitter {
        metadata: TableMetadata,
        committed: Mutex<Vec<Vec<IcebergWriterReport>>>,
        aborted: Mutex<Vec<Vec<IcebergWriterReport>>>,
    }

    #[test]
    fn lazy_service_activation_is_idempotent_and_constructed_once() {
        let registry = IcebergWriteServiceRegistry::default();
        let operation_id = ConnectorWriteOperationId::from_bytes([91; 16]);
        let constructions = Arc::new(AtomicUsize::new(0));
        let factory_calls = Arc::clone(&constructions);
        registry
            .register_lazy(operation_id, [7; 32], move || {
                factory_calls.fetch_add(1, Ordering::SeqCst);
                Ok(Arc::new(LazyBackend))
            })
            .expect("reserve lazy operation");
        registry
            .register_lazy(operation_id, [7; 32], || Ok(Arc::new(LazyBackend)))
            .expect("same activation is idempotent");
        assert!(
            registry
                .register_lazy(operation_id, [8; 32], || Ok(Arc::new(LazyBackend)))
                .is_err(),
            "different activation facts must fail closed"
        );
        let _first = registry.resolve(operation_id).expect("first resolve");
        let _second = registry.resolve(operation_id).expect("cached resolve");
        assert_eq!(constructions.load(Ordering::SeqCst), 1);
    }

    impl IcebergWriteReportCommitter for FakeCommitter {
        fn table_metadata(&self) -> &TableMetadata {
            &self.metadata
        }

        fn commit_iceberg_writer_reports(
            &self,
            reports: Vec<IcebergWriterReport>,
        ) -> Result<CommitOutcome, CommitServiceError> {
            self.committed.lock().expect("commit lock").push(reports);
            Ok(CommitOutcome {
                new_snapshot_id: 88,
                written_manifest_paths: Vec::new(),
            })
        }

        fn abort_iceberg_writer_reports(
            &self,
            reports: Vec<IcebergWriterReport>,
        ) -> Result<CleanupAttempt, String> {
            self.aborted.lock().expect("abort lock").push(reports);
            Ok(CleanupAttempt::completed(Vec::new()))
        }

        fn recovery_evidence(&self) -> RecoveryEvidence {
            RecoveryEvidence {
                table_ident: "db.t".to_string(),
                op_kind: super::super::commit::CommitOpKind::FastAppend,
                base_snapshot_id: None,
                base_sequence_number: 0,
                staging_dir: "file:///warehouse/db/t/data".to_string(),
            }
        }
    }

    fn metadata() -> TableMetadata {
        TableMetadataBuilder::new(
            IcebergSchema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .expect("schema"),
            PartitionSpec::unpartition_spec(),
            iceberg::spec::SortOrder::unsorted_order(),
            "file:///warehouse/db/t".to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .expect("metadata builder")
        .build()
        .expect("metadata")
        .metadata
    }

    fn sink_plan() -> IcebergSinkPlan {
        let schema = Arc::new(Schema::empty());
        IcebergSinkPlan {
            mode: super::super::sink_plan::IcebergSinkMode::Data,
            table_location: "s3://warehouse/db/t".to_string(),
            data_location: "s3://warehouse/db/t/data".to_string(),
            target_partition_spec_id: 0,
            target_table_metadata: None,
            target_snapshot_id: None,
            position_delete_data_file_partitions: HashMap::new(),
            position_delete_data_file_partition_index_input: None,
            object_store_s3: None,
            file_format: IcebergFileFormat::Parquet,
            report_file_format: "parquet".to_string(),
            compression: Compression::UNCOMPRESSED,
            output_schema: Arc::clone(&schema),
            target_schema: schema,
            equality_delete_columns: Vec::new(),
            row_lineage_data: false,
            output_exprs: Vec::new(),
            partition_exprs: Vec::new(),
            partition_source_column_names: Vec::new(),
            partition_column_names: Vec::new(),
            transform_exprs: Vec::new(),
            position_delete_binding: None,
        }
    }

    fn key() -> ConnectorExecutionBindingKey {
        ConnectorExecutionBindingKey {
            instance_id: ConnectorInstanceId::parse("iceberg.service").expect("instance"),
            incarnation: ConnectorInstanceIncarnation::from_bytes([7; 16]),
        }
    }

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(5),
            Arc::new(NeverCancelled),
            1024,
            4096,
        )
        .expect("context")
    }

    fn payload() -> IcebergWritePlanPayloadV1 {
        IcebergWritePlanPayloadV1 {
            version: 1,
            target: "ice.db.t".to_string(),
            target_ref: "main".to_string(),
        }
    }

    fn request(owner: ConnectorExecutionBindingKey) -> ConnectorWritePlanningRequest {
        let operation_id = ConnectorWriteOperationId::from_bytes([1; 16]);
        let cohort_id = ConnectorWriteCohortId::primary(operation_id);
        let execution_id = ConnectorWriteExecutionId::new([2; 16], 3);
        ConnectorWritePlanningRequest {
            operation_id,
            cohort_id,
            execution_id,
            table: ConnectorTableHandle::try_new(
                owner.instance_id.clone(),
                Bytes::from_static(b"t"),
            )
            .expect("table"),
            intent: ConnectorWriteIntent::Append,
            input_schema: Arc::new(Schema::empty()),
            expected_writers: vec![ConnectorWriterIdentity::new(
                operation_id,
                cohort_id,
                execution_id,
                [4; 16],
                5,
                6,
                0,
                owner,
            )],
            provider_payload: payload().encode().expect("payload"),
            context: context(),
        }
    }

    fn aggregate_requests(
        owner: ConnectorExecutionBindingKey,
        request: &ConnectorWritePlanningRequest,
        plan: &IcebergWriteControlPlan,
        staged: ConnectorStagedReport,
    ) -> (ConnectorWriteCommitRequest, ConnectorWriteAbortRequest) {
        let descriptor = ConnectorWriteCohortDescriptor::new(
            request.cohort_id,
            request.intent,
            request.stable_digest(&owner).expect("planning digest"),
        );
        let sealed = ConnectorSealedWriteCohortSet::try_new(request.operation_id, vec![descriptor])
            .expect("sealed");
        let attempt = ConnectorWriteAttemptCompletion::try_new(
            owner.clone(),
            request.operation_id,
            request.cohort_id,
            request.execution_id,
            [9; 32],
            vec![staged],
            plan.control_payload.clone(),
        )
        .expect("attempt");
        let cohort =
            ConnectorWriteCohortCompletion::try_new(request.cohort_id, Some(attempt), Vec::new())
                .expect("cohort");
        let completion = ConnectorWriteOperationCompletion::try_new(
            owner.clone(),
            sealed.clone(),
            vec![cohort.clone()],
        )
        .expect("completion");
        let commit = ConnectorWriteCommitRequest {
            completion,
            context: context(),
        };
        let abort = ConnectorWriteAbortRequest::try_new(owner, sealed, vec![cohort], context())
            .expect("abort");
        (commit, abort)
    }

    fn service(fake: Arc<FakeCommitter>) -> IcebergWriteControlService {
        IcebergWriteControlService::new(
            IcebergWriteControlServiceContext::new(Arc::new(sink_plan()), payload(), fake)
                .expect("service context"),
        )
    }

    fn report() -> IcebergWriterReport {
        IcebergWriterReport {
            file: IcebergWrittenFileReport {
                path: "file:///warehouse/db/t/data/a.parquet".to_string(),
                format: "parquet".to_string(),
                content: IcebergFileContent::Data,
                record_count: 1,
                file_size_in_bytes: 2,
                partition: IcebergPartitionReport {
                    partition_path: String::new(),
                    null_fingerprint: String::new(),
                    partition_spec_id: 0,
                    partition_values: iceberg::spec::Struct::empty(),
                },
                split_offsets: None,
                column_stats: None,
                referenced_data_file: None,
                first_row_id: None,
                equality_ids: None,
                key_metadata: None,
                content_offset: None,
                content_size_in_bytes: None,
                cardinality: None,
            },
            is_overwrite: None,
            is_rewrite: None,
        }
    }

    fn written_data_file(path: &str) -> WrittenFile {
        WrittenFile {
            path: path.to_string(),
            format: DataFileFormat::Parquet,
            content: DataContentType::Data,
            partition_values: iceberg::spec::Struct::empty(),
            partition_spec_id: 0,
            record_count: 1,
            file_size_in_bytes: 2,
            split_offsets: Vec::new(),
            column_sizes: HashMap::new(),
            value_counts: HashMap::new(),
            null_value_counts: HashMap::new(),
            nan_value_counts: HashMap::new(),
            lower_bounds: HashMap::new(),
            upper_bounds: HashMap::new(),
            key_metadata: None,
            referenced_data_file: None,
            equality_ids: None,
            first_row_id: Some(0),
            content_offset: None,
            content_size_in_bytes: None,
            cardinality: None,
        }
    }

    fn derived_cohort(
        operation_id: ConnectorWriteOperationId,
        role: &[u8],
        semantic_byte: u8,
    ) -> ConnectorWriteCohortId {
        ConnectorWriteCohortId::derive(operation_id, role, [semantic_byte; 32]).expect("cohort ID")
    }

    #[test]
    fn plan_uses_canonical_secret_free_writer_handles() {
        let fake = Arc::new(FakeCommitter {
            metadata: metadata(),
            committed: Mutex::new(Vec::new()),
            aborted: Mutex::new(Vec::new()),
        });
        let service = service(fake);
        let request = request(key());
        let plan = service.plan(&request).expect("plan");
        assert_eq!(plan.handles.len(), 1);
        assert_eq!(plan.handles[0].writer(), &request.expected_writers[0]);
        assert_eq!(plan.control_payload, payload().encode().expect("payload"));
        assert!(!String::from_utf8_lossy(plan.handles[0].payload()).contains("secret"));
    }

    #[test]
    fn plan_rejects_a_different_or_noncanonical_operation_payload() {
        let fake = Arc::new(FakeCommitter {
            metadata: metadata(),
            committed: Mutex::new(Vec::new()),
            aborted: Mutex::new(Vec::new()),
        });
        let service = service(fake);
        let mut request = request(key());
        request.provider_payload =
            Bytes::from_static(b"{\"target_ref\":\"main\",\"target\":\"ice.db.t\",\"version\":1}");
        let error = match service.plan(&request) {
            Ok(_) => panic!("noncanonical payload must fail closed"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[test]
    fn registered_backend_requires_an_exact_operation_service() {
        let registry = IcebergWriteServiceRegistry::default();
        let backend = RegisteredIcebergWriteControlBackend::new(registry.clone());
        let request = request(key());
        let error = match backend.plan(&request) {
            Ok(_) => panic!("unregistered operation must fail closed"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), ConnectorErrorKind::NotFound);

        let fake = Arc::new(FakeCommitter {
            metadata: metadata(),
            committed: Mutex::new(Vec::new()),
            aborted: Mutex::new(Vec::new()),
        });
        registry
            .register(request.operation_id, service(fake))
            .expect("register operation service");
        assert_eq!(
            backend
                .plan(&request)
                .expect("registered operation plans")
                .handles
                .len(),
            1
        );
        assert!(
            registry
                .register(
                    request.operation_id,
                    service(Arc::new(FakeCommitter {
                        metadata: metadata(),
                        committed: Mutex::new(Vec::new()),
                        aborted: Mutex::new(Vec::new()),
                    })),
                )
                .is_err()
        );
    }

    #[test]
    fn commit_and_abort_decode_provider_reports_before_the_executor() {
        let fake = Arc::new(FakeCommitter {
            metadata: metadata(),
            committed: Mutex::new(Vec::new()),
            aborted: Mutex::new(Vec::new()),
        });
        let service = service(Arc::clone(&fake));
        let request = request(key());
        let plan = service.plan(&request).expect("plan");
        let staged = staged_report_from_iceberg_reports(
            request.expected_writers[0].clone(),
            ConnectorWriterTerminalState::Staged,
            ConnectorStagedReportSummary::default(),
            &[report()],
            fake.table_metadata(),
        )
        .expect("canonical report");
        let (commit, abort) = aggregate_requests(key(), &request, &plan, staged);
        assert_eq!(service.commit(&commit).expect("commit").new_snapshot_id, 88);
        let committed = fake.committed.lock().expect("commit lock");
        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].len(), 1);
        assert_eq!(committed[0][0].file.path, report().file.path);

        assert_eq!(
            service.abort(&abort).expect("abort"),
            ExternalMutationFinalization::Complete
        );
        let aborted = fake.aborted.lock().expect("abort lock");
        assert_eq!(aborted.len(), 1);
        assert_eq!(aborted[0].len(), 1);
        assert_eq!(aborted[0][0].file.path, report().file.path);
    }

    #[test]
    fn cow_rewrite_set_keeps_replacement_files_owned_by_their_old_file() {
        let operation_id = ConnectorWriteOperationId::from_bytes([21; 16]);
        let rewrite_a = derived_cohort(operation_id, b"cow-rewrite", 1);
        let rewrite_b = derived_cohort(operation_id, b"cow-rewrite", 2);
        let append = derived_cohort(operation_id, b"cow-append", 3);
        let (all_files, rewrite) = build_cow_rewrite_set(
            "table-uuid".to_string(),
            vec![
                IcebergConvertedCohort {
                    cohort_id: rewrite_b,
                    role: IcebergWriteCohortRole::CowRewrite {
                        base_snapshot_id: 44,
                        old_file: "old-b.parquet".to_string(),
                        matched_row_ids: vec![20],
                    },
                    files: vec![written_data_file("new-b-1.parquet")],
                },
                IcebergConvertedCohort {
                    cohort_id: rewrite_a,
                    role: IcebergWriteCohortRole::CowRewrite {
                        base_snapshot_id: 44,
                        old_file: "old-a.parquet".to_string(),
                        matched_row_ids: vec![3, 7],
                    },
                    files: vec![
                        written_data_file("new-a-1.parquet"),
                        written_data_file("new-a-2.parquet"),
                    ],
                },
                IcebergConvertedCohort {
                    cohort_id: append,
                    role: IcebergWriteCohortRole::CowAppend {
                        base_snapshot_id: 44,
                    },
                    files: vec![written_data_file("fresh.parquet")],
                },
            ],
        )
        .expect("valid COW aggregate");

        assert_eq!(all_files.len(), 4);
        assert_eq!(rewrite.base_snapshot_id, 44);
        assert_eq!(rewrite.updated_row_ids, vec![3, 7, 20]);
        assert_eq!(rewrite.touched_data_files.len(), 2);
        assert_eq!(rewrite.touched_data_files[0].old_file, "old-a.parquet");
        assert_eq!(
            rewrite.touched_data_files[0].new_files,
            vec!["new-a-1.parquet", "new-a-2.parquet"]
        );
        assert_eq!(rewrite.touched_data_files[1].old_file, "old-b.parquet");
        assert_eq!(
            rewrite.touched_data_files[1].new_files,
            vec!["new-b-1.parquet"]
        );
        assert_eq!(rewrite.appended_files.len(), 1);
        assert_eq!(rewrite.appended_files[0].path, "fresh.parquet");
    }

    #[test]
    fn cow_rewrite_set_rejects_a_replacement_path_shared_across_cohorts() {
        let operation_id = ConnectorWriteOperationId::from_bytes([22; 16]);
        let error = build_cow_rewrite_set(
            "table-uuid".to_string(),
            vec![
                IcebergConvertedCohort {
                    cohort_id: derived_cohort(operation_id, b"cow-rewrite", 1),
                    role: IcebergWriteCohortRole::CowRewrite {
                        base_snapshot_id: 45,
                        old_file: "old-a.parquet".to_string(),
                        matched_row_ids: vec![1],
                    },
                    files: vec![written_data_file("shared.parquet")],
                },
                IcebergConvertedCohort {
                    cohort_id: derived_cohort(operation_id, b"cow-rewrite", 2),
                    role: IcebergWriteCohortRole::CowRewrite {
                        base_snapshot_id: 45,
                        old_file: "old-b.parquet".to_string(),
                        matched_row_ids: vec![2],
                    },
                    files: vec![written_data_file("shared.parquet")],
                },
            ],
        )
        .expect_err("shared replacement path must fail closed");
        assert!(error.message().contains("belongs to multiple cohorts"));
    }
}
