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

//! Transitional reverse port for the frontend-owned INSERT application flow.
//!
//! The types in this module deliberately expose neither an application state
//! nor connector implementations. The frontend owns INSERT conversion,
//! dispatch, shaping, and transaction orchestration; core retains execution,
//! connector, and external commit truth behind this object-safe boundary.

use std::any::Any;
use std::sync::Arc;

use novarocks_types::schema::ColumnDef;

use crate::catalog_application::resolver::TargetBackend;
use crate::common::admitted_query_context::{QueryExecutionContext, RequestContext};
use crate::connector::backend::ResolvedTable;
use crate::query_execution::dml::external_write_fence::{
    ExternalWriteFenceProposal, external_fence_authority_unavailable, invalid_fence_request,
};
use crate::query_execution::dml::iceberg_writer;
use crate::query_execution::kernels::DmlExecutionKernel;
use novarocks_parser::ast::{Insert, Query};
use novarocks_proto::lifecycle::QueryOptions;
use novarocks_sql::semantic::{Literal, ObjectName};

pub use crate::query_execution::dml::iceberg_writer::PreparedIcebergWriteNativeEncoding;

/// Encode one constant JSON literal for frontend-owned INSERT conversion.
///
/// The binary format remains an execution-layer concern; frontend receives
/// only opaque bytes and owns the decision to fold `parse_json(...)`.
pub fn encode_insert_variant_json(json_text: &str) -> Result<Vec<u8>, String> {
    novarocks_types::value::variant_encode::encode_json_text_to_variant_bytes(json_text)
}

/// One admitted INSERT statement at the frontend route boundary.
pub struct InsertRequest<'a> {
    /// SQLP-5 owns INSERT family and capability facts. `source` below may
    /// only be sliced through spans carried by this statement.
    pub statement: &'a Insert,
    pub source: &'a str,
    pub context: &'a RequestContext,
    pub query_options: Option<&'a QueryOptions>,
}

/// A target name before catalog/backend resolution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InsertTargetName {
    pub parts: Vec<String>,
}

/// INSERT literal independent of core's legacy custom statement AST.
#[derive(Clone, Debug, PartialEq)]
pub enum InsertValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Date(String),
    Array(Vec<InsertValue>),
    Map(Vec<(InsertValue, InsertValue)>),
    Struct(Vec<InsertValue>),
}

/// Overwrite semantics owned by the frontend INSERT command.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InsertOverwriteMode {
    Append,
    FullTable,
    DynamicPartitions,
}

/// Resolve and load a target using the immutable execution admitted for this
/// statement.
pub struct ResolveInsertTarget {
    pub current_catalog: Option<String>,
    pub current_database: String,
    pub target: InsertTargetName,
    pub query_options: Option<QueryOptions>,
    pub execution: QueryExecutionContext,
}

/// Iceberg target metadata used by frontend dispatch and shaping.
#[derive(Clone)]
pub struct ResolvedInsertTarget {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
    pub columns: Vec<ColumnDef>,
    pub planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
}

impl std::fmt::Debug for ResolvedInsertTarget {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResolvedInsertTarget")
            .field("catalog", &self.catalog)
            .field("namespace", &self.namespace)
            .field("table", &self.table)
            .field("columns", &self.columns)
            .finish_non_exhaustive()
    }
}

/// One non-UNION source for an Iceberg INSERT transaction.
#[derive(Clone, Debug, PartialEq)]
pub enum IcebergInsertSource {
    Rows(Vec<Vec<InsertValue>>),
    Query(Box<Query>),
}

/// Prepare an Iceberg INSERT without starting writers or external commit.
pub struct PrepareIcebergInsert {
    pub target: ResolvedInsertTarget,
    pub insert_columns: Vec<String>,
    pub source: IcebergInsertSource,
    /// Original client SQL retained for typed analysis error rendering after
    /// native assembly.
    pub sql_source: String,
    pub overwrite_mode: InsertOverwriteMode,
    pub target_ref: String,
    pub query_options: Option<QueryOptions>,
    pub execution: QueryExecutionContext,
}

/// Opaque core-owned prepared write payload.
pub trait IcebergPreparedInsert: Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

/// Opaque core-owned commit payload produced by a coordinated write.
pub trait IcebergInsertCommit: Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

/// Stable operation facts required by the frontend transaction runner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IcebergInsertOperation {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
    pub target_ref: String,
    pub attempt_id: String,
    /// Application-visible statement classification; the provider commit mode
    /// remains private to Core and the connector.
    pub is_overwrite: bool,
    pub base_snapshot_id: Option<i64>,
}

/// Prepared operation facts plus a payload that frontend can only return.
pub struct PreparedIcebergInsert {
    pub operation: IcebergInsertOperation,
    pub handle: Arc<dyn IcebergPreparedInsert>,
    pub sql_source: String,
}

/// Connector-neutral result of the coordinated writer phase.
pub enum IcebergWriteReport {
    Aborted {
        reason: String,
        has_staged_files: bool,
    },
    NoOp,
    CommitRequired(Arc<dyn IcebergInsertCommit>),
}

/// Iceberg write port used by frontend-owned native INSERT orchestration.
// Design: ADR-0021 (docs/adr/ADR-0021-native-frontend-insert-is-iceberg-only.md)
pub trait InsertEngine: Send + Sync {
    fn resolve_target(&self, request: ResolveInsertTarget) -> Result<ResolvedInsertTarget, String>;

    fn prepare_iceberg_write(
        &self,
        request: PrepareIcebergInsert,
    ) -> Result<PreparedIcebergInsert, String>;

    /// Establish the caller's external operation fence on the exact write
    /// authority this preparation retained (CP-3B spec D2).
    ///
    /// The frontend calls this after its durable intent reaches `Writing` and
    /// before [`Self::run_iceberg_write`], because the coordinated write is the
    /// first call that can reach a writer. The preparation already holds the
    /// activated write lease, so the fence lands on the same authority that
    /// later commits.
    ///
    /// The default fails closed. There is deliberately no unfenced dispatch:
    /// an engine that cannot expose its write authority must not run a writer.
    fn establish_iceberg_write_external_fence(
        &self,
        _prepared: &dyn IcebergPreparedInsert,
        _proposal: &dyn ExternalWriteFenceProposal,
    ) -> Result<
        novarocks_spi::connector::ConnectorEstablishedWriteFence,
        novarocks_spi::connector::ConnectorError,
    > {
        Err(external_fence_authority_unavailable(
            "Iceberg INSERT engine does not expose an external operation fence authority",
        ))
    }

    fn run_iceberg_write(
        &self,
        prepared: &dyn IcebergPreparedInsert,
    ) -> Result<IcebergWriteReport, String>;

    /// Borrow the exact Core-sealed plan/preparation pair for Frontend native
    /// wire assembly. Implementations that do not own a real DML plan fail
    /// closed rather than falling back to Core-side encoding.
    fn iceberg_write_native_encoding<'a>(
        &self,
        _prepared: &'a dyn IcebergPreparedInsert,
    ) -> Result<PreparedIcebergWriteNativeEncoding<'a>, crate::dml::error::DmlExecutionError> {
        Err(crate::dml::error::DmlExecutionError::from(
            "Iceberg INSERT engine does not expose native encoding input".to_string(),
        ))
    }

    /// Execute the request finalized from the exact pair previously borrowed
    /// through [`Self::iceberg_write_native_encoding`].
    fn run_iceberg_write_with_native_bundle(
        &self,
        _prepared: &dyn IcebergPreparedInsert,
        _native_bundle: crate::query_execution::native_fragment::NativeFragmentAttachment,
    ) -> Result<IcebergWriteReport, String> {
        Err("Iceberg INSERT engine requires Frontend native fragment assembly".to_string())
    }

    fn commit_iceberg_write_terminal(
        &self,
        _prepared: &dyn IcebergPreparedInsert,
        _commit: &dyn IcebergInsertCommit,
    ) -> Result<
        novarocks_spi::connector::ExternalMutationOutcome<
            novarocks_spi::connector::ConnectorWriteReceipt,
        >,
        String,
    > {
        Err("Iceberg INSERT engine does not expose a connector terminal outcome".to_string())
    }

    fn finalize_iceberg_write(&self, prepared: &dyn IcebergPreparedInsert) -> Result<(), String>;
}

struct CorePreparedIcebergInsert {
    prepared: iceberg_writer::PreparedIcebergWrite,
}

impl IcebergPreparedInsert for CorePreparedIcebergInsert {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

struct CoreIcebergInsertCommit {
    completion: crate::query_execution::ConnectorWriteCompletion,
}

impl IcebergInsertCommit for CoreIcebergInsertCommit {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl InsertEngine for DmlExecutionKernel {
    fn resolve_target(&self, request: ResolveInsertTarget) -> Result<ResolvedInsertTarget, String> {
        let name = ObjectName {
            parts: request.target.parts,
        };
        let connector_context = crate::connector::connector_request_context_for_execution(
            request.query_options.as_ref(),
            &request.execution,
        )?;
        crate::connector::validate_request_context(&connector_context)?;

        let target = crate::catalog_application::resolver::resolve_existing_table_target(
            self,
            &name,
            request.current_catalog.as_deref(),
            &request.current_database,
        )?;
        let planning_lease = crate::connector::acquire_metadata_planning_lease(
            self.connector_control().as_ref(),
            &target.catalog,
        )?;
        let metadata = crate::connector::metadata_load_connector_table_with_planning_lease(
            &planning_lease,
            connector_context,
            &target.namespace,
            &target.table,
            novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
        )?;
        crate::mv::domain::iceberg_guard::reject_if_iceberg_mv_table_with_ports(
            self.connector_control().as_ref(),
            self.mv_storage_observation().as_ref(),
            &target,
            crate::mv::domain::iceberg_guard::IcebergMvUserMutation::Insert,
        )?;
        let columns = insert_columns_from_connector_metadata(&metadata);
        Ok(ResolvedInsertTarget {
            catalog: target.catalog,
            namespace: target.namespace,
            table: target.table,
            columns,
            planning_lease,
        })
    }

    fn prepare_iceberg_write(
        &self,
        request: PrepareIcebergInsert,
    ) -> Result<PreparedIcebergInsert, String> {
        let target = target_backend(&request.target, "iceberg");
        let resolved = resolved_table(&request.target);
        let source = match request.source {
            IcebergInsertSource::Rows(rows) => iceberg_writer::IcebergWriteInput::Rows(
                rows.iter()
                    .map(|row| row.iter().map(insert_value_to_literal).collect())
                    .collect(),
            ),
            IcebergInsertSource::Query(query) => iceberg_writer::IcebergWriteInput::Query(query),
        };
        let overwrite_mode = match request.overwrite_mode {
            InsertOverwriteMode::Append => iceberg_writer::IcebergWriteMode::Append,
            InsertOverwriteMode::FullTable => iceberg_writer::IcebergWriteMode::FullTableOverwrite,
            InsertOverwriteMode::DynamicPartitions => {
                iceberg_writer::IcebergWriteMode::DynamicPartitionOverwrite
            }
        };
        let connector_context = crate::connector::connector_request_context_for_execution(
            request.query_options.as_ref(),
            &request.execution,
        )?;
        crate::connector::validate_request_context(&connector_context)?;
        let prepared = iceberg_writer::prepare_iceberg_write(
            self,
            &target,
            &resolved,
            &request.insert_columns,
            &source,
            overwrite_mode,
            &request.target_ref,
            Some(request.execution),
            &connector_context,
            request.target.planning_lease.clone(),
        )?;
        let operation = IcebergInsertOperation {
            catalog: prepared.target().catalog.clone(),
            namespace: prepared.target().namespace.clone(),
            table: prepared.target().table.clone(),
            target_ref: request.target_ref,
            attempt_id: prepared.attempt_id().to_string(),
            is_overwrite: prepared.is_overwrite(),
            base_snapshot_id: prepared.base_snapshot_id(),
        };
        Ok(PreparedIcebergInsert {
            operation,
            handle: Arc::new(CorePreparedIcebergInsert { prepared }),
            sql_source: request.sql_source,
        })
    }

    fn establish_iceberg_write_external_fence(
        &self,
        prepared: &dyn IcebergPreparedInsert,
        proposal: &dyn ExternalWriteFenceProposal,
    ) -> Result<
        novarocks_spi::connector::ConnectorEstablishedWriteFence,
        novarocks_spi::connector::ConnectorError,
    > {
        downcast_prepared(prepared)
            .map_err(invalid_fence_request)?
            .prepared
            .external_fence_authority()?
            .establish(proposal)
    }

    fn run_iceberg_write(
        &self,
        _prepared: &dyn IcebergPreparedInsert,
    ) -> Result<IcebergWriteReport, String> {
        Err("Iceberg INSERT requires Frontend native fragment assembly".to_string())
    }

    fn iceberg_write_native_encoding<'a>(
        &self,
        prepared: &'a dyn IcebergPreparedInsert,
    ) -> Result<PreparedIcebergWriteNativeEncoding<'a>, crate::dml::error::DmlExecutionError> {
        downcast_prepared(prepared)?.prepared.native_encoding()
    }

    fn run_iceberg_write_with_native_bundle(
        &self,
        prepared: &dyn IcebergPreparedInsert,
        native_bundle: crate::query_execution::native_fragment::NativeFragmentAttachment,
    ) -> Result<IcebergWriteReport, String> {
        let prepared = downcast_prepared(prepared)?;
        Ok(iceberg_write_report_from_result(
            prepared
                .prepared
                .run_coordinated_write_with_native_bundle(native_bundle)?,
        ))
    }

    fn commit_iceberg_write_terminal(
        &self,
        prepared: &dyn IcebergPreparedInsert,
        commit: &dyn IcebergInsertCommit,
    ) -> Result<
        novarocks_spi::connector::ExternalMutationOutcome<
            novarocks_spi::connector::ConnectorWriteReceipt,
        >,
        String,
    > {
        let prepared = downcast_prepared(prepared)?;
        let commit = commit
            .as_any()
            .downcast_ref::<CoreIcebergInsertCommit>()
            .ok_or_else(|| "foreign Iceberg INSERT commit handle".to_string())?;
        prepared.prepared.commit_terminal(&commit.completion)
    }

    fn finalize_iceberg_write(&self, prepared: &dyn IcebergPreparedInsert) -> Result<(), String> {
        downcast_prepared(prepared)?.prepared.finalize()
    }
}

/// Shape the INSERT target columns from one exact connector metadata load.
///
/// Every fact INSERT shaping needs is signed per column by the provider, so no
/// concrete table is opened here:
///
/// - `ConnectorTableMetadata::schema` is the full physical Arrow schema. Hidden
///   columns are *marked* in the planning facts rather than removed from the
///   schema, so this field set is the provider's whole current schema.
/// - `write_target_type()` is the provider-signed DML write type, published only
///   where it differs from the read type, so falling back to the Arrow field
///   type reproduces the read-side type exactly.
/// - `write_default()` is the value a write omitting the column receives; it
///   projects onto the neutral catalog vocabulary variant-for-variant.
fn insert_columns_from_connector_metadata(
    metadata: &novarocks_spi::connector::ConnectorTableMetadata,
) -> Vec<ColumnDef> {
    let column_facts = metadata.planning_facts.column_facts();
    metadata
        .schema
        .fields()
        .iter()
        .enumerate()
        // The neutral schema is the read schema: it carries the Iceberg
        // metadata columns (`_file`, `_pos`, row-lineage) that a scan exposes
        // but that are not part of the table's declared column list. SQL column
        // binding must see only declared columns, so drop the ones the provider
        // marked as system columns. Hidden-but-declared columns (the IMV apply
        // key, aggregate-state columns) are `Ordinary` and stay.
        .filter(|(ordinal, _)| {
            column_facts.get(*ordinal).is_none_or(|fact| {
                fact.role() != novarocks_spi::connector::ConnectorTableColumnRole::RowLineageSystem
            })
        })
        .map(|(ordinal, field)| ColumnDef {
            name: field.name().clone(),
            data_type: column_facts
                .get(ordinal)
                .and_then(|fact| fact.write_target_type())
                .cloned()
                .unwrap_or_else(|| field.data_type().clone()),
            nullable: field.is_nullable(),
            write_default: crate::connector::connector_write_default_at(
                &metadata.planning_facts,
                ordinal,
            ),
            logical_type: None,
        })
        .collect()
}

fn downcast_prepared(
    prepared: &dyn IcebergPreparedInsert,
) -> Result<&CorePreparedIcebergInsert, String> {
    prepared
        .as_any()
        .downcast_ref::<CorePreparedIcebergInsert>()
        .ok_or_else(|| "foreign Iceberg INSERT prepared handle".to_string())
}

fn iceberg_write_report_from_result(
    result: crate::query_execution::outcome::QueryExecutionResult,
) -> IcebergWriteReport {
    if let Some(abort) = result.write_abort {
        let has_staged_files = abort
            .completed_writer_outputs
            .iter()
            .any(|writer| !writer.connector_staged_report_frames.is_empty());
        return IcebergWriteReport::Aborted {
            reason: abort.reason,
            has_staged_files,
        };
    }
    let Some(completion) = result.connector_completion else {
        return IcebergWriteReport::NoOp;
    };
    let commit: Arc<dyn IcebergInsertCommit> = Arc::new(CoreIcebergInsertCommit { completion });
    IcebergWriteReport::CommitRequired(commit)
}

fn target_backend(target: &ResolvedInsertTarget, backend_name: &'static str) -> TargetBackend {
    TargetBackend {
        backend_name,
        catalog: target.catalog.clone(),
        namespace: target.namespace.clone(),
        table: target.table.clone(),
    }
}

fn resolved_table(target: &ResolvedInsertTarget) -> ResolvedTable {
    ResolvedTable {
        catalog: target.catalog.clone(),
        namespace: target.namespace.clone(),
        table: target.table.clone(),
        columns: target.columns.clone(),
        statistics_pin: None,
    }
}

fn insert_value_to_literal(value: &InsertValue) -> Literal {
    match value {
        InsertValue::Null => Literal::Null,
        InsertValue::Bool(value) => Literal::Bool(*value),
        InsertValue::Int(value) => Literal::Int(*value),
        InsertValue::Float(value) => Literal::Float(*value),
        InsertValue::String(value) => Literal::String(value.clone()),
        InsertValue::Date(value) => Literal::Date(value.clone()),
        InsertValue::Array(values) => {
            Literal::Array(values.iter().map(insert_value_to_literal).collect())
        }
        InsertValue::Map(values) => Literal::Map(
            values
                .iter()
                .map(|(key, value)| (insert_value_to_literal(key), insert_value_to_literal(value)))
                .collect(),
        ),
        InsertValue::Struct(values) => {
            Literal::Struct(values.iter().map(insert_value_to_literal).collect())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::common::admitted_query_context::{RequestAdmission, RequestContext};
    use crate::common::backend_topology::BackendTopologySnapshot;
    use crate::common::query_cancellation::{QueryCancellationReason, QueryCancellationSource};
    use crate::query_execution::outcome::QueryExecutionResult;
    use crate::query_execution::write::{
        WriteAbortInput, WriteCommitInput, WriterCommitInput, WriterKey,
    };
    use crate::runtime::query_result::QueryResult;
    use novarocks_sql::compiler::SessionOptimizerSettings;
    use novarocks_types::ClusterRole;
    use novarocks_types::UniqueId;

    fn cancelled_execution() -> QueryExecutionContext {
        let cancellation = QueryCancellationSource::new();
        let request = RequestContext::admit(RequestAdmission::new(
            None,
            "db".to_string(),
            ClusterRole::Fe,
            BackendTopologySnapshot::empty(19),
            None,
            cancellation.view(),
            SessionOptimizerSettings::default(),
        ));
        cancellation.request(QueryCancellationReason::ClientDisconnected);
        request.execution().clone()
    }

    fn test_dml_kernel() -> DmlExecutionKernel {
        let connector_control: Arc<dyn novarocks_spi::connector::ConnectorControlRegistry> =
            Arc::new(crate::query_execution::compiler::TestConnectorControlRegistry::default());
        DmlExecutionKernel::new(
            Arc::new(crate::catalog_application::query_catalog::new_query_catalog_service()),
            None,
            Arc::clone(&connector_control),
            Arc::new(crate::connector::unified_statistics::UnifiedStatisticsResolver::default()),
            Arc::new(novarocks_spi::connector::UnavailableMvStorageObservationPort),
            crate::query_execution::compiler::test_query_execution_service(),
        )
    }

    fn write_abort_with_staged_report() -> WriteAbortInput {
        let write_id = UniqueId::new(10, 20);
        let writer_key = WriterKey {
            query_id: write_id,
            fragment_instance_id: UniqueId::new(101, 201),
            backend_num: 0,
        };
        WriteAbortInput {
            write_id,
            reason: "query timed out waiting for write final reports".to_string(),
            completed_writer_outputs: vec![WriterCommitInput {
                writer_id: 0,
                fragment_id: 0,
                writer_key,
                connector_staged_report_frames: vec![
                    novarocks_proto::novarocks::ConnectorStagedReportFrame::default(),
                ],
                load_counters: BTreeMap::from([("loaded.rows".to_string(), "11".to_string())]),
                loaded_rows: 11,
                loaded_bytes: 110,
                filtered_rows: 0,
            }],
            incomplete_writers: Vec::new(),
        }
    }

    #[test]
    fn insert_engine_is_object_safe() {
        fn accepts_object_safe_engine(_engine: Option<&dyn InsertEngine>) {}
        accepts_object_safe_engine(None);
    }

    #[test]
    fn target_resolution_rechecks_cancellation_before_metadata_lookup() {
        let kernel = test_dml_kernel();
        let error = kernel
            .resolve_target(ResolveInsertTarget {
                current_catalog: None,
                current_database: "db".to_string(),
                target: InsertTargetName {
                    parts: vec!["ice".to_string(), "db".to_string(), "orders".to_string()],
                },
                query_options: None,
                execution: cancelled_execution(),
            })
            .expect_err("cancelled INSERT must fail before connector metadata lookup");

        assert_eq!(error, "connector request was cancelled");
    }

    #[test]
    fn query_execution_result_ignores_legacy_fileless_overwrite_carrier() {
        let report = iceberg_write_report_from_result(QueryExecutionResult {
            query_result: QueryResult::empty(),
            write_commit: Some(WriteCommitInput {
                write_id: UniqueId::new(1, 2),
                writers: Vec::new(),
            }),
            write_abort: None,
            connector_completion: None,
            fragment_profiles: Vec::new(),
        });

        assert!(matches!(report, IcebergWriteReport::NoOp));
    }

    #[test]
    fn query_execution_result_maps_absent_commit_to_noop() {
        let report = iceberg_write_report_from_result(QueryExecutionResult {
            query_result: QueryResult::empty(),
            write_commit: None,
            write_abort: None,
            connector_completion: None,
            fragment_profiles: Vec::new(),
        });

        assert!(matches!(report, IcebergWriteReport::NoOp));
    }

    #[test]
    fn query_execution_result_maps_fileless_fast_append_to_noop() {
        let report = iceberg_write_report_from_result(QueryExecutionResult {
            query_result: QueryResult::empty(),
            write_commit: Some(WriteCommitInput {
                write_id: UniqueId::new(1, 2),
                writers: Vec::new(),
            }),
            write_abort: None,
            connector_completion: None,
            fragment_profiles: Vec::new(),
        });

        assert!(matches!(report, IcebergWriteReport::NoOp));
    }

    #[test]
    fn query_execution_result_maps_writer_abort_with_staged_files() {
        let report = iceberg_write_report_from_result(QueryExecutionResult {
            query_result: QueryResult::empty(),
            write_commit: None,
            write_abort: Some(write_abort_with_staged_report()),
            connector_completion: None,
            fragment_profiles: Vec::new(),
        });

        let IcebergWriteReport::Aborted {
            reason,
            has_staged_files,
        } = report
        else {
            panic!("writer abort must stay an aborted report");
        };
        assert!(reason.contains("timed out"));
        assert!(has_staged_files);
    }

    #[test]
    fn query_execution_result_does_not_treat_empty_commit_info_as_staged_file() {
        let mut abort = write_abort_with_staged_report();
        abort.completed_writer_outputs[0]
            .connector_staged_report_frames
            .clear();
        let report = iceberg_write_report_from_result(QueryExecutionResult {
            query_result: QueryResult::empty(),
            write_commit: None,
            write_abort: Some(abort),
            connector_completion: None,
            fragment_profiles: Vec::new(),
        });

        let IcebergWriteReport::Aborted {
            has_staged_files, ..
        } = report
        else {
            panic!("writer abort must stay an aborted report");
        };
        assert!(!has_staged_files);
    }
}
