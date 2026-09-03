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
use std::sync::{Arc, Mutex};

use novarocks_types::schema::ColumnDef;

use crate::catalog_application::resolver::TargetBackend;
use crate::common::admitted_query_context::{QueryExecutionContext, RequestContext};
use crate::connector::backend::ResolvedTable;
use crate::query_execution::dml::iceberg_writer;
use crate::query_execution::kernels::DmlExecutionKernel;
use novarocks_parser::ast::{Insert, Query};
use novarocks_proto_codec::lifecycle::QueryOptions;
use novarocks_spi::connector::{ConnectorWriteOperationId, LakePublicationId};
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
pub struct ResolvedInsertTarget {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
    pub columns: Vec<ColumnDef>,
    pub planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
    /// Reserved before the first vended metadata observation.  The target is
    /// move-only so this capability remains paired with the native write
    /// attempt that consumes the same provider response.
    pub(crate) attempt_reservation:
        Option<crate::query_execution::completion::QueryAttemptReservation>,
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
    pub publication_id: LakePublicationId,
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
    pub publication_id: LakePublicationId,
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

    fn adjudicate_iceberg_write_publication(
        &self,
        _prepared: &dyn IcebergPreparedInsert,
        _commit: &dyn IcebergInsertCommit,
        _evidence: novarocks_spi::connector::ExternalMutationEvidence,
    ) -> Result<
        novarocks_spi::connector::ExternalMutationOutcome<
            novarocks_spi::connector::ConnectorWriteReceipt,
        >,
        String,
    > {
        Err(
            "Iceberg INSERT engine does not expose same-session publication adjudication"
                .to_string(),
        )
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

/// One INSERT's commit authority.
///
/// The completion is taken by the single commit, so a second commit attempt
/// finds nothing rather than asking the connector twice. The session stays
/// beside it because a `CommitUnknown` is resolved through the same session
/// that issued the commit, never through a replacement.
struct CoreIcebergInsertCommit {
    session: Arc<crate::query_execution::write_session::ConnectorWriteSession>,
    completion: Mutex<Option<crate::query_execution::outcome::ConnectorWriteSessionCompletion>>,
    /// Set only after a commit that is known to have succeeded.
    affected_rows: Mutex<Option<u64>>,
}

impl CoreIcebergInsertCommit {
    fn new(completion: crate::query_execution::outcome::ConnectorWriteSessionCompletion) -> Self {
        Self {
            session: Arc::clone(completion.session()),
            completion: Mutex::new(Some(completion)),
            affected_rows: Mutex::new(None),
        }
    }

    fn commit(
        &self,
        context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<
        novarocks_spi::connector::ExternalMutationOutcome<
            novarocks_spi::connector::ConnectorWriteReceipt,
        >,
        String,
    > {
        let completion = self
            .completion
            .lock()
            .map_err(|_| "Iceberg INSERT commit handle is poisoned".to_string())?
            .take()
            .ok_or_else(|| "Iceberg INSERT write session was already committed".to_string())?;
        let committed =
            crate::query_execution::write_session::finish_write_session(completion, context)
                .map_err(|error| error.to_string())?;
        // Rows become reportable exactly here: after the external commit said
        // it succeeded, and never on a commit whose outcome is unknown.
        *self
            .affected_rows
            .lock()
            .map_err(|_| "Iceberg INSERT commit handle is poisoned".to_string())? =
            committed.affected_rows();
        Ok(committed.into_outcome())
    }

    /// The rows a client may be told about. `None` until a known-successful
    /// commit has happened.
    #[allow(
        dead_code,
        reason = "The gated affected-row count is surfaced to the MySQL result by a later task."
    )]
    fn affected_rows(&self) -> Option<u64> {
        self.affected_rows.lock().ok().and_then(|rows| *rows)
    }
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
        // A REST metadata response may be the first place an Iceberg catalog
        // reveals vended object-store credentials. Reserve the native attempt
        // before that observation, then retain the move-only reservation
        // until this INSERT's raw distributed lifecycle consumes it.
        let attempt_reservation = self
            .query_execution()
            .reserve_initial_attempt()
            .map_err(|error| error.to_string())?;
        let connector_context = attempt_reservation.connector_request_context(connector_context);

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
            connector_context.clone(),
            &target.namespace,
            &target.table,
            novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
        )?;
        crate::mv::domain::iceberg_guard::reject_if_iceberg_mv_table_with_planning_lease_and_context(
            self.mv_storage_observation().as_ref(),
            &planning_lease,
            &target,
            crate::mv::domain::iceberg_guard::IcebergMvUserMutation::Insert,
            connector_context,
        )?;
        let columns = insert_columns_from_connector_metadata(&metadata);
        Ok(ResolvedInsertTarget {
            catalog: target.catalog,
            namespace: target.namespace,
            table: target.table,
            columns,
            planning_lease,
            attempt_reservation: Some(attempt_reservation),
        })
    }

    fn prepare_iceberg_write(
        &self,
        request: PrepareIcebergInsert,
    ) -> Result<PreparedIcebergInsert, String> {
        let ResolvedInsertTarget {
            catalog,
            namespace,
            table,
            columns,
            planning_lease,
            attempt_reservation,
        } = request.target;
        let attempt_reservation = attempt_reservation.ok_or_else(|| {
            "Iceberg INSERT target is missing its reserved native attempt".to_string()
        })?;
        let target = TargetBackend {
            backend_name: "iceberg",
            catalog: catalog.clone(),
            namespace: namespace.clone(),
            table: table.clone(),
        };
        let resolved = ResolvedTable {
            catalog,
            namespace,
            table,
            columns,
            statistics_pin: None,
        };
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
        let connector_context = attempt_reservation.connector_request_context(connector_context);
        let prepared = iceberg_writer::prepare_iceberg_write_with_options(
            self,
            &target,
            &resolved,
            &request.insert_columns,
            &source,
            overwrite_mode,
            &request.target_ref,
            Some(request.execution),
            &connector_context,
            iceberg_writer::IcebergWritePreparationOptions::new(ConnectorWriteOperationId::from(
                request.publication_id,
            )),
            planning_lease.clone(),
            attempt_reservation,
        )?;
        let operation = IcebergInsertOperation {
            publication_id: request.publication_id,
            catalog: prepared.target().catalog.clone(),
            namespace: prepared.target().namespace.clone(),
            table: prepared.target().table.clone(),
            target_ref: request.target_ref,
            attempt_id: request.publication_id.to_string(),
            is_overwrite: prepared.is_overwrite(),
            base_snapshot_id: prepared.base_snapshot_id(),
        };
        Ok(PreparedIcebergInsert {
            operation,
            handle: Arc::new(CorePreparedIcebergInsert { prepared }),
            sql_source: request.sql_source,
        })
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
        commit.commit(prepared.prepared.terminal_request_context())
    }

    fn adjudicate_iceberg_write_publication(
        &self,
        prepared: &dyn IcebergPreparedInsert,
        commit: &dyn IcebergInsertCommit,
        evidence: novarocks_spi::connector::ExternalMutationEvidence,
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
        commit
            .session
            .reconcile(evidence, prepared.prepared.terminal_request_context())
            .map_err(|error| error.to_string())
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
    let Some(completion) = result.write_session else {
        return IcebergWriteReport::NoOp;
    };
    let commit: Arc<dyn IcebergInsertCommit> = Arc::new(CoreIcebergInsertCommit::new(completion));
    IcebergWriteReport::CommitRequired(commit)
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
    use super::*;
    use crate::common::admitted_query_context::{RequestAdmission, RequestContext};
    use crate::common::backend_topology::BackendTopologySnapshot;
    use crate::common::query_cancellation::{QueryCancellationReason, QueryCancellationSource};
    use crate::query_execution::outcome::QueryExecutionResult;
    use crate::runtime::query_result::QueryResult;
    use novarocks_sql::compiler::SessionOptimizerSettings;
    use novarocks_types::ClusterRole;

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
            Arc::new(
                novarocks_sql::compiler::build_builtin_engine_function_catalog()
                    .expect("builtin function catalog"),
            ),
            Arc::new(crate::catalog_application::query_catalog::new_query_catalog_service()),
            None,
            Arc::clone(&connector_control),
            std::sync::Arc::new(crate::connector::ConnectorControlHost::new()),
            Arc::new(crate::connector::unified_statistics::UnifiedStatisticsResolver::default()),
            Arc::new(novarocks_spi::connector::UnavailableMvStorageObservationPort),
            crate::query_execution::compiler::test_query_execution_service(),
        )
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

    /// The whole INSERT terminal, from the completion the coordinator hands
    /// back to the one external commit: the session is asked to commit exactly
    /// once, and only then does the statement have rows it may report.
    #[test]
    fn a_completed_write_session_commits_once_and_then_reports_its_rows() {
        use crate::query_execution::write_session::tests as write_session_fixture;

        let fixture = write_session_fixture::fixture_with_outcome(
            1,
            16,
            write_session_fixture::known_committed(),
        );
        let report = iceberg_write_report_from_result(QueryExecutionResult {
            query_result: QueryResult::empty(),
            write_session: Some(
                crate::query_execution::outcome::ConnectorWriteSessionCompletion::for_test(
                    Arc::clone(&fixture.session),
                    crate::query_execution::write_result::DecodedPreparedWriteSet::for_test(
                        11,
                        Vec::new(),
                    ),
                ),
            ),
            fragment_profiles: Vec::new(),
        });

        let IcebergWriteReport::CommitRequired(handle) = report else {
            panic!("a completed write session must require a commit");
        };
        let commit = handle
            .as_any()
            .downcast_ref::<CoreIcebergInsertCommit>()
            .expect("core commit handle");
        assert_eq!(fixture.session.finish_invocations(), 0);
        assert!(
            commit.affected_rows().is_none(),
            "no rows may be reported before the commit"
        );

        let outcome = commit
            .commit(write_session_fixture::request_context())
            .expect("commit");
        assert!(matches!(
            outcome,
            novarocks_spi::connector::ExternalMutationOutcome::KnownCommitted { .. }
        ));
        assert_eq!(fixture.session.finish_invocations(), 1);
        assert_eq!(fixture.recorded.lock().expect("recorded").finish, 1);
        assert_eq!(commit.affected_rows(), Some(11));

        // The completion was taken by that one commit, so a second attempt
        // cannot ask the connector again.
        let error = commit
            .commit(write_session_fixture::request_context())
            .expect_err("second commit");
        assert!(error.contains("already committed"), "unexpected: {error}");
        assert_eq!(fixture.session.finish_invocations(), 1);
    }

    #[test]
    fn an_unknown_commit_outcome_leaves_the_statement_without_reportable_rows() {
        use crate::query_execution::write_session::tests as write_session_fixture;

        let fixture = write_session_fixture::fixture_with_outcome(
            1,
            16,
            write_session_fixture::commit_unknown(),
        );
        let report = iceberg_write_report_from_result(QueryExecutionResult {
            query_result: QueryResult::empty(),
            write_session: Some(
                crate::query_execution::outcome::ConnectorWriteSessionCompletion::for_test(
                    Arc::clone(&fixture.session),
                    crate::query_execution::write_result::DecodedPreparedWriteSet::for_test(
                        11,
                        Vec::new(),
                    ),
                ),
            ),
            fragment_profiles: Vec::new(),
        });
        let IcebergWriteReport::CommitRequired(handle) = report else {
            panic!("a completed write session must require a commit");
        };
        let commit = handle
            .as_any()
            .downcast_ref::<CoreIcebergInsertCommit>()
            .expect("core commit handle");

        let outcome = commit
            .commit(write_session_fixture::request_context())
            .expect("commit");
        assert!(matches!(
            outcome,
            novarocks_spi::connector::ExternalMutationOutcome::CommitUnknown { .. }
        ));
        assert!(
            commit.affected_rows().is_none(),
            "an unknown commit outcome must not report success"
        );
    }

    #[test]
    fn query_execution_result_maps_absent_write_session_to_noop() {
        let report = iceberg_write_report_from_result(QueryExecutionResult {
            query_result: QueryResult::empty(),
            write_session: None,
            fragment_profiles: Vec::new(),
        });

        assert!(matches!(report, IcebergWriteReport::NoOp));
    }
}
