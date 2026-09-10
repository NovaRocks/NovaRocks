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

//! Statement-specific reverse port for frontend-local CTAS orchestration.
//!
//! The frontend owns one non-durable statement attempt. Core owns pure SQL
//! preparation, the exact admitted execution context, source execution, and connector calls.
//! Opaque handles ensure the frontend cannot inspect or reconstruct compiler,
//! writer, or provider-private staged-table state.

use std::any::Any;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use novarocks_spi::connector::{
    ConnectorColumnDefinition, ConnectorMutationFailure, ConnectorPartitionTransform,
    ConnectorStagedCreateLease, ConnectorStagedCreatePrepareOutcome,
    ConnectorStagedCreatePrepareRequest, ConnectorStagedCreatePublicationAdjudicationOutcome,
    ConnectorStagedCreatePublicationAdjudicationRequest, ConnectorStagedCreatePublishOutcome,
    ConnectorStagedCreatePublishRequest, ConnectorStagedTableHandle,
    ConnectorStagedWritePlanningRequest, ConnectorStagedWriteProof,
    ConnectorUnanchoredCtasCleanupLease, ConnectorWriteAdmissionPurpose,
    ConnectorWriteFieldRequest, ConnectorWriteInputRequest, ConnectorWriteIntent,
    ConnectorWriteLease, CreatePolicy, ExternalMutationEvidence, ExternalMutationFinalization,
    ExternalMutationOutcome, LakePublicationId,
};
use novarocks_user_error::UserError;

use crate::common::admitted_query_context::QueryExecutionContext;
use crate::query_execution::kernels::DmlExecutionKernel;
use novarocks_parser::ast::{
    CreateTableAsSelect, Literal, LiteralKind, PartitionTransform, Query, TablePartition,
    TableStatement,
};
use novarocks_parser::printer;
use novarocks_proto_codec::lifecycle::QueryOptions;
use novarocks_sql::semantic::ObjectName;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CtasCommand {
    pub target_parts: Vec<String>,
    pub if_not_exists: bool,
    pub source: Query,
    pub partitioning: Vec<ConnectorPartitionTransform>,
    pub properties: BTreeMap<Arc<str>, Arc<str>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CtasAdmissionFailure {
    pub span: novarocks_parser::Span,
    pub message: String,
}

impl CtasCommand {
    /// Lower one already-admitted CTAS node to the narrow execution request.
    ///
    /// The embedded query is already a parser-owned typed AST. This method
    /// never reparses or canonicalizes the surrounding DDL text.
    pub fn from_typed(
        statement: &CreateTableAsSelect,
        _source: &str,
    ) -> Result<Self, CtasAdmissionFailure> {
        let TableStatement::Create(table) = &statement.table;
        if table.temporary || table.external {
            return Err(unsupported(
                table.span,
                "CTAS does not support TEMPORARY or EXTERNAL tables",
            ));
        }
        if table.like.is_some() {
            return Err(unsupported(
                table.span,
                "CTAS does not support CREATE TABLE LIKE",
            ));
        }
        if !table.columns.is_empty() {
            return Err(unsupported(
                table.span,
                "CTAS with explicit column definitions is not supported; use CREATE TABLE then INSERT instead",
            ));
        }
        if let Some(engine) = &table.engine
            && !engine.value.eq_ignore_ascii_case("iceberg")
        {
            return Err(unsupported(
                engine.span,
                format!("CTAS does not support ENGINE = {}", engine.value),
            ));
        }
        if let Some(key) = &table.key {
            return Err(unsupported(key.span, "CTAS does not support table keys"));
        }
        if let Some(distribution) = &table.distribution {
            return Err(unsupported(
                distribution.span,
                "CTAS does not support DISTRIBUTED BY",
            ));
        }
        if !table.order_by.is_empty() {
            return Err(unsupported(table.span, "CTAS does not support ORDER BY"));
        }

        let partitioning = match &table.partition {
            None => Vec::new(),
            Some(TablePartition::Transform(partition)) => partition
                .expressions
                .iter()
                .map(lower_partition_transform)
                .collect::<Result<Vec<_>, _>>()?,
            Some(TablePartition::LegacyRange(partition)) => {
                return Err(unsupported(
                    partition.span,
                    "CTAS does not support legacy RANGE partition definitions",
                ));
            }
        };
        let mut properties: BTreeMap<Arc<str>, Arc<str>> = BTreeMap::new();
        for property in &table.properties {
            let key = literal_text(&property.key)?;
            let value = literal_text(&property.value)?;
            if key.eq_ignore_ascii_case("format-version") && value != "3" {
                return Err(unsupported(
                    property.span,
                    format!("CTAS only supports format-version=3, got '{value}'"),
                ));
            }
            if (key.eq_ignore_ascii_case("row-lineage")
                || key.eq_ignore_ascii_case("write.row-lineage"))
                && !value.eq_ignore_ascii_case("true")
            {
                return Err(unsupported(
                    property.span,
                    format!("CTAS requires row-lineage=true, got '{value}'"),
                ));
            }
            properties.insert(Arc::from(key), Arc::from(value));
        }
        if let Some(comment) = &table.comment {
            properties.insert(Arc::from("comment"), Arc::from(literal_text(comment)?));
        }
        properties.retain(|key, _| {
            !key.eq_ignore_ascii_case("format-version")
                && !key.eq_ignore_ascii_case("write.row-lineage")
        });
        properties.insert(Arc::from("format-version"), Arc::from("3"));
        properties.insert(Arc::from("write.row-lineage"), Arc::from("true"));

        Ok(Self {
            target_parts: table
                .name
                .parts
                .iter()
                .map(|part| part.value.clone())
                .collect(),
            if_not_exists: table.if_not_exists,
            source: statement.query.clone(),
            partitioning,
            properties,
        })
    }
}

fn unsupported(span: novarocks_parser::Span, message: impl Into<String>) -> CtasAdmissionFailure {
    CtasAdmissionFailure {
        span,
        message: message.into(),
    }
}

fn literal_text(literal: &Literal) -> Result<String, CtasAdmissionFailure> {
    match &literal.kind {
        LiteralKind::Null => Err(unsupported(
            literal.span,
            "CTAS properties do not support NULL literals",
        )),
        LiteralKind::Boolean(value) => Ok(value.to_string()),
        LiteralKind::Number(value) | LiteralKind::String(value) | LiteralKind::HexString(value) => {
            Ok(value.clone())
        }
    }
}

fn lower_partition_transform(
    transform: &PartitionTransform,
) -> Result<ConnectorPartitionTransform, CtasAdmissionFailure> {
    let as_arc = |column: &novarocks_parser::ast::Ident| Arc::from(column.value.as_str());
    match transform {
        PartitionTransform::Identity { column, .. } => Ok(ConnectorPartitionTransform::Identity {
            column: as_arc(column),
        }),
        PartitionTransform::Year { column, .. } => Ok(ConnectorPartitionTransform::Year {
            column: as_arc(column),
        }),
        PartitionTransform::Month { column, .. } => Ok(ConnectorPartitionTransform::Month {
            column: as_arc(column),
        }),
        PartitionTransform::Day { column, .. } => Ok(ConnectorPartitionTransform::Day {
            column: as_arc(column),
        }),
        PartitionTransform::Hour { column, .. } => Ok(ConnectorPartitionTransform::Hour {
            column: as_arc(column),
        }),
        PartitionTransform::Bucket {
            buckets,
            column: column_name,
            span,
        } => Ok(ConnectorPartitionTransform::Bucket {
            column: as_arc(column_name),
            num_buckets: u32::try_from(*buckets)
                .map_err(|_| unsupported(*span, "CTAS bucket count exceeds u32"))?,
        }),
        PartitionTransform::Truncate {
            width,
            column: column_name,
            span,
        } => Ok(ConnectorPartitionTransform::Truncate {
            column: as_arc(column_name),
            width: u32::try_from(*width)
                .map_err(|_| unsupported(*span, "CTAS truncate width exceeds u32"))?,
        }),
        PartitionTransform::Void { column, .. } => Ok(ConnectorPartitionTransform::Void {
            column: as_arc(column),
        }),
    }
}

pub enum CtasTargetPreflightOutcome {
    Ready(PreparedCtasTargetPreflight),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CtasTargetPreflightFacts {
    pub provider_id: String,
    pub instance_id: String,
    pub control_runtime_id: [u8; 16],
    pub capability_version: u32,
    pub target_namespace: String,
    pub target_table: String,
}

pub trait CtasPreparedTargetPreflight: Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

pub struct PreparedCtasTargetPreflight {
    pub facts: CtasTargetPreflightFacts,
    pub handle: Arc<dyn CtasPreparedTargetPreflight>,
}

pub trait CtasPreparedCatalogAction: Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

pub struct PreparedCtasCatalogAction {
    pub input_digest: [u8; 32],
    pub handle: Arc<dyn CtasPreparedCatalogAction>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CtasFailureKind {
    InvalidRequest,
    NotFound,
    AlreadyExists,
    Conflict,
    Unsupported,
    Cancelled,
    DeadlineExceeded,
    Unavailable,
    Internal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CtasFailure {
    pub kind: CtasFailureKind,
    pub message: String,
    pub(crate) user_error: Option<CtasUserError>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CtasUserError {
    Analyze(novarocks_sql::analyze_error::AnalyzeError),
}

impl CtasFailure {
    /// Retains the typed failure until the CTAS statement boundary can render
    /// its parser-owned span against the original SQL source.
    fn analyze(error: novarocks_sql::analyze_error::AnalyzeError) -> Self {
        Self {
            kind: CtasFailureKind::InvalidRequest,
            message: error.message().to_string(),
            user_error: Some(CtasUserError::Analyze(error)),
        }
    }

    pub(crate) fn user_error(&self, source: Option<&str>) -> Option<UserError> {
        match self.user_error.as_ref()? {
            CtasUserError::Analyze(error) => Some(error.to_user_error(source)),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct CtasPreparedSourceFacts {
    pub target_catalog: String,
    pub target_namespace: String,
    pub target_table: String,
    pub source_catalog: Option<String>,
    pub source_database: String,
    pub plan_digest: [u8; 32],
    pub schema_digest: [u8; 32],
    pub execution_identity: [u8; 32],
    pub output_columns: Vec<ConnectorColumnDefinition>,
}

pub struct PrepareCtasSourceRequest {
    pub command: CtasCommand,
    pub current_catalog: Option<String>,
    pub current_database: String,
    pub query_options: Option<QueryOptions>,
    pub execution: QueryExecutionContext,
}

pub trait CtasPreparedSource: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn execution_identity(&self) -> [u8; 32];
}

pub struct PreparedCtasSource {
    pub facts: CtasPreparedSourceFacts,
    pub handle: Arc<dyn CtasPreparedSource>,
}

/// Opaque target session. A concrete core implementation retains the same
/// exact fenced-publication lease, fence, ordinary writer handle, opaque
/// locator and proof for foreground stage, publish and abort.
pub trait CtasPreparedTarget: Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

pub trait CtasPreparedWrite: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn execution_identity(&self) -> [u8; 32];
    fn native_encoding(&self) -> Result<CtasNativeEncoding<'_>, CtasFailure>;
}

/// Borrowed access to the exact Core-retained encoding input. Frontend may
/// inspect it only for the native encoder call; Core consumes the same input
/// when the resulting bundle is bound for dispatch.
pub struct CtasNativeEncoding<'a> {
    encoding: std::sync::MutexGuard<
        'a,
        Option<crate::query_execution::compiler::NativeFragmentEncodingInput>,
    >,
}

impl CtasNativeEncoding<'_> {
    pub fn input(
        &self,
    ) -> Result<&crate::query_execution::compiler::NativeFragmentEncodingInput, CtasFailure> {
        self.encoding
            .as_ref()
            .ok_or_else(|| internal_failure("CTAS native encoding input was already consumed"))
    }
}

/// Crash-only CTAS target facts.  Unlike the legacy fenced facts, this is
/// deliberately only the exact staged-create identity that can reach the
/// single publish frontier.  It contains no branch, fence, locator or cleanup
/// authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StandardCtasTargetFacts {
    pub provider_id: String,
    pub instance_id: String,
    pub control_runtime_id: [u8; 16],
    pub publication_id: LakePublicationId,
    pub target_handle_digest: [u8; 32],
}

pub struct PreparedStandardCtasTarget {
    pub facts: StandardCtasTargetFacts,
    pub handle: Arc<dyn CtasPreparedTarget>,
}

pub struct PreparedStandardCtasCatalogAction {
    pub input_digest: [u8; 32],
    pub handle: Arc<dyn CtasPreparedCatalogAction>,
}

pub enum StandardCtasStageOutcome {
    Prepared {
        target: PreparedStandardCtasTarget,
        receipt: novarocks_spi::connector::ConnectorStagedCreateReceipt,
    },
    KnownUncommitted {
        failure: CtasFailure,
    },
    CommitUnknown {
        failure: CtasFailure,
        evidence: ExternalMutationEvidence,
    },
}

pub struct PreparedStandardCtasWrite {
    pub target_facts: StandardCtasTargetFacts,
    pub execution_identity: [u8; 32],
    pub handle: Arc<dyn CtasPreparedWrite>,
}

pub enum StandardCtasPublishOutcome {
    Applied {
        receipt: novarocks_spi::connector::ConnectorStagedCreateReceipt,
        finalization: ExternalMutationFinalization,
    },
    NoOp {
        receipt: novarocks_spi::connector::ConnectorStagedCreateReceipt,
        finalization: ExternalMutationFinalization,
    },
    KnownUncommitted {
        failure: CtasFailure,
    },
    CommitUnknown {
        failure: CtasFailure,
        evidence: ExternalMutationEvidence,
    },
}

/// The only same-statement observation permitted after the standard CTAS
/// publication call returned an exact `CommitUnknown` evidence carrier.
/// It cannot represent a negative result as cleanup or another publication.
pub enum StandardCtasPublicationAdjudicationOutcome {
    Published {
        receipt: novarocks_spi::connector::ConnectorStagedCreateReceipt,
        finalization: ExternalMutationFinalization,
    },
    CommitUnknown {
        failure: CtasFailure,
    },
}

/// The standard staged-create path never carries a takeover fence. Keeping a
/// separate outcome type prevents the retired authority carrier from leaking
/// back into the crash-only CTAS frontier while legacy code is being removed.
///
/// `CommitUnknown` is retained but is no longer reachable from the staged
/// write: a staged write session seals its artifacts without touching the
/// catalog, so until the publication runs there is nothing whose external
/// outcome could be in doubt. A write that fails now says so.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StandardCtasWriteOutcome {
    Completed {
        write: ConnectorStagedWriteProof,
        execution_identity: [u8; 32],
    },
    KnownUncommitted {
        failure: CtasFailure,
    },
    CommitUnknown {
        failure: CtasFailure,
        evidence: ExternalMutationEvidence,
    },
}

/// One-to-one core capability consumed by the frontend CTAS application
/// owner. It is intentionally not a generic connector DML facade.
pub trait CtasEngine: Send + Sync {
    /// Crash-only standard staged-create preflight.  The standard path is
    /// intentionally separate from the legacy fenced surface while T50 still
    /// compiles the historical recovery implementation.
    fn preflight_standard_ctas_target(
        &self,
        _statement: &CreateTableAsSelect,
        _source: &str,
        _command: &CtasCommand,
        _current_catalog: Option<&str>,
        _current_database: &str,
    ) -> Result<CtasTargetPreflightOutcome, CtasFailure> {
        Err(standard_ctas_unsupported())
    }

    fn prepare_standard_ctas_source(
        &self,
        _preflight: &dyn CtasPreparedTargetPreflight,
        _request: PrepareCtasSourceRequest,
    ) -> Result<PreparedCtasSource, CtasFailure> {
        Err(standard_ctas_unsupported())
    }

    fn prepare_standard_ctas_target(
        &self,
        _source: &dyn CtasPreparedSource,
        _publication_id: LakePublicationId,
        _policy: CreatePolicy,
    ) -> Result<PreparedStandardCtasCatalogAction, CtasFailure> {
        Err(standard_ctas_unsupported())
    }

    fn stage_standard_ctas_target(
        &self,
        _action: &dyn CtasPreparedCatalogAction,
    ) -> Result<StandardCtasStageOutcome, CtasFailure> {
        Err(standard_ctas_unsupported())
    }

    fn prepare_standard_ctas_write(
        &self,
        _source: &dyn CtasPreparedSource,
        _target: &dyn CtasPreparedTarget,
    ) -> Result<PreparedStandardCtasWrite, CtasFailure> {
        Err(standard_ctas_unsupported())
    }

    fn bind_standard_ctas_write_native_bundle(
        &self,
        _prepared: &dyn CtasPreparedWrite,
        _native_bundle: crate::query_execution::native_fragment::NativeFragmentAttachment,
    ) -> Result<(), CtasFailure> {
        Err(standard_ctas_unsupported())
    }

    fn execute_standard_ctas_write(
        &self,
        _prepared: &dyn CtasPreparedWrite,
    ) -> StandardCtasWriteOutcome {
        StandardCtasWriteOutcome::KnownUncommitted {
            failure: standard_ctas_unsupported(),
        }
    }

    fn prepare_standard_publish_ctas(
        &self,
        _target: &dyn CtasPreparedTarget,
        _publication_id: LakePublicationId,
        _write: ConnectorStagedWriteProof,
    ) -> Result<PreparedStandardCtasCatalogAction, CtasFailure> {
        Err(standard_ctas_unsupported())
    }

    fn publish_standard_ctas(
        &self,
        _action: &dyn CtasPreparedCatalogAction,
    ) -> Result<StandardCtasPublishOutcome, CtasFailure> {
        Err(standard_ctas_unsupported())
    }

    /// Read-only, exact-evidence publication adjudication. Callers receive
    /// this permission only after one publish outcome reported `CommitUnknown`.
    fn adjudicate_standard_ctas_publication(
        &self,
        _target: &dyn CtasPreparedTarget,
        _evidence: ExternalMutationEvidence,
    ) -> Result<StandardCtasPublicationAdjudicationOutcome, CtasFailure> {
        Err(standard_ctas_unsupported())
    }
}

/// Core-private guard embedded in concrete prepared source/write handles.
/// It proves preparation is inert, preserves one execution identity, and
/// rejects any second execution before reaching the coordinator.
pub(crate) struct CtasSourceExecutionGate {
    execution_identity: [u8; 32],
    executed: AtomicBool,
    source_artifact: Arc<dyn Any + Send + Sync>,
    retained_execution: Mutex<Option<QueryExecutionContext>>,
}

/// Pure CTAS source artifact. It retains the one optimized tree and the exact
/// scan bindings produced by analysis so target preparation cannot trigger a
/// second SQL compilation or a current-generation metadata lookup.
pub(crate) struct PlannedCtasSourceQuery {
    source: novarocks_sql::planning::dml::DmlCtasSourcePlan,
    table_bindings: Arc<crate::catalog_application::query_bindings::QueryTableBindingStore>,
    optimizer_settings: novarocks_sql::compiler::SessionOptimizerSettings,
}

#[allow(clippy::too_many_arguments)]
fn plan_query_for_ctas_source(
    state: &DmlExecutionKernel,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &Query,
    execution: &QueryExecutionContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<PlannedCtasSourceQuery, CtasFailure> {
    let mut query = query.clone();
    if crate::query_execution::planning::time_travel::has_time_travel_refs(&query) {
        crate::query_execution::planning::time_travel::rewrite_time_travel_refs(
            state,
            current_catalog,
            current_database,
            &mut query,
            connector_context,
        )
        .map_err(|error| match error {
            crate::query_execution::planning::time_travel::TimeTravelRewriteError::Engine(
                error,
            ) => internal_failure(error),
            crate::query_execution::planning::time_travel::TimeTravelRewriteError::Analyze(
                error,
            ) => CtasFailure::analyze(error),
        })?;
    }
    let catalog_service_snapshot =
        crate::catalog_application::query_catalog::catalog_service_snapshot(state);
    let analyzer_provider =
        crate::catalog_application::query_materializer::build_catalog_service_provider(
            current_catalog,
            &catalog_service_snapshot,
            state.connector_control().as_ref(),
            connector_context.clone(),
            novarocks_sql::planning::catalog::TableLookupMode::SchemaOnly,
            state.catalog_application().map(Arc::as_ref),
        );
    let table_bindings = analyzer_provider.query_table_bindings();
    let catalog_snapshot =
        novarocks_sql::compiler::SqlPlannerTableSnapshot::new(&analyzer_provider);
    let request = novarocks_sql::compiler::SqlAnalyzeRequest::new(
        novarocks_sql::compiler::SqlStatementInput::parsed_query(Box::new(query)),
        novarocks_sql::compiler::SqlCompileIntent::IcebergWrite {
            root_distribution: novarocks_sql::compiler::RootDistributionRequirement::Any,
        },
        novarocks_sql::compiler::SqlSessionContext {
            current_catalog: current_catalog.map(str::to_string),
            current_database: current_database.to_string(),
            optimizer_settings: execution.optimizer_settings().clone(),
        },
        novarocks_sql::compiler::SqlPlanningEnvironment::Distributed,
        &catalog_snapshot,
        state.function_catalog().as_ref(),
        crate::query_execution::constant_eval::constant_evaluator(),
        None,
        novarocks_sql::compiler::SqlCompileControl::new(
            execution.deadline(),
            crate::query_execution::planning::sql_cancellation_observation(
                execution.cancellation().clone(),
            ),
        ),
    );
    let analyzed = novarocks_sql::compiler::SqlCompiler::analyze(request)
        .map_err(|error| match error {
            novarocks_sql::compiler::SqlCompileError::Analyze(error) => CtasFailure::analyze(error),
            error => internal_failure(error.to_string()),
        })?
        .into_pending()
        .map_err(|error| internal_failure(error.to_string()))?;
    let statistics =
        crate::query_execution::planning::statistics::QueryStatisticsContext::from_statistics_resolver_with_bindings(
            state,
            Arc::clone(&table_bindings),
            connector_context,
        )
        .map_err(internal_failure)?;
    let source = novarocks_sql::planning::dml::compile_ctas_source(
        novarocks_sql::compiler::SqlOptimizeRequest::new(analyzed, &statistics),
    )
    .map_err(internal_failure)?;
    Ok(PlannedCtasSourceQuery {
        source,
        table_bindings,
        optimizer_settings: execution.optimizer_settings().clone(),
    })
}

fn prepare_planned_ctas_connector_write(
    state: &DmlExecutionKernel,
    planned: &PlannedCtasSourceQuery,
    input_schema: arrow::datatypes::SchemaRef,
    query_options: Option<QueryOptions>,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    session: Arc<crate::query_execution::write_session::ConnectorWriteSession>,
) -> Result<
    (
        crate::query_execution::compiler::NativeFragmentEncodingInput,
        PendingCtasDistributedWrite,
    ),
    String,
> {
    // The session both selects this plan shape and owns the writer recipes it
    // carries, so the two are sealed together rather than by two independent
    // choices that could disagree. Sealing first also means the plan's write
    // target ordinal is read off the session instead of assumed.
    let sealed = session
        .seal_write_targets()
        .map_err(|error| error.to_string())?;
    let write_target_ordinal = sealed.sole_target_ordinal()?;
    let dataflow = novarocks_sql::planning::dml::build_ctas_connector_write_dataflow_plan(
        &planned.source,
        input_schema,
        write_target_ordinal,
        session
            .statistics_requirements(write_target_ordinal)
            .map_err(|error| error.to_string())?,
        &planned.optimizer_settings,
    )?;
    let prepared = crate::query_execution::preparation::prepare_fragments(
        &dataflow,
        state.connector_control().as_ref(),
        connector_context,
        Some(planned.table_bindings.as_ref()),
        None,
        crate::query_execution::preparation::ScanPreparationOptions::new(
            planned
                .optimizer_settings
                .connector_static_predicate_pushdown_enabled(),
            None,
        )
        .with_typed_connector_control(
            Arc::clone(state.typed_connector_control()),
            crate::query_execution::compiler::typed_connector_session()?,
        ),
    )?;
    Ok((
        crate::query_execution::compiler::NativeFragmentEncodingInput::new(prepared)
            .with_sealed_write_targets(sealed),
        PendingCtasDistributedWrite {
            query_options,
            session,
        },
    ))
}

enum CoreCtasCatalogActionKind {
    StandardStage {
        preflight: CoreStandardCtasTargetPreflight,
        request: ConnectorStagedCreatePrepareRequest,
        target_slot: Arc<Mutex<Option<Arc<CoreStandardCtasTargetSession>>>>,
    },
    StandardPublish {
        target: Arc<CoreStandardCtasTargetSession>,
        request: ConnectorStagedCreatePublishRequest,
    },
}

struct CoreCtasCatalogAction {
    kind: CoreCtasCatalogActionKind,
    dispatched: AtomicBool,
}

impl CtasPreparedCatalogAction for CoreCtasCatalogAction {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl CoreCtasCatalogAction {
    fn begin_dispatch(&self) -> Result<(), CtasFailure> {
        self.dispatched
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .map_err(|_| CtasFailure {
                kind: CtasFailureKind::InvalidRequest,
                message: "prepared CTAS catalog action has already been dispatched".into(),
                user_error: None,
            })
    }
}

#[derive(Clone)]
struct CoreStandardCtasTargetPreflight {
    target: crate::catalog_application::resolver::TargetBackend,
    lease: ConnectorStagedCreateLease,
    /// Retained so the write session opens on the *same* control generation
    /// that staged the target. Re-acquiring the current one at write time could
    /// hand the session a runtime that never saw the staged create.
    planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
    unanchored_ctas_cleanup_lease: novarocks_spi::connector::ConnectorUnanchoredCtasCleanupLease,
    write_lease: ConnectorWriteLease,
    target_catalog_properties: novarocks_spi::connector::CatalogProperties,
    attempt_reservation:
        Arc<Mutex<Option<crate::query_execution::completion::QueryAttemptReservation>>>,
}

impl CtasPreparedTargetPreflight for CoreStandardCtasTargetPreflight {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

struct CorePreparedStandardCtasSource {
    gate: Arc<CtasSourceExecutionGate>,
    preflight: CoreStandardCtasTargetPreflight,
    command: CtasCommand,
    target: crate::catalog_application::resolver::TargetBackend,
    query_options: Option<QueryOptions>,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
    attempt_reservation: Mutex<Option<crate::query_execution::completion::QueryAttemptReservation>>,
    output_schema: arrow::datatypes::SchemaRef,
    output_columns: Vec<ConnectorColumnDefinition>,
    target_session: Arc<Mutex<Option<Arc<CoreStandardCtasTargetSession>>>>,
    target_prepare_started: AtomicBool,
}

impl CtasPreparedSource for CorePreparedStandardCtasSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn execution_identity(&self) -> [u8; 32] {
        self.gate.execution_identity()
    }
}

struct CoreStandardCtasTargetSession {
    lease: ConnectorStagedCreateLease,
    write_lease: ConnectorWriteLease,
    planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
    target: crate::catalog_application::resolver::TargetBackend,
    handle: ConnectorStagedTableHandle,
    publication_id: LakePublicationId,
    context: Mutex<novarocks_spi::connector::ConnectorRequestContext>,
    write_plan_started: AtomicBool,
    write_unknown_latched: AtomicBool,
    publish_started: AtomicBool,
}

impl CoreStandardCtasTargetSession {
    fn facts(&self) -> StandardCtasTargetFacts {
        StandardCtasTargetFacts {
            provider_id: "iceberg".to_string(),
            instance_id: self.lease.instance_id().as_str().to_string(),
            control_runtime_id: self.lease.control_runtime_id().to_bytes(),
            publication_id: self.publication_id,
            target_handle_digest: self.handle.digest(),
        }
    }

    fn prepare_publish(
        &self,
        publication_id: LakePublicationId,
        write: ConnectorStagedWriteProof,
    ) -> Result<ConnectorStagedCreatePublishRequest, CtasFailure> {
        if publication_id != self.publication_id {
            return Err(internal_failure(
                "standard CTAS publish changed its statement publication ID",
            ));
        }
        if self.write_unknown_latched.load(Ordering::Acquire) {
            return Err(CtasFailure {
                kind: CtasFailureKind::Unavailable,
                message: "standard CTAS writer outcome is unresolved; publication is forbidden"
                    .to_string(),
                user_error: None,
            });
        }
        if self
            .publish_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(internal_failure(
                "standard CTAS publish has already been prepared",
            ));
        }
        Ok(ConnectorStagedCreatePublishRequest {
            operation_id: novarocks_spi::connector::ConnectorMutationOperationId::from_bytes(
                publication_id.to_bytes(),
            ),
            handle: self.handle.clone(),
            write,
            context: self
                .context
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
        })
    }
}

impl CtasPreparedTarget for CoreStandardCtasTargetSession {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// What a staged CTAS target lets its write do.
///
/// Opening the session and binding its result are the whole surface: the target
/// hands the session the frozen facts a catalog load would otherwise have
/// returned, and takes back the one receipt that session produced.
trait CtasWriteTarget: Send + Sync {
    fn begin_write_session(
        &self,
        host: &crate::connector::control_host::ConnectorControlHost,
        output_schema: &arrow::datatypes::SchemaRef,
        context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<Arc<crate::query_execution::write_session::ConnectorWriteSession>, CtasFailure>;

    /// Bind the sealed writer receipt and transfer the attempt's terminal
    /// storage capability to staged-create publication.
    fn bind_write(
        &self,
        write: ConnectorStagedWriteProof,
        terminal_context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<(), novarocks_spi::connector::ConnectorError>;

    fn mark_write_unknown(&self) -> Result<(), novarocks_spi::connector::ConnectorError>;
}

impl CtasWriteTarget for CoreStandardCtasTargetSession {
    fn begin_write_session(
        &self,
        host: &crate::connector::control_host::ConnectorControlHost,
        output_schema: &arrow::datatypes::SchemaRef,
        context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<Arc<crate::query_execution::write_session::ConnectorWriteSession>, CtasFailure>
    {
        if self.write_unknown_latched.load(Ordering::Acquire) {
            return Err(CtasFailure {
                kind: CtasFailureKind::Unavailable,
                message: "standard CTAS writer outcome is unresolved".to_string(),
                user_error: None,
            });
        }
        if self
            .write_plan_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(internal_failure(
                "standard CTAS staged target write has already been prepared",
            ));
        }
        // The staged target vends the opaque table facts the session opens on.
        // This is the only way to reach them: the catalog will not know this
        // table until the publication commits.
        let binding = self
            .lease
            .plan_write(ConnectorStagedWritePlanningRequest {
                handle: self.handle.clone(),
                context,
            })
            .map_err(connector_failure)?;
        let stack =
            crate::connector::write_target::derive_write_stack_lease(host, &self.planning_lease)
                .map_err(internal_failure)?;
        let request = novarocks_spi::connector::write_stack::ConnectorWriteBeginRequest {
            table: Arc::from(format!("{}.{}", self.target.namespace, self.target.table).as_str()),
            target_ref: novarocks_spi::connector::ConnectorWriteTargetRef::main(),
            intent: ConnectorWriteIntent::Append,
            purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
            input: ConnectorWriteInputRequest::Data {
                fields: output_schema
                    .fields()
                    .iter()
                    .map(|field| ConnectorWriteFieldRequest::new(field.as_ref().clone()))
                    .collect(),
            },
            base: None,
            flavor:
                novarocks_spi::connector::write_stack::ConnectorWriteSessionFlavor::StagedCreate(
                    binding.table().clone(),
                ),
            context: binding.context().clone(),
        };
        crate::query_execution::write_session::begin_connector_write_session(
            stack,
            &self.write_lease,
            request,
        )
        .map_err(internal_failure)
    }

    fn bind_write(
        &self,
        write: ConnectorStagedWriteProof,
        terminal_context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<(), novarocks_spi::connector::ConnectorError> {
        self.lease.bind_write(self.handle.clone(), write)?;
        *self
            .context
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = terminal_context;
        Ok(())
    }

    fn mark_write_unknown(&self) -> Result<(), novarocks_spi::connector::ConnectorError> {
        self.write_unknown_latched.store(true, Ordering::Release);
        self.lease.mark_write_unknown(&self.handle)
    }
}

impl CtasSourceExecutionGate {
    pub(crate) fn new(
        execution_identity: [u8; 32],
        source_artifact: Arc<dyn Any + Send + Sync>,
        execution: QueryExecutionContext,
    ) -> Self {
        Self {
            execution_identity,
            executed: AtomicBool::new(false),
            source_artifact,
            retained_execution: Mutex::new(Some(execution)),
        }
    }

    pub(crate) const fn execution_identity(&self) -> [u8; 32] {
        self.execution_identity
    }

    pub(crate) fn source_artifact(&self) -> &(dyn Any + Send + Sync) {
        self.source_artifact.as_ref()
    }

    pub(crate) fn execute_once<T>(
        &self,
        expected_identity: [u8; 32],
        execute: impl FnOnce(&(dyn Any + Send + Sync), QueryExecutionContext) -> Result<T, String>,
    ) -> Result<T, String> {
        if expected_identity != self.execution_identity {
            return Err("CTAS prepared write execution identity mismatch".to_string());
        }
        if self
            .executed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err("CTAS prepared source has already been executed".to_string());
        }
        let execution = self
            .retained_execution
            .lock()
            .map_err(|error| format!("CTAS execution context lock: {error}"))?
            .take()
            .ok_or_else(|| "CTAS admitted execution context was already consumed".to_string())?;
        execute(self.source_artifact(), execution)
    }
}

#[allow(
    dead_code,
    reason = "Retained for staged query-execution DML recovery and connector wiring."
)]
pub(crate) fn mutation_failure(failure: ConnectorMutationFailure) -> CtasFailure {
    let kind = match failure.kind() {
        novarocks_spi::connector::ConnectorMutationFailureKind::InvalidRequest => {
            CtasFailureKind::InvalidRequest
        }
        novarocks_spi::connector::ConnectorMutationFailureKind::NotFound => {
            CtasFailureKind::NotFound
        }
        novarocks_spi::connector::ConnectorMutationFailureKind::AlreadyExists => {
            CtasFailureKind::AlreadyExists
        }
        novarocks_spi::connector::ConnectorMutationFailureKind::Conflict => {
            CtasFailureKind::Conflict
        }
        novarocks_spi::connector::ConnectorMutationFailureKind::Unsupported => {
            CtasFailureKind::Unsupported
        }
        novarocks_spi::connector::ConnectorMutationFailureKind::Cancelled => {
            CtasFailureKind::Cancelled
        }
        novarocks_spi::connector::ConnectorMutationFailureKind::DeadlineExceeded => {
            CtasFailureKind::DeadlineExceeded
        }
        novarocks_spi::connector::ConnectorMutationFailureKind::Unavailable => {
            CtasFailureKind::Unavailable
        }
        _ => CtasFailureKind::Internal,
    };
    CtasFailure {
        kind,
        message: failure.message().to_string(),
        user_error: None,
    }
}

fn connector_failure(error: novarocks_spi::connector::ConnectorError) -> CtasFailure {
    use novarocks_spi::connector::ConnectorErrorKind;
    let kind = match error.kind() {
        ConnectorErrorKind::InvalidRequest => CtasFailureKind::InvalidRequest,
        ConnectorErrorKind::NotFound => CtasFailureKind::NotFound,
        ConnectorErrorKind::Unsupported => CtasFailureKind::Unsupported,
        ConnectorErrorKind::Cancelled => CtasFailureKind::Cancelled,
        ConnectorErrorKind::DeadlineExceeded => CtasFailureKind::DeadlineExceeded,
        ConnectorErrorKind::Unavailable => CtasFailureKind::Unavailable,
        ConnectorErrorKind::PermissionDenied
        | ConnectorErrorKind::ResourceExhausted
        | ConnectorErrorKind::CorruptData
        | ConnectorErrorKind::Internal => CtasFailureKind::Internal,
    };
    CtasFailure {
        kind,
        message: error.to_string(),
        user_error: None,
    }
}

fn internal_failure(message: impl Into<String>) -> CtasFailure {
    CtasFailure {
        kind: CtasFailureKind::Internal,
        message: message.into(),
        user_error: None,
    }
}

struct CorePreparedCtasWrite {
    state: DmlExecutionKernel,
    gate: Arc<CtasSourceExecutionGate>,
    target: Arc<dyn CtasWriteTarget>,
    native_encoding: Mutex<Option<crate::query_execution::compiler::NativeFragmentEncodingInput>>,
    pending: Mutex<Option<PendingCtasDistributedWrite>>,
    prepared: Mutex<Option<BoundCtasDistributedWrite>>,
    terminal_context: novarocks_spi::connector::ConnectorRequestContext,
    attempt_reservation: Mutex<Option<crate::query_execution::completion::QueryAttemptReservation>>,
    execution_identity: [u8; 32],
}

/// Core-retained write facts that are not part of the Frontend-owned native
/// encoding step. They are consumed exactly once when Frontend returns the
/// native bundle for the sealed plan/preparation pair.
struct PendingCtasDistributedWrite {
    query_options: Option<QueryOptions>,
    session: Arc<crate::query_execution::write_session::ConnectorWriteSession>,
}

/// The same facts once the native bundle has been bound to them. It is the
/// exact input of the one distributed round this CTAS runs.
struct BoundCtasDistributedWrite {
    encoding: crate::query_execution::post_compile::NativeFragmentEncodingInput,
    native_bundle: crate::query_execution::native_fragment::NativeFragmentAttachment,
    query_options: Option<QueryOptions>,
    session: Arc<crate::query_execution::write_session::ConnectorWriteSession>,
}

impl CtasPreparedWrite for CorePreparedCtasWrite {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn execution_identity(&self) -> [u8; 32] {
        self.execution_identity
    }

    fn native_encoding(&self) -> Result<CtasNativeEncoding<'_>, CtasFailure> {
        let encoding = self
            .native_encoding
            .lock()
            .map_err(|error| internal_failure(format!("CTAS native encoding lock: {error}")))?;
        if encoding.is_none() {
            return Err(internal_failure(
                "CTAS native encoding input was already consumed",
            ));
        }
        Ok(CtasNativeEncoding { encoding })
    }
}

fn downcast_standard_source(
    source: &dyn CtasPreparedSource,
) -> Result<&CorePreparedStandardCtasSource, CtasFailure> {
    source
        .as_any()
        .downcast_ref::<CorePreparedStandardCtasSource>()
        .ok_or_else(|| {
            internal_failure("CTAS source handle does not belong to the standard core engine")
        })
}

fn downcast_standard_preflight(
    preflight: &dyn CtasPreparedTargetPreflight,
) -> Result<&CoreStandardCtasTargetPreflight, CtasFailure> {
    preflight
        .as_any()
        .downcast_ref::<CoreStandardCtasTargetPreflight>()
        .ok_or_else(|| {
            internal_failure("CTAS target preflight does not belong to the standard core engine")
        })
}

fn sweep_unanchored_ctas_roots(
    kernel: &DmlExecutionKernel,
    lease: &ConnectorUnanchoredCtasCleanupLease,
    context: novarocks_spi::connector::ConnectorRequestContext,
) -> Result<(), CtasFailure> {
    let policy = kernel.lake_publication_runtime_policy().ok_or_else(|| CtasFailure {
        kind: CtasFailureKind::Unsupported,
        message: "standard CTAS is unsupported until the shared lake publication GC policy is installed"
            .to_string(),
        user_error: None,
    })?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| internal_failure("system clock is before Unix epoch"))?;
    let now_ms = i64::try_from(now.as_millis())
        .map_err(|_| internal_failure("system clock exceeds i64 milliseconds"))?;
    let safe_age_ms = i64::try_from(policy.safe_gc_age().as_millis())
        .map_err(|_| internal_failure("lake publication safe GC age exceeds i64 milliseconds"))?;
    let cutoff_ms = now_ms
        .checked_sub(safe_age_ms)
        .ok_or_else(|| internal_failure("lake publication safe GC cutoff underflows"))?;
    let warehouse_root = lease.warehouse_root().map_err(connector_failure)?;
    let candidates = lease
        .discover_unanchored_ctas(warehouse_root.clone(), cutoff_ms, context.clone())
        .map_err(connector_failure)?;
    for provenance in candidates {
        // An exact delete whose acknowledgement is uncertain stays an aged
        // residue for a later GC pass. It never grants this new CTAS attempt
        // authority to reconcile, retry, or infer the old publication.
        let _ = lease
            .inspect_then_delete_unanchored_ctas(
                warehouse_root.clone(),
                cutoff_ms,
                provenance,
                context.clone(),
            )
            .map_err(connector_failure)?;
    }
    Ok(())
}

fn downcast_catalog_action(
    action: &dyn CtasPreparedCatalogAction,
) -> Result<&CoreCtasCatalogAction, CtasFailure> {
    action
        .as_any()
        .downcast_ref::<CoreCtasCatalogAction>()
        .ok_or_else(|| internal_failure("CTAS catalog action does not belong to the core engine"))
}

fn prepared_catalog_action(
    input_digest: [u8; 32],
    kind: CoreCtasCatalogActionKind,
) -> PreparedCtasCatalogAction {
    PreparedCtasCatalogAction {
        input_digest,
        handle: Arc::new(CoreCtasCatalogAction {
            kind,
            dispatched: AtomicBool::new(false),
        }),
    }
}

fn downcast_standard_target(
    target: &dyn CtasPreparedTarget,
) -> Result<&CoreStandardCtasTargetSession, CtasFailure> {
    target
        .as_any()
        .downcast_ref::<CoreStandardCtasTargetSession>()
        .ok_or_else(|| {
            internal_failure("CTAS target handle does not belong to the standard core engine")
        })
}

fn downcast_write(write: &dyn CtasPreparedWrite) -> Result<&CorePreparedCtasWrite, CtasFailure> {
    write
        .as_any()
        .downcast_ref::<CorePreparedCtasWrite>()
        .ok_or_else(|| internal_failure("CTAS write handle does not belong to the core engine"))
}

fn sha256(parts: &[&[u8]]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    for part in parts {
        digest.update((part.len() as u64).to_be_bytes());
        digest.update(part);
    }
    digest.finalize().into()
}

fn standard_stage_input_digest(request: &ConnectorStagedCreatePrepareRequest) -> [u8; 32] {
    let policy = match request.policy {
        CreatePolicy::FailIfExists => b"fail".as_slice(),
        CreatePolicy::NoOpIfExists => b"noop".as_slice(),
    };
    let columns = request
        .columns
        .iter()
        .map(|column| format!("{column:?}"))
        .collect::<Vec<_>>()
        .join("\n");
    let partitioning = request
        .partitioning
        .iter()
        .map(|transform| format!("{transform:?}"))
        .collect::<Vec<_>>()
        .join("\n");
    let properties = request
        .properties
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("\n");
    sha256(&[
        b"novarocks.standard-ctas-stage.v1",
        &request.publication_id.to_bytes(),
        request.table.instance_id.as_str().as_bytes(),
        request.table.namespace.as_bytes(),
        request.table.table.as_bytes(),
        policy,
        columns.as_bytes(),
        partitioning.as_bytes(),
        properties.as_bytes(),
    ])
}

fn standard_publish_input_digest(request: &ConnectorStagedCreatePublishRequest) -> [u8; 32] {
    sha256(&[
        b"novarocks.standard-ctas-publish.v1",
        &request.operation_id.to_bytes(),
        &request.handle.digest(),
        &request.write.digest(),
        &request.write.row_count().to_be_bytes(),
    ])
}

/// Turn one completed write session into the proof its publication needs.
///
/// This is the whole terminal of a staged CTAS write: seal exactly once, take
/// the rows only if the seal is known to have succeeded, and bind the resulting
/// receipt to the staged target. Every failure below is proven-uncommitted --
/// a staged seal touches no catalog, so the target still does not exist.
fn seal_ctas_write(
    target: &dyn CtasWriteTarget,
    execution_identity: [u8; 32],
    completion: Option<crate::query_execution::outcome::ConnectorWriteSessionCompletion>,
    context: novarocks_spi::connector::ConnectorRequestContext,
) -> StandardCtasWriteOutcome {
    // An empty prepared write set arrives here exactly like a full one. Whether
    // an empty CTAS publishes an empty table is the provider's decision, and it
    // cannot make it if the frontend short-circuits on "there were no
    // fragments".
    let Some(completion) = completion else {
        return staged_write_failed(target, "CTAS write produced no sealed write set");
    };
    let committed = match crate::query_execution::write_session::finish_write_session_for_following_terminal_action(
        completion,
        context,
    ) {
            Ok(committed) => committed,
            Err(error) => {
                return staged_write_failed(
                    target,
                    format!("CTAS write set could not be sealed: {error}"),
                );
            }
    };
    let (outcome, affected_rows, terminal_context) = committed.into_parts();
    // Rows become reportable exactly here, and only on an outcome known to
    // have succeeded.
    let Some(row_count) = affected_rows else {
        let message = match outcome {
            ExternalMutationOutcome::KnownUncommitted { failure } => {
                format!("CTAS write was not sealed: {}", failure.message())
            }
            ExternalMutationOutcome::CommitUnknown { failure, .. } => format!(
                "CTAS write sealing outcome is unresolved: {}",
                failure.message()
            ),
            ExternalMutationOutcome::KnownCommitted { .. } => {
                "CTAS write sealed without reporting its rows".to_string()
            }
        };
        return staged_write_failed(target, message);
    };
    let ExternalMutationOutcome::KnownCommitted { receipt, .. } = outcome else {
        return staged_write_failed(target, "CTAS write reported rows without a receipt");
    };
    let proof = match ConnectorStagedWriteProof::try_new(receipt, row_count) {
        Ok(proof) => proof,
        Err(error) => {
            return staged_write_failed(
                target,
                format!("CTAS write receipt is not publishable: {error}"),
            );
        }
    };
    if let Err(error) = target.bind_write(proof.clone(), terminal_context) {
        return staged_write_failed(
            target,
            format!("CTAS target refused its sealed write: {error}"),
        );
    }
    StandardCtasWriteOutcome::Completed {
        write: proof,
        execution_identity,
    }
}

/// Close a staged target whose write never produced a publishable receipt.
///
/// The latch matters even though the publication would refuse anyway: a target
/// whose write is unresolved must not be aborted either, because aborting
/// deletes objects and this process no longer knows what the write left behind.
fn staged_write_failed(
    target: &dyn CtasWriteTarget,
    message: impl Into<String>,
) -> StandardCtasWriteOutcome {
    let mut message = message.into();
    if let Err(error) = target.mark_write_unknown() {
        message.push_str(&format!(
            "; standard CTAS target rejected write-unknown transition: {error}"
        ));
    }
    StandardCtasWriteOutcome::KnownUncommitted {
        failure: CtasFailure {
            kind: CtasFailureKind::Unavailable,
            message,
            user_error: None,
        },
    }
}

fn standard_ctas_unsupported() -> CtasFailure {
    CtasFailure {
        kind: CtasFailureKind::Unsupported,
        message: "connector does not support crash-only standard CTAS staged-create".to_string(),
        user_error: None,
    }
}

impl CtasEngine for DmlExecutionKernel {
    fn preflight_standard_ctas_target(
        &self,
        _statement: &CreateTableAsSelect,
        _source: &str,
        command: &CtasCommand,
        current_catalog: Option<&str>,
        current_database: &str,
    ) -> Result<CtasTargetPreflightOutcome, CtasFailure> {
        let target = crate::catalog_application::resolver::resolve_table_target(
            self,
            &ObjectName {
                parts: command.target_parts.clone(),
            },
            current_catalog,
            current_database,
        )
        .map_err(internal_failure)?;
        // CTAS reaches the target catalog before its distributed writer is
        // built. Reserve the one native attempt here so target preflight,
        // source planning, staged creation, and the eventual writer share one
        // request-local vended credential authority.
        let attempt_reservation = self
            .query_execution()
            .reserve_initial_attempt()
            .map_err(|error| internal_failure(error.to_string()))?;
        let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse(&target.catalog)
            .map_err(connector_failure)?;
        let planning = self
            .connector_control()
            .acquire_current(&instance_id)
            .map_err(connector_failure)?;
        // The standard staged-create capability is the only mutation
        // authority.  Acquiring it before source planning preserves the
        // unsupported-at-zero-side-effect boundary.
        let lease = planning
            .derive_staged_create_lease()
            .map_err(connector_failure)?;
        let unanchored_ctas_cleanup_lease = planning
            .derive_unanchored_ctas_cleanup_lease()
            .map_err(connector_failure)?;
        let write_lease = planning.derive_write_lease().map_err(connector_failure)?;
        if !lease.matches_write_lease(&write_lease)
            || lease.control_runtime_id() != unanchored_ctas_cleanup_lease.control_runtime_id()
        {
            return Err(internal_failure(
                "standard CTAS staged-create, cleanup, and writer leases do not share one exact generation",
            ));
        }
        // The provider's staged-create publication contract is the only
        // authority allowed to decide whether IF NOT EXISTS is a no-op. A
        // frontend metadata read is not a substitute for its exact commit
        // result and would race the publication frontier.
        let binding = planning.binding();
        let target_catalog_properties = binding
            .catalog_properties()
            .map_err(connector_failure)?
            .clone();
        Ok(CtasTargetPreflightOutcome::Ready(
            PreparedCtasTargetPreflight {
                facts: CtasTargetPreflightFacts {
                    provider_id: binding.descriptor().provider_id.as_str().to_string(),
                    instance_id: binding.descriptor().instance_id.as_str().to_string(),
                    control_runtime_id: lease.control_runtime_id().to_bytes(),
                    capability_version:
                        novarocks_spi::connector::CONNECTOR_STAGED_CREATE_CONTRACT_VERSION,
                    target_namespace: target.namespace.clone(),
                    target_table: target.table.clone(),
                },
                handle: Arc::new(CoreStandardCtasTargetPreflight {
                    target,
                    lease,
                    planning_lease: planning.clone(),
                    unanchored_ctas_cleanup_lease,
                    write_lease,
                    target_catalog_properties,
                    attempt_reservation: Arc::new(Mutex::new(Some(attempt_reservation))),
                }),
            },
        ))
    }

    fn prepare_standard_ctas_source(
        &self,
        preflight: &dyn CtasPreparedTargetPreflight,
        request: PrepareCtasSourceRequest,
    ) -> Result<PreparedCtasSource, CtasFailure> {
        let preflight = downcast_standard_preflight(preflight)?;
        let target = crate::catalog_application::resolver::resolve_table_target(
            self,
            &ObjectName {
                parts: request.command.target_parts.clone(),
            },
            request.current_catalog.as_deref(),
            &request.current_database,
        )
        .map_err(internal_failure)?;
        if target != preflight.target {
            return Err(CtasFailure {
                kind: CtasFailureKind::InvalidRequest,
                message: "standard CTAS source target does not match its exact preflight"
                    .to_string(),
                user_error: None,
            });
        }
        let connector_context = crate::connector::connector_request_context_for_execution(
            request.query_options.as_ref(),
            &request.execution,
        )
        .map_err(internal_failure)?;
        let connector_context = {
            let reservation = preflight.attempt_reservation.lock().map_err(|error| {
                internal_failure(format!("CTAS attempt reservation lock: {error}"))
            })?;
            let reservation = reservation.as_ref().ok_or_else(|| {
                internal_failure("standard CTAS attempt reservation was already consumed")
            })?;
            reservation
                .connector_request_context(connector_context)
                .with_vended_credential_lease_collection(
                    preflight.target_catalog_properties.clone(),
                )
                .map_err(|error| internal_failure(error.to_string()))?
        };
        let planned = plan_query_for_ctas_source(
            self,
            request.current_catalog.as_deref(),
            &request.current_database,
            &request.command.source,
            &request.execution,
            &connector_context,
        )?;
        let source_columns = planned.source.output_columns();
        if source_columns.is_empty() {
            return Err(CtasFailure {
                kind: CtasFailureKind::InvalidRequest,
                message: "CTAS source has no output columns".to_string(),
                user_error: None,
            });
        }
        let output_schema = Arc::new(arrow::datatypes::Schema::new(
            source_columns
                .iter()
                .map(|column| {
                    arrow::datatypes::Field::new(
                        &column.name,
                        column.data_type.clone(),
                        column.nullable,
                    )
                })
                .collect::<Vec<_>>(),
        ));
        let table_columns =
            crate::query_execution::dml::iceberg_ctas::arrow_schema_to_table_column_defs(
                output_schema.as_ref(),
            )
            .map_err(internal_failure)?;
        let output_columns = table_columns
            .iter()
            .map(crate::catalog_application::statement::connector_column)
            .collect::<Result<Vec<_>, _>>()
            .map_err(internal_failure)?;
        let schema_text = format!("{output_schema:?}");
        let optimized_fingerprint = planned.source.capture_fingerprint();
        let settings_material =
            novarocks_sql::planning::dml::optimizer_settings_stable_digest_material(
                &planned.optimizer_settings,
            );
        let binding_material = planned.table_bindings.stable_digest_material();
        let execution_nonce = uuid::Uuid::now_v7();
        let execution_identity =
            sha256(&[b"novarocks.ctas-execution.v1", execution_nonce.as_bytes()]);
        let plan_digest = sha256(&[
            b"novarocks.ctas-plan.v1",
            printer::print_query(&request.command.source).as_bytes(),
            optimized_fingerprint.as_slice(),
            settings_material.as_slice(),
            binding_material.as_slice(),
        ]);
        let schema_digest = sha256(&[schema_text.as_bytes()]);
        let gate = Arc::new(CtasSourceExecutionGate::new(
            execution_identity,
            Arc::new(planned),
            request.execution,
        ));
        let attempt_reservation = preflight
            .attempt_reservation
            .lock()
            .map_err(|error| internal_failure(format!("CTAS attempt reservation lock: {error}")))?
            .take()
            .ok_or_else(|| {
                internal_failure("standard CTAS attempt reservation was already consumed")
            })?;
        Ok(PreparedCtasSource {
            facts: CtasPreparedSourceFacts {
                target_catalog: target.catalog.clone(),
                target_namespace: target.namespace.clone(),
                target_table: target.table.clone(),
                source_catalog: request.current_catalog.clone(),
                source_database: request.current_database.clone(),
                plan_digest,
                schema_digest,
                execution_identity,
                output_columns: output_columns.clone(),
            },
            handle: Arc::new(CorePreparedStandardCtasSource {
                gate,
                preflight: preflight.clone(),
                command: request.command,
                target,
                query_options: request.query_options,
                connector_context,
                attempt_reservation: Mutex::new(Some(attempt_reservation)),
                output_schema,
                output_columns,
                target_session: Arc::new(Mutex::new(None)),
                target_prepare_started: AtomicBool::new(false),
            }),
        })
    }

    fn prepare_standard_ctas_target(
        &self,
        source: &dyn CtasPreparedSource,
        publication_id: LakePublicationId,
        policy: CreatePolicy,
    ) -> Result<PreparedStandardCtasCatalogAction, CtasFailure> {
        let source = downcast_standard_source(source)?;
        if source
            .target_prepare_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(CtasFailure {
                kind: CtasFailureKind::InvalidRequest,
                message: "standard CTAS target preparation has already been attempted".to_string(),
                user_error: None,
            });
        }
        let request = source.preflight.lease.prepare_request(
            publication_id,
            novarocks_spi::connector::ConnectorMutationOperationId::from_bytes(
                publication_id.to_bytes(),
            ),
            novarocks_spi::connector::ConnectorTableIdentity {
                instance_id: source.preflight.lease.instance_id().clone(),
                namespace: Arc::from(source.target.namespace.as_str()),
                table: Arc::from(source.target.table.as_str()),
            },
            source.output_columns.clone(),
            source.command.partitioning.clone(),
            source.command.properties.clone(),
            policy,
            source.connector_context.clone(),
        );
        Ok(PreparedStandardCtasCatalogAction {
            input_digest: standard_stage_input_digest(&request),
            handle: Arc::new(CoreCtasCatalogAction {
                kind: CoreCtasCatalogActionKind::StandardStage {
                    preflight: source.preflight.clone(),
                    request,
                    target_slot: Arc::clone(&source.target_session),
                },
                dispatched: AtomicBool::new(false),
            }),
        })
    }

    fn stage_standard_ctas_target(
        &self,
        action: &dyn CtasPreparedCatalogAction,
    ) -> Result<StandardCtasStageOutcome, CtasFailure> {
        let action = downcast_catalog_action(action)?;
        let CoreCtasCatalogActionKind::StandardStage {
            preflight,
            request,
            target_slot,
        } = &action.kind
        else {
            return Err(internal_failure(
                "CTAS catalog action is not a standard stage action",
            ));
        };
        action.begin_dispatch()?;
        match preflight
            .lease
            .prepare(request.clone())
            .map_err(connector_failure)?
        {
            ConnectorStagedCreatePrepareOutcome::Prepared {
                handle, receipt, ..
            } => {
                let target = Arc::new(CoreStandardCtasTargetSession {
                    lease: preflight.lease.clone(),
                    write_lease: preflight.write_lease.clone(),
                    planning_lease: preflight.planning_lease.clone(),
                    target: preflight.target.clone(),
                    handle,
                    publication_id: request.publication_id,
                    context: Mutex::new(request.context.clone()),
                    write_plan_started: AtomicBool::new(false),
                    write_unknown_latched: AtomicBool::new(false),
                    publish_started: AtomicBool::new(false),
                });
                // A vended catalog cannot clean its warehouse before staged
                // creation: the staged response is the first place it can
                // disclose this request's storage authority. Run the same
                // maintenance step only after that response has populated the
                // request-local lease collector. This ordering is universal,
                // so static and vended catalogs retain one CTAS lifecycle.
                sweep_unanchored_ctas_roots(
                    self,
                    &preflight.unanchored_ctas_cleanup_lease,
                    request.context.clone(),
                )?;
                *target_slot
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::clone(&target));
                Ok(StandardCtasStageOutcome::Prepared {
                    target: PreparedStandardCtasTarget {
                        facts: target.facts(),
                        handle: target,
                    },
                    receipt,
                })
            }
            ConnectorStagedCreatePrepareOutcome::Conflict { failure }
            | ConnectorStagedCreatePrepareOutcome::KnownUncommitted { failure } => {
                Ok(StandardCtasStageOutcome::KnownUncommitted {
                    failure: mutation_failure(failure),
                })
            }
            ConnectorStagedCreatePrepareOutcome::CommitUnknown { failure, evidence } => {
                Ok(StandardCtasStageOutcome::CommitUnknown {
                    failure: mutation_failure(failure),
                    evidence,
                })
            }
        }
    }

    fn prepare_standard_ctas_write(
        &self,
        source: &dyn CtasPreparedSource,
        target: &dyn CtasPreparedTarget,
    ) -> Result<PreparedStandardCtasWrite, CtasFailure> {
        let source = downcast_standard_source(source)?;
        let target = downcast_standard_target(target)?;
        if source.gate.execution_identity() != source.execution_identity() {
            return Err(internal_failure(
                "standard CTAS source execution identity drift",
            ));
        }
        let target_arc = source
            .target_session
            .lock()
            .map_err(|error| {
                internal_failure(format!("standard CTAS target session lock: {error}"))
            })?
            .clone()
            .ok_or_else(|| {
                internal_failure("standard CTAS source has no retained target session")
            })?;
        if !std::ptr::eq(target_arc.as_ref(), target) {
            return Err(internal_failure(
                "standard CTAS target does not match the source-retained exact session",
            ));
        }
        // The session is opened before the plan is compiled, because it owns
        // the writer recipes that plan's writer node carries.
        let session = target.begin_write_session(
            self.typed_connector_control(),
            &source.output_schema,
            source.connector_context.clone(),
        )?;
        let planned = source
            .gate
            .source_artifact()
            .downcast_ref::<PlannedCtasSourceQuery>()
            .ok_or_else(|| {
                internal_failure("standard CTAS retained source artifact type mismatch")
            })?;
        let (native_encoding, pending) = prepare_planned_ctas_connector_write(
            self,
            planned,
            Arc::clone(&source.output_schema),
            source.query_options.clone(),
            &source.connector_context,
            session,
        )
        .map_err(internal_failure)?;
        let attempt_reservation = source
            .attempt_reservation
            .lock()
            .map_err(|error| internal_failure(format!("CTAS attempt reservation lock: {error}")))?
            .take()
            .ok_or_else(|| {
                internal_failure("standard CTAS attempt reservation was already consumed")
            })?;
        let identity = source.gate.execution_identity();
        let target_facts = target_arc.facts();
        let target_for_write: Arc<dyn CtasWriteTarget> = target_arc;
        Ok(PreparedStandardCtasWrite {
            target_facts,
            execution_identity: identity,
            handle: Arc::new(CorePreparedCtasWrite {
                state: self.clone(),
                gate: Arc::clone(&source.gate),
                target: target_for_write,
                native_encoding: Mutex::new(Some(native_encoding)),
                pending: Mutex::new(Some(pending)),
                prepared: Mutex::new(None),
                terminal_context: source.connector_context.clone(),
                attempt_reservation: Mutex::new(Some(attempt_reservation)),
                execution_identity: identity,
            }),
        })
    }

    fn bind_standard_ctas_write_native_bundle(
        &self,
        prepared: &dyn CtasPreparedWrite,
        native_bundle: crate::query_execution::native_fragment::NativeFragmentAttachment,
    ) -> Result<(), CtasFailure> {
        let prepared = downcast_write(prepared)?;
        let pending = prepared
            .pending
            .lock()
            .map_err(|error| internal_failure(format!("CTAS pending write lock: {error}")))?
            .take()
            .ok_or_else(|| internal_failure("CTAS native bundle was already bound"))?;
        let encoding = prepared
            .native_encoding
            .lock()
            .map_err(|error| internal_failure(format!("CTAS native encoding lock: {error}")))?
            .take()
            .ok_or_else(|| internal_failure("CTAS native encoding input was already consumed"))?;
        *prepared
            .prepared
            .lock()
            .map_err(|error| internal_failure(format!("CTAS prepared write lock: {error}")))? =
            Some(BoundCtasDistributedWrite {
                encoding,
                native_bundle,
                query_options: pending.query_options,
                session: pending.session,
            });
        Ok(())
    }

    fn execute_standard_ctas_write(
        &self,
        prepared: &dyn CtasPreparedWrite,
    ) -> StandardCtasWriteOutcome {
        let prepared = match downcast_write(prepared) {
            Ok(prepared) => prepared,
            Err(failure) => return StandardCtasWriteOutcome::KnownUncommitted { failure },
        };
        let result = prepared
            .gate
            .execute_once(prepared.execution_identity, |_, execution| {
                let bound = prepared
                    .prepared
                    .lock()
                    .map_err(|error| format!("CTAS prepared write lock: {error}"))?
                    .take()
                    .ok_or_else(|| "CTAS prepared write was already consumed".to_string())?;
                let BoundCtasDistributedWrite {
                    encoding,
                    native_bundle,
                    query_options,
                    session,
                } = bound;
                let request =
                    crate::query_execution::contract::build_distributed_query_request_with_execution(
                        encoding,
                        native_bundle,
                        query_options,
                        crate::query_execution::contract::DistributedQueryIntent::Write,
                        &execution,
                    )
                    .map_err(|error| error.to_string())?;
                let request = crate::query_execution::contract::with_connector_write_session(
                    request,
                    Arc::clone(&session),
                )
                .map_err(|error| error.to_string())?;
                let attempt_reservation = prepared
                    .attempt_reservation
                    .lock()
                    .map_err(|error| format!("CTAS attempt reservation lock: {error}"))?
                    .take()
                    .ok_or_else(|| "CTAS attempt reservation was already consumed".to_string())?;
                // Nothing below can leave the publication in doubt. A staged
                // write session seals its artifacts without touching the
                // catalog, so every failure here is proven-uncommitted: the
                // target still does not exist, and the objects the backends
                // staged are collected by the unanchored-CTAS sweep.
                let outcome = match prepared
                    .state
                    .query_execution()
                    .execute_reserved(request, attempt_reservation)
                {
                    Ok(outcome) => outcome,
                    Err(error) => {
                        return Ok(staged_write_failed(
                            prepared.target.as_ref(),
                            format!("CTAS write did not complete: {error}"),
                        ));
                    }
                };
                let write = match outcome.into_write() {
                    Ok(write) => write,
                    Err(error) => {
                        return Ok(staged_write_failed(
                            prepared.target.as_ref(),
                            format!("CTAS write reached no terminal: {error}"),
                        ));
                    }
                };
                // The data plane closed and every participant succeeded, so
                // the session is asked to seal.
                Ok(seal_ctas_write(
                    prepared.target.as_ref(),
                    prepared.execution_identity,
                    write.into_write_session(),
                    prepared.terminal_context.clone(),
                ))
            });
        match result {
            Ok(outcome) => outcome,
            Err(message) => StandardCtasWriteOutcome::KnownUncommitted {
                failure: internal_failure(message),
            },
        }
    }

    fn prepare_standard_publish_ctas(
        &self,
        target: &dyn CtasPreparedTarget,
        publication_id: LakePublicationId,
        write: ConnectorStagedWriteProof,
    ) -> Result<PreparedStandardCtasCatalogAction, CtasFailure> {
        let target = downcast_standard_target(target)?;
        let request = target.prepare_publish(publication_id, write)?;
        Ok(PreparedStandardCtasCatalogAction {
            input_digest: standard_publish_input_digest(&request),
            handle: Arc::new(CoreCtasCatalogAction {
                kind: CoreCtasCatalogActionKind::StandardPublish {
                    target: Arc::new(CoreStandardCtasTargetSession {
                        lease: target.lease.clone(),
                        write_lease: target.write_lease.clone(),
                        planning_lease: target.planning_lease.clone(),
                        target: target.target.clone(),
                        handle: target.handle.clone(),
                        publication_id: target.publication_id,
                        context: Mutex::new(
                            target
                                .context
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .clone(),
                        ),
                        write_plan_started: AtomicBool::new(true),
                        write_unknown_latched: AtomicBool::new(
                            target.write_unknown_latched.load(Ordering::Acquire),
                        ),
                        publish_started: AtomicBool::new(true),
                    }),
                    request,
                },
                dispatched: AtomicBool::new(false),
            }),
        })
    }

    fn publish_standard_ctas(
        &self,
        action: &dyn CtasPreparedCatalogAction,
    ) -> Result<StandardCtasPublishOutcome, CtasFailure> {
        let action = downcast_catalog_action(action)?;
        let CoreCtasCatalogActionKind::StandardPublish { target, request } = &action.kind else {
            return Err(internal_failure(
                "CTAS catalog action is not a standard publish action",
            ));
        };
        action.begin_dispatch()?;
        match target
            .lease
            .publish(request.clone())
            .map_err(connector_failure)?
        {
            ConnectorStagedCreatePublishOutcome::Applied {
                receipt,
                finalization,
            } => Ok(StandardCtasPublishOutcome::Applied {
                receipt,
                finalization,
            }),
            ConnectorStagedCreatePublishOutcome::NoOp {
                receipt,
                finalization,
            } => Ok(StandardCtasPublishOutcome::NoOp {
                receipt,
                finalization,
            }),
            ConnectorStagedCreatePublishOutcome::Conflict { failure }
            | ConnectorStagedCreatePublishOutcome::KnownUncommitted { failure } => {
                Ok(StandardCtasPublishOutcome::KnownUncommitted {
                    failure: mutation_failure(failure),
                })
            }
            ConnectorStagedCreatePublishOutcome::CommitUnknown { failure, evidence } => {
                Ok(StandardCtasPublishOutcome::CommitUnknown {
                    failure: mutation_failure(failure),
                    evidence,
                })
            }
        }
    }

    fn adjudicate_standard_ctas_publication(
        &self,
        target: &dyn CtasPreparedTarget,
        evidence: ExternalMutationEvidence,
    ) -> Result<StandardCtasPublicationAdjudicationOutcome, CtasFailure> {
        let target = downcast_standard_target(target)?;
        let outcome = target
            .lease
            .adjudicate_publication(ConnectorStagedCreatePublicationAdjudicationRequest {
                target_operation_id:
                    novarocks_spi::connector::ConnectorStagedCreateOperationId::from_bytes(
                        target.publication_id.to_bytes(),
                    ),
                evidence,
                context: target
                    .context
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone(),
            })
            .map_err(connector_failure)?;
        match outcome {
            ConnectorStagedCreatePublicationAdjudicationOutcome::Published {
                receipt,
                finalization,
            } => Ok(StandardCtasPublicationAdjudicationOutcome::Published {
                receipt,
                finalization,
            }),
            ConnectorStagedCreatePublicationAdjudicationOutcome::CommitUnknown {
                failure, ..
            } => Ok(StandardCtasPublicationAdjudicationOutcome::CommitUnknown {
                failure: mutation_failure(failure),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use novarocks_spi::connector::ConnectorError;

    use super::*;
    use crate::query_execution::write_session::tests as write_session_fixture;

    /// A staged target that only records what the write terminal asked of it.
    #[derive(Default)]
    struct RecordingTarget {
        bound: Mutex<Vec<ConnectorStagedWriteProof>>,
        unknown: AtomicUsize,
    }

    impl CtasWriteTarget for RecordingTarget {
        fn begin_write_session(
            &self,
            _host: &crate::connector::control_host::ConnectorControlHost,
            _output_schema: &arrow::datatypes::SchemaRef,
            _context: novarocks_spi::connector::ConnectorRequestContext,
        ) -> Result<Arc<crate::query_execution::write_session::ConnectorWriteSession>, CtasFailure>
        {
            unreachable!("these tests drive the write terminal, not admission")
        }

        fn bind_write(
            &self,
            write: ConnectorStagedWriteProof,
            _terminal_context: novarocks_spi::connector::ConnectorRequestContext,
        ) -> Result<(), ConnectorError> {
            self.bound
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(write);
            Ok(())
        }

        fn mark_write_unknown(&self) -> Result<(), ConnectorError> {
            self.unknown.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    impl RecordingTarget {
        fn bound(&self) -> Vec<ConnectorStagedWriteProof> {
            self.bound
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }
    }

    fn completion(
        session: &Arc<crate::query_execution::write_session::ConnectorWriteSession>,
        row_count: u64,
    ) -> crate::query_execution::outcome::ConnectorWriteSessionCompletion {
        crate::query_execution::outcome::ConnectorWriteSessionCompletion::for_test(
            Arc::clone(session),
            crate::query_execution::write_result::DecodedPreparedWriteSet::for_test(
                row_count,
                Vec::new(),
            ),
        )
    }

    /// The whole CTAS write terminal on a successful seal: the session is asked
    /// exactly once, the rows it made durable travel with the receipt, and the
    /// staged target binds that receipt so nothing else can publish under it.
    #[test]
    fn a_sealed_ctas_write_binds_one_receipt_and_carries_its_rows() {
        let fixture = write_session_fixture::fixture_with_outcome(
            1,
            16,
            write_session_fixture::known_committed(),
        );
        let target = RecordingTarget::default();
        assert_eq!(fixture.session.finish_invocations(), 0);

        let outcome = seal_ctas_write(
            &target,
            [9; 32],
            Some(completion(&fixture.session, 11)),
            write_session_fixture::request_context(),
        );

        let StandardCtasWriteOutcome::Completed {
            write,
            execution_identity,
        } = outcome
        else {
            panic!("a known-committed seal must complete");
        };
        assert_eq!(execution_identity, [9; 32]);
        assert_eq!(write.row_count(), 11);
        assert_eq!(fixture.session.finish_invocations(), 1);
        assert_eq!(fixture.recorded.lock().expect("recorded").finish, 1);
        assert_eq!(target.bound(), vec![write]);
        assert_eq!(target.unknown.load(Ordering::SeqCst), 0);
    }

    /// An unresolved seal reports no rows and binds nothing, so the publication
    /// has nothing to publish -- and the target is latched closed, because a
    /// process that does not know what its write left behind must not abort it
    /// either.
    #[test]
    fn an_unresolved_ctas_write_reports_no_rows_and_leaves_nothing_to_publish() {
        let fixture = write_session_fixture::fixture_with_outcome(
            1,
            16,
            write_session_fixture::commit_unknown(),
        );
        let target = RecordingTarget::default();

        let outcome = seal_ctas_write(
            &target,
            [9; 32],
            Some(completion(&fixture.session, 11)),
            write_session_fixture::request_context(),
        );

        assert!(matches!(
            outcome,
            StandardCtasWriteOutcome::KnownUncommitted { .. }
        ));
        assert_eq!(fixture.session.finish_invocations(), 1);
        assert!(target.bound().is_empty());
        assert_eq!(target.unknown.load(Ordering::SeqCst), 1);
    }

    /// A write that staged no artifact still reaches the provider's seal.
    ///
    /// Whether an empty CTAS publishes an empty table is the provider's call;
    /// the frontend must not make it by short-circuiting on "there were no
    /// fragments".
    #[test]
    fn a_ctas_write_that_staged_nothing_still_reaches_the_provider() {
        let fixture = write_session_fixture::fixture_with_outcome(
            1,
            16,
            write_session_fixture::known_committed(),
        );
        let target = RecordingTarget::default();

        let outcome = seal_ctas_write(
            &target,
            [9; 32],
            Some(completion(&fixture.session, 0)),
            write_session_fixture::request_context(),
        );

        let StandardCtasWriteOutcome::Completed { write, .. } = outcome else {
            panic!("an empty staged write is still the provider's decision");
        };
        assert_eq!(write.row_count(), 0);
        assert_eq!(fixture.session.finish_invocations(), 1);
        assert_eq!(target.bound().len(), 1);
    }

    /// A write whose data plane never closed has no session to seal, so the
    /// connector is never asked at all.
    #[test]
    fn a_ctas_write_without_a_closed_data_plane_never_reaches_the_connector() {
        let fixture = write_session_fixture::fixture_with_outcome(
            1,
            16,
            write_session_fixture::known_committed(),
        );
        let target = RecordingTarget::default();

        let outcome = seal_ctas_write(
            &target,
            [9; 32],
            None,
            write_session_fixture::request_context(),
        );

        assert!(matches!(
            outcome,
            StandardCtasWriteOutcome::KnownUncommitted { .. }
        ));
        assert_eq!(fixture.session.finish_invocations(), 0);
        assert_eq!(fixture.recorded.lock().expect("recorded").finish, 0);
        assert!(target.bound().is_empty());
    }
}
