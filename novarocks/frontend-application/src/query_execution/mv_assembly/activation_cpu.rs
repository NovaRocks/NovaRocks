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

//! Closed pure turns of the original MV write activation.
//! Actual catalog, statistics and read observations stay in the async driver.

use super::refresh_preparation::io_gate::{contract_error, provider_error};
use crate::mv::domain::application::MvApplicationError;
use crate::query_execution::{
    mv_native_write::{PreparedMvNativeWriteAssembly, PreparedMvNativeWriteParts},
    package_freeze::CompiledPackageCarrier,
    provider_read_facts::FrozenProviderRead,
    write_session::ConnectorWriteSession,
};
use novarocks_query_application::preparation::{
    CompletedPhysicalPlanCandidate, FinalPlanRuntimeAccess,
};
use novarocks_spi::connector::{StatisticsRequiredAggregation, write_stack::WriteTargetOrdinal};
use novarocks_sql::{
    compiler::{
        ProviderReadFact, ProviderReadNeed, SessionOptimizerSettings, SqlCapturedCatalog,
        SqlCompileControl, SqlCompileError, SqlImvPlanningInput, SqlImvRewriteSnapshotHandle,
    },
    planning::{
        dml::{
            DmlChangeStreamCompletion, DmlChangeStreamRoute, DmlChangeStreamStatisticsTarget,
            DmlStatisticsSnapshot, DmlWriteCompletion, DmlWritePlanInput,
        },
        mv::first_refresh::*,
    },
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

/// Only captured SQL values and the immutable function catalog are reachable
/// from analysis. No application catalog loader can be smuggled into this type.
pub(crate) struct MvWriteAnalysisContext {
    pub(crate) catalog: SqlCapturedCatalog,
    pub(crate) functions: Arc<novarocks_functions::EngineFunctionCatalog>,
    pub(crate) constant_policy: novarocks_functions::ConstantPolicy,
    pub(crate) control: SqlCompileControl,
    pub(crate) current_catalog: Option<String>,
    pub(crate) current_database: String,
    pub(crate) settings: SessionOptimizerSettings,
}

pub(crate) enum MvWriteAnalysisInput {
    FirstSql {
        source: SqlMvFirstRefreshArtifact,
        context: MvWriteAnalysisContext,
        sink: DmlWritePlanInput,
        required: Vec<StatisticsRequiredAggregation>,
        ordinal: WriteTargetOrdinal,
    },
    FirstJoin {
        query: Box<novarocks_parser::ast::Query>,
        rewrite: SqlImvRewriteSnapshotHandle,
        root_hash_column: String,
        context: MvWriteAnalysisContext,
        sink: DmlWritePlanInput,
        required: Vec<StatisticsRequiredAggregation>,
        ordinal: WriteTargetOrdinal,
    },
    IncrementalCanonical {
        query: Box<novarocks_parser::ast::Query>,
        rewrite: SqlImvPlanningInput,
        mode: SqlMvIncrementalWriteMode,
        context: MvWriteAnalysisContext,
        routes: Vec<DmlChangeStreamRoute>,
        statistics_targets: Vec<DmlChangeStreamStatisticsTarget>,
    },
    IncrementalJoin {
        query: Box<novarocks_parser::ast::Query>,
        rewrite: SqlImvRewriteSnapshotHandle,
        join_mode: SqlMvJoinIncrementalRefreshMode,
        mode: SqlMvIncrementalWriteMode,
        context: MvWriteAnalysisContext,
        routes: Vec<DmlChangeStreamRoute>,
        statistics_targets: Vec<DmlChangeStreamStatisticsTarget>,
    },
}

pub(crate) enum MvWriteAnalysis {
    First(
        SqlMvFirstRefreshAnalyzed,
        Vec<StatisticsRequiredAggregation>,
        WriteTargetOrdinal,
    ),
    FirstJoin(
        SqlMvJoinFirstRefreshAnalyzed,
        Vec<StatisticsRequiredAggregation>,
        WriteTargetOrdinal,
    ),
    Incremental(
        SqlMvIncrementalRefreshAnalyzed,
        Vec<DmlChangeStreamStatisticsTarget>,
    ),
    IncrementalJoin(
        SqlMvJoinIncrementalRefreshAnalyzed,
        Vec<DmlChangeStreamStatisticsTarget>,
    ),
}

impl MvWriteAnalysisInput {
    fn analyze(self) -> Result<MvWriteAnalysis, SqlCompileError> {
        use novarocks_sql::compiler::SqlPlanningEnvironment::Distributed;
        match self {
            Self::FirstSql {
                source,
                context: c,
                sink,
                required,
                ordinal,
            } => {
                let analyzed = analyze_mv_first_refresh_connector_write(
                    source,
                    SqlMvFirstRefreshAnalyzeContext {
                        current_catalog: c.current_catalog,
                        current_database: c.current_database,
                        optimizer_settings: c.settings,
                        environment: Distributed,
                        catalog: &c.catalog,
                        functions: c.functions.as_ref(),
                        constant_evaluator:
                            crate::query_execution::constant_eval::constant_evaluator(),
                        constant_policy: c.constant_policy,
                        control: c.control,
                        sink,
                    },
                )?;
                Ok(MvWriteAnalysis::First(analyzed, required, ordinal))
            }
            Self::FirstJoin {
                query,
                rewrite,
                root_hash_column,
                context: c,
                sink,
                required,
                ordinal,
            } => {
                let analyzed = analyze_join_first_refresh_connector_write(
                    SqlMvJoinFirstRefreshAnalyzeContext {
                        canonical_query: query,
                        rewrite_snapshot: rewrite,
                        expected_root_hash_column: root_hash_column,
                        current_catalog: c.current_catalog,
                        current_database: c.current_database,
                        optimizer_settings: c.settings,
                        environment: Distributed,
                        catalog: &c.catalog,
                        functions: c.functions.as_ref(),
                        constant_evaluator:
                            crate::query_execution::constant_eval::constant_evaluator(),
                        constant_policy: c.constant_policy,
                        control: c.control,
                        sink,
                    },
                )?;
                Ok(MvWriteAnalysis::FirstJoin(analyzed, required, ordinal))
            }
            Self::IncrementalCanonical {
                query,
                rewrite,
                mode,
                context: c,
                routes,
                statistics_targets,
            } => {
                let analyzed = analyze_mv_incremental_refresh_change_stream(
                    SqlMvIncrementalRefreshAnalyzeContext {
                        canonical_query: query,
                        imv_rewrite: rewrite,
                        write_mode: mode,
                        routes,
                        current_catalog: c.current_catalog,
                        current_database: c.current_database,
                        environment: Distributed,
                        catalog: &c.catalog,
                        functions: c.functions.as_ref(),
                        constant_evaluator:
                            crate::query_execution::constant_eval::constant_evaluator(),
                        constant_policy: c.constant_policy,
                        control: c.control,
                    },
                )?;
                Ok(MvWriteAnalysis::Incremental(analyzed, statistics_targets))
            }
            Self::IncrementalJoin {
                query,
                rewrite,
                join_mode,
                mode,
                context: c,
                routes,
                statistics_targets,
            } => {
                let analyzed = analyze_join_incremental_refresh_change_stream(
                    SqlMvJoinIncrementalRefreshAnalyzeContext {
                        canonical_query: query,
                        rewrite_snapshot: rewrite,
                        join_mode,
                        write_mode: mode,
                        routes,
                        current_catalog: c.current_catalog,
                        current_database: c.current_database,
                        optimizer_settings: c.settings,
                        environment: Distributed,
                        catalog: &c.catalog,
                        functions: c.functions.as_ref(),
                        constant_evaluator:
                            crate::query_execution::constant_eval::constant_evaluator(),
                        constant_policy: c.constant_policy,
                        control: c.control,
                    },
                )?;
                Ok(MvWriteAnalysis::IncrementalJoin(
                    analyzed,
                    statistics_targets,
                ))
            }
        }
    }
}

pub(crate) enum MvWriteContinuation {
    First(DmlWriteCompletion),
    Incremental(DmlChangeStreamCompletion),
}

pub(crate) struct MvWriteProviderNeeds {
    pub(crate) continuation: MvWriteContinuation,
    pub(crate) needs: Box<[ProviderReadNeed]>,
}

impl MvWriteAnalysis {
    fn optimize(
        self,
        statistics: &DmlStatisticsSnapshot,
        control: SqlCompileControl,
    ) -> Result<MvWriteProviderNeeds, SqlCompileError> {
        let (continuation, needs) = match self {
            Self::First(analyzed, required, ordinal) => {
                let (completion, needs) = begin_final_mv_first_refresh_connector_write_plan(
                    analyzed, statistics, control, &required, ordinal,
                )?;
                (MvWriteContinuation::First(completion), needs)
            }
            Self::FirstJoin(analyzed, required, ordinal) => {
                let (completion, needs) = begin_final_join_first_refresh_connector_write_plan(
                    analyzed, statistics, control, &required, ordinal,
                )?;
                (MvWriteContinuation::First(completion), needs)
            }
            Self::Incremental(analyzed, targets) => {
                let (completion, needs) = begin_final_mv_incremental_refresh_change_stream(
                    analyzed,
                    statistics,
                    control,
                    targets,
                    novarocks_sql::planning::dml::DmlWritePlanShape::Dataflow,
                )?;
                (MvWriteContinuation::Incremental(completion), needs)
            }
            Self::IncrementalJoin(analyzed, targets) => {
                let (completion, needs) = begin_final_join_incremental_refresh_change_stream(
                    analyzed,
                    statistics,
                    control,
                    targets,
                    novarocks_sql::planning::dml::DmlWritePlanShape::Dataflow,
                )?;
                (MvWriteContinuation::Incremental(completion), needs)
            }
        };
        Ok(MvWriteProviderNeeds {
            continuation,
            needs,
        })
    }
}

pub(crate) struct MvWriteFinishInput {
    pub(crate) continuation: Option<MvWriteContinuation>,
    pub(crate) facts: Option<Vec<ProviderReadFact>>,
    pub(crate) access: Option<FinalPlanRuntimeAccess<FrozenProviderRead>>,
    pub(crate) session: Arc<ConnectorWriteSession>,
    pub(crate) carrier: CompiledPackageCarrier,
    pub(crate) constant_policy: novarocks_functions::ConstantPolicy,
    pub(crate) options: novarocks_proto_codec::lifecycle::QueryOptions,
    pub(crate) control: SqlCompileControl,
}
impl MvWriteFinishInput {
    fn finish(&mut self) -> Result<PreparedMvNativeWriteParts, MvApplicationError> {
        use novarocks_sql::planning::dml::*;
        let targets = DmlFinalizedWriteTargetSet::try_new(
            self.session
                .targets()
                .iter()
                .map(|target| {
                    Ok(DmlFinalizedWriteTarget {
                        ordinal: target.ordinal(),
                        handle: self
                            .session
                            .encode_writer_handle_payload(target.handle())
                            .map_err(provider_error)?,
                    })
                })
                .collect::<Result<Vec<_>, MvApplicationError>>()?,
        )
        .map_err(contract_error)?;
        let version = crate::query_execution::physical_encoding::mint_plan_version();
        let dop = crate::query_execution::contract::completed_plan_dop_domain(None)
            .map_err(contract_error)?;
        let reads = DmlFinalizedProviderReadSet::try_new(
            self.facts
                .take()
                .expect("original read facts")
                .into_iter()
                .map(|fact| DmlFinalizedProviderRead {
                    fact,
                    read_budget: novarocks_physical_plan::ScanReadBudget {
                        max_batch_rows: novarocks_physical_plan::MAX_SCAN_BATCH_ROWS,
                        max_batch_bytes: novarocks_physical_plan::MAX_SCAN_BATCH_BYTES,
                    },
                }),
        )
        .map_err(contract_error)?;
        let plan = match self
            .continuation
            .take()
            .expect("original MV SQL continuation")
        {
            MvWriteContinuation::First(completion) => completion
                .finish(version, dop, reads, targets, &self.control)
                .map_err(MvApplicationError::from_compile)?,
            MvWriteContinuation::Incremental(completion) => {
                completion
                    .finish(
                        DmlFinalWritePlanContext::new(
                            DmlFinalPlanContext::new(version, dop, reads),
                            targets,
                        ),
                        &self.control,
                    )
                    .map_err(MvApplicationError::from_compile)?
                    .into_parts()
                    .0
            }
        };
        let candidate = CompletedPhysicalPlanCandidate::for_sql_program(plan, &self.control)
            .and_then(|candidate| {
                candidate.freeze_root_output(
                    novarocks_result_contract::FrozenRootOutput::InternalFacts(
                        novarocks_result_contract::InternalResultDomain::PreparedWriteCommitV1,
                    ),
                )
            })
            .map_err(|error| {
                use novarocks_query_application::preparation::FinalPlanCompletionError as E;
                let compile = match error {
                    E::Cancelled { .. } => SqlCompileError::Cancelled,
                    E::DeadlineExceeded => SqlCompileError::DeadlineExceeded,
                    E::ResourceExhausted => SqlCompileError::ResourceExhausted,
                    E::Source { source } => SqlCompileError::CompilationSource(source),
                    E::Analyze { error } => SqlCompileError::Analyze(error),
                    error => return contract_error(error.to_string()),
                };
                MvApplicationError::from_compile(compile)
            })?;
        // Encoding and exact mapping checks borrow the original capabilities.
        // No Provider generation or read Source moves onto the worker stack.
        let access = self.access.as_ref().expect("original MV read access");
        let encoded =
            crate::query_execution::physical_encoding::encode_completed_write_plan_borrowed(
                candidate,
                access,
                &self.carrier,
                self.constant_policy,
                &crate::query_execution::physical_encoding::WriteTargetFacts {
                    session: self.session.as_ref(),
                },
                &self.control,
            )
            .map_err(crate::query_execution::mv_native_write::encode_compile_error)
            .map_err(MvApplicationError::from_compile)?;
        let encoded = encoded
            .prepare_access(access)
            .map_err(crate::query_execution::mv_native_write::encode_compile_error)
            .map_err(MvApplicationError::from_compile)?;
        PreparedMvNativeWriteParts::try_new(encoded, version, Some(self.options.clone()))
            .map_err(contract_error)
    }
}

/// This closed turn only binds the original publication documents. It is
/// neither a dispatch mark nor proof of commit or physical CPU exit.
#[derive(Clone, Copy)]
pub(crate) enum MvWritePublicationInputKind {
    Data,
    MetadataOnly,
}

pub(crate) struct MvWritePreparePublicationInput {
    pub(crate) session: Arc<ConnectorWriteSession>,
    pub(crate) intent: novarocks_mv_application::publication::MvRefreshPublicationIntent,
    pub(crate) admitted: Arc<crate::mv::domain::staged_create::AdmittedMvDataPublication>,
    pub(crate) context: novarocks_spi::connector::ConnectorRequestContext,
    pub(crate) kind: MvWritePublicationInputKind,
}

impl MvWritePreparePublicationInput {
    fn prepare(&self) -> Result<(), MvApplicationError> {
        let previous = self.admitted.previous_logical_rows();
        let rows = match self.kind {
            MvWritePublicationInputKind::Data => self
                .session
                .logical_rows_after_received_write(previous, &self.context),
            MvWritePublicationInputKind::MetadataOnly => self
                .session
                .logical_rows_after_empty_publication(previous, &self.context),
        }
        .map_err(provider_error)?;
        crate::mv::bind_publication_documents(
            &self.intent,
            self.admitted.as_ref(),
            self.session.as_ref(),
            rows,
        )
    }
}

#[expect(
    clippy::large_enum_variant,
    reason = "The complete request belongs to the original Arc-backed activation Body and its loan; inline variants avoid an additional variant allocation."
)]
pub(crate) enum MvWriteActivationRequest {
    Analyze(Option<MvWriteAnalysisInput>),
    Optimize {
        analysis: Option<MvWriteAnalysis>,
        statistics: DmlStatisticsSnapshot,
        control: SqlCompileControl,
    },
    Finish(MvWriteFinishInput),
    PreparePublication(MvWritePreparePublicationInput),
    Create(super::create_publication::CreatePublicationInput),
}
#[expect(
    clippy::large_enum_variant,
    reason = "The complete result is retained in the original Arc-backed activation Body before its existing receiving transfer; inline variants avoid an additional variant allocation."
)]
pub(crate) enum MvWriteActivationOutput {
    Analyzed(MvWriteAnalysis),
    ProviderNeeds(MvWriteProviderNeeds),
    Static(PreparedMvNativeWriteParts),
    PublicationPrepared,
    Create(super::create_publication::CreatePublicationOutput),
}
struct Body {
    request: MvWriteActivationRequest,
    result: Option<Result<MvWriteActivationOutput, MvApplicationError>>,
    started: bool,
}
pub(crate) struct MvWriteActivationOriginal {
    body: Mutex<Option<Body>>,
    in_foreign: AtomicBool,
}
struct BodyLoan<'a> {
    original: &'a MvWriteActivationOriginal,
    body: Option<Body>,
}
impl Drop for BodyLoan<'_> {
    fn drop(&mut self) {
        let mut target = self.original.body.lock().unwrap_or_else(|p| p.into_inner());
        assert!(target.is_none(), "one original MV activation CPU loan");
        *target = self.body.take();
    }
}
impl MvWriteActivationOriginal {
    pub(crate) fn new(request: MvWriteActivationRequest) -> Arc<Self> {
        Arc::new(Self {
            body: Mutex::new(Some(Body {
                request,
                result: None,
                started: false,
            })),
            in_foreign: AtomicBool::new(false),
        })
    }
    /// Only the closed CPU factory calls this, with the original physical grant.
    /// A panic restores the same body and leaves the poison fact set; the outer
    /// original CPU job retains its raw failure and actual-exit evidence.
    pub(crate) fn run_once(&self) {
        if self.in_foreign.swap(true, Ordering::AcqRel) {
            return;
        }
        let body = self.body.lock().unwrap_or_else(|p| p.into_inner()).take();
        let mut loan = BodyLoan {
            original: self,
            body,
        };
        let body = loan.body.as_mut().expect("original MV activation CPU body");
        assert!(!body.started, "one original MV activation CPU turn");
        body.started = true;
        let result = match &mut body.request {
            MvWriteActivationRequest::Analyze(input) => input
                .take()
                .expect("original MV analysis input")
                .analyze()
                .map_err(MvApplicationError::from_compile)
                .map(MvWriteActivationOutput::Analyzed),
            MvWriteActivationRequest::Optimize {
                analysis,
                statistics,
                control,
            } => analysis
                .take()
                .expect("original MV analyzed input")
                .optimize(statistics, control.clone())
                .map_err(MvApplicationError::from_compile)
                .map(MvWriteActivationOutput::ProviderNeeds),
            MvWriteActivationRequest::Finish(input) => {
                input.finish().map(MvWriteActivationOutput::Static)
            }
            MvWriteActivationRequest::PreparePublication(input) => input
                .prepare()
                .map(|()| MvWriteActivationOutput::PublicationPrepared),
            MvWriteActivationRequest::Create(input) => {
                input.run().map(MvWriteActivationOutput::Create)
            }
        };
        body.result = Some(result);
        drop(loan);
        self.in_foreign.store(false, Ordering::Release);
    }
    pub(crate) fn foreign_exit_unproven(&self) -> bool {
        self.in_foreign.load(Ordering::Acquire)
    }
    /// Called after the original CPU receipt and forwarder have really closed.
    /// Every fallible encode/mapping/description check completed while access
    /// was borrowed; this receiving step performs only the original move.
    pub(crate) fn take_assembly_in(
        &self,
        target: &mut Option<PreparedMvNativeWriteAssembly>,
    ) -> Result<(), &'static str> {
        if target.is_some() {
            return Err("MV assembly receiver is occupied");
        }
        if self.foreign_exit_unproven() {
            return Err("original MV activation turn is unproven");
        }
        let body = self.body.lock().unwrap_or_else(|p| p.into_inner()).take();
        let mut loan = BodyLoan {
            original: self,
            body,
        };
        let body = loan
            .body
            .as_mut()
            .ok_or("original MV activation body is borrowed")?;
        if !matches!(&body.result, Some(Ok(MvWriteActivationOutput::Static(_)))) {
            return Err("MV activation has no prepared static plan");
        }
        let MvWriteActivationRequest::Finish(input) = &mut body.request else {
            return Err("MV static plan has a foreign original input");
        };
        if input.access.is_none() {
            return Err("MV original read access was already received");
        }
        let Some(Ok(MvWriteActivationOutput::Static(parts))) = body.result.take() else {
            unreachable!()
        };
        *target = Some(parts.bind_access(input.access.take().unwrap(), Arc::clone(&input.session)));
        Ok(())
    }
    /// A failed finalization leaves the unique original access in this same
    /// Source. The caller receives the typed error only after CPU actual exit.
    pub(crate) fn take_failure_in(
        &self,
        target: &mut Option<MvApplicationError>,
    ) -> Result<bool, &'static str> {
        if target.is_some() {
            return Err("MV failure receiver is occupied");
        }
        if self.foreign_exit_unproven() {
            return Err("MV original foreign exit is unproven");
        }
        let mut state = self.body.lock().unwrap_or_else(|p| p.into_inner());
        let body = state.as_mut().ok_or("MV original body is borrowed")?;
        if !matches!(body.result, Some(Err(_))) {
            return Ok(false);
        }
        let Some(Err(error)) = body.result.take() else {
            unreachable!()
        };
        *target = Some(error);
        Ok(true)
    }
    pub(crate) fn take_output_in(
        &self,
        target: &mut Option<Result<MvWriteActivationOutput, MvApplicationError>>,
    ) -> Result<(), &'static str> {
        if target.is_some() {
            return Err("MV activation result receiver is occupied");
        }
        if self.foreign_exit_unproven() {
            return Err("original MV activation CPU exit is unproven");
        }
        let mut state = self.body.lock().unwrap_or_else(|p| p.into_inner());
        let body = state
            .as_mut()
            .ok_or("original MV activation body is borrowed")?;
        if body.result.is_none() {
            return Err("original MV activation CPU result is not ready");
        }
        *target = body.result.take();
        Ok(())
    }
}
