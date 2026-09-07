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

//! Result-free SQL physicalization for MV first refresh.
//!
//! A first refresh writes a fresh, empty staging target. This module makes the
//! physical rows needed by that append cohort explicit, so the caller can put a
//! connector writer at the native distributed root without materializing data
//! in the frontend.

mod sql_shape;
use crate::binding::SqlTableBindingId;
use crate::column_id::ColumnRefFactory;
use crate::compiler::RootDistributionRequirement;
use crate::mv_refresh::aggregate_shape::{
    SQL_MV_AGG_RETRACTION_COUNT_STATE_COLUMN, SQL_MV_ROW_ID_COLUMN, SqlAggregateCalls,
    rewrite_select_sql_for_state, state_column_name,
};
use crate::mv_refresh::{AggregateFunctionKind, VisibleAggregateOutput};
use crate::planner::logical::LogicalPlanNode;
use crate::planner::vocabulary::BRANCH_ID_COLUMN_NAME;
use arrow::datatypes::{DataType, Schema, SchemaRef};
use novarocks_parser::Span;
use novarocks_parser::ast;
use std::cell::RefCell;
use std::collections::BTreeSet;
use std::rc::Rc;
use std::sync::Arc;

pub use self::sql_shape::SqlMvSnapshotPin;
use self::sql_shape::{
    branch_union_queries, pin_state_sql, prepare_projection_full_read_sql,
    prepare_union_projection_full_read_sql,
};

/// SQL-only input for one first-refresh planning step.
///
/// The application has already frozen the target binding before constructing
/// this value.  It deliberately carries neither a connector table handle nor
/// a write operation/cohort: those are lifecycle facts and are attached only
/// after the application admits an exact write lease.
#[allow(
    dead_code,
    reason = "The first-refresh input contract is retained while application-side admission wiring is staged."
)]
pub(crate) struct SqlMvFirstRefreshPlannerInput {
    pub(crate) shape: MvFirstRefreshShape,
    pub(crate) target_contract: MvFirstRefreshTargetContract,
    pub(crate) target_binding: SqlTableBindingId,
    pub(crate) root_distribution: RootDistributionRequirement,
    pub(crate) artifact: SqlMvFirstRefreshArtifactInput,
}

/// A first-refresh artifact before it becomes an immutable plan.  The logical
/// variant contains only SQL planner values; it intentionally has no refresh
/// context or provider authority.
#[expect(
    clippy::large_enum_variant,
    reason = "The inline SQL plan payload avoids an allocation at the compiler handoff boundary."
)]
#[allow(
    dead_code,
    reason = "The first-refresh artifact input preserves the staged SQL-to-application handoff contract."
)]
pub(crate) enum SqlMvFirstRefreshArtifactInput {
    Sql(MvFirstRefreshPhysicalSql),
    Logical {
        plan: LogicalPlanNode,
        factory: ColumnRefFactory,
        root_hash_column: String,
    },
}

/// Immutable SQL first-refresh artifact handed to the application lifecycle.
///
/// This is the complete SQL boundary: a logical/physical plan, shape, target
/// contract, root distribution requirement and query-local binding token.  In
/// particular, it contains no operation/cohort ID, connector handle/request
/// context, prepared write, catalog object or commit lifecycle value.
#[allow(
    dead_code,
    reason = "The immutable first-refresh plan is retained for the pending application handoff."
)]
pub(crate) struct SqlMvFirstRefreshPlan {
    shape: MvFirstRefreshShape,
    target_contract: MvFirstRefreshTargetContract,
    target_binding: SqlTableBindingId,
    root_distribution: RootDistributionRequirement,
    artifact: SqlMvFirstRefreshPlanArtifact,
}

#[expect(
    clippy::large_enum_variant,
    reason = "The inline SQL plan payload avoids an allocation at the compiler handoff boundary."
)]
#[allow(
    dead_code,
    reason = "The first-refresh artifact remains part of the retained immutable handoff contract."
)]
pub(crate) enum SqlMvFirstRefreshPlanArtifact {
    Sql(MvFirstRefreshPhysicalSql),
    Logical {
        plan: LogicalPlanNode,
        factory: ColumnRefFactory,
    },
}

/// Canonical, side-effect-free SQL planner for an MV first refresh.
#[allow(
    dead_code,
    reason = "The side-effect-free planner is retained until application-side first-refresh admission is wired."
)]
pub(crate) struct SqlMvFirstRefreshPlanner;

impl SqlMvFirstRefreshPlanner {
    #[allow(
        dead_code,
        reason = "The planner entry point is retained with its staged first-refresh handoff contract."
    )]
    pub(crate) fn plan(
        input: SqlMvFirstRefreshPlannerInput,
    ) -> Result<SqlMvFirstRefreshPlan, String> {
        let (artifact, root_hash_column) = match input.artifact {
            SqlMvFirstRefreshArtifactInput::Sql(sql) => {
                let root_hash_column = sql.root_hash_column().to_string();
                (SqlMvFirstRefreshPlanArtifact::Sql(sql), root_hash_column)
            }
            SqlMvFirstRefreshArtifactInput::Logical {
                plan,
                factory,
                root_hash_column,
            } => {
                if root_hash_column.is_empty() {
                    return Err(
                        "MV first-refresh logical artifact has no root hash column".to_string()
                    );
                }
                (
                    SqlMvFirstRefreshPlanArtifact::Logical { plan, factory },
                    root_hash_column,
                )
            }
        };
        validate_root_distribution(
            &input.root_distribution,
            &root_hash_column,
            input.target_contract.hidden_hash_key(),
        )?;
        Ok(SqlMvFirstRefreshPlan {
            shape: input.shape,
            target_contract: input.target_contract,
            target_binding: input.target_binding,
            root_distribution: input.root_distribution,
            artifact,
        })
    }
}

impl SqlMvFirstRefreshPlan {
    #[allow(
        dead_code,
        reason = "The retained handoff plan exposes its validated SQL shape."
    )]
    pub(crate) const fn shape(&self) -> MvFirstRefreshShape {
        self.shape
    }

    #[allow(
        dead_code,
        reason = "The retained handoff plan exposes its frozen target contract."
    )]
    pub(crate) fn target_contract(&self) -> &MvFirstRefreshTargetContract {
        &self.target_contract
    }

    #[allow(
        dead_code,
        reason = "The retained handoff plan exposes its frozen target binding."
    )]
    pub(crate) const fn target_binding(&self) -> SqlTableBindingId {
        self.target_binding
    }

    #[allow(
        dead_code,
        reason = "The retained handoff plan exposes its root distribution requirement."
    )]
    pub(crate) fn root_distribution(&self) -> &RootDistributionRequirement {
        &self.root_distribution
    }

    #[allow(
        dead_code,
        reason = "The retained handoff plan exposes its frozen SQL artifact."
    )]
    pub(crate) fn into_artifact(self) -> SqlMvFirstRefreshPlanArtifact {
        self.artifact
    }
}

fn validate_root_distribution(
    requirement: &RootDistributionRequirement,
    root_hash_column: &str,
    target_hidden_hash_key: &str,
) -> Result<(), String> {
    if root_hash_column != target_hidden_hash_key {
        return Err(
            "MV first-refresh root distribution does not match the target hidden hash key"
                .to_string(),
        );
    }
    match requirement {
        RootDistributionRequirement::ShuffleOutputName(name) if name == root_hash_column => Ok(()),
        RootDistributionRequirement::ShuffleOutputName(_) => Err(
            "MV first-refresh root distribution output name does not match the SQL artifact"
                .to_string(),
        ),
        RootDistributionRequirement::ShuffleOutputOrdinal(_) => {
            Err("MV first-refresh requires a named root distribution key".to_string())
        }
        RootDistributionRequirement::Any => {
            Err("MV first-refresh requires an explicit root distribution key".to_string())
        }
    }
}

/// Immutable SQL artifact for a distributed first-refresh write.
///
/// `root_hash_column` is the target contract's hidden apply key. The native
/// planner must derive its actual writer fanout from the admitted topology.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MvFirstRefreshPhysicalSql {
    sql: String,
    root_hash_column: String,
}

/// Move-only SQL source artifact for a first-refresh write.
///
/// Applications can retain and hand this value back to SQL, but cannot read
/// its SQL text or obtain a logical/physical planner graph from it.
pub struct SqlMvFirstRefreshArtifact(MvFirstRefreshPhysicalSql);

impl SqlMvFirstRefreshArtifact {
    fn from_physical(physical: MvFirstRefreshPhysicalSql) -> Self {
        Self(physical)
    }

    pub fn root_hash_column(&self) -> &str {
        self.0.root_hash_column()
    }

    fn sql(&self) -> &str {
        self.0.sql()
    }
}

/// Immutable application facts required to consume one opaque first-refresh
/// source.  Connector handles, write leases, lifecycle state and wire payloads
/// are deliberately absent.
pub struct SqlMvFirstRefreshAnalyzeContext<'a> {
    pub current_catalog: Option<String>,
    pub current_database: String,
    pub optimizer_settings: crate::compiler::SessionOptimizerSettings,
    pub environment: crate::compiler::SqlPlanningEnvironment,
    pub catalog: &'a dyn crate::compiler::SqlCatalogSnapshot,
    pub functions: &'a dyn crate::compiler::SqlFunctionCatalog,
    pub constant_evaluator: &'static dyn crate::compiler::SqlConstantEvaluator,
    pub control: crate::compiler::SqlCompileControl,
    pub sink: crate::planning::dml::DmlWritePlanInput,
}

pub struct SqlMvFirstRefreshAnalyzed {
    analyzed: crate::compiler::SqlAnalyzedQuery,
    sink: crate::planning::dml::DmlWritePlanInput,
    settings: crate::compiler::SessionOptimizerSettings,
}

/// Compile an opaque first-refresh source directly into a sealed connector
/// write plan.  No raw SQL, logical plan, optimizer tree, or physical graph
/// can cross this terminal boundary.
pub fn analyze_mv_first_refresh_connector_write(
    artifact: SqlMvFirstRefreshArtifact,
    context: SqlMvFirstRefreshAnalyzeContext<'_>,
) -> Result<SqlMvFirstRefreshAnalyzed, String> {
    let root_distribution = crate::compiler::RootDistributionRequirement::ShuffleOutputName(
        artifact.root_hash_column().to_string(),
    );
    let settings = context.optimizer_settings.clone();
    let request = crate::compiler::SqlAnalyzeRequest::new(
        crate::compiler::SqlStatementInput::sql(artifact.sql()),
        crate::compiler::SqlCompileIntent::IcebergWrite { root_distribution },
        crate::compiler::SqlSessionContext {
            current_catalog: context.current_catalog,
            current_database: context.current_database,
            optimizer_settings: context.optimizer_settings,
        },
        context.environment,
        context.catalog,
        context.functions,
        context.constant_evaluator,
        None,
        context.control,
    );
    let analyzed = crate::compiler::SqlCompiler::analyze(request)
        .map_err(|error| error.to_string())?
        .into_pending()
        .map_err(|error| error.to_string())?;
    Ok(SqlMvFirstRefreshAnalyzed {
        analyzed,
        sink: context.sink,
        settings,
    })
}

pub fn compile_mv_first_refresh_connector_write_dataflow(
    analyzed: SqlMvFirstRefreshAnalyzed,
    statistics: &crate::planning::dml::DmlStatisticsSnapshot,
    required_aggregations: &[novarocks_spi::connector::StatisticsRequiredAggregation],
    write_target_ordinal: novarocks_spi::connector::write_stack::WriteTargetOrdinal,
) -> Result<crate::plan_read::DistributedPlan, String> {
    crate::planning::dml::compile_connector_write_dataflow_plan(
        crate::compiler::SqlOptimizeRequest::new(analyzed.analyzed, statistics),
        analyzed.sink,
        write_target_ordinal,
        required_aggregations,
        &analyzed.settings,
    )
}

/// Immutable inputs for the join-MV first-refresh terminal.  The snapshot is
/// already sealed by the compiler facade; the query is syntax only, not a
/// logical or physical planner graph.
pub struct SqlMvJoinFirstRefreshAnalyzeContext<'a> {
    pub canonical_query: Box<ast::Query>,
    pub rewrite_snapshot: crate::compiler::SqlImvRewriteSnapshotHandle,
    pub expected_root_hash_column: String,
    pub current_catalog: Option<String>,
    pub current_database: String,
    pub optimizer_settings: crate::compiler::SessionOptimizerSettings,
    pub environment: crate::compiler::SqlPlanningEnvironment,
    pub catalog: &'a dyn crate::compiler::SqlCatalogSnapshot,
    pub functions: &'a dyn crate::compiler::SqlFunctionCatalog,
    pub constant_evaluator: &'static dyn crate::compiler::SqlConstantEvaluator,
    pub control: crate::compiler::SqlCompileControl,
    pub sink: crate::planning::dml::DmlWritePlanInput,
}

pub struct SqlMvJoinFirstRefreshAnalyzed {
    analyzed: crate::compiler::SqlAnalyzedQuery,
    sink: crate::planning::dml::DmlWritePlanInput,
    settings: crate::compiler::SessionOptimizerSettings,
}

/// Compile the canonical join first-refresh query all the way to a sealed
/// connector-write plan.  SQL alone creates the hidden join key, validates
/// frozen lineage and physicalizes the resulting append projection.
pub fn analyze_join_first_refresh_connector_write(
    context: SqlMvJoinFirstRefreshAnalyzeContext<'_>,
) -> Result<SqlMvJoinFirstRefreshAnalyzed, String> {
    let snapshot = context.rewrite_snapshot.snapshot();
    let root_hash_column = snapshot
        .schema_contract
        .target
        .hidden_apply_key
        .column_name
        .clone();
    if !root_hash_column.eq_ignore_ascii_case(&context.expected_root_hash_column) {
        return Err(
            "join first-refresh root hash column does not match the sealed target contract"
                .to_string(),
        );
    }
    let settings = context.optimizer_settings.clone();
    let mut query = *context.canonical_query;
    crate::planning::mv::strip_catalog_from_three_part_names(&mut query);
    let request = plain_join_first_refresh_logical_request(
        query,
        context.current_catalog.clone(),
        context.current_database.clone(),
        context.optimizer_settings.clone(),
        context.environment,
        context.catalog,
        context.functions,
        context.constant_evaluator,
        context.control.clone(),
    );
    let logical_output = crate::compiler::SqlCompiler::analyze(request)
        .map_err(|error| error.to_string())?
        .into_complete()
        .map_err(|error| error.to_string())?
        .into_logical_output()
        .map_err(|_| {
            "join first-refresh logical intent did not produce logical SQL facts".to_string()
        })?;
    let (plan, factory) = build_join_first_refresh_append_logical_plan(
        crate::planner::imv_rewrite::entrypoint::normalize_imv_rewrite_root_project(
            logical_output.logical_plan,
        ),
        logical_output.factory,
        snapshot,
    )?;
    let logical_request = crate::compiler::SqlAnalyzeRequest::new_logical(
        plan,
        factory,
        crate::compiler::SqlCompileIntent::IcebergWrite {
            root_distribution: crate::compiler::RootDistributionRequirement::ShuffleOutputName(
                root_hash_column,
            ),
        },
        crate::compiler::SqlSessionContext {
            current_catalog: context.current_catalog,
            current_database: context.current_database,
            optimizer_settings: context.optimizer_settings,
        },
        context.environment,
        Some(context.constant_evaluator),
        context.control,
    )
    .with_function_catalog(context.functions.snapshot());
    let analyzed = crate::compiler::SqlCompiler::analyze(logical_request)
        .map_err(|error| error.to_string())?
        .into_pending()
        .map_err(|error| error.to_string())?;
    Ok(SqlMvJoinFirstRefreshAnalyzed {
        analyzed,
        sink: context.sink,
        settings,
    })
}

pub fn compile_join_first_refresh_connector_write_dataflow(
    analyzed: SqlMvJoinFirstRefreshAnalyzed,
    statistics: &crate::planning::dml::DmlStatisticsSnapshot,
    required_aggregations: &[novarocks_spi::connector::StatisticsRequiredAggregation],
    write_target_ordinal: novarocks_spi::connector::write_stack::WriteTargetOrdinal,
) -> Result<crate::plan_read::DistributedPlan, String> {
    crate::planning::dml::compile_connector_write_dataflow_plan(
        crate::compiler::SqlOptimizeRequest::new(analyzed.analyzed, statistics),
        analyzed.sink,
        write_target_ordinal,
        required_aggregations,
        &analyzed.settings,
    )
}

/// SQL-only change shape for an incremental join refresh.  The application
/// selects this from frozen provider observations; it cannot attach a planner
/// graph or mutate the sealed snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SqlMvJoinIncrementalRefreshMode {
    AppendOnly,
    Coalesce,
}

/// Frozen producer-route shape for an incremental refresh.  This is distinct
/// from the join rewrite mode: a coalesced join can still write a full row
/// delta, while an append-only refresh installs one append producer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SqlMvIncrementalWriteMode {
    FastAppend,
    RowDelta,
}

/// Immutable inputs for the join-incremental change-stream terminal.  The
/// route facts are provider-signed values and the rewrite snapshot is opaque;
/// no logical/optimized plan, factory, mutable DAG, lease, or lifecycle state
/// can cross this API.
pub struct SqlMvJoinIncrementalRefreshAnalyzeContext<'a> {
    pub canonical_query: Box<ast::Query>,
    pub rewrite_snapshot: crate::compiler::SqlImvRewriteSnapshotHandle,
    pub join_mode: SqlMvJoinIncrementalRefreshMode,
    pub write_mode: SqlMvIncrementalWriteMode,
    pub routes: Vec<crate::planning::dml::DmlChangeStreamRoute>,
    pub current_catalog: Option<String>,
    pub current_database: String,
    pub optimizer_settings: crate::compiler::SessionOptimizerSettings,
    pub environment: crate::compiler::SqlPlanningEnvironment,
    pub catalog: &'a dyn crate::compiler::SqlCatalogSnapshot,
    pub functions: &'a dyn crate::compiler::SqlFunctionCatalog,
    pub constant_evaluator: &'static dyn crate::compiler::SqlConstantEvaluator,
    pub control: crate::compiler::SqlCompileControl,
}

pub struct SqlMvJoinIncrementalRefreshAnalyzed {
    analyzed: crate::compiler::SqlAnalyzedQuery,
    change_stream_override:
        Option<crate::planner::imv_rewrite::change_stream::ImvChangeStreamDescriptor>,
    write_mode: SqlMvIncrementalWriteMode,
    routes: Vec<crate::planning::dml::DmlChangeStreamRoute>,
}

/// Compile an incremental join refresh all the way to a sealed change-stream
/// plan.  Canonical compilation deliberately remains plain `LogicalOnly`;
/// the sealed snapshot is consumed only by the SQL-owned rewrite stage, which
/// preserves the former Core logical-path semantics.
pub fn analyze_join_incremental_refresh_change_stream(
    context: SqlMvJoinIncrementalRefreshAnalyzeContext<'_>,
) -> Result<SqlMvJoinIncrementalRefreshAnalyzed, String> {
    validate_join_incremental_routes(&context.routes)?;
    let snapshot = context.rewrite_snapshot.snapshot();
    validate_join_incremental_snapshot(snapshot)?;
    let mut query = *context.canonical_query;
    crate::planning::mv::strip_catalog_from_three_part_names(&mut query);
    let request = plain_join_first_refresh_logical_request(
        query,
        context.current_catalog,
        context.current_database,
        context.optimizer_settings,
        context.environment,
        context.catalog,
        context.functions,
        context.constant_evaluator,
        context.control.clone(),
    );
    let logical_output = crate::compiler::SqlCompiler::analyze(request)
        .map_err(|error| error.to_string())?
        .into_complete()
        .map_err(|error| error.to_string())?
        .into_logical_output()
        .map_err(|_| {
            "join incremental refresh logical intent did not produce logical SQL facts".to_string()
        })?;
    let logical = crate::planner::imv_rewrite::entrypoint::normalize_imv_rewrite_root_project(
        logical_output.logical_plan,
    );
    let (plan, factory, change_stream_override) = build_join_incremental_refresh_logical_plan(
        snapshot,
        context.join_mode,
        logical,
        logical_output.factory,
        context.functions,
    )?;
    let logical_request = crate::compiler::SqlAnalyzeRequest::new_logical(
        plan,
        factory,
        crate::compiler::SqlCompileIntent::ChangeStreamWrite,
        crate::compiler::SqlSessionContext {
            current_catalog: None,
            current_database: String::new(),
            optimizer_settings: crate::planning::dml::dml_change_stream_optimizer_settings(),
        },
        crate::compiler::SqlPlanningEnvironment::NotApplicable,
        Some(context.constant_evaluator),
        context.control,
    )
    .with_function_catalog(context.functions.snapshot());
    let analyzed = crate::compiler::SqlCompiler::analyze(logical_request)
        .map_err(|error| error.to_string())?
        .into_pending()
        .map_err(|error| error.to_string())?;
    Ok(SqlMvJoinIncrementalRefreshAnalyzed {
        analyzed,
        change_stream_override,
        write_mode: context.write_mode,
        routes: context.routes,
    })
}

pub fn compile_join_incremental_refresh_change_stream(
    analyzed: SqlMvJoinIncrementalRefreshAnalyzed,
    statistics: &crate::planning::dml::DmlStatisticsSnapshot,
    statistics_targets: Vec<crate::planning::dml::DmlChangeStreamStatisticsTarget>,
    shape: crate::planning::dml::DmlWritePlanShape,
) -> Result<crate::planning::dml::DmlChangeStreamPlan, String> {
    let compiled = crate::compiler::SqlCompiler::optimize(
        crate::compiler::SqlOptimizeRequest::new(analyzed.analyzed, statistics),
    )
    .map_err(|error| error.to_string())?
    .into_optimized_output()
    .map_err(|_| {
        "join incremental logical input did not produce an optimized SQL plan".to_string()
    })?;
    let function_catalog = std::sync::Arc::clone(&compiled.function_catalog);
    let change_stream = analyzed
        .change_stream_override
        .unwrap_or(compiled.change_stream);
    let producer = add_join_incremental_change_stream_effect(
        compiled.optimized_tree,
        &change_stream,
        analyzed.write_mode,
    )?;
    crate::planning::dml::seal_change_stream_producer_with_effect_column(
        producer,
        analyzed.routes,
        statistics_targets,
        JOIN_INCREMENTAL_EFFECT_COLUMN,
        None,
        shape,
        function_catalog.as_ref(),
    )
}

/// Immutable inputs for the canonical incremental-MV change-stream terminal.
/// The rewrite snapshot remains sealed inside [`SqlImvPlanningInput`], while
/// provider-signed route facts are bound only after SQL has produced the
/// complete change-stream producer.
pub struct SqlMvIncrementalRefreshAnalyzeContext<'a> {
    pub canonical_query: Box<ast::Query>,
    pub imv_rewrite: crate::compiler::SqlImvPlanningInput,
    pub write_mode: SqlMvIncrementalWriteMode,
    pub routes: Vec<crate::planning::dml::DmlChangeStreamRoute>,
    pub current_catalog: Option<String>,
    pub current_database: String,
    pub environment: crate::compiler::SqlPlanningEnvironment,
    pub catalog: &'a dyn crate::compiler::SqlCatalogSnapshot,
    pub functions: &'a dyn crate::compiler::SqlFunctionCatalog,
    pub constant_evaluator: &'static dyn crate::compiler::SqlConstantEvaluator,
    pub control: crate::compiler::SqlCompileControl,
}

pub struct SqlMvIncrementalRefreshAnalyzed {
    analyzed: crate::compiler::SqlAnalyzedQuery,
    write_mode: SqlMvIncrementalWriteMode,
    routes: Vec<crate::planning::dml::DmlChangeStreamRoute>,
}

/// Compile a canonical incremental MV query all the way to a sealed
/// change-stream plan. The canonical request keeps its former sealed
/// `ChangeStreamWrite` rewrite semantics: the SQL compiler consumes the IMV
/// input directly, then this terminal installs the provider effect projection
/// and writer topology before returning only a sealed distributed plan.
pub fn analyze_mv_incremental_refresh_change_stream(
    context: SqlMvIncrementalRefreshAnalyzeContext<'_>,
) -> Result<SqlMvIncrementalRefreshAnalyzed, String> {
    validate_join_incremental_routes(&context.routes)?;
    let mut query = *context.canonical_query;
    if matches!(
        context.imv_rewrite.validation,
        crate::compiler::SqlImvRewriteValidation::Aggregate
            | crate::compiler::SqlImvRewriteValidation::JoinAggregate
    ) {
        alias_incremental_aggregate_group_key_projection(
            &mut query,
            context.imv_rewrite.snapshot(),
        )?;
    }
    crate::planning::mv::strip_catalog_from_three_part_names(&mut query);
    let request = canonical_incremental_change_stream_request(
        query,
        &context.imv_rewrite,
        context.current_catalog,
        context.current_database,
        context.environment,
        context.catalog,
        context.functions,
        context.constant_evaluator,
        context.control,
    );
    let analyzed = crate::compiler::SqlCompiler::analyze(request)
        .map_err(|error| error.to_string())?
        .into_pending()
        .map_err(|error| error.to_string())?;
    Ok(SqlMvIncrementalRefreshAnalyzed {
        analyzed,
        write_mode: context.write_mode,
        routes: context.routes,
    })
}

pub fn compile_mv_incremental_refresh_change_stream(
    analyzed: SqlMvIncrementalRefreshAnalyzed,
    statistics: &crate::planning::dml::DmlStatisticsSnapshot,
    statistics_targets: Vec<crate::planning::dml::DmlChangeStreamStatisticsTarget>,
    shape: crate::planning::dml::DmlWritePlanShape,
) -> Result<crate::planning::dml::DmlChangeStreamPlan, String> {
    let compiled = crate::compiler::SqlCompiler::optimize(
        crate::compiler::SqlOptimizeRequest::new(analyzed.analyzed, statistics),
    )
    .map_err(|error| error.to_string())?
    .into_optimized_output()
    .map_err(|_| {
        "canonical incremental MV intent did not produce an optimized SQL plan".to_string()
    })?;
    let function_catalog = std::sync::Arc::clone(&compiled.function_catalog);
    let producer = add_join_incremental_change_stream_effect(
        compiled.optimized_tree,
        &compiled.change_stream,
        analyzed.write_mode,
    )?;
    crate::planning::dml::seal_change_stream_producer_with_effect_column(
        producer,
        analyzed.routes,
        statistics_targets,
        JOIN_INCREMENTAL_EFFECT_COLUMN,
        None,
        shape,
        function_catalog.as_ref(),
    )
}

#[allow(clippy::too_many_arguments)]
fn canonical_incremental_change_stream_request<'a>(
    query: ast::Query,
    imv_rewrite: &'a crate::compiler::SqlImvPlanningInput,
    current_catalog: Option<String>,
    current_database: String,
    environment: crate::compiler::SqlPlanningEnvironment,
    catalog: &'a dyn crate::compiler::SqlCatalogSnapshot,
    functions: &'a dyn crate::compiler::SqlFunctionCatalog,
    constant_evaluator: &'static dyn crate::compiler::SqlConstantEvaluator,
    control: crate::compiler::SqlCompileControl,
) -> crate::compiler::SqlAnalyzeRequest<'a> {
    crate::compiler::SqlAnalyzeRequest::new(
        crate::compiler::SqlStatementInput::parsed_query(Box::new(query)),
        crate::compiler::SqlCompileIntent::ChangeStreamWrite,
        crate::compiler::SqlSessionContext {
            current_catalog,
            current_database,
            optimizer_settings: crate::planning::dml::dml_change_stream_optimizer_settings(),
        },
        environment,
        catalog,
        functions,
        constant_evaluator,
        None,
        control,
    )
    .with_imv_rewrite(imv_rewrite)
}

fn alias_incremental_aggregate_group_key_projection(
    query: &mut ast::Query,
    snapshot: &crate::compiler::mv_rewrite::SqlImvRewriteSnapshot,
) -> Result<(), String> {
    let (calls, layout) = snapshot.aggregate_shape_and_layout_for_execution()?;
    let ast::SetExpr::Select(select) = query.body.as_mut() else {
        return Err("aggregate MV incremental refresh SELECT body is required".to_string());
    };
    for (projection_index, output) in calls.visible_outputs.iter().enumerate() {
        let crate::mv_refresh::VisibleAggregateOutput::GroupKey(group_key_index) = output else {
            continue;
        };
        let visible_source_index = layout
            .group_key_source_indexes
            .get(*group_key_index)
            .ok_or_else(|| {
                format!("aggregate MV group key projection index {group_key_index} out of range")
            })?;
        let expected_name = &layout
            .visible_columns
            .get(*visible_source_index)
            .ok_or_else(|| {
                format!(
                    "aggregate MV group key visible source index {visible_source_index} out of range"
                )
            })?
            .name;
        let item = select.projection.get_mut(projection_index).ok_or_else(|| {
            format!("aggregate MV group key projection position {projection_index} is missing")
        })?;
        alias_incremental_select_projection_item(item, expected_name)?;
        if let ast::GroupBy::Expressions { expressions, .. } = &mut select.group_by
            && let Some(group_expr) = expressions.get_mut(*group_key_index)
        {
            *group_expr = ast::Expr::Identifier(incremental_alias_ident(expected_name));
        }
    }
    Ok(())
}

fn alias_incremental_select_projection_item(
    item: &mut ast::SelectItem,
    alias: &str,
) -> Result<(), String> {
    use ast::SelectItem;

    let alias = incremental_alias_ident(alias);
    match item {
        SelectItem::UnnamedExpr(expr) => {
            let expr = expr.clone();
            *item = SelectItem::ExprWithAlias {
                expr,
                alias,
                explicit_as: true,
                span: Span::new(0, 0),
            };
            Ok(())
        }
        SelectItem::ExprWithAlias {
            alias: existing, ..
        } => {
            *existing = alias;
            Ok(())
        }
        SelectItem::QualifiedWildcard { .. } | SelectItem::Wildcard { .. } => {
            Err("aggregate MV group key projection cannot be a wildcard".to_string())
        }
    }
}

fn incremental_alias_ident(alias: &str) -> ast::Ident {
    let mut chars = alias.chars();
    let is_plain = chars
        .next()
        .map(|first| first.is_ascii_alphabetic() || first == '_')
        .unwrap_or(false)
        && chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_');
    if is_plain {
        ast::Ident {
            value: alias.to_string(),
            quoted: false,
            quote_style: None,
            span: Span::new(0, 0),
        }
    } else {
        ast::Ident {
            value: alias.to_string(),
            quoted: true,
            quote_style: Some('`'),
            span: Span::new(0, 0),
        }
    }
}

fn validate_join_incremental_routes(
    routes: &[crate::planning::dml::DmlChangeStreamRoute],
) -> Result<(), String> {
    if routes.is_empty() {
        return Err(
            "join incremental refresh requires at least one admitted writer route".to_string(),
        );
    }
    if routes.iter().any(|route| route.input_fields.is_empty()) {
        return Err(
            "join incremental refresh has an admitted writer route without inputs".to_string(),
        );
    }
    Ok(())
}

fn validate_join_incremental_snapshot(
    snapshot: &crate::compiler::mv_rewrite::SqlImvRewriteSnapshot,
) -> Result<(), String> {
    let join = snapshot
        .schema_contract
        .join
        .as_ref()
        .ok_or_else(|| "join incremental refresh snapshot has no join contract".to_string())?;
    if join.predicates.is_empty() {
        return Err("join incremental refresh snapshot has no join predicate facts".to_string());
    }
    if snapshot.base_snapshots.len() < 2 {
        return Err(
            "join incremental refresh snapshot has fewer than two pinned bases".to_string(),
        );
    }
    Ok(())
}

#[cfg_attr(test, allow(unused_variables))]
fn build_join_incremental_refresh_logical_plan(
    snapshot: &Arc<crate::compiler::mv_rewrite::SqlImvRewriteSnapshot>,
    mode: SqlMvJoinIncrementalRefreshMode,
    plan: crate::planner::logical::LogicalPlanNode,
    factory: crate::column_id::ColumnRefFactory,
    functions: &dyn crate::compiler::SqlFunctionCatalog,
) -> Result<
    (
        crate::planner::logical::LogicalPlanNode,
        crate::column_id::ColumnRefFactory,
        Option<crate::planner::imv_rewrite::change_stream::ImvChangeStreamDescriptor>,
    ),
    String,
> {
    let is_aggregate_refresh = snapshot.schema_contract.aggregate.is_some();
    let factory_cell = Rc::new(RefCell::new(factory));
    let outcome = crate::planner::imv_rewrite::entrypoint::run_imv_rewrite(
        crate::planner::imv_rewrite::entrypoint::ImvRewriteInput {
            plan,
            snapshot: Arc::clone(snapshot),
            disabled_rules: join_incremental_disabled_rules(is_aggregate_refresh),
            deadline: None,
            column_ref_factory: Rc::clone(&factory_cell),
            #[cfg(not(test))]
            function_catalog: functions.snapshot(),
        },
    )
    .map_err(|error| format!("join refresh logical rewrite: {error}"))?;
    let mut factory = Rc::try_unwrap(factory_cell)
        .map_err(|_| "IMV rewrite leaked ColumnRefFactory references".to_string())?
        .into_inner();
    let mut change_stream_override = None;
    let plan = match mode {
        SqlMvJoinIncrementalRefreshMode::AppendOnly => outcome.plan,
        SqlMvJoinIncrementalRefreshMode::Coalesce if is_aggregate_refresh => outcome.plan,
        SqlMvJoinIncrementalRefreshMode::Coalesce => {
            let descriptor = outcome
                .annotation
                .change_stream
                .join_refresh
                .clone()
                .ok_or_else(|| {
                    format!(
                        "iceberg join MV {} incremental refresh rewrite did not produce join refresh descriptor",
                        snapshot.target.fqn()
                    )
                })?;
            descriptor.validate().map_err(|error| {
                format!(
                    "iceberg join MV {} incremental refresh descriptor is invalid: {error}",
                    snapshot.target.fqn()
                )
            })?;
            change_stream_override = Some(
                crate::planner::imv_rewrite::change_stream::ImvChangeStreamDescriptor {
                    aggregate: None,
                    join_refresh: Some(descriptor.clone()),
                },
            );
            let locator_columns =
                allocate_join_incremental_locator_column_ids(&mut factory, &outcome.plan)?;
            crate::planner::imv_rewrite::join_refresh_builder::build_join_delta_coalesce_plan_with_locator(
                outcome.plan,
                &descriptor,
                &crate::planner::imv_rewrite::join_refresh_builder::JoinRefreshTargetLocatorBinding::from_snapshot(snapshot),
                &mut factory,
                locator_columns.net,
                locator_columns.file,
                locator_columns.pos,
                locator_columns.row_id,
                locator_columns.last_updated_sequence_number,
                #[cfg(not(test))]
                functions,
            )
            .map_err(|error| format!("build join refresh coalesce logical plan: {error}"))?
        }
    };
    reserve_factory_for_plan(&mut factory, &plan)?;
    Ok((plan, factory, change_stream_override))
}

fn join_incremental_disabled_rules(is_aggregate_refresh: bool) -> Vec<String> {
    let mut disabled_rules =
        crate::optimizer::options::SessionOptimizerSettings::default().disabled_rules;
    if !disabled_rules
        .iter()
        .any(|rule| rule == "InjectTargetLocatorJoin")
    {
        disabled_rules.push("InjectTargetLocatorJoin".to_string());
    }
    if is_aggregate_refresh
        && !disabled_rules
            .iter()
            .any(|rule| rule == "RecordJoinRefreshDescriptor")
    {
        disabled_rules.push("RecordJoinRefreshDescriptor".to_string());
    }
    disabled_rules
}

struct JoinIncrementalLocatorColumnIds {
    net: u32,
    file: u32,
    pos: u32,
    row_id: u32,
    last_updated_sequence_number: u32,
}

fn allocate_join_incremental_locator_column_ids(
    factory: &mut crate::column_id::ColumnRefFactory,
    plan: &crate::planner::logical::LogicalPlanNode,
) -> Result<JoinIncrementalLocatorColumnIds, String> {
    reserve_factory_for_plan(factory, plan)?;
    Ok(JoinIncrementalLocatorColumnIds {
        net: factory
            .create(
                None,
                "net".to_string(),
                arrow::datatypes::DataType::Int64,
                false,
            )
            .0,
        file: factory
            .create(
                None,
                "_file".to_string(),
                arrow::datatypes::DataType::Utf8,
                true,
            )
            .0,
        pos: factory
            .create(
                None,
                "_pos".to_string(),
                arrow::datatypes::DataType::Int64,
                true,
            )
            .0,
        row_id: factory
            .create(
                None,
                "_row_id".to_string(),
                arrow::datatypes::DataType::Int64,
                true,
            )
            .0,
        last_updated_sequence_number: factory
            .create(
                None,
                "_last_updated_sequence_number".to_string(),
                arrow::datatypes::DataType::Int64,
                true,
            )
            .0,
    })
}

const JOIN_INCREMENTAL_EFFECT_COLUMN: &str = "__imv_change_stream_effect";
const JOIN_INCREMENTAL_EFFECT_EXISTING: i32 = 1;
const JOIN_INCREMENTAL_EFFECT_APPENDED: i32 = 2;

#[derive(Clone, Copy)]
enum JoinIncrementalEffectMode {
    Constant(i32),
    ByRowLineage,
}

fn add_join_incremental_change_stream_effect(
    optimized_tree: crate::optimizer::OptimizedOperatorNode,
    change_stream: &crate::planner::imv_rewrite::change_stream::ImvChangeStreamDescriptor,
    write_mode: SqlMvIncrementalWriteMode,
) -> Result<crate::optimizer::OptimizedOperatorNode, String> {
    let output_columns = &optimized_tree.output_columns;
    let has_delete_branch = matches!(write_mode, SqlMvIncrementalWriteMode::RowDelta);
    let action_output = has_delete_branch
        .then(|| join_incremental_change_op_output(change_stream, output_columns))
        .transpose()?;
    let effect_mode = match write_mode {
        SqlMvIncrementalWriteMode::FastAppend => {
            JoinIncrementalEffectMode::Constant(JOIN_INCREMENTAL_EFFECT_APPENDED)
        }
        SqlMvIncrementalWriteMode::RowDelta => JoinIncrementalEffectMode::ByRowLineage,
    };
    let row_lineage_output = match effect_mode {
        JoinIncrementalEffectMode::Constant(_) => None,
        JoinIncrementalEffectMode::ByRowLineage => Some(
            join_incremental_output_by_name(
                output_columns,
                "_file",
                "reuse/fresh route target locator column",
            )?
            .clone(),
        ),
    };
    let route_output = crate::analysis::OutputColumn {
        column_id: crate::column_id::ColumnId(
            output_columns
                .iter()
                .map(|column| column.column_id.0)
                .max()
                .unwrap_or(0)
                + 1,
        ),
        name: JOIN_INCREMENTAL_EFFECT_COLUMN.to_string(),
        data_type: arrow::datatypes::DataType::Int8,
        nullable: false,
        is_internal: true,
    };
    let mut arena = optimized_tree
        .execution_props
        .scalar_arena
        .as_ref()
        .ok_or_else(|| "IMV change-stream route projection requires a scalar arena".to_string())?
        .as_ref()
        .clone();
    let mut items = Vec::with_capacity(output_columns.len() + 1);
    for column in output_columns {
        arena.remember_source_column_display(column.column_id, None, column.name.clone());
        let expr = arena.intern(
            crate::optimizer::scalar::ScalarNode::ColumnRef(column.column_id),
            column.data_type.clone(),
            column.nullable,
        );
        items.push(crate::optimizer::operator::ScalarProjectItem {
            expr,
            output_name: column.name.clone(),
            output_column_id: column.column_id,
            expr_display: None,
        });
    }
    let effect_expr = join_incremental_effect_scalar(
        &mut arena,
        action_output.as_ref(),
        row_lineage_output.as_ref(),
        effect_mode,
    )?;
    arena.remember_project_output_display(route_output.column_id, None, route_output.name.clone());
    items.push(crate::optimizer::operator::ScalarProjectItem {
        expr: effect_expr,
        output_name: route_output.name.clone(),
        output_column_id: route_output.column_id,
        expr_display: None,
    });
    let output_property = optimized_tree.execution_props.output_property.clone();
    let stats = optimized_tree.stats.clone();
    let mut output_columns = output_columns.clone();
    output_columns.push(route_output);
    let arena = Arc::new(arena);
    let mut plan = crate::optimizer::OptimizedOperatorNode {
        op: crate::optimizer::operator::Operator::PhysicalProject(
            crate::optimizer::operator::ProjectOp {
                items,
                output_qualifier: None,
            },
        ),
        children: vec![optimized_tree],
        stats,
        explain_stats: crate::optimizer::optimized_tree::OptimizerExplainStats::default(),
        output_columns,
        execution_props: crate::optimizer::optimized_tree::PlanExecutionProps {
            output_property: output_property.clone(),
            child_output_properties: vec![output_property],
            join_distribution: None,
            scalar_arena: Some(Arc::clone(&arena)),
        },
    };
    crate::optimizer::optimized_tree::attach_scalar_arena(&mut plan, arena);
    Ok(plan)
}

fn join_incremental_change_op_output(
    change_stream: &crate::planner::imv_rewrite::change_stream::ImvChangeStreamDescriptor,
    output_columns: &[crate::analysis::OutputColumn],
) -> Result<crate::analysis::OutputColumn, String> {
    if let Some(aggregate) = change_stream.aggregate() {
        return join_incremental_output_by_column_id(
            output_columns,
            aggregate.action_column_id,
            "aggregate change-stream action column",
        )
        .cloned();
    }
    if let Some(join) = change_stream.join_refresh.as_ref() {
        return join_incremental_output_by_column_id(
            output_columns,
            join.action_column.column_id,
            "join change-stream action column",
        )
        .cloned();
    }
    join_incremental_output_by_name(
        output_columns,
        crate::common::CHANGE_OP_COLUMN,
        "change-stream action column",
    )
    .cloned()
}

fn join_incremental_output_by_column_id<'a>(
    output_columns: &'a [crate::analysis::OutputColumn],
    column_id: crate::column_id::ColumnId,
    label: &str,
) -> Result<&'a crate::analysis::OutputColumn, String> {
    let matches = output_columns
        .iter()
        .filter(|column| column.column_id == column_id)
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [column] => Ok(column),
        [] => Err(format!(
            "IMV change-stream {label} ColumnId({}) not found",
            column_id.0
        )),
        _ => Err(format!(
            "IMV change-stream {label} ColumnId({}) is ambiguous",
            column_id.0
        )),
    }
}

fn join_incremental_output_by_name<'a>(
    output_columns: &'a [crate::analysis::OutputColumn],
    name: &str,
    label: &str,
) -> Result<&'a crate::analysis::OutputColumn, String> {
    let matches = output_columns
        .iter()
        .filter(|column| column.name.eq_ignore_ascii_case(name))
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [column] => Ok(column),
        [] => Err(format!(
            "IMV change-stream {label} `{name}` not found in plan output"
        )),
        _ => Err(format!(
            "IMV change-stream {label} `{name}` is ambiguous in plan output"
        )),
    }
}

fn join_incremental_effect_scalar(
    arena: &mut crate::optimizer::scalar::ScalarArena,
    action_output: Option<&crate::analysis::OutputColumn>,
    row_lineage_output: Option<&crate::analysis::OutputColumn>,
    mode: JoinIncrementalEffectMode,
) -> Result<crate::optimizer::scalar::ScalarId, String> {
    use crate::common::{BinOp, CHANGE_OP_DELETE, LiteralValue};
    use crate::optimizer::scalar::{HashableLiteral, ScalarNode};

    let route_value = match mode {
        JoinIncrementalEffectMode::Constant(value) => arena.intern(
            ScalarNode::Literal(HashableLiteral(LiteralValue::Int(
                incremental_route_effect_code(value),
            ))),
            arrow::datatypes::DataType::Int8,
            false,
        ),
        JoinIncrementalEffectMode::ByRowLineage => {
            let lineage = row_lineage_output.ok_or_else(|| {
                "IMV reuse/fresh route requires preserved row-lineage output".to_string()
            })?;
            let lineage_ref = arena.intern(
                ScalarNode::ColumnRef(lineage.column_id),
                lineage.data_type.clone(),
                lineage.nullable,
            );
            let is_fresh = arena.intern(
                ScalarNode::IsNull {
                    child: lineage_ref,
                    negated: false,
                },
                arrow::datatypes::DataType::Boolean,
                false,
            );
            let fresh = arena.intern(
                ScalarNode::Literal(HashableLiteral(LiteralValue::Int(3))),
                arrow::datatypes::DataType::Int8,
                false,
            );
            let existing = arena.intern(
                ScalarNode::Literal(HashableLiteral(LiteralValue::Int(2))),
                arrow::datatypes::DataType::Int8,
                false,
            );
            arena.intern(
                ScalarNode::Case {
                    operand: None,
                    when_then: vec![(is_fresh, fresh)],
                    else_expr: Some(existing),
                },
                arrow::datatypes::DataType::Int8,
                false,
            )
        }
    };
    let Some(action) = action_output else {
        return Ok(route_value);
    };
    let action_ref = arena.intern(
        ScalarNode::ColumnRef(action.column_id),
        action.data_type.clone(),
        action.nullable,
    );
    let delete = arena.intern(
        ScalarNode::Literal(HashableLiteral(LiteralValue::Int(CHANGE_OP_DELETE as i64))),
        action.data_type.clone(),
        false,
    );
    let is_delete = arena.intern(
        ScalarNode::BinaryOp {
            op: BinOp::Eq,
            left: action_ref,
            right: delete,
        },
        arrow::datatypes::DataType::Boolean,
        action.nullable,
    );
    let delete_effect = arena.intern(
        ScalarNode::Literal(HashableLiteral(LiteralValue::Int(1))),
        arrow::datatypes::DataType::Int8,
        false,
    );
    Ok(arena.intern(
        ScalarNode::Case {
            operand: None,
            when_then: vec![(is_delete, delete_effect)],
            else_expr: Some(route_value),
        },
        arrow::datatypes::DataType::Int8,
        false,
    ))
}

const fn incremental_route_effect_code(route: i32) -> i64 {
    match route {
        JOIN_INCREMENTAL_EFFECT_EXISTING => 2,
        JOIN_INCREMENTAL_EFFECT_APPENDED => 3,
        _ => 1,
    }
}

/// Deliberately builds a plain `LogicalOnly` request.  The sealed rewrite
/// snapshot is consumed only after canonical planning to construct the join
/// append descriptor; injecting it here would silently change the prior Core
/// canonical-query semantics.
#[expect(
    clippy::too_many_arguments,
    reason = "These are distinct frozen SQL planning facts and grouping them would obscure the compiler boundary."
)]
fn plain_join_first_refresh_logical_request<'a>(
    query: ast::Query,
    current_catalog: Option<String>,
    current_database: String,
    optimizer_settings: crate::compiler::SessionOptimizerSettings,
    environment: crate::compiler::SqlPlanningEnvironment,
    catalog: &'a dyn crate::compiler::SqlCatalogSnapshot,
    functions: &'a dyn crate::compiler::SqlFunctionCatalog,
    constant_evaluator: &'static dyn crate::compiler::SqlConstantEvaluator,
    control: crate::compiler::SqlCompileControl,
) -> crate::compiler::SqlAnalyzeRequest<'a> {
    crate::compiler::SqlAnalyzeRequest::new(
        crate::compiler::SqlStatementInput::parsed_query(Box::new(query)),
        crate::compiler::SqlCompileIntent::LogicalOnly,
        crate::compiler::SqlSessionContext {
            current_catalog,
            current_database,
            optimizer_settings,
        },
        environment,
        catalog,
        functions,
        constant_evaluator,
        None,
        control,
    )
}

fn build_join_first_refresh_append_logical_plan(
    plan: crate::planner::logical::LogicalPlanNode,
    mut factory: crate::column_id::ColumnRefFactory,
    snapshot: &crate::compiler::mv_rewrite::SqlImvRewriteSnapshot,
) -> Result<
    (
        crate::planner::logical::LogicalPlanNode,
        crate::column_id::ColumnRefFactory,
    ),
    String,
> {
    let (left, right) = join_base_snapshots(snapshot)?;
    let crate::planner::logical::LogicalPlanNode {
        kind, mut children, ..
    } = plan;
    let crate::planner::logical::LogicalPlanKind::Project(mut project) = kind else {
        return Err("join first-refresh requires a root Project".to_string());
    };
    if children.len() != 1 {
        return Err(format!(
            "join first-refresh root Project expected one input, got {}",
            children.len()
        ));
    }
    let input = children.remove(0);
    let payload_columns = project
        .items
        .iter()
        .map(|item| crate::analysis::OutputColumn {
            column_id: item.output_column_id,
            name: item.output_name.clone(),
            data_type: item.expr.data_type.clone(),
            nullable: item.expr.nullable,
            is_internal: false,
        })
        .collect::<Vec<_>>();
    validate_join_payload(snapshot, &payload_columns)?;
    let left_scan = find_unique_base_scan(&input, &left.table, "left")?;
    let right_scan = find_unique_base_scan(&input, &right.table, "right")?;
    let left_row_id = find_row_id_column(&left_scan, "left")?;
    let right_row_id = find_row_id_column(&right_scan, "right")?;
    let key_pairs = join_key_pairs(snapshot, &left.table, &right.table, &left_scan, &right_scan)?;
    project.items.push(project_item(&left_row_id));
    project.items.push(project_item(&right_row_id));
    let input = crate::planner::logical::LogicalPlanNode::new(
        crate::planner::logical::LogicalPlanKind::Project(project),
        vec![input],
        None,
    );
    reserve_factory_for_plan(&mut factory, &input)?;
    let join_apply_key_id = factory.create(
        None,
        "__nova_join_row_key".to_string(),
        arrow::datatypes::DataType::Utf8,
        false,
    );
    let action_id = factory.create(
        None,
        crate::common::CHANGE_OP_COLUMN.to_string(),
        arrow::datatypes::DataType::Int8,
        false,
    );
    let join_apply_key = output_column(
        join_apply_key_id,
        "__nova_join_row_key",
        arrow::datatypes::DataType::Utf8,
        false,
        true,
    );
    let action = output_column(
        action_id,
        crate::common::CHANGE_OP_COLUMN,
        arrow::datatypes::DataType::Int8,
        false,
        true,
    );
    let descriptor = build_join_descriptor(
        snapshot,
        &left.table,
        &right.table,
        payload_columns,
        left_row_id,
        right_row_id,
        action,
        join_apply_key,
        key_pairs,
    )?;
    descriptor
        .validate()
        .map_err(|error| format!("join first-refresh descriptor is invalid: {error}"))?;
    let plan =
        crate::planner::imv_rewrite::join_refresh_builder::build_join_apply_key_append_project(
            input,
            &descriptor,
            &left.table_object_id,
            &right.table_object_id,
            join_apply_key_id.0,
        )
        .map_err(|error| format!("build join first-refresh append projection: {error}"))?;
    reserve_factory_for_plan(&mut factory, &plan)?;
    Ok((plan, factory))
}

fn join_base_snapshots(
    snapshot: &crate::compiler::mv_rewrite::SqlImvRewriteSnapshot,
) -> Result<
    (
        &crate::compiler::mv_rewrite::SqlImvBaseSnapshot,
        &crate::compiler::mv_rewrite::SqlImvBaseSnapshot,
    ),
    String,
> {
    let predicate = snapshot
        .schema_contract
        .join
        .as_ref()
        .and_then(|join| join.predicates.first())
        .ok_or_else(|| "join first-refresh snapshot has no join predicate facts".to_string())?;
    let left = snapshot
        .base_snapshots
        .iter()
        .find(|base| {
            base.table
                .fqn()
                .eq_ignore_ascii_case(&predicate.left.table_fqn)
        })
        .ok_or_else(|| {
            "join first-refresh left base is absent from the sealed snapshot".to_string()
        })?;
    let right = snapshot
        .base_snapshots
        .iter()
        .find(|base| {
            base.table
                .fqn()
                .eq_ignore_ascii_case(&predicate.right.table_fqn)
        })
        .ok_or_else(|| {
            "join first-refresh right base is absent from the sealed snapshot".to_string()
        })?;
    if left.table.fqn().eq_ignore_ascii_case(&right.table.fqn()) {
        return Err("join first-refresh requires distinct left and right bases".to_string());
    }
    Ok((left, right))
}

fn validate_join_payload(
    snapshot: &crate::compiler::mv_rewrite::SqlImvRewriteSnapshot,
    payload_columns: &[crate::analysis::OutputColumn],
) -> Result<(), String> {
    let expected = &snapshot.schema_contract.target.visible_columns;
    if payload_columns.len() != expected.len() {
        return Err(
            "join first-refresh payload count does not match the sealed target contract"
                .to_string(),
        );
    }
    for (actual, expected) in payload_columns.iter().zip(expected) {
        if !actual.name.eq_ignore_ascii_case(&expected.output_name) {
            return Err(format!(
                "join first-refresh payload column `{}` does not match target `{}`",
                actual.name, expected.output_name
            ));
        }
    }
    Ok(())
}

#[derive(Clone)]
struct JoinBaseScan {
    columns: Vec<crate::analysis::OutputColumn>,
}

fn find_unique_base_scan(
    plan: &crate::planner::logical::LogicalPlanNode,
    base: &novarocks_types::naming::TableIdentity,
    role: &str,
) -> Result<JoinBaseScan, String> {
    let mut scans = Vec::new();
    collect_base_scans(plan, base, &mut scans);
    match scans.as_slice() {
        [scan] => Ok(scan.clone()),
        [] => Err(format!(
            "join first-refresh cannot find {role} base scan {}",
            base.fqn()
        )),
        _ => Err(format!(
            "join first-refresh found multiple {role} base scans {}",
            base.fqn()
        )),
    }
}

fn collect_base_scans(
    plan: &crate::planner::logical::LogicalPlanNode,
    base: &novarocks_types::naming::TableIdentity,
    scans: &mut Vec<JoinBaseScan>,
) {
    if let crate::planner::logical::LogicalPlanKind::Scan(scan) = &plan.kind
        && let crate::planner::table::ScanSource::Sql(source) = &scan.table.source
        && source.table.catalog.eq_ignore_ascii_case(&base.catalog)
        && source.table.namespace.eq_ignore_ascii_case(&base.namespace)
        && source.table.table.eq_ignore_ascii_case(&base.table)
    {
        scans.push(JoinBaseScan {
            columns: scan.columns.clone(),
        });
    }
    for child in &plan.children {
        collect_base_scans(child, base, scans);
    }
}

fn find_row_id_column(
    scan: &JoinBaseScan,
    role: &str,
) -> Result<crate::analysis::OutputColumn, String> {
    let column = find_unique_column(
        &scan.columns,
        crate::common::ICEBERG_ROW_ID_COL,
        &format!("{role} row-id"),
    )?;
    if column.data_type != arrow::datatypes::DataType::Int64 || column.nullable {
        return Err(format!(
            "join first-refresh {role} row-id has invalid shape"
        ));
    }
    Ok(output_column(
        column.column_id,
        crate::common::ICEBERG_ROW_ID_COL,
        arrow::datatypes::DataType::Int64,
        false,
        true,
    ))
}

fn join_key_pairs(
    snapshot: &crate::compiler::mv_rewrite::SqlImvRewriteSnapshot,
    left: &novarocks_types::naming::TableIdentity,
    right: &novarocks_types::naming::TableIdentity,
    left_scan: &JoinBaseScan,
    right_scan: &JoinBaseScan,
) -> Result<Vec<crate::planner::imv_rewrite::join_refresh_descriptor::JoinRefreshJoinKeyPair>, String>
{
    let join = snapshot
        .schema_contract
        .join
        .as_ref()
        .ok_or_else(|| "join first-refresh snapshot has no join contract".to_string())?;
    join.predicates
        .iter()
        .map(|predicate| {
            let (left_lineage, right_lineage) =
                if predicate.left.table_fqn.eq_ignore_ascii_case(&left.fqn())
                    && predicate.right.table_fqn.eq_ignore_ascii_case(&right.fqn())
                {
                    (&predicate.left, &predicate.right)
                } else if predicate.left.table_fqn.eq_ignore_ascii_case(&right.fqn())
                    && predicate.right.table_fqn.eq_ignore_ascii_case(&left.fqn())
                {
                    (&predicate.right, &predicate.left)
                } else {
                    return Err(
                        "join first-refresh predicate does not align with sealed bases".to_string(),
                    );
                };
            let left_name = base_field_name(snapshot, &left.fqn(), left_lineage.field_id)?;
            let right_name = base_field_name(snapshot, &right.fqn(), right_lineage.field_id)?;
            Ok(
                crate::planner::imv_rewrite::join_refresh_descriptor::JoinRefreshJoinKeyPair {
                    left_column: find_unique_column(
                        &left_scan.columns,
                        &left_name,
                        "left join key",
                    )?,
                    right_column: find_unique_column(
                        &right_scan.columns,
                        &right_name,
                        "right join key",
                    )?,
                },
            )
        })
        .collect()
}

fn base_field_name(
    snapshot: &crate::compiler::mv_rewrite::SqlImvRewriteSnapshot,
    table_fqn: &str,
    field_id: i32,
) -> Result<String, String> {
    snapshot
        .schema_contract
        .bases
        .iter()
        .find(|base| base.table_fqn.eq_ignore_ascii_case(table_fqn))
        .and_then(|base| base.fields.iter().find(|field| field.field_id == field_id))
        .map(|field| field.name_at_create.clone())
        .ok_or_else(|| {
            format!(
                "join first-refresh lineage references unknown base field {table_fqn}#{field_id}"
            )
        })
}

#[expect(
    clippy::too_many_arguments,
    reason = "These are distinct frozen SQL planning facts and grouping them would obscure the compiler boundary."
)]
fn build_join_descriptor(
    snapshot: &crate::compiler::mv_rewrite::SqlImvRewriteSnapshot,
    left: &novarocks_types::naming::TableIdentity,
    right: &novarocks_types::naming::TableIdentity,
    payload_columns: Vec<crate::analysis::OutputColumn>,
    left_row_id_column: crate::analysis::OutputColumn,
    right_row_id_column: crate::analysis::OutputColumn,
    action_column: crate::analysis::OutputColumn,
    join_apply_key_column: crate::analysis::OutputColumn,
    join_key_pairs: Vec<
        crate::planner::imv_rewrite::join_refresh_descriptor::JoinRefreshJoinKeyPair,
    >,
) -> Result<crate::planner::imv_rewrite::join_refresh_descriptor::JoinRefreshDescriptor, String> {
    use crate::planner::imv_rewrite::join_refresh_descriptor as descriptor;
    let mut output_mappings = payload_columns
        .iter()
        .map(|column| descriptor::JoinRefreshOutputMapping {
            mv_output_column: column.clone(),
            source: descriptor::JoinRefreshOutputSource::Payload(column.column_id),
        })
        .collect::<Vec<_>>();
    output_mappings.push(descriptor::JoinRefreshOutputMapping {
        mv_output_column: join_apply_key_column.clone(),
        source: descriptor::JoinRefreshOutputSource::JoinApplyKey(join_apply_key_column.column_id),
    });
    output_mappings.push(descriptor::JoinRefreshOutputMapping {
        mv_output_column: action_column.clone(),
        source: descriptor::JoinRefreshOutputSource::Action(action_column.column_id),
    });
    Ok(descriptor::JoinRefreshDescriptor {
        mode: descriptor::JoinRefreshMode::Full,
        mv_identity: descriptor::JoinRefreshMvIdentity {
            catalog: snapshot.target.catalog.clone(),
            database: snapshot.target.namespace.clone(),
            name: snapshot.target.table.clone(),
        },
        left_base_fqn: left.fqn(),
        right_base_fqn: right.fqn(),
        left_row_id_column,
        right_row_id_column,
        action_column,
        join_apply_key_column,
        payload_columns,
        join_key_pairs,
        output_mappings,
        branches: Vec::new(),
        needs_target_locator: false,
    })
}

fn find_unique_column(
    columns: &[crate::analysis::OutputColumn],
    name: &str,
    role: &str,
) -> Result<crate::analysis::OutputColumn, String> {
    let matches = columns
        .iter()
        .filter(|column| column.name.eq_ignore_ascii_case(name))
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [column] => Ok((*column).clone()),
        [] => Err(format!(
            "join first-refresh cannot find {role} column {name}"
        )),
        _ => Err(format!(
            "join first-refresh found multiple {role} columns named {name}"
        )),
    }
}

fn project_item(column: &crate::analysis::OutputColumn) -> crate::analysis::ProjectItem {
    crate::analysis::ProjectItem {
        expr: crate::analysis::TypedExpr {
            kind: crate::analysis::ExprKind::ColumnRef {
                column_id: column.column_id,
                qualifier: None,
                column: column.name.clone(),
            },
            data_type: column.data_type.clone(),
            nullable: column.nullable,
        },
        output_name: column.name.clone(),
        output_column_id: column.column_id,
    }
}

fn output_column(
    column_id: crate::column_id::ColumnId,
    name: &str,
    data_type: arrow::datatypes::DataType,
    nullable: bool,
    is_internal: bool,
) -> crate::analysis::OutputColumn {
    crate::analysis::OutputColumn {
        column_id,
        name: name.to_string(),
        data_type,
        nullable,
        is_internal,
    }
}

fn reserve_factory_for_plan(
    factory: &mut crate::column_id::ColumnRefFactory,
    plan: &crate::planner::logical::LogicalPlanNode,
) -> Result<(), String> {
    let mut max_id = crate::planner::plan_output_columns(plan)?
        .iter()
        .map(|column| column.column_id.0)
        .max()
        .unwrap_or(0);
    for child in &plan.children {
        max_id = max_id.max(max_plan_column_id(child)?);
    }
    factory.reserve_until(max_id.saturating_add(1));
    Ok(())
}

fn max_plan_column_id(plan: &crate::planner::logical::LogicalPlanNode) -> Result<u32, String> {
    let mut max_id = crate::planner::plan_output_columns(plan)?
        .iter()
        .map(|column| column.column_id.0)
        .max()
        .unwrap_or(0);
    for child in &plan.children {
        max_id = max_id.max(max_plan_column_id(child)?);
    }
    Ok(max_id)
}

impl MvFirstRefreshPhysicalSql {
    pub(crate) fn sql(&self) -> &str {
        &self.sql
    }

    pub(crate) fn root_hash_column(&self) -> &str {
        &self.root_hash_column
    }
}

/// Validated logical shape of a first-refresh append.  All variants have one
/// empty target and therefore one sealed primary append cohort.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(
    dead_code,
    reason = "The validated first-refresh shapes are retained until application-side refresh admission consumes them."
)]
pub(crate) enum MvFirstRefreshShape {
    Projection,
    UnionProjection,
    Aggregate,
    FanInAggregate,
    BranchUnionAggregate,
    Join,
    JoinAggregate,
    ComposedAggregate,
}

/// Target facts frozen before a first-refresh writer is admitted.  It carries
/// Arrow schema and field identities, never an Iceberg table/client or a
/// provider decoder.
/// Opaque, value-only target facts for first-refresh SQL shaping.
///
/// This is deliberately not an IMV planner graph: Core may construct it from
/// already frozen target facts, but it contains neither provider authority nor
/// a mutable planner tree.
#[derive(Clone)]
pub struct MvFirstRefreshTargetContract {
    schema: SchemaRef,
    field_ids: Vec<i32>,
    partition_spec_id: i32,
    hidden_hash_key: String,
}

impl MvFirstRefreshTargetContract {
    pub fn try_new(
        schema: SchemaRef,
        field_ids: Vec<i32>,
        partition_spec_id: i32,
        hidden_hash_key: String,
    ) -> Result<Self, String> {
        if schema.fields().is_empty()
            || schema.fields().len() != field_ids.len()
            || field_ids.iter().any(|field_id| *field_id <= 0)
            || field_ids.iter().collect::<BTreeSet<_>>().len() != field_ids.len()
            || partition_spec_id < 0
            || hidden_hash_key.is_empty()
        {
            return Err("invalid MV first-refresh target physical contract".to_string());
        }
        Ok(Self {
            schema,
            field_ids,
            partition_spec_id,
            hidden_hash_key,
        })
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn field_ids(&self) -> &[i32] {
        &self.field_ids
    }

    pub const fn partition_spec_id(&self) -> i32 {
        self.partition_spec_id
    }

    pub fn hidden_hash_key(&self) -> &str {
        &self.hidden_hash_key
    }

    /// Verify provider-observed target facts before a deferred writer is
    /// activated. This is value-only so the SQL contract retains neither a
    /// catalog handle nor a provider codec.
    pub(crate) fn validate_observed(
        &self,
        schema: &Schema,
        field_ids: &[i32],
        partition_spec_id: i32,
    ) -> Result<(), String> {
        if schema != self.schema.as_ref()
            || field_ids != self.field_ids
            || partition_spec_id != self.partition_spec_id
        {
            return Err(
                "MV first-refresh target physical contract drifted after preparation".to_string(),
            );
        }
        if !self
            .schema
            .fields()
            .iter()
            .any(|field| field.name() == &self.hidden_hash_key)
        {
            return Err(
                "MV first-refresh target contract has no hidden hash key field".to_string(),
            );
        }
        Ok(())
    }

    fn validate_for_artifact(&self) -> Result<(), String> {
        self.validate_observed(
            self.schema.as_ref(),
            &self.field_ids,
            self.partition_spec_id,
        )
    }
}

/// Closed source-shaping choices for a first-refresh SQL artifact.
///
/// These values contain copied SQL syntax and Arrow type facts only.  They
/// cannot carry a planner tree, catalog/provider handle, lease, or lifecycle
/// state into the SQL compiler.
pub enum SqlMvFirstRefreshArtifactShape {
    Projection,
    UnionProjection {
        branch_count: usize,
    },
    Aggregate {
        calls: crate::planning::mv::SqlMvAggregateCalls,
        aggregate_input_types: Vec<Option<DataType>>,
    },
    FanInAggregate {
        calls: crate::planning::mv::SqlMvAggregateCalls,
        aggregate_input_types: Vec<Option<DataType>>,
    },
    BranchUnionAggregate {
        branch_count: usize,
        calls: crate::planning::mv::SqlMvAggregateCalls,
    },
}

/// Facts-only builder for the move-only first-refresh SQL artifact.
///
/// Core supplies immutable, already-admitted target and snapshot facts. SQL
/// alone selects the private state-shaping path and verifies that its root
/// distribution matches the frozen target contract.
pub struct SqlMvFirstRefreshArtifactBuilder {
    select_query: ast::Query,
    pin: SqlMvSnapshotPin,
    current_catalog: Option<String>,
    current_database: String,
    target_contract: MvFirstRefreshTargetContract,
    shape: SqlMvFirstRefreshArtifactShape,
}

impl SqlMvFirstRefreshArtifactBuilder {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        select_query: ast::Query,
        pin: SqlMvSnapshotPin,
        current_catalog: Option<String>,
        current_database: String,
        target_contract: MvFirstRefreshTargetContract,
        shape: SqlMvFirstRefreshArtifactShape,
    ) -> Result<Self, String> {
        if current_database.trim().is_empty() {
            return Err("invalid MV first-refresh artifact facts".to_string());
        }
        target_contract.validate_for_artifact()?;
        match &shape {
            SqlMvFirstRefreshArtifactShape::UnionProjection { branch_count }
            | SqlMvFirstRefreshArtifactShape::BranchUnionAggregate { branch_count, .. }
                if *branch_count == 0 =>
            {
                return Err("MV first-refresh branch count must be non-zero".to_string());
            }
            _ => {}
        }
        Ok(Self {
            select_query,
            pin,
            current_catalog,
            current_database,
            target_contract,
            shape,
        })
    }

    pub fn build(self) -> Result<SqlMvFirstRefreshArtifact, String> {
        let current_catalog = self.current_catalog.as_deref();
        let target_schema = self.target_contract.schema();
        let physical = match &self.shape {
            SqlMvFirstRefreshArtifactShape::Projection => {
                prepare_projection_first_refresh_write_sql(
                    &self.select_query,
                    &self.pin,
                    current_catalog,
                    &self.current_database,
                )?
            }
            SqlMvFirstRefreshArtifactShape::UnionProjection { branch_count } => {
                prepare_union_projection_first_refresh_write_sql(
                    &self.select_query,
                    *branch_count,
                    &self.pin,
                    current_catalog,
                    &self.current_database,
                )?
            }
            SqlMvFirstRefreshArtifactShape::Aggregate {
                calls,
                aggregate_input_types,
            } => {
                let calls = private_aggregate_calls(calls);
                prepare_aggregate_first_refresh_write_sql_with_target_schema_and_input_types(
                    &self.select_query,
                    &calls,
                    &self.pin,
                    current_catalog,
                    &self.current_database,
                    Some(target_schema),
                    Some(aggregate_input_types),
                )?
            }
            SqlMvFirstRefreshArtifactShape::FanInAggregate {
                calls,
                aggregate_input_types,
            } => {
                let calls = private_aggregate_calls(calls);
                prepare_fan_in_aggregate_first_refresh_write_sql_with_target_schema_and_input_types(
                    &self.select_query,
                    &calls,
                    &self.pin,
                    current_catalog,
                    &self.current_database,
                    Some(target_schema),
                    Some(aggregate_input_types),
                )?
            }
            SqlMvFirstRefreshArtifactShape::BranchUnionAggregate {
                branch_count,
                calls,
            } => {
                let calls = private_aggregate_calls(calls);
                prepare_branch_union_aggregate_first_refresh_write_sql_with_target_schema(
                    &self.select_query,
                    *branch_count,
                    &calls,
                    &self.pin,
                    current_catalog,
                    &self.current_database,
                    Some(target_schema),
                )?
            }
        };
        validate_root_distribution(
            &RootDistributionRequirement::ShuffleOutputName(
                physical.root_hash_column().to_string(),
            ),
            physical.root_hash_column(),
            self.target_contract.hidden_hash_key(),
        )?;
        Ok(physical)
    }
}

fn private_aggregate_calls(calls: &crate::planning::mv::SqlMvAggregateCalls) -> SqlAggregateCalls {
    use crate::mv_refresh::aggregate_shape::{
        SqlAggregateCall, SqlAggregateGroupKey, SqlAggregateInput,
    };
    use crate::planning::mv::AggregateInput;

    SqlAggregateCalls {
        group_keys: calls
            .group_keys
            .iter()
            .map(|key| SqlAggregateGroupKey {
                output_name: key.output_name.clone(),
                expr: key.expr.clone(),
            })
            .collect(),
        aggregates: calls
            .aggregates
            .iter()
            .map(|aggregate| SqlAggregateCall {
                output_name: aggregate.output_name.clone(),
                function: aggregate.function,
                input: match &aggregate.input {
                    AggregateInput::Star => SqlAggregateInput::Star,
                    AggregateInput::Expr(expr) => SqlAggregateInput::Expr(expr.clone()),
                },
            })
            .collect(),
        visible_outputs: calls.visible_outputs.clone(),
    }
}

pub(crate) fn prepare_projection_first_refresh_write_sql(
    select_query: &ast::Query,
    pin: &SqlMvSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<SqlMvFirstRefreshArtifact, String> {
    let sql =
        prepare_projection_full_read_sql(select_query, pin, current_catalog, current_database)?;
    Ok(SqlMvFirstRefreshArtifact::from_physical(
        MvFirstRefreshPhysicalSql {
            sql,
            root_hash_column: crate::planner::vocabulary::HIDDEN_APPLY_KEY_COLUMN_NAME.to_string(),
        },
    ))
}

pub(crate) fn prepare_union_projection_first_refresh_write_sql(
    select_query: &ast::Query,
    branch_count: usize,
    pin: &SqlMvSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<SqlMvFirstRefreshArtifact, String> {
    let sql = prepare_union_projection_full_read_sql(
        select_query,
        branch_count,
        pin,
        current_catalog,
        current_database,
    )?;
    Ok(SqlMvFirstRefreshArtifact::from_physical(
        MvFirstRefreshPhysicalSql {
            sql,
            root_hash_column: crate::planner::vocabulary::HIDDEN_APPLY_KEY_COLUMN_NAME.to_string(),
        },
    ))
}

#[allow(
    dead_code,
    reason = "The aggregate first-refresh SQL builder is retained for the staged application handoff."
)]
pub(crate) fn prepare_aggregate_first_refresh_write_sql(
    select_query: &ast::Query,
    calls: &SqlAggregateCalls,
    pin: &SqlMvSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<SqlMvFirstRefreshArtifact, String> {
    prepare_aggregate_first_refresh_write_sql_with_target_schema(
        select_query,
        calls,
        pin,
        current_catalog,
        current_database,
        None,
    )
}

#[allow(
    dead_code,
    reason = "The schema-aware aggregate builder is retained for the staged first-refresh handoff."
)]
pub(crate) fn prepare_aggregate_first_refresh_write_sql_with_target_schema(
    select_query: &ast::Query,
    calls: &SqlAggregateCalls,
    pin: &SqlMvSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
    target_schema: Option<&Schema>,
) -> Result<SqlMvFirstRefreshArtifact, String> {
    prepare_aggregate_first_refresh_write_sql_with_target_schema_and_input_types(
        select_query,
        calls,
        pin,
        current_catalog,
        current_database,
        target_schema,
        None,
    )
}

pub(crate) fn prepare_aggregate_first_refresh_write_sql_with_target_schema_and_input_types(
    select_query: &ast::Query,
    calls: &SqlAggregateCalls,
    pin: &SqlMvSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
    target_schema: Option<&Schema>,
    aggregate_input_types: Option<&[Option<DataType>]>,
) -> Result<SqlMvFirstRefreshArtifact, String> {
    let state_sql = prepare_aggregate_first_refresh_state_sql(
        select_query,
        calls,
        pin,
        current_catalog,
        current_database,
    )?;
    Ok(SqlMvFirstRefreshArtifact::from_physical(
        MvFirstRefreshPhysicalSql {
            sql: aggregate_physical_sql(
                &state_sql,
                calls,
                None,
                target_schema,
                aggregate_input_types,
            )?,
            root_hash_column: SQL_MV_ROW_ID_COLUMN.to_string(),
        },
    ))
}

/// Fan-in aggregate first refresh uses the same state-shaped physical project
/// as a single aggregate.  The canonical SELECT already contains the pinned
/// UNION ALL input, so keeping this as a separate entry point makes the shape
/// contract explicit without reintroducing a frontend materialization phase.
#[allow(
    dead_code,
    reason = "The fan-in aggregate builder is retained for the staged first-refresh handoff."
)]
pub(crate) fn prepare_fan_in_aggregate_first_refresh_write_sql(
    select_query: &ast::Query,
    calls: &SqlAggregateCalls,
    pin: &SqlMvSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<SqlMvFirstRefreshArtifact, String> {
    prepare_fan_in_aggregate_first_refresh_write_sql_with_target_schema(
        select_query,
        calls,
        pin,
        current_catalog,
        current_database,
        None,
    )
}

#[allow(
    dead_code,
    reason = "The schema-aware fan-in builder is retained for the staged first-refresh handoff."
)]
pub(crate) fn prepare_fan_in_aggregate_first_refresh_write_sql_with_target_schema(
    select_query: &ast::Query,
    calls: &SqlAggregateCalls,
    pin: &SqlMvSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
    target_schema: Option<&Schema>,
) -> Result<SqlMvFirstRefreshArtifact, String> {
    prepare_fan_in_aggregate_first_refresh_write_sql_with_target_schema_and_input_types(
        select_query,
        calls,
        pin,
        current_catalog,
        current_database,
        target_schema,
        None,
    )
}

pub(crate) fn prepare_fan_in_aggregate_first_refresh_write_sql_with_target_schema_and_input_types(
    select_query: &ast::Query,
    calls: &SqlAggregateCalls,
    pin: &SqlMvSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
    target_schema: Option<&Schema>,
    aggregate_input_types: Option<&[Option<DataType>]>,
) -> Result<SqlMvFirstRefreshArtifact, String> {
    prepare_aggregate_first_refresh_write_sql_with_target_schema_and_input_types(
        select_query,
        calls,
        pin,
        current_catalog,
        current_database,
        target_schema,
        aggregate_input_types,
    )
}

/// A composed aggregate (for example aggregate-over-join) is still one
/// state-shaped SELECT.  Its join/fan-in relationship lives below the common
/// aggregate project and therefore remains BE-owned all the way to the
/// connector writer.
#[allow(
    dead_code,
    reason = "The composed aggregate builder is retained for the staged first-refresh handoff."
)]
pub(crate) fn prepare_composed_aggregate_first_refresh_write_sql(
    select_query: &ast::Query,
    calls: &SqlAggregateCalls,
    pin: &SqlMvSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<SqlMvFirstRefreshArtifact, String> {
    prepare_aggregate_first_refresh_write_sql(
        select_query,
        calls,
        pin,
        current_catalog,
        current_database,
    )
}

#[allow(
    dead_code,
    reason = "The branch-union aggregate builder is retained for the staged first-refresh handoff."
)]
pub(crate) fn prepare_branch_union_aggregate_first_refresh_write_sql(
    select_query: &ast::Query,
    branch_count: usize,
    first_branch_calls: &SqlAggregateCalls,
    pin: &SqlMvSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<SqlMvFirstRefreshArtifact, String> {
    prepare_branch_union_aggregate_first_refresh_write_sql_with_target_schema(
        select_query,
        branch_count,
        first_branch_calls,
        pin,
        current_catalog,
        current_database,
        None,
    )
}

pub(crate) fn prepare_branch_union_aggregate_first_refresh_write_sql_with_target_schema(
    select_query: &ast::Query,
    branch_count: usize,
    first_branch_calls: &SqlAggregateCalls,
    pin: &SqlMvSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
    target_schema: Option<&Schema>,
) -> Result<SqlMvFirstRefreshArtifact, String> {
    let branches = prepare_branch_union_aggregate_first_refresh_state_sqls(
        select_query,
        branch_count,
        first_branch_calls,
        pin,
        current_catalog,
        current_database,
    )?;
    let sql = branches
        .into_iter()
        .enumerate()
        .map(|(branch_index, (calls, state_sql))| {
            validate_branch_aggregate_contract(branch_index, &calls, first_branch_calls)?;
            let branch_id = i32::try_from(branch_index).map_err(|_| {
                format!("MV first-refresh branch index {branch_index} exceeds Int32")
            })?;
            aggregate_physical_sql(&state_sql, &calls, Some(branch_id), target_schema, None)
        })
        .collect::<Result<Vec<_>, _>>()?
        .join(" UNION ALL ");
    Ok(SqlMvFirstRefreshArtifact::from_physical(
        MvFirstRefreshPhysicalSql {
            sql,
            root_hash_column: SQL_MV_ROW_ID_COLUMN.to_string(),
        },
    ))
}

fn prepare_aggregate_first_refresh_state_sql(
    select_query: &ast::Query,
    calls: &SqlAggregateCalls,
    pin: &SqlMvSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<String, String> {
    let state_query = rewrite_select_sql_for_state(select_query, calls)?;
    pin_state_sql(&state_query, pin, current_catalog, current_database)
}

fn prepare_branch_union_aggregate_first_refresh_state_sqls(
    select_query: &ast::Query,
    branch_count: usize,
    first_branch_calls: &SqlAggregateCalls,
    pin: &SqlMvSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<Vec<(SqlAggregateCalls, String)>, String> {
    branch_union_queries(select_query, branch_count)?
        .into_iter()
        .enumerate()
        .map(|(branch_index, (branch_query, _branch_sql))| {
            let branch_calls = SqlAggregateCalls::extract(&branch_query)?;
            if branch_index == 0 && &branch_calls != first_branch_calls {
                return Err(
                    "branch UNION ALL aggregate first branch calls drifted from the validated contract"
                        .to_string(),
                );
            }
            let state_sql = prepare_aggregate_first_refresh_state_sql(
                &branch_query,
                &branch_calls,
                pin,
                current_catalog,
                current_database,
            )?;
            Ok((branch_calls, state_sql))
        })
        .collect()
}

fn aggregate_physical_sql(
    state_sql: &str,
    calls: &SqlAggregateCalls,
    branch_id: Option<i32>,
    target_schema: Option<&Schema>,
    aggregate_input_types: Option<&[Option<DataType>]>,
) -> Result<String, String> {
    let mut projection = Vec::with_capacity(
        1 + calls.visible_outputs.len() + calls.aggregates.len() + usize::from(branch_id.is_some()),
    );
    let group_key_refs = calls
        .group_keys
        .iter()
        .map(|key| qualified_column("state", &key.output_name))
        .collect::<Vec<_>>();
    projection.push(format!(
        "mv_group_row_id({}) AS {}",
        group_key_refs.join(", "),
        quote_sql_identifier(SQL_MV_ROW_ID_COLUMN),
    ));

    for output in &calls.visible_outputs {
        match output {
            VisibleAggregateOutput::GroupKey(group_key_index) => {
                let key = calls.group_keys.get(*group_key_index).ok_or_else(|| {
                    format!("MV first-refresh group key index {group_key_index} out of range")
                })?;
                projection.push(format!(
                    "{} AS {}",
                    qualified_column("state", &key.output_name),
                    quote_sql_identifier(&key.output_name),
                ));
            }
            VisibleAggregateOutput::Aggregate(aggregate_index) => {
                let aggregate = calls.aggregates.get(*aggregate_index).ok_or_else(|| {
                    format!("MV first-refresh aggregate index {aggregate_index} out of range")
                })?;
                let state_name = state_column_name(&aggregate.output_name);
                let witness = if matches!(
                    aggregate.function,
                    AggregateFunctionKind::Sum
                        | AggregateFunctionKind::Min
                        | AggregateFunctionKind::Max
                ) {
                    target_schema
                        .and_then(|schema| {
                            schema
                                .fields()
                                .iter()
                                .find(|field| field.name() == &aggregate.output_name)
                        })
                        .map(|field| aggregate_visible_type_witness(field.data_type()))
                        .transpose()?
                } else {
                    None
                };
                let args = if aggregate.function == AggregateFunctionKind::Avg {
                    let input_type = aggregate_input_types
                        .and_then(|types| types.get(*aggregate_index))
                        .and_then(Option::as_ref);
                    let output_witness = target_schema
                        .and_then(|schema| {
                            schema
                                .fields()
                                .iter()
                                .find(|field| field.name() == &aggregate.output_name)
                        })
                        .map(|field| aggregate_visible_type_witness(field.data_type()))
                        .transpose()?;
                    match output_witness {
                        Some(witness) => {
                            let input_scale = match input_type {
                                Some(DataType::Decimal128(_, scale)) => i64::from(*scale),
                                _ => -1,
                            };
                            format!(
                                "{}, CAST({input_scale} AS BIGINT), {witness}",
                                qualified_column("state", &state_name)
                            )
                        }
                        None => qualified_column("state", &state_name),
                    }
                } else {
                    match witness {
                        Some(witness) => {
                            format!("{}, {witness}", qualified_column("state", &state_name))
                        }
                        None => qualified_column("state", &state_name),
                    }
                };
                projection.push(format!(
                    "{}({args}) AS {}",
                    aggregate_visible_function(aggregate.function),
                    quote_sql_identifier(&aggregate.output_name),
                ));
            }
        }
    }

    for aggregate in &calls.aggregates {
        let state_name = state_column_name(&aggregate.output_name);
        projection.push(format!(
            "{} AS {}",
            qualified_column("state", &state_name),
            quote_sql_identifier(&state_name),
        ));
    }
    if calls.needs_retraction_count_state() {
        projection.push(format!(
            "{} AS {}",
            qualified_column("state", SQL_MV_AGG_RETRACTION_COUNT_STATE_COLUMN),
            quote_sql_identifier(SQL_MV_AGG_RETRACTION_COUNT_STATE_COLUMN),
        ));
    }
    if let Some(branch_id) = branch_id {
        projection.push(format!(
            "CAST({branch_id} AS INT) AS {}",
            quote_sql_identifier(BRANCH_ID_COLUMN_NAME),
        ));
    }

    Ok(format!(
        "SELECT {} FROM ({state_sql}) AS state",
        projection.join(", "),
    ))
}

fn aggregate_visible_type_witness(data_type: &DataType) -> Result<String, String> {
    let sql_type = match data_type {
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Int8 => "TINYINT".to_string(),
        DataType::Int16 => "SMALLINT".to_string(),
        DataType::Int32 => "INT".to_string(),
        DataType::Int64 => "BIGINT".to_string(),
        DataType::Float32 => "FLOAT".to_string(),
        DataType::Float64 => "DOUBLE".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 => "STRING".to_string(),
        DataType::Date32 => "DATE".to_string(),
        DataType::Timestamp(_, _) => "DATETIME".to_string(),
        DataType::Decimal128(precision, scale) => format!("DECIMAL({precision},{scale})"),
        other => {
            return Err(format!(
                "unsupported MV aggregate visible target type {other:?}"
            ));
        }
    };
    Ok(format!("CAST(NULL AS {sql_type})"))
}

fn validate_branch_aggregate_contract(
    branch_index: usize,
    calls: &SqlAggregateCalls,
    expected: &SqlAggregateCalls,
) -> Result<(), String> {
    if calls.visible_outputs != expected.visible_outputs {
        return Err(format!(
            "MV first-refresh aggregate branch {branch_index} visible output order differs from branch 0"
        ));
    }
    if calls.group_keys.len() != expected.group_keys.len() {
        return Err(format!(
            "MV first-refresh aggregate branch {branch_index} group-key count differs from branch 0"
        ));
    }
    if calls.aggregates.len() != expected.aggregates.len() {
        return Err(format!(
            "MV first-refresh aggregate branch {branch_index} aggregate count differs from branch 0"
        ));
    }
    for (aggregate_index, (actual, expected)) in calls
        .aggregates
        .iter()
        .zip(expected.aggregates.iter())
        .enumerate()
    {
        if actual.function != expected.function {
            return Err(format!(
                "MV first-refresh aggregate branch {branch_index} aggregate {aggregate_index} function differs from branch 0"
            ));
        }
    }
    Ok(())
}

fn aggregate_visible_function(kind: AggregateFunctionKind) -> &'static str {
    match kind {
        AggregateFunctionKind::Count => "count_state_visible",
        AggregateFunctionKind::Sum => "sum_state_visible",
        AggregateFunctionKind::Avg => "avg_state_visible",
        AggregateFunctionKind::Min => "min_state_visible",
        AggregateFunctionKind::Max => "max_state_visible",
        AggregateFunctionKind::BoolOr => "bool_or_state_visible",
        AggregateFunctionKind::BoolAnd => "bool_and_state_visible",
        AggregateFunctionKind::CountDistinct => "count_distinct_state_visible",
        AggregateFunctionKind::ApproxCountDistinct => "approx_count_distinct_state_visible",
    }
}

fn qualified_column(qualifier: &str, column: &str) -> String {
    format!(
        "{}.{}",
        quote_sql_identifier(qualifier),
        quote_sql_identifier(column)
    )
}

fn quote_sql_identifier(identifier: &str) -> String {
    format!("`{}`", identifier.replace('`', "``"))
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};
    use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
    use std::sync::Arc;

    use super::*;

    fn parse_query(sql: &str) -> ast::Query {
        let statements = novarocks_parser::parse(sql).expect("parse query");
        let [ast::Statement::Query(query)] = statements.as_slice() else {
            panic!("fixture must be a query");
        };
        query.clone()
    }

    fn sqlx2_target_binding() -> SqlTableBindingId {
        SqlTableBindingId::new(
            crate::binding::SqlTableBindingScopeId::new(NonZeroU64::new(701).unwrap()),
            NonZeroU32::new(1).unwrap(),
        )
    }

    fn sqlx2_target_contract() -> MvFirstRefreshTargetContract {
        MvFirstRefreshTargetContract::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "__apply_key__",
                DataType::Utf8,
                false,
            )])),
            vec![1],
            0,
            "__apply_key__".to_string(),
        )
        .expect("valid SQL target contract")
    }

    struct CanonicalCatalog;

    impl crate::compiler::SqlCatalogSnapshot for CanonicalCatalog {
        fn planner_table_provider(&self) -> &dyn crate::catalog::PlannerTableProvider {
            panic!("plain canonical request construction must not resolve a catalog")
        }
    }

    #[derive(Debug)]
    struct CanonicalFunctions;

    impl crate::compiler::SqlFunctionCatalog for CanonicalFunctions {
        fn snapshot(&self) -> Arc<dyn crate::compiler::SqlFunctionCatalog> {
            Arc::new(Self)
        }

        fn resolve_scalar_signature(
            &self,
            _name: &str,
            _arg_types: &[arrow::datatypes::DataType],
        ) -> Result<crate::functions::ResolvedScalarFunction, crate::functions::ResolveError>
        {
            panic!("plain canonical request construction must not resolve functions")
        }

        fn contains_aggregate(&self, _name: &str) -> bool {
            panic!("plain canonical request construction must not classify functions")
        }

        fn resolve_aggregate_signature(
            &self,
            _name: &str,
            _arg_types: &[arrow::datatypes::DataType],
        ) -> Result<novarocks_functions::ResolvedAggregateSignature, crate::functions::ResolveError>
        {
            panic!("plain canonical request construction must not resolve functions")
        }

        fn resolve_aggregate_trusted(
            &self,
            _name: &str,
            _arg_types: &[arrow::datatypes::DataType],
        ) -> Result<novarocks_functions::ResolvedAggregateSignature, crate::functions::ResolveError>
        {
            panic!("plain canonical request construction must not resolve functions")
        }

        fn volatility(&self, _name: &str) -> crate::functions::FunctionVolatility {
            panic!("plain canonical request construction must not resolve functions")
        }
    }

    #[test]
    fn join_first_refresh_canonical_request_avoids_imv_rewrite_and_terminal_is_sealed() {
        let query = parse_query("SELECT 1");
        let catalog = CanonicalCatalog;
        let functions = CanonicalFunctions;
        let request = plain_join_first_refresh_logical_request(
            query,
            None,
            "db".to_string(),
            crate::compiler::SessionOptimizerSettings::default(),
            crate::compiler::SqlPlanningEnvironment::Distributed {
                backend_count: NonZeroUsize::new(1).expect("non-zero"),
            },
            &catalog,
            &functions,
            crate::compiler::noop_constant_evaluator(),
            crate::compiler::SqlCompileControl::unbounded(),
        );
        assert!(request.imv_rewrite.is_none());
        let _: fn(
            SqlMvJoinFirstRefreshAnalyzeContext<'_>,
        ) -> Result<SqlMvJoinFirstRefreshAnalyzed, String> =
            analyze_join_first_refresh_connector_write;
        let _: fn(
            SqlMvJoinFirstRefreshAnalyzed,
            &crate::planning::dml::DmlStatisticsSnapshot,
            &[novarocks_spi::connector::StatisticsRequiredAggregation],
            novarocks_spi::connector::write_stack::WriteTargetOrdinal,
        ) -> Result<crate::plan_read::DistributedPlan, String> =
            compile_join_first_refresh_connector_write_dataflow;
    }

    #[test]
    fn join_incremental_terminal_keeps_canonical_request_plain_and_sealed() {
        let query = parse_query("SELECT 1");
        let catalog = CanonicalCatalog;
        let functions = CanonicalFunctions;
        let request = plain_join_first_refresh_logical_request(
            query,
            None,
            "db".to_string(),
            crate::compiler::SessionOptimizerSettings::default(),
            crate::compiler::SqlPlanningEnvironment::Distributed {
                backend_count: NonZeroUsize::new(1).expect("non-zero"),
            },
            &catalog,
            &functions,
            crate::compiler::noop_constant_evaluator(),
            crate::compiler::SqlCompileControl::unbounded(),
        );
        assert!(request.imv_rewrite.is_none());
        let _: fn(
            SqlMvJoinIncrementalRefreshAnalyzeContext<'_>,
        ) -> Result<SqlMvJoinIncrementalRefreshAnalyzed, String> =
            analyze_join_incremental_refresh_change_stream;
        let _: fn(
            SqlMvJoinIncrementalRefreshAnalyzed,
            &crate::planning::dml::DmlStatisticsSnapshot,
            Vec<crate::planning::dml::DmlChangeStreamStatisticsTarget>,
            crate::planning::dml::DmlWritePlanShape,
        ) -> Result<crate::planning::dml::DmlChangeStreamPlan, String> =
            compile_join_incremental_refresh_change_stream;
    }

    #[test]
    fn canonical_incremental_terminal_enables_only_sealed_imv_rewrite() {
        let query = parse_query("SELECT 1");
        let input = crate::compiler::SqlImvPlanningInput::new(
            crate::compiler::mv_rewrite::test_incremental_snapshot_handle(),
            crate::compiler::SqlImvRewriteValidation::None,
        );
        let catalog = CanonicalCatalog;
        let functions = CanonicalFunctions;
        let request = canonical_incremental_change_stream_request(
            query,
            &input,
            None,
            "db".to_string(),
            crate::compiler::SqlPlanningEnvironment::Distributed {
                backend_count: NonZeroUsize::new(1).expect("non-zero"),
            },
            &catalog,
            &functions,
            crate::compiler::noop_constant_evaluator(),
            crate::compiler::SqlCompileControl::unbounded(),
        );
        assert!(request.imv_rewrite.is_some());
        assert_eq!(
            request
                .session
                .optimizer_settings
                .enable_global_runtime_filter,
            Some(false)
        );
        let _: fn(
            SqlMvIncrementalRefreshAnalyzeContext<'_>,
        ) -> Result<SqlMvIncrementalRefreshAnalyzed, String> =
            analyze_mv_incremental_refresh_change_stream;
        let _: fn(
            SqlMvIncrementalRefreshAnalyzed,
            &crate::planning::dml::DmlStatisticsSnapshot,
            Vec<crate::planning::dml::DmlChangeStreamStatisticsTarget>,
            crate::planning::dml::DmlWritePlanShape,
        ) -> Result<crate::planning::dml::DmlChangeStreamPlan, String> =
            compile_mv_incremental_refresh_change_stream;
    }

    #[test]
    fn canonical_incremental_terminal_aliases_aggregate_group_keys_inside_sql() {
        let mut query = parse_query("SELECT k, sum(v) FROM b GROUP BY k");
        let snapshot = crate::compiler::mv_rewrite::test_aggregate_snapshot(Vec::new(), None, None);
        alias_incremental_aggregate_group_key_projection(&mut query, &snapshot)
            .expect("sealed aggregate snapshot aliases the canonical query");
        let ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("fixture must retain SELECT body");
        };
        let ast::SelectItem::ExprWithAlias { alias, .. } = &select.projection[0] else {
            panic!("group key must have SQL-owned alias");
        };
        assert_eq!(alias.value, "k");
    }

    #[test]
    fn canonical_incremental_terminal_preserves_provider_effect_codes() {
        assert_eq!(incremental_route_effect_code(0), 1, "delete");
        assert_eq!(
            incremental_route_effect_code(JOIN_INCREMENTAL_EFFECT_EXISTING),
            2,
            "reuse"
        );
        assert_eq!(
            incremental_route_effect_code(JOIN_INCREMENTAL_EFFECT_APPENDED),
            3,
            "fresh"
        );
    }

    #[test]
    fn join_incremental_terminal_rejects_incomplete_route_facts() {
        let error = validate_join_incremental_routes(&[])
            .expect_err("no admitted route must fail closed before planning");
        assert!(error.contains("at least one admitted writer route"));
    }

    #[test]
    fn join_coalesce_locator_ids_reserve_rewritten_plan_outputs() {
        let child_output = crate::analysis::OutputColumn {
            column_id: crate::column_id::ColumnId(42),
            name: "child_k".to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        };
        let root_output = crate::analysis::OutputColumn {
            column_id: crate::column_id::ColumnId(6),
            name: "root_k".to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        };
        let child = crate::planner::logical::LogicalPlanNode::new(
            crate::planner::logical::LogicalPlanKind::Values(
                crate::planner::payload::PlanValuesNode {
                    rows: Vec::new(),
                    columns: vec![child_output.clone()],
                },
            ),
            Vec::new(),
            None,
        );
        let plan = crate::planner::logical::LogicalPlanNode::new(
            crate::planner::logical::LogicalPlanKind::Project(
                crate::planner::payload::PlanProjectNode {
                    items: vec![crate::analysis::ProjectItem {
                        expr: crate::analysis::TypedExpr {
                            kind: crate::analysis::ExprKind::ColumnRef {
                                column_id: child_output.column_id,
                                qualifier: None,
                                column: child_output.name.clone(),
                            },
                            data_type: child_output.data_type.clone(),
                            nullable: child_output.nullable,
                        },
                        output_name: root_output.name.clone(),
                        output_column_id: root_output.column_id,
                    }],
                    output_qualifier: None,
                },
            ),
            vec![child],
            None,
        );
        let mut factory = crate::column_id::ColumnRefFactory::new();
        let ids = allocate_join_incremental_locator_column_ids(&mut factory, &plan)
            .expect("allocate locator column ids");
        let allocated = [
            ids.net,
            ids.file,
            ids.pos,
            ids.row_id,
            ids.last_updated_sequence_number,
        ];
        assert!(allocated.iter().all(|id| *id > child_output.column_id.0));
        assert_eq!(
            allocated
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            allocated.len()
        );
    }

    #[test]
    fn join_coalesce_locator_factory_metadata_stays_registered() {
        let plan = crate::planner::logical::LogicalPlanNode::new(
            crate::planner::logical::LogicalPlanKind::Values(
                crate::planner::payload::PlanValuesNode {
                    rows: Vec::new(),
                    columns: vec![crate::analysis::OutputColumn {
                        column_id: crate::column_id::ColumnId(109),
                        name: "payload".to_string(),
                        data_type: DataType::Int64,
                        nullable: false,
                        is_internal: false,
                    }],
                },
            ),
            Vec::new(),
            None,
        );
        let mut factory = crate::column_id::ColumnRefFactory::new();
        let ids = allocate_join_incremental_locator_column_ids(&mut factory, &plan)
            .expect("allocate locator column ids");
        for (id, name, data_type, nullable) in [
            (ids.net, "net", DataType::Int64, false),
            (ids.file, "_file", DataType::Utf8, true),
            (ids.pos, "_pos", DataType::Int64, true),
            (ids.row_id, "_row_id", DataType::Int64, true),
            (
                ids.last_updated_sequence_number,
                "_last_updated_sequence_number",
                DataType::Int64,
                true,
            ),
        ] {
            let metadata = factory.get(crate::column_id::ColumnId(id));
            assert!(!metadata.name.starts_with("__reserved_col_"));
            assert_eq!(metadata.name, name);
            assert_eq!(metadata.data_type, data_type);
            assert_eq!(metadata.nullable, nullable);
        }
    }

    #[test]
    fn sqlx2_mv_first_refresh_plan_is_sql_only_and_binding_scoped() {
        let plan = SqlMvFirstRefreshPlanner::plan(SqlMvFirstRefreshPlannerInput {
            shape: MvFirstRefreshShape::Projection,
            target_contract: sqlx2_target_contract(),
            target_binding: sqlx2_target_binding(),
            root_distribution: RootDistributionRequirement::ShuffleOutputName(
                "__apply_key__".to_string(),
            ),
            artifact: SqlMvFirstRefreshArtifactInput::Sql(MvFirstRefreshPhysicalSql {
                sql: "SELECT 1 AS `__apply_key__`".to_string(),
                root_hash_column: "__apply_key__".to_string(),
            }),
        })
        .expect("pure SQL first-refresh plan");

        assert_eq!(plan.shape(), MvFirstRefreshShape::Projection);
        assert_eq!(plan.target_binding(), sqlx2_target_binding());
        assert_eq!(plan.target_contract().hidden_hash_key(), "__apply_key__");
        assert!(matches!(
            plan.into_artifact(),
            SqlMvFirstRefreshPlanArtifact::Sql(_)
        ));
    }

    #[test]
    fn sqlx2_mv_first_refresh_plan_rejects_implicit_or_wrong_distribution() {
        let make_input = |root_distribution| SqlMvFirstRefreshPlannerInput {
            shape: MvFirstRefreshShape::Projection,
            target_contract: sqlx2_target_contract(),
            target_binding: sqlx2_target_binding(),
            root_distribution,
            artifact: SqlMvFirstRefreshArtifactInput::Sql(MvFirstRefreshPhysicalSql {
                sql: "SELECT 1 AS `__apply_key__`".to_string(),
                root_hash_column: "__apply_key__".to_string(),
            }),
        };

        assert!(
            SqlMvFirstRefreshPlanner::plan(make_input(RootDistributionRequirement::Any)).is_err()
        );
        assert!(
            SqlMvFirstRefreshPlanner::plan(make_input(
                RootDistributionRequirement::ShuffleOutputName("other".to_string())
            ))
            .is_err()
        );
    }

    fn pin() -> SqlMvSnapshotPin {
        SqlMvSnapshotPin::from_entries_for_tests(&[("ice.db.fact", 42, "fact-uuid")])
    }

    fn aggregate_calls(sql: &str) -> crate::planning::mv::SqlMvAggregateCalls {
        crate::planning::mv::extract_aggregate_sql_calls(&parse_query(sql)).unwrap()
    }

    fn aggregate_target_contract() -> MvFirstRefreshTargetContract {
        MvFirstRefreshTargetContract::try_new(
            Arc::new(Schema::new(vec![
                Field::new(SQL_MV_ROW_ID_COLUMN, DataType::Utf8, false),
                Field::new("k", DataType::Int64, true),
                Field::new("total", DataType::Int64, true),
                Field::new("__agg_state_total", DataType::Binary, true),
                Field::new(
                    SQL_MV_AGG_RETRACTION_COUNT_STATE_COLUMN,
                    DataType::Binary,
                    true,
                ),
            ])),
            vec![1, 2, 3, 4, 5],
            0,
            SQL_MV_ROW_ID_COLUMN.to_string(),
        )
        .expect("valid aggregate target contract")
    }

    fn projection_target_contract() -> MvFirstRefreshTargetContract {
        let hidden_key = crate::planner::vocabulary::HIDDEN_APPLY_KEY_COLUMN_NAME;
        MvFirstRefreshTargetContract::try_new(
            Arc::new(Schema::new(vec![Field::new(
                hidden_key,
                DataType::Utf8,
                false,
            )])),
            vec![1],
            0,
            hidden_key.to_string(),
        )
        .expect("valid projection target contract")
    }

    #[test]
    fn first_refresh_artifact_builder_seals_every_supported_shape() {
        let aggregate_sql = "SELECT k, sum(v) AS total FROM ice.db.fact GROUP BY k";
        let aggregate_shape = aggregate_calls(aggregate_sql);
        let union_sql = "SELECT v FROM ice.db.a UNION ALL SELECT v FROM ice.db.b";
        let union_pin = SqlMvSnapshotPin::from_entries_for_tests(&[
            ("ice.db.a", 11, "a-uuid"),
            ("ice.db.b", 22, "b-uuid"),
        ]);
        let branch_sql = "SELECT k, sum(v) AS total FROM ice.db.a GROUP BY k UNION ALL SELECT k, sum(v) AS total FROM ice.db.b GROUP BY k";
        let branch_calls = aggregate_calls("SELECT k, sum(v) AS total FROM ice.db.a GROUP BY k");

        let projection = SqlMvFirstRefreshArtifactBuilder::try_new(
            parse_query("SELECT v FROM ice.db.fact"),
            pin(),
            Some("ice".to_string()),
            "db".to_string(),
            projection_target_contract(),
            SqlMvFirstRefreshArtifactShape::Projection,
        )
        .unwrap()
        .build()
        .unwrap();
        assert_eq!(
            projection.root_hash_column(),
            crate::planner::vocabulary::HIDDEN_APPLY_KEY_COLUMN_NAME
        );

        let union = SqlMvFirstRefreshArtifactBuilder::try_new(
            parse_query(union_sql),
            union_pin.clone(),
            Some("ice".to_string()),
            "db".to_string(),
            projection_target_contract(),
            SqlMvFirstRefreshArtifactShape::UnionProjection { branch_count: 2 },
        )
        .unwrap()
        .build()
        .unwrap();
        assert_eq!(
            union.root_hash_column(),
            crate::planner::vocabulary::HIDDEN_APPLY_KEY_COLUMN_NAME
        );

        let aggregate = SqlMvFirstRefreshArtifactBuilder::try_new(
            parse_query(aggregate_sql),
            pin(),
            Some("ice".to_string()),
            "db".to_string(),
            aggregate_target_contract(),
            SqlMvFirstRefreshArtifactShape::Aggregate {
                calls: aggregate_shape.clone(),
                aggregate_input_types: vec![None],
            },
        )
        .unwrap()
        .build()
        .unwrap();
        assert_eq!(aggregate.root_hash_column(), SQL_MV_ROW_ID_COLUMN);

        let fan_in_sql = "SELECT k, sum(v) AS total FROM (SELECT k, v FROM ice.db.a UNION ALL SELECT k, v FROM ice.db.b) AS input GROUP BY k";
        let fan_in = SqlMvFirstRefreshArtifactBuilder::try_new(
            parse_query(fan_in_sql),
            union_pin.clone(),
            Some("ice".to_string()),
            "db".to_string(),
            aggregate_target_contract(),
            SqlMvFirstRefreshArtifactShape::FanInAggregate {
                calls: aggregate_calls(fan_in_sql),
                aggregate_input_types: vec![None],
            },
        )
        .unwrap()
        .build()
        .unwrap();
        assert_eq!(fan_in.root_hash_column(), SQL_MV_ROW_ID_COLUMN);

        let branch = SqlMvFirstRefreshArtifactBuilder::try_new(
            parse_query(branch_sql),
            union_pin,
            Some("ice".to_string()),
            "db".to_string(),
            aggregate_target_contract(),
            SqlMvFirstRefreshArtifactShape::BranchUnionAggregate {
                branch_count: 2,
                calls: branch_calls,
            },
        )
        .unwrap()
        .build()
        .unwrap();
        assert_eq!(branch.root_hash_column(), SQL_MV_ROW_ID_COLUMN);
    }

    #[test]
    fn first_refresh_artifact_builder_fails_closed_for_malformed_facts() {
        assert!(
            SqlMvFirstRefreshArtifactBuilder::try_new(
                parse_query("SELECT v FROM ice.db.fact"),
                pin(),
                Some("ice".to_string()),
                "db".to_string(),
                projection_target_contract(),
                SqlMvFirstRefreshArtifactShape::UnionProjection { branch_count: 0 },
            )
            .is_err()
        );

        let missing_root_contract = MvFirstRefreshTargetContract::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                true,
            )])),
            vec![1],
            0,
            "__missing_apply_key__".to_string(),
        )
        .unwrap();
        assert!(
            SqlMvFirstRefreshArtifactBuilder::try_new(
                parse_query("SELECT v FROM ice.db.fact"),
                pin(),
                Some("ice".to_string()),
                "db".to_string(),
                missing_root_contract,
                SqlMvFirstRefreshArtifactShape::Projection,
            )
            .is_err()
        );

        let mismatched_root_contract = MvFirstRefreshTargetContract::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "other_key",
                DataType::Utf8,
                false,
            )])),
            vec![1],
            0,
            "other_key".to_string(),
        )
        .unwrap();
        assert!(
            SqlMvFirstRefreshArtifactBuilder::try_new(
                parse_query("SELECT v FROM ice.db.fact"),
                pin(),
                Some("ice".to_string()),
                "db".to_string(),
                mismatched_root_contract,
                SqlMvFirstRefreshArtifactShape::Projection,
            )
            .unwrap()
            .build()
            .is_err()
        );
    }

    #[test]
    fn projection_keeps_pinned_hidden_apply_key_for_writer_distribution() {
        let prepared = prepare_projection_first_refresh_write_sql(
            &parse_query("SELECT v FROM ice.db.fact"),
            &pin(),
            Some("ice"),
            "db",
        )
        .unwrap();
        assert_eq!(
            prepared.root_hash_column(),
            crate::planner::vocabulary::HIDDEN_APPLY_KEY_COLUMN_NAME
        );
        assert!(prepared.sql().contains("__nova_base_row_id"));
        assert!(
            prepared.sql().contains("VERSION AS OF 42"),
            "expected pinned physical SQL, got: {}",
            prepared.sql()
        );
    }

    #[test]
    fn aggregate_uses_be_visible_and_state_projection() {
        let query = parse_query("SELECT k, sum(v) AS total FROM ice.db.fact GROUP BY k");
        let calls = SqlAggregateCalls::extract(&query).unwrap();
        let prepared =
            prepare_aggregate_first_refresh_write_sql(&query, &calls, &pin(), Some("ice"), "db")
                .unwrap();
        assert_eq!(prepared.root_hash_column(), SQL_MV_ROW_ID_COLUMN);
        assert!(prepared.sql().contains("mv_group_row_id"));
        assert!(prepared.sql().contains("sum_state_visible"));
        assert!(prepared.sql().contains("__agg_state_total"));
        assert!(!prepared.sql().contains("RecordBatch"));
    }

    #[test]
    fn fan_in_aggregate_remains_one_pinned_be_state_project() {
        let sql = "SELECT k, sum(v) AS total FROM (SELECT k, v FROM ice.db.a UNION ALL SELECT k, v FROM ice.db.b) AS input GROUP BY k";
        let query = parse_query(sql);
        let calls = SqlAggregateCalls::extract(&query).unwrap();
        let pin = SqlMvSnapshotPin::from_entries_for_tests(&[
            ("ice.db.a", 11, "a-uuid"),
            ("ice.db.b", 22, "b-uuid"),
        ]);
        let prepared = prepare_fan_in_aggregate_first_refresh_write_sql(
            &query,
            &calls,
            &pin,
            Some("ice"),
            "db",
        )
        .unwrap();
        assert_eq!(prepared.root_hash_column(), SQL_MV_ROW_ID_COLUMN);
        assert!(prepared.sql().contains("VERSION AS OF 11"));
        assert!(prepared.sql().contains("VERSION AS OF 22"));
        assert!(prepared.sql().contains("sum_state_visible"));
    }

    #[test]
    fn fan_in_decimal_avg_freezes_input_scale_and_visible_type_in_be_sql() {
        let sql = "SELECT k, avg(d) AS a_d FROM (SELECT k, d FROM ice.db.a UNION ALL SELECT k, d FROM ice.db.b) AS input GROUP BY k";
        let query = parse_query(sql);
        let calls = SqlAggregateCalls::extract(&query).unwrap();
        let target = Schema::new(vec![
            Field::new("k", DataType::Int32, true),
            Field::new("a_d", DataType::Decimal128(38, 12), true),
        ]);
        let prepared =
            prepare_fan_in_aggregate_first_refresh_write_sql_with_target_schema_and_input_types(
                &query,
                &calls,
                &SqlMvSnapshotPin::from_entries_for_tests(&[
                    ("ice.db.a", 11, "a"),
                    ("ice.db.b", 22, "b"),
                ]),
                Some("ice"),
                "db",
                Some(&target),
                Some(&[Some(DataType::Decimal128(20, 4))]),
            )
            .unwrap();
        assert!(prepared.sql().contains("avg_state_visible(`state`.`__agg_state_a_d`, CAST(4 AS BIGINT), CAST(NULL AS DECIMAL(38,12)))"), "{}", prepared.sql());
    }

    #[test]
    fn composed_aggregate_remains_one_pinned_be_state_project() {
        let sql = "SELECT a.k, count(*) AS total FROM ice.db.a AS a JOIN ice.db.b AS b ON a.k = b.k GROUP BY a.k";
        let query = parse_query(sql);
        let calls = SqlAggregateCalls::extract(&query).unwrap();
        let pin = SqlMvSnapshotPin::from_entries_for_tests(&[
            ("ice.db.a", 11, "a-uuid"),
            ("ice.db.b", 22, "b-uuid"),
        ]);
        let prepared = prepare_composed_aggregate_first_refresh_write_sql(
            &query,
            &calls,
            &pin,
            Some("ice"),
            "db",
        )
        .unwrap();
        assert_eq!(prepared.root_hash_column(), SQL_MV_ROW_ID_COLUMN);
        assert!(prepared.sql().contains("VERSION AS OF 11"));
        assert!(prepared.sql().contains("VERSION AS OF 22"));
        assert!(prepared.sql().contains("count_state_visible"));
    }

    #[test]
    fn target_contract_rejects_schema_identity_and_partition_drift() {
        let expected = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int64, true),
            Field::new("__apply_key__", DataType::Utf8, false),
        ]));
        let contract = MvFirstRefreshTargetContract::try_new(
            Arc::clone(&expected),
            vec![1, 2],
            7,
            "__apply_key__".to_string(),
        )
        .expect("valid target contract");
        contract
            .validate_observed(expected.as_ref(), &[1, 2], 7)
            .expect("exact observed contract");
        assert!(
            contract
                .validate_observed(expected.as_ref(), &[1, 3], 7)
                .is_err()
        );
        assert!(
            contract
                .validate_observed(expected.as_ref(), &[1, 2], 8)
                .is_err()
        );
        let drifted_schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int64, false),
            Field::new("__apply_key__", DataType::Utf8, false),
        ]));
        assert!(
            contract
                .validate_observed(drifted_schema.as_ref(), &[1, 2], 7)
                .is_err()
        );
    }
}
