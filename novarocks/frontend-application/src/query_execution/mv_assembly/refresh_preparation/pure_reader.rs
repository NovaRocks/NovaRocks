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

//! The original refresh algorithm, suspended only at named I/O leaves.

use super::async_facts::{PreparationError, require_source_snapshot, validate_source_snapshot};
use super::async_original::{MvCatalogCaptureInput, MvCatalogCaptureQuery, capture_catalog};
use super::io_gate::MvRefreshPreparationIo;
use super::*;
use crate::catalog_application::query_bindings::QueryTableBindingStore;
use crate::mv::domain::application::{MvApplicationError, MvRepartitionFields};
use crate::mv::domain::refresh::pending_fence::PendingFence;
use crate::mv::domain::refresh::planning::RefreshBaseRelationOccurrence;
use crate::mv::domain::refresh::strategy::{
    RefreshStrategy, RefreshStrategyDecision, RefreshStrategyFacts,
};
use crate::mv::domain::refresh::target_binding::MvTargetBinding;
use crate::mv::domain::rewrite::context::IcebergMvRewriteContext;
use novarocks_spi::connector::{
    ConnectorChangeWindowFacts, ConnectorChangeWindowNegativeWeight,
    ConnectorChangeWindowPartitionImpact, ConnectorRequestContext,
};

pub(crate) struct MvRefreshPreparationInput {
    pub(crate) query_memory_budget_bytes: u64,
    pub(crate) optimizer_settings: novarocks_sql::compiler::SessionOptimizerSettings,
    pub(crate) source: IcebergMvCorePorts,
    pub(crate) session_properties:
        Option<Arc<novarocks_query_application::sql::session::registry::SessionPropertyInputs>>,
    pub(crate) current_catalog: Option<String>,
    pub(crate) current_database: String,
    pub(crate) statement: MvRefreshRequest,
    pub(crate) connector_context: novarocks_spi::connector::ConnectorRequestContext,
    pub(crate) repartition_fields: Option<MvRepartitionFields>,
    pub(crate) replay: Option<Arc<PendingFence>>,

    pub(crate) request: MvRefreshPreparationRequest,
}

/// These are the exact final preparation observations, not a new Current proof.
/// The activation and publication owners retain their own fresh validation.
pub(crate) struct MvRefreshPrepared {
    pub(crate) prepared: PreparedMvRefresh,
    pub(crate) target_binding: Option<MvTargetBinding>,
    pub(crate) rewrite: Option<Arc<IcebergMvRewriteContext>>,
    pub(crate) strategy: Option<RefreshStrategyDecision>,
}
#[derive(Default)]
struct CapturedPreparation {
    target: Option<MvTargetBinding>,
    rewrite: Option<Arc<IcebergMvRewriteContext>>,
    strategy: Option<RefreshStrategyDecision>,
}

pub(crate) async fn prepare_refresh(
    input: Arc<MvRefreshPreparationInput>,
    io: &MvRefreshPreparationIo,
) -> Result<MvRefreshPrepared, MvApplicationError> {
    prepare_refresh_inner(&input, io)
        .await
        .map_err(|error| error.0)
}
async fn prepare_refresh_inner(
    input: &MvRefreshPreparationInput,
    io: &MvRefreshPreparationIo,
) -> Result<MvRefreshPrepared, PreparationError> {
    let source = &input.source;
    let current_catalog = input.current_catalog.as_deref();
    let current_database = input.current_database.as_str();
    let statement = &input.statement;
    let connector_context = &input.connector_context;
    let repartition_fields = input.repartition_fields.as_ref();
    let request = input.request.clone();
    let mut captured = CapturedPreparation::default();

    request.validate().map_err(PreparationError::user)?;
    if request.statement != statement.sql_refresh_statement() {
        return Err(PreparationError::user(
            "MV refresh preparation statement does not match the admitted SQL request",
        ));
    }
    let refresh = crate::mv::domain::iceberg_refresh::async_preparation::plan_iceberg_mv_refresh_with_connector_context(
            io,
            source,
            current_catalog,
            current_database,
            statement,
            request.target.clone(),
            repartition_fields.is_some(),
            input.session_properties.as_deref(),
            input.replay.as_deref(),
            connector_context,
        ).await?;
    let mut plan = refresh.plan;
    let retained_repartition_target = if repartition_fields.is_some() {
        Some(retain_exact_repartition_target(io, source, &plan.contract, connector_context).await?)
    } else {
        None
    };
    let repartition_transition = match repartition_fields {
        Some(fields) => {
            plan.contract.decision = ExecutableRefreshDecision::FirstRefresh;
            Some(
                prepare_managed_repartition_transition(
                    io,
                    source,
                    current_catalog,
                    current_database,
                    fields,
                    &plan.contract,
                    request.attempt.write_operation_id(),
                    retained_repartition_target.as_ref().ok_or_else(|| {
                        "MV repartition preparation lost its retained target binding".to_string()
                    })?,
                    connector_context,
                )
                .await?,
            )
        }
        None => None,
    };
    let catalog = plan
        .contract
        .target
        .catalog
        .as_deref()
        .ok_or_else(|| "Iceberg MV refresh target has no connector catalog".to_string())?;
    let instance_id = ConnectorInstanceId::parse(catalog)
        .map_err(|error| PreparationError::user(error.to_string()))?;
    let observed_binding = match retained_repartition_target.as_ref() {
        Some(retained) => execution_binding_key_for_target(retained.binding.planning_pin()),
        None => source
            .connector_control()
            .observe_current_binding(&instance_id)
            .map_err(PreparationError::from)?,
    };
    if input.session_properties.is_some() && observed_binding != refresh.target_binding {
        return Err(PreparationError(MvApplicationError::new(
            crate::mv::domain::application::MvApplicationErrorKind::BindingInvalidated,
            "MV refresh target binding changed after session settings were resolved",
        )));
    }
    let mut base_table_object_ids = BTreeMap::new();
    for base in &plan.contract.base_refs {
        let observed = io.schema(source, &base.table, connector_context).await?;
        base_table_object_ids.insert(base.occurrence_id, observed.table_object_id().clone());
    }
    let expected_target_snapshot_id = match &plan.contract.state_baseline {
        RefreshStateBaseline::SnapshotBacked {
            target_snapshot_id, ..
        } => *target_snapshot_id,
    };
    let target_table_uuid = match &plan.contract.state_baseline {
        RefreshStateBaseline::SnapshotBacked {
            target_table_uuid, ..
        } => target_table_uuid.clone(),
    };
    let full_overwrite = refresh.full_overwrite;
    let mut work = match plan.contract.decision {
        ExecutableRefreshDecision::SkipEmpty => PreparedMvRefreshWork::NoOp,
        ExecutableRefreshDecision::MetadataOnly => {
            let (write, admitted) = prepare_metadata_only_publication(
                io,
                &mut captured,
                source,
                &plan.contract,
                &request.attempt,
                &observed_binding,
                connector_context,
                &base_table_object_ids,
            )
            .await?;
            PreparedMvRefreshWork::MetadataOnly { write, admitted }
        }
        ExecutableRefreshDecision::FirstRefresh => {
            let (write, admitted) = prepare_frontend_first_refresh_write(
                io,
                &mut captured,
                source,
                current_catalog,
                current_database,
                FirstRefreshTargetFacts {
                    contract: &plan.contract,
                    canonical_query: &refresh.canonical_query,
                    base_table_object_ids: &base_table_object_ids,
                    repartition_transition: repartition_transition.as_ref(),
                    retained_repartition_target: retained_repartition_target.as_ref(),
                    rebuild: refresh.rebuild.as_ref(),
                },
                &request.attempt,
                observed_binding.clone(),
                connector_context.clone(),
            )
            .await?;
            PreparedMvRefreshWork::DataProducing {
                write: PreparedMvRefreshWrite::first_refresh(
                    if full_overwrite {
                        write.into_full_overwrite()
                    } else {
                        write
                    },
                    io.captured_sources(),
                ),
                admitted,
            }
        }
        ExecutableRefreshDecision::Incremental => match prepare_frontend_incremental_write(
            io,
            &mut captured,
            input,
            &plan.contract,
            &refresh.canonical_query,
            &request.attempt,
            refresh.can_incrementally_refresh,
            &refresh.original_current,
            refresh.replay.as_ref(),
            observed_binding.clone(),
        )
        .await?
        {
            PreparedIncrementalRefreshWork::ChangeStream(incremental, admitted) => {
                PreparedMvRefreshWork::DataProducing {
                    write: PreparedMvRefreshWrite::incremental(incremental, io.captured_sources()),
                    admitted,
                }
            }
            PreparedIncrementalRefreshWork::FullRebuild(rebuild, admitted) => {
                PreparedMvRefreshWork::DataProducing {
                    write: PreparedMvRefreshWrite::first_refresh(rebuild, io.captured_sources()),
                    admitted,
                }
            }
            PreparedIncrementalRefreshWork::PartitionReplace {
                write,
                admitted,
                overlays,
            } => PreparedMvRefreshWork::DataProducing {
                write: PreparedMvRefreshWrite::first_refresh(write, io.captured_sources())
                    .with_partition_replacement_overlays(overlays),
                admitted,
            },
            PreparedIncrementalRefreshWork::MetadataOnly => {
                let (write, admitted) = prepare_metadata_only_publication(
                    io,
                    &mut captured,
                    source,
                    &plan.contract,
                    &request.attempt,
                    &observed_binding,
                    connector_context,
                    &base_table_object_ids,
                )
                .await?;
                PreparedMvRefreshWork::MetadataOnly { write, admitted }
            }
        },
    };
    if let PreparedMvRefreshWork::DataProducing { write, admitted } = &mut work
        && write.publication_intent().technique() == MvRefreshPublicationTechnique::Full
        && !refresh.original_current.interpretation.is_aggregate()
    {
        use novarocks_mv_application::persistence::{
            codec::RecoveryRecord,
            resolution::{FrozenMvMarkerKeys, rebuild_mv_marker_policy},
        };
        let (policy, recovery) = match refresh.explicit_recovery {
            Some(recovered_from) => (
                rebuild_mv_marker_policy(),
                Some(RecoveryRecord {
                    recovered_from,
                    operation: novarocks_spi::connector::ConnectorMutationOperationId::from_bytes(
                        request.attempt.publication_id.to_bytes(),
                    ),
                    recovered_at_ms: crate::mv::domain::staged_create::now_unix_millis()?,
                }),
            ),
            None => (
                FrozenMvMarkerKeys::capture(&refresh.original_current.view)
                    .into_full_refresh_policy(),
                None,
            ),
        };
        admitted.bind_full_refresh_markers(policy, recovery)?;
    }
    Ok(MvRefreshPrepared {
        prepared: PreparedMvRefresh {
            attempt: request.attempt,
            observed_binding,
            finalize: MvRefreshFinalizeFacts {
                mv_id: plan.contract.mv_id.ok_or_else(|| {
                    "Iceberg MV refresh plan has no persisted materialized-view ID".to_string()
                })?,
                target: plan.contract.target,
                base_snapshots: plan.contract.snapshot_pins,
                base_table_object_ids,
                expected_target_snapshot_id,
                target_table_uuid,
            },
            work,
        },
        target_binding: captured.target,
        rewrite: captured.rewrite,
        strategy: captured.strategy,
    })
}

pub(crate) async fn frozen_refresh_aggregate_analysis(
    io: &MvRefreshPreparationIo,
    source: &IcebergMvCorePorts,
    projection: &StoredKeyedMvProjection,
    query: &novarocks_parser::ast::Query,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<
    Option<(
        novarocks_sql::planning::mv::SqlMvAggregateCalls,
        novarocks_sql::planning::mv_aggregate_layout::SqlMvAggregatePhysicalLayout,
    )>,
    PreparationError,
> {
    if projection.facts.interpretation().aggregates.is_empty() {
        return Ok(None);
    }
    let resolution = &projection.facts.definition().query.resolution;
    let representative = if projection.facts.interpretation().branches.is_empty() {
        query.clone()
    } else {
        crate::mv::domain::rewrite::context::first_union_branch_query(query)?
    };
    let calls = extract_aggregate_sql_calls(&representative)?;
    let layout = build_aggregate_layout_for_refresh_query(
        io,
        source,
        Some(resolution.default_catalog.as_str()),
        &resolution.default_namespace,
        &representative,
        connector_context,
    )
    .await?;
    Ok(Some((calls, layout)))
}

async fn build_aggregate_layout_for_refresh_query(
    io: &MvRefreshPreparationIo,
    ports: &IcebergMvCorePorts,
    current_catalog: Option<&str>,
    current_database: &str,
    visible_query: &novarocks_parser::ast::Query,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<
    novarocks_sql::planning::mv_aggregate_layout::SqlMvAggregatePhysicalLayout,
    PreparationError,
> {
    let visible_analysis = analyze_visible_refresh_query(
        io,
        ports,
        current_catalog,
        current_database,
        visible_query,
        connector_context,
    )
    .await?;
    let facts = visible_analysis
        .refresh_input
        .aggregate_layout_facts(visible_query, SqlMvAggregateLayoutScope::WholeQuery)?;
    novarocks_sql::planning::mv_aggregate_layout::build_sql_mv_aggregate_physical_layout(&facts)
        .map_err(PreparationError::from)
}

pub(crate) async fn analyze_visible_refresh_query(
    io: &MvRefreshPreparationIo,
    ports: &IcebergMvCorePorts,
    current_catalog: Option<&str>,
    current_database: &str,
    visible_query: &novarocks_parser::ast::Query,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<crate::mv::domain::analysis::MvAnalysis, PreparationError> {
    let (analysis, _) = capture_visible_refresh_analysis(
        io,
        ports,
        current_catalog,
        current_database,
        visible_query,
        connector_context,
    )
    .await?;
    Ok(analysis)
}

pub(crate) async fn capture_visible_refresh_analysis(
    io: &MvRefreshPreparationIo,
    ports: &IcebergMvCorePorts,
    current_catalog: Option<&str>,
    current_database: &str,
    visible_query: &novarocks_parser::ast::Query,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<
    (
        crate::mv::domain::analysis::MvAnalysis,
        Arc<QueryTableBindingStore>,
    ),
    PreparationError,
> {
    let captured_catalog = super::async_original::capture_catalog_with_source_history(
        MvCatalogCaptureInput {
            query: MvCatalogCaptureQuery::Query(Box::new(visible_query.clone())),
            ports: ports.clone(),
            current_catalog: current_catalog.map(str::to_owned),
            current_database: current_database.to_owned(),
            context: connector_context.clone(),
            control: crate::query_execution::planning::sql_compile_control_from_connector_request(
                connector_context,
            ),
            bindings: Arc::new(QueryTableBindingStore::try_new()?),
            overlays: super::super::query_local_bindings::freeze_captured_source_overlays(
                &io.captured_sources(),
            )?,
        },
        io,
        true,
    )
    .await?;
    let analysis = crate::mv::domain::analysis_adapter::analyze_mv_select_with_provider(
        current_catalog,
        captured_catalog.catalog.provider(),
        current_database,
        visible_query,
        ports.function_catalog().as_ref(),
        ports.constant_policy(),
        crate::query_execution::planning::sql_compile_control_from_connector_request(
            connector_context,
        ),
    )
    .map_err(PreparationError::from)?;
    Ok((analysis, captured_catalog.bindings))
}

async fn retain_exact_repartition_target(
    io: &MvRefreshPreparationIo,
    source: &IcebergMvCorePorts,
    contract: &RefreshPlanContract,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<RetainedRepartitionTarget, PreparationError> {
    let target = IcebergMvTarget {
        catalog: contract
            .target
            .catalog
            .clone()
            .ok_or_else(|| "MV repartition target has no connector catalog".to_string())?,
        namespace: contract.target.database.clone(),
        table: contract.target.name.clone(),
    };
    let binding = io
        .target_binding(source, &target, connector_context)
        .await?;
    validate_retained_target_identity(&target, binding.identity())?;
    let schema_validation = crate::mv::domain::storage_observation::observe_schema_validation(
        source.storage_observation(),
        binding.planning_pin(),
        binding.metadata(),
        connector_context.clone(),
    )
    .map_err(PreparationError::from)?;
    Ok(RetainedRepartitionTarget {
        binding,
        schema_validation,
    })
}

async fn prepare_managed_repartition_transition(
    io: &MvRefreshPreparationIo,
    source: &IcebergMvCorePorts,
    current_catalog: Option<&str>,
    current_database: &str,
    fields: &MvRepartitionFields,
    contract: &RefreshPlanContract,
    operation_id: novarocks_spi::connector::ConnectorWriteOperationId,
    retained_target: &RetainedRepartitionTarget,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<PreparedManagedRepartitionTransition, PreparationError> {
    let target = IcebergMvTarget {
        catalog: contract
            .target
            .catalog
            .clone()
            .ok_or_else(|| "MV repartition target has no connector catalog".to_string())?,
        namespace: contract.target.database.clone(),
        table: contract.target.name.clone(),
    };
    validate_retained_target_identity(&target, retained_target.binding.identity())?;
    if fields.owner != execution_binding_key_for_target(retained_target.binding.planning_pin()) {
        return Err(MvApplicationError::new(
            crate::mv::domain::application::MvApplicationErrorKind::BindingInvalidated,
            "MV partition property scope belongs to a different connector generation",
        )
        .into());
    }
    let projection = io.definition(source, &target).await?;
    let interpretation = projection.facts.interpretation();
    if !interpretation.branches.is_empty() && !interpretation.aggregates.is_empty() {
        return Err(
            "UnsupportedRepartitionShape: ALTER MATERIALIZED VIEW ... REPARTITION does not support branch UNION ALL aggregates"
                .to_string().into(),
        );
    }
    let query = canonical_mv_select_query(&projection)?;
    let bindings = validate_projection_target(&projection, &retained_target.schema_validation)?;
    select_repartition_shape(&RefreshCapabilities::from_canonical_facts(
        interpretation,
        &bindings,
        projection.facts.binding().occurrences.len(),
        definition_has_join(&query),
    )?)?;
    validate_projection_target(&projection, &retained_target.schema_validation)?;
    let base_tables = contract_base_tables(contract);
    let occurrences = &projection.facts.binding().occurrences;
    if occurrences.len() != base_tables.len() {
        return Err(PreparationError::user(
            "MV repartition source references do not retain every D occurrence",
        ));
    }
    for (occurrence, table) in occurrences.iter().zip(&base_tables) {
        crate::mv::domain::refresh::observation::validate_occurrence_locator(occurrence, table)?;
        let observed = io.schema(source, table, connector_context).await?;
        let renames =
            crate::mv::domain::refresh::schema_contract::validate_relation_occurrence_schema(
                occurrence, &observed,
            )?;
        if !renames.is_empty() {
            return Err(PreparationError::user(
                "MV repartition requires occurrence-aware SQL field rebinding",
            ));
        }
    }

    let captured_catalog = capture_catalog(
        MvCatalogCaptureInput {
            query: MvCatalogCaptureQuery::Query(Box::new(query.clone())),
            ports: source.clone(),
            current_catalog: current_catalog.map(str::to_owned),
            current_database: current_database.to_owned(),
            context: connector_context.clone(),
            control: crate::query_execution::planning::sql_compile_control_from_connector_request(
                connector_context,
            ),
            bindings: Arc::new(QueryTableBindingStore::try_new()?),
            overlays: super::super::query_local_bindings::freeze_captured_source_overlays(
                &io.captured_sources(),
            )?,
        },
        io,
    )
    .await?;
    let completion_control =
        crate::query_execution::planning::sql_compile_control_from_connector_request(
            connector_context,
        );
    let analysis = crate::mv::domain::analysis_adapter::analyze_mv_select_with_provider(
        current_catalog,
        captured_catalog.catalog.provider(),
        current_database,
        &query,
        source.function_catalog().as_ref(),
        source.constant_policy(),
        completion_control.clone(),
    )?;
    if derive_fragment_property(&analysis, &completion_control)?
        .is_composed_aggregate_schema_contract_fallback()
    {
        return Err("partitioned composed aggregate Iceberg MV is not supported"
            .to_string()
            .into());
    }

    // Both the prior specification and the physical field IDs come from the
    // one retained target observation. Canonical L keeps provider-opaque
    // partition identities and typed transforms; none are decoded here.
    let prior_partition = retained_target.binding.partition();
    let prior_fields = prior_partition
        .fields
        .iter()
        .enumerate()
        .map(|(position, field)| {
            novarocks_spi::connector::ConnectorManagedPartitionField::try_new(
                field.source_target_field_id,
                u32::try_from(position)
                    .map_err(|_| "MV repartition prior field count exceeds u32".to_string())?,
                managed_partition_transform(&field.transform),
            )
            .map_err(|error| error.to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let expected_prior =
        novarocks_spi::connector::ConnectorManagedPartitionSpecObservation::try_from_fields(
            prior_partition.target_spec_id,
            &prior_fields,
        )
        .map_err(|error| error.to_string())?;
    let target_field_id_by_name = retained_target_field_id_by_name(retained_target)?;
    let MvRepartitionFields {
        properties,
        partitioning_index,
        configuration,
        ..
    } = fields;
    use novarocks_spi::connector::{DdlPropertyChange, DdlPropertyValue};
    let (_, change) = properties.get(*partitioning_index).ok_or_else(|| {
        PreparationError::user("MV repartition lost its evaluated partition property")
    })?;
    let value = match change {
        DdlPropertyChange::Set(value) | DdlPropertyChange::Reset(value) => value,
    };
    let DdlPropertyValue::ArrayVarchar(fields) = value else {
        return Err(PreparationError::user(
            "MV repartition requires array(varchar)",
        ));
    };
    let mut visible_sources = std::collections::HashSet::new();
    for column in &analysis.output_columns {
        novarocks_spi::connector::ConnectorOperationControl::check_active(connector_context)?;
        visible_sources.insert(novarocks_types::naming::normalize_identifier(&column.name)?);
    }
    let write_lease = retained_target
        .binding
        .planning_pin()
        .derive_write_lease()
        .map_err(PreparationError::from)?;
    let replacement_fields = bind_property_repartition_fields(
        fields,
        &visible_sources,
        &target_field_id_by_name,
        connector_context,
        |field, index| {
            write_lease.parse_managed_partitioning_element(
                retained_target.binding.handle(),
                field,
                index,
                connector_context,
            )
        },
    )?;
    let replacement = novarocks_spi::connector::ConnectorManagedPartitionSpecReplacement::try_new(
        operation_id,
        expected_prior,
        replacement_fields,
    )
    .map_err(|error| error.to_string())?;
    let replacement = if properties.len() > 1 {
        replacement.with_storage_properties(Arc::clone(properties), *partitioning_index)?
    } else {
        replacement
    };
    let preview = write_lease
        .preview_managed_partition_spec(ConnectorManagedPartitionSpecPreviewRequest::new(
            operation_id,
            retained_target.binding.handle().clone(),
            replacement.clone(),
            connector_context.clone(),
        ))
        .map_err(PreparationError::from)?;
    Ok(PreparedManagedRepartitionTransition {
        replacement,
        preview,
        configuration: configuration.clone(),
    })
}

/// Bind each typed partition field to the retained physical target before
/// interpreting the next entry. The resulting IDs go to the original preview.
fn bind_property_repartition_fields(
    fields: &[Option<String>],
    visible_sources: &std::collections::HashSet<String>,
    physical_fields: &[(String, i32)],
    control: &dyn novarocks_spi::connector::ConnectorOperationControl,
    mut parse: impl FnMut(
        Option<&str>,
        usize,
    ) -> Result<
        novarocks_spi::connector::ConnectorPartitionTransform,
        novarocks_spi::connector::ConnectorError,
    >,
) -> Result<Vec<novarocks_spi::connector::ConnectorManagedPartitionField>, PreparationError> {
    use novarocks_spi::connector::{
        ConnectorError, ConnectorErrorKind, ConnectorManagedPartitionField,
        ConnectorManagedPartitionTransform as Managed, ConnectorPartitionTransform as Input,
        InvalidTableProperty,
    };

    let property_error = |message: String| {
        InvalidTableProperty::from_property_error(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            message,
        ))
    };
    let mut result = Vec::with_capacity(fields.len());
    for (position, field) in fields.iter().enumerate() {
        control.check_active()?;
        let field = parse(field.as_deref(), position)?;
        let (column, transform) = match field {
            Input::Identity { column } => (column, Managed::Identity),
            Input::Year { column } => (column, Managed::Year),
            Input::Month { column } => (column, Managed::Month),
            Input::Day { column } => (column, Managed::Day),
            Input::Hour { column } => (column, Managed::Hour),
            Input::Bucket {
                column,
                num_buckets,
            } => (
                column,
                Managed::Bucket {
                    buckets: num_buckets,
                },
            ),
            Input::Truncate { column, width } => (column, Managed::Truncate { width }),
            Input::Void { column } => (column, Managed::Void),
        };
        let Some((_, source_field_id)) = physical_fields
            .iter()
            .find(|(name, _)| name.as_str() == column.as_ref())
        else {
            return Err(property_error(format!(
                "MV repartition source column `{column}` is missing from the exact target observation"
            )).into());
        };
        if !visible_sources.contains(column.as_ref()) {
            return Err(property_error(format!(
                "materialized view partition source column `{column}` must be an output column"
            ))
            .into());
        }
        result.push(ConnectorManagedPartitionField::try_new(
            *source_field_id,
            u32::try_from(position)
                .map_err(|_| "MV repartition field count exceeds u32".to_string())?,
            transform,
        )?);
    }
    control.check_active()?;
    Ok(result)
}

struct FirstRefreshTargetFacts<'contract, 'objects, 'transition, 'retained, 'rebuild> {
    contract: &'contract RefreshPlanContract,
    canonical_query: &'contract novarocks_parser::ast::Query,
    base_table_object_ids: &'objects BTreeMap<SqlMvRelationOccurrenceId, ConnectorTableObjectId>,
    repartition_transition: Option<&'transition PreparedManagedRepartitionTransition>,
    retained_repartition_target: Option<&'retained RetainedRepartitionTarget>,
    rebuild: Option<
        &'rebuild crate::mv::domain::iceberg_refresh::async_preparation::RefreshRebuildBinding,
    >,
}

async fn prepare_frontend_first_refresh_write(
    io: &MvRefreshPreparationIo,
    captured: &mut CapturedPreparation,
    source: &IcebergMvCorePorts,
    current_catalog: Option<&str>,
    current_database: &str,
    target_facts: FirstRefreshTargetFacts<'_, '_, '_, '_, '_>,
    attempt: &MvRefreshAttemptIdentity,
    observed_binding: ConnectorProviderBindingKey,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
) -> Result<(PreparedMvFirstRefreshWrite, AdmittedMvDataPublication), PreparationError> {
    let FirstRefreshTargetFacts {
        contract,
        canonical_query: query,
        base_table_object_ids,
        repartition_transition,
        retained_repartition_target,
        rebuild,
    } = target_facts;
    captured.rewrite = None;
    let target = IcebergMvTarget {
        catalog: contract.target.catalog.clone().ok_or_else(|| {
            "Iceberg MV first-refresh target has no connector catalog".to_string()
        })?,
        namespace: contract.target.database.clone(),
        table: contract.target.name.clone(),
    };
    if let Some(retained) = retained_repartition_target {
        validate_retained_target_identity(&target, retained.binding.identity())?;
    }
    let planning_pin = match retained_repartition_target {
        Some(retained) => retained.binding.planning_pin().clone(),
        None => {
            crate::connector::metadata_binding_typed(source.connector_control(), &target.catalog)?
        }
    };
    let write_lease = planning_pin
        .derive_write_lease()
        .map_err(PreparationError::from)?;
    if !write_lease.matches_provider_binding_key(&observed_binding) {
        return Err(
            "MV first-refresh target connector generation changed during admission"
                .to_string()
                .into(),
        );
    }
    let projection = io.definition(source, &target).await?;
    let refresh_query_source = projection.facts.definition().query.effective_sql.clone();
    let (effective_binding, effective_interpretation) = match rebuild {
        Some(rebuild) => (
            &rebuild.candidate.observed.document,
            &rebuild.candidate.interpretation,
        ),
        None => (
            projection.facts.binding(),
            projection.facts.interpretation(),
        ),
    };
    let mut admitted_publication = io
        .admit(
            source,
            &planning_pin,
            &projection,
            None,
            attempt.publication_id,
            repartition_transition.and_then(|transition| transition.configuration.as_ref()),
            &connector_context,
        )
        .await?;
    if let Some(rebuild) = rebuild {
        admitted_publication.replace_refresh_layout(
            rebuild.candidate.versions.clone(),
            rebuild.candidate.interpretation.clone(),
            rebuild.markers_remove.clone(),
        )?;
    }
    if let Some(transition) = repartition_transition {
        admitted_publication.set_repartition_partitioning(&transition.preview)?;
    }
    let mut publication_intent = frontend_refresh_publication_intent(
        io,
        source.connector_control(),
        &connector_context,
        contract,
        attempt,
        &projection,
        &admitted_publication,
        &refresh_query_source,
        base_table_object_ids,
    )
    .await?;
    if let Some(transition) = repartition_transition {
        // The retained binding already proved this is the exact installed
        // generation: `validate_projection_target` compared L's own
        // partition-spec version against it. There is no descriptor left to
        // compare, and the replacement carries the provider's expected prior
        // specification itself.
        retained_repartition_target.ok_or_else(|| {
            "MV repartition preparation lost its retained target binding".to_string()
        })?;
        publication_intent = publication_intent.with_partition_spec_replacement(
            transition.replacement.clone(),
            transition.preview.committed_partitioning().clone(),
            transition.preview.storage_projection_digest(),
            admitted_publication.configuration_slot_update().cloned(),
        );
    }
    let loaded_target_binding;
    let target_binding = match retained_repartition_target {
        Some(retained) => &retained.binding,
        None => {
            loaded_target_binding = io
                .target_binding_with_lease(
                    source.storage_observation(),
                    &novarocks_types::naming::TableIdentity {
                        catalog: target.catalog.clone(),
                        namespace: target.namespace.clone(),
                        table: target.table.clone(),
                    },
                    planning_pin.clone(),
                    &connector_context,
                )
                .await?;
            &loaded_target_binding
        }
    };
    let loaded_target_schema_validation;
    let target_schema_validation = match retained_repartition_target {
        Some(retained) => &retained.schema_validation,
        None => {
            loaded_target_schema_validation =
                crate::mv::domain::storage_observation::observe_schema_validation(
                    source.storage_observation(),
                    target_binding.planning_pin(),
                    target_binding.metadata(),
                    connector_context.clone(),
                )
                .map_err(PreparationError::from)?;
            &loaded_target_schema_validation
        }
    };
    captured.target = Some(target_binding.clone());
    let runtime_bindings = match rebuild {
        Some(_) => {
            let exact = target_schema_validation.exact_schema();
            novarocks_mv_application::persistence::runtime_bindings::reconstruct_runtime_bindings(
                effective_binding,
                effective_interpretation,
                &projection.facts.source_revision().target_object_id,
                &exact.metadata_version,
                exact,
            )?
        }
        None => validate_projection_target(&projection, target_schema_validation)?,
    };
    let capabilities = RefreshCapabilities::from_canonical_facts(
        effective_interpretation,
        &runtime_bindings,
        effective_binding.occurrences.len(),
        definition_has_join(query),
    )?;
    let target_arrow_schema = target_binding.physical_write_schema()?.as_ref().clone();
    let target_write_fields = target_binding.write_input_fields()?;
    let target_field_ids = target_binding.observation().field_ids().to_vec();
    // `validate_projection_target` already proved this observation carries L's
    // exact partition-spec version; the numeric spec ID below is the provider's
    // own fact from that same observation, never a decode of the opaque bytes.
    let partition_spec_id = target_binding.partition().target_spec_id;
    let target_contract =
        novarocks_sql::planning::mv::first_refresh::MvFirstRefreshTargetContract::try_new(
            Arc::new(target_arrow_schema),
            target_field_ids,
            partition_spec_id,
            capabilities.apply_key_column.clone().unwrap_or_default(),
        )?;
    let pin = pin_contract_bases(
        io,
        source,
        contract,
        effective_binding,
        base_table_object_ids,
        &connector_context,
        "MV first-refresh",
    )
    .await?;
    // P's input watermark is exactly what this pin says, so it is frozen here,
    // beside the pin, rather than reconstructed at the commit from facts that
    // would by then be a second reading of the same sources.
    // The canonical definition owns name resolution. The execution artifact is
    // transient and may be canonicalized, but it must never fall back to the
    // session which happens to issue REFRESH.
    let resolution = &projection.facts.definition().query.resolution;
    let source_catalog = Some(resolution.default_catalog.clone());
    let source_database = resolution.default_namespace.clone();
    let admitted_publication = AdmittedMvDataPublication::try_new(
        admitted_publication,
        &pin.exact_revisions_by_occurrence(),
    )?;
    let expected_target_snapshot_id = match &contract.state_baseline {
        RefreshStateBaseline::SnapshotBacked {
            target_snapshot_id, ..
        } => *target_snapshot_id,
    };
    if definition_has_join(query) && !capabilities.has_agg_state {
        let rewrite =
            crate::mv::domain::refresh::rewrite_context::freeze_refresh_rewrite_context_original(
                io,
                RefreshRewriteInputs {
                    recovery_publication: None,
                    connector_control: source.connector_control(),
                    storage_observation: source.storage_observation(),
                    connector_context: &connector_context,
                    projection: Arc::new(projection.clone()),
                    rebuild_layout: rebuild.map(|rebuild| {
                        Arc::new(
                            crate::mv::domain::rewrite::context::MvRewriteRebuildLayout {
                                binding: rebuild.candidate.observed.document.clone(),
                                interpretation: rebuild.candidate.interpretation.clone(),
                            },
                        )
                    }),
                    pin: &pin,
                    state_baseline: &contract.state_baseline,
                    target_binding,
                    target_observation: target_schema_validation,
                    runtime_bindings: &runtime_bindings,
                    join_analysis: novarocks_sql::planning::mv::extract_refresh_join_analysis(
                        query,
                    )?,
                    aggregate: None,
                },
            )
            .await?;
        captured.rewrite = Some(Arc::clone(&rewrite));
        let frozen_base_overlays = crate::query_execution::mv_assembly::query_local_bindings::freeze_imv_base_query_local_overlays_from_captured_inputs_original(io,
            source.connector_control(),
            &connector_context,
            &rewrite,
        ).await?;
        let table = target_binding.handle().clone();
        let request = MvFirstRefreshWriteRequest::try_new(
            target.catalog,
            target.namespace,
            target.table,
            attempt.staging_branch(),
            current_catalog.map(str::to_string),
            current_database.to_string(),
            expected_target_snapshot_id,
            table,
            Arc::clone(&target_write_fields),
            observed_binding,
            attempt.write_operation_id(),
        )?;
        let prepared = MvFirstRefreshWritePreparer::prepare_join_logical(
            request,
            crate::query_execution::mv_assembly::first_refresh_staging::frozen_logical_context_from_rewrite(
                &rewrite,
                contract.affected_partitions.clone(),
                Some(frozen_base_overlays),
            )?,
            publication_intent,
        )?;
        return Ok((
            if repartition_transition.is_some() {
                prepared.into_full_overwrite()
            } else {
                prepared
            },
            admitted_publication,
        ));
    }
    let sql_pin =
        novarocks_sql::planning::mv::first_refresh::SqlMvSnapshotPin::try_from_occurrences(
            pin.occurrences()
                .iter()
                .map(|occurrence| {
                    novarocks_sql::planning::mv::first_refresh::SqlMvSnapshotPinOccurrence::try_new(
                        occurrence.occurrence_id(),
                        occurrence.table().clone(),
                        occurrence.snapshot_id(),
                        occurrence.table_object_id().clone(),
                    )
                })
                .collect::<Result<Vec<_>, _>>()?,
        )?;
    let sql_pin = if effective_binding.dependencies.iter().any(|dependency| {
        matches!(
            dependency,
            novarocks_mv_application::persistence::codec::BindingDependency::View { .. }
        )
    }) {
        sql_pin.with_captured_sources()
    } else {
        sql_pin
    };
    // L owns the branch and relation shape; the count of UNION branches is the
    // number of branch interpretations, not a separately persisted number.
    let branch_count = effective_interpretation.branches.len();
    let multi_relation = effective_binding.occurrences.len() > 1;
    let shape = if capabilities.has_agg_state {
        // A branch UNION ALL has no top-level GROUP BY. Its aggregate-state
        // layout is defined by the first branch and CREATE-time validation
        // guarantees the remaining branches share that layout.
        let aggregate_query = if branch_count > 0 {
            crate::mv::domain::rewrite::context::first_union_branch_query(query)?
        } else {
            query.clone()
        };
        let calls = extract_aggregate_sql_calls(&aggregate_query)?;
        // The analyzer attaches aggregate input types to a SELECT body.  A
        // top-level branch UNION has no such body, while the first branch has
        // the validated representative aggregate layout.
        let aggregate_layout = build_aggregate_layout_for_refresh_query(
            io,
            source,
            source_catalog.as_deref(),
            &source_database,
            &aggregate_query,
            &connector_context,
        )
        .await?;
        if branch_count > 0 {
            novarocks_sql::planning::mv::first_refresh::SqlMvFirstRefreshArtifactShape::BranchUnionAggregate {
                branch_count,
                calls,
            }
        } else if multi_relation {
            novarocks_sql::planning::mv::first_refresh::SqlMvFirstRefreshArtifactShape::FanInAggregate {
                calls,
                aggregate_input_types: aggregate_layout
                    .runtime_layout()
                    .aggregate_input_types()
                    .to_vec(),
            }
        } else {
            novarocks_sql::planning::mv::first_refresh::SqlMvFirstRefreshArtifactShape::Aggregate {
                calls,
                aggregate_input_types: aggregate_layout
                    .runtime_layout()
                    .aggregate_input_types()
                    .to_vec(),
            }
        }
    } else if branch_count > 0 {
        novarocks_sql::planning::mv::first_refresh::SqlMvFirstRefreshArtifactShape::UnionProjection {
            branch_count,
        }
    } else {
        novarocks_sql::planning::mv::first_refresh::SqlMvFirstRefreshArtifactShape::Projection
    };
    let physical_sql =
        novarocks_sql::planning::mv::first_refresh::SqlMvFirstRefreshArtifactBuilder::try_new(
            query.clone(),
            sql_pin,
            source_catalog,
            source_database,
            target_contract,
            shape,
        )?
        .build()?;
    let table = target_binding.handle().clone();
    let request = MvFirstRefreshWriteRequest::try_new(
        target.catalog,
        target.namespace,
        target.table,
        attempt.staging_branch(),
        current_catalog.map(str::to_string),
        current_database.to_string(),
        expected_target_snapshot_id,
        table,
        target_write_fields,
        observed_binding,
        attempt.write_operation_id(),
    )?;
    let prepared = MvFirstRefreshWritePreparer::prepare(request, physical_sql, publication_intent)?;
    Ok((
        if repartition_transition.is_some() {
            prepared.into_full_overwrite()
        } else {
            prepared
        },
        admitted_publication,
    ))
}

async fn frontend_refresh_publication_intent(
    io: &MvRefreshPreparationIo,
    connector_control: &dyn novarocks_spi::connector::ConnectorControlRegistry,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    contract: &RefreshPlanContract,
    attempt: &MvRefreshAttemptIdentity,
    projection: &StoredKeyedMvProjection,
    admitted: &crate::mv::domain::staged_create::AdmittedMvPublication,
    select_sql: &str,
    base_table_object_ids: &BTreeMap<SqlMvRelationOccurrenceId, ConnectorTableObjectId>,
) -> Result<MvRefreshPublicationIntent, PreparationError> {
    let snapshots = pinned_snapshots_by_occurrence(contract, "MV staging provenance")?;
    let previous_sources = baseline_previous_sources(&contract.state_baseline);
    mv_refresh_publication_intent(
        io,
        connector_control,
        connector_context,
        attempt.publication_id,
        projection.facts.source_revision().target_object_id.clone(),
        expected_target_snapshot(contract),
        admitted,
        MvRefreshPublicationTechnique::Full,
        &snapshots,
        base_table_object_ids,
        previous_sources,
        mv_definition_fingerprint(select_sql),
        contract
            .target
            .catalog
            .clone()
            .ok_or_else(|| "MV refresh publication target has no connector catalog".to_string())?,
        contract.target.database.clone(),
        contract.target.name.clone(),
    )
    .await
}

async fn pin_contract_bases(
    io: &MvRefreshPreparationIo,
    source: &IcebergMvCorePorts,
    contract: &RefreshPlanContract,
    binding: &novarocks_mv_application::persistence::codec::BindingDocument,
    base_table_object_ids: &BTreeMap<SqlMvRelationOccurrenceId, ConnectorTableObjectId>,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    what: &str,
) -> Result<RefreshSnapshotPin, PreparationError> {
    let definition_occurrences = &binding.occurrences;
    if definition_occurrences.len() != contract.base_refs.len() {
        return Err(format!("{what} base facts do not retain every D occurrence").into());
    }
    let mut occurrences = Vec::with_capacity(definition_occurrences.len());
    for (occurrence, base) in definition_occurrences.iter().zip(&contract.base_refs) {
        if occurrence.occurrence_id != base.occurrence_id.get()
            || occurrence.catalog_at_binding != base.table.catalog
            || occurrence.namespace_at_binding != base.table.namespace
            || occurrence.relation_at_binding != base.table.table
        {
            return Err(format!(
                "{what} base does not match D occurrence {}",
                occurrence.occurrence_id,
            )
            .into());
        }
        let named = base.display();
        let snapshot_id = require_source_snapshot(
            contract
                .snapshot_pins
                .get(&base.occurrence_id)
                .copied()
                .flatten(),
            what,
            &named,
        )?;
        let (observed, exact_revision) = io
            .current_revision(source, &base.table, connector_context)
            .await?;
        let object_id = observed.object_id().clone();
        let (persisted_object, _) = persist_exact_connector_revision(&exact_revision)
            .map_err(|error| format!("persist {what} source identity: {error}"))?;
        let expected_object_id = base_table_object_ids
            .get(&base.occurrence_id)
            .ok_or_else(|| format!("{what} has no object-ID fact for {named}"))?;
        if &object_id != expected_object_id {
            return Err(
                format!("{what} base table identity changed after planning for {named}").into(),
            );
        }
        validate_source_snapshot(
            occurrence,
            &persisted_object,
            snapshot_id,
            observed.current_snapshot_id(),
            what,
            &named,
        )?;
        occurrences.push(RefreshSnapshotPinOccurrence::try_new(
            base.occurrence_id,
            base.table.clone(),
            snapshot_id,
            object_id,
            exact_revision,
        )?);
    }
    RefreshSnapshotPin::try_from_occurrences(occurrences).map_err(PreparationError::from)
}

async fn prepare_metadata_only_publication(
    io: &MvRefreshPreparationIo,
    captured: &mut CapturedPreparation,
    source: &IcebergMvCorePorts,
    contract: &RefreshPlanContract,
    attempt: &MvRefreshAttemptIdentity,
    observed_binding: &novarocks_spi::connector::ConnectorProviderBindingKey,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    base_table_object_ids: &BTreeMap<SqlMvRelationOccurrenceId, ConnectorTableObjectId>,
) -> Result<(PreparedMvMetadataOnlyWrite, AdmittedMvDataPublication), PreparationError> {
    let snapshots = pinned_snapshots_by_occurrence(contract, "MV metadata-only provenance")?;
    let RefreshStateBaseline::SnapshotBacked {
        previous_sources,
        definition_fingerprint,
        ..
    } = &contract.state_baseline;
    let target = IcebergMvTarget {
        catalog: contract
            .target
            .catalog
            .clone()
            .ok_or_else(|| "MV metadata-only target has no connector catalog".to_string())?,
        namespace: contract.target.database.clone(),
        table: contract.target.name.clone(),
    };
    let projection = io.definition(source, &target).await?;
    let planning_pin =
        crate::connector::metadata_binding_typed(source.connector_control(), &target.catalog)?;
    let admitted = io
        .admit(
            source,
            &planning_pin,
            &projection,
            None,
            attempt.publication_id,
            None,
            connector_context,
        )
        .await?;
    let intent = mv_refresh_publication_intent(
        io,
        source.connector_control(),
        connector_context,
        attempt.publication_id,
        projection.facts.source_revision().target_object_id.clone(),
        expected_target_snapshot(contract),
        &admitted,
        MvRefreshPublicationTechnique::MetadataOnly,
        &snapshots,
        base_table_object_ids,
        previous_sources,
        definition_fingerprint.clone(),
        contract
            .target
            .catalog
            .clone()
            .ok_or_else(|| "MV metadata-only target has no connector catalog".to_string())?,
        contract.target.database.clone(),
        contract.target.name.clone(),
    )
    .await?;
    let pin = pin_contract_bases(
        io,
        source,
        contract,
        projection.facts.binding(),
        base_table_object_ids,
        connector_context,
        "MV metadata-only refresh",
    )
    .await?;
    let admitted =
        AdmittedMvDataPublication::try_new(admitted, &pin.exact_revisions_by_occurrence())?;
    // The session opens against the target itself, so it declares the same
    // handle and input shape a data publication does. Both are read from the
    // exact installed binding, so a metadata-only publication cannot declare a
    // shape the target no longer has.
    let target_binding = io
        .target_binding_with_lease(
            source.storage_observation(),
            &novarocks_types::naming::TableIdentity {
                catalog: target.catalog.clone(),
                namespace: target.namespace.clone(),
                table: target.table.clone(),
            },
            planning_pin.clone(),
            connector_context,
        )
        .await?;
    captured.target = Some(target_binding.clone());
    captured.rewrite = None;
    let write_input_fields = target_binding.write_input_fields()?;
    let write = PreparedMvMetadataOnlyWrite::try_new(
        intent,
        target_binding.handle().clone(),
        write_input_fields,
        observed_binding.clone(),
    )?;
    Ok((write, admitted))
}

#[expect(
    clippy::too_many_arguments,
    reason = "This boundary retains the original owned publication target facts across asynchronous provenance observation before constructing the complete product intent."
)]
async fn mv_refresh_publication_intent(
    io: &MvRefreshPreparationIo,
    connector_control: &dyn novarocks_spi::connector::ConnectorControlRegistry,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    publication_id: novarocks_spi::connector::LakePublicationId,
    target_object_id: ConnectorTableObjectId,
    expected_target_snapshot_id: Option<i64>,
    admitted: &crate::mv::domain::staged_create::AdmittedMvPublication,
    technique: MvRefreshPublicationTechnique,
    snapshots: &BTreeMap<SqlMvRelationOccurrenceId, (novarocks_types::naming::TableIdentity, i64)>,
    base_table_object_ids: &BTreeMap<SqlMvRelationOccurrenceId, ConnectorTableObjectId>,
    previous_sources: &[RefreshStateBaselineSource],
    definition_fingerprint: String,
    target_catalog: String,
    target_namespace: String,
    target_name: String,
) -> Result<MvRefreshPublicationIntent, PreparationError> {
    // The publication records the window it consumed, so each base carries the
    // point the published baseline pinned it at as well as the point this
    // refresh read. A source this MV has not published before has no
    // predecessor, which is a fact about a first publication rather than a
    // missing one.
    let previous = crate::mv::domain::refresh::planning::baseline_predecessors_original(
        io,
        previous_sources,
        connector_control,
        connector_context,
    )
    .await?;
    let bases = snapshots
        .iter()
        .map(|(occurrence_id, (table, to_snapshot))| {
            MvRefreshPublicationBase::try_new(
                occurrence_id.get(),
                table.fqn(),
                base_table_object_ids
                    .get(occurrence_id)
                    .cloned()
                    .ok_or_else(|| {
                        format!(
                            "MV refresh publication has no object-ID fact for {}",
                            table.fqn()
                        )
                    })?,
                previous.snapshots.get(occurrence_id).copied(),
                *to_snapshot,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    MvRefreshPublicationIntent::try_new(
        publication_id,
        target_object_id,
        expected_target_snapshot_id,
        admitted.admission().clone(),
        technique,
        bases,
        definition_fingerprint,
        target_catalog,
        target_namespace,
        target_name,
    )
    .map(|intent| intent.with_expected_root(admitted.root_binding()))
    .map_err(PreparationError::from)
}

fn project_occurrence_window_facts(
    io: &MvRefreshPreparationIo,
    source: &IcebergMvCorePorts,
    rewrite: &IcebergMvRewriteContext,
    base: &RefreshBaseRelationOccurrence,
    from_snapshot_id: i64,
    to_snapshot_id: i64,
    context: &ConnectorRequestContext,
) -> Result<ConnectorChangeWindowFacts, PreparationError> {
    let (facts, _) = project_prepared_change_window_for_table_original(
        io,
        source.storage_observation(),
        &base.table,
        rewrite.previous_revision(base)?,
        from_snapshot_id,
        to_snapshot_id,
        context,
    )?;
    let (outcome, reason, net_zero_basis, difference) = match &facts {
        ConnectorChangeWindowFacts::NotIncremental(reason) => {
            ("NotIncremental", Some(reason), None, None)
        }
        ConnectorChangeWindowFacts::NetZero(basis) => ("NetZero", None, Some(basis), None),
        ConnectorChangeWindowFacts::Difference {
            negative_weight,
            partition_impact,
            volume,
        } => (
            "Difference",
            None,
            None,
            Some((negative_weight, partition_impact, volume)),
        ),
    };
    let (impact, added_partitions, removed_partitions) =
        match difference.map(|(_, impact, _)| impact) {
            Some(ConnectorChangeWindowPartitionImpact::Exact(partitions)) => (
                Some("Exact"),
                Some(partitions.added().len()),
                Some(partitions.removed().len()),
            ),
            Some(ConnectorChangeWindowPartitionImpact::Unavailable) => {
                (Some("Unavailable"), None, None)
            }
            Some(ConnectorChangeWindowPartitionImpact::Unpartitioned) => {
                (Some("Unpartitioned"), None, None)
            }
            None => (None, None, None),
        };
    tracing::debug!(
        target = %rewrite.target.fqn(),
        occurrence = base.occurrence_id.get(),
        source = %base.table.fqn(),
        from_snapshot_id,
        to_snapshot_id,
        outcome,
        reason = ?reason,
        net_zero_basis = ?net_zero_basis,
        negative_weight = ?difference.map(|(weight, _, _)| weight),
        partition_impact = ?impact,
        added_partitions = ?added_partitions,
        removed_partitions = ?removed_partitions,
        source_event_rows_upper_bound = ?difference.map(|(_, _, volume)| volume.source_event_rows_upper_bound),
        source_data_file_bytes = ?difference.map(|(_, _, volume)| volume.source_data_file_bytes),
        "Projected MV source window facts"
    );
    Ok(facts)
}

/// Compute the same non-aggregate decision for execution and read-only EXPLAIN.
// Design: ADR-0278 (docs/adr/ADR-0278-mv-maintains-visible-tuples-and-replays-verification.md)
pub(crate) async fn choose_content_strategy(
    io: &MvRefreshPreparationIo,
    source: &IcebergMvCorePorts,
    rewrite: &IcebergMvRewriteContext,
    target: &MvTargetBinding,
    context: &ConnectorRequestContext,
    can_incrementally_refresh: bool,
    budget: u64,
    optimizer_settings: &novarocks_sql::compiler::SessionOptimizerSettings,
) -> Result<
    (
        RefreshStrategyDecision,
        Option<super::super::partition_replace::PreparedPartitionReplacement>,
    ),
    PreparationError,
> {
    let windows = rewrite
        .base_refs
        .iter()
        .map(|base| {
            project_occurrence_window_facts(
                io,
                source,
                rewrite,
                base,
                rewrite.previous_snapshot_id(base)?,
                rewrite.pinned_snapshot_id(base)?,
                context,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let window_refs = windows.iter().collect::<Vec<_>>();
    let is_join = definition_has_join(&rewrite.canonical_select_query);
    let facts = RefreshStrategyFacts {
        windows: &window_refs,
        force_full: false,
        pending_verification: false,
        can_incrementally_refresh,
        append_admitted: !is_join
            || crate::mv::domain::iceberg_join_branch::is_append_only_join_delta_eligible(
                &rewrite.canonical_select_query,
            ),
        partition_admitted: false,
        complete_to_available: true,
    };
    let admission = if facts.can_locate_content() {
        estimate_content_admission(
            io, source, rewrite, target, context, &windows, is_join, budget,
        )
        .await?
    } else {
        Err("MV content location is ineligible for the observed source windows".to_string())
    };
    let mut decision = facts.decide(|| admission)?;
    if decision.strategy == RefreshStrategy::MetadataOnly || !can_incrementally_refresh {
        return Ok((decision, None));
    }
    let partition = super::super::partition_replace::prepare_partition_replacement(
        io,
        source,
        rewrite,
        target.partition(),
        context,
        &windows,
    )
    .await?;
    let mut candidates = vec![(decision.strategy.clone(), None)];
    if partition.is_some() {
        candidates.push((RefreshStrategy::PartitionReplace, partition.as_ref()));
    }
    if decision.strategy != RefreshStrategy::FullReplace {
        candidates.push((RefreshStrategy::FullReplace, None));
    }
    if candidates.len() == 1 {
        return Ok((decision, None));
    }
    let mut best = None;
    for (strategy, scoped) in candidates {
        let cost = super::super::refresh_cost::cost_candidate(
            io,
            source,
            rewrite,
            target,
            context,
            optimizer_settings,
            &strategy,
            scoped,
        )
        .await?;
        if best.is_none_or(|best| cost < best) {
            decision.strategy = strategy.clone();
            best = Some(cost);
        }
        decision.costs.push((strategy, cost));
    }
    let partition = partition.filter(|_| decision.strategy == RefreshStrategy::PartitionReplace);
    Ok((decision, partition))
}

async fn estimate_content_admission(
    io: &MvRefreshPreparationIo,
    source: &IcebergMvCorePorts,
    rewrite: &IcebergMvRewriteContext,
    target: &MvTargetBinding,
    context: &ConnectorRequestContext,
    windows: &[ConnectorChangeWindowFacts],
    is_join: bool,
    budget: u64,
) -> Result<
    Result<novarocks_mv_application::persistence::codec::PendingVerificationRequirement, String>,
    PreparationError,
> {
    use crate::query_execution::mv_assembly::content_estimate::{
        ContentEstimateColumn, ContentJoinAmplification, ContentSourceStatistics,
        estimate_content_apply, has_average_size, needs_average_size,
    };
    use novarocks_mv_application::persistence::codec::{
        MappingPushdownClass, PendingVerificationRequirement,
    };
    use novarocks_spi::connector::{StatisticsMetric, StatisticsMetricRequest};

    let fields = rewrite.content_target_fields(target.metadata())?;
    let declarations = fields
        .iter()
        .map(|field| field.read_column.declared_value_type())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    if let Err(cause) = novarocks_sql::planning::mv::content_route_keys(&declarations) {
        return Ok(Err(cause.to_string()));
    }
    if rewrite.target_snapshot_id.is_none() {
        return Ok(Err(
            "MV content location has no retained target snapshot".to_string()
        ));
    }

    let target_metrics = fields
        .iter()
        .zip(&declarations)
        .filter(|(_, declaration)| needs_average_size(declaration))
        .map(|(field, _)| StatisticsMetric::AverageSize {
            column: Arc::from(field.read_column.name.as_str()),
        })
        .collect::<Vec<_>>();
    let target_statistics = if target_metrics.is_empty() {
        None
    } else {
        io.content_statistics(
            target.planning_pin(),
            target.metadata(),
            StatisticsMetricRequest::try_new(target_metrics)?,
            context,
        )
        .await?
    };

    let captured = io.captured_sources();
    let mut coordinates = vec![None; fields.len()];
    let mut source_metrics = BTreeMap::<u32, std::collections::BTreeSet<StatisticsMetric>>::new();
    let mapping = &rewrite.mv_definition.facts.mapping().columns;
    if let (Some(direct), Some(analyzed)) =
        (captured.direct_output_sources(), captured.analyzed_fields())
    {
        for (index, ((output, _), declaration)) in rewrite
            .runtime_bindings
            .outputs
            .iter()
            .zip(&declarations)
            .enumerate()
        {
            if !needs_average_size(declaration)
                || mapping[index].pushdown != MappingPushdownClass::Identity
            {
                continue;
            }
            if target_statistics
                .as_ref()
                .is_some_and(|evidence| has_average_size(evidence, &fields[index].read_column.name))
            {
                continue;
            }
            let Some(reference) = direct.get(output) else {
                continue;
            };
            let Some(field) = analyzed
                .get(&reference.occurrence_id)
                .and_then(|fields| {
                    fields
                        .iter()
                        .find(|field| field.field_id == reference.field_id)
                })
                .filter(|field| field.source_type == mapping[index].storage_type)
            else {
                continue;
            };
            let name = field.name.as_str();
            source_metrics
                .entry(reference.occurrence_id)
                .or_default()
                .insert(StatisticsMetric::AverageSize {
                    column: Arc::from(name),
                });
            coordinates[index] = Some((reference.occurrence_id, name));
        }
    }
    let mut source_statistics = BTreeMap::new();
    for (occurrence_id, metrics) in source_metrics {
        let base = rewrite
            .base_refs
            .iter()
            .find(|base| base.occurrence_id.get() == occurrence_id)
            .ok_or("MV content statistics lost its exact source occurrence")?;
        let pinned = captured.require(&base.table)?;
        let metadata = pinned.metadata()?;
        if let Some(evidence) = io
            .content_statistics(
                &pinned.planning_pin,
                &metadata,
                StatisticsMetricRequest::try_new(metrics.into_iter().collect())?,
                context,
            )
            .await?
        {
            source_statistics.insert(occurrence_id, evidence);
        }
    }
    let columns = fields
        .iter()
        .zip(&declarations)
        .zip(coordinates)
        .map(|((field, declaration), coordinate)| ContentEstimateColumn {
            storage_column: field.read_column.name.as_str(),
            storage_type: declaration,
            source: coordinate.and_then(|(occurrence_id, source_column)| {
                source_statistics
                    .get(&occurrence_id)
                    .map(|statistics| ContentSourceStatistics {
                        occurrence_id,
                        source_column,
                        statistics,
                    })
            }),
        })
        .collect::<Vec<_>>();
    let join = if is_join {
        crate::query_execution::mv_assembly::content_estimate::estimate_join_amplification(
            io, source, rewrite, context, windows,
        )
        .await?
    } else {
        ContentJoinAmplification::NotJoined
    };
    let estimate =
        match estimate_content_apply(windows.iter(), &columns, target_statistics.as_ref(), join) {
            Ok(estimate) => estimate,
            Err(cause) => return Ok(Err(cause.to_string())),
        };
    if i64::try_from(budget).is_err() {
        return Ok(Err(format!(
            "MV deployment memory budget {budget} bytes exceeds the Native option range"
        )));
    }
    if estimate.estimate_bytes > u128::from(budget) {
        return Ok(Err(format!(
            "MV content location estimate {} bytes exceeds its deployment budget {budget} bytes",
            estimate.estimate_bytes,
        )));
    }
    Ok(
        PendingVerificationRequirement::content_locate(estimate.estimate_bytes as u64, budget)
            .map_err(|cause| cause.to_string()),
    )
}

async fn prepare_frontend_incremental_write(
    io: &MvRefreshPreparationIo,
    captured: &mut CapturedPreparation,
    input: &MvRefreshPreparationInput,
    contract: &RefreshPlanContract,
    canonical_query: &novarocks_parser::ast::Query,
    attempt: &MvRefreshAttemptIdentity,
    can_incrementally_refresh: bool,
    original_current: &novarocks_mv_application::persistence::keyed_documents::MvKeyedObservedCurrentDocuments,
    replay: Option<&Arc<PendingFence>>,
    observed_binding: ConnectorProviderBindingKey,
) -> Result<PreparedIncrementalRefreshWork, PreparationError> {
    let source = &input.source;
    let current_catalog = input.current_catalog.as_deref();
    let current_database = input.current_database.as_str();
    let connector_context = input.connector_context.clone();
    let nonaggregate_admission = (
        can_incrementally_refresh,
        input.query_memory_budget_bytes,
        &input.optimizer_settings,
    );
    let target = IcebergMvTarget {
        catalog: contract.target.catalog.clone().ok_or_else(|| {
            "Iceberg MV incremental refresh target has no connector catalog".to_string()
        })?,
        namespace: contract.target.database.clone(),
        table: contract.target.name.clone(),
    };
    let ready = io.definition(source, &target).await?;
    let (projection, recovery_management) = if let Some(fence) = replay {
        let installed = ready.facts.source_revision();
        if installed.target != original_current.view.table
            || installed.target_object_id != original_current.view.object_id
            || installed.deployment_owner != original_current.deployment_owner
            || installed.process_incarnation != original_current.process_incarnation
            || ready.facts.versions() != &original_current.versions
            || ready
                .facts
                .publication()
                .is_some_and(|record| &record.publication_id != fence.publication())
        {
            return Err("MV strict recovery lost the installed management binding".into());
        }
        fence.verify(original_current)?;
        (
            StoredKeyedMvProjection {
                mv_id: ready.mv_id,
                facts: novarocks_mv_application::persistence::keyed_documents::MvKeyedDocumentProjection::try_from_current(original_current.clone())?,
            },
            Some(ready),
        )
    } else {
        (ready, None)
    };

    let interpretation = projection.facts.interpretation();
    let is_join = definition_has_join(canonical_query);
    let is_aggregate = !interpretation.aggregates.is_empty();
    let is_branch_union = !interpretation.branches.is_empty();
    let join_bases = if is_join {
        let (left, right) =
            join_base_refs_for_definition(&projection, canonical_query, &contract.base_refs)?;
        Some((left.clone(), right.clone()))
    } else {
        None
    };
    let RefreshStateBaseline::SnapshotBacked {
        target_snapshot_id,
        definition_fingerprint,
        ..
    } = &contract.state_baseline;
    let definition_occurrences = &projection.facts.binding().occurrences;
    if definition_occurrences.len() != contract.base_refs.len() {
        return Err(
            "MV incremental refresh base facts do not retain every D occurrence"
                .to_string()
                .into(),
        );
    }
    let mut pin_occurrences = Vec::with_capacity(definition_occurrences.len());
    for (occurrence, base) in definition_occurrences.iter().zip(&contract.base_refs) {
        if occurrence.occurrence_id != base.occurrence_id.get()
            || occurrence.catalog_at_binding != base.table.catalog
            || occurrence.namespace_at_binding != base.table.namespace
            || occurrence.relation_at_binding != base.table.table
        {
            return Err(PreparationError::user(format!(
                "MV incremental refresh base does not match D occurrence {}",
                occurrence.occurrence_id
            )));
        }
        let named = base.display();
        let snapshot_id = require_source_snapshot(
            contract
                .snapshot_pins
                .get(&base.occurrence_id)
                .copied()
                .flatten(),
            "MV incremental refresh",
            &named,
        )?;
        let (observed, exact_revision) = io
            .current_revision(source, &base.table, &connector_context)
            .await?;
        let (persisted_object, _) = persist_exact_connector_revision(&exact_revision)
            .map_err(|error| format!("persist incremental-refresh source identity: {error}"))?;
        validate_source_snapshot(
            occurrence,
            &persisted_object,
            snapshot_id,
            observed.current_snapshot_id(),
            "MV incremental refresh",
            &named,
        )?;
        pin_occurrences.push(RefreshSnapshotPinOccurrence::try_new(
            base.occurrence_id,
            base.table.clone(),
            snapshot_id,
            observed.object_id().clone(),
            exact_revision,
        )?);
    }
    let pin = RefreshSnapshotPin::try_from_occurrences(pin_occurrences)?;
    let (projection, _refresh_query_source) = io
        .rebind_definition(
            source,
            &projection,
            &contract_base_tables(contract),
            &target,
            None,
            &connector_context,
        )
        .await?;
    let target_binding = io
        .target_binding(source, &target, &connector_context)
        .await?;
    captured.target = Some(target_binding.clone());
    let target_schema_validation =
        crate::mv::domain::storage_observation::observe_schema_validation(
            source.storage_observation(),
            target_binding.planning_pin(),
            target_binding.metadata(),
            connector_context.clone(),
        )
        .map_err(PreparationError::from)?;
    let runtime_bindings = validate_projection_target(&projection, &target_schema_validation)?;
    if projection.facts.publication().is_some() {
        novarocks_mv_application::persistence::aggregate_bindings::validate_published_aggregate_contracts(projection.facts.interpretation())?;
    }
    // An incremental refresh of an aggregate view needs the same analyzed
    // aggregate facts a first refresh does: L says the view aggregates, and a
    // rewrite that cannot say which SQL call each aggregate identity belongs
    // to has nothing to maintain the state columns with.
    let aggregate = frozen_refresh_aggregate_analysis(
        io,
        source,
        &projection,
        canonical_query,
        &connector_context,
    )
    .await?;
    let mut rewrite =
        crate::mv::domain::refresh::rewrite_context::freeze_refresh_rewrite_context_original(
            io,
            RefreshRewriteInputs {
                rebuild_layout: None,
                recovery_publication: replay.and_then(|fence| fence.recovery_publication()),
                connector_control: source.connector_control(),
                storage_observation: source.storage_observation(),
                connector_context: &connector_context,
                projection: Arc::new(projection),
                pin: &pin,
                state_baseline: &contract.state_baseline,
                target_binding: &target_binding,
                target_observation: &target_schema_validation,
                runtime_bindings: &runtime_bindings,
                join_analysis: novarocks_sql::planning::mv::extract_refresh_join_analysis(
                    canonical_query,
                )?,
                aggregate,
            },
        )
        .await?;

    if !is_aggregate {
        let (decision, replacement) = if let Some(fence) = replay {
            (
                RefreshStrategyDecision {
                    strategy: RefreshStrategy::ContentLocate(fence.requirement()),
                    content_rejection: None,
                    costs: Vec::new(),
                },
                None,
            )
        } else {
            choose_content_strategy(
                io,
                source,
                &rewrite,
                &target_binding,
                &connector_context,
                nonaggregate_admission.0,
                nonaggregate_admission.1,
                nonaggregate_admission.2,
            )
            .await?
        };
        let strategy = decision.strategy.clone();
        captured.strategy = Some(decision);
        if strategy == RefreshStrategy::MetadataOnly {
            return Ok(PreparedIncrementalRefreshWork::MetadataOnly);
        }
        if matches!(
            strategy,
            RefreshStrategy::FullReplace | RefreshStrategy::PartitionReplace
        ) {
            let overlays = replacement.as_ref().map(|proof| proof.reads.clone());
            let (rebuild, mut admitted) = prepare_frontend_first_refresh_write(
                io,
                captured,
                source,
                current_catalog,
                current_database,
                FirstRefreshTargetFacts {
                    contract,
                    canonical_query,
                    base_table_object_ids: &rewrite.pinned_objects_by_occurrence()?,
                    repartition_transition: None,
                    retained_repartition_target: None,
                    rebuild: None,
                },
                attempt,
                observed_binding,
                connector_context,
            )
            .await?;
            if let (RefreshStrategy::PartitionReplace, Some(proof), Some(overlays)) =
                (&strategy, replacement, overlays)
            {
                admitted.bind_partition_replace(proof.target)?;
                return Ok(PreparedIncrementalRefreshWork::PartitionReplace {
                    write: rebuild.into_full_overwrite(),
                    admitted,
                    overlays,
                });
            }
            if strategy == RefreshStrategy::PartitionReplace {
                return Err(
                    "MV partition replacement lost its admitted source or target scope".into(),
                );
            }
            return Ok(PreparedIncrementalRefreshWork::FullRebuild(
                rebuild.into_full_overwrite(),
                admitted,
            ));
        }
        let mut verified_current = None;
        let mode = match strategy {
            RefreshStrategy::AppendOnly => MvIncrementalWriteMode::FastAppend,
            RefreshStrategy::ContentLocate(_) if replay.is_some() => {
                let fence = replay
                    .expect("strict recovery reuses the original fence")
                    .clone();
                fence.verify(original_current)?;
                io.retain_pending_fence(
                    Arc::clone(&fence),
                    target_binding.planning_pin().clone(),
                    Arc::new(rewrite.mv_definition.facts.clone()),
                )?;
                verified_current = Some((fence, original_current.clone()));
                MvIncrementalWriteMode::RowDelta
            }
            RefreshStrategy::ContentLocate(requirement) => {
                let fence = Arc::new(PendingFence::freeze(
                    original_current,
                    &rewrite,
                    attempt,
                    requirement,
                )?);
                let planning = target_binding.planning_pin();
                io.retain_pending_fence(
                    Arc::clone(&fence),
                    planning.clone(),
                    Arc::new(rewrite.mv_definition.facts.clone()),
                )?;
                let documents = planning.derive_document_storage_lease()?;
                let observed = io
                    .observe_current(
                        source,
                        &documents,
                        crate::mv::domain::staged_create::keyed_request(
                            &documents,
                            original_current.view.table.clone(),
                            original_current.view.object_id.clone(),
                            connector_context.clone(),
                        ),
                    )
                    .await?;
                if observed.view != original_current.view {
                    return Err("MV Current changed before its Pending fence".into());
                }
                io.write_binding_marker(
                    source,
                    planning,
                    observed,
                    &rewrite.mv_definition.facts,
                    fence.marker(),
                    connector_context.clone(),
                )
                .await?;
                let observed = io
                    .observe_current(
                        source,
                        &documents,
                        crate::mv::domain::staged_create::keyed_request(
                            &documents,
                            original_current.view.table.clone(),
                            original_current.view.object_id.clone(),
                            connector_context.clone(),
                        ),
                    )
                    .await?;
                fence.verify(&observed)?;
                verified_current = Some((fence, observed));
                MvIncrementalWriteMode::RowDelta
            }
            _ => unreachable!(
                "non-writing refresh strategies returned before change-stream preparation"
            ),
        };
        let (target_binding, pending) = if let Some((fence, observed)) = verified_current {
            // Pending changes the catalog pointer even though main still names the
            // same snapshot. Position deletes must freeze the complete generation
            // that actually contains the verified fence.
            let planning = target_binding.planning_pin();
            let identity = target_binding.identity();
            let current = io
                .source_table_metadata(
                    planning,
                    connector_context.clone(),
                    &identity.namespace,
                    &identity.table,
                )
                .await?;
            let schema = crate::mv::domain::storage_observation::observe_schema_validation(
                source.storage_observation(),
                planning,
                &current,
                connector_context.clone(),
            )?;
            if schema.exact_schema().metadata_version != observed.view.metadata_version {
                return Err("MV Current changed after its Pending fence was verified".into());
            }
            let projection = StoredKeyedMvProjection {
                mv_id: rewrite.mv_definition.mv_id,
                facts: novarocks_mv_application::persistence::keyed_documents::MvKeyedDocumentProjection::try_from_current(observed)?,
            };
            let bindings = validate_projection_target(&projection, &schema)?;
            if projection.facts.versions() != rewrite.mv_definition.facts.versions()
                || projection.facts.publication() != rewrite.mv_definition.facts.publication()
                || bindings != rewrite.runtime_bindings
            {
                return Err(
                    "MV Pending fence changed the exact logical or storage bindings".into(),
                );
            }
            Arc::get_mut(&mut rewrite)
                .expect("the refresh has not shared its frozen rewrite")
                .mv_definition = Arc::new(projection);
            let current = crate::mv::domain::refresh::target_binding::complete_target_binding(
                source.storage_observation(),
                planning.clone(),
                current,
                &connector_context,
            )?;
            captured.target = Some(current.clone());
            (current, Some(fence))
        } else {
            (target_binding, None)
        };
        captured.rewrite = Some(Arc::clone(&rewrite));
        return prepare_canonical_incremental_write(
            io,
            source,
            &rewrite,
            &target_binding,
            recovery_management.as_ref().map(|ready| {
                // Management still owns its installed dependencies. The
                // Provider write is fenced against the proved Current.
                ready
                    .facts
                    .management_dependencies(target_binding.planning_pin().control_runtime_id())
            }),
            target,
            contract,
            attempt,
            current_database,
            *target_snapshot_id,
            observed_binding,
            connector_context,
            pending,
            mode,
            MvIncrementalRewriteEvidence::None,
        )
        .await;
    }

    captured.rewrite = Some(Arc::clone(&rewrite));
    if let Some((left_ref, right_ref)) = join_bases.as_ref() {
        let left_from = rewrite.previous_snapshot_id(left_ref)?;
        let right_from = rewrite.previous_snapshot_id(right_ref)?;
        let left_to = rewrite.pinned_snapshot_id(left_ref)?;
        let right_to = rewrite.pinned_snapshot_id(right_ref)?;
        let left_facts = project_occurrence_window_facts(
            io,
            source,
            &rewrite,
            left_ref,
            left_from,
            left_to,
            &connector_context,
        )?;
        let right_facts = project_occurrence_window_facts(
            io,
            source,
            &rewrite,
            right_ref,
            right_from,
            right_to,
            &connector_context,
        )?;
        let left_window = occurrence_window(&left_facts);
        let right_window = occurrence_window(&right_facts);
        let mut full_rebuild_reasons = Vec::new();
        for (side, base, window) in [
            ("left", left_ref, left_window),
            ("right", right_ref, right_window),
        ] {
            if let Err(reason) = window {
                full_rebuild_reasons.push(format!(
                    "{side} {}: {}",
                    base.display(),
                    not_incremental_reason_message(reason),
                ));
            }
        }
        if !full_rebuild_reasons.is_empty() {
            tracing::info!(
                target = %rewrite.target.fqn(),
                reasons = %full_rebuild_reasons.join("; "),
                "MV join refresh admission selected a distributed full-rebuild staging overwrite"
            );
            let (rebuild, admitted) = prepare_frontend_first_refresh_write(
                io,
                captured,
                source,
                current_catalog,
                current_database,
                FirstRefreshTargetFacts {
                    contract,
                    canonical_query,
                    base_table_object_ids: &rewrite.pinned_objects_by_occurrence()?,
                    repartition_transition: None,
                    retained_repartition_target: None,
                    rebuild: None,
                },
                attempt,
                observed_binding,
                connector_context,
            )
            .await?;
            return Ok(PreparedIncrementalRefreshWork::FullRebuild(
                rebuild.into_full_overwrite(),
                admitted,
            ));
        }
        let left_window = left_window.expect("non-incremental left window returned above");
        let right_window = right_window.expect("non-incremental right window returned above");
        let branches = crate::mv::domain::iceberg_join_branch::plan_join_delta_branches(
            &left_ref.table,
            &right_ref.table,
            crate::mv::domain::iceberg_join_branch::SnapshotWindow {
                from: left_from,
                to: left_to,
            },
            crate::mv::domain::iceberg_join_branch::SnapshotWindow {
                from: right_from,
                to: right_to,
            },
            matches!(left_window, OccurrenceWindow::Changed { .. }),
            matches!(right_window, OccurrenceWindow::Changed { .. }),
        );
        if branches.is_empty() {
            return Ok(PreparedIncrementalRefreshWork::MetadataOnly);
        }
        let join_mode = if is_aggregate {
            MvIncrementalJoinMode::Coalesce
        } else {
            select_join_incremental_execution_mode(
                left_window.negative_weight_possible(),
                right_window.negative_weight_possible(),
            )
        };
        let request = MvIncrementalWriteRequest::try_new(
            target.catalog.clone(),
            target.namespace.clone(),
            target.table.clone(),
            attempt.staging_branch(),
            current_database.to_string(),
            *target_snapshot_id,
            observed_binding,
            attempt.write_operation_id(),
            target_binding.handle().clone(),
        )?;
        let admitted_publication = io
            .admit(
                source,
                target_binding.planning_pin(),
                rewrite.mv_definition.as_ref(),
                None,
                attempt.publication_id,
                None,
                &connector_context,
            )
            .await?;
        let publication_intent = mv_refresh_publication_intent(
            io,
            source.connector_control(),
            &connector_context,
            attempt.publication_id,
            rewrite
                .mv_definition
                .facts
                .source_revision()
                .target_object_id
                .clone(),
            *target_snapshot_id,
            &admitted_publication,
            MvRefreshPublicationTechnique::Incremental,
            &rewrite.pinned_snapshots_by_occurrence()?,
            &rewrite.pinned_objects_by_occurrence()?,
            baseline_previous_sources(&contract.state_baseline),
            definition_fingerprint.clone(),
            target.catalog.clone(),
            target.namespace.clone(),
            target.table.clone(),
        )
        .await?;
        let frozen_base_overlays = crate::query_execution::mv_assembly::query_local_bindings::freeze_imv_base_query_local_overlays_from_captured_inputs_original(io,
            source.connector_control(),
            &connector_context,
            &rewrite,
        ).await?;
        let admitted_publication = AdmittedMvDataPublication::try_new(
            admitted_publication,
            &rewrite.exact_revisions_by_occurrence(),
        )?;
        return MvIncrementalWritePreparer::prepare(
            request,
            crate::query_execution::mv_assembly::first_refresh_staging::frozen_logical_context_from_rewrite(
                &rewrite,
                contract.affected_partitions.clone(),
                Some(frozen_base_overlays),
            )?,
            match join_mode {
                MvIncrementalJoinMode::AppendOnly => {
                    MvIncrementalWriteMode::FastAppend
                }
                MvIncrementalJoinMode::Coalesce => {
                    MvIncrementalWriteMode::RowDelta
                }
            },
            if is_aggregate {
                MvIncrementalRewriteEvidence::JoinAggregate
            } else {
                MvIncrementalRewriteEvidence::None
            },
            MvIncrementalExecutionArtifact::JoinLogical {
                mode: match join_mode {
                    MvIncrementalJoinMode::AppendOnly => {
                        MvIncrementalJoinMode::AppendOnly
                    }
                    MvIncrementalJoinMode::Coalesce => {
                        MvIncrementalJoinMode::Coalesce
                    }
                },
            },
            publication_intent,
        )
        .map(|write| PreparedIncrementalRefreshWork::ChangeStream(write, admitted_publication))
        .map_err(PreparationError::from);
    }

    let mut loaded_bases = Vec::with_capacity(rewrite.base_refs.len());
    for base in rewrite.base_refs.iter() {
        let previous_snapshot_id = rewrite.previous_snapshot_id(base)?;
        let current_snapshot_id = rewrite.pinned_snapshot_id(base)?;
        let current_table_object_id = rewrite.pinned_table_object_id(base)?;
        let observed = io.schema(source, &base.table, &connector_context).await?;
        if observed.table_object_id() != &current_table_object_id {
            return Err(format!(
                "MV incremental refresh base table identity changed after planning for {}",
                base.display()
            )
            .into());
        }
        let window_facts = project_occurrence_window_facts(
            io,
            source,
            &rewrite,
            base,
            previous_snapshot_id,
            current_snapshot_id,
            &connector_context,
        )?;
        loaded_bases.push((
            base,
            current_snapshot_id,
            current_table_object_id,
            window_facts,
        ));
    }
    let changes = loaded_bases
        .iter()
        .map(
            |(base_ref, current_snapshot_id, current_table_object_id, window_facts)| {
                NonJoinBaseChange {
                    occurrence_id: base_ref.occurrence_id,
                    base_ref: &base_ref.table,
                    current_snapshot_id: *current_snapshot_id,
                    current_table_object_id,
                    window_facts: window_facts.clone(),
                }
            },
        )
        .collect::<Vec<_>>();
    let (mode, evidence) = match plan_non_join_incremental_changes(&changes)? {
        NonJoinIncrementalChangePlan::MetadataOnly(_) => {
            return Ok(PreparedIncrementalRefreshWork::MetadataOnly);
        }
        NonJoinIncrementalChangePlan::FullRebuild { reasons, .. } => {
            for reason in reasons {
                tracing::info!(
                    target = %rewrite.target.fqn(),
                    reason = %reason.log_message(),
                    "MV refresh SQL preparation selected a distributed full-rebuild staging overwrite"
                );
            }
            let (rebuild, admitted) = prepare_frontend_first_refresh_write(
                io,
                captured,
                source,
                current_catalog,
                current_database,
                FirstRefreshTargetFacts {
                    contract,
                    canonical_query,
                    base_table_object_ids: &rewrite.pinned_objects_by_occurrence()?,
                    repartition_transition: None,
                    retained_repartition_target: None,
                    rebuild: None,
                },
                attempt,
                observed_binding,
                connector_context,
            )
            .await?;
            return Ok(PreparedIncrementalRefreshWork::FullRebuild(
                rebuild.into_full_overwrite(),
                admitted,
            ));
        }
        NonJoinIncrementalChangePlan::ChangeStream {
            negative_weight, ..
        } => {
            let mode = non_join_incremental_write_mode(
                is_aggregate,
                negative_weight == ConnectorChangeWindowNegativeWeight::Possible,
            );
            let evidence = if is_aggregate {
                if is_branch_union {
                    MvIncrementalRewriteEvidence::BranchUnionAggregate
                } else {
                    MvIncrementalRewriteEvidence::Aggregate
                }
            } else {
                MvIncrementalRewriteEvidence::None
            };
            (mode, evidence)
        }
    };
    prepare_canonical_incremental_write(
        io,
        source,
        &rewrite,
        &target_binding,
        None,
        target,
        contract,
        attempt,
        current_database,
        *target_snapshot_id,
        observed_binding,
        connector_context,
        None,
        mode,
        evidence,
    )
    .await
}

#[expect(
    clippy::too_many_arguments,
    reason = "Each original plan, observation, and destination is explicit at write admission."
)]
async fn prepare_canonical_incremental_write(
    io: &MvRefreshPreparationIo,
    source: &IcebergMvCorePorts,
    rewrite: &IcebergMvRewriteContext,
    target_binding: &MvTargetBinding,
    recovery_management_dependencies: Option<
        novarocks_mv_application::management::ManagementDependencySet,
    >,
    target: IcebergMvTarget,
    contract: &RefreshPlanContract,
    attempt: &MvRefreshAttemptIdentity,
    current_database: &str,
    target_snapshot_id: Option<i64>,
    observed_binding: ConnectorProviderBindingKey,
    connector_context: ConnectorRequestContext,
    pending: Option<Arc<PendingFence>>,
    mode: MvIncrementalWriteMode,
    evidence: MvIncrementalRewriteEvidence,
) -> Result<PreparedIncrementalRefreshWork, PreparationError> {
    let RefreshStateBaseline::SnapshotBacked {
        definition_fingerprint,
        ..
    } = &contract.state_baseline;
    let request = MvIncrementalWriteRequest::try_new(
        target.catalog.clone(),
        target.namespace.clone(),
        target.table.clone(),
        attempt.staging_branch(),
        current_database.to_string(),
        target_snapshot_id,
        observed_binding,
        attempt.write_operation_id(),
        target_binding.handle().clone(),
    )?;
    let mut admitted_publication = io
        .admit(
            source,
            target_binding.planning_pin(),
            rewrite.mv_definition.as_ref(),
            recovery_management_dependencies,
            attempt.publication_id,
            None,
            &connector_context,
        )
        .await?;
    if let Some(pending) = pending {
        admitted_publication.bind_content_pending(pending)?;
    }
    let publication_intent = mv_refresh_publication_intent(
        io,
        source.connector_control(),
        &connector_context,
        attempt.publication_id,
        rewrite
            .mv_definition
            .facts
            .source_revision()
            .target_object_id
            .clone(),
        target_snapshot_id,
        &admitted_publication,
        MvRefreshPublicationTechnique::Incremental,
        &rewrite.pinned_snapshots_by_occurrence()?,
        &rewrite.pinned_objects_by_occurrence()?,
        baseline_previous_sources(&contract.state_baseline),
        definition_fingerprint.clone(),
        target.catalog.clone(),
        target.namespace.clone(),
        target.table.clone(),
    )
    .await?;
    let frozen_base_overlays = crate::query_execution::mv_assembly::query_local_bindings::freeze_imv_base_query_local_overlays_from_captured_inputs_original(io,
        source.connector_control(),
        &connector_context,
        rewrite,
    ).await?;
    let admitted_publication = AdmittedMvDataPublication::try_new(
        admitted_publication,
        &rewrite.exact_revisions_by_occurrence(),
    )?;
    MvIncrementalWritePreparer::prepare(
        request,
        crate::query_execution::mv_assembly::first_refresh_staging::frozen_logical_context_from_rewrite(
            rewrite,
            contract.affected_partitions.clone(),
            Some(frozen_base_overlays),
        )?,
        mode,
        evidence,
        MvIncrementalExecutionArtifact::CanonicalQuery,
        publication_intent,
    )
    .map(|write| PreparedIncrementalRefreshWork::ChangeStream(write, admitted_publication))
    .map_err(PreparationError::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_spi::connector::{
        ConnectorError, ConnectorManagedPartitionTransform, ConnectorOperationControl,
    };

    struct Active;
    impl ConnectorOperationControl for Active {
        fn check_active(&self) -> Result<(), ConnectorError> {
            Ok(())
        }
    }

    #[test]
    fn typed_repartition_binds_a_visible_physical_field_before_advancing() {
        let visible = ["id".into()].into_iter().collect();
        let physical = [("id".into(), 8), ("hidden_key".into(), 2)];
        let parse = |field: Option<&str>, _index| {
            novarocks_connector_iceberg::table_properties::parse_table_partitioning_element(
                field.expect("non-NULL fixture field"),
                &Active,
            )
        };
        let fields = bind_property_repartition_fields(
            &[Some("bucket(id, 16)".into())],
            &visible,
            &physical,
            &Active,
            parse,
        )
        .unwrap();
        assert_eq!(fields[0].source_field_id(), 8);
        assert_eq!(fields[0].position(), 0);
        assert_eq!(
            fields[0].transform(),
            ConnectorManagedPartitionTransform::Bucket { buckets: 16 },
        );
        let error = bind_property_repartition_fields(
            &[Some("hidden_key".into()), Some("?".into())],
            &visible,
            &physical,
            &Active,
            parse,
        )
        .unwrap_err();
        assert!(error.to_string().contains("must be an output column"));
    }
}
