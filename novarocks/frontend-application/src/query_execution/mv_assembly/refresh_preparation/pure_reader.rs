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
use crate::mv::domain::application::MvApplicationError;
use crate::mv::domain::refresh::target_binding::MvTargetBinding;
use crate::mv::domain::rewrite::context::IcebergMvRewriteContext;

pub(crate) struct MvRefreshPreparationInput {
    pub(crate) source: IcebergMvCorePorts,
    pub(crate) current_catalog: Option<String>,
    pub(crate) current_database: String,
    pub(crate) statement: MvRefreshRequest,
    pub(crate) connector_context: novarocks_spi::connector::ConnectorRequestContext,
    pub(crate) repartition_fields: Option<Vec<IcebergPartitionFieldExpr>>,
    pub(crate) request: MvRefreshPreparationRequest,
}

/// These are the exact final preparation observations, not a new Current proof.
/// The activation and publication owners retain their own fresh validation.
pub(crate) struct MvRefreshPrepared {
    pub(crate) prepared: PreparedMvRefresh,
    pub(crate) target_binding: Option<MvTargetBinding>,
    pub(crate) rewrite: Option<Arc<IcebergMvRewriteContext>>,
}
#[derive(Default)]
struct CapturedPreparation {
    target: Option<MvTargetBinding>,
    rewrite: Option<Arc<IcebergMvRewriteContext>>,
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
    let repartition_fields = input.repartition_fields.as_deref();
    let request = input.request.clone();
    let mut captured = CapturedPreparation::default();

    request.validate().map_err(PreparationError::user)?;
    if request.statement != statement.sql_refresh_statement() {
        return Err(PreparationError::user(
            "MV refresh preparation statement does not match the admitted SQL request",
        ));
    }
    let mut plan = crate::mv::domain::iceberg_refresh::async_preparation::plan_iceberg_mv_refresh_with_connector_context(
            io,
            source,
            current_catalog,
            current_database,
            statement,
            request.target.clone(),
            repartition_fields.is_some(),
            connector_context,
        ).await?;
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
        Some(retained) => execution_binding_key_for_target(retained.binding.lease()),
        None => source
            .connector_control()
            .observe_current_binding(&instance_id)
            .map_err(PreparationError::from)?,
    };
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
    let full_overwrite = statement.full;
    let work = match plan.contract.decision {
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
                    base_table_object_ids: &base_table_object_ids,
                    repartition_transition: repartition_transition.as_ref(),
                    retained_repartition_target: retained_repartition_target.as_ref(),
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
            source,
            current_catalog,
            current_database,
            &plan.contract,
            &request.attempt,
            observed_binding.clone(),
            connector_context.clone(),
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
    let layout = build_aggregate_layout_for_refresh_select_sql(
        io,
        source,
        Some(resolution.default_catalog.as_str()),
        &resolution.default_namespace,
        &novarocks_parser::printer::print_query(&representative),
        connector_context,
    )
    .await?;
    Ok(Some((calls, layout)))
}

async fn build_aggregate_layout_for_refresh_select_sql(
    io: &MvRefreshPreparationIo,
    ports: &IcebergMvCorePorts,
    current_catalog: Option<&str>,
    current_database: &str,
    select_sql: &str,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<
    novarocks_sql::planning::mv_aggregate_layout::SqlMvAggregatePhysicalLayout,
    PreparationError,
> {
    let visible_query = parse_mv_select_query(select_sql)?;
    let captured_catalog = capture_catalog(
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
    )
    .await?;
    let visible_analysis = crate::mv::domain::analysis_adapter::analyze_mv_select_with_provider(
        current_catalog,
        captured_catalog.catalog.provider(),
        current_database,
        &visible_query,
        ports.function_catalog().as_ref(),
        ports.constant_policy(),
        crate::query_execution::planning::sql_compile_control_from_connector_request(
            connector_context,
        ),
    )?;
    let facts = visible_analysis
        .refresh_input
        .aggregate_layout_facts(&visible_query, SqlMvAggregateLayoutScope::WholeQuery)?;
    novarocks_sql::planning::mv_aggregate_layout::build_sql_mv_aggregate_physical_layout(&facts)
        .map_err(PreparationError::from)
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
        binding.lease(),
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
    fields: &[IcebergPartitionFieldExpr],
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
        projection.facts.definition().relation_occurrences.len(),
        definition_has_join(&query),
    )?)?;
    validate_projection_target(&projection, &retained_target.schema_validation)?;
    let base_tables = contract_base_tables(contract);
    let occurrences = &projection.facts.definition().relation_occurrences;
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
    validate_mv_partition_columns(Some(fields), &analysis.output_columns)?;
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
    let replacement_fields = fields
        .iter()
        .enumerate()
        .map(|(position, field)| {
            let (column, transform) = managed_repartition_field(field);
            let source_field_id = target_field_id_by_name
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case(column))
                .map(|(_, field_id)| *field_id)
                .ok_or_else(|| {
                    format!(
                        "MV repartition source column `{column}` is missing from the exact target observation"
                    )
                })?;
            novarocks_spi::connector::ConnectorManagedPartitionField::try_new(
                source_field_id,
                u32::try_from(position)
                    .map_err(|_| "MV repartition field count exceeds u32".to_string())?,
                transform,
            )
            .map_err(|error| error.to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let replacement = novarocks_spi::connector::ConnectorManagedPartitionSpecReplacement::try_new(
        operation_id,
        expected_prior,
        replacement_fields,
    )
    .map_err(|error| error.to_string())?;
    let write_lease = retained_target
        .binding
        .lease()
        .derive_write_lease()
        .map_err(PreparationError::from)?;
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
    })
}

struct FirstRefreshTargetFacts<'contract, 'objects, 'transition, 'retained> {
    contract: &'contract RefreshPlanContract,
    base_table_object_ids: &'objects BTreeMap<SqlMvRelationOccurrenceId, ConnectorTableObjectId>,
    repartition_transition: Option<&'transition PreparedManagedRepartitionTransition>,
    retained_repartition_target: Option<&'retained RetainedRepartitionTarget>,
}

async fn prepare_frontend_first_refresh_write(
    io: &MvRefreshPreparationIo,
    captured: &mut CapturedPreparation,
    source: &IcebergMvCorePorts,
    current_catalog: Option<&str>,
    current_database: &str,
    target_facts: FirstRefreshTargetFacts<'_, '_, '_, '_>,
    attempt: &MvRefreshAttemptIdentity,
    observed_binding: ConnectorProviderBindingKey,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
) -> Result<(PreparedMvFirstRefreshWrite, AdmittedMvDataPublication), PreparationError> {
    let FirstRefreshTargetFacts {
        contract,
        base_table_object_ids,
        repartition_transition,
        retained_repartition_target,
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
    let planning_lease = match retained_repartition_target {
        Some(retained) => retained.binding.lease().clone(),
        None => {
            crate::connector::metadata_binding_typed(source.connector_control(), &target.catalog)?
        }
    };
    let write_lease = planning_lease
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
    let (projection, refresh_query_source) = io
        .rebind_definition(
            source,
            &projection,
            &contract_base_tables(contract),
            &target,
            retained_repartition_target.map(|retained| &retained.schema_validation),
            &connector_context,
        )
        .await?;
    let mut admitted_publication = io
        .admit(
            source,
            &planning_lease,
            &projection,
            attempt.publication_id,
            &connector_context,
        )
        .await?;
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
                    planning_lease.clone(),
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
                    target_binding.lease(),
                    target_binding.metadata(),
                    connector_context.clone(),
                )
                .map_err(PreparationError::from)?;
            &loaded_target_schema_validation
        }
    };
    captured.target = Some(target_binding.clone());
    let query = canonical_mv_select_query_from_source(&projection, &refresh_query_source)?;
    let runtime_bindings = validate_projection_target(&projection, target_schema_validation)?;
    let capabilities = RefreshCapabilities::from_canonical_facts(
        projection.facts.interpretation(),
        &runtime_bindings,
        projection.facts.definition().relation_occurrences.len(),
        definition_has_join(&query),
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
            capabilities.apply_key_column.clone(),
        )?;
    let pin = pin_contract_bases(
        io,
        source,
        contract,
        &projection,
        base_table_object_ids,
        &connector_context,
        "MV first-refresh",
    )
    .await?;
    // P's input watermark is exactly what this pin says, so it is frozen here,
    // beside the pin, rather than reconstructed at the commit from facts that
    // would by then be a second reading of the same sources.
    let admitted_publication = AdmittedMvDataPublication::try_new(
        admitted_publication,
        &pin.exact_revisions_by_occurrence(),
    )?;
    // The canonical definition owns name resolution. The execution artifact is
    // transient and may be canonicalized, but it must never fall back to the
    // session which happens to issue REFRESH.
    let resolution = &projection.facts.definition().query.resolution;
    let source_catalog = Some(resolution.default_catalog.clone());
    let source_database = resolution.default_namespace.clone();
    let expected_target_snapshot_id = match &contract.state_baseline {
        RefreshStateBaseline::SnapshotBacked {
            target_snapshot_id, ..
        } => *target_snapshot_id,
    };
    if definition_has_join(&query) && !capabilities.has_agg_state {
        let rewrite =
            crate::mv::domain::refresh::rewrite_context::freeze_refresh_rewrite_context_original(
                io,
                RefreshRewriteInputs {
                    connector_control: source.connector_control(),
                    connector_context: &connector_context,
                    projection: Arc::new(projection.clone()),
                    pin: &pin,
                    state_baseline: &contract.state_baseline,
                    target_binding,
                    target_observation: target_schema_validation,
                    runtime_bindings: &runtime_bindings,
                    join_analysis: novarocks_sql::planning::mv::extract_refresh_join_analysis(
                        &query,
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
    // L owns the branch and relation shape; the count of UNION branches is the
    // number of branch interpretations, not a separately persisted number.
    let branch_count = projection.facts.interpretation().branches.len();
    let multi_relation = projection.facts.definition().relation_occurrences.len() > 1;
    let shape = if capabilities.has_agg_state {
        // A branch UNION ALL has no top-level GROUP BY. Its aggregate-state
        // layout is defined by the first branch and CREATE-time validation
        // guarantees the remaining branches share that layout.
        let aggregate_query = if branch_count > 0 {
            crate::mv::domain::rewrite::context::first_union_branch_query(&query)?
        } else {
            query.clone()
        };
        let calls = extract_aggregate_sql_calls(&aggregate_query)?;
        // The analyzer attaches aggregate input types to a SELECT body.  A
        // top-level branch UNION has no such body, while the first branch has
        // the validated representative aggregate layout.
        let aggregate_layout_sql = if branch_count > 0 {
            novarocks_parser::printer::print_query(&aggregate_query)
        } else {
            novarocks_parser::printer::print_query(&query)
        };
        let aggregate_layout = build_aggregate_layout_for_refresh_select_sql(
            io,
            source,
            source_catalog.as_deref(),
            &source_database,
            &aggregate_layout_sql,
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
        admitted.admission().clone(),
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
    projection: &novarocks_mv_application::persistence::keyed_documents::StoredKeyedMvProjection,
    base_table_object_ids: &BTreeMap<SqlMvRelationOccurrenceId, ConnectorTableObjectId>,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    what: &str,
) -> Result<RefreshSnapshotPin, PreparationError> {
    let definition_occurrences = &projection.facts.definition().relation_occurrences;
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
    let planning_lease =
        crate::connector::metadata_binding_typed(source.connector_control(), &target.catalog)?;
    let admitted = io
        .admit(
            source,
            &planning_lease,
            &projection,
            attempt.publication_id,
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
        admitted.admission().clone(),
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
        &projection,
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
            planning_lease.clone(),
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
    admission: novarocks_spi::connector::document_storage::ConnectorDocumentManagementAdmission,
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
        admission,
        technique,
        bases,
        definition_fingerprint,
        target_catalog,
        target_namespace,
        target_name,
    )
    .map_err(PreparationError::from)
}

async fn prepare_frontend_incremental_write(
    io: &MvRefreshPreparationIo,
    captured: &mut CapturedPreparation,
    source: &IcebergMvCorePorts,
    current_catalog: Option<&str>,
    current_database: &str,
    contract: &RefreshPlanContract,
    attempt: &MvRefreshAttemptIdentity,
    observed_binding: ConnectorProviderBindingKey,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
) -> Result<PreparedIncrementalRefreshWork, PreparationError> {
    let target = IcebergMvTarget {
        catalog: contract.target.catalog.clone().ok_or_else(|| {
            "Iceberg MV incremental refresh target has no connector catalog".to_string()
        })?,
        namespace: contract.target.database.clone(),
        table: contract.target.name.clone(),
    };
    let projection = io.definition(source, &target).await?;
    let canonical_query = canonical_mv_select_query(&projection)?;
    let interpretation = projection.facts.interpretation();
    let is_join = definition_has_join(&canonical_query);
    let is_aggregate = !interpretation.aggregates.is_empty();
    let is_branch_union = !interpretation.branches.is_empty();
    let join_bases = if is_join {
        let (left, right) =
            join_base_refs_for_definition(&projection, &canonical_query, &contract.base_refs)?;
        Some((left.clone(), right.clone()))
    } else {
        None
    };
    let RefreshStateBaseline::SnapshotBacked {
        target_snapshot_id,
        definition_fingerprint,
        ..
    } = &contract.state_baseline;
    let definition_occurrences = &projection.facts.definition().relation_occurrences;
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
            target_binding.lease(),
            target_binding.metadata(),
            connector_context.clone(),
        )
        .map_err(PreparationError::from)?;
    let runtime_bindings = validate_projection_target(&projection, &target_schema_validation)?;
    // An incremental refresh of an aggregate view needs the same analyzed
    // aggregate facts a first refresh does: L says the view aggregates, and a
    // rewrite that cannot say which SQL call each aggregate identity belongs
    // to has nothing to maintain the state columns with.
    let aggregate = frozen_refresh_aggregate_analysis(
        io,
        source,
        &projection,
        &canonical_query,
        &connector_context,
    )
    .await?;
    let rewrite =
        crate::mv::domain::refresh::rewrite_context::freeze_refresh_rewrite_context_original(
            io,
            RefreshRewriteInputs {
                connector_control: source.connector_control(),
                connector_context: &connector_context,
                projection: Arc::new(projection),
                pin: &pin,
                state_baseline: &contract.state_baseline,
                target_binding: &target_binding,
                target_observation: &target_schema_validation,
                runtime_bindings: &runtime_bindings,
                join_analysis: novarocks_sql::planning::mv::extract_refresh_join_analysis(
                    &canonical_query,
                )?,
                aggregate,
            },
        )
        .await?;

    captured.rewrite = Some(Arc::clone(&rewrite));
    if let Some((left_ref, right_ref)) = join_bases.as_ref() {
        let left_from = rewrite.previous_snapshot_id(left_ref)?;
        let right_from = rewrite.previous_snapshot_id(right_ref)?;
        let left_to = rewrite.pinned_snapshot_id(left_ref)?;
        let right_to = rewrite.pinned_snapshot_id(right_ref)?;
        let (left_admission, _) = crate::mv::domain::refresh::rewrite_context::project_prepared_change_window_for_table_original(io,
                        source.storage_observation(),
            &left_ref.table,
            rewrite.previous_revision(left_ref)?,
            left_from,
            left_to,
            &connector_context,
        )?;
        let (right_admission, _) = crate::mv::domain::refresh::rewrite_context::project_prepared_change_window_for_table_original(io,
                        source.storage_observation(),
            &right_ref.table,
            rewrite.previous_revision(right_ref)?,
            right_from,
            right_to,
            &connector_context,
        )?;
        let left_facts = admitted_change_facts(&left_admission);
        let right_facts = admitted_change_facts(&right_admission);
        let mut full_rebuild_reasons = Vec::new();
        if let Err(reason) = &left_facts {
            full_rebuild_reasons.push(format!("{}: {reason}", left_ref.display()));
        }
        if let Err(reason) = &right_facts {
            full_rebuild_reasons.push(format!("{}: {reason}", right_ref.display()));
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
                    base_table_object_ids: &rewrite.pinned_objects_by_occurrence()?,
                    repartition_transition: None,
                    retained_repartition_target: None,
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
        let left_facts = left_facts.expect("full-rebuild admission returned above");
        let right_facts = right_facts.expect("full-rebuild admission returned above");
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
            left_facts.has_inserts || left_facts.has_deletes,
            right_facts.has_inserts || right_facts.has_deletes,
        );
        if branches.is_empty() {
            return Ok(PreparedIncrementalRefreshWork::MetadataOnly);
        }
        let join_mode = if is_aggregate {
            MvIncrementalJoinMode::Coalesce
        } else {
            select_join_incremental_execution_mode(left_facts.has_deletes, right_facts.has_deletes)
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
                target_binding.lease(),
                rewrite.mv_definition.as_ref(),
                attempt.publication_id,
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
            admitted_publication.admission().clone(),
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
        let (admission, _) = crate::mv::domain::refresh::rewrite_context::project_prepared_change_window_for_table_original(io,
                                source.storage_observation(),
                &base.table,
                rewrite.previous_revision(base)?,
                previous_snapshot_id,
                current_snapshot_id,
                &connector_context,
            )?;
        loaded_bases.push((
            base,
            current_snapshot_id,
            current_table_object_id.clone(),
            admission,
        ));
    }
    let changes = loaded_bases
        .iter()
        .map(
            |(base_ref, current_snapshot_id, current_table_object_id, admission)| {
                NonJoinBaseChange {
                    occurrence_id: base_ref.occurrence_id,
                    base_ref: &base_ref.table,
                    current_snapshot_id: *current_snapshot_id,
                    current_table_object_id,
                    admission: admission.clone(),
                }
            },
        )
        .collect::<Vec<_>>();
    let (mode, evidence) = match plan_non_join_incremental_changes(&changes)? {
        NonJoinIncrementalChangePlan::MetadataOnly(_) => {
            return Ok(PreparedIncrementalRefreshWork::MetadataOnly);
        }
        NonJoinIncrementalChangePlan::FullRebuild { reason, .. } => {
            tracing::info!(
                target = %rewrite.target.fqn(),
                "MV refresh SQL preparation selected a distributed full-rebuild staging overwrite: {reason}"
            );
            let (rebuild, admitted) = prepare_frontend_first_refresh_write(
                io,
                captured,
                source,
                current_catalog,
                current_database,
                FirstRefreshTargetFacts {
                    contract,
                    base_table_object_ids: &rewrite.pinned_objects_by_occurrence()?,
                    repartition_transition: None,
                    retained_repartition_target: None,
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
            has_delete_changes, ..
        } => {
            let mode = non_join_incremental_write_mode(is_aggregate, has_delete_changes);
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
            target_binding.lease(),
            rewrite.mv_definition.as_ref(),
            attempt.publication_id,
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
        admitted_publication.admission().clone(),
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
    MvIncrementalWritePreparer::prepare(
        request,
        crate::query_execution::mv_assembly::first_refresh_staging::frozen_logical_context_from_rewrite(
            &rewrite,
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
