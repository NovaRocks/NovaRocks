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

use super::*;
use crate::query_execution::mv_assembly::refresh_preparation::async_facts::PreparationError as RefreshError;
use crate::query_execution::mv_assembly::refresh_preparation::io_gate::MvRefreshPreparationIo;
use novarocks_spi::connector::ConnectorOperationControl;

/// Borrowed facts from the same retained refresh generation.
pub(crate) struct RefreshPlanningFacts<'a> {
    iceberg_target: &'a IcebergMvTarget,
    mv_definition: &'a StoredKeyedMvProjection,
    base_refs: &'a [TableIdentity],
    target_binding: &'a MvTargetBinding,
    target_observation: &'a MvSchemaValidationObservation,
    state_baseline: &'a RefreshStateBaseline,
}

/// Snapshot observations keyed by the original relation occurrences.
struct OccurrenceSnapshotWindow<'a> {
    bases: &'a [RefreshBaseRelationOccurrence],
    previous: &'a BTreeMap<SqlMvRelationOccurrenceId, i64>,
    current: &'a BTreeMap<SqlMvRelationOccurrenceId, Option<i64>>,
}

pub(crate) async fn plan_iceberg_mv_refresh_with_connector_context(
    io: &MvRefreshPreparationIo,
    source: &IcebergMvCorePorts,
    current_catalog: Option<&str>,
    current_database: &str,
    stmt: &MvRefreshRequest,
    target: MvTarget,
    full_sources: bool,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<RefreshPlan, RefreshError> {
    let full_statement = full_sources.then(|| {
        let mut full = stmt.clone();
        full.full = true;
        full
    });
    let stmt = full_statement.as_ref().unwrap_or(stmt);
    let iceberg_target =
        resolve_refresh_target(current_catalog, current_database, &stmt.name_parts)
            .map_err(RefreshError::from)?;
    connector_context
        .original_operation_control()
        .check_active()
        .map_err(RefreshError::from)?;
    // Preparation normally only observes the currently admitted catalog and
    // MV facts. A snapshot fence may rebuild sealed Current only as a
    // read-only candidate; this path has no authority to readmit management
    // readiness and therefore still fails the refresh.
    // Metadata maintenance may renew an existing admission only while its
    // carrier facts remain identical. This observation cannot recover an old
    // document format or reopen management without explicit readmission.
    if reobserve_current_management_before_refresh(io, source, &iceberg_target, connector_context)
        .await?
    {
        return Err(RefreshError::user(
            "MV Current marker names another process incarnation; management is closed".to_string(),
        ));
    }
    let mv_definition = io
        .definition(source, &iceberg_target)
        .await
        .map_err(RefreshError::from)?;
    let target_binding = io
        .target_binding(source, &iceberg_target, connector_context)
        .await
        .map_err(RefreshError::from)?;
    if let Err(snapshot_error) = validate_target_snapshot_original(
        io,
        source,
        &iceberg_target,
        &mv_definition,
        &target_binding,
        connector_context,
    )
    .await
    {
        reconcile_published_lake_projection(
            io,
            source,
            &iceberg_target,
            &target_binding,
            connector_context,
        )
        .await
        .map_err(|recovery_error| {
            recovery_error.with_context(format!(
                "{snapshot_error}; sealed Current read-only recovery failed",
            ))
        })?;
        return Err(RefreshError::user(format!(
            "{snapshot_error}; sealed Current was installed as a read-only MV candidate; management Current readmission is required before refresh",
        )));
    }
    // D's own occurrence inventory is the base-locator authority now; the
    // retired projection copy of the resolved FQN list is gone.
    let refresh_state_baseline = build_refresh_state_baseline(&mv_definition, &target_binding)
        .map_err(RefreshError::from)?;
    io.capture_refresh_sources(
        source,
        &mv_definition,
        &refresh_state_baseline,
        stmt.full || full_sources,
        connector_context,
    )
    .await?;
    let base_refs = definition_base_refs(&mv_definition).map_err(RefreshError::from)?;
    // One exact target generation serves the whole plan: the snapshot fence
    // above, the state baseline, the L bindings, and the typed partition facts
    // every sub-planner reads. Re-resolving `latest` per fact could split one
    // attempt across two generations.
    let target_schema_observation =
        crate::mv::domain::storage_observation::observe_schema_validation(
            source.storage_observation(),
            target_binding.lease(),
            target_binding.metadata(),
            connector_context.clone(),
        )
        .map_err(RefreshError::from)?;

    let (mv_definition, refresh_query_source) = io
        .rebind_definition(
            source,
            &mv_definition,
            &base_refs,
            &iceberg_target,
            Some(&target_schema_observation),
            connector_context,
        )
        .await?;

    let resolution = &mv_definition.facts.definition().query.resolution;
    let canonical_select_query = canonicalize_iceberg_mv_select_query(
        &parse_mv_select_query(&refresh_query_source).map_err(RefreshError::from)?,
        Some(resolution.default_catalog.as_str()),
        &resolution.default_namespace,
    );
    // Driver dispatch: plan-side dispatch is capability-driven, matching the
    // execute path. The capabilities come from L plus the exact target
    // observation L was validated against, never from a persisted contract.
    let runtime_bindings = validate_projection_target(&mv_definition, &target_schema_observation)
        .map_err(RefreshError::from)?;
    let caps = RefreshCapabilities::from_canonical_facts(
        mv_definition.facts.interpretation(),
        &runtime_bindings,
        mv_definition.facts.definition().relation_occurrences.len(),
        definition_has_join(&canonical_select_query),
    )
    .map_err(RefreshError::from)?;
    let is_join = matches!(
        caps.snapshot_policy,
        BaseSnapshotPolicy::JoinPairPartialInitialSkip
    );
    match (caps.has_agg_state, &caps.snapshot_policy, &caps.identity) {
        // UNION ALL of projection/filter branches.
        (false, BaseSnapshotPolicy::AllBasesRequired, _) => {
            // The query's own branch count; L's branch interpretations are the
            // persisted counterpart the per-base validator compares it against.
            let branch_count = union_branch_count(&canonical_select_query) as usize;
            return plan_iceberg_union_projection_mv_refresh(
                io,
                source,
                RefreshPlanningFacts {
                    iceberg_target: &iceberg_target,
                    mv_definition: &mv_definition,
                    base_refs: &base_refs,
                    target_binding: &target_binding,
                    target_observation: &target_schema_observation,
                    state_baseline: &refresh_state_baseline,
                },
                target,
                stmt,
                branch_count,
                connector_context,
            )
            .await;
        }
        // Aggregate shapes: single-base, fan-in, branch-union, and join
        // aggregate all route through the aggregate planner, which selects the
        // per-shape plan by capability. The branch-union sub-path sources its
        // branch count + first-branch aggregate calls from the focused extractor
        // (not the union classifier), so composed branches are supported.
        (true, _, _) => {
            return plan_iceberg_aggregate_mv_refresh(
                io,
                source,
                RefreshPlanningFacts {
                    iceberg_target: &iceberg_target,
                    mv_definition: &mv_definition,
                    base_refs: &base_refs,
                    target_binding: &target_binding,
                    target_observation: &target_schema_observation,
                    state_baseline: &refresh_state_baseline,
                },
                target,
                stmt,
                current_catalog,
                current_database,
                &caps,
                &canonical_select_query,
                connector_context,
            )
            .await;
        }
        // Join / single-base projection-filter: fall through to the inline
        // paths below.
        (false, BaseSnapshotPolicy::JoinPairPartialInitialSkip, _)
        | (false, BaseSnapshotPolicy::SingleBase, _) => {}
    }
    if is_join {
        // The join projection/filter plan path resolves left/right from D's own
        // effective SQL. There is no persisted join lineage and no contract
        // version to compare: the canonical documents carry their own revisions
        // and were already verified when the projection was reconstructed.
        let occurrences = definition_occurrences_for_base_refs(&mv_definition, &base_refs)
            .map_err(RefreshError::from)?;
        let base_occurrences =
            base_relation_occurrences(&mv_definition, &base_refs).map_err(RefreshError::from)?;
        let (left_occurrence, right_occurrence) = join_base_refs_for_definition(
            &mv_definition,
            &canonical_select_query,
            &base_occurrences,
        )
        .map_err(RefreshError::from)?;
        let left_refresh = io
            .current_base(source, &left_occurrence.table, connector_context)
            .await
            .map_err(RefreshError::from)?;
        let right_refresh = io
            .current_base(source, &right_occurrence.table, connector_context)
            .await
            .map_err(RefreshError::from)?;
        let left_observation = io
            .schema(source, &left_occurrence.table, connector_context)
            .await?;
        let right_observation = io
            .schema(source, &right_occurrence.table, connector_context)
            .await?;
        // The join validator demands D occurrence order, which is not the
        // join's own left/right order, so the observations are re-paired with
        // their occurrences rather than with the join sides. By occurrence, not
        // by table: in a self-join both sides read the same table.
        let bases = occurrences
            .iter()
            .copied()
            .map(|occurrence| {
                if occurrence.occurrence_id == left_occurrence.occurrence_id.get() {
                    Ok((occurrence, &left_observation))
                } else if occurrence.occurrence_id == right_occurrence.occurrence_id.get() {
                    Ok((occurrence, &right_observation))
                } else {
                    Err(RefreshError::user(format!(
                        "iceberg join MV D occurrence {} is neither join side",
                        occurrence.occurrence_id
                    )))
                }
            })
            .collect::<Result<Vec<_>, RefreshError>>()?;
        let renames =
            validate_join_schema_contract(&mv_definition, &bases, &target_schema_observation)
                .map_err(RefreshError::from)?;
        require_no_occurrence_rebind(&renames).map_err(RefreshError::from)?;
        let left_current = left_refresh.current_snapshot_id();
        let right_current = right_refresh.current_snapshot_id();
        // Each side is pinned as the occurrence it is. A join of one table
        // with itself has two occurrences of one name, and keying by name
        // would let the second side overwrite the first.
        let mut snapshot_pins = BTreeMap::new();
        snapshot_pins.insert(left_occurrence.occurrence_id, left_current);
        snapshot_pins.insert(right_occurrence.occurrence_id, right_current);
        let mut current_snapshots = BTreeMap::new();
        current_snapshots.insert(left_occurrence.occurrence_id, left_current);
        current_snapshots.insert(right_occurrence.occurrence_id, right_current);
        let previous = previous_refresh_locators(
            io,
            &refresh_state_baseline,
            source.connector_control(),
            connector_context,
        )
        .await?;
        let previous_snapshots = &previous.snapshots;
        let refresh_label = format!(
            "iceberg join MV {}.{}.{}",
            iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
        );
        let refresh_statuses = base_snapshot_statuses_for_plan(
            &base_occurrences,
            previous_snapshots,
            &current_snapshots,
        );
        let decision = decide_requested_refresh_plan(
            &RefreshPlanningInput {
                snapshot_policy: BaseSnapshotPolicy::JoinPairPartialInitialSkip,
                base_snapshots: &refresh_statuses,
                label: &refresh_label,
            },
            stmt.full,
        )
        .map_err(RefreshError::from)?;
        let has_previous = base_occurrences
            .iter()
            .any(|base| previous_snapshots.contains_key(&base.occurrence_id));
        if has_previous {
            for base in &base_occurrences {
                let named = base.display();
                if previous_snapshots.contains_key(&base.occurrence_id)
                    && current_snapshots
                        .get(&base.occurrence_id)
                        .copied()
                        .flatten()
                        .is_none()
                {
                    return Err(RefreshError::user(format!(
                        "cannot refresh iceberg join materialized view {}.{}.{}: previously-refreshed base snapshot for {} is no longer reachable",
                        iceberg_target.catalog,
                        iceberg_target.namespace,
                        iceberg_target.table,
                        named
                    )));
                }
            }
            for base in &base_occurrences {
                let named = base.display();
                previous_snapshots
                    .get(&base.occurrence_id)
                    .copied()
                    .ok_or_else(|| {
                        RefreshError::user(format!(
                            "iceberg join MV {}.{}.{} has partial previous refresh snapshots; recreate the MV",
                            iceberg_target.catalog,
                            iceberg_target.namespace,
                            iceberg_target.table
                        ))
                    })?;
                current_snapshots
                    .get(&base.occurrence_id)
                    .copied()
                    .flatten()
                    .ok_or_else(|| {
                        RefreshError::user(format!(
                            "cannot refresh iceberg join materialized view {}.{}.{}: previously-refreshed base snapshot for {} is no longer reachable",
                            iceberg_target.catalog,
                            iceberg_target.namespace,
                            iceberg_target.table,
                            named
                        ))
                    })?;
            }
        }
        let affected_partitions = unknown_join_affected_partitions();
        log_planned_iceberg_mv_affected_partitions(&iceberg_target, &affected_partitions);
        return Ok(RefreshPlan {
            contract: RefreshPlanContract {
                mv_id: Some(mv_definition.mv_id),
                target,
                storage_engine: MvStorageEngine::Iceberg,
                decision: decision.refresh,
                state_baseline: refresh_state_baseline.clone(),
                base_refs: base_occurrences.clone(),
                snapshot_pins,
                affected_partitions,
            },
        });
    }
    // The capability dispatch above (has_agg=false, SingleBase, non-join) routes only
    // single-base projection/filter MVs to this point. No classify guard needed.
    let [base_ref] = base_refs.as_slice() else {
        return Err(RefreshError::user(
            "iceberg materialized view refresh requires exactly one base table reference",
        ));
    };
    let occurrences = definition_occurrences_for_base_refs(&mv_definition, &base_refs)
        .map_err(RefreshError::from)?;
    let [occurrence] = occurrences.as_slice() else {
        return Err(RefreshError::user(
            "iceberg materialized view refresh requires exactly one D relation occurrence",
        ));
    };
    let base_occurrences =
        base_relation_occurrences(&mv_definition, &base_refs).map_err(RefreshError::from)?;
    let [base_occurrence] = base_occurrences.as_slice() else {
        return Err(RefreshError::user(
            "iceberg materialized view refresh requires exactly one base relation occurrence",
        ));
    };
    let current_snapshot_id_before_pin = io
        .current_base(source, base_ref, connector_context)
        .await
        .map_err(RefreshError::from)?
        .current_snapshot_id();
    let previous = previous_refresh_locators(
        io,
        &refresh_state_baseline,
        source.connector_control(),
        connector_context,
    )
    .await?;
    let previous_snapshot_id = previous
        .snapshots
        .get(&base_occurrence.occurrence_id)
        .copied();
    let refresh_label = format!(
        "iceberg materialized view {}.{}.{}",
        iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
    );
    let pre_pin_statuses = [base_snapshot_status_for_refresh(
        base_occurrence,
        previous_snapshot_id,
        current_snapshot_id_before_pin,
    )];
    let pre_pin_decision = decide_requested_refresh_plan(
        &RefreshPlanningInput {
            snapshot_policy: BaseSnapshotPolicy::SingleBase,
            base_snapshots: &pre_pin_statuses,
            label: &refresh_label,
        },
        stmt.full,
    )
    .map_err(RefreshError::from)?;
    let base_observation = io.schema(source, base_ref, connector_context).await?;
    let renames = validate_schema_contract(
        &mv_definition,
        occurrence,
        &base_observation,
        &target_schema_observation,
    )
    .map_err(RefreshError::from)?;
    require_no_occurrence_rebind(&renames).map_err(RefreshError::from)?;
    match pre_pin_decision.refresh {
        ExecutableRefreshDecision::SkipEmpty => {
            let mut snapshot_pins = BTreeMap::new();
            snapshot_pins.insert(base_occurrence.occurrence_id, None);
            let affected_partitions = noop_affected_partitions(target_binding.partition());
            log_planned_iceberg_mv_affected_partitions(&iceberg_target, &affected_partitions);
            return Ok(RefreshPlan {
                contract: RefreshPlanContract {
                    mv_id: Some(mv_definition.mv_id),
                    target,
                    storage_engine: MvStorageEngine::Iceberg,
                    decision: pre_pin_decision.refresh,
                    state_baseline: refresh_state_baseline.clone(),
                    base_refs: base_occurrences.clone(),
                    snapshot_pins,
                    affected_partitions,
                },
            });
        }
        ExecutableRefreshDecision::FirstRefresh
        | ExecutableRefreshDecision::MetadataOnly
        | ExecutableRefreshDecision::Incremental => {}
    }

    let current_snapshot_id = current_snapshot_id_before_pin;

    let refresh_statuses = [base_snapshot_status_for_refresh(
        base_occurrence,
        previous_snapshot_id,
        current_snapshot_id,
    )];
    let decision = decide_requested_refresh_plan(
        &RefreshPlanningInput {
            snapshot_policy: BaseSnapshotPolicy::SingleBase,
            base_snapshots: &refresh_statuses,
            label: &refresh_label,
        },
        stmt.full,
    )
    .map_err(RefreshError::from)?;
    let mode = decision.mode();
    let mut snapshot_pins = BTreeMap::new();
    snapshot_pins.insert(base_occurrence.occurrence_id, current_snapshot_id);
    let affected_partitions = plan_aggregate_mv_affected_partitions(
        io,
        source,
        connector_context,
        base_occurrence,
        &refresh_state_baseline,
        &mv_definition,
        target_binding.partition(),
        mode,
        previous_snapshot_id,
        current_snapshot_id,
    )
    .await?;
    log_planned_iceberg_mv_affected_partitions(&iceberg_target, &affected_partitions);
    Ok(RefreshPlan {
        contract: RefreshPlanContract {
            mv_id: Some(mv_definition.mv_id),
            target,
            storage_engine: MvStorageEngine::Iceberg,
            decision: decision.refresh,
            state_baseline: refresh_state_baseline,
            base_refs: base_occurrences.clone(),
            snapshot_pins,
            affected_partitions,
        },
    })
}

pub(crate) async fn plan_iceberg_union_projection_mv_refresh(
    io: &MvRefreshPreparationIo,
    source: &IcebergMvCorePorts,
    facts: RefreshPlanningFacts<'_>,
    target: MvTarget,
    stmt: &MvRefreshRequest,
    branch_count: usize,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<RefreshPlan, RefreshError> {
    let RefreshPlanningFacts {
        iceberg_target,
        mv_definition,
        base_refs,
        target_binding,
        target_observation,
        state_baseline,
    } = facts;
    // The resolved bases ARE D's occurrences, in D's own order; the retired
    // contract base set that used to be compared here no longer exists.
    let occurrences = definition_occurrences_for_base_refs(mv_definition, base_refs)
        .map_err(RefreshError::from)?;
    let base_occurrences =
        base_relation_occurrences(mv_definition, base_refs).map_err(RefreshError::from)?;

    let mut current_snapshots = BTreeMap::new();
    let mut current_table_object_ids = BTreeMap::new();
    let mut snapshot_pins = BTreeMap::new();
    for (occurrence, base) in occurrences.iter().copied().zip(&base_occurrences) {
        let base_ref = &base.table;
        let refresh = io
            .current_base(source, base_ref, connector_context)
            .await
            .map_err(RefreshError::from)?;
        let base_observation = io.schema(source, base_ref, connector_context).await?;
        validate_union_projection_schema_contract_for_base(
            iceberg_target,
            mv_definition,
            branch_count,
            occurrence,
            &base_observation,
            target_observation,
        )
        .map_err(RefreshError::from)?;
        let current = refresh.current_snapshot_id();
        snapshot_pins.insert(base.occurrence_id, current);
        current_snapshots.insert(base.occurrence_id, current);
        current_table_object_ids.insert(base.occurrence_id, refresh.object_id().clone());
    }

    let previous = previous_refresh_locators(
        io,
        state_baseline,
        source.connector_control(),
        connector_context,
    )
    .await?;
    let previous_snapshots = &previous.snapshots;
    let previous_table_object_ids = &previous.table_object_ids;
    let has_previous_snapshots = base_occurrences
        .iter()
        .any(|base| previous_snapshots.contains_key(&base.occurrence_id));
    let has_previous_table_object_ids = base_occurrences
        .iter()
        .any(|base| previous_table_object_ids.contains_key(&base.occurrence_id));
    let has_previous = has_previous_snapshots || has_previous_table_object_ids;
    let all_previous_snapshots = base_occurrences
        .iter()
        .all(|base| previous_snapshots.contains_key(&base.occurrence_id));
    let all_previous_table_object_ids = base_occurrences
        .iter()
        .all(|base| previous_table_object_ids.contains_key(&base.occurrence_id));

    if has_previous && (!all_previous_snapshots || !all_previous_table_object_ids) {
        return Err(RefreshError::user(format!(
            "iceberg UNION ALL projection/filter MV {}.{}.{} has partial previous refresh metadata; recreate the MV",
            iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
        )));
    }
    let refresh_label = format!(
        "iceberg UNION ALL projection/filter MV {}.{}.{}",
        iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
    );
    let refresh_statuses =
        base_snapshot_statuses_for_plan(&base_occurrences, previous_snapshots, &current_snapshots);
    let decision = decide_requested_refresh_plan(
        &RefreshPlanningInput {
            snapshot_policy: BaseSnapshotPolicy::AllBasesRequired,
            base_snapshots: &refresh_statuses,
            label: &refresh_label,
        },
        stmt.full,
    )
    .map_err(RefreshError::from)?;
    let mode = decision.mode();
    if has_previous {
        for base in &base_occurrences {
            let named = base.display();
            if let Some(previous_object_id) = previous_table_object_ids.get(&base.occurrence_id) {
                let current_object_id = current_table_object_ids
                    .get(&base.occurrence_id)
                    .ok_or_else(|| {
                        RefreshError::user(format!(
                            "refresh observation missing object ID for base {named} (this should not happen)"
                        ))
                    })?;
                if previous_object_id != current_object_id {
                    return Err(RefreshError::user(format!(
                        "iceberg MV base table identity changed for {named}; incremental refresh is unsafe, rebuild or recreate the MV"
                    )));
                }
            }
            previous_snapshots
                .get(&base.occurrence_id)
                .copied()
                .ok_or_else(|| {
                    RefreshError::user(format!(
                        "iceberg UNION ALL projection/filter MV {}.{}.{} has partial previous refresh metadata; recreate the MV",
                        iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
                    ))
                })?;
            current_snapshots
                .get(&base.occurrence_id)
                .copied()
                .flatten()
                .ok_or_else(|| {
                    RefreshError::user(format!(
                        "cannot refresh iceberg UNION ALL projection/filter MV {}.{}.{}: previously-refreshed base snapshot for {} is no longer reachable",
                        iceberg_target.catalog,
                        iceberg_target.namespace,
                        iceberg_target.table,
                        named
                    ))
                })?;
        }
    }

    let affected_partitions = plan_multi_base_affected_partitions_original(
        io,
        source,
        state_baseline,
        connector_context,
        mv_definition,
        target_binding.partition(),
        mode,
        OccurrenceSnapshotWindow {
            bases: &base_occurrences,
            previous: previous_snapshots,
            current: &current_snapshots,
        },
        "UNION ALL MV affected partition planning",
    )
    .await?;
    log_planned_iceberg_mv_affected_partitions(iceberg_target, &affected_partitions);
    Ok(RefreshPlan {
        contract: RefreshPlanContract {
            mv_id: Some(mv_definition.mv_id),
            target,
            storage_engine: MvStorageEngine::Iceberg,
            decision: decision.refresh,
            state_baseline: state_baseline.clone(),
            base_refs: base_occurrences.clone(),
            snapshot_pins,
            affected_partitions,
        },
    })
}

pub(crate) async fn plan_iceberg_all_bases_aggregate_mv_refresh(
    io: &MvRefreshPreparationIo,
    source: &IcebergMvCorePorts,
    facts: RefreshPlanningFacts<'_>,
    target: MvTarget,
    stmt: &MvRefreshRequest,
    current_catalog: Option<&str>,
    current_database: &str,
    caps: &RefreshCapabilities,
    canonical_select_query: &ast::Query,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<RefreshPlan, RefreshError> {
    let RefreshPlanningFacts {
        iceberg_target,
        mv_definition,
        base_refs,
        target_binding,
        target_observation,
        state_baseline,
    } = facts;
    // The branch-union variant validates the branch count (off the AST, so a
    // composed branch union is supported) + the resolved base-ref set; the
    // fan-in variant validates the resolved base-ref set directly (the resolved
    // bases ARE the fan-in base set now that the classifier is retired).
    let is_branch_union = matches!(caps.identity, RefreshIdentity::BranchScoped(_));
    let is_composed_join_aggregate =
        !is_branch_union && !from_clause_is_fan_in_union(canonical_select_query);
    let occurrences = definition_occurrences_for_base_refs(mv_definition, base_refs)
        .map_err(RefreshError::from)?;
    if is_branch_union {
        let branch_count = union_branch_count(canonical_select_query) as usize;
        validate_branch_union_contract(
            iceberg_target,
            mv_definition,
            branch_count,
            target_observation,
        )
        .map_err(RefreshError::from)?;
        // Nothing more to check about the bases here: whether they correspond
        // one-for-one to D's occurrences is settled by
        // `definition_occurrences_for_base_refs` above, and each one is checked
        // against the persisted schema contract per occurrence below. Both
        // paths used to also require the bases to be distinct by table name,
        // which was never the invariant -- a union whose branches read one
        // table twice is two occurrences of it, not a duplicate.
    } else if is_composed_join_aggregate {
        validate_composed_aggregate_fallback_query(canonical_select_query)
            .map_err(RefreshError::from)?;
    }
    let base_occurrences =
        base_relation_occurrences(mv_definition, base_refs).map_err(RefreshError::from)?;
    let mut current_snapshots = BTreeMap::new();
    let mut snapshot_pins = BTreeMap::new();
    for (occurrence, base) in occurrences.iter().copied().zip(&base_occurrences) {
        let base_ref = &base.table;
        let refresh = io
            .current_base(source, base_ref, connector_context)
            .await
            .map_err(RefreshError::from)?;
        let base_observation = io.schema(source, base_ref, connector_context).await?;
        let renames = validate_aggregate_schema_contract_for_base(
            mv_definition,
            occurrence,
            &base_observation,
            target_observation,
        )
        .map_err(RefreshError::from)?;
        require_no_occurrence_rebind(&renames).map_err(RefreshError::from)?;
        let current = refresh.current_snapshot_id();
        current_snapshots.insert(base.occurrence_id, current);
        snapshot_pins.insert(base.occurrence_id, current);
    }
    let previous = previous_refresh_locators(
        io,
        state_baseline,
        source.connector_control(),
        connector_context,
    )
    .await?;
    let previous_snapshots = &previous.snapshots;
    let refresh_kind_label = if is_branch_union {
        "branch UNION ALL aggregate"
    } else if is_composed_join_aggregate {
        "composed join aggregate"
    } else {
        "aggregate-over-UNION-ALL"
    };
    let refresh_label = format!(
        "iceberg {refresh_kind_label} MV {}.{}.{}",
        iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
    );
    let refresh_statuses =
        base_snapshot_statuses_for_plan(&base_occurrences, previous_snapshots, &current_snapshots);
    let decision = decide_requested_refresh_plan(
        &RefreshPlanningInput {
            snapshot_policy: BaseSnapshotPolicy::AllBasesRequired,
            base_snapshots: &refresh_statuses,
            label: &refresh_label,
        },
        stmt.full,
    )
    .map_err(RefreshError::from)?;
    let mode = decision.mode();
    let has_previous = base_occurrences
        .iter()
        .any(|base| previous_snapshots.contains_key(&base.occurrence_id));
    if has_previous {
        for base in &base_occurrences {
            let named = base.display();
            previous_snapshots
                .get(&base.occurrence_id)
                .copied()
                .ok_or_else(|| {
                    RefreshError::user(format!(
                        "iceberg {refresh_kind_label} MV {}.{}.{} has partial previous refresh snapshots; recreate the MV",
                        iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
                    ))
                })?;
            current_snapshots
                .get(&base.occurrence_id)
                .copied()
                .flatten()
                .ok_or_else(|| {
                    RefreshError::user(format!(
                        "cannot refresh iceberg {refresh_kind_label} MV {}.{}.{}: previously-refreshed base snapshot for {} is no longer reachable",
                        iceberg_target.catalog,
                        iceberg_target.namespace,
                        iceberg_target.table,
                        named
                    ))
                })?;
        }
    }
    let affected_partition_context =
        format!("iceberg {refresh_kind_label} MV affected partition planning");
    let affected_partitions = plan_multi_base_affected_partitions_original(
        io,
        source,
        state_baseline,
        connector_context,
        mv_definition,
        target_binding.partition(),
        mode,
        OccurrenceSnapshotWindow {
            bases: &base_occurrences,
            previous: previous_snapshots,
            current: &current_snapshots,
        },
        &affected_partition_context,
    )
    .await?;
    log_planned_iceberg_mv_affected_partitions(iceberg_target, &affected_partitions);
    Ok(build_iceberg_refresh_plan(
        mv_definition,
        target,
        stmt,
        current_catalog,
        current_database,
        &base_occurrences,
        snapshot_pins,
        decision.refresh,
        state_baseline.clone(),
        affected_partitions,
    ))
}

pub(crate) async fn plan_iceberg_aggregate_mv_refresh(
    io: &MvRefreshPreparationIo,
    source: &IcebergMvCorePorts,
    facts: RefreshPlanningFacts<'_>,
    target: MvTarget,
    stmt: &MvRefreshRequest,
    current_catalog: Option<&str>,
    current_database: &str,
    caps: &RefreshCapabilities,
    canonical_select_query: &ast::Query,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<RefreshPlan, RefreshError> {
    let RefreshPlanningFacts {
        iceberg_target,
        mv_definition,
        base_refs,
        target_binding,
        target_observation,
        state_baseline,
    } = facts;
    // L is the only aggregate authority; there is no persisted contract version
    // to compare against any more.
    validate_aggregate_schema_contract_metadata(iceberg_target, mv_definition)
        .map_err(RefreshError::from)?;
    // Aggregate plan dispatch (Phase 3 / B2): selected by capability.
    //   SingleBase                 -> single-base aggregate
    //   AllBasesRequired           -> fan-in (GroupRowId) or branch-union
    //                                 (BranchScoped), gated on the row identity
    //   JoinPairPartialInitialSkip -> join aggregate
    match &caps.snapshot_policy {
        BaseSnapshotPolicy::SingleBase | BaseSnapshotPolicy::AllBasesRequired => {
            // AllBasesRequired (fan-in `GroupRowId` / branch-union
            // `BranchScoped`) is planned by a dedicated helper (I2) mirroring
            // the execute-side `refresh_fan_in_aggregate_iceberg_mv` fold.
            if matches!(caps.snapshot_policy, BaseSnapshotPolicy::AllBasesRequired) {
                return plan_iceberg_all_bases_aggregate_mv_refresh(
                    io,
                    source,
                    RefreshPlanningFacts {
                        iceberg_target,
                        mv_definition,
                        base_refs,
                        target_binding,
                        target_observation,
                        state_baseline,
                    },
                    target,
                    stmt,
                    current_catalog,
                    current_database,
                    caps,
                    canonical_select_query,
                    connector_context,
                )
                .await;
            }
            let [base_ref] = base_refs else {
                return Err(RefreshError::user(
                    "iceberg aggregate materialized view refresh requires exactly one base table reference",
                ));
            };
            let occurrences = definition_occurrences_for_base_refs(mv_definition, base_refs)
                .map_err(RefreshError::from)?;
            let [occurrence] = occurrences.as_slice() else {
                return Err(RefreshError::user(
                    "iceberg aggregate materialized view refresh requires exactly one D relation occurrence",
                ));
            };
            let base_occurrences =
                base_relation_occurrences(mv_definition, base_refs).map_err(RefreshError::from)?;
            let [base_occurrence] = base_occurrences.as_slice() else {
                return Err(RefreshError::user(
                    "iceberg aggregate materialized view refresh requires exactly one base relation occurrence",
                ));
            };
            let refresh = io
                .current_base(source, base_ref, connector_context)
                .await
                .map_err(RefreshError::from)?;
            let base_observation = io.schema(source, base_ref, connector_context).await?;
            let renames = validate_schema_contract(
                mv_definition,
                occurrence,
                &base_observation,
                target_observation,
            )
            .map_err(RefreshError::from)?;
            require_no_occurrence_rebind(&renames).map_err(RefreshError::from)?;
            let current = refresh.current_snapshot_id();
            let previous_locators = previous_refresh_locators(
                io,
                state_baseline,
                source.connector_control(),
                connector_context,
            )
            .await?;
            let previous = previous_locators
                .snapshots
                .get(&base_occurrence.occurrence_id)
                .copied();
            let refresh_label = format!(
                "iceberg aggregate materialized view {}.{}.{}",
                iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
            );
            let refresh_statuses = [base_snapshot_status_for_refresh(
                base_occurrence,
                previous,
                current,
            )];
            let decision = decide_requested_refresh_plan(
                &RefreshPlanningInput {
                    snapshot_policy: BaseSnapshotPolicy::SingleBase,
                    base_snapshots: &refresh_statuses,
                    label: &refresh_label,
                },
                stmt.full,
            )
            .map_err(RefreshError::from)?;
            let mode = decision.mode();
            let mut snapshot_pins = BTreeMap::new();
            snapshot_pins.insert(base_occurrence.occurrence_id, current);
            let affected_partitions = plan_aggregate_mv_affected_partitions(
                io,
                source,
                connector_context,
                base_occurrence,
                state_baseline,
                mv_definition,
                target_binding.partition(),
                mode,
                previous,
                current,
            )
            .await?;
            log_planned_iceberg_mv_affected_partitions(iceberg_target, &affected_partitions);
            Ok(build_iceberg_refresh_plan(
                mv_definition,
                target,
                stmt,
                current_catalog,
                current_database,
                &base_occurrences,
                snapshot_pins,
                decision.refresh,
                state_baseline.clone(),
                affected_partitions,
            ))
        }
        BaseSnapshotPolicy::JoinPairPartialInitialSkip => {
            // The join-aggregate plan resolves left/right from D's own effective
            // SQL (FROM-side only); the join ON keys are never read by the plan
            // path.
            let occurrences = definition_occurrences_for_base_refs(mv_definition, base_refs)
                .map_err(RefreshError::from)?;
            let base_occurrences =
                base_relation_occurrences(mv_definition, base_refs).map_err(RefreshError::from)?;
            let (left_occurrence, right_occurrence) = join_base_refs_for_definition(
                mv_definition,
                canonical_select_query,
                &base_occurrences,
            )
            .map_err(RefreshError::from)?;
            let left_refresh = io
                .current_base(source, &left_occurrence.table, connector_context)
                .await
                .map_err(RefreshError::from)?;
            let right_refresh = io
                .current_base(source, &right_occurrence.table, connector_context)
                .await
                .map_err(RefreshError::from)?;
            let left_observation = io
                .schema(source, &left_occurrence.table, connector_context)
                .await?;
            let right_observation = io
                .schema(source, &right_occurrence.table, connector_context)
                .await?;
            // The join validator demands D occurrence order, which is not the
            // join's own left/right order, so the observations are re-paired
            // with their occurrences rather than with the join sides. By
            // occurrence, not by table: in a self-join both sides read the
            // same table.
            let bases = occurrences
                .iter()
                .copied()
                .map(|occurrence| {
                    if occurrence.occurrence_id == left_occurrence.occurrence_id.get() {
                        Ok((occurrence, &left_observation))
                    } else if occurrence.occurrence_id == right_occurrence.occurrence_id.get() {
                        Ok((occurrence, &right_observation))
                    } else {
                        Err(RefreshError::user(format!(
                            "iceberg join aggregate MV D occurrence {} is neither join side",
                            occurrence.occurrence_id
                        )))
                    }
                })
                .collect::<Result<Vec<_>, RefreshError>>()?;
            let renames = validate_join_schema_contract(mv_definition, &bases, target_observation)
                .map_err(RefreshError::from)?;
            require_no_occurrence_rebind(&renames).map_err(RefreshError::from)?;

            let mut snapshot_pins = BTreeMap::new();
            let mut current_snapshots = BTreeMap::new();
            current_snapshots.insert(
                left_occurrence.occurrence_id,
                left_refresh.current_snapshot_id(),
            );
            current_snapshots.insert(
                right_occurrence.occurrence_id,
                right_refresh.current_snapshot_id(),
            );
            for base in &base_occurrences {
                snapshot_pins.insert(
                    base.occurrence_id,
                    current_snapshots
                        .get(&base.occurrence_id)
                        .copied()
                        .flatten(),
                );
            }
            let previous_locators = previous_refresh_locators(
                io,
                state_baseline,
                source.connector_control(),
                connector_context,
            )
            .await?;
            let previous_snapshots = &previous_locators.snapshots;
            let refresh_label = format!(
                "iceberg join aggregate MV {}.{}.{}",
                iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
            );
            let refresh_statuses = base_snapshot_statuses_for_plan(
                &base_occurrences,
                previous_snapshots,
                &current_snapshots,
            );
            let decision = decide_requested_refresh_plan(
                &RefreshPlanningInput {
                    snapshot_policy: BaseSnapshotPolicy::JoinPairPartialInitialSkip,
                    base_snapshots: &refresh_statuses,
                    label: &refresh_label,
                },
                stmt.full,
            )
            .map_err(RefreshError::from)?;
            let has_previous = base_occurrences
                .iter()
                .any(|base| previous_snapshots.contains_key(&base.occurrence_id));
            if has_previous {
                for base in &base_occurrences {
                    let named = base.display();
                    previous_snapshots
                        .get(&base.occurrence_id)
                        .copied()
                        .ok_or_else(|| {
                            RefreshError::user(format!(
                                "iceberg join aggregate MV {}.{}.{} has partial previous refresh snapshots; recreate the MV",
                                iceberg_target.catalog,
                                iceberg_target.namespace,
                                iceberg_target.table
                            ))
                        })?;
                    current_snapshots
                        .get(&base.occurrence_id)
                        .copied()
                        .flatten()
                        .ok_or_else(|| {
                            RefreshError::user(format!(
                                "cannot refresh iceberg join aggregate MV {}.{}.{}: previously-refreshed base snapshot for {} is no longer reachable",
                                iceberg_target.catalog,
                                iceberg_target.namespace,
                                iceberg_target.table,
                                named
                            ))
                        })?;
                }
            }
            let affected_partitions = unknown_join_affected_partitions();
            log_planned_iceberg_mv_affected_partitions(iceberg_target, &affected_partitions);
            Ok(build_iceberg_refresh_plan(
                mv_definition,
                target,
                stmt,
                current_catalog,
                current_database,
                &base_occurrences,
                snapshot_pins,
                decision.refresh,
                state_baseline.clone(),
                affected_partitions,
            ))
        }
    }
}

/// Read-only policy projection over the already captured source/target facts.
/// It performs no target reload, management recovery, publication or split opening.
pub(crate) async fn captured_refresh_decision_original(
    io: &MvRefreshPreparationIo,
    source: &IcebergMvCorePorts,
    projection: &StoredKeyedMvProjection,
    bindings: &novarocks_mv_application::persistence::runtime_bindings::MvRuntimeBindings,
    baseline: &RefreshStateBaseline,
    query: &novarocks_parser::ast::Query,
    full: bool,
    context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<ExecutableRefreshDecision, RefreshError> {
    let refs = definition_base_refs(projection)?;
    let bases = base_relation_occurrences(projection, &refs)?;
    let previous =
        previous_refresh_locators(io, baseline, source.connector_control(), context).await?;
    let mut current = BTreeMap::new();
    for base in &bases {
        current.insert(
            base.occurrence_id,
            io.current_base(source, &base.table, context)
                .await?
                .current_snapshot_id(),
        );
    }
    let caps = RefreshCapabilities::from_canonical_facts(
        projection.facts.interpretation(),
        bindings,
        projection.facts.definition().relation_occurrences.len(),
        definition_has_join(query),
    )?;
    let statuses = base_snapshot_statuses_for_plan(&bases, &previous.snapshots, &current);
    Ok(decide_requested_refresh_plan(
        &RefreshPlanningInput {
            snapshot_policy: caps.snapshot_policy,
            base_snapshots: &statuses,
            label: "EXPLAIN REFRESH MATERIALIZED VIEW",
        },
        full,
    )?
    .refresh)
}

async fn previous_refresh_locators(
    io: &MvRefreshPreparationIo,
    baseline: &RefreshStateBaseline,
    connector_control: &dyn ConnectorControlRegistry,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<PreviousRefreshLocators, RefreshError> {
    let previous_sources = match baseline {
        RefreshStateBaseline::SnapshotBacked {
            previous_sources, ..
        } => previous_sources.as_slice(),
    };
    let predecessors = crate::mv::domain::refresh::planning::baseline_predecessors_original(
        io,
        previous_sources,
        connector_control,
        connector_context,
    )
    .await?;
    Ok(PreviousRefreshLocators {
        snapshots: predecessors.snapshots,
        table_object_ids: predecessors.table_object_ids,
    })
}

pub(crate) async fn plan_aggregate_mv_affected_partitions(
    io: &MvRefreshPreparationIo,
    source: &IcebergMvCorePorts,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    base: &RefreshBaseRelationOccurrence,
    state_baseline: &RefreshStateBaseline,
    projection: &StoredKeyedMvProjection,
    target_partition: &mv_schema::MvPartitionContract,
    mode: RefreshMode,
    previous_snapshot_id: Option<i64>,
    current_snapshot_id: Option<i64>,
) -> Result<crate::mv::domain::model::AffectedTargetPartitions, RefreshError> {
    use crate::mv::domain::model::AffectedTargetPartitions;
    match mode {
        RefreshMode::Noop => Ok(noop_affected_partitions(target_partition)),
        RefreshMode::Incremental => {
            let previous = previous_snapshot_id
                .ok_or("incremental partition planning has no From snapshot")?;
            let current =
                current_snapshot_id.ok_or("incremental partition planning has no To snapshot")?;
            plan_prepared_base_partitions(
                io,
                source,
                connector_context,
                base,
                state_baseline,
                projection,
                target_partition,
                previous,
                current,
            )
            .await
        }
        RefreshMode::Full | RefreshMode::Rebuild => {
            if is_unpartitioned_mv_target(target_partition) {
                return Ok(AffectedTargetPartitions::Unpartitioned);
            }
            crate::mv::domain::partition::planner::plan_affected_partitions(
                &crate::mv::domain::partition::planner::AffectedPartitionPlanInput {
                    projection,
                    source_occurrence_id: base.occurrence_id.get(),
                    target_partition,
                    prepared_source: None,
                },
            )
            .map_err(Into::into)
        }
    }
}

async fn plan_prepared_base_partitions(
    io: &MvRefreshPreparationIo,
    source: &IcebergMvCorePorts,
    context: &novarocks_spi::connector::ConnectorRequestContext,
    base: &RefreshBaseRelationOccurrence,
    baseline: &RefreshStateBaseline,
    projection: &StoredKeyedMvProjection,
    target_partition: &mv_schema::MvPartitionContract,
    previous: i64,
    current: i64,
) -> Result<crate::mv::domain::model::AffectedTargetPartitions, RefreshError> {
    let revision =
        crate::mv::domain::refresh::planning::baseline_revision_for_occurrence(baseline, base)?;
    let captured = io.captured_sources().require(&base.table)?;
    if captured.snapshot != Some(current) {
        return Err("partition planning To differs from its original pin".into());
    }
    let receipt = captured
        .receipt(Some(revision))
        .ok_or("partition planning has no original typed window")?;
    let novarocks_spi::connector::read_stack::ConnectorReadSourceSelection::ChangeWindow(window) =
        receipt.request().selection()
    else {
        return Err("partition planning cannot use a snapshot as a window".into());
    };
    if window.from_snapshot_id() != previous || window.to_snapshot_id() != current {
        return Err(
            "partition planning numeric window differs from its exact prepared source".into(),
        );
    }
    let observation = io.schema(source, &base.table, context).await?;
    crate::mv::domain::partition::planner::plan_affected_partitions(
        &crate::mv::domain::partition::planner::AffectedPartitionPlanInput {
            projection,
            source_occurrence_id: base.occurrence_id.get(),
            target_partition,
            prepared_source: Some(
                crate::mv::domain::partition::planner::PreparedPartitionSource {
                    receipt: &receipt,
                    planning: &captured.lease,
                    schema: &observation,
                },
            ),
        },
    )
    .map_err(Into::into)
}

async fn plan_multi_base_affected_partitions_original(
    io: &MvRefreshPreparationIo,
    source: &IcebergMvCorePorts,
    baseline: &RefreshStateBaseline,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    projection: &StoredKeyedMvProjection,
    target_partition: &mv_schema::MvPartitionContract,
    mode: RefreshMode,
    window: OccurrenceSnapshotWindow<'_>,
    context: &str,
) -> Result<crate::mv::domain::model::AffectedTargetPartitions, RefreshError> {
    use crate::mv::domain::model::AffectedTargetPartitions;
    let OccurrenceSnapshotWindow {
        bases,
        previous,
        current,
    } = window;
    match mode {
        RefreshMode::Noop => Ok(noop_affected_partitions(target_partition)),
        RefreshMode::Full | RefreshMode::Rebuild => {
            if is_unpartitioned_mv_target(target_partition) {
                Ok(AffectedTargetPartitions::Unpartitioned)
            } else {
                Ok(AffectedTargetPartitions::not_derived(format!(
                    "{context}: full refresh affected partition planning is not implemented"
                )))
            }
        }
        RefreshMode::Incremental => {
            let mut results = Vec::with_capacity(bases.len());
            for base in bases {
                let previous = previous
                    .get(&base.occurrence_id)
                    .copied()
                    .ok_or("incremental partition planning has no From snapshot")?;
                let current = current
                    .get(&base.occurrence_id)
                    .copied()
                    .flatten()
                    .ok_or("incremental partition planning has no To snapshot")?;
                let result = plan_prepared_base_partitions(
                    io,
                    source,
                    connector_context,
                    base,
                    baseline,
                    projection,
                    target_partition,
                    previous,
                    current,
                )
                .await?;
                let stop = matches!(result, AffectedTargetPartitions::NotDerived { .. });
                results.push((base.display(), result));
                if stop {
                    break;
                }
            }
            Ok(merge_affected_partition_results(context, results))
        }
    }
}

async fn reobserve_current_management_before_refresh(
    io: &MvRefreshPreparationIo,
    source: &IcebergMvCorePorts,
    target: &IcebergMvTarget,
    context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<bool, RefreshError> {
    let instance = ConnectorInstanceId::parse(&target.catalog).map_err(|e| e.to_string())?;
    let table = novarocks_spi::connector::ConnectorTableIdentity {
        instance_id: instance.clone(),
        namespace: Arc::from(target.namespace.as_str()),
        table: Arc::from(target.table.as_str()),
    };
    if !source
        .management_entrance()?
        .management_phase(&table)
        .is_manageable()
    {
        return Ok(false);
    }
    let lease =
        crate::connector::metadata_binding_typed(source.connector_control(), &target.catalog)?;
    let metadata = io
        .metadata(
            &lease,
            context.clone(),
            &target.namespace,
            &target.table,
            novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
        )
        .await?;
    let schema = crate::mv::domain::storage_observation::observe_schema_validation(
        source.storage_observation(),
        &lease,
        &metadata,
        context.clone(),
    )?;
    crate::mv::domain::refresh::observation::validate_observed_schema_locator(
        schema.table(),
        &table,
    )?;
    let documents = lease.derive_document_storage_lease()?;
    let request = crate::mv::domain::staged_create::keyed_request(
        &documents,
        table,
        schema.table_object_id().clone(),
        context.clone(),
    );
    let observed = io
        .observe_current(source, &documents, request.clone())
        .await?;
    let closed = io
        .close_mismatch(source, &documents, request.clone())
        .await?;
    if closed {
        tracing::warn!(catalog=%target.catalog,database=%target.namespace,name=%target.table,
            "fresh Current MV management slot names another writer; management closed");
    } else if let Some(ready) = io
        .load_ready(
            source,
            MvTarget {
                catalog: Some(target.catalog.clone()),
                database: target.namespace.clone(),
                name: target.table.clone(),
            },
        )
        .await?
    {
        let expected = &ready.projection.facts;
        if expected.metadata_version() != &observed.view.metadata_version
            && crate::mv::domain::staged_create::metadata_only_current_matches(expected, &observed)
        {
            io.install_current(
                source,
                request.catalog_handle.clone(),
                Arc::new(expected.clone()),
                context.clone(),
            )
            .await?;
        }
    }
    Ok(closed)
}
async fn reconcile_published_lake_projection(
    io: &MvRefreshPreparationIo,
    source: &IcebergMvCorePorts,
    target: &IcebergMvTarget,
    binding: &crate::mv::domain::refresh::target_binding::MvTargetBinding,
    context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<(), RefreshError> {
    let catalog = binding.lease().binding().catalog_handle()?.clone();
    let target_identity = novarocks_mv_application::product::MvTarget::try_new(
        Some(target.catalog.clone()),
        target.namespace.clone(),
        target.table.clone(),
    )
    .map_err(|error| RefreshError::user(error.to_string()))?;
    // Capture the read-only Source's exact table before lending it to the
    // readiness operation; only that operation performs the document read.
    let metadata = io
        .metadata(
            binding.lease(),
            context.clone(),
            &target.namespace,
            &target.table,
            novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
        )
        .await?;
    let schema = crate::mv::domain::storage_observation::observe_schema_validation(
        source.storage_observation(),
        binding.lease(),
        &metadata,
        context.clone(),
    )?;
    crate::mv::domain::refresh::observation::validate_observed_schema_locator(
        schema.table(),
        &metadata.identity,
    )?;
    io.rebuild_read_only(
        source,
        binding.lease().clone(),
        catalog,
        target_identity,
        metadata.identity.clone(),
        schema.table_object_id().clone(),
        context.clone(),
    )
    .await?;
    Ok(())
}

/// The original document observation runs outside the pure reader's CPU turn.
pub(crate) async fn validate_target_snapshot_original(
    io: &MvRefreshPreparationIo,
    source: &IcebergMvCorePorts,
    target: &IcebergMvTarget,
    definition: &StoredKeyedMvProjection,
    binding: &crate::mv::domain::refresh::target_binding::MvTargetBinding,
    context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<(), RefreshError> {
    let documents = binding.lease().derive_document_storage_lease()?;
    let request = crate::mv::domain::staged_create::keyed_request(
        &documents,
        binding.identity().clone(),
        definition.facts.source_revision().target_object_id.clone(),
        context.clone(),
    );
    let observed = io.observe_current(source, &documents, request).await?;
    crate::mv::domain::refresh::target::validate_target_snapshot_observed(
        target, definition, binding, &observed,
    )
    .map_err(Into::into)
}

#[cfg(test)]
mod partition_planning_tests {
    use super::*;
    use crate::mv::domain::test_admission::KeyedProjectionFixture as ProjectionFixture;
    use crate::query_execution::{
        internal_result_cpu::InternalResultCpuOwner,
        maintenance::io_admission::MetadataIoAdmission,
        mv_assembly::refresh_preparation::io_gate::MvRefreshPreparationServices,
    };
    use crate::task_execution::status_intake::StatusIntakeWake;
    use mv_schema::{MvPartitionContract, MvPartitionFieldContract, MvPartitionTransformContract};
    use novarocks_query_application::{
        cancellation::QueryCancellationView, cpu::QueryBlockingExecutorConfig,
    };
    use novarocks_spi::connector::{
        ConnectorControlResolver, ConnectorRequestContext, ConnectorStopOwner,
    };
    use novarocks_state_store_api::{
        StateStoreExecutionContext, StateStoreExecutionFailure, StateStoreExecutionRequests,
        StateStoreReadExecution,
    };
    use novarocks_workload_control::{
        ControlStorageConfig, ResultCapacityConfig, WorkClass, WorkRequest, WorkloadConfig,
        WorkloadControl,
        joint_admission::{JointProgress, ResponsibilityConfig},
    };
    use std::{
        future::{Future, poll_fn},
        num::NonZeroUsize,
        task::Poll,
        time::{Duration, Instant},
    };

    struct NoRepositoryIo;
    impl StateStoreReadExecution for NoRepositoryIo {
        fn read_transaction_context(
            &self,
        ) -> Result<StateStoreExecutionContext, StateStoreExecutionFailure> {
            panic!("unchanged bases must not request repository I/O")
        }
    }
    impl StateStoreExecutionRequests for NoRepositoryIo {
        fn write_operation_context(
            &self,
        ) -> Result<StateStoreExecutionContext, StateStoreExecutionFailure> {
            panic!("unchanged bases must not request a repository write")
        }
    }
    #[derive(Debug)]
    struct Wake;
    impl StatusIntakeWake for Wake {
        fn wake(&self) {}
    }

    async fn with_driver<F: Future>(
        control: &WorkloadControl,
        driver: &mut novarocks_workload_control::joint_admission::JointProgressDriver,
        future: F,
    ) -> F::Output {
        tokio::pin!(future);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match driver.step() {
                    JointProgress::More => tokio::task::yield_now().await,
                    JointProgress::Idle(revision) => tokio::select! {
                        biased;
                        result = &mut future => return result,
                        _ = control.wait_progress(revision) => {},
                    },
                }
            }
        })
        .await
        .expect("unchanged-base component cleanup watchdog")
    }

    fn base_ref(table: &str) -> TableIdentity {
        TableIdentity {
            catalog: "ice".to_string(),
            namespace: "db".to_string(),
            table: table.to_string(),
        }
    }

    /// The MV target's partitioning is the provider's own typed observation,
    /// carried by the exact target binding. The retired schema contract used to
    /// keep a second copy; it is gone.
    fn identity_partition() -> MvPartitionContract {
        MvPartitionContract {
            target_spec_id: 7,
            fields: vec![MvPartitionFieldContract {
                partition_field_id: 100,
                partition_field_name: "id".to_string(),
                source_target_field_id: 10,
                source_column_name: "id".to_string(),
                transform: MvPartitionTransformContract::Identity,
            }],
        }
    }

    fn projection() -> StoredKeyedMvProjection {
        StoredKeyedMvProjection {
            mv_id: 1,
            facts: ProjectionFixture::new(
                novarocks_mv_application::product::MvTarget::from_parts(Some("ice"), "sales", "mv"),
                Some(11),
            )
            .build()
            .expect("fixture projection"),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn unchanged_partition_bases_require_the_original_exact_window_preparation() {
        let projection = projection();
        let partition = identity_partition();
        let base_refs = vec![
            RefreshBaseRelationOccurrence {
                occurrence_id: SqlMvRelationOccurrenceId::new(0),
                table: base_ref("left"),
            },
            RefreshBaseRelationOccurrence {
                occurrence_id: SqlMvRelationOccurrenceId::new(1),
                table: base_ref("right"),
            },
        ];
        let previous_snapshots = BTreeMap::from([
            (SqlMvRelationOccurrenceId::new(0), 11_i64),
            (SqlMvRelationOccurrenceId::new(1), 22_i64),
        ]);
        let current_snapshots = BTreeMap::from([
            (SqlMvRelationOccurrenceId::new(0), Some(11_i64)),
            (SqlMvRelationOccurrenceId::new(1), Some(22_i64)),
        ]);

        let mut control = WorkloadControl::try_new_counted(WorkloadConfig {
            root_limit: 8,
            business_limit: 8,
            query_concurrency_limit: 8,
            scope_records_limit: 8,
            ..WorkloadConfig::default()
        })
        .unwrap()
        .owner;
        let (_, mut driver) = control
            .configure_responsibility(ResponsibilityConfig {
                work_per_turn: NonZeroUsize::new(7).unwrap(),
            })
            .unwrap();
        control
            .configure_control_storage(ControlStorageConfig::new(8, &[]))
            .unwrap();
        control
            .configure_result_capacity(ResultCapacityConfig::V1)
            .unwrap();
        let mut cpu = InternalResultCpuOwner::try_new(
            QueryBlockingExecutorConfig::new(
                NonZeroUsize::new(1).unwrap(),
                NonZeroUsize::new(1).unwrap(),
            ),
            &mut control,
        )
        .unwrap();
        control.mark_ready().unwrap();
        let root = control
            .root_admission()
            .try_begin_root(WorkRequest::new(WorkClass::Management))
            .unwrap();
        let scope = root.owner.scope();
        let io = MetadataIoAdmission::for_original_source(&root.owner, Vec::new()).unwrap();
        with_driver(
            &control,
            &mut driver,
            poll_fn(|cx| {
                if io.poll_progress(&root.owner, cx).unwrap() {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }),
        )
        .await;
        let capacity = io.root_capacity().unwrap();
        let window = capacity.try_window_alias().unwrap();
        let stop = ConnectorStopOwner::new();
        let context = ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(5),
            stop.view(),
            4096,
            64 * 1024,
        )
        .unwrap()
        .with_io_admission(io.clone());
        let cancellation = QueryCancellationView::governed(scope.cancellation().unwrap(), None);
        let gate = Arc::new(MvRefreshPreparationIo::new(
            MvRefreshPreparationServices {
                cpu: cpu.runtime(),
                scope: scope.clone(),
                window: window.clone(),
                cancellation,
                runtime: tokio::runtime::Handle::current(),
                wake: Arc::new(Wake),
                requests: Arc::new(NoRepositoryIo),
            },
            io.original_result_scope(&context).unwrap(),
        ));
        let source_revision = projection.facts.source_revision();
        let instance = source_revision.target.instance_id.clone();
        let binding = novarocks_catalog_application::test_support::test_control_binding_for(
            instance.clone(),
            8,
        )
        .with_catalog_properties(crate::mv::domain::test_admission::catalog_properties(
            &instance,
        ))
        .unwrap();
        let host = Arc::new(novarocks_catalog_application::ConnectorControlHost::new());
        host.register(binding).unwrap();
        let planning = host.acquire_current(&instance).unwrap();
        let (_product, repository) = crate::mv::domain::test_admission::keyed_product().await;
        let entrance = crate::mv::domain::test_admission::management_entrance();
        let readiness = Arc::new(crate::mv::domain::readiness::MvReadinessPort::from_product(
            novarocks_mv_application::readiness::MvKeyedReadinessService::new(
                repository,
                Arc::default(),
            ),
            tokio::runtime::Handle::current(),
            entrance.clone(),
        ));
        let ports = IcebergMvCorePorts::new_with_management_entrance(
            Arc::new(novarocks_sql::compiler::build_builtin_engine_function_catalog().unwrap()),
            Arc::new(crate::catalog_application::query_catalog::new_query_catalog_service()),
            None,
            host.clone(),
            host.clone(),
            readiness,
            Arc::new(novarocks_spi::connector::UnavailableMvStorageObservationPort),
            entrance,
            crate::application::test_constant_policy(),
        );
        // Equal numeric IDs cannot replace the original exact source/window preparation.
        let baseline = RefreshStateBaseline::SnapshotBacked {
            previous_sources: Vec::new(),
            target_snapshot_id: projection.facts.publication_snapshot(),
            target_table_uuid: "unchanged-partition-target".to_string(),
            definition_fingerprint: mv_definition_fingerprint(
                &projection.facts.definition().query.effective_sql,
            ),
        };
        let planned = {
            let mut future = std::pin::pin!(plan_multi_base_affected_partitions_original(
                &gate,
                &ports,
                &baseline,
                &context,
                &projection,
                &partition,
                RefreshMode::Incremental,
                OccurrenceSnapshotWindow {
                    bases: &base_refs,
                    previous: &previous_snapshots,
                    current: &current_snapshots,
                },
                "UNION ALL MV affected partition planning",
            ));
            // Exactly one poll: a requested change window cannot be serviced here.
            poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx))).await
        };
        // Preserve the no-call observation before close may withdraw an unstarted request.
        let requested_change_window = gate.has_request();
        with_driver(&control, &mut driver, poll_fn(|cx| gate.poll_closed(cx)))
            .await
            .unwrap();
        drop(gate);
        drop(ports);
        drop(planning);
        drop(host);
        drop(context);
        let _ = with_driver(
            &control,
            &mut driver,
            poll_fn(|cx| io.poll_actual_quiescent_continuing(cx)),
        )
        .await
        .unwrap();
        drop(window);
        drop(capacity);
        drop(io);
        drop(scope);
        root.owner.complete();
        root.business.release();
        drop(driver);
        drop(control);
        cpu.shutdown_until(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
        assert!(
            !requested_change_window,
            "unprepared source facts must fail before submitting physical work"
        );
        let Poll::Ready(planned) = planned else {
            panic!("missing original exact facts must refuse synchronously");
        };

        let error = planned
            .expect_err("unprepared unchanged windows cannot authorize an empty partition set");
        assert!(error.to_string().contains("no exact revision"));
    }
}
