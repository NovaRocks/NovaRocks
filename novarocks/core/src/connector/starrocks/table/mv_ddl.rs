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

//! Engine-boundary helpers for CREATE / DROP / SHOW MATERIALIZED VIEW.
//!
//! REFRESH lives in `mv_refresh.rs` because it needs the query executor.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::connector::starrocks::ObjectStoreProfile;
use crate::connector::starrocks::lake::context::{get_tablet_runtime, remove_tablet_runtime};
use crate::connector::starrocks::lake::schema::create_lake_tablet_from_req_with_schema_patch;
use crate::connector::starrocks::lake::storage_schema_wire::encode_tablet_schema_bytes;
use crate::connector::starrocks::lake::transactions::delete_tablet;
use crate::engine::query_prep::drop_local_table_registration_if_exists;
use crate::formats::starrocks::metadata::load_tablet_snapshot;
use crate::meta::MetaReadTxn;
use crate::meta::repository::mv::{CreateMvDefinitionRequest, MvRefreshState};
use crate::meta::repository::starrocks_table::{
    CreateStarRocksColumnRequest, CreateStarRocksTableLayoutRequest,
    StarRocksTableKind as RepoStarRocksTableKind,
};
use crate::mv::persistence::definition::{StoredMvDefinition, StoredMvRefreshPolicy};
use crate::service::grpc_client::proto::starrocks::DeleteTabletRequest;
use crate::sql::analysis::{ExprKind, OutputColumn, QueryBody, ResolvedQuery};
use crate::sql::parser::ast::{
    CreateMaterializedViewStmt, DropMaterializedViewStmt, MaterializedViewDistribution,
    ShowMaterializedViewsStmt, TableKeyDesc, TableKeyKind,
};
use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_catalog::identifier::normalize_identifier;

use crate::connector::starrocks::table::catalog::{
    StarRocksTableCatalog, register_starrocks_table_in_catalog,
};
use crate::connector::starrocks::table::ddl::{
    keys_type_name, patch_tablet_schema_column_flags, stored_columns_from_physical_columns,
    table_columns_from_physical_columns,
};
use crate::connector::starrocks::table::model::{
    StarRocksMvStorageEngine, StarRocksTableKind, StarRocksTableState,
};
use crate::connector::starrocks::table::schema_adapter::{
    build_create_tablet_request, build_tablet_schema,
};
use crate::engine::mv::lifecycle::MvListRow;
use crate::engine::{StandaloneState, StatementResult};
use crate::mv::aggregate_state::mv_shape::{AggregateMvShape, IncrementalMvShape};
use crate::mv::aggregate_state::physical_column::{
    StarRocksPhysicalColumn, starrocks_physical_column,
    validate_unique_aggregate_physical_column_names,
};
use crate::mv::aggregate_state::sql_type::arrow_data_type_to_sql_type;
use crate::mv::analysis::{
    MvAnalysis, ResolvedTableRef, analyze_mv_select_with, output_column_to_table_column,
    resolve_mv_name, validate_aggregate_distribution_columns, validate_distribution_columns,
    validate_starrocks_mv_partition_columns,
};
use crate::mv::model::{AggregateFunctionKind, MvStorageEngine, VisibleAggregateOutput};
use crate::runtime::query_result::{QueryResult, QueryResultColumn, record_batch_to_chunk};
use novarocks_catalog::identifier::TableIdentity;

pub(crate) fn resolve_mv_storage_engine(
    properties: &[(String, String)],
    default_from_config: &str,
) -> Result<StarRocksMvStorageEngine, String> {
    let property = properties
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("storage_engine"))
        .map(|(_, v)| v.as_str());
    let raw = property.unwrap_or(default_from_config);
    StarRocksMvStorageEngine::parse_sql_str(raw)
}

pub(crate) fn create_mv(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    stmt: &CreateMaterializedViewStmt,
) -> Result<StatementResult, String> {
    let (db_name, mv_name) = resolve_mv_name(&stmt.name, current_database)?;
    let default_engine = state
        .starrocks_table_config
        .as_ref()
        .map(|c| c.mv_default_storage_engine.as_str())
        .unwrap_or("starrocks");
    let storage_engine = resolve_mv_storage_engine(&stmt.properties, default_engine)?;
    {
        let catalog = state
            .catalog_service
            .local()
            .read()
            .expect("standalone catalog read lock");
        let database_exists = catalog.database_exists(&db_name)?;
        if !database_exists && storage_engine != StarRocksMvStorageEngine::Iceberg {
            return Err(format!("unknown database: {db_name}"));
        }
        if database_exists
            && storage_engine != StarRocksMvStorageEngine::Iceberg
            && catalog.get(&db_name, &mv_name).is_ok()
        {
            if stmt.if_not_exists {
                return Ok(StatementResult::Ok);
            }
            return Err(format!(
                "materialized view or table already exists: {db_name}.{mv_name}"
            ));
        }
    }

    if storage_engine == StarRocksMvStorageEngine::Iceberg {
        return Err(
            "StarRocks table MV backend cannot create storage_engine='iceberg' materialized views"
                .to_string(),
        );
    }

    let starrocks_table_config = state
        .starrocks_table_config
        .clone()
        .ok_or_else(|| "standalone StarRocks table config is missing".to_string())?;
    let provider = state.metadata_provider.as_ref().ok_or_else(|| {
        "StarRocks table create materialized view requires metadata provider".to_string()
    })?;

    let analysis = analyze_mv_select(state, current_catalog, current_database, &stmt.select_query)?;
    validate_starrocks_mv_partition_columns(
        stmt.partition_by.as_deref(),
        &analysis.output_columns,
    )?;
    let created_at_ms = now_ms();
    let resolved_dependencies = crate::engine::mv::dependency::resolve_create_mv_dependencies(
        state,
        &analysis.resolved_refs,
        created_at_ms,
    )?;
    let dependency_target =
        crate::mv::dependency::model::starrocks_mv_dependency_ref(&db_name, &mv_name);
    // Defensive: this check runs after the StarRocks table "already exists" guard
    // above, so user-facing CREATE statements can't reach it (a brand-new MV
    // target has no inbound edges, while an existing target fails on existence
    // first). Kept as a safety net for future paths that bypass the existence
    // check — e.g. ALTER MATERIALIZED VIEW rewriting a SELECT, or racy
    // metadata writes. Canonical cycle algorithm coverage lives in
    // crate::mv::dependency::graph::tests.
    crate::engine::mv::dependency::validate_no_create_cycle(
        state,
        &dependency_target,
        &resolved_dependencies.dependencies,
    )
    .map_err(|e| format!("cannot create materialized view {db_name}.{mv_name}: {e}"))?;
    let base_refs = resolved_dependencies.base_refs;

    // IVM Phase-2 PRIMARY KEY validation. Only runs when the user opted in
    // by writing `PRIMARY KEY (...)` in the DDL; otherwise behavior is
    // unchanged.
    let primary_key_base_descriptor = if let Some(pk_cols) = stmt.primary_key.as_deref() {
        if base_refs.len() != 1 {
            return Err(
                "PRIMARY KEY on materialized view requires exactly one iceberg base table"
                    .to_string(),
            );
        }
        let base_ref = &base_refs[0];
        let loaded =
            crate::connector::starrocks::table::mv_refresh::load_current_iceberg_base_table(
                state, base_ref,
            )?;
        let descriptor = descriptor_from_loaded(&loaded);
        validate_ivm_primary_key(pk_cols, &descriptor).map_err(|e| e.to_string())?;
        Some(descriptor)
    } else {
        None
    };

    let distribution = stmt
        .distribution
        .as_ref()
        .ok_or_else(|| "CREATE MATERIALIZED VIEW requires DISTRIBUTED BY".to_string())?;
    let bucket_count = distribution.bucket_count.ok_or_else(|| {
        "DISTRIBUTED BY HASH(...) BUCKETS n is required (BUCKETS <n> is mandatory in phase 1)"
            .to_string()
    })?;
    if analysis.output_columns.is_empty() {
        return Err("materialized view SELECT must produce at least one column".to_string());
    }
    let mv_shape =
        crate::mv::aggregate_state::mv_shape::classify_incremental_mv_query(&stmt.select_query)?;
    validate_incremental_mv_analyzed_types(&mv_shape, &analysis.resolved_query)?;
    let storage_layout = build_mv_storage_layout(
        &mv_shape,
        distribution,
        &analysis.output_columns,
        Some(&analysis.resolved_query),
        stmt.primary_key.as_deref().unwrap_or(&[]),
        primary_key_base_descriptor.as_ref(),
    )?;
    let key_desc = storage_layout.key_desc;
    let physical_columns = storage_layout.physical_columns;

    let mut starrocks = state
        .starrocks_table
        .write()
        .expect("standalone StarRocks table write lock");
    if starrocks.contains_table(&db_name, &mv_name)? {
        if stmt.if_not_exists {
            return Ok(StatementResult::Ok);
        }
        return Err(format!(
            "materialized view or table already exists: {db_name}.{mv_name}"
        ));
    }

    let bucket_num = i64::from(bucket_count);
    if bucket_num <= 0 {
        return Err("CREATE MATERIALIZED VIEW requires BUCKETS > 0".to_string());
    }

    let table_columns = table_columns_from_physical_columns(&physical_columns);
    let stored_columns = stored_columns_from_physical_columns(0, &key_desc, &physical_columns)
        .into_iter()
        .map(|column| CreateStarRocksColumnRequest {
            column_name: column.column_name,
            logical_type: column.logical_type,
            nullable: column.nullable,
            visible: column.visible,
            is_key: column.is_key,
        })
        .collect::<Vec<_>>();

    let mut txn = provider
        .begin_write("create StarRocks table materialized view")
        .map_err(|e| format!("open StarRocks materialized view create transaction failed: {e}"))?;
    let database = state
        .starrocks_table_repo
        .get_or_create_database(txn.as_mut(), &db_name)
        .map_err(|e| format!("create StarRocks database metadata failed: {e}"))?;
    let reclaimed = state
        .starrocks_table_repo
        .purge_dropping_table_for_reuse(txn.as_mut(), database.db_id, &mv_name)
        .map_err(|e| format!("reclaim dropping StarRocks table metadata failed: {e}"))?;
    for table_id in &reclaimed {
        state
            .starrocks_txn_repo
            .delete_for_table(txn.as_mut(), *table_id)
            .map_err(|e| format!("delete reclaimed StarRocks txns failed: {e}"))?;
        state
            .job_repo
            .delete_for_table(txn.as_mut(), *table_id)
            .map_err(|e| format!("delete reclaimed erase jobs failed: {e}"))?;
        state
            .mv_repo
            .drop_by_id(txn.as_mut(), *table_id)
            .map_err(|e| format!("delete reclaimed materialized view definition failed: {e}"))?;
    }

    let created = state
        .starrocks_table_repo
        .create_table_layout(
            txn.as_mut(),
            CreateStarRocksTableLayoutRequest {
                db_id: database.db_id,
                table_name: mv_name.clone(),
                keys_type: keys_type_name(key_desc.kind).to_string(),
                bucket_num,
                kind: RepoStarRocksTableKind::MaterializedView,
                schema_version: 0,
                tablet_schema_pb: Vec::new(),
                columns: stored_columns,
                partition_name: "p0".to_string(),
                warehouse_uri: starrocks_table_config.warehouse_uri.clone(),
            },
        )
        .map_err(|e| format!("create StarRocks materialized view metadata failed: {e}"))?;
    let request_schema = build_tablet_schema(&table_columns, &key_desc, created.schema.schema_id)?;
    let mut tablet_schema_pb =
        crate::connector::starrocks::lake::schema_adapter::build_tablet_schema_pb_from_thrift(
            &request_schema,
        )?;
    patch_tablet_schema_column_flags(&mut tablet_schema_pb, &physical_columns)?;
    state
        .starrocks_table_repo
        .update_schema_payload(
            txn.as_mut(),
            created.schema.schema_id,
            encode_tablet_schema_bytes(&tablet_schema_pb),
        )
        .map_err(|e| format!("update StarRocks materialized view schema metadata failed: {e}"))?;
    state
        .starrocks_txn_repo
        .record_visible_bootstrap(
            txn.as_mut(),
            created.table.table_id,
            created.partition.partition_id,
        )
        .map_err(|e| {
            format!("create StarRocks materialized view bootstrap txn metadata failed: {e}")
        })?;
    let mv_definition = state
        .mv_repo
        .create_definition_with_id(
            txn.as_mut(),
            created.table.table_id,
            CreateMvDefinitionRequest {
                select_sql: stmt.select_sql.clone(),
                base_table_refs: iceberg_table_ref_fqns(&base_refs),
                primary_key_columns: stmt.primary_key.clone().unwrap_or_default(),
                storage_engine: StarRocksMvStorageEngine::StarRocks.as_sql_str().to_string(),
                target_catalog: None,
                target_namespace: None,
                target_table: None,
                schema_contract: None,
                partition_spec: None,
                created_at_ms,
            },
        )
        .map_err(|e| format!("persist materialized view definition failed: {e}"))?;
    state
        .mv_repo
        .update_refresh_metadata(
            txn.as_mut(),
            crate::engine::mv_flow::refresh_metadata_request_for_create(
                mv_definition.mv_id,
                &stmt.refresh_policy,
            ),
        )
        .map_err(|e| format!("persist materialized view refresh metadata failed: {e}"))?;
    state
        .mv_repo
        .replace_dependencies_for_mv(
            txn.as_mut(),
            mv_definition.mv_id,
            resolved_dependencies.dependencies,
        )
        .map_err(|e| format!("persist materialized view dependencies failed: {e}"))?;

    let object_store_profile =
        ObjectStoreProfile::from_s3_store_config(&starrocks_table_config.s3)?;
    let mut bootstrapped_tablet_ids = Vec::new();
    for tablet in &created.tablets {
        let request = build_create_tablet_request(
            tablet.tablet_id,
            created.table.table_id,
            created.partition.partition_id,
            request_schema.clone(),
        );
        if let Err(err) = create_lake_tablet_from_req_with_schema_patch(
            &request,
            &tablet.tablet_root_path,
            Some(starrocks_table_config.s3.clone()),
            |schema| patch_tablet_schema_column_flags(schema, &physical_columns),
        ) {
            cleanup_bootstrapped_tablets(&bootstrapped_tablet_ids);
            let _ = txn.abort();
            return Err(err);
        }
        bootstrapped_tablet_ids.push(tablet.tablet_id);
        let runtime_schema = match get_tablet_runtime(tablet.tablet_id) {
            Ok(runtime) => runtime.schema,
            Err(err) => {
                cleanup_bootstrapped_tablets(&bootstrapped_tablet_ids);
                let _ = txn.abort();
                return Err(err);
            }
        };
        let loaded = match load_tablet_snapshot(
            tablet.tablet_id,
            1,
            &tablet.tablet_root_path,
            Some(&object_store_profile),
        ) {
            Ok(loaded) => loaded,
            Err(err) => {
                cleanup_bootstrapped_tablets(&bootstrapped_tablet_ids);
                let _ = txn.abort();
                return Err(err);
            }
        };
        if loaded.tablet_schema != runtime_schema {
            cleanup_bootstrapped_tablets(&bootstrapped_tablet_ids);
            let _ = txn.abort();
            return Err(format!(
                "StarRocks tablet schema mismatch after bootstrap: tablet_id={}",
                tablet.tablet_id
            ));
        }
    }
    if let Err(err) = txn.commit() {
        cleanup_bootstrapped_tablets(&bootstrapped_tablet_ids);
        return Err(format!(
            "commit StarRocks materialized view metadata failed: {err}"
        ));
    }

    let read = provider
        .begin_read()
        .map_err(|e| format!("open StarRocks materialized view reload transaction failed: {e}"))?;
    let snapshot = state
        .starrocks_table_repo
        .load_snapshot(read.as_ref())
        .map_err(|e| format!("reload StarRocks materialized view metadata failed: {e}"))?;
    let rebuilt =
        StarRocksTableCatalog::rebuild_from_repository(Some(starrocks_table_config), snapshot)?;
    rebuilt.re_register_active_tablet_runtimes()?;
    let runtime = rebuilt.table(&db_name, &mv_name)?.clone();
    *starrocks = rebuilt;
    drop(starrocks);

    let mut catalog = state
        .catalog_service
        .local()
        .write()
        .expect("standalone catalog write lock");
    register_starrocks_table_in_catalog(&mut catalog, &runtime)?;
    Ok(StatementResult::Ok)
}

#[derive(Clone, Debug)]
struct MvStorageLayout {
    key_desc: TableKeyDesc,
    physical_columns: Vec<StarRocksPhysicalColumn>,
}

fn build_mv_storage_layout(
    mv_shape: &IncrementalMvShape,
    distribution: &MaterializedViewDistribution,
    output_columns: &[OutputColumn],
    resolved_query: Option<&ResolvedQuery>,
    primary_key_columns: &[String],
    base_descriptor: Option<&BaseTableDescriptor>,
) -> Result<MvStorageLayout, String> {
    match mv_shape {
        IncrementalMvShape::ProjectionFilter(_) => {
            validate_distribution_columns(distribution, output_columns)?;
            let visible_columns = output_columns
                .iter()
                .map(output_column_to_table_column)
                .collect::<Result<Vec<_>, _>>()?;
            let key_columns = if primary_key_columns.is_empty() {
                distribution.hash_columns.clone()
            } else {
                projection_mv_key_columns(primary_key_columns)?
            };
            let key_desc = TableKeyDesc {
                kind: if primary_key_columns.is_empty() {
                    TableKeyKind::Duplicate
                } else {
                    TableKeyKind::Primary
                },
                columns: key_columns,
            };
            let mut physical_columns = Vec::with_capacity(
                primary_key_columns
                    .len()
                    .saturating_add(visible_columns.len()),
            );
            if !primary_key_columns.is_empty() {
                let base = base_descriptor.ok_or_else(|| {
                    "projection/filter materialized view PRIMARY KEY layout requires base table descriptor"
                        .to_string()
                })?;
                physical_columns.extend(projection_mv_hidden_primary_key_columns(
                    output_columns,
                    primary_key_columns,
                    base,
                )?);
            }
            physical_columns.extend(visible_columns.iter().map(|column| {
                starrocks_physical_column(
                    column.name.clone(),
                    column.data_type.clone(),
                    column.nullable,
                    true,
                    false,
                )
            }));
            Ok(MvStorageLayout {
                key_desc,
                physical_columns,
            })
        }
        IncrementalMvShape::Aggregate(shape) => {
            validate_aggregate_distribution_columns(distribution, shape)?;
            let aggregate_input_types = if let Some(resolved_query) = resolved_query {
                crate::mv::aggregate_state::mv_agg_state::aggregate_input_types_from_resolved_query(
                    &crate::mv::aggregate_state::aggregate_sql_calls::AggregateSqlCalls::from(
                        shape,
                    ),
                    resolved_query,
                )?
            } else {
                vec![None; shape.aggregates.len()]
            };
            let layout = crate::mv::aggregate_state::mv_agg_state::build_aggregate_mv_layout_with_input_types(
                &crate::mv::aggregate_state::aggregate_sql_calls::AggregateSqlCalls::from(shape),
                output_columns,
                &aggregate_input_types,
            )?;
            validate_unique_aggregate_physical_column_names(&layout.physical_columns)?;
            Ok(MvStorageLayout {
                key_desc: TableKeyDesc {
                    kind: TableKeyKind::Primary,
                    columns: vec![
                        crate::mv::aggregate_state::mv_agg_state::ROW_ID_COLUMN.to_string(),
                    ],
                },
                physical_columns: layout.physical_columns,
            })
        }
        IncrementalMvShape::UnionAll(_) => Err(
            "UNION ALL IMV storage layout is not supported by legacy StarRocks MV DDL".to_string(),
        ),
        IncrementalMvShape::JoinProjectionFilter(_) => Err(
            "join projection/filter IMV storage layout is not supported by legacy StarRocks MV DDL"
                .to_string(),
        ),
        IncrementalMvShape::JoinAggregate(_) => Err(
            "join aggregate IMV storage layout is not supported by legacy StarRocks MV DDL"
                .to_string(),
        ),
    }
}

pub(crate) fn projection_mv_hidden_pk_column_name(key: &str) -> Result<String, String> {
    Ok(format!("__mv_pk_{}", normalize_identifier(key)?))
}

fn projection_mv_key_columns(primary_key_columns: &[String]) -> Result<Vec<String>, String> {
    primary_key_columns
        .iter()
        .map(|key| projection_mv_hidden_pk_column_name(key))
        .collect()
}

fn projection_mv_hidden_primary_key_columns(
    output_columns: &[OutputColumn],
    primary_key_columns: &[String],
    base: &BaseTableDescriptor,
) -> Result<Vec<StarRocksPhysicalColumn>, String> {
    let output_names = output_columns
        .iter()
        .map(|column| normalize_identifier(&column.name))
        .collect::<Result<HashSet<_>, _>>()?;
    let mut out = Vec::with_capacity(primary_key_columns.len());
    for key in primary_key_columns {
        let hidden_name = projection_mv_hidden_pk_column_name(key)?;
        if output_names.contains(&normalize_identifier(&hidden_name)?) {
            return Err(format!(
                "projection/filter materialized view hidden PRIMARY KEY column `{hidden_name}` collides with SELECT output"
            ));
        }
        let base_col = base
            .columns
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case(key))
            .ok_or_else(|| {
                format!(
                    "projection/filter materialized view PRIMARY KEY column `{key}` does not exist on the iceberg base table"
                )
            })?;
        out.push(starrocks_physical_column(
            hidden_name,
            arrow_data_type_to_sql_type(&base_col.data_type)?,
            base_col.nullable,
            false,
            true,
        ));
    }
    Ok(out)
}

fn validate_incremental_mv_analyzed_types(
    mv_shape: &IncrementalMvShape,
    resolved: &ResolvedQuery,
) -> Result<(), String> {
    match mv_shape {
        IncrementalMvShape::ProjectionFilter(_) => Ok(()),
        IncrementalMvShape::Aggregate(shape) => {
            validate_aggregate_mv_analyzed_types(shape, resolved)
        }
        IncrementalMvShape::UnionAll(_) => Err(
            "UNION ALL IMV analyzer validation is not supported by legacy StarRocks MV DDL"
                .to_string(),
        ),
        IncrementalMvShape::JoinProjectionFilter(_) => Err(
            "join projection/filter IMV analyzer validation is not supported by legacy StarRocks MV DDL"
                .to_string(),
        ),
        IncrementalMvShape::JoinAggregate(_) => Err(
            "join aggregate IMV analyzer validation is not supported by legacy StarRocks MV DDL"
                .to_string(),
        ),
    }
}

fn validate_aggregate_mv_analyzed_types(
    shape: &AggregateMvShape,
    resolved: &ResolvedQuery,
) -> Result<(), String> {
    let QueryBody::Select(select) = &resolved.body else {
        return Err("incremental aggregate MV analyzer result must be SELECT".to_string());
    };
    if select.projection.len() != shape.visible_outputs.len() {
        return Err(format!(
            "aggregate MV analyzer projection count mismatch: analyzed_projection={} shape_outputs={}",
            select.projection.len(),
            shape.visible_outputs.len()
        ));
    }

    for (projection_index, visible_output) in shape.visible_outputs.iter().enumerate() {
        let VisibleAggregateOutput::Aggregate(aggregate_index) = visible_output else {
            continue;
        };
        let aggregate = shape.aggregates.get(*aggregate_index).ok_or_else(|| {
            format!("aggregate MV aggregate index out of range: aggregate_index={aggregate_index}")
        })?;
        let projection = &select.projection[projection_index];
        let ExprKind::AggregateCall { name, args, .. } = &projection.expr.kind else {
            return Err(format!(
                "aggregate MV analyzed projection `{}` is not an aggregate expression",
                projection.output_name
            ));
        };
        validate_aggregate_mv_input_type(aggregate.function, name, &aggregate.output_name, args)?;
    }

    Ok(())
}

fn validate_aggregate_mv_input_type(
    function: AggregateFunctionKind,
    analyzed_name: &str,
    output_name: &str,
    args: &[crate::sql::analysis::TypedExpr],
) -> Result<(), String> {
    match function {
        AggregateFunctionKind::Sum => validate_sum_mv_input_type(analyzed_name, output_name, args),
        AggregateFunctionKind::Avg => validate_avg_mv_input_type(analyzed_name, output_name, args),
        AggregateFunctionKind::CountDistinct => {
            validate_count_distinct_mv_input_type(analyzed_name, output_name, args)
        }
        AggregateFunctionKind::ApproxCountDistinct => {
            validate_approx_count_distinct_mv_input_type(analyzed_name, output_name, args)
        }
        _ => Ok(()),
    }
}

fn validate_sum_mv_input_type(
    analyzed_name: &str,
    output_name: &str,
    args: &[crate::sql::analysis::TypedExpr],
) -> Result<(), String> {
    if !analyzed_name.eq_ignore_ascii_case("sum") {
        return Err(format!(
            "aggregate MV analyzed aggregate mismatch for `{output_name}`: expected SUM, got {analyzed_name}"
        ));
    }
    let input_type = args
        .first()
        .map(|arg| &arg.data_type)
        .ok_or_else(|| "SUM aggregate requires a column expression argument".to_string())?;
    if matches!(
        input_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Decimal128(_, _)
    ) {
        return Ok(());
    }
    Err(format!(
        "SUM state type is unsupported for aggregate `{output_name}` input: {input_type:?}"
    ))
}

fn validate_avg_mv_input_type(
    analyzed_name: &str,
    output_name: &str,
    args: &[crate::sql::analysis::TypedExpr],
) -> Result<(), String> {
    if !analyzed_name.eq_ignore_ascii_case("avg") {
        return Err(format!(
            "aggregate MV analyzed aggregate mismatch for `{output_name}`: expected AVG, got {analyzed_name}"
        ));
    }
    let input_type = args
        .first()
        .map(|arg| &arg.data_type)
        .ok_or_else(|| "AVG aggregate requires a column expression argument".to_string())?;
    if matches!(
        input_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Decimal128(_, _)
    ) {
        return Ok(());
    }
    Err(format!(
        "AVG state type is unsupported for aggregate `{output_name}` input: {input_type:?}"
    ))
}

fn validate_count_distinct_mv_input_type(
    analyzed_name: &str,
    output_name: &str,
    args: &[crate::sql::analysis::TypedExpr],
) -> Result<(), String> {
    if !matches!(
        analyzed_name.to_ascii_lowercase().as_str(),
        "count" | "count_distinct" | "multi_distinct_count"
    ) {
        return Err(format!(
            "aggregate MV analyzed aggregate mismatch for `{output_name}`: expected COUNT DISTINCT, got {analyzed_name}"
        ));
    }
    let input_type = args
        .first()
        .map(|arg| &arg.data_type)
        .ok_or_else(|| "COUNT(DISTINCT) requires exactly one column expression".to_string())?;
    if count_distinct_key_type_allowed(input_type) {
        return Ok(());
    }
    Err(format!(
        "COUNT(DISTINCT) state key type is unsupported for aggregate `{output_name}` input: {input_type:?}; project to a supported scalar key type"
    ))
}

fn validate_approx_count_distinct_mv_input_type(
    analyzed_name: &str,
    output_name: &str,
    args: &[crate::sql::analysis::TypedExpr],
) -> Result<(), String> {
    if !matches!(
        analyzed_name.to_ascii_lowercase().as_str(),
        "approx_count_distinct" | "ndv" | "hll_ndv"
    ) {
        return Err(format!(
            "aggregate MV analyzed aggregate mismatch for `{output_name}`: expected APPROX_COUNT_DISTINCT, got {analyzed_name}"
        ));
    }
    let input_type = args.first().map(|arg| &arg.data_type).ok_or_else(|| {
        "APPROX_COUNT_DISTINCT requires exactly one column expression".to_string()
    })?;
    if count_distinct_key_type_allowed(input_type) {
        return Ok(());
    }
    Err(format!(
        "APPROX_COUNT_DISTINCT state key type is unsupported for aggregate `{output_name}` input: {input_type:?}; project to a supported scalar key type"
    ))
}

fn count_distinct_key_type_allowed(input_type: &DataType) -> bool {
    matches!(
        input_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Date32
            | DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _)
            | DataType::Utf8
            | DataType::LargeUtf8
    )
}

/// Lightweight projection of the iceberg base table that
/// `validate_ivm_primary_key` needs. Built once at the top of `create_mv`
/// from the loaded iceberg table; passing this struct keeps validation
/// pure and easy to unit-test.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BaseColumnDescriptor {
    pub name: String,
    pub data_type: DataType,
    /// Uppercased SQL type as the analyzer/iceberg-schema mapper produced
    /// it (e.g. `BIGINT`, `STRING`, `DECIMAL(18,2)`, `ARRAY<STRING>`).
    pub sql_type: String,
    pub nullable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BaseTableDescriptor {
    pub format_version: i32,
    pub columns: Vec<BaseColumnDescriptor>,
}

/// Validate that a parsed `PRIMARY KEY (col, ...)` clause on a CREATE
/// MATERIALIZED VIEW statement satisfies the IVM Phase-2 contract:
///
/// 1. The base table is iceberg format-version 2.
/// 2. Every PK column exists on the base table.
/// 3. Every PK column is NOT NULL on the base table.
/// 4. Every PK column has a hashable scalar type.
///
/// Errors fail fast in declared column order — the first mismatch wins.
/// Returns `Ok(())` on success and discards the PK list (PR-1 does not
/// persist it; PR-3 will).
pub(crate) fn validate_ivm_primary_key(
    pk_columns: &[String],
    base: &BaseTableDescriptor,
) -> Result<(), crate::connector::iceberg::changes::ChangeError> {
    use crate::connector::iceberg::changes::ChangeError;

    if base.format_version != 2 && base.format_version != 3 {
        return Err(ChangeError::IcebergFormatUnsupported {
            format_version: base.format_version,
        });
    }
    for pk in pk_columns {
        let col = base
            .columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(pk))
            .ok_or_else(|| ChangeError::PrimaryKeyMissingFromBase { pk_col: pk.clone() })?;
        if col.nullable {
            return Err(ChangeError::PrimaryKeyNullable {
                pk_col: col.name.clone(),
            });
        }
        if !is_hashable_pk_type(&col.sql_type) {
            return Err(ChangeError::PrimaryKeyTypeUnsupported {
                pk_col: col.name.clone(),
                ty: col.sql_type.clone(),
            });
        }
    }
    Ok(())
}

/// Hashable scalar-type predicate for IVM Phase-2 PRIMARY KEY columns.
/// Accepts: BIGINT, INT, SMALLINT, TINYINT, STRING, VARCHAR, DATE,
/// DATETIME, DECIMAL (with or without precision/scale).
/// Rejects: BOOLEAN, FLOAT, DOUBLE, ARRAY, MAP, STRUCT, JSON.
fn is_hashable_pk_type(sql_type: &str) -> bool {
    let upper = sql_type.to_ascii_uppercase();
    let head = upper.split(['(', '<']).next().unwrap_or("").trim();
    matches!(
        head,
        "BIGINT"
            | "INT"
            | "INTEGER"
            | "SMALLINT"
            | "TINYINT"
            | "STRING"
            | "VARCHAR"
            | "CHAR"
            | "DATE"
            | "DATETIME"
            | "TIMESTAMP"
            | "DECIMAL"
    )
}

/// Map an Arrow `DataType` to the SQL head token that
/// `is_hashable_pk_type` recognizes. Returns the token only — no
/// precision/scale or element-type tail. Anything not on the accepted
/// list falls through to the Arrow Debug form (e.g. `Float32`,
/// `List(...)`), which `is_hashable_pk_type` will then reject.
fn arrow_data_type_pk_head(dt: &arrow::datatypes::DataType) -> String {
    use arrow::datatypes::DataType;
    match dt {
        DataType::Int8 => "TINYINT".to_string(),
        DataType::Int16 => "SMALLINT".to_string(),
        DataType::Int32 => "INT".to_string(),
        DataType::Int64 => "BIGINT".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 => "STRING".to_string(),
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => "DECIMAL".to_string(),
        DataType::Date32 | DataType::Date64 => "DATE".to_string(),
        DataType::Timestamp(_, _) => "DATETIME".to_string(),
        // Explicitly unsupported as PK: floats (NaN equality), booleans
        // (degenerate cardinality), composites (no stable hash). Fall
        // through to Debug form so is_hashable_pk_type rejects them.
        other => format!("{other:?}"),
    }
}

/// Build the `BaseTableDescriptor` projection from an already-loaded
/// iceberg table. Used by `create_mv` and `create_iceberg_mv` before
/// invoking `validate_ivm_primary_key`.
pub(crate) fn descriptor_from_loaded(
    loaded: &crate::connector::iceberg::catalog::IcebergLoadedTable,
) -> BaseTableDescriptor {
    let format_version = loaded.table.metadata().format_version() as i32;
    let columns = loaded
        .columns
        .iter()
        .map(|col| BaseColumnDescriptor {
            name: col.name.clone(),
            data_type: col.data_type.clone(),
            sql_type: arrow_data_type_pk_head(&col.data_type),
            nullable: col.nullable,
        })
        .collect();
    BaseTableDescriptor {
        format_version,
        columns,
    }
}

pub(crate) fn drop_mv(
    state: &Arc<StandaloneState>,
    _current_catalog: Option<&str>,
    current_database: &str,
    stmt: &DropMaterializedViewStmt,
) -> Result<StatementResult, String> {
    let (db_name, mv_name) = resolve_mv_name(&stmt.name, current_database)?;
    let runtime = {
        let starrocks = state
            .starrocks_table
            .read()
            .expect("standalone StarRocks table read lock");
        starrocks.table(&db_name, &mv_name).ok().cloned()
    };
    let Some(runtime) = runtime else {
        if stmt.if_exists {
            return Ok(StatementResult::Ok);
        }
        if state
            .catalog_service
            .local()
            .read()
            .expect("standalone catalog read lock")
            .get(&db_name, &mv_name)
            .is_ok()
        {
            return Err(format!(
                "`{db_name}.{mv_name}` is not a materialized view; use DROP TABLE instead"
            ));
        }
        return Err(format!(
            "materialized view does not exist: {db_name}.{mv_name}"
        ));
    };
    if runtime.table.kind != StarRocksTableKind::MaterializedView {
        return Err(format!(
            "`{db_name}.{mv_name}` is not a materialized view; use DROP TABLE instead"
        ));
    }

    crate::engine::mv::dependency::ensure_no_downstream_dependencies(
        state,
        &crate::mv::dependency::model::starrocks_mv_dependency_ref(&db_name, &mv_name),
    )?;

    crate::connector::starrocks::table::ddl::drop_starrocks_table_with_metadata(
        state,
        &db_name,
        &mv_name,
        |txn, table_id| {
            state
                .mv_repo
                .drop_by_id(txn, table_id)
                .map_err(|e| format!("delete materialized view definition failed: {e}"))?;
            Ok(())
        },
    )?;
    Ok(StatementResult::Ok)
}

pub(crate) fn iceberg_table_ref_fqns(base_refs: &[TableIdentity]) -> Vec<String> {
    base_refs.iter().map(TableIdentity::fqn).collect()
}

pub(crate) fn list_mv_rows(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    stmt: &ShowMaterializedViewsStmt,
    storage_filter: Option<MvStorageEngine>,
) -> Result<Vec<MvListRow>, String> {
    let Some(provider) = state.metadata_provider.as_ref() else {
        return Ok(vec![]);
    };
    // Share a single read transaction across `list_definitions` and every
    // per-row `dependency_display_for_mv` lookup. This avoids M+1 RAII
    // open/close cycles for M materialized views and, more importantly,
    // gives the entire SHOW MATERIALIZED VIEWS result a consistent
    // metadata snapshot: concurrent CREATE/DROP MV writers cannot make
    // dependency display drift away from the MV list we just read.
    let read = provider
        .begin_read()
        .map_err(|e| format!("open metadata read transaction failed: {e}"))?;
    let definitions = state
        .mv_repo
        .list_definitions(read.as_ref())
        .map_err(|e| format!("load materialized view definitions failed: {e}"))?;
    let snapshot = state
        .starrocks_table
        .read()
        .expect("standalone StarRocks table read lock")
        .snapshot
        .clone();
    let now_ms = now_ms();

    let mut rows = Vec::new();
    for mv in &definitions {
        if let Some(filter) = storage_filter
            && !mv.storage_engine.eq_ignore_ascii_case(filter.as_sql_str())
        {
            continue;
        }
        let engine = MvStorageEngine::from_sql_str(&mv.storage_engine)?;
        let (refresh_state, retry_after_time) =
            refresh_status_for_mv(state, read.as_ref(), mv, now_ms)?;
        if engine == MvStorageEngine::Iceberg {
            let Some(target_catalog) = mv.target_catalog.as_deref() else {
                continue;
            };
            if let Some(current_catalog) = current_catalog
                && !target_catalog.eq_ignore_ascii_case(current_catalog)
            {
                continue;
            }
            let Some(target_namespace) = mv.target_namespace.clone() else {
                continue;
            };
            if let Some(filter_db) = stmt.database.as_deref()
                && !target_namespace.eq_ignore_ascii_case(filter_db)
            {
                continue;
            }
            let Some(target_table) = mv.target_table.clone() else {
                continue;
            };
            rows.push(MvListRow {
                name: target_table,
                database: target_namespace,
                storage_engine: mv.storage_engine.clone(),
                refresh_mode: mv.refresh_policy.as_sql_str().to_string(),
                last_refresh_time: mv.last_refresh_ms.map(|value| value.to_string()),
                last_refresh_rows: mv.last_refresh_rows.map(|value| value.to_string()),
                base_tables: mv.base_table_refs.join(", "),
                select_text: mv.select_sql.clone(),
                dependencies: dependency_display_for_mv(state, read.as_ref(), mv.mv_id)?,
                refresh_paused: mv.refresh_paused.to_string(),
                next_refresh_time: mv.next_refresh_after_ms.map(|value| value.to_string()),
                last_scheduler_error: mv.last_scheduler_error.clone(),
                max_staleness_ms: mv.max_staleness_ms.map(|value| value.to_string()),
                refresh_state,
                retry_after_time,
            });
            continue;
        }
        let Some(table) = snapshot.tables.iter().find(|table| {
            table.table_id == mv.mv_id && table.kind == StarRocksTableKind::MaterializedView
        }) else {
            continue;
        };
        if table.state != StarRocksTableState::Active {
            continue;
        }
        let Some(database) = snapshot
            .databases
            .iter()
            .find(|database| database.db_id == table.db_id)
            .map(|database| database.name.clone())
        else {
            continue;
        };
        if let Some(filter_db) = stmt.database.as_deref()
            && !database.eq_ignore_ascii_case(filter_db)
        {
            continue;
        }
        rows.push(MvListRow {
            name: table.name.clone(),
            database,
            storage_engine: mv.storage_engine.clone(),
            refresh_mode: mv.refresh_policy.as_sql_str().to_string(),
            last_refresh_time: mv.last_refresh_ms.map(|value| value.to_string()),
            last_refresh_rows: mv.last_refresh_rows.map(|value| value.to_string()),
            base_tables: mv.base_table_refs.join(", "),
            select_text: mv.select_sql.clone(),
            dependencies: dependency_display_for_mv(state, read.as_ref(), mv.mv_id)?,
            refresh_paused: mv.refresh_paused.to_string(),
            next_refresh_time: mv.next_refresh_after_ms.map(|value| value.to_string()),
            last_scheduler_error: mv.last_scheduler_error.clone(),
            max_staleness_ms: mv.max_staleness_ms.map(|value| value.to_string()),
            refresh_state,
            retry_after_time,
        });
    }
    Ok(rows)
}

fn refresh_status_for_mv(
    state: &Arc<StandaloneState>,
    read: &dyn MetaReadTxn,
    mv: &StoredMvDefinition,
    now_ms: i64,
) -> Result<(String, Option<String>), String> {
    let retry_after_time = mv
        .last_scheduler_error
        .as_ref()
        .and_then(|_| mv.next_refresh_after_ms)
        .filter(|next| *next > now_ms)
        .map(|value| value.to_string());
    if mv.refresh_paused {
        return Ok(("PAUSED".to_string(), retry_after_time));
    }
    if let Some(refresh_id) = mv.active_refresh_id {
        let refresh = state
            .mv_repo
            .load_refresh(read, refresh_id)
            .map_err(|e| format!("load active MV refresh failed: {e}"))?;
        if refresh
            .as_ref()
            .map(|refresh| refresh.state == MvRefreshState::CommitUnknown)
            .unwrap_or(false)
        {
            return Ok(("BLOCKED_RECOVERY".to_string(), retry_after_time));
        }
        return Ok(("RUNNING".to_string(), retry_after_time));
    }
    if mv.refresh_in_progress {
        return Ok(("RUNNING".to_string(), retry_after_time));
    }
    if mv
        .last_scheduler_error
        .as_ref()
        .map(|err| err.trim_start().starts_with("USER_ERROR: "))
        .unwrap_or(false)
    {
        return Ok(("FAILED_USER_ERROR".to_string(), retry_after_time));
    }
    if mv.last_scheduler_error.is_some()
        && mv
            .next_refresh_after_ms
            .map(|next| next > now_ms)
            .unwrap_or(false)
    {
        return Ok(("FAILED_BACKOFF".to_string(), retry_after_time));
    }
    if matches!(mv.refresh_policy, StoredMvRefreshPolicy::Manual) {
        return Ok(("MANUAL".to_string(), retry_after_time));
    }
    if mv
        .next_refresh_after_ms
        .map(|next| next > now_ms)
        .unwrap_or(false)
    {
        Ok(("SUCCEEDED".to_string(), retry_after_time))
    } else {
        Ok(("PENDING".to_string(), retry_after_time))
    }
}

/// Render the dependency-column text for a single MV row. Callers must pass
/// the shared read transaction opened by `list_mv_rows` so that every row
/// observes the same metadata snapshot and we avoid M+1 transaction opens.
fn dependency_display_for_mv(
    state: &Arc<StandaloneState>,
    read: &dyn MetaReadTxn,
    mv_id: i64,
) -> Result<String, String> {
    let dependencies = state
        .mv_repo
        .list_dependencies_by_downstream(read, mv_id)
        .map_err(|e| format!("load MV dependencies for display failed: {e}"))?;
    Ok(dependencies
        .iter()
        .map(|dep| dep.upstream.display_name())
        .collect::<Vec<_>>()
        .join(", "))
}

pub(crate) fn analyze_mv_select(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &sqlparser::ast::Query,
) -> Result<MvAnalysis, String> {
    analyze_mv_select_with(
        query,
        current_catalog,
        current_database,
        |resolved_refs| register_iceberg_tables_for_mv_analysis(state, resolved_refs),
        |query_for_analysis| {
            let catalog = state
                .catalog_service
                .local()
                .read()
                .expect("standalone catalog read lock");
            let (resolved, _, _factory) =
                crate::sql::analyzer::analyze(query_for_analysis, &*catalog, current_database)?;
            drop(catalog);
            Ok(resolved)
        },
    )
}

fn register_iceberg_tables_for_mv_analysis(
    state: &Arc<StandaloneState>,
    resolved_refs: &[ResolvedTableRef],
) -> Result<(), String> {
    let (catalog_backend, table_source) = {
        let registry = state
            .connectors
            .read()
            .expect("standalone connector registry read lock");
        (
            registry.catalog_backend("iceberg")?,
            registry.table_source("iceberg")?,
        )
    };

    for table_ref in resolved_refs {
        let ResolvedTableRef::Iceberg {
            catalog,
            namespace,
            table,
        } = table_ref
        else {
            continue;
        };
        drop_local_table_registration_if_exists(state, namespace, table)?;
        let resolved = catalog_backend
            .load_table_for_read(catalog, namespace, table)
            .map_err(|err| {
                format!("load iceberg table {catalog}.{namespace}.{table} failed: {err}")
            })?;
        let mut table_def = table_source.build_table_def(&resolved)?;
        table_def.name = table.clone();
        let mut local_catalog = state
            .catalog_service
            .local()
            .write()
            .map_err(|e| format!("standalone catalog write lock: {e}"))?;
        local_catalog.create_database(namespace)?;
        local_catalog.register(namespace, table_def)?;
    }
    Ok(())
}

pub(crate) fn build_mv_rows_result(rows: &[MvListRow]) -> Result<QueryResult, String> {
    let columns = vec![
        QueryResultColumn {
            name: "Name".to_string(),
            data_type: DataType::Utf8,
            nullable: false,
            logical_type: None,
        },
        QueryResultColumn {
            name: "Database".to_string(),
            data_type: DataType::Utf8,
            nullable: false,
            logical_type: None,
        },
        QueryResultColumn {
            name: "StorageEngine".to_string(),
            data_type: DataType::Utf8,
            nullable: false,
            logical_type: None,
        },
        QueryResultColumn {
            name: "RefreshMode".to_string(),
            data_type: DataType::Utf8,
            nullable: false,
            logical_type: None,
        },
        QueryResultColumn {
            name: "LastRefreshTime".to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            logical_type: None,
        },
        QueryResultColumn {
            name: "LastRefreshRows".to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            logical_type: None,
        },
        QueryResultColumn {
            name: "BaseTables".to_string(),
            data_type: DataType::Utf8,
            nullable: false,
            logical_type: None,
        },
        QueryResultColumn {
            name: "SelectText".to_string(),
            data_type: DataType::Utf8,
            nullable: false,
            logical_type: None,
        },
        QueryResultColumn {
            name: "Dependencies".to_string(),
            data_type: DataType::Utf8,
            nullable: false,
            logical_type: None,
        },
        QueryResultColumn {
            name: "RefreshPaused".to_string(),
            data_type: DataType::Utf8,
            nullable: false,
            logical_type: None,
        },
        QueryResultColumn {
            name: "NextRefreshTime".to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            logical_type: None,
        },
        QueryResultColumn {
            name: "LastSchedulerError".to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            logical_type: None,
        },
        QueryResultColumn {
            name: "MaxStalenessMs".to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            logical_type: None,
        },
        QueryResultColumn {
            name: "RefreshState".to_string(),
            data_type: DataType::Utf8,
            nullable: false,
            logical_type: None,
        },
        QueryResultColumn {
            name: "RetryAfterTime".to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            logical_type: None,
        },
    ];

    let schema = Arc::new(Schema::new(vec![
        Field::new("Name", DataType::Utf8, false),
        Field::new("Database", DataType::Utf8, false),
        Field::new("StorageEngine", DataType::Utf8, false),
        Field::new("RefreshMode", DataType::Utf8, false),
        Field::new("LastRefreshTime", DataType::Utf8, true),
        Field::new("LastRefreshRows", DataType::Utf8, true),
        Field::new("BaseTables", DataType::Utf8, false),
        Field::new("SelectText", DataType::Utf8, false),
        Field::new("Dependencies", DataType::Utf8, false),
        Field::new("RefreshPaused", DataType::Utf8, false),
        Field::new("NextRefreshTime", DataType::Utf8, true),
        Field::new("LastSchedulerError", DataType::Utf8, true),
        Field::new("MaxStalenessMs", DataType::Utf8, true),
        Field::new("RefreshState", DataType::Utf8, false),
        Field::new("RetryAfterTime", DataType::Utf8, true),
    ]));
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| Some(row.name.clone()))
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| Some(row.database.clone()))
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| Some(row.storage_engine.clone()))
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| Some(row.refresh_mode.clone()))
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.last_refresh_time.clone())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.last_refresh_rows.clone())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| Some(row.base_tables.clone()))
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| Some(row.select_text.clone()))
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| Some(row.dependencies.clone()))
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| Some(row.refresh_paused.clone()))
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.next_refresh_time.clone())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.last_scheduler_error.clone())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.max_staleness_ms.clone())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| Some(row.refresh_state.clone()))
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.retry_after_time.clone())
                .collect::<Vec<_>>(),
        )),
    ];
    let batch = RecordBatch::try_new(schema, arrays)
        .map_err(|e| format!("build SHOW MATERIALIZED VIEWS batch failed: {e}"))?;
    Ok(QueryResult {
        columns,
        chunks: vec![record_batch_to_chunk(batch)?],
    })
}

pub(crate) fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn cleanup_bootstrapped_tablets(tablet_ids: &[i64]) {
    if tablet_ids.is_empty() {
        return;
    }
    if let Err(err) = delete_tablet(&DeleteTabletRequest {
        tablet_ids: tablet_ids.to_vec(),
    }) {
        tracing::warn!(
            "StarRocks materialized view create cleanup failed to delete bootstrapped tablets: tablet_ids={:?} error={}",
            tablet_ids,
            err
        );
        for tablet_id in tablet_ids {
            let _ = remove_tablet_runtime(*tablet_id);
        }
    }
}
