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
use std::time::Duration;

use chrono::NaiveDateTime;
use reqwest::blocking::Client;
use serde_json::Value as JsonValue;
use thrift::OrderedFloat;

use crate::frontend_service;
use crate::types;

use super::chunk_builder::{SchemaRow, SchemaValue, normalize_column_key};
use super::frontend::{
    build_auth_info, effective_current_user_ident, ensure_ok_status, extract_db_name,
    forward_show_result, with_frontend_client,
};
use super::{SchemaScanContext, SchemaTable};

const DEFAULT_TABLE_CATALOG: &str = "def";

pub(crate) fn fetch_rows(
    table: &SchemaTable,
    ctx: &SchemaScanContext,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Vec<SchemaRow>, String> {
    match table {
        SchemaTable::AnalyzeStatus => fetch_analyze_status(ctx, fe_addr),
        SchemaTable::CharacterSets => fetch_character_sets(ctx, fe_addr),
        SchemaTable::Collations => fetch_collations(ctx, fe_addr),
        SchemaTable::ColumnPrivileges => Ok(Vec::new()),
        SchemaTable::ColumnStatsUsage => fetch_column_stats_usage(ctx, fe_addr),
        SchemaTable::Events => Ok(Vec::new()),
        SchemaTable::FeMetrics => fetch_fe_metrics(ctx, fe_addr),
        SchemaTable::Keywords => fetch_keywords(ctx, fe_addr),
        SchemaTable::ApplicableRoles => fetch_applicable_roles(ctx, fe_addr),
        SchemaTable::Engines => fetch_engines(ctx, fe_addr),
        SchemaTable::FeThreads => fetch_fe_threads(ctx, fe_addr),
        SchemaTable::FeTabletSchedules => fetch_fe_tablet_schedules(ctx, fe_addr),
        SchemaTable::GlobalVariables => fetch_global_variables(ctx, fe_addr),
        SchemaTable::KeyColumnUsage => Ok(Vec::new()),
        SchemaTable::Partitions => Ok(Vec::new()),
        SchemaTable::PartitionsMeta => fetch_partitions_meta(ctx, fe_addr),
        SchemaTable::PipeFiles => fetch_pipe_files(ctx, fe_addr),
        SchemaTable::Pipes => fetch_pipes(ctx, fe_addr),
        SchemaTable::ReferentialConstraints => Ok(Vec::new()),
        SchemaTable::RoutineLoadJobs => fetch_routine_load_jobs(ctx, fe_addr),
        SchemaTable::Routines => Ok(Vec::new()),
        SchemaTable::SessionVariables => fetch_session_variables(ctx, fe_addr),
        SchemaTable::Statistics => Ok(Vec::new()),
        SchemaTable::StreamLoads => fetch_stream_loads(ctx, fe_addr),
        SchemaTable::TableConstraints => Ok(Vec::new()),
        SchemaTable::Tables => fetch_tables(ctx, fe_addr),
        SchemaTable::TablesConfig => fetch_tables_config(ctx, fe_addr),
        SchemaTable::TablePrivileges => fetch_table_privileges(ctx, fe_addr),
        SchemaTable::Tasks => fetch_tasks(ctx, fe_addr),
        SchemaTable::TempTables => fetch_temp_tables(ctx, fe_addr),
        SchemaTable::Triggers => Ok(Vec::new()),
        SchemaTable::UserPrivileges => fetch_user_privileges(ctx, fe_addr),
        SchemaTable::VerboseSessionVariables => fetch_verbose_session_variables(ctx, fe_addr),
        SchemaTable::Views => fetch_views(ctx, fe_addr),
        SchemaTable::Columns => fetch_columns(ctx, fe_addr),
        SchemaTable::Schemata => fetch_schemata(ctx, fe_addr),
        SchemaTable::MaterializedViews => fetch_materialized_views(ctx, fe_addr),
        SchemaTable::TaskRuns => fetch_task_runs(ctx, fe_addr),
        SchemaTable::WarehouseMetrics => fetch_warehouse_metrics(ctx, fe_addr),
        SchemaTable::WarehouseQueries => fetch_warehouse_queries(ctx, fe_addr),
        SchemaTable::TabletReshardJobs => fetch_tablet_reshard_jobs(fe_addr),
        SchemaTable::ClusterSnapshots => fetch_cluster_snapshots(fe_addr),
        SchemaTable::ClusterSnapshotJobs => fetch_cluster_snapshot_jobs(fe_addr),
        SchemaTable::RecyclebinCatalogs => fetch_recyclebin_catalogs(ctx, fe_addr),
        SchemaTable::ObjectDependencies => fetch_object_dependencies(ctx, fe_addr),
        SchemaTable::FeLocks => fetch_fe_locks(ctx, fe_addr),
        SchemaTable::FeMemoryUsage => fetch_fe_memory_usage(ctx, fe_addr),
        SchemaTable::SchemaPrivileges => fetch_schema_privileges(ctx, fe_addr),
        SchemaTable::GrantsToRoles => fetch_grants_to_roles(ctx, fe_addr),
        SchemaTable::GrantsToUsers => fetch_grants_to_users(ctx, fe_addr),
        SchemaTable::RoleEdges => fetch_role_edges(fe_addr),
        _ => Err(format!(
            "unsupported FE schema table {}",
            table.table_name()
        )),
    }
}

fn fetch_show_rows(
    ctx: &SchemaScanContext,
    fe_addr: Option<&types::TNetworkAddress>,
    sql: &str,
) -> Result<Vec<Vec<String>>, String> {
    Ok(forward_show_result(ctx, fe_addr, sql)?.result_rows)
}

fn fetch_analyze_status(
    ctx: &SchemaScanContext,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Vec<SchemaRow>, String> {
    let request = frontend_service::TAnalyzeStatusReq::new(
        Some(build_auth_info(ctx)),
        ctx.catalog_name.clone(),
        ctx.db.clone(),
        ctx.table.clone(),
    );
    let response = with_frontend_client(fe_addr, |client| {
        client
            .get_analyze_status(request)
            .map_err(|err| err.to_string())
    })?;
    Ok(response
        .items
        .unwrap_or_default()
        .iter()
        .map(build_analyze_status_row)
        .collect())
}

fn fetch_character_sets(
    ctx: &SchemaScanContext,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Vec<SchemaRow>, String> {
    Ok(fetch_show_rows(ctx, fe_addr, "SHOW CHARSET")?
        .into_iter()
        .map(|row| build_character_set_row(&row))
        .collect())
}

fn fetch_collations(
    ctx: &SchemaScanContext,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Vec<SchemaRow>, String> {
    Ok(fetch_show_rows(ctx, fe_addr, "SHOW COLLATION")?
        .into_iter()
        .map(|row| build_collation_row(&row))
        .collect())
}

fn fetch_column_stats_usage(
    ctx: &SchemaScanContext,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Vec<SchemaRow>, String> {
    let request = frontend_service::TColumnStatsUsageReq::new(
        Some(build_auth_info(ctx)),
        ctx.catalog_name.clone(),
        ctx.db.clone(),
        ctx.table.clone(),
    );
    let response = with_frontend_client(fe_addr, |client| {
        client
            .get_column_stats_usage(request)
            .map_err(|err| err.to_string())
    })?;
    Ok(response
        .items
        .unwrap_or_default()
        .iter()
        .map(build_column_stats_usage_row)
        .collect())
}

fn fetch_engines(
    ctx: &SchemaScanContext,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Vec<SchemaRow>, String> {
    Ok(fetch_show_rows(ctx, fe_addr, "SHOW ENGINES")?
        .into_iter()
        .map(|row| build_engine_row(&row))
        .collect())
}

fn fetch_fe_tablet_schedules(
    ctx: &SchemaScanContext,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Vec<SchemaRow>, String> {
    let request = frontend_service::TGetTabletScheduleRequest::new(
        ctx.table_id,
        ctx.partition_id,
        ctx.tablet_id,
        ctx.type_.clone(),
        ctx.state.clone(),
        ctx.limit,
        effective_current_user_ident(ctx),
    );
    let response = with_frontend_client(fe_addr, |client| {
        client
            .get_tablet_schedule(request)
            .map_err(|err| err.to_string())
    })?;
    Ok(response
        .tablet_schedules
        .unwrap_or_default()
        .iter()
        .map(build_fe_tablet_schedule_row)
        .collect())
}

fn fetch_fe_metrics(
    ctx: &SchemaScanContext,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Vec<SchemaRow>, String> {
    let http_client = Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .map_err(|err| format!("build FE metrics http client failed: {err}"))?;
    let mut rows = Vec::new();
    let frontends = if ctx.frontends.is_empty() {
        fetch_show_rows(ctx, fe_addr, "SHOW FRONTENDS")?
            .into_iter()
            .map(|frontend| {
                let fe_id = frontend
                    .first()
                    .filter(|value| !value.trim().is_empty())
                    .cloned()
                    .ok_or_else(|| "SHOW FRONTENDS did not return FE id".to_string())?;
                let host = frontend
                    .get(2)
                    .filter(|value| !value.trim().is_empty())
                    .cloned()
                    .ok_or_else(|| format!("SHOW FRONTENDS missing host for FE id={fe_id}"))?;
                let http_port = frontend
                    .get(4)
                    .and_then(|value| value.trim().parse::<u16>().ok())
                    .ok_or_else(|| format!("SHOW FRONTENDS missing http port for FE id={fe_id}"))?;
                Ok((fe_id, host, http_port))
            })
            .collect::<Result<Vec<_>, String>>()?
    } else {
        ctx.frontends
            .iter()
            .map(|frontend| {
                let fe_id = frontend
                    .id
                    .as_deref()
                    .filter(|value| !value.trim().is_empty())
                    .map(str::to_string)
                    .ok_or_else(|| "schema scan frontend list missing FE id".to_string())?;
                let host = frontend
                    .ip
                    .as_deref()
                    .filter(|value| !value.trim().is_empty())
                    .map(str::to_string)
                    .ok_or_else(|| {
                        format!("schema scan frontend list missing host for FE id={fe_id}")
                    })?;
                let http_port = frontend
                    .http_port
                    .and_then(|value| u16::try_from(value).ok())
                    .ok_or_else(|| {
                        format!("schema scan frontend list missing http port for FE id={fe_id}")
                    })?;
                Ok((fe_id, host, http_port))
            })
            .collect::<Result<Vec<_>, String>>()?
    };
    for (fe_id, host, http_port) in frontends {
        let metrics_url = format!("http://{host}:{http_port}/metrics?type=json");
        let metrics: JsonValue = http_client
            .get(&metrics_url)
            .send()
            .map_err(|err| format!("fetch FE metrics from {metrics_url} failed: {err}"))?
            .error_for_status()
            .map_err(|err| format!("fetch FE metrics from {metrics_url} failed: {err}"))?
            .json()
            .map_err(|err| format!("parse FE metrics from {metrics_url} failed: {err}"))?;
        let metrics = metrics.as_array().ok_or_else(|| {
            format!("FE metrics endpoint {metrics_url} did not return a JSON array")
        })?;
        for metric in metrics {
            let Some(tags) = metric.get("tags") else {
                continue;
            };
            let Some(name) = tags.get("metric").and_then(JsonValue::as_str) else {
                continue;
            };
            let Some(value) = metric.get("value").and_then(metric_value_as_i64) else {
                continue;
            };
            let mut row = SchemaRow::new();
            put_str(&mut row, "FE_ID", &fe_id);
            put_str(&mut row, "NAME", name);
            put_str(
                &mut row,
                "LABELS",
                &serde_json::to_string(tags)
                    .map_err(|err| format!("serialize FE metric labels failed: {err}"))?,
            );
            put_i64(&mut row, "VALUE", Some(value));
            rows.push(row);
        }
    }
    Ok(rows)
}

fn fetch_global_variables(
    ctx: &SchemaScanContext,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Vec<SchemaRow>, String> {
    Ok(fetch_show_rows(ctx, fe_addr, "SHOW GLOBAL VARIABLES")?
        .into_iter()
        .map(|row| build_variable_row(&row))
        .collect())
}

fn fetch_routine_load_jobs(
    ctx: &SchemaScanContext,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Vec<SchemaRow>, String> {
    let request = frontend_service::TGetLoadsParams::new(
        ctx.db.clone(),
        ctx.job_id,
        ctx.txn_id,
        ctx.label.clone(),
        ctx.type_.clone(),
    );
    let response = with_frontend_client(fe_addr, |client| {
        client
            .get_routine_load_jobs(request)
            .map_err(|err| err.to_string())
    })?;
    Ok(response
        .loads
        .unwrap_or_default()
        .iter()
        .map(build_routine_load_job_row)
        .collect())
}

fn fetch_session_variables(
    ctx: &SchemaScanContext,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Vec<SchemaRow>, String> {
    Ok(fetch_show_rows(ctx, fe_addr, "SHOW SESSION VARIABLES")?
        .into_iter()
        .map(|row| build_variable_row(&row))
        .collect())
}

fn fetch_stream_loads(
    ctx: &SchemaScanContext,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Vec<SchemaRow>, String> {
    let request = frontend_service::TGetLoadsParams::new(
        ctx.db.clone(),
        ctx.job_id,
        ctx.txn_id,
        ctx.label.clone(),
        ctx.type_.clone(),
    );
    let response = with_frontend_client(fe_addr, |client| {
        client
            .get_stream_loads(request)
            .map_err(|err| err.to_string())
    })?;
    Ok(response
        .loads
        .unwrap_or_default()
        .iter()
        .map(build_stream_load_row)
        .collect())
}

fn fetch_table_privileges(
    ctx: &SchemaScanContext,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Vec<SchemaRow>, String> {
    let request = frontend_service::TGetTablePrivsParams::new(effective_current_user_ident(ctx));
    let response = with_frontend_client(fe_addr, |client| {
        client
            .get_table_privs(request)
            .map_err(|err| err.to_string())
    })?;
    Ok(response
        .table_privs
        .unwrap_or_default()
        .iter()
        .map(build_table_privilege_row)
        .collect())
}

fn fetch_tasks(
    ctx: &SchemaScanContext,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Vec<SchemaRow>, String> {
    let request = frontend_service::TGetTasksParams::new(
        ctx.db.clone(),
        effective_current_user_ident(ctx),
        None::<String>,
        None::<String>,
        ctx.state.clone(),
        ctx.limit_as_usize().map(|limit| {
            frontend_service::TRequestPagination::new(None::<i64>, Some(limit as i64))
        }),
    );
    let response = with_frontend_client(fe_addr, |client| {
        client.get_tasks(request).map_err(|err| err.to_string())
    })?;
    Ok(response.tasks.iter().map(build_task_row).collect())
}

fn fetch_temp_tables(
    ctx: &SchemaScanContext,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Vec<SchemaRow>, String> {
    let request = frontend_service::TGetTemporaryTablesInfoRequest::new(
        Some(build_auth_info(ctx)),
        ctx.limit,
    );
    let response = with_frontend_client(fe_addr, |client| {
        client
            .get_temporary_tables_info(request)
            .map_err(|err| err.to_string())
    })?;
    Ok(response
        .tables_infos
        .unwrap_or_default()
        .iter()
        .map(build_temp_table_row)
        .collect())
}

fn fetch_user_privileges(
    ctx: &SchemaScanContext,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Vec<SchemaRow>, String> {
    let request = frontend_service::TGetUserPrivsParams::new(effective_current_user_ident(ctx));
    let response = with_frontend_client(fe_addr, |client| {
        client
            .get_user_privs(request)
            .map_err(|err| err.to_string())
    })?;
    Ok(response
        .user_privs
        .unwrap_or_default()
        .iter()
        .map(build_user_privilege_row)
        .collect())
}

fn fetch_verbose_session_variables(
    ctx: &SchemaScanContext,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Vec<SchemaRow>, String> {
    Ok(fetch_show_rows(ctx, fe_addr, "SHOW VERBOSE VARIABLES")?
        .into_iter()
        .map(|row| build_verbose_variable_row(&row))
        .collect())
}

fn fetch_schema_privileges(
    ctx: &SchemaScanContext,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Vec<SchemaRow>, String> {
    let request = frontend_service::TGetDBPrivsParams::new(effective_current_user_ident(ctx));
    let response = with_frontend_client(fe_addr, |client| {
        client.get_d_b_privs(request).map_err(|err| err.to_string())
    })?;
    Ok(response
        .db_privs
        .unwrap_or_default()
        .iter()
        .map(build_schema_privilege_row)
        .collect())
}

fn fetch_grants_to_roles(
    ctx: &SchemaScanContext,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Vec<SchemaRow>, String> {
    fetch_grants_to(frontend_service::TGrantsToType::ROLE, ctx, fe_addr)
}

fn fetch_grants_to_users(
    ctx: &SchemaScanContext,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Vec<SchemaRow>, String> {
    fetch_grants_to(frontend_service::TGrantsToType::USER, ctx, fe_addr)
}

fn fetch_grants_to(
    grants_type: frontend_service::TGrantsToType,
    _ctx: &SchemaScanContext,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Vec<SchemaRow>, String> {
    let request = frontend_service::TGetGrantsToRolesOrUserRequest::new(Some(grants_type));
    let response = with_frontend_client(fe_addr, |client| {
        client.get_grants_to(request).map_err(|err| err.to_string())
    })?;
    Ok(response
        .grants_to
        .unwrap_or_default()
        .iter()
        .map(build_grants_to_row)
        .collect())
}

fn fetch_role_edges(fe_addr: Option<&types::TNetworkAddress>) -> Result<Vec<SchemaRow>, String> {
    let response = with_frontend_client(fe_addr, |client| {
        client
            .get_role_edges(frontend_service::TGetRoleEdgesRequest::new())
            .map_err(|err| err.to_string())
    })?;
    Ok(response
        .role_edges
        .unwrap_or_default()
        .iter()
        .map(build_role_edge_row)
        .collect())
}

fn fetch_keywords(
    ctx: &SchemaScanContext,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Vec<SchemaRow>, String> {
    let auth_info = build_auth_info(ctx);
    let mut rows = Vec::new();
    let mut offset = Some(0_i64);
    while let Some(start) = offset.take() {
        let request =
            frontend_service::TGetKeywordsRequest::new(Some(auth_info.clone()), Some(start));
        let response = with_frontend_client(fe_addr, |client| {
            client.get_keywords(request).map_err(|err| err.to_string())
        })?;
        for keyword in response.keywords.unwrap_or_default() {
            let mut row = SchemaRow::new();
            put_str(
                &mut row,
                "WORD",
                keyword.keyword.as_deref().unwrap_or_default(),
            );
            put_bool(&mut row, "RESERVED", keyword.reserved);
            rows.push(row);
        }
        offset = response.next_table_id_offset.filter(|value| *value > 0);
    }
    Ok(rows)
}

fn fetch_applicable_roles(
    ctx: &SchemaScanContext,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Vec<SchemaRow>, String> {
    let auth_info = build_auth_info(ctx);
    let mut rows = Vec::new();
    let mut offset = Some(0_i64);
    while let Some(start) = offset.take() {
        let request =
            frontend_service::TGetApplicableRolesRequest::new(Some(auth_info.clone()), Some(start));
        let response = with_frontend_client(fe_addr, |client| {
            client
                .get_applicable_roles(request)
                .map_err(|err| err.to_string())
        })?;
        for role in response.roles.unwrap_or_default() {
            let mut row = SchemaRow::new();
            put_opt_str(&mut row, "USER", role.user.as_deref());
            put_opt_str(&mut row, "HOST", role.host.as_deref());
            put_opt_str(&mut row, "GRANTEE", role.grantee.as_deref());
            put_opt_str(&mut row, "GRANTEE_HOST", role.grantee_host.as_deref());
            put_opt_str(&mut row, "ROLE_NAME", role.role_name.as_deref());
            put_opt_str(&mut row, "ROLE_HOST", role.role_host.as_deref());
            put_opt_str(&mut row, "IS_GRANTABLE", role.is_grantable.as_deref());
            put_opt_str(&mut row, "IS_DEFAULT", role.is_default.as_deref());
            put_opt_str(&mut row, "IS_MANDATORY", role.is_mandatory.as_deref());
            rows.push(row);
        }
        offset = response.next_table_id_offset.filter(|value| *value > 0);
    }
    Ok(rows)
}

fn fetch_fe_threads(
    ctx: &SchemaScanContext,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Vec<SchemaRow>, String> {
    let request = frontend_service::TGetFeThreadsRequest::new(Some(build_auth_info(ctx)));
    let response = with_frontend_client(fe_addr, |client| {
        client
            .get_fe_threads(request)
            .map_err(|err| err.to_string())
    })?;
    ensure_ok_status(response.status.as_ref(), "get FE threads")?;
    Ok(response
        .threads
        .unwrap_or_default()
        .into_iter()
        .map(|thread| {
            let mut row = SchemaRow::new();
            put_opt_str(&mut row, "FE_ADDRESS", thread.fe_address.as_deref());
            put_i64(&mut row, "THREAD_ID", thread.thread_id);
            put_opt_str(&mut row, "THREAD_NAME", thread.thread_name.as_deref());
            put_opt_str(&mut row, "THREAD_STATE", thread.thread_state.as_deref());
            put_bool(&mut row, "IS_DAEMON", thread.is_daemon);
            put_i32(&mut row, "PRIORITY", thread.priority);
            put_i64(&mut row, "CPU_TIME_MS", thread.cpu_time_ms);
            put_i64(&mut row, "USER_TIME_MS", thread.user_time_ms);
            row
        })
        .collect())
}

fn fetch_partitions_meta(
    ctx: &SchemaScanContext,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Vec<SchemaRow>, String> {
    let auth_info = build_auth_info(ctx);
    let mut rows = Vec::new();
    let mut offset = Some(0_i64);
    while let Some(start) = offset.take() {
        let request =
            frontend_service::TGetPartitionsMetaRequest::new(Some(auth_info.clone()), Some(start));
        let response = with_frontend_client(fe_addr, |client| {
            client
                .get_partitions_meta(request)
                .map_err(|err| err.to_string())
        })?;
        for partition in response.partitions_meta_infos.unwrap_or_default() {
            rows.push(build_partition_meta_row(&partition));
        }
        offset = response.next_table_id_offset.filter(|value| *value > 0);
    }
    Ok(rows)
}

fn fetch_pipes(
    ctx: &SchemaScanContext,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Vec<SchemaRow>, String> {
    let request = frontend_service::TListPipesParams::new(effective_current_user_ident(ctx));
    let response = with_frontend_client(fe_addr, |client| {
        client.list_pipes(request).map_err(|err| err.to_string())
    })?;
    Ok(response
        .pipes
        .unwrap_or_default()
        .iter()
        .map(build_pipe_row)
        .collect())
}

fn fetch_pipe_files(
    ctx: &SchemaScanContext,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Vec<SchemaRow>, String> {
    let request = frontend_service::TListPipeFilesParams::new(effective_current_user_ident(ctx));
    let response = with_frontend_client(fe_addr, |client| {
        client
            .list_pipe_files(request)
            .map_err(|err| err.to_string())
    })?;
    Ok(response
        .pipe_files
        .unwrap_or_default()
        .iter()
        .map(build_pipe_file_row)
        .collect())
}

fn fetch_tables(
    ctx: &SchemaScanContext,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Vec<SchemaRow>, String> {
    let request =
        frontend_service::TGetTablesInfoRequest::new(Some(build_auth_info(ctx)), None::<String>);
    let response = with_frontend_client(fe_addr, |client| {
        client
            .get_tables_info(request)
            .map_err(|err| err.to_string())
    })?;
    Ok(response
        .tables_infos
        .unwrap_or_default()
        .iter()
        .map(build_table_row)
        .collect())
}

fn fetch_tables_config(
    ctx: &SchemaScanContext,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Vec<SchemaRow>, String> {
    let request = frontend_service::TGetTablesConfigRequest::new(Some(build_auth_info(ctx)));
    let response = with_frontend_client(fe_addr, |client| {
        client
            .get_tables_config(request)
            .map_err(|err| err.to_string())
    })?;
    Ok(response
        .tables_config_infos
        .unwrap_or_default()
        .iter()
        .map(build_tables_config_row)
        .collect())
}

fn fetch_views(
    ctx: &SchemaScanContext,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Vec<SchemaRow>, String> {
    let request =
        frontend_service::TGetTablesInfoRequest::new(Some(build_auth_info(ctx)), None::<String>);
    let response = with_frontend_client(fe_addr, |client| {
        client
            .get_tables_info(request)
            .map_err(|err| err.to_string())
    })?;
    Ok(response
        .tables_infos
        .unwrap_or_default()
        .iter()
        .filter(|table| table.table_type.as_deref() == Some("VIEW"))
        .map(|table| {
            let mut row = SchemaRow::new();
            put_str(
                &mut row,
                "TABLE_CATALOG",
                table
                    .table_catalog
                    .as_deref()
                    .unwrap_or(DEFAULT_TABLE_CATALOG),
            );
            put_opt_str(&mut row, "TABLE_SCHEMA", table.table_schema.as_deref());
            put_opt_str(&mut row, "TABLE_NAME", table.table_name.as_deref());
            row
        })
        .collect())
}

fn fetch_schemata(
    ctx: &SchemaScanContext,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Vec<SchemaRow>, String> {
    let dbs = fetch_db_names(ctx, fe_addr)?;
    Ok(dbs
        .iter()
        .map(|db| {
            let mut row = SchemaRow::new();
            put_str(
                &mut row,
                "CATALOG_NAME",
                ctx.catalog_name.as_deref().unwrap_or(DEFAULT_TABLE_CATALOG),
            );
            put_str(&mut row, "SCHEMA_NAME", db);
            put_str(&mut row, "DEFAULT_CHARACTER_SET_NAME", "utf8");
            put_str(&mut row, "DEFAULT_COLLATION_NAME", "utf8_general_ci");
            put_str(&mut row, "SQL_PATH", "");
            row
        })
        .collect())
}

fn fetch_columns(
    ctx: &SchemaScanContext,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Vec<SchemaRow>, String> {
    let mut rows = Vec::new();
    for db_name in fetch_db_names(ctx, fe_addr)? {
        let table_names = fetch_table_names(ctx, fe_addr, &db_name)?;
        for table_name in table_names {
            let request = frontend_service::TDescribeTableParams::new(
                Some(db_name.clone()),
                table_name.clone(),
                ctx.user.clone(),
                ctx.user_ip.clone().or_else(|| ctx.ip.clone()),
                effective_current_user_ident(ctx),
                None::<i64>,
                ctx.catalog_name.clone(),
            );
            let response = with_frontend_client(fe_addr, |client| {
                client
                    .describe_table(request)
                    .map_err(|err| err.to_string())
            })?;
            for (ordinal, column) in response.columns.iter().enumerate() {
                rows.push(build_column_row(
                    ctx.catalog_name.as_deref().unwrap_or(DEFAULT_TABLE_CATALOG),
                    &db_name,
                    &table_name,
                    ordinal + 1,
                    column,
                ));
            }
        }
    }
    Ok(rows)
}

fn fetch_materialized_views(
    ctx: &SchemaScanContext,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Vec<SchemaRow>, String> {
    let mut rows = Vec::new();
    for db_name in fetch_db_names(ctx, fe_addr)? {
        let request = frontend_service::TGetTablesParams::new(
            Some(db_name.clone()),
            None::<String>,
            ctx.user.clone(),
            ctx.user_ip.clone().or_else(|| ctx.ip.clone()),
            effective_current_user_ident(ctx),
            Some(types::TTableType::MATERIALIZED_VIEW),
            None::<i64>,
            ctx.catalog_name.clone(),
            None::<String>,
        );
        let response = with_frontend_client(fe_addr, |client| {
            client
                .list_materialized_view_status(request)
                .map_err(|err| err.to_string())
        })?;
        for mv in response.materialized_views.unwrap_or_default() {
            rows.push(build_materialized_view_row(&mv));
        }
    }
    Ok(rows)
}

fn fetch_task_runs(
    ctx: &SchemaScanContext,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Vec<SchemaRow>, String> {
    let request = frontend_service::TGetTasksParams::new(
        ctx.db.clone(),
        effective_current_user_ident(ctx),
        None::<String>,
        None::<String>,
        None::<String>,
        ctx.limit_as_usize().map(|limit| {
            frontend_service::TRequestPagination::new(None::<i64>, Some(limit as i64))
        }),
    );
    let response = with_frontend_client(fe_addr, |client| {
        client.get_task_runs(request).map_err(|err| err.to_string())
    })?;
    Ok(response
        .task_runs
        .unwrap_or_default()
        .iter()
        .map(build_task_run_row)
        .collect())
}

fn fetch_warehouse_metrics(
    ctx: &SchemaScanContext,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Vec<SchemaRow>, String> {
    let request = frontend_service::TGetWarehouseMetricsRequest::new(Some(build_auth_info(ctx)));
    let response = with_frontend_client(fe_addr, |client| {
        client
            .get_warehouse_metrics(request)
            .map_err(|err| err.to_string())
    })?;
    Ok(response
        .metrics
        .unwrap_or_default()
        .iter()
        .map(build_warehouse_metrics_row)
        .collect())
}

fn fetch_warehouse_queries(
    ctx: &SchemaScanContext,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Vec<SchemaRow>, String> {
    let request = frontend_service::TGetWarehouseQueriesRequest::new(Some(build_auth_info(ctx)));
    let response = with_frontend_client(fe_addr, |client| {
        client
            .get_warehouse_queries(request)
            .map_err(|err| err.to_string())
    })?;
    Ok(response
        .queries
        .unwrap_or_default()
        .iter()
        .map(build_warehouse_queries_row)
        .collect())
}

fn fetch_tablet_reshard_jobs(
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Vec<SchemaRow>, String> {
    let request = frontend_service::TTabletReshardJobsRequest::new();
    let response = with_frontend_client(fe_addr, |client| {
        client
            .get_tablet_reshard_jobs_info(request)
            .map_err(|err| err.to_string())
    })?;
    ensure_ok_status(response.status.as_ref(), "get tablet reshard jobs")?;
    Ok(response
        .items
        .unwrap_or_default()
        .iter()
        .map(build_tablet_reshard_job_row)
        .collect())
}

fn fetch_cluster_snapshots(
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Vec<SchemaRow>, String> {
    let request = frontend_service::TClusterSnapshotsRequest::new();
    let response = with_frontend_client(fe_addr, |client| {
        client
            .get_cluster_snapshots_info(request)
            .map_err(|err| err.to_string())
    })?;
    Ok(response
        .items
        .unwrap_or_default()
        .iter()
        .map(build_cluster_snapshot_row)
        .collect())
}

fn fetch_cluster_snapshot_jobs(
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Vec<SchemaRow>, String> {
    let request = frontend_service::TClusterSnapshotJobsRequest::new();
    let response = with_frontend_client(fe_addr, |client| {
        client
            .get_cluster_snapshot_jobs_info(request)
            .map_err(|err| err.to_string())
    })?;
    Ok(response
        .items
        .unwrap_or_default()
        .iter()
        .map(build_cluster_snapshot_job_row)
        .collect())
}

fn fetch_recyclebin_catalogs(
    ctx: &SchemaScanContext,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Vec<SchemaRow>, String> {
    let request =
        frontend_service::TListRecycleBinCatalogsParams::new(effective_current_user_ident(ctx));
    let response = with_frontend_client(fe_addr, |client| {
        client
            .list_recycle_bin_catalogs(request)
            .map_err(|err| err.to_string())
    })?;
    Ok(response
        .recyclebin_catalogs
        .unwrap_or_default()
        .iter()
        .map(build_recyclebin_catalog_row)
        .collect())
}

fn fetch_object_dependencies(
    ctx: &SchemaScanContext,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Vec<SchemaRow>, String> {
    let request = frontend_service::TObjectDependencyReq::new(Some(build_auth_info(ctx)));
    let response = with_frontend_client(fe_addr, |client| {
        client
            .list_object_dependencies(request)
            .map_err(|err| err.to_string())
    })?;
    Ok(response
        .items
        .unwrap_or_default()
        .iter()
        .map(build_object_dependency_row)
        .collect())
}

fn fetch_fe_locks(
    ctx: &SchemaScanContext,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Vec<SchemaRow>, String> {
    let request = frontend_service::TFeLocksReq::new(Some(build_auth_info(ctx)));
    let response = with_frontend_client(fe_addr, |client| {
        client.list_fe_locks(request).map_err(|err| err.to_string())
    })?;
    Ok(response
        .items
        .unwrap_or_default()
        .iter()
        .map(build_fe_lock_row)
        .collect())
}

fn fetch_fe_memory_usage(
    ctx: &SchemaScanContext,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Vec<SchemaRow>, String> {
    let request = frontend_service::TFeMemoryReq::new(Some(build_auth_info(ctx)));
    let response = with_frontend_client(fe_addr, |client| {
        client
            .list_fe_memory_usage(request)
            .map_err(|err| err.to_string())
    })?;
    Ok(response
        .items
        .unwrap_or_default()
        .iter()
        .map(build_fe_memory_row)
        .collect())
}

fn fetch_db_names(
    ctx: &SchemaScanContext,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Vec<String>, String> {
    let request = frontend_service::TGetDbsParams::new(
        ctx.wild.clone().or_else(|| ctx.db.clone()),
        ctx.user.clone(),
        ctx.user_ip.clone().or_else(|| ctx.ip.clone()),
        effective_current_user_ident(ctx),
        ctx.catalog_name.clone(),
    );
    let response = with_frontend_client(fe_addr, |client| {
        client.get_db_names(request).map_err(|err| err.to_string())
    })?;
    Ok(response
        .dbs
        .unwrap_or_default()
        .iter()
        .map(|db| extract_db_name(db))
        .collect())
}

fn fetch_table_names(
    ctx: &SchemaScanContext,
    fe_addr: Option<&types::TNetworkAddress>,
    db_name: &str,
) -> Result<Vec<String>, String> {
    let request = frontend_service::TGetTablesParams::new(
        Some(db_name.to_string()),
        None::<String>,
        ctx.user.clone(),
        ctx.user_ip.clone().or_else(|| ctx.ip.clone()),
        effective_current_user_ident(ctx),
        None::<types::TTableType>,
        None::<i64>,
        ctx.catalog_name.clone(),
        None::<String>,
    );
    let response = with_frontend_client(fe_addr, |client| {
        client
            .get_table_names(request)
            .map_err(|err| err.to_string())
    })?;
    Ok(response.tables.unwrap_or_default())
}

fn build_partition_meta_row(info: &frontend_service::TPartitionMetaInfo) -> SchemaRow {
    let mut row = SchemaRow::new();
    put_opt_str(&mut row, "DB_NAME", info.db_name.as_deref());
    put_opt_str(&mut row, "TABLE_NAME", info.table_name.as_deref());
    put_opt_str(&mut row, "PARTITION_NAME", info.partition_name.as_deref());
    put_i64(&mut row, "PARTITION_ID", info.partition_id);
    put_i64(&mut row, "COMPACT_VERSION", info.compact_version);
    put_i64(&mut row, "VISIBLE_VERSION", info.visible_version);
    put_ts_seconds_positive(&mut row, "VISIBLE_VERSION_TIME", info.visible_version_time);
    put_i64(&mut row, "NEXT_VERSION", info.next_version);
    put_i64(&mut row, "DATA_VERSION", info.data_version);
    put_i64(&mut row, "VERSION_EPOCH", info.version_epoch);
    put_opt_str(
        &mut row,
        "VERSION_TXN_TYPE",
        info.version_txn_type.as_ref().map(txn_type_name),
    );
    put_opt_str(&mut row, "PARTITION_KEY", info.partition_key.as_deref());
    put_opt_str(&mut row, "PARTITION_VALUE", info.partition_value.as_deref());
    put_opt_str(
        &mut row,
        "DISTRIBUTION_KEY",
        info.distribution_key.as_deref(),
    );
    put_i32(&mut row, "BUCKETS", info.buckets);
    put_i32(&mut row, "REPLICATION_NUM", info.replication_num);
    put_opt_str(&mut row, "STORAGE_MEDIUM", info.storage_medium.as_deref());
    put_ts_seconds_positive(&mut row, "COOLDOWN_TIME", info.cooldown_time);
    put_ts_seconds_positive(
        &mut row,
        "LAST_CONSISTENCY_CHECK_TIME",
        info.last_consistency_check_time,
    );
    put_bool(&mut row, "IS_IN_MEMORY", info.is_in_memory.or(Some(false)));
    put_bool(&mut row, "IS_TEMP", info.is_temp);
    put_i64_from_string(&mut row, "DATA_SIZE", info.data_size.as_deref());
    put_i64(&mut row, "ROW_COUNT", info.row_count);
    put_bool(&mut row, "ENABLE_DATACACHE", info.enable_datacache);
    put_f64(&mut row, "AVG_CS", info.avg_cs);
    put_f64(&mut row, "P50_CS", info.p50_cs);
    put_f64(&mut row, "MAX_CS", info.max_cs);
    put_opt_str(&mut row, "STORAGE_PATH", info.storage_path.as_deref());
    put_i64(&mut row, "STORAGE_SIZE", info.storage_size);
    put_bool(&mut row, "TABLET_BALANCED", info.tablet_balanced);
    put_i64(
        &mut row,
        "METADATA_SWITCH_VERSION",
        info.metadata_switch_version,
    );
    row
}

fn build_table_row(info: &frontend_service::TTableInfo) -> SchemaRow {
    let mut row = SchemaRow::new();
    let table_schema = info.table_schema.as_deref().map(extract_db_name_owned);
    put_str(
        &mut row,
        "TABLE_CATALOG",
        info.table_catalog
            .as_deref()
            .unwrap_or(DEFAULT_TABLE_CATALOG),
    );
    put_opt_str(&mut row, "TABLE_SCHEMA", table_schema.as_deref());
    put_opt_str(&mut row, "TABLE_NAME", info.table_name.as_deref());
    put_opt_str(&mut row, "TABLE_TYPE", info.table_type.as_deref());
    put_opt_str(&mut row, "ENGINE", info.engine.as_deref());
    put_i64(&mut row, "VERSION", info.version);
    put_opt_str(&mut row, "ROW_FORMAT", info.row_format.as_deref());
    put_i64(&mut row, "TABLE_ROWS", info.table_rows);
    put_i64(&mut row, "AVG_ROW_LENGTH", info.avg_row_length);
    put_i64(&mut row, "DATA_LENGTH", info.data_length);
    put_i64(&mut row, "MAX_DATA_LENGTH", info.max_data_length);
    put_i64(&mut row, "INDEX_LENGTH", info.index_length);
    put_i64(&mut row, "DATA_FREE", info.data_free);
    put_i64(&mut row, "AUTO_INCREMENT", info.auto_increment);
    put_ts_seconds_allow_zero(&mut row, "CREATE_TIME", info.create_time);
    put_ts_seconds_allow_zero(&mut row, "UPDATE_TIME", info.update_time);
    put_ts_seconds_allow_zero(&mut row, "CHECK_TIME", info.check_time);
    put_opt_str(&mut row, "TABLE_COLLATION", info.table_collation.as_deref());
    put_i64(&mut row, "CHECKSUM", info.checksum);
    put_opt_str(&mut row, "CREATE_OPTIONS", info.create_options.as_deref());
    put_opt_str(&mut row, "TABLE_COMMENT", info.table_comment.as_deref());
    row
}

fn build_tables_config_row(info: &frontend_service::TTableConfigInfo) -> SchemaRow {
    let mut row = SchemaRow::new();
    put_opt_str(&mut row, "TABLE_SCHEMA", info.table_schema.as_deref());
    put_opt_str(&mut row, "TABLE_NAME", info.table_name.as_deref());
    put_opt_str(&mut row, "TABLE_ENGINE", info.table_engine.as_deref());
    put_opt_str(&mut row, "TABLE_MODEL", info.table_model.as_deref());
    put_opt_str(&mut row, "PRIMARY_KEY", info.primary_key.as_deref());
    put_opt_str(&mut row, "PARTITION_KEY", info.partition_key.as_deref());
    put_opt_str(&mut row, "DISTRIBUTE_KEY", info.distribute_key.as_deref());
    put_opt_str(&mut row, "DISTRIBUTE_TYPE", info.distribute_type.as_deref());
    put_i32(&mut row, "DISTRIBUTE_BUCKET", info.distribute_bucket);
    put_opt_str(&mut row, "SORT_KEY", info.sort_key.as_deref());
    put_opt_str(&mut row, "PROPERTIES", info.properties.as_deref());
    put_i64(&mut row, "TABLE_ID", info.table_id);
    row
}

fn build_column_row(
    catalog_name: &str,
    db_name: &str,
    table_name: &str,
    ordinal_position: usize,
    column: &frontend_service::TColumnDef,
) -> SchemaRow {
    let mut row = SchemaRow::new();
    let desc = &column.column_desc;
    put_str(&mut row, "TABLE_CATALOG", catalog_name);
    put_str(&mut row, "TABLE_SCHEMA", db_name);
    put_str(&mut row, "TABLE_NAME", table_name);
    put_str(&mut row, "COLUMN_NAME", &desc.column_name);
    put_i64(&mut row, "ORDINAL_POSITION", Some(ordinal_position as i64));
    if let Some(default_value) = desc.column_default.as_ref() {
        put_str(&mut row, "COLUMN_DEFAULT", default_value);
    }
    put_str(
        &mut row,
        "IS_NULLABLE",
        if desc.allow_null.unwrap_or(false) {
            "YES"
        } else {
            "NO"
        },
    );
    put_str(
        &mut row,
        "DATA_TYPE",
        desc.data_type
            .as_deref()
            .unwrap_or_else(|| primitive_type_name(Some(&desc.column_type))),
    );
    put_i32(&mut row, "CHARACTER_MAXIMUM_LENGTH", desc.column_length);
    put_i32(&mut row, "CHARACTER_OCTET_LENGTH", desc.column_length);
    put_i32(&mut row, "NUMERIC_PRECISION", desc.column_precision);
    put_i32(&mut row, "NUMERIC_SCALE", desc.column_scale);
    put_i32(&mut row, "DATETIME_PRECISION", None::<i32>);
    put_opt_str(&mut row, "COLUMN_TYPE", desc.column_type_str.as_deref());
    put_opt_str(&mut row, "COLUMN_KEY", desc.column_key.as_deref());
    put_str(
        &mut row,
        "EXTRA",
        if desc.generated_column_expr_str.is_some() {
            "GENERATED"
        } else {
            ""
        },
    );
    put_str(&mut row, "PRIVILEGES", "");
    put_str(
        &mut row,
        "COLUMN_COMMENT",
        column.comment.as_deref().unwrap_or_default(),
    );
    put_i32(&mut row, "COLUMN_SIZE", desc.column_length);
    put_i32(&mut row, "DECIMAL_DIGITS", desc.column_scale);
    put_opt_str(
        &mut row,
        "GENERATION_EXPRESSION",
        desc.generated_column_expr_str.as_deref(),
    );
    row
}

fn build_materialized_view_row(info: &frontend_service::TMaterializedViewStatus) -> SchemaRow {
    let mut row = SchemaRow::new();
    put_i64_from_string(&mut row, "MATERIALIZED_VIEW_ID", info.id.as_deref());
    put_opt_str(&mut row, "TABLE_SCHEMA", info.database_name.as_deref());
    put_opt_str(&mut row, "TABLE_NAME", info.name.as_deref());
    put_opt_str(&mut row, "REFRESH_TYPE", info.refresh_type.as_deref());
    put_opt_str(&mut row, "IS_ACTIVE", info.is_active.as_deref());
    put_opt_str(&mut row, "INACTIVE_REASON", info.inactive_reason.as_deref());
    put_opt_str(&mut row, "PARTITION_TYPE", info.partition_type.as_deref());
    put_i64_from_string(&mut row, "TASK_ID", info.task_id.as_deref());
    put_opt_str(&mut row, "TASK_NAME", info.task_name.as_deref());
    put_ts_text(
        &mut row,
        "LAST_REFRESH_START_TIME",
        info.last_refresh_start_time.as_deref(),
    );
    put_ts_text(
        &mut row,
        "LAST_REFRESH_FINISHED_TIME",
        info.last_refresh_finished_time.as_deref(),
    );
    put_f64_from_string(
        &mut row,
        "LAST_REFRESH_DURATION",
        info.last_refresh_duration.as_deref(),
    );
    put_opt_str(
        &mut row,
        "LAST_REFRESH_STATE",
        info.last_refresh_state.as_deref(),
    );
    put_opt_str(
        &mut row,
        "LAST_REFRESH_FORCE_REFRESH",
        info.last_refresh_force_refresh.as_deref(),
    );
    put_opt_str(
        &mut row,
        "LAST_REFRESH_START_PARTITION",
        info.last_refresh_start_partition.as_deref(),
    );
    put_opt_str(
        &mut row,
        "LAST_REFRESH_END_PARTITION",
        info.last_refresh_end_partition.as_deref(),
    );
    put_opt_str(
        &mut row,
        "LAST_REFRESH_BASE_REFRESH_PARTITIONS",
        info.last_refresh_base_refresh_partitions.as_deref(),
    );
    put_opt_str(
        &mut row,
        "LAST_REFRESH_MV_REFRESH_PARTITIONS",
        info.last_refresh_mv_refresh_partitions.as_deref(),
    );
    put_opt_str(
        &mut row,
        "LAST_REFRESH_ERROR_CODE",
        info.last_refresh_error_code.as_deref(),
    );
    put_opt_str(
        &mut row,
        "LAST_REFRESH_ERROR_MESSAGE",
        info.last_refresh_error_message.as_deref(),
    );
    put_i64_from_string(&mut row, "TABLE_ROWS", info.rows.as_deref());
    put_opt_str(
        &mut row,
        "MATERIALIZED_VIEW_DEFINITION",
        info.text.as_deref().or(info.ddl_sql.as_deref()),
    );
    put_opt_str(&mut row, "EXTRA_MESSAGE", info.extra_message.as_deref());
    put_opt_str(
        &mut row,
        "QUERY_REWRITE_STATUS",
        info.query_rewrite_status.as_deref(),
    );
    put_opt_str(&mut row, "CREATOR", info.creator.as_deref());
    put_ts_text(
        &mut row,
        "LAST_REFRESH_PROCESS_TIME",
        info.last_refresh_process_time.as_deref(),
    );
    put_opt_str(
        &mut row,
        "LAST_REFRESH_JOB_ID",
        info.last_refresh_job_id.as_deref(),
    );
    row
}

fn build_task_run_row(info: &frontend_service::TTaskRunInfo) -> SchemaRow {
    let mut row = SchemaRow::new();
    put_opt_str(&mut row, "QUERY_ID", info.query_id.as_deref());
    put_opt_str(&mut row, "TASK_NAME", info.task_name.as_deref());
    put_ts_seconds_positive(&mut row, "CREATE_TIME", info.create_time);
    put_ts_seconds_positive(&mut row, "FINISH_TIME", info.finish_time);
    put_opt_str(&mut row, "STATE", info.state.as_deref());
    put_opt_str(&mut row, "CATALOG", info.catalog.as_deref());
    put_opt_str(&mut row, "WAREHOUSE", info.warehouse.as_deref());
    put_opt_str(&mut row, "DATABASE", info.database.as_deref());
    put_opt_str(&mut row, "DEFINITION", info.definition.as_deref());
    put_ts_seconds_positive(&mut row, "EXPIRE_TIME", info.expire_time);
    put_i32(&mut row, "ERROR_CODE", info.error_code);
    put_opt_str(&mut row, "ERROR_MESSAGE", info.error_message.as_deref());
    put_opt_str(&mut row, "PROGRESS", info.progress.as_deref());
    put_opt_str(&mut row, "EXTRA_MESSAGE", info.extra_message.as_deref());
    put_opt_str(&mut row, "PROPERTIES", info.properties.as_deref());
    put_i64_from_string(&mut row, "JOB_ID", info.job_id.as_deref());
    put_ts_seconds_positive(&mut row, "PROCESS_TIME", info.process_time);
    row
}

fn build_analyze_status_row(info: &frontend_service::TAnalyzeStatusItem) -> SchemaRow {
    let mut row = SchemaRow::new();
    put_str(&mut row, "ID", info.id.as_deref().unwrap_or_default());
    put_str(
        &mut row,
        "CATALOG",
        info.catalog_name.as_deref().unwrap_or_default(),
    );
    put_str(
        &mut row,
        "DATABASE",
        info.database_name.as_deref().unwrap_or_default(),
    );
    put_str(
        &mut row,
        "TABLE",
        info.table_name.as_deref().unwrap_or_default(),
    );
    put_str(
        &mut row,
        "COLUMNS",
        info.columns.as_deref().unwrap_or_default(),
    );
    put_str(&mut row, "TYPE", info.type_.as_deref().unwrap_or_default());
    put_str(
        &mut row,
        "SCHEDULE",
        info.schedule.as_deref().unwrap_or_default(),
    );
    put_str(
        &mut row,
        "STATUS",
        info.status.as_deref().unwrap_or_default(),
    );
    put_str(
        &mut row,
        "STARTTIME",
        info.start_time.as_deref().unwrap_or_default(),
    );
    put_str(
        &mut row,
        "ENDTIME",
        info.end_time.as_deref().unwrap_or_default(),
    );
    put_str(
        &mut row,
        "PROPERTIES",
        info.properties.as_deref().unwrap_or_default(),
    );
    put_str(
        &mut row,
        "REASON",
        info.reason.as_deref().unwrap_or_default(),
    );
    row
}

fn build_character_set_row(values: &[String]) -> SchemaRow {
    let mut row = SchemaRow::new();
    put_opt_str(&mut row, "CHARACTER_SET_NAME", show_value(values, 0));
    put_opt_str(&mut row, "DEFAULT_COLLATE_NAME", show_value(values, 2));
    put_opt_str(&mut row, "DESCRIPTION", show_value(values, 1));
    put_i64_from_string(&mut row, "MAXLEN", show_value(values, 3));
    row
}

fn build_collation_row(values: &[String]) -> SchemaRow {
    let mut row = SchemaRow::new();
    put_opt_str(&mut row, "COLLATION_NAME", show_value(values, 0));
    put_opt_str(&mut row, "CHARACTER_SET_NAME", show_value(values, 1));
    put_i64_from_string(&mut row, "ID", show_value(values, 2));
    put_opt_str(&mut row, "IS_DEFAULT", show_value(values, 3));
    put_opt_str(&mut row, "IS_COMPILED", show_value(values, 4));
    put_i64_from_string(&mut row, "SORTLEN", show_value(values, 5));
    row
}

fn build_column_stats_usage_row(info: &frontend_service::TColumnStatsUsage) -> SchemaRow {
    let mut row = SchemaRow::new();
    put_opt_str(&mut row, "TABLE_CATALOG", info.table_catalog.as_deref());
    put_opt_str(&mut row, "TABLE_DATABASE", info.table_database.as_deref());
    put_opt_str(&mut row, "TABLE_NAME", info.table_name.as_deref());
    put_opt_str(&mut row, "COLUMN_NAME", info.column_name.as_deref());
    put_opt_str(&mut row, "USAGE", info.usage.as_deref());
    put_ts_seconds_positive(&mut row, "LAST_USED", info.last_used);
    put_ts_seconds_positive(&mut row, "CREATED", info.created);
    row
}

fn build_engine_row(values: &[String]) -> SchemaRow {
    let mut row = SchemaRow::new();
    put_opt_str(&mut row, "ENGINE", show_value(values, 0));
    put_opt_str(&mut row, "SUPPORT", show_value(values, 1));
    put_opt_str(&mut row, "COMMENT", show_value(values, 2));
    put_opt_str(&mut row, "TRANSACTIONS", show_value(values, 3));
    put_opt_str(&mut row, "XA", show_value(values, 4));
    put_opt_str(&mut row, "SAVEPOINTS", show_value(values, 5));
    row
}

fn build_fe_tablet_schedule_row(info: &frontend_service::TTabletSchedule) -> SchemaRow {
    let mut row = SchemaRow::new();
    put_i64(&mut row, "TABLET_ID", info.tablet_id);
    put_i64(&mut row, "TABLE_ID", info.table_id);
    put_i64(&mut row, "PARTITION_ID", info.partition_id);
    put_opt_str(&mut row, "TYPE", info.type_.as_deref());
    put_opt_str(&mut row, "STATE", info.state.as_deref());
    put_opt_str(&mut row, "SCHEDULE_REASON", info.schedule_reason.as_deref());
    put_opt_str(&mut row, "MEDIUM", info.medium.as_deref());
    put_opt_str(&mut row, "PRIORITY", info.priority.as_deref());
    put_opt_str(&mut row, "ORIG_PRIORITY", info.orig_priority.as_deref());
    put_ts_seconds_positive(
        &mut row,
        "LAST_PRIORITY_ADJUST_TIME",
        info.last_priority_adjust_time,
    );
    put_i64(&mut row, "VISIBLE_VERSION", info.visible_version);
    put_i64(&mut row, "COMMITTED_VERSION", info.committed_version);
    put_i64(&mut row, "SRC_BE_ID", info.src_be_id);
    put_opt_str(&mut row, "SRC_PATH", info.src_path.as_deref());
    put_i64(&mut row, "DEST_BE_ID", info.dest_be_id);
    put_opt_str(&mut row, "DEST_PATH", info.dest_path.as_deref());
    put_i64(&mut row, "TIMEOUT", info.timeout);
    put_ts_seconds_f64_float(&mut row, "CREATE_TIME", info.create_time);
    put_ts_seconds_f64_float(&mut row, "SCHEDULE_TIME", info.schedule_time);
    put_ts_seconds_f64_float(&mut row, "FINISH_TIME", info.finish_time);
    put_i64(&mut row, "CLONE_BYTES", info.clone_bytes);
    put_f64(&mut row, "CLONE_DURATION", info.clone_duration);
    put_f64(&mut row, "CLONE_RATE", info.clone_rate);
    put_i32(
        &mut row,
        "FAILED_SCHEDULE_COUNT",
        info.failed_schedule_count,
    );
    put_i32(&mut row, "FAILED_RUNNING_COUNT", info.failed_running_count);
    put_opt_str(&mut row, "MSG", info.error_msg.as_deref());
    row
}

fn build_pipe_row(info: &frontend_service::TListPipesInfo) -> SchemaRow {
    let mut row = SchemaRow::new();
    put_opt_str(&mut row, "DATABASE_NAME", info.database_name.as_deref());
    put_i64(&mut row, "PIPE_ID", info.pipe_id);
    put_opt_str(&mut row, "PIPE_NAME", info.pipe_name.as_deref());
    put_opt_str(&mut row, "PROPERTIES", info.properties.as_deref());
    put_opt_str(&mut row, "STATE", info.state.as_deref());
    put_opt_str(&mut row, "TABLE_NAME", info.table_name.as_deref());
    put_opt_str(&mut row, "LOAD_STATUS", info.load_status.as_deref());
    put_opt_str(&mut row, "LAST_ERROR", info.last_error.as_deref());
    put_ts_seconds_positive(&mut row, "CREATED_TIME", info.created_time);
    row
}

fn build_pipe_file_row(info: &frontend_service::TListPipeFilesInfo) -> SchemaRow {
    let mut row = SchemaRow::new();
    put_opt_str(&mut row, "DATABASE_NAME", info.database_name.as_deref());
    put_i64(&mut row, "PIPE_ID", info.pipe_id);
    put_opt_str(&mut row, "PIPE_NAME", info.pipe_name.as_deref());
    put_opt_str(&mut row, "FILE_NAME", info.file_name.as_deref());
    put_opt_str(&mut row, "FILE_VERSION", info.file_version.as_deref());
    put_i64(&mut row, "FILE_SIZE", info.file_size);
    put_opt_str(&mut row, "LAST_MODIFIED", info.last_modified.as_deref());
    put_opt_str(&mut row, "LOAD_STATE", info.state.as_deref());
    put_opt_str(&mut row, "STAGED_TIME", info.staged_time.as_deref());
    put_opt_str(&mut row, "START_LOAD_TIME", info.start_load.as_deref());
    put_opt_str(&mut row, "FINISH_LOAD_TIME", info.finish_load.as_deref());
    put_opt_str(&mut row, "ERROR_MSG", info.first_error_msg.as_deref());
    row
}

fn build_routine_load_job_row(info: &frontend_service::TRoutineLoadJobInfo) -> SchemaRow {
    let mut row = SchemaRow::new();
    put_i64(&mut row, "ID", info.id);
    put_opt_str(&mut row, "NAME", info.name.as_deref());
    put_ts_text(&mut row, "CREATE_TIME", info.create_time.as_deref());
    put_ts_text(&mut row, "PAUSE_TIME", info.pause_time.as_deref());
    put_ts_text(&mut row, "END_TIME", info.end_time.as_deref());
    put_opt_str(&mut row, "DB_NAME", info.db_name.as_deref());
    put_opt_str(&mut row, "TABLE_NAME", info.table_name.as_deref());
    put_opt_str(&mut row, "STATE", info.state.as_deref());
    put_opt_str(
        &mut row,
        "DATA_SOURCE_TYPE",
        info.data_source_type.as_deref(),
    );
    put_i64(&mut row, "CURRENT_TASK_NUM", info.current_task_num);
    put_opt_str(&mut row, "JOB_PROPERTIES", info.job_properties.as_deref());
    put_opt_str(
        &mut row,
        "DATA_SOURCE_PROPERTIES",
        info.data_source_properties.as_deref(),
    );
    put_opt_str(
        &mut row,
        "CUSTOM_PROPERTIES",
        info.custom_properties.as_deref(),
    );
    put_opt_str(&mut row, "STATISTICS", info.statistic.as_deref());
    put_opt_str(&mut row, "PROGRESS", info.progress.as_deref());
    put_opt_str(
        &mut row,
        "REASONS_OF_STATE_CHANGED",
        info.reasons_of_state_changed.as_deref(),
    );
    put_opt_str(&mut row, "ERROR_LOG_URLS", info.error_log_urls.as_deref());
    put_opt_str(&mut row, "TRACKING_SQL", info.tracking_sql.as_deref());
    put_opt_str(&mut row, "OTHER_MSG", info.other_msg.as_deref());
    put_opt_str(
        &mut row,
        "LATEST_SOURCE_POSITION",
        info.latest_source_position.as_deref(),
    );
    put_opt_str(&mut row, "OFFSET_LAG", info.offset_lag.as_deref());
    put_opt_str(
        &mut row,
        "TIMESTAMP_PROGRESS",
        info.timestamp_progress.as_deref(),
    );
    row
}

fn build_stream_load_row(info: &frontend_service::TStreamLoadInfo) -> SchemaRow {
    let mut row = SchemaRow::new();
    put_opt_str(&mut row, "LABEL", info.label.as_deref());
    put_i64(&mut row, "ID", info.id);
    put_opt_str(&mut row, "LOAD_ID", info.load_id.as_deref());
    put_i64(&mut row, "TXN_ID", info.txn_id);
    put_opt_str(&mut row, "DB_NAME", info.db_name.as_deref());
    put_opt_str(&mut row, "TABLE_NAME", info.table_name.as_deref());
    put_opt_str(&mut row, "STATE", info.state.as_deref());
    put_opt_str(&mut row, "ERROR_MSG", info.error_msg.as_deref());
    put_opt_str(&mut row, "TRACKING_URL", info.tracking_url.as_deref());
    put_i64(&mut row, "CHANNEL_NUM", info.channel_num);
    put_i64(&mut row, "PREPARED_CHANNEL_NUM", info.prepared_channel_num);
    put_i64(&mut row, "NUM_ROWS_NORMAL", info.num_rows_normal);
    put_i64(&mut row, "NUM_ROWS_AB_NORMAL", info.num_rows_ab_normal);
    put_i64(&mut row, "NUM_ROWS_UNSELECTED", info.num_rows_unselected);
    put_i64(&mut row, "NUM_LOAD_BYTES", info.num_load_bytes);
    put_i64(&mut row, "TIMEOUT_SECOND", info.timeout_second);
    put_ts_text(&mut row, "CREATE_TIME_MS", info.create_time_ms.as_deref());
    put_ts_text(
        &mut row,
        "BEFORE_LOAD_TIME_MS",
        info.before_load_time_ms.as_deref(),
    );
    put_ts_text(
        &mut row,
        "START_LOADING_TIME_MS",
        info.start_loading_time_ms.as_deref(),
    );
    put_ts_text(
        &mut row,
        "START_PREPARING_TIME_MS",
        info.start_preparing_time_ms.as_deref(),
    );
    put_ts_text(
        &mut row,
        "FINISH_PREPARING_TIME_MS",
        info.finish_preparing_time_ms.as_deref(),
    );
    put_ts_text(&mut row, "END_TIME_MS", info.end_time_ms.as_deref());
    put_opt_str(&mut row, "CHANNEL_STATE", info.channel_state.as_deref());
    put_opt_str(&mut row, "TYPE", info.type_.as_deref());
    put_opt_str(&mut row, "TRACKING_SQL", info.tracking_sql.as_deref());
    row
}

fn build_task_row(info: &frontend_service::TTaskInfo) -> SchemaRow {
    let mut row = SchemaRow::new();
    put_opt_str(&mut row, "TASK_NAME", info.task_name.as_deref());
    put_ts_seconds_positive(&mut row, "CREATE_TIME", info.create_time);
    put_opt_str(&mut row, "SCHEDULE", info.schedule.as_deref());
    put_opt_str(&mut row, "CATALOG", info.catalog.as_deref());
    put_opt_str(&mut row, "DATABASE", info.database.as_deref());
    put_opt_str(&mut row, "DEFINITION", info.definition.as_deref());
    put_ts_seconds_positive(&mut row, "EXPIRE_TIME", info.expire_time);
    put_opt_str(&mut row, "PROPERTIES", info.properties.as_deref());
    put_opt_str(&mut row, "CREATOR", info.creator.as_deref());
    row
}

fn build_temp_table_row(info: &frontend_service::TTableInfo) -> SchemaRow {
    let mut row = build_table_row(info);
    put_opt_str(&mut row, "SESSION", info.session_id.as_deref());
    row
}

fn build_user_privilege_row(info: &frontend_service::TUserPrivDesc) -> SchemaRow {
    let mut row = SchemaRow::new();
    put_opt_str(&mut row, "GRANTEE", info.user_ident_str.as_deref());
    put_str(&mut row, "TABLE_CATALOG", DEFAULT_TABLE_CATALOG);
    put_opt_str(&mut row, "PRIVILEGE_TYPE", info.priv_.as_deref());
    put_yes_no(&mut row, "IS_GRANTABLE", info.is_grantable);
    row
}

fn build_schema_privilege_row(info: &frontend_service::TDBPrivDesc) -> SchemaRow {
    let mut row = SchemaRow::new();
    let table_schema = info.db_name.as_deref().map(extract_db_name_owned);
    put_opt_str(&mut row, "GRANTEE", info.user_ident_str.as_deref());
    put_str(&mut row, "TABLE_CATALOG", DEFAULT_TABLE_CATALOG);
    put_opt_str(&mut row, "TABLE_SCHEMA", table_schema.as_deref());
    put_opt_str(&mut row, "PRIVILEGE_TYPE", info.priv_.as_deref());
    put_yes_no(&mut row, "IS_GRANTABLE", info.is_grantable);
    row
}

fn build_table_privilege_row(info: &frontend_service::TTablePrivDesc) -> SchemaRow {
    let mut row = SchemaRow::new();
    let table_schema = info.db_name.as_deref().map(extract_db_name_owned);
    put_opt_str(&mut row, "GRANTEE", info.user_ident_str.as_deref());
    put_str(&mut row, "TABLE_CATALOG", DEFAULT_TABLE_CATALOG);
    put_opt_str(&mut row, "TABLE_SCHEMA", table_schema.as_deref());
    put_opt_str(&mut row, "TABLE_NAME", info.table_name.as_deref());
    put_opt_str(&mut row, "PRIVILEGE_TYPE", info.priv_.as_deref());
    put_yes_no(&mut row, "IS_GRANTABLE", info.is_grantable);
    row
}

fn build_variable_row(values: &[String]) -> SchemaRow {
    let mut row = SchemaRow::new();
    put_opt_str(&mut row, "VARIABLE_NAME", show_value(values, 0));
    put_opt_str(&mut row, "VARIABLE_VALUE", show_value(values, 1));
    row
}

fn build_verbose_variable_row(values: &[String]) -> SchemaRow {
    let mut row = build_variable_row(values);
    put_opt_str(&mut row, "DEFAULT_VALUE", show_value(values, 2));
    put_bool_text(&mut row, "IS_CHANGED", show_value(values, 3));
    row
}

fn build_grants_to_row(info: &frontend_service::TGetGrantsToRolesOrUserItem) -> SchemaRow {
    let mut row = SchemaRow::new();
    put_opt_str(&mut row, "GRANTEE", info.grantee.as_deref());
    put_opt_str(&mut row, "OBJECT_CATALOG", info.object_catalog.as_deref());
    put_opt_str(&mut row, "OBJECT_DATABASE", info.object_database.as_deref());
    put_opt_str(&mut row, "OBJECT_NAME", info.object_name.as_deref());
    put_opt_str(&mut row, "OBJECT_TYPE", info.object_type.as_deref());
    put_opt_str(&mut row, "PRIVILEGE_TYPE", info.privilege_type.as_deref());
    put_yes_no(&mut row, "IS_GRANTABLE", info.is_grantable);
    row
}

fn build_role_edge_row(info: &frontend_service::TGetRoleEdgesItem) -> SchemaRow {
    let mut row = SchemaRow::new();
    put_opt_str(&mut row, "FROM_ROLE", info.from_role.as_deref());
    put_opt_str(&mut row, "TO_ROLE", info.to_role.as_deref());
    put_opt_str(&mut row, "TO_USER", info.to_user.as_deref());
    row
}

fn build_warehouse_metrics_row(
    info: &frontend_service::TGetWarehouseMetricsResponeItem,
) -> SchemaRow {
    let mut row = SchemaRow::new();
    put_i64_from_string(&mut row, "WAREHOUSE_ID", info.warehouse_id.as_deref());
    put_opt_str(&mut row, "WAREHOUSE_NAME", info.warehouse_name.as_deref());
    put_opt_str(
        &mut row,
        "QUEUE_PENDING_LENGTH",
        info.queue_pending_length.as_deref(),
    );
    put_opt_str(
        &mut row,
        "QUEUE_RUNNING_LENGTH",
        info.queue_running_length.as_deref(),
    );
    put_opt_str(
        &mut row,
        "MAX_PENDING_LENGTH",
        info.max_pending_length.as_deref(),
    );
    put_opt_str(
        &mut row,
        "MAX_PENDING_TIME_SECOND",
        info.max_pending_time_second.as_deref(),
    );
    put_opt_str(
        &mut row,
        "EARLIEST_QUERY_WAIT_TIME",
        info.earliest_query_wait_time.as_deref(),
    );
    put_opt_str(
        &mut row,
        "MAX_REQUIRED_SLOTS",
        info.max_required_slots.as_deref(),
    );
    put_opt_str(
        &mut row,
        "SUM_REQUIRED_SLOTS",
        info.sum_required_slots.as_deref(),
    );
    put_opt_str(&mut row, "REMAIN_SLOTS", info.remain_slots.as_deref());
    put_opt_str(&mut row, "MAX_SLOTS", info.max_slots.as_deref());
    put_opt_str(&mut row, "EXTRA_MESSAGE", info.extra_message.as_deref());
    row
}

fn build_warehouse_queries_row(
    info: &frontend_service::TGetWarehouseQueriesResponseItem,
) -> SchemaRow {
    let mut row = SchemaRow::new();
    put_i64_from_string(&mut row, "WAREHOUSE_ID", info.warehouse_id.as_deref());
    put_opt_str(&mut row, "WAREHOUSE_NAME", info.warehouse_name.as_deref());
    put_opt_str(&mut row, "QUERY_ID", info.query_id.as_deref());
    put_opt_str(&mut row, "STATE", info.state.as_deref());
    put_opt_str(&mut row, "EST_COSTS_SLOTS", info.est_costs_slots.as_deref());
    put_opt_str(&mut row, "ALLOCATE_SLOTS", info.allocate_slots.as_deref());
    put_opt_str(
        &mut row,
        "QUEUED_WAIT_SECONDS",
        info.queued_wait_seconds.as_deref(),
    );
    put_opt_str(&mut row, "QUERY", info.query.as_deref());
    put_opt_str(
        &mut row,
        "QUERY_START_TIME",
        info.query_start_time.as_deref(),
    );
    put_opt_str(&mut row, "QUERY_END_TIME", info.query_end_time.as_deref());
    put_opt_str(&mut row, "QUERY_DURATION", info.query_duration.as_deref());
    put_opt_str(&mut row, "EXTRA_MESSAGE", info.extra_message.as_deref());
    row
}

fn build_tablet_reshard_job_row(info: &frontend_service::TTabletReshardJobsItem) -> SchemaRow {
    let mut row = SchemaRow::new();
    put_i64(&mut row, "JOB_ID", info.job_id);
    put_i64(&mut row, "DB_ID", info.db_id);
    put_opt_str(&mut row, "DB_NAME", info.db_name.as_deref());
    put_i64(&mut row, "TABLE_ID", info.table_id);
    put_opt_str(&mut row, "TABLE_NAME", info.table_name.as_deref());
    put_opt_str(&mut row, "JOB_TYPE", info.job_type.as_deref());
    put_opt_str(&mut row, "JOB_STATE", info.job_state.as_deref());
    put_i64(&mut row, "TRANSACTION_ID", info.transaction_id);
    put_i64(&mut row, "PARALLEL_PARTITIONS", info.parallel_partitions);
    put_i64(&mut row, "PARALLEL_TABLETS", info.parallel_tablets);
    put_ts_seconds_allow_zero(&mut row, "CREATED_TIME", info.created_time);
    put_ts_seconds_positive(&mut row, "FINISHED_TIME", info.finished_time);
    put_opt_str(&mut row, "ERROR_MESSAGE", info.error_message.as_deref());
    row
}

fn build_cluster_snapshot_row(info: &frontend_service::TClusterSnapshotsItem) -> SchemaRow {
    let mut row = SchemaRow::new();
    put_opt_str(&mut row, "SNAPSHOT_NAME", info.snapshot_name.as_deref());
    put_opt_str(&mut row, "SNAPSHOT_TYPE", info.snapshot_type.as_deref());
    put_ts_seconds_allow_zero(&mut row, "CREATED_TIME", info.created_time);
    put_i64(&mut row, "FE_JOURANL_ID", info.fe_jouranl_id);
    put_i64(&mut row, "STARMGR_JOURANL_ID", info.starmgr_jouranl_id);
    put_opt_str(&mut row, "PROPERTIES", info.properties.as_deref());
    put_opt_str(&mut row, "STORAGE_VOLUME", info.storage_volume.as_deref());
    put_opt_str(&mut row, "STORAGE_PATH", info.storage_path.as_deref());
    row
}

fn build_cluster_snapshot_job_row(info: &frontend_service::TClusterSnapshotJobsItem) -> SchemaRow {
    let mut row = SchemaRow::new();
    put_opt_str(&mut row, "SNAPSHOT_NAME", info.snapshot_name.as_deref());
    put_i64(&mut row, "JOB_ID", info.job_id);
    put_ts_seconds_allow_zero(&mut row, "CREATED_TIME", info.created_time);
    put_ts_seconds_positive(&mut row, "FINISHED_TIME", info.finished_time);
    put_opt_str(&mut row, "STATE", info.state.as_deref());
    put_opt_str(&mut row, "DETAIL_INFO", info.detail_info.as_deref());
    put_opt_str(&mut row, "ERROR_MESSAGE", info.error_message.as_deref());
    row
}

fn build_recyclebin_catalog_row(info: &frontend_service::TListRecycleBinCatalogsInfo) -> SchemaRow {
    let mut row = SchemaRow::new();
    put_opt_str(&mut row, "TYPE", info.type_.as_deref());
    put_opt_str(&mut row, "NAME", info.name.as_deref());
    put_i64(&mut row, "DB_ID", info.dbid);
    put_i64(&mut row, "TABLE_ID", info.tableid);
    put_i64(&mut row, "PARTITION_ID", info.partitionid);
    put_ts_millis_allow_zero(&mut row, "DROP_TIME", info.droptime);
    row
}

fn build_object_dependency_row(info: &frontend_service::TObjectDependencyItem) -> SchemaRow {
    let mut row = SchemaRow::new();
    put_i64(&mut row, "OBJECT_ID", info.object_id);
    put_opt_str(&mut row, "OBJECT_NAME", info.object_name.as_deref());
    put_opt_str(&mut row, "OBJECT_DATABASE", info.database.as_deref());
    put_opt_str(&mut row, "OBJECT_CATALOG", info.catalog.as_deref());
    put_opt_str(
        &mut row,
        "OBJECT_TYPE",
        info.object_type
            .as_deref()
            .map(normalize_dependency_object_type),
    );
    put_i64(&mut row, "REF_OBJECT_ID", info.ref_object_id);
    put_opt_str(&mut row, "REF_OBJECT_NAME", info.ref_object_name.as_deref());
    put_opt_str(
        &mut row,
        "REF_OBJECT_DATABASE",
        info.ref_database.as_deref(),
    );
    put_opt_str(&mut row, "REF_OBJECT_CATALOG", info.ref_catalog.as_deref());
    put_opt_str(
        &mut row,
        "REF_OBJECT_TYPE",
        info.ref_object_type
            .as_deref()
            .map(normalize_dependency_object_type),
    );
    row
}

fn build_fe_lock_row(info: &frontend_service::TFeLocksItem) -> SchemaRow {
    let mut row = SchemaRow::new();
    put_opt_str(&mut row, "LOCK_TYPE", info.lock_type.as_deref());
    put_opt_str(&mut row, "LOCK_OBJECT", info.lock_object.as_deref());
    put_opt_str(&mut row, "LOCK_MODE", info.lock_mode.as_deref());
    put_ts_millis_allow_zero(&mut row, "START_TIME", info.start_time);
    put_i64(&mut row, "HOLD_TIME_MS", info.hold_time_ms);
    put_opt_str(&mut row, "THREAD_INFO", info.thread_info.as_deref());
    put_bool(&mut row, "GRANTED", info.granted);
    put_opt_str(&mut row, "WAITER_LIST", info.waiter_list.as_deref());
    row
}

fn build_fe_memory_row(info: &frontend_service::TFeMemoryItem) -> SchemaRow {
    let mut row = SchemaRow::new();
    put_opt_str(&mut row, "MODULE_NAME", info.module_name.as_deref());
    put_opt_str(&mut row, "CLASS_NAME", info.class_name.as_deref());
    put_i64(&mut row, "CURRENT_CONSUMPTION", info.current_consumption);
    put_i64(&mut row, "PEAK_CONSUMPTION", info.peak_consumption);
    put_opt_str(&mut row, "COUNTER_INFO", info.counter_info.as_deref());
    row
}

fn put_bool(row: &mut SchemaRow, column: &str, value: Option<bool>) {
    if let Some(value) = value {
        row.insert(normalize_column_key(column), SchemaValue::Boolean(value));
    }
}

fn put_i32(row: &mut SchemaRow, column: &str, value: Option<i32>) {
    if let Some(value) = value {
        row.insert(normalize_column_key(column), SchemaValue::Int32(value));
    }
}

fn put_i64(row: &mut SchemaRow, column: &str, value: Option<i64>) {
    if let Some(value) = value {
        row.insert(normalize_column_key(column), SchemaValue::Int64(value));
    }
}

fn put_i64_from_string(row: &mut SchemaRow, column: &str, value: Option<&str>) {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return;
    };
    if let Ok(value) = value.parse::<i64>() {
        put_i64(row, column, Some(value));
    }
}

fn put_f64(row: &mut SchemaRow, column: &str, value: Option<OrderedFloat<f64>>) {
    if let Some(value) = value {
        row.insert(
            normalize_column_key(column),
            SchemaValue::Float64(value.into_inner()),
        );
    }
}

fn put_f64_from_string(row: &mut SchemaRow, column: &str, value: Option<&str>) {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return;
    };
    if let Ok(value) = value.parse::<f64>() {
        row.insert(normalize_column_key(column), SchemaValue::Float64(value));
    }
}

fn put_yes_no(row: &mut SchemaRow, column: &str, value: Option<bool>) {
    let Some(value) = value else {
        return;
    };
    put_str(row, column, if value { "YES" } else { "NO" });
}

fn put_bool_text(row: &mut SchemaRow, column: &str, value: Option<&str>) {
    let Some(value) = parse_bool_text(value) else {
        return;
    };
    put_bool(row, column, Some(value));
}

fn put_opt_str(row: &mut SchemaRow, column: &str, value: Option<&str>) {
    let Some(value) = value else {
        return;
    };
    row.insert(
        normalize_column_key(column),
        SchemaValue::Utf8(value.to_string()),
    );
}

fn put_str(row: &mut SchemaRow, column: &str, value: &str) {
    row.insert(
        normalize_column_key(column),
        SchemaValue::Utf8(value.to_string()),
    );
}

fn put_ts_seconds_allow_zero(row: &mut SchemaRow, column: &str, value: Option<i64>) {
    if let Some(value) = value {
        row.insert(
            normalize_column_key(column),
            SchemaValue::TimestampMicrosecond(value.saturating_mul(1_000_000)),
        );
    }
}

fn put_ts_seconds_positive(row: &mut SchemaRow, column: &str, value: Option<i64>) {
    if let Some(value) = value.filter(|value| *value > 0) {
        row.insert(
            normalize_column_key(column),
            SchemaValue::TimestampMicrosecond(value.saturating_mul(1_000_000)),
        );
    }
}

fn put_ts_seconds_f64_float(row: &mut SchemaRow, column: &str, value: Option<OrderedFloat<f64>>) {
    let Some(value) = value else {
        return;
    };
    let value = value.into_inner();
    if !value.is_finite() || value <= 0.0 {
        return;
    }
    row.insert(
        normalize_column_key(column),
        SchemaValue::TimestampMicrosecond((value * 1_000_000.0).round() as i64),
    );
}

fn put_ts_millis_allow_zero(row: &mut SchemaRow, column: &str, value: Option<i64>) {
    if let Some(value) = value {
        row.insert(
            normalize_column_key(column),
            SchemaValue::TimestampMicrosecond(value.saturating_mul(1_000)),
        );
    }
}

fn put_ts_text(row: &mut SchemaRow, column: &str, value: Option<&str>) {
    let Some(value) = parse_datetime_to_micros(value) else {
        return;
    };
    row.insert(
        normalize_column_key(column),
        SchemaValue::TimestampMicrosecond(value),
    );
}

fn parse_datetime_to_micros(raw: Option<&str>) -> Option<i64> {
    let raw = raw?.trim();
    if raw.is_empty() {
        return None;
    }
    NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S"))
        .map(|dt| dt.and_utc().timestamp_micros())
        .ok()
}

fn parse_bool_text(raw: Option<&str>) -> Option<bool> {
    let raw = raw?.trim();
    if raw.is_empty() {
        return None;
    }
    if raw.eq_ignore_ascii_case("true") || raw.eq_ignore_ascii_case("yes") || raw == "1" {
        return Some(true);
    }
    if raw.eq_ignore_ascii_case("false") || raw.eq_ignore_ascii_case("no") || raw == "0" {
        return Some(false);
    }
    None
}

fn show_value(values: &[String], idx: usize) -> Option<&str> {
    values.get(idx).map(|value| value.as_str())
}

fn metric_value_as_i64(value: &JsonValue) -> Option<i64> {
    if let Some(value) = value.as_i64() {
        return Some(value);
    }
    value
        .as_f64()
        .filter(|value| value.is_finite())
        .map(|value| value as i64)
}

fn txn_type_name(txn_type: &types::TTxnType) -> &'static str {
    match *txn_type {
        types::TTxnType::TXN_NORMAL => "TXN_NORMAL",
        types::TTxnType::TXN_REPLICATION => "TXN_REPLICATION",
        _ => "UNKNOWN",
    }
}

fn primitive_type_name(primitive_type: Option<&types::TPrimitiveType>) -> &'static str {
    match primitive_type.copied() {
        Some(types::TPrimitiveType::BOOLEAN) => "boolean",
        Some(types::TPrimitiveType::TINYINT) => "tinyint",
        Some(types::TPrimitiveType::SMALLINT) => "smallint",
        Some(types::TPrimitiveType::INT) => "int",
        Some(types::TPrimitiveType::BIGINT) => "bigint",
        Some(types::TPrimitiveType::LARGEINT) => "largeint",
        Some(types::TPrimitiveType::FLOAT) => "float",
        Some(types::TPrimitiveType::DOUBLE) => "double",
        Some(types::TPrimitiveType::DATE) => "date",
        Some(types::TPrimitiveType::DATETIME) => "datetime",
        Some(types::TPrimitiveType::CHAR) => "char",
        Some(types::TPrimitiveType::VARCHAR) => "varchar",
        Some(types::TPrimitiveType::DECIMAL32)
        | Some(types::TPrimitiveType::DECIMAL64)
        | Some(types::TPrimitiveType::DECIMAL128)
        | Some(types::TPrimitiveType::DECIMALV2) => "decimal",
        _ => "unknown",
    }
}

fn normalize_dependency_object_type(value: &str) -> &str {
    match value {
        "CLOUD_NATIVE_MATERIALIZED_VIEW" => "MATERIALIZED_VIEW",
        "CLOUD_NATIVE" => "OLAP",
        other => other,
    }
}

fn extract_db_name_owned(full_name: &str) -> String {
    extract_db_name(full_name)
}
