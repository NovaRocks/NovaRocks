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

//! Existing session-property semantics shared by SQL assignments and header input.
use super::*;

#[derive(Clone, Debug)]
pub(crate) enum SessionProperty {
    Catalog(Option<String>),
    SqlMode(novarocks_sql::sql_mode::SqlMode),
    Autocommit(bool),
    DecimalOverflowToDouble(bool),
    QueryTimeout(u64),
    GroupConcatMaxLen(i64),
    PipelineDop(i32),
    RuntimeFilterScanWait(i64),
    RuntimeFilterWaitTimeout(i32),
    ParquetPageIndex(bool),
    ScanDatacache(bool),
    PopulateDatacache(bool),
    DisabledRules(Vec<String>),
    EliminateAgg(bool),
    UkfkOpt(bool),
    BroadcastBackendCount(f64),
    BroadcastNodeMemory(f64),
    RuntimeFilterBuildMax(u64),
    RuntimeFilterBuildMin(u64),
    RuntimeFilterProbeMin(u64),
    RuntimeFilterSelectivity(f64),
    ReorderExhaustive(usize),
    ReorderDp(usize),
    ReorderGreedy(usize),
    ReorderMax(usize),
    QueryRewriteTablePrune(bool),
    CboTablePrune(bool),
    TablePruneOnUpdate(bool),
    CommonSubexprReuse(bool),
    GlobalRuntimeFilter(bool),
    MaterializedViewRewrite(bool),
    ConnectorStaticPredicatePushdown(bool),
    DpJoinReorder(bool),
    GreedyJoinReorder(bool),
    CrossExchangeRuntimeFilter(bool),
}
impl SessionProperty {
    pub(super) fn parse(name: &str, value: &str) -> Result<Option<Self>, QueryServiceError> {
        Ok(Some(match name {
            "sql_mode" => Self::SqlMode(novarocks_sql::sql_mode::SqlMode::from_assignment(value)),
            "autocommit" => Self::Autocommit(match lower_autocommit_value(value)? {
                AutocommitSetting::Enabled => true,
                AutocommitSetting::Disabled => {
                    return Err(QueryServiceError::new(
                        QueryServiceErrorKind::InvalidValue,
                        "autocommit must be enabled",
                    ));
                }
            }),
            "decimal_overflow_to_double" => Self::DecimalOverflowToDouble(parse_bool(value)?),
            "query_timeout" => Self::QueryTimeout(parse_value(value, name)?),
            "group_concat_max_len" => Self::GroupConcatMaxLen(parse_value(value, name)?),
            "pipeline_dop" => Self::PipelineDop(parse_value(value, name)?),
            "runtime_filter_scan_wait_time" => {
                Self::RuntimeFilterScanWait(parse_value(value, name)?)
            }
            "global_runtime_filter_wait_timeout" => {
                Self::RuntimeFilterWaitTimeout(parse_value(value, name)?)
            }
            "enable_parquet_reader_page_index" => Self::ParquetPageIndex(parse_bool(value)?),
            "enable_scan_datacache" => Self::ScanDatacache(parse_bool(value)?),
            "enable_populate_datacache" => Self::PopulateDatacache(parse_bool(value)?),
            "disable_optimizer_rules" | "cbo_disabled_rules" => Self::DisabledRules(
                value
                    .split(',')
                    .map(str::trim)
                    .filter(|rule| !rule.is_empty())
                    .map(ToOwned::to_owned)
                    .collect(),
            ),
            "enable_eliminate_agg" => Self::EliminateAgg(parse_bool(value)?),
            "enable_ukfk_opt" => Self::UkfkOpt(parse_bool(value)?),
            "cbo_broadcast_backend_count" => Self::BroadcastBackendCount(parse_value(value, name)?),
            "cbo_broadcast_node_mem_budget_bytes" => {
                Self::BroadcastNodeMemory(parse_value(value, name)?)
            }
            "global_runtime_filter_build_max_size" => {
                Self::RuntimeFilterBuildMax(parse_value(value, name)?)
            }
            "global_runtime_filter_build_min_size" => {
                Self::RuntimeFilterBuildMin(parse_value(value, name)?)
            }
            "global_runtime_filter_probe_min_size" => {
                Self::RuntimeFilterProbeMin(parse_value(value, name)?)
            }
            "global_runtime_filter_probe_min_selectivity" => {
                Self::RuntimeFilterSelectivity(parse_value(value, name)?)
            }
            "cbo_max_reorder_node_use_exhaustive" => {
                Self::ReorderExhaustive(parse_value(value, name)?)
            }
            "cbo_max_reorder_node_use_dp" => Self::ReorderDp(parse_value(value, name)?),
            "cbo_max_reorder_node_use_greedy" => Self::ReorderGreedy(parse_value(value, name)?),
            "cbo_max_reorder_node" => Self::ReorderMax(parse_value(value, name)?),
            "enable_query_rewrite_table_prune" => Self::QueryRewriteTablePrune(parse_bool(value)?),
            "enable_cbo_table_prune" => Self::CboTablePrune(parse_bool(value)?),
            "enable_table_prune_on_update" => Self::TablePruneOnUpdate(parse_bool(value)?),
            "enable_common_subexpr_reuse" => Self::CommonSubexprReuse(parse_bool(value)?),
            "enable_global_runtime_filter" => Self::GlobalRuntimeFilter(parse_bool(value)?),
            "enable_materialized_view_rewrite" => Self::MaterializedViewRewrite(parse_bool(value)?),
            "enable_connector_static_predicate_pushdown" => {
                Self::ConnectorStaticPredicatePushdown(parse_bool(value)?)
            }
            "cbo_enable_dp_join_reorder" => Self::DpJoinReorder(parse_bool(value)?),
            "cbo_enable_greedy_join_reorder" => Self::GreedyJoinReorder(parse_bool(value)?),
            "enable_global_runtime_filter_cross_exchange" => {
                Self::CrossExchangeRuntimeFilter(parse_bool(value)?)
            }
            _ => return Ok(None),
        }))
    }
    /// Canonical writes retire only obsolete client spellings, not SQL values.
    pub(crate) fn obsolete_aliases(&self) -> &'static [&'static str] {
        match self {
            Self::DisabledRules(_) => &["cbo_disabled_rules"],
            _ => &[],
        }
    }
    pub(crate) fn name(&self) -> &'static str {
        match self {
            Self::Catalog(..) => "catalog",
            Self::SqlMode(..) => "sql_mode",
            Self::Autocommit(..) => "autocommit",
            Self::DecimalOverflowToDouble(..) => "decimal_overflow_to_double",
            Self::QueryTimeout(..) => "query_timeout",
            Self::GroupConcatMaxLen(..) => "group_concat_max_len",
            Self::PipelineDop(..) => "pipeline_dop",
            Self::RuntimeFilterScanWait(..) => "runtime_filter_scan_wait_time",
            Self::RuntimeFilterWaitTimeout(..) => "global_runtime_filter_wait_timeout",
            Self::ParquetPageIndex(..) => "enable_parquet_reader_page_index",
            Self::ScanDatacache(..) => "enable_scan_datacache",
            Self::PopulateDatacache(..) => "enable_populate_datacache",
            Self::DisabledRules(..) => "disable_optimizer_rules",
            Self::EliminateAgg(..) => "enable_eliminate_agg",
            Self::UkfkOpt(..) => "enable_ukfk_opt",
            Self::BroadcastBackendCount(..) => "cbo_broadcast_backend_count",
            Self::BroadcastNodeMemory(..) => "cbo_broadcast_node_mem_budget_bytes",
            Self::RuntimeFilterBuildMax(..) => "global_runtime_filter_build_max_size",
            Self::RuntimeFilterBuildMin(..) => "global_runtime_filter_build_min_size",
            Self::RuntimeFilterProbeMin(..) => "global_runtime_filter_probe_min_size",
            Self::RuntimeFilterSelectivity(..) => "global_runtime_filter_probe_min_selectivity",
            Self::ReorderExhaustive(..) => "cbo_max_reorder_node_use_exhaustive",
            Self::ReorderDp(..) => "cbo_max_reorder_node_use_dp",
            Self::ReorderGreedy(..) => "cbo_max_reorder_node_use_greedy",
            Self::ReorderMax(..) => "cbo_max_reorder_node",
            Self::QueryRewriteTablePrune(..) => "enable_query_rewrite_table_prune",
            Self::CboTablePrune(..) => "enable_cbo_table_prune",
            Self::TablePruneOnUpdate(..) => "enable_table_prune_on_update",
            Self::CommonSubexprReuse(..) => "enable_common_subexpr_reuse",
            Self::GlobalRuntimeFilter(..) => "enable_global_runtime_filter",
            Self::MaterializedViewRewrite(..) => "enable_materialized_view_rewrite",
            Self::ConnectorStaticPredicatePushdown(..) => {
                "enable_connector_static_predicate_pushdown"
            }
            Self::DpJoinReorder(..) => "cbo_enable_dp_join_reorder",
            Self::GreedyJoinReorder(..) => "cbo_enable_greedy_join_reorder",
            Self::CrossExchangeRuntimeFilter(..) => "enable_global_runtime_filter_cross_exchange",
        }
    }
    pub(crate) fn render(&self) -> String {
        match self {
            Self::Catalog(value) => value
                .as_deref()
                .unwrap_or(novarocks_types::naming::DEFAULT_CATALOG)
                .to_owned(),
            Self::SqlMode(value) => value.assignment().to_owned(),
            Self::Autocommit(value) => value.to_string(),
            Self::DecimalOverflowToDouble(value) => value.to_string(),
            Self::QueryTimeout(value) => value.to_string(),
            Self::GroupConcatMaxLen(value) => value.to_string(),
            Self::PipelineDop(value) => {
                if *value <= 0 {
                    "0".into()
                } else {
                    value.to_string()
                }
            }
            Self::RuntimeFilterScanWait(value) => value.to_string(),
            Self::RuntimeFilterWaitTimeout(value) => value.to_string(),
            Self::ParquetPageIndex(value) => value.to_string(),
            Self::ScanDatacache(value) => value.to_string(),
            Self::PopulateDatacache(value) => value.to_string(),
            Self::DisabledRules(value) => value.join(","),
            Self::EliminateAgg(value) => value.to_string(),
            Self::UkfkOpt(value) => value.to_string(),
            Self::BroadcastBackendCount(value) => value.to_string(),
            Self::BroadcastNodeMemory(value) => value.to_string(),
            Self::RuntimeFilterBuildMax(value) => value.to_string(),
            Self::RuntimeFilterBuildMin(value) => value.to_string(),
            Self::RuntimeFilterProbeMin(value) => value.to_string(),
            Self::RuntimeFilterSelectivity(value) => value.to_string(),
            Self::ReorderExhaustive(value) => value.to_string(),
            Self::ReorderDp(value) => value.to_string(),
            Self::ReorderGreedy(value) => value.to_string(),
            Self::ReorderMax(value) => value.to_string(),
            Self::QueryRewriteTablePrune(value) => value.to_string(),
            Self::CboTablePrune(value) => value.to_string(),
            Self::TablePruneOnUpdate(value) => value.to_string(),
            Self::CommonSubexprReuse(value) => value.to_string(),
            Self::GlobalRuntimeFilter(value) => value.to_string(),
            Self::MaterializedViewRewrite(value) => value.to_string(),
            Self::ConnectorStaticPredicatePushdown(value) => value.to_string(),
            Self::DpJoinReorder(value) => value.to_string(),
            Self::GreedyJoinReorder(value) => value.to_string(),
            Self::CrossExchangeRuntimeFilter(value) => value.to_string(),
        }
    }
    pub(super) fn apply(&self, state: &mut SessionSqlState) -> Result<(), QueryServiceError> {
        match self {
            Self::Catalog(value) => {
                state.current_catalog = value.clone();
            }
            Self::SqlMode(value) => {
                state.sql_semantics = state.sql_semantics.clone().with_sql_mode(value.clone());
            }
            Self::Autocommit(value) => {
                debug_assert!(*value);
            }
            Self::DecimalOverflowToDouble(value) => {
                state.sql_semantics = state
                    .sql_semantics
                    .clone()
                    .with_decimal_overflow_to_double(*value);
            }
            Self::QueryTimeout(value) => {
                state.execution_settings.set_query_timeout_secs(*value);
            }
            Self::GroupConcatMaxLen(value) => {
                state.execution_settings.set_group_concat_max_len(*value);
            }
            Self::PipelineDop(value) => {
                state.execution_settings.set_pipeline_dop(*value);
            }
            Self::RuntimeFilterScanWait(value) => {
                state
                    .execution_settings
                    .set_runtime_filter_scan_wait_time_ms(*value)
                    .map_err(session_setting_error)?;
            }
            Self::RuntimeFilterWaitTimeout(value) => {
                state
                    .execution_settings
                    .set_runtime_filter_wait_timeout_ms(*value)
                    .map_err(session_setting_error)?;
            }
            Self::ParquetPageIndex(value) => {
                state
                    .execution_settings
                    .set_enable_parquet_reader_page_index(*value);
            }
            Self::ScanDatacache(value) => {
                state.execution_settings.set_enable_scan_datacache(*value);
            }
            Self::PopulateDatacache(value) => {
                state
                    .execution_settings
                    .set_enable_populate_datacache(*value);
            }
            Self::DisabledRules(value) => {
                state.optimizer_settings.set_disabled_rules(value.clone());
            }
            Self::EliminateAgg(value) => {
                state.optimizer_settings.set_enable_eliminate_agg(*value);
            }
            Self::UkfkOpt(value) => {
                state.optimizer_settings.set_enable_ukfk_opt(*value);
            }
            Self::BroadcastBackendCount(value) => {
                state.optimizer_settings.set_broadcast_backend_count(*value);
            }
            Self::BroadcastNodeMemory(value) => {
                state.optimizer_settings.cbo_broadcast_node_mem_budget_bytes = Some(*value);
            }
            Self::RuntimeFilterBuildMax(value) => {
                state.optimizer_settings.rf_build_max_bytes = Some(*value);
            }
            Self::RuntimeFilterBuildMin(value) => {
                state.optimizer_settings.rf_build_min_bytes = Some(*value);
            }
            Self::RuntimeFilterProbeMin(value) => {
                state.optimizer_settings.rf_probe_min_bytes = Some(*value);
            }
            Self::RuntimeFilterSelectivity(value) => {
                state.optimizer_settings.rf_probe_min_selectivity = Some(*value);
            }
            Self::ReorderExhaustive(value) => {
                state.optimizer_settings.max_reorder_node_use_exhaustive = Some(*value);
            }
            Self::ReorderDp(value) => {
                state.optimizer_settings.max_reorder_node_use_dp = Some(*value);
            }
            Self::ReorderGreedy(value) => {
                state.optimizer_settings.max_reorder_node_use_greedy = Some(*value);
            }
            Self::ReorderMax(value) => {
                state.optimizer_settings.max_reorder_node = Some(*value);
            }
            Self::QueryRewriteTablePrune(value) => {
                state.optimizer_settings.enable_query_rewrite_table_prune = *value;
            }
            Self::CboTablePrune(value) => {
                state.optimizer_settings.enable_cbo_table_prune = *value;
            }
            Self::TablePruneOnUpdate(value) => {
                state.optimizer_settings.enable_table_prune_on_update = *value;
            }
            Self::CommonSubexprReuse(value) => {
                state.optimizer_settings.enable_common_subexpr_reuse = Some(*value);
            }
            Self::GlobalRuntimeFilter(value) => {
                state.optimizer_settings.enable_global_runtime_filter = Some(*value);
            }
            Self::MaterializedViewRewrite(value) => {
                state.optimizer_settings.enable_materialized_view_rewrite = Some(*value);
            }
            Self::ConnectorStaticPredicatePushdown(value) => {
                state
                    .optimizer_settings
                    .enable_connector_static_predicate_pushdown = Some(*value);
            }
            Self::DpJoinReorder(value) => {
                state.optimizer_settings.enable_dp_join_reorder = Some(*value);
            }
            Self::GreedyJoinReorder(value) => {
                state.optimizer_settings.enable_greedy_join_reorder = Some(*value);
            }
            Self::CrossExchangeRuntimeFilter(value) => {
                state.optimizer_settings.allow_cross_exchange_rf = Some(*value);
            }
        }
        Ok(())
    }
    pub(super) fn read(name: &str, state: &SessionSqlState) -> Option<Self> {
        Some(match name {
            "catalog" => Self::Catalog(state.current_catalog.clone()),
            "sql_mode" => Self::SqlMode(state.sql_semantics.sql_mode().clone()),
            "autocommit" => Self::Autocommit(true),
            "decimal_overflow_to_double" => {
                Self::DecimalOverflowToDouble(state.sql_semantics.decimal_overflow_to_double())
            }
            "query_timeout" => {
                Self::QueryTimeout(state.execution_settings.query_timeout_secs().unwrap_or(0))
            }
            "group_concat_max_len" => {
                Self::GroupConcatMaxLen(state.execution_settings.group_concat_max_len())
            }
            "pipeline_dop" => {
                Self::PipelineDop(state.execution_settings.pipeline_dop().unwrap_or(0))
            }
            "runtime_filter_scan_wait_time" => Self::RuntimeFilterScanWait(
                state
                    .execution_settings
                    .runtime_filter_scan_wait_time_ms()?,
            ),
            "global_runtime_filter_wait_timeout" => Self::RuntimeFilterWaitTimeout(
                state.execution_settings.runtime_filter_wait_timeout_ms()?,
            ),
            "enable_parquet_reader_page_index" => {
                Self::ParquetPageIndex(state.execution_settings.enable_parquet_reader_page_index())
            }
            "enable_scan_datacache" => {
                Self::ScanDatacache(state.execution_settings.enable_scan_datacache())
            }
            "enable_populate_datacache" => {
                Self::PopulateDatacache(state.execution_settings.enable_populate_datacache())
            }
            "disable_optimizer_rules" | "cbo_disabled_rules" => {
                Self::DisabledRules(state.optimizer_settings.disabled_rules.clone())
            }
            "enable_eliminate_agg" => {
                Self::EliminateAgg(state.optimizer_settings.enable_eliminate_agg)
            }
            "enable_ukfk_opt" => Self::UkfkOpt(state.optimizer_settings.enable_ukfk_opt),
            "cbo_broadcast_backend_count" => {
                Self::BroadcastBackendCount(state.optimizer_settings.cbo_broadcast_backend_count?)
            }
            "cbo_broadcast_node_mem_budget_bytes" => Self::BroadcastNodeMemory(
                state
                    .optimizer_settings
                    .cbo_broadcast_node_mem_budget_bytes?,
            ),
            "global_runtime_filter_build_max_size" => {
                Self::RuntimeFilterBuildMax(state.optimizer_settings.rf_build_max_bytes?)
            }
            "global_runtime_filter_build_min_size" => {
                Self::RuntimeFilterBuildMin(state.optimizer_settings.rf_build_min_bytes?)
            }
            "global_runtime_filter_probe_min_size" => {
                Self::RuntimeFilterProbeMin(state.optimizer_settings.rf_probe_min_bytes?)
            }
            "global_runtime_filter_probe_min_selectivity" => {
                Self::RuntimeFilterSelectivity(state.optimizer_settings.rf_probe_min_selectivity?)
            }
            "cbo_max_reorder_node_use_exhaustive" => {
                Self::ReorderExhaustive(state.optimizer_settings.max_reorder_node_use_exhaustive?)
            }
            "cbo_max_reorder_node_use_dp" => {
                Self::ReorderDp(state.optimizer_settings.max_reorder_node_use_dp?)
            }
            "cbo_max_reorder_node_use_greedy" => {
                Self::ReorderGreedy(state.optimizer_settings.max_reorder_node_use_greedy?)
            }
            "cbo_max_reorder_node" => Self::ReorderMax(state.optimizer_settings.max_reorder_node?),
            "enable_query_rewrite_table_prune" => Self::QueryRewriteTablePrune(
                state.optimizer_settings.enable_query_rewrite_table_prune,
            ),
            "enable_cbo_table_prune" => {
                Self::CboTablePrune(state.optimizer_settings.enable_cbo_table_prune)
            }
            "enable_table_prune_on_update" => {
                Self::TablePruneOnUpdate(state.optimizer_settings.enable_table_prune_on_update)
            }
            "enable_common_subexpr_reuse" => {
                Self::CommonSubexprReuse(state.optimizer_settings.enable_common_subexpr_reuse?)
            }
            "enable_global_runtime_filter" => {
                Self::GlobalRuntimeFilter(state.optimizer_settings.enable_global_runtime_filter?)
            }
            "enable_materialized_view_rewrite" => Self::MaterializedViewRewrite(
                state.optimizer_settings.enable_materialized_view_rewrite?,
            ),
            "enable_connector_static_predicate_pushdown" => Self::ConnectorStaticPredicatePushdown(
                state
                    .optimizer_settings
                    .enable_connector_static_predicate_pushdown?,
            ),
            "cbo_enable_dp_join_reorder" => {
                Self::DpJoinReorder(state.optimizer_settings.enable_dp_join_reorder?)
            }
            "cbo_enable_greedy_join_reorder" => {
                Self::GreedyJoinReorder(state.optimizer_settings.enable_greedy_join_reorder?)
            }
            "enable_global_runtime_filter_cross_exchange" => {
                Self::CrossExchangeRuntimeFilter(state.optimizer_settings.allow_cross_exchange_rf?)
            }
            _ => return None,
        })
    }
}

#[cfg(test)]
mod tests;
