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

//! Each public system name occurs once, beside its type, default and projection.
use super::super::SessionExecutionSettings;
use super::*;
use novarocks_sql::{
    compiler::SessionOptimizerSettings,
    sql_mode::{DEFAULT_RECURSIVE_CTE_MAX_DEPTH, SqlMode, SqlSemanticSettings},
};

#[derive(Clone, Copy)]
enum Constraint {
    Any,
    NonNegative,
    Positive,
    PlatformCount,
}
fn check(constraint: Constraint, value: &SessionPropertyValue) -> Result<(), String> {
    use SessionPropertyValue as V;
    let accepted = match constraint {
        Constraint::Any => true,
        Constraint::NonNegative => match value {
            V::Integer(v) => *v >= 0,
            V::Bigint(v) => *v >= 0,
            _ => false,
        },
        Constraint::Positive => match value {
            V::Integer(v) => *v > 0,
            V::Bigint(v) => *v > 0,
            _ => false,
        },
        Constraint::PlatformCount => matches!(value, V::Bigint(v) if usize::try_from(*v).is_ok()),
    };
    if accepted {
        Ok(())
    } else {
        Err(match constraint {
            Constraint::Positive => "Value must be positive",
            Constraint::PlatformCount => "Value must be a non-negative platform count",
            _ => "Value must be non-negative",
        }
        .into())
    }
}
fn bigint_u64(value: Option<u64>) -> Result<Option<SessionPropertyValue>, SessionPropertyError> {
    value
        .map(|v| {
            i64::try_from(v)
                .map(SessionPropertyValue::Bigint)
                .map_err(|_| SessionPropertyError::InvalidDeclaration {
                    detail: "Default exceeds SQL bigint",
                })
        })
        .transpose()
}
fn bigint_usize(
    value: Option<usize>,
) -> Result<Option<SessionPropertyValue>, SessionPropertyError> {
    value
        .map(|v| {
            i64::try_from(v)
                .map(SessionPropertyValue::Bigint)
                .map_err(|_| SessionPropertyError::InvalidDeclaration {
                    detail: "Default exceeds SQL bigint",
                })
        })
        .transpose()
}

pub(super) fn system_declarations() -> Result<Vec<PropertyDeclaration>, SessionPropertyError> {
    use SessionPropertyValue as V;
    let execution = SessionExecutionSettings::default();
    let semantics = SqlSemanticSettings::default();
    // Metadata only: never install these defaults as explicit overrides.
    let optimizer = SessionOptimizerSettings::declaration_defaults();
    let mut values = Vec::new();
    macro_rules! declaration {
        ($name:literal, $kind:ident, $default:expr, $description:literal, $constraint:ident, |$state:ident, $value:ident| $project:block) => {{
            values.push(PropertyDeclaration {
                name: $name.into(), value_type: SessionPropertyType::$kind,
                default: $default, description: $description.into(), hidden: false,
                validate: Arc::new(|value| check(Constraint::$constraint, value)),
                project: Some(|$state, typed| {
                    let V::$kind($value) = typed else { return Err(SessionPropertyError::ProjectionInvariant); };
                    $project
                    Ok(())
                }),
            });
        }};
    }
    declaration!(
        "sql_mode",
        Varchar,
        Some(V::Varchar(semantics.sql_mode().assignment().into())),
        "SQL semantic modes",
        Any,
        |state, value| {
            state.sql_semantics = state
                .sql_semantics
                .clone()
                .with_sql_mode(SqlMode::from_assignment(value));
        }
    );
    declaration!(
        "decimal_overflow_to_double",
        Boolean,
        Some(V::Boolean(semantics.decimal_overflow_to_double())),
        "Promote decimal overflow to double",
        Any,
        |state, value| {
            state.sql_semantics = state
                .sql_semantics
                .clone()
                .with_decimal_overflow_to_double(*value);
        }
    );
    declaration!(
        "query_timeout",
        Bigint,
        bigint_u64(execution.query_timeout_secs())?,
        "Query timeout in seconds; zero adds no session deadline",
        NonNegative,
        |state, value| {
            state.execution_settings.set_query_timeout_secs(
                u64::try_from(*value).map_err(|_| SessionPropertyError::ProjectionInvariant)?,
            );
        }
    );
    declaration!(
        "group_concat_max_len",
        Bigint,
        Some(V::Bigint(execution.group_concat_max_len())),
        "Maximum GROUP_CONCAT result length",
        Any,
        |state, value| {
            state.execution_settings.set_group_concat_max_len(*value);
        }
    );
    declaration!(
        "pipeline_dop",
        Integer,
        execution.pipeline_dop().map(V::Integer),
        "Pipeline parallelism; non-positive values select automatic parallelism",
        Any,
        |state, value| {
            state.execution_settings.set_pipeline_dop(*value);
        }
    );
    declaration!(
        "runtime_filter_scan_wait_time",
        Bigint,
        execution.runtime_filter_scan_wait_time_ms().map(V::Bigint),
        "Runtime filter scan wait in milliseconds",
        NonNegative,
        |state, value| {
            state
                .execution_settings
                .set_runtime_filter_scan_wait_time_ms(*value)
                .map_err(SessionPropertyError::SettingProjection)?;
        }
    );
    declaration!(
        "global_runtime_filter_wait_timeout",
        Integer,
        execution.runtime_filter_wait_timeout_ms().map(V::Integer),
        "Global runtime filter wait in milliseconds",
        NonNegative,
        |state, value| {
            state
                .execution_settings
                .set_runtime_filter_wait_timeout_ms(*value)
                .map_err(SessionPropertyError::SettingProjection)?;
        }
    );
    declaration!(
        "enable_parquet_reader_page_index",
        Boolean,
        Some(V::Boolean(execution.enable_parquet_reader_page_index())),
        "Use Parquet page indexes",
        Any,
        |state, value| {
            state
                .execution_settings
                .set_enable_parquet_reader_page_index(*value);
        }
    );
    declaration!(
        "enable_scan_datacache",
        Boolean,
        Some(V::Boolean(execution.enable_scan_datacache())),
        "Read from the scan data cache",
        Any,
        |state, value| {
            state.execution_settings.set_enable_scan_datacache(*value);
        }
    );
    declaration!(
        "enable_populate_datacache",
        Boolean,
        Some(V::Boolean(execution.enable_populate_datacache())),
        "Populate the scan data cache",
        Any,
        |state, value| {
            state
                .execution_settings
                .set_enable_populate_datacache(*value);
        }
    );
    declaration!(
        "disable_optimizer_rules",
        Varchar,
        Some(V::Varchar(optimizer.disabled_rules.join(","))),
        "Comma-separated optimizer rules to disable",
        Any,
        |state, value| {
            state.optimizer_settings.set_disabled_rules(
                value
                    .split(',')
                    .map(str::trim)
                    .filter(|rule| !rule.is_empty())
                    .map(str::to_owned)
                    .collect(),
            );
        }
    );
    declaration!(
        "enable_eliminate_agg",
        Boolean,
        Some(V::Boolean(optimizer.enable_eliminate_agg)),
        "Eliminate redundant aggregation",
        Any,
        |state, value| {
            state.optimizer_settings.set_enable_eliminate_agg(*value);
        }
    );
    declaration!(
        "enable_ukfk_opt",
        Boolean,
        Some(V::Boolean(optimizer.enable_ukfk_opt)),
        "Use unique and foreign key optimization",
        Any,
        |state, value| {
            state.optimizer_settings.set_enable_ukfk_opt(*value);
        }
    );
    declaration!(
        "cbo_broadcast_backend_count",
        Double,
        optimizer.cbo_broadcast_backend_count.map(V::Double),
        "Override broadcast backend count; absent uses the frozen topology",
        Any,
        |state, value| {
            state.optimizer_settings.set_broadcast_backend_count(*value);
        }
    );
    declaration!(
        "cbo_broadcast_node_mem_budget_bytes",
        Double,
        optimizer.cbo_broadcast_node_mem_budget_bytes.map(V::Double),
        "Override per-node broadcast memory; absent uses the original resource profile",
        Any,
        |state, value| {
            state.optimizer_settings.cbo_broadcast_node_mem_budget_bytes = Some(*value);
        }
    );
    declaration!(
        "global_runtime_filter_build_max_size",
        Bigint,
        bigint_u64(optimizer.rf_build_max_bytes)?,
        "Runtime filter build maximum size in bytes",
        NonNegative,
        |state, value| {
            state.optimizer_settings.rf_build_max_bytes =
                Some(u64::try_from(*value).map_err(|_| SessionPropertyError::ProjectionInvariant)?);
        }
    );
    declaration!(
        "global_runtime_filter_build_min_size",
        Bigint,
        bigint_u64(optimizer.rf_build_min_bytes)?,
        "Runtime filter build minimum size in bytes",
        NonNegative,
        |state, value| {
            state.optimizer_settings.rf_build_min_bytes =
                Some(u64::try_from(*value).map_err(|_| SessionPropertyError::ProjectionInvariant)?);
        }
    );
    declaration!(
        "global_runtime_filter_probe_min_size",
        Bigint,
        bigint_u64(optimizer.rf_probe_min_bytes)?,
        "Runtime filter probe minimum size in bytes",
        NonNegative,
        |state, value| {
            state.optimizer_settings.rf_probe_min_bytes =
                Some(u64::try_from(*value).map_err(|_| SessionPropertyError::ProjectionInvariant)?);
        }
    );
    declaration!(
        "global_runtime_filter_probe_min_selectivity",
        Double,
        optimizer.rf_probe_min_selectivity.map(V::Double),
        "Runtime filter minimum probe selectivity",
        Any,
        |state, value| {
            state.optimizer_settings.rf_probe_min_selectivity = Some(*value);
        }
    );
    declaration!(
        "cbo_max_reorder_node_use_exhaustive",
        Bigint,
        bigint_usize(optimizer.max_reorder_node_use_exhaustive)?,
        "Maximum join count for exhaustive reordering",
        PlatformCount,
        |state, value| {
            state.optimizer_settings.max_reorder_node_use_exhaustive = Some(
                usize::try_from(*value).map_err(|_| SessionPropertyError::ProjectionInvariant)?,
            );
        }
    );
    declaration!(
        "cbo_max_reorder_node_use_dp",
        Bigint,
        bigint_usize(optimizer.max_reorder_node_use_dp)?,
        "Maximum join count for dynamic programming reordering",
        PlatformCount,
        |state, value| {
            state.optimizer_settings.max_reorder_node_use_dp = Some(
                usize::try_from(*value).map_err(|_| SessionPropertyError::ProjectionInvariant)?,
            );
        }
    );
    declaration!(
        "cbo_max_reorder_node_use_greedy",
        Bigint,
        bigint_usize(optimizer.max_reorder_node_use_greedy)?,
        "Maximum join count for greedy reordering",
        PlatformCount,
        |state, value| {
            state.optimizer_settings.max_reorder_node_use_greedy = Some(
                usize::try_from(*value).map_err(|_| SessionPropertyError::ProjectionInvariant)?,
            );
        }
    );
    declaration!(
        "cbo_max_reorder_node",
        Bigint,
        bigint_usize(optimizer.max_reorder_node)?,
        "Maximum join count to reorder",
        PlatformCount,
        |state, value| {
            state.optimizer_settings.max_reorder_node = Some(
                usize::try_from(*value).map_err(|_| SessionPropertyError::ProjectionInvariant)?,
            );
        }
    );
    declaration!(
        "enable_query_rewrite_table_prune",
        Boolean,
        Some(V::Boolean(optimizer.enable_query_rewrite_table_prune)),
        "Prune tables during query rewrite",
        Any,
        |state, value| {
            state.optimizer_settings.enable_query_rewrite_table_prune = *value;
        }
    );
    declaration!(
        "enable_cbo_table_prune",
        Boolean,
        Some(V::Boolean(optimizer.enable_cbo_table_prune)),
        "Prune tables during cost optimization",
        Any,
        |state, value| {
            state.optimizer_settings.enable_cbo_table_prune = *value;
        }
    );
    declaration!(
        "enable_table_prune_on_update",
        Boolean,
        Some(V::Boolean(optimizer.enable_table_prune_on_update)),
        "Prune tables during UPDATE planning",
        Any,
        |state, value| {
            state.optimizer_settings.enable_table_prune_on_update = *value;
        }
    );
    declaration!(
        "enable_common_subexpr_reuse",
        Boolean,
        optimizer.enable_common_subexpr_reuse.map(V::Boolean),
        "Reuse common subexpressions",
        Any,
        |state, value| {
            state.optimizer_settings.enable_common_subexpr_reuse = Some(*value);
        }
    );
    declaration!(
        "enable_global_runtime_filter",
        Boolean,
        optimizer.enable_global_runtime_filter.map(V::Boolean),
        "Enable global runtime filters",
        Any,
        |state, value| {
            state.optimizer_settings.enable_global_runtime_filter = Some(*value);
        }
    );
    declaration!(
        "enable_materialized_view_rewrite",
        Boolean,
        optimizer.enable_materialized_view_rewrite.map(V::Boolean),
        "Enable transparent materialized view rewrite",
        Any,
        |state, value| {
            state.optimizer_settings.enable_materialized_view_rewrite = Some(*value);
        }
    );
    declaration!(
        "enable_connector_static_predicate_pushdown",
        Boolean,
        optimizer
            .enable_connector_static_predicate_pushdown
            .map(V::Boolean),
        "Offer static predicates to connectors",
        Any,
        |state, value| {
            state
                .optimizer_settings
                .enable_connector_static_predicate_pushdown = Some(*value);
        }
    );
    declaration!(
        "cbo_enable_dp_join_reorder",
        Boolean,
        optimizer.enable_dp_join_reorder.map(V::Boolean),
        "Enable dynamic programming join reordering",
        Any,
        |state, value| {
            state.optimizer_settings.enable_dp_join_reorder = Some(*value);
        }
    );
    declaration!(
        "cbo_enable_greedy_join_reorder",
        Boolean,
        optimizer.enable_greedy_join_reorder.map(V::Boolean),
        "Enable greedy join reordering",
        Any,
        |state, value| {
            state.optimizer_settings.enable_greedy_join_reorder = Some(*value);
        }
    );
    declaration!(
        "enable_global_runtime_filter_cross_exchange",
        Boolean,
        optimizer.allow_cross_exchange_rf.map(V::Boolean),
        "Place runtime filters across exchange boundaries",
        Any,
        |state, value| {
            state.optimizer_settings.allow_cross_exchange_rf = Some(*value);
        }
    );
    declaration!(
        "query_mem_limit",
        Bigint,
        execution.query_mem_limit_bytes().map(V::Bigint),
        "Session query memory limit in bytes",
        Positive,
        |state, value| {
            state
                .execution_settings
                .set_query_mem_limit_bytes(*value)
                .map_err(SessionPropertyError::SettingProjection)?;
        }
    );
    declaration!(
        "recursive_cte_max_depth",
        Integer,
        Some(V::Integer(DEFAULT_RECURSIVE_CTE_MAX_DEPTH)),
        "Maximum recursive CTE expansion depth",
        Positive,
        |state, value| {
            state.sql_semantics = state
                .sql_semantics
                .clone()
                .with_recursive_cte_max_depth(*value);
        }
    );
    Ok(values)
}
