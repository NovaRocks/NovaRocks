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
mod be_compaction_stats_store;
mod be_tablet_write_log_store;
mod be_txn_store;
mod chunk_builder;
mod context;
mod fe_tables;
mod frontend;
mod load_tracking_logs;
mod loads;
mod op;

pub(crate) use be_tablet_write_log_store::BeTabletWriteLoadLogRecord;
pub(crate) use be_txn_store::BeTxnActiveRecord;
pub(crate) use context::SchemaScanContext;
pub(crate) use op::SchemaScanOp;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum BeSchemaTable {
    TabletWriteLog,
    Txns,
    Compactions,
    CloudNativeCompactions,
    Configs,
    DatacacheMetrics,
    Logs,
    Tablets,
    Threads,
    Bvars,
    Metrics,
    Unsupported(String),
}

impl BeSchemaTable {
    pub(crate) fn from_table_name(table_name: &str) -> Option<Self> {
        let normalized = table_name.trim().to_ascii_lowercase();
        if !normalized.starts_with("be_") {
            return None;
        }
        Some(match normalized.as_str() {
            "be_tablet_write_log" => Self::TabletWriteLog,
            "be_txns" => Self::Txns,
            "be_compactions" => Self::Compactions,
            "be_cloud_native_compactions" => Self::CloudNativeCompactions,
            "be_configs" => Self::Configs,
            "be_datacache_metrics" => Self::DatacacheMetrics,
            "be_logs" => Self::Logs,
            "be_tablets" => Self::Tablets,
            "be_threads" => Self::Threads,
            "be_bvars" => Self::Bvars,
            "be_metrics" => Self::Metrics,
            _ => Self::Unsupported(normalized),
        })
    }

    pub(crate) fn table_name(&self) -> &str {
        match self {
            Self::TabletWriteLog => "be_tablet_write_log",
            Self::Txns => "be_txns",
            Self::Compactions => "be_compactions",
            Self::CloudNativeCompactions => "be_cloud_native_compactions",
            Self::Configs => "be_configs",
            Self::DatacacheMetrics => "be_datacache_metrics",
            Self::Logs => "be_logs",
            Self::Tablets => "be_tablets",
            Self::Threads => "be_threads",
            Self::Bvars => "be_bvars",
            Self::Metrics => "be_metrics",
            Self::Unsupported(name) => name.as_str(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SchemaTable {
    Loads,
    LoadTrackingLogs,
    AnalyzeStatus,
    CharacterSets,
    Collations,
    ColumnPrivileges,
    ColumnStatsUsage,
    Events,
    FeMetrics,
    Keywords,
    ApplicableRoles,
    Engines,
    FeThreads,
    FeTabletSchedules,
    GlobalVariables,
    KeyColumnUsage,
    PartitionsMeta,
    Partitions,
    PipeFiles,
    Pipes,
    ReferentialConstraints,
    RoutineLoadJobs,
    Routines,
    SessionVariables,
    Statistics,
    StreamLoads,
    TableConstraints,
    Tables,
    TablesConfig,
    TablePrivileges,
    Tasks,
    TempTables,
    Triggers,
    UserPrivileges,
    VerboseSessionVariables,
    Views,
    Columns,
    Schemata,
    MaterializedViews,
    TaskRuns,
    WarehouseMetrics,
    WarehouseQueries,
    TabletReshardJobs,
    ClusterSnapshots,
    ClusterSnapshotJobs,
    RecyclebinCatalogs,
    ObjectDependencies,
    FeLocks,
    FeMemoryUsage,
    SchemaPrivileges,
    GrantsToRoles,
    GrantsToUsers,
    RoleEdges,
    Be(BeSchemaTable),
}

impl SchemaTable {
    pub(crate) fn from_table_name(table_name: &str) -> Option<Self> {
        let normalized = table_name.trim().to_ascii_lowercase();
        match normalized.as_str() {
            "loads" => Some(Self::Loads),
            "load_tracking_logs" => Some(Self::LoadTrackingLogs),
            "analyze_status" => Some(Self::AnalyzeStatus),
            "character_sets" => Some(Self::CharacterSets),
            "collations" => Some(Self::Collations),
            "column_privileges" => Some(Self::ColumnPrivileges),
            "column_stats_usage" => Some(Self::ColumnStatsUsage),
            "events" => Some(Self::Events),
            "fe_metrics" => Some(Self::FeMetrics),
            "keywords" => Some(Self::Keywords),
            "applicable_roles" => Some(Self::ApplicableRoles),
            "engines" => Some(Self::Engines),
            "fe_threads" => Some(Self::FeThreads),
            "fe_tablet_schedules" => Some(Self::FeTabletSchedules),
            "global_variables" => Some(Self::GlobalVariables),
            "key_column_usage" => Some(Self::KeyColumnUsage),
            "partitions" => Some(Self::Partitions),
            "partitions_meta" => Some(Self::PartitionsMeta),
            "pipe_files" => Some(Self::PipeFiles),
            "pipes" => Some(Self::Pipes),
            "referential_constraints" => Some(Self::ReferentialConstraints),
            "routine_load_jobs" => Some(Self::RoutineLoadJobs),
            "routines" => Some(Self::Routines),
            "session_variables" => Some(Self::SessionVariables),
            "statistics" => Some(Self::Statistics),
            "stream_loads" => Some(Self::StreamLoads),
            "table_constraints" => Some(Self::TableConstraints),
            "tables" => Some(Self::Tables),
            "tables_config" => Some(Self::TablesConfig),
            "table_privileges" => Some(Self::TablePrivileges),
            "tasks" => Some(Self::Tasks),
            "temp_tables" => Some(Self::TempTables),
            "triggers" => Some(Self::Triggers),
            "user_privileges" => Some(Self::UserPrivileges),
            "verbose_session_variables" => Some(Self::VerboseSessionVariables),
            "views" => Some(Self::Views),
            "columns" => Some(Self::Columns),
            "schemata" => Some(Self::Schemata),
            "materialized_views" => Some(Self::MaterializedViews),
            "task_runs" => Some(Self::TaskRuns),
            "warehouse_metrics" => Some(Self::WarehouseMetrics),
            "warehouse_queries" => Some(Self::WarehouseQueries),
            "tablet_reshard_jobs" => Some(Self::TabletReshardJobs),
            "cluster_snapshots" => Some(Self::ClusterSnapshots),
            "cluster_snapshot_jobs" => Some(Self::ClusterSnapshotJobs),
            "recyclebin_catalogs" => Some(Self::RecyclebinCatalogs),
            "object_dependencies" => Some(Self::ObjectDependencies),
            "fe_locks" => Some(Self::FeLocks),
            "fe_memory_usage" => Some(Self::FeMemoryUsage),
            "schema_privileges" => Some(Self::SchemaPrivileges),
            "grants_to_roles" => Some(Self::GrantsToRoles),
            "grants_to_users" => Some(Self::GrantsToUsers),
            "role_edges" => Some(Self::RoleEdges),
            _ => BeSchemaTable::from_table_name(&normalized).map(Self::Be),
        }
    }

    pub(crate) fn table_name(&self) -> &str {
        match self {
            Self::Loads => "loads",
            Self::LoadTrackingLogs => "load_tracking_logs",
            Self::AnalyzeStatus => "analyze_status",
            Self::CharacterSets => "character_sets",
            Self::Collations => "collations",
            Self::ColumnPrivileges => "column_privileges",
            Self::ColumnStatsUsage => "column_stats_usage",
            Self::Events => "events",
            Self::FeMetrics => "fe_metrics",
            Self::Keywords => "keywords",
            Self::ApplicableRoles => "applicable_roles",
            Self::Engines => "engines",
            Self::FeThreads => "fe_threads",
            Self::FeTabletSchedules => "fe_tablet_schedules",
            Self::GlobalVariables => "global_variables",
            Self::KeyColumnUsage => "key_column_usage",
            Self::Partitions => "partitions",
            Self::PartitionsMeta => "partitions_meta",
            Self::PipeFiles => "pipe_files",
            Self::Pipes => "pipes",
            Self::ReferentialConstraints => "referential_constraints",
            Self::RoutineLoadJobs => "routine_load_jobs",
            Self::Routines => "routines",
            Self::SessionVariables => "session_variables",
            Self::Statistics => "statistics",
            Self::StreamLoads => "stream_loads",
            Self::TableConstraints => "table_constraints",
            Self::Tables => "tables",
            Self::TablesConfig => "tables_config",
            Self::TablePrivileges => "table_privileges",
            Self::Tasks => "tasks",
            Self::TempTables => "temp_tables",
            Self::Triggers => "triggers",
            Self::UserPrivileges => "user_privileges",
            Self::VerboseSessionVariables => "verbose_session_variables",
            Self::Views => "views",
            Self::Columns => "columns",
            Self::Schemata => "schemata",
            Self::MaterializedViews => "materialized_views",
            Self::TaskRuns => "task_runs",
            Self::WarehouseMetrics => "warehouse_metrics",
            Self::WarehouseQueries => "warehouse_queries",
            Self::TabletReshardJobs => "tablet_reshard_jobs",
            Self::ClusterSnapshots => "cluster_snapshots",
            Self::ClusterSnapshotJobs => "cluster_snapshot_jobs",
            Self::RecyclebinCatalogs => "recyclebin_catalogs",
            Self::ObjectDependencies => "object_dependencies",
            Self::FeLocks => "fe_locks",
            Self::FeMemoryUsage => "fe_memory_usage",
            Self::SchemaPrivileges => "schema_privileges",
            Self::GrantsToRoles => "grants_to_roles",
            Self::GrantsToUsers => "grants_to_users",
            Self::RoleEdges => "role_edges",
            Self::Be(table) => table.table_name(),
        }
    }
}

pub(crate) fn record_tablet_write_load_log(record: BeTabletWriteLoadLogRecord) {
    be_tablet_write_log_store::record_load(record);
}

pub(crate) fn record_be_txn_active(record: BeTxnActiveRecord) {
    be_txn_store::record_active(record);
}

pub(crate) fn mark_be_txn_published(txn_id: i64, tablet_id: i64, publish_time: i64, version: i64) {
    be_txn_store::mark_published(txn_id, tablet_id, publish_time, version);
}

pub(crate) fn abort_be_txn_active(txn_id: i64, tablet_id: i64) {
    be_txn_store::abort_active(txn_id, tablet_id);
}

#[cfg(test)]
pub(crate) fn test_lock() -> &'static std::sync::Mutex<()> {
    use std::sync::{Mutex, OnceLock};
    static TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    TEST_LOCK.get_or_init(|| Mutex::new(()))
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn clear_for_test() {
    be_tablet_write_log_store::clear_for_test();
    be_txn_store::clear_for_test();
    be_compaction_stats_store::clear_for_test();
}
