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

#[test]
fn row_statements_and_empty_input_do_not_publish_an_update_type() {
    use StatementKind::*;
    for kind in [
        Empty,
        Query,
        Explain,
        ShowBackends,
        ShowProcessList,
        ShowAnalyzeJobs,
        ShowTableStats,
        ShowBasicStatsMeta,
        ShowHistogramStatsMeta,
        ShowCreateTable,
        ShowOptimize,
        ShowMaterializedViews,
        ExplainMaterializedViewRefresh,
        ShowViews,
        ShowCreateView,
    ] {
        assert_eq!(update_type(kind), None, "{kind:?}");
    }
}

#[test]
fn task_names_match_trino_483_and_novarocks_extensions() {
    use StatementKind::*;
    // Trino execution/*Task.java getName() is the name oracle, rather than
    // NovaRocks parser spelling (notably SCHEMA, SET COLUMN TYPE and COMMENT).
    for (kind, expected) in [
        (Analyze, "ANALYZE"),
        (CreateCatalog, "CREATE CATALOG"),
        (DropCatalog, "DROP CATALOG"),
        (CreateDatabase, "CREATE SCHEMA"),
        (DropDatabase, "DROP SCHEMA"),
        (CreateTable, "CREATE TABLE"),
        (DropTable, "DROP TABLE"),
        (TruncateTable, "TRUNCATE TABLE"),
        (AddColumn, "ADD COLUMN"),
        (DropColumn, "DROP COLUMN"),
        (RenameColumn, "RENAME COLUMN"),
        (AlterColumnType, "SET COLUMN TYPE"),
        (DropColumnNotNull, "DROP NOT NULL"),
        (CommentColumn, "COMMENT"),
        (CommentTable, "COMMENT"),
        (SetTableProperties, "SET PROPERTIES"),
        (AlterMaterializedViewProperties, "SET PROPERTIES"),
        (CreateBranch, "CREATE BRANCH"),
        (DropBranch, "DROP BRANCH"),
        (CreateMaterializedView, "CREATE MATERIALIZED VIEW"),
        (DropMaterializedView, "DROP MATERIALIZED VIEW"),
        (RefreshMaterializedView, "REFRESH MATERIALIZED VIEW"),
        (CreateView, "CREATE VIEW"),
        (DropView, "DROP VIEW"),
        (Insert, "INSERT"),
        (Delete, "DELETE"),
        (Update, "UPDATE"),
        (Merge, "MERGE"),
        (SetSession, "SET SESSION"),
        (Use, "USE"),
        (BeginTransaction, "START TRANSACTION"),
        (Commit, "COMMIT"),
        (Rollback, "ROLLBACK"),
        (CancelAnalyze, "CANCEL ANALYZE"),
        (DropStats, "DROP STATS"),
        (DropHistogram, "DROP HISTOGRAM"),
        (DropMultipleColumnsStats, "DROP MULTIPLE COLUMNS STATS"),
        (ReorderColumn, "REORDER COLUMN"),
        (SetColumnNotNull, "SET NOT NULL"),
        (UnsetTableProperties, "UNSET PROPERTIES"),
        (AddPartitionField, "ADD PARTITION FIELD"),
        (DropPartitionField, "DROP PARTITION FIELD"),
        (CreateTag, "CREATE TAG"),
        (DropTag, "DROP TAG"),
        (AddFiles, "ADD FILES"),
        (Optimize, "OPTIMIZE"),
        (RewriteManifests, "REWRITE MANIFESTS"),
        (ExpireSnapshots, "EXPIRE SNAPSHOTS"),
        (RemoveOrphanFiles, "REMOVE ORPHAN FILES"),
        (
            AlterMaterializedViewRefresh,
            "ALTER MATERIALIZED VIEW REFRESH",
        ),
        (
            PauseMaterializedViewRefresh,
            "PAUSE MATERIALIZED VIEW REFRESH",
        ),
        (
            ResumeMaterializedViewRefresh,
            "RESUME MATERIALIZED VIEW REFRESH",
        ),
        (RepartitionMaterializedView, "REPARTITION MATERIALIZED VIEW"),
        (AddEqualityDelete, "ADD EQUALITY DELETE"),
        (KillQuery, "KILL QUERY"),
        (KillConnection, "KILL CONNECTION"),
        (Savepoint, "SAVEPOINT"),
    ] {
        assert_eq!(update_type(kind), Some(expected), "{kind:?}");
    }
}

#[test]
fn call_remains_a_candidate_until_the_actual_output_carrier_is_known() {
    assert_eq!(update_type(StatementKind::Call), Some("CALL"));
}
