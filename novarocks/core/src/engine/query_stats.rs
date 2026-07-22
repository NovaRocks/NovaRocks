#![allow(dead_code)]
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

use std::sync::Arc;

use crate::connector::stats::{
    ScanSourceIdentity, TableSnapshotRef, TableStatsProvider, TableStatsRequest,
};
use crate::sql::optimizer::operator::Operator;
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::stats_input::{
    BaseTableStatistics, QueryStatsSnapshot, StatsMissingReason, StatsRef,
};
use crate::sql::planner::table::ScanSource;

#[derive(Clone, Default)]
pub(crate) struct QueryStatsProviders {
    iceberg: Option<Arc<dyn TableStatsProvider>>,
    standalone_state: Option<Arc<super::StandaloneState>>,
}

impl QueryStatsProviders {
    pub(crate) fn none() -> Self {
        Self::default()
    }

    pub(crate) fn from_connectors(connectors: &crate::connector::ConnectorRegistry) -> Self {
        let iceberg = connectors
            .table_source("iceberg")
            .ok()
            .and_then(|source| source.stats_provider());
        Self {
            iceberg,
            standalone_state: None,
        }
    }

    pub(crate) fn from_standalone_state(state: &Arc<super::StandaloneState>) -> Self {
        let connectors = state
            .connectors
            .read()
            .expect("standalone connectors read lock");
        let mut providers = Self::from_connectors(&connectors);
        providers.standalone_state = Some(Arc::clone(state));
        providers
    }

    pub(crate) fn from_optional_state(state: Option<&Arc<super::StandaloneState>>) -> Self {
        state
            .map(Self::from_standalone_state)
            .unwrap_or_else(Self::none)
    }
}

pub(crate) struct QueryStatsPlan {
    pub snapshot: QueryStatsSnapshot,
    next_stats_ref: u32,
}

impl QueryStatsPlan {
    fn new(snapshot: QueryStatsSnapshot, next_stats_ref: u32) -> Self {
        Self {
            snapshot,
            next_stats_ref,
        }
    }

    pub(crate) fn add_stats(
        &mut self,
        label: impl Into<String>,
        stats: BaseTableStatistics,
    ) -> StatsRef {
        let stats_ref = StatsRef::new(self.next_stats_ref);
        self.next_stats_ref += 1;
        self.snapshot.insert(stats_ref, label, stats);
        stats_ref
    }
}

pub(crate) struct QueryStatsCollector {
    providers: QueryStatsProviders,
    next_stats_ref: u32,
    snapshot: QueryStatsSnapshot,
}

impl QueryStatsCollector {
    pub(crate) fn new(providers: QueryStatsProviders) -> Self {
        Self {
            providers,
            next_stats_ref: 0,
            snapshot: QueryStatsSnapshot::empty(),
        }
    }

    pub(crate) fn collect(mut self, opt_expr: &mut OptExpr) -> QueryStatsPlan {
        self.walk(opt_expr);
        QueryStatsPlan::new(self.snapshot, self.next_stats_ref)
    }

    fn walk(&mut self, expr: &mut OptExpr) {
        if let Operator::LogicalScan(scan) = &mut expr.op {
            let stats_ref = StatsRef::new(self.next_stats_ref);
            self.next_stats_ref += 1;
            scan.stats_ref = Some(stats_ref);

            let (label, stats) = self.collect_scan(scan);
            self.snapshot.insert(stats_ref, label, stats);
        }

        for child in &mut expr.children {
            self.walk(child);
        }
    }

    fn collect_scan(
        &self,
        scan: &crate::sql::optimizer::operator::ScanOp,
    ) -> (String, BaseTableStatistics) {
        collect_table_stats(&self.providers, &scan.database, &scan.table)
    }
}

pub(super) fn collect_table_stats(
    providers: &QueryStatsProviders,
    database: &str,
    table_def: &crate::sql::planner::table::TableDef,
) -> (String, BaseTableStatistics) {
    let label = table_label(database, table_def);
    if let Some(stats) = collect_standalone_catalog_stats(providers, database, table_def, &label) {
        return (label, stats);
    }
    let Some(request) = table_stats_request(database, table_def) else {
        return (
            label,
            BaseTableStatistics::missing(StatsMissingReason::ConnectorUnsupported(
                "scan source does not expose query stats".to_string(),
            )),
        );
    };

    let stats = match &request.source {
        ScanSourceIdentity::IcebergTable { .. } => {
            let Some(provider) = providers.iceberg.as_deref() else {
                return (
                    label,
                    BaseTableStatistics::missing(StatsMissingReason::ConnectorUnsupported(
                        "iceberg stats provider is not registered".to_string(),
                    )),
                );
            };
            provider
                .estimate_table_statistics(&request)
                .unwrap_or_else(|err| BaseTableStatistics::missing(err.into_missing_reason()))
        }
        ScanSourceIdentity::Unsupported { reason } => {
            BaseTableStatistics::missing(StatsMissingReason::ConnectorUnsupported(reason.clone()))
        }
    };

    (label, stats)
}

fn collect_standalone_catalog_stats(
    providers: &QueryStatsProviders,
    database: &str,
    table_def: &crate::sql::planner::table::TableDef,
    label: &str,
) -> Option<BaseTableStatistics> {
    if matches!(
        table_def.source,
        ScanSource::IcebergDataFiles { .. }
            | ScanSource::IcebergMetadataTable { .. }
            | ScanSource::IcebergVersionTable { .. }
            | ScanSource::IcebergDeltaTable { .. }
            | ScanSource::IcebergMvTargetState(_)
    ) {
        return None;
    }
    let state = providers.standalone_state.as_ref()?;
    match crate::engine::statistics::catalog_base_table_statistics(
        state,
        database,
        &table_def.name,
        &table_def.columns,
        standalone_stats_source(&table_def.source),
    ) {
        Ok(Some(stats)) => Some(stats),
        Ok(None) => None,
        Err(err) => Some(BaseTableStatistics::missing(
            StatsMissingReason::CatalogLoadError(format!("{label}: {err}")),
        )),
    }
}

fn standalone_stats_source(source: &ScanSource) -> crate::sql::optimizer::stats_input::StatsSource {
    match source {
        ScanSource::StarRocks { .. } => {
            crate::sql::optimizer::stats_input::StatsSource::StarRocksTableMetadata
        }
        _ => crate::sql::optimizer::stats_input::StatsSource::ManagedLakeMetadata,
    }
}

fn table_label(database: &str, table_def: &crate::sql::planner::table::TableDef) -> String {
    match &table_def.source {
        ScanSource::IcebergDataFiles { table, .. }
        | ScanSource::IcebergVersionTable { table, .. }
        | ScanSource::IcebergDeltaTable { table, .. } => {
            format!("{}.{}.{}", table.catalog, table.namespace, table.table)
        }
        _ => format!("{}.{}", database, table_def.name),
    }
}

fn table_stats_request(
    database: &str,
    table_def: &crate::sql::planner::table::TableDef,
) -> Option<TableStatsRequest> {
    match &table_def.source {
        ScanSource::IcebergDataFiles { table, .. } => Some(TableStatsRequest {
            catalog: Some(table.catalog.clone()),
            database: table.namespace.clone(),
            table: table.table.clone(),
            source: ScanSourceIdentity::IcebergTable {
                catalog: table.catalog.clone(),
                namespace: table.namespace.clone(),
                table: table.table.clone(),
            },
            snapshot: Some(TableSnapshotRef::Current),
        }),
        ScanSource::IcebergVersionTable { table, snapshot_id } => Some(TableStatsRequest {
            catalog: Some(table.catalog.clone()),
            database: table.namespace.clone(),
            table: table.table.clone(),
            source: ScanSourceIdentity::IcebergTable {
                catalog: table.catalog.clone(),
                namespace: table.namespace.clone(),
                table: table.table.clone(),
            },
            snapshot: Some(TableSnapshotRef::SnapshotId(*snapshot_id)),
        }),
        ScanSource::IcebergDeltaTable { table, .. } => Some(TableStatsRequest {
            catalog: Some(table.catalog.clone()),
            database: table.namespace.clone(),
            table: table.table.clone(),
            source: ScanSourceIdentity::Unsupported {
                reason: "iceberg delta scan stats are not supported".to_string(),
            },
            snapshot: None,
        }),
        _ => Some(TableStatsRequest {
            catalog: None,
            database: database.to_string(),
            table: table_def.name.clone(),
            source: ScanSourceIdentity::Unsupported {
                reason: "scan source does not expose query stats".to_string(),
            },
            snapshot: None,
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Mutex;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::connector::iceberg::scan_model::{
        IcebergDataFileBinding, IcebergSchemaDef, IcebergTableInfo,
    };
    use crate::connector::stats::StatsProviderError;
    use crate::sql::column_id::ColumnId;
    use crate::sql::column_id::ColumnRefFactory;
    use crate::sql::common::{JoinKind, OutputColumn};
    use crate::sql::optimizer::operator::{LogicalJoinOp, Operator, ScanOp};
    use crate::sql::optimizer::scalar::ScalarArena;
    use crate::sql::optimizer::stats_input::{StatValue, StatsSource};
    use crate::sql::planner::table::{ScanSource, TableDef};
    use novarocks_catalog::schema::ColumnDef;

    #[test]
    fn collector_binds_each_scan_in_the_same_opt_expr_traversal() {
        let mut expr = test_join_with_two_scans();

        let plan = QueryStatsCollector::new(QueryStatsProviders::none()).collect(&mut expr);

        let refs = collect_scan_refs_for_test(&expr);
        assert_eq!(refs.len(), 2);
        assert_ne!(refs[0], refs[1]);
        assert_eq!(plan.snapshot.len(), 2);
    }

    #[test]
    fn table_stats_request_maps_iceberg_current_and_version_sources() {
        let current_scan = test_iceberg_scan_op(ScanSource::IcebergDataFiles {
            table: iceberg_info("cat", "db", "tbl"),
            files: vec![],
            cloud_properties: BTreeMap::new(),
            binding: IcebergDataFileBinding::CurrentSnapshot,
        });
        let current = table_stats_request(&current_scan.database, &current_scan.table)
            .expect("current iceberg scan should have stats request");
        assert_eq!(current.catalog.as_deref(), Some("cat"));
        assert_eq!(current.database, "db");
        assert_eq!(current.table, "tbl");
        assert_eq!(current.snapshot, Some(TableSnapshotRef::Current));
        assert_eq!(
            current.source,
            ScanSourceIdentity::IcebergTable {
                catalog: "cat".to_string(),
                namespace: "db".to_string(),
                table: "tbl".to_string(),
            }
        );

        let version_scan = test_iceberg_scan_op(ScanSource::IcebergVersionTable {
            table: iceberg_info("cat", "db", "tbl"),
            snapshot_id: 42,
        });
        let version = table_stats_request(&version_scan.database, &version_scan.table)
            .expect("version iceberg scan should have stats request");
        assert_eq!(version.snapshot, Some(TableSnapshotRef::SnapshotId(42)));
        assert_eq!(
            version.source,
            ScanSourceIdentity::IcebergTable {
                catalog: "cat".to_string(),
                namespace: "db".to_string(),
                table: "tbl".to_string(),
            }
        );
    }

    #[test]
    fn table_stats_request_marks_iceberg_delta_unsupported() {
        let delta_scan = test_iceberg_scan_op(ScanSource::IcebergDeltaTable {
            table: iceberg_info("cat", "db", "tbl"),
            from_snapshot_id: 1,
            to_snapshot_id: 2,
        });
        let delta = table_stats_request(&delta_scan.database, &delta_scan.table)
            .expect("delta iceberg scan should produce unsupported request");

        assert_eq!(delta.snapshot, None);
        assert_eq!(
            delta.source,
            ScanSourceIdentity::Unsupported {
                reason: "iceberg delta scan stats are not supported".to_string(),
            }
        );
    }

    #[test]
    fn provider_error_becomes_missing_stats_without_blocking_collection() {
        let provider = Arc::new(FailingStatsProvider::default());
        let providers = QueryStatsProviders {
            iceberg: Some(provider.clone()),
            standalone_state: None,
        };
        let mut expr = test_iceberg_scan(ScanSource::IcebergDataFiles {
            table: iceberg_info("cat", "db", "tbl"),
            files: vec![],
            cloud_properties: BTreeMap::new(),
            binding: IcebergDataFileBinding::CurrentSnapshot,
        });

        let plan = QueryStatsCollector::new(providers).collect(&mut expr);

        let refs = collect_scan_refs_for_test(&expr);
        let stats_ref = refs[0].expect("scan should be bound");
        let stats = plan.snapshot.get(stats_ref).expect("snapshot entry");
        assert_eq!(
            stats.row_count,
            StatValue::missing(StatsMissingReason::CatalogLoadError(
                "catalog unavailable".to_string()
            ))
        );
        assert_eq!(provider.requests.lock().expect("requests").len(), 1);
    }

    #[test]
    fn standalone_catalog_stats_flow_through_snapshot_and_optimizer() {
        let state = Arc::new(crate::engine::StandaloneState::default());
        let table_name = "misleading_sales_fact_dim_lineitem";
        crate::engine::statistics::replace_catalog_stats_for_test(
            &state,
            "db",
            table_name,
            &[("k", 3, "1", "3", "3")],
        )
        .expect("install catalog stats");
        let providers = QueryStatsProviders::from_standalone_state(&state);
        let mut opt_expr = test_scan(table_name, 1);

        let plan = QueryStatsCollector::new(providers).collect(&mut opt_expr);

        let stats_ref = collect_scan_refs_for_test(&opt_expr)[0].expect("scan should be bound");
        let stats = plan.snapshot.get(stats_ref).expect("snapshot entry");
        assert_eq!(
            stats.row_count,
            StatValue::known(
                3,
                crate::sql::optimizer::statistics::Confidence::Exact,
                StatsSource::StarRocksTableMetadata
            )
        );
        assert!(
            plan.snapshot
                .display_rows()
                .iter()
                .any(|line| line.contains("rows=3")
                    && line.contains("source=StarRocksTableMetadata")),
            "catalog stats source should be visible in query-scoped stats"
        );

        let optimized_tree = crate::sql::optimizer::optimize(
            opt_expr,
            ScalarArena::new(),
            &plan.snapshot,
            ColumnRefFactory::default(),
            Vec::new(),
        )
        .expect("optimizer should consume bound catalog stats");
        assert_eq!(optimized_tree.stats.output_row_count, 3.0);
    }

    #[test]
    fn add_stats_allocates_from_next_stats_ref_not_snapshot_len() {
        let mut snapshot = QueryStatsSnapshot::empty();
        snapshot.insert(
            StatsRef::new(99),
            "preexisting",
            BaseTableStatistics::missing(StatsMissingReason::NoDataFiles),
        );
        let mut plan = QueryStatsPlan::new(snapshot, 7);

        let stats_ref = plan.add_stats(
            "extra",
            BaseTableStatistics {
                row_count: StatValue::known(
                    5,
                    crate::sql::optimizer::statistics::Confidence::Exact,
                    StatsSource::TestFixture,
                ),
                columns: HashMap::new(),
                source: StatsSource::TestFixture,
            },
        );

        assert_eq!(stats_ref, StatsRef::new(7));
        assert!(plan.snapshot.get(StatsRef::new(7)).is_some());
        assert!(plan.snapshot.get(StatsRef::new(99)).is_some());
    }

    fn test_join_with_two_scans() -> OptExpr {
        OptExpr::new(
            Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::Inner,
                condition: None,
            }),
            vec![test_scan("left", 1), test_scan("right", 2)],
        )
    }

    fn test_scan(name: &str, column_id: u32) -> OptExpr {
        OptExpr::leaf(Operator::LogicalScan(ScanOp {
            database: "db".to_string(),
            table: TableDef {
                name: name.to_string(),
                columns: vec![ColumnDef {
                    name: "k".to_string(),
                    data_type: DataType::Int64,
                    nullable: true,
                    write_default: None,
                    logical_type: None,
                }],
                iceberg_row_lineage_metadata_columns: vec![],
                source: ScanSource::StarRocks {
                    db_id: 0,
                    table_id: i64::from(column_id),
                },
            },
            alias: None,
            stats_ref: None,
            columns: vec![OutputColumn {
                column_id: ColumnId::new_for_test(column_id),
                name: "k".to_string(),
                data_type: DataType::Int64,
                nullable: true,
                is_internal: false,
            }],
            predicates: vec![],
            required_columns: None,
            variant_columns: vec![],
            mv_rewritten_from: None,
        }))
    }

    fn test_iceberg_scan(source: ScanSource) -> OptExpr {
        OptExpr::leaf(Operator::LogicalScan(test_iceberg_scan_op(source)))
    }

    fn test_iceberg_scan_op(source: ScanSource) -> ScanOp {
        ScanOp {
            database: "db".to_string(),
            table: TableDef {
                name: "tbl".to_string(),
                columns: vec![],
                iceberg_row_lineage_metadata_columns: vec![],
                source,
            },
            alias: None,
            stats_ref: None,
            columns: vec![OutputColumn {
                column_id: ColumnId::new_for_test(10),
                name: "k".to_string(),
                data_type: DataType::Int64,
                nullable: true,
                is_internal: false,
            }],
            predicates: vec![],
            required_columns: None,
            variant_columns: vec![],
            mv_rewritten_from: None,
        }
    }

    fn iceberg_info(catalog: &str, namespace: &str, table: &str) -> IcebergTableInfo {
        IcebergTableInfo {
            catalog: catalog.to_string(),
            namespace: namespace.to_string(),
            table: table.to_string(),
            table_uuid: Some("00000000-0000-0000-0000-000000000001".to_string()),
            current_snapshot_id: Some(1),
            schema_id: 1,
            location: format!("file:///tmp/{table}"),
            schema: IcebergSchemaDef { fields: vec![] },
            serialized_metadata: None,
            serialized_metadata_rows: None,
        }
    }

    fn collect_scan_refs_for_test(expr: &OptExpr) -> Vec<Option<StatsRef>> {
        let mut refs = Vec::new();
        collect_scan_refs(expr, &mut refs);
        refs
    }

    fn collect_scan_refs(expr: &OptExpr, refs: &mut Vec<Option<StatsRef>>) {
        if let Operator::LogicalScan(scan) = &expr.op {
            refs.push(scan.stats_ref);
        }
        for child in &expr.children {
            collect_scan_refs(child, refs);
        }
    }

    #[derive(Default)]
    struct FailingStatsProvider {
        requests: Mutex<Vec<TableStatsRequest>>,
    }

    impl TableStatsProvider for FailingStatsProvider {
        fn estimate_table_statistics(
            &self,
            request: &TableStatsRequest,
        ) -> Result<BaseTableStatistics, StatsProviderError> {
            self.requests
                .lock()
                .expect("requests")
                .push(request.clone());
            Err(StatsProviderError::Catalog(
                "catalog unavailable".to_string(),
            ))
        }
    }
}
