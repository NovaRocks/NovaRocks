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

use std::collections::BTreeMap;

use crate::connector::iceberg::scan_model::{
    IcebergDataFileBinding, IcebergDataFileInfo, IcebergTableInfo,
};
#[cfg(test)]
use crate::connector::iceberg::scan_model::{IcebergSchemaDef, IcebergSchemaFieldDef};
use novarocks_catalog::schema::ColumnDef;

/// Metadata for an IMV target-state scan source. This struct carries only
/// planner-safe metadata for the MV's own target state — catalog identity,
/// column definitions, and the aggregate/join logical contract. It has no
/// execution or catalog handles and is designed to be inspectable during
/// analyzer/optimizer phases without triggering runtime behavior. The
/// standalone refresh codegen lowers this source into the local target-state
/// scan used by aggregate-state merge execution.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct IcebergMvTargetStateScan {
    pub(crate) catalog: String,
    pub(crate) database: String,
    pub(crate) table: String,
    pub(crate) target_table_uuid: String,
    pub(crate) target_snapshot_id: Option<i64>,
    pub(crate) aggregate_state_layout_version: u16,
    pub(crate) columns: Vec<ColumnDef>,
    pub(crate) group_key_names: Vec<String>,
    pub(crate) aggregate_state_names: Vec<String>,
    pub(crate) physical_column_names: Vec<String>,
    pub(crate) row_id_column_name: String,
    pub(crate) row_filter: IcebergMvTargetStateRowFilter,
    pub(crate) partition_constraint: IcebergMvTargetStatePartitionConstraint,
}

/// Metadata for an IMV target-locator scan source. It is a refresh-only
/// placeholder that reads the MV target at the refresh-before snapshot and
/// projects the physical apply-key columns plus Iceberg `_file` / `_pos`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IcebergMvTargetLocatorScan {
    pub(crate) catalog: String,
    pub(crate) database: String,
    pub(crate) table: String,
    pub(crate) target_table_uuid: String,
    pub(crate) target_snapshot_id: Option<i64>,
    pub(crate) apply_key_column: String,
    pub(crate) branch_id_column: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BranchScope {
    pub(crate) branch_id_column_name: String,
    pub(crate) branch_id: i32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum IcebergMvTargetStateRowFilter {
    DeltaInputRowIds {
        row_id_column_name: String,
        branch_scope: Option<BranchScope>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum IcebergMvTargetStatePartitionConstraint {
    Unpartitioned,
    AffectedPartitionAllowListRequired,
}

impl IcebergMvTargetStateScan {
    pub(crate) fn fqn(&self) -> String {
        format!("{}.{}.{}", self.catalog, self.database, self.table)
    }

    pub(crate) fn constraint_summary(&self) -> String {
        let row_filter = match &self.row_filter {
            IcebergMvTargetStateRowFilter::DeltaInputRowIds {
                row_id_column_name,
                branch_scope: None,
            } => {
                format!("row_filter=delta_input_row_ids({row_id_column_name})")
            }
            IcebergMvTargetStateRowFilter::DeltaInputRowIds {
                row_id_column_name,
                branch_scope: Some(scope),
            } => format!(
                "row_filter=delta_input_row_ids({row_id_column_name}, {}={})",
                scope.branch_id_column_name, scope.branch_id
            ),
        };
        let partition = match self.partition_constraint {
            IcebergMvTargetStatePartitionConstraint::Unpartitioned => "partition=unpartitioned",
            IcebergMvTargetStatePartitionConstraint::AffectedPartitionAllowListRequired => {
                "partition=affected_allow_list_required"
            }
        };
        format!(
            "uuid={} snapshot={} layout={} {} {}",
            self.target_table_uuid,
            self.target_snapshot_id
                .map(|id| id.to_string())
                .unwrap_or_else(|| "none".to_string()),
            self.aggregate_state_layout_version,
            row_filter,
            partition
        )
    }
}

impl IcebergMvTargetLocatorScan {
    pub(crate) fn fqn(&self) -> String {
        format!("{}.{}.{}", self.catalog, self.database, self.table)
    }
}

/// Plan-time description of how the scan operator enumerates physical
/// inputs for a table. Each variant covers a different lane:
///
/// - `StarRocks`: StarRocks table identity; the connector scan planner reads
///   the current tablet/version layout from the live StarRocks runtime.
/// - `IcebergDataFiles`: Iceberg `rest`/`hadoop`/IVM-delta-stamped
///   parquet files — a concrete list of data files plus table identity,
///   optional cloud-store credentials, and scan-binding provenance.
/// - `IcebergMetadataTable`: synthetic source for iceberg metadata
///   tables (`t$snapshots` etc.); the operator reads
///   `iceberg::spec::TableMetadata` natively in Rust.
/// - `IcebergDeltaTable`: plan-time identity for IVM-A1 delta
///   scans; codegen expands it into an explicit change-file payload.
#[derive(Clone, Debug)]
pub enum ScanSource {
    /// StarRocks table: data lives in object storage (s3:// or
    /// file://) and metadata lives in a `MetaStoreProvider` (currently
    /// SQLite). The `(db_id, table_id)` identity carried here lets plan-time
    /// consumers resolve the StarRocks table without relying on mutable names.
    /// The connector scan planner validates this planned identity against the
    /// live runtime before producing splits.
    StarRocks { db_id: i64, table_id: i64 },
    IcebergDataFiles {
        table: IcebergTableInfo,
        files: Vec<IcebergDataFileInfo>,
        cloud_properties: BTreeMap<String, String>,
        binding: IcebergDataFileBinding,
    },
    /// Synthetic scan source for an Iceberg metadata-table reference
    /// (`t$snapshots` / `t$history` / `t$refs` / `t$partitions`). The
    /// analyzer rewrites such references into a regular `Scan` over a
    /// synthetic `TableDef` whose source is this variant; codegen then
    /// emits an `HDFS_SCAN_NODE` whose lowering builds an
    /// `IcebergMetadataScanOp` that reads `iceberg::spec::TableMetadata`
    /// natively in Rust (no JVM / JNI bridge — the embedded-JVM path
    /// was removed in favor of iceberg-rust) — see
    /// `src/connector/iceberg/metadata.rs`.
    IcebergMetadataTable {
        table: IcebergTableInfo,
        metadata_table_type: crate::connector::iceberg::IcebergMetadataTableType,
        /// JSON-serialized iceberg-rust `TableMetadata` (produced by
        /// `serde_json::to_string` in
        /// `connector/iceberg/catalog/backend.rs`). The metadata-scan
        /// operator parses it back into a `TableMetadata` and reads
        /// snapshots / history / refs directly off it.
        serialized_table: String,
        /// Cloud properties from the underlying iceberg table's storage.
        /// Forwarded onto `THdfsScanNode.cloud_configuration` for parity
        /// with regular HDFS scans; the native metadata-scan path itself
        /// does not need them today.
        cloud_properties: BTreeMap<String, String>,
        /// Native-Rust metadata table payload used by flavors that need
        /// manifest-derived file aggregates after planning.
        metadata_payload: Option<String>,
    },
    /// IVM-A1 plan-time Iceberg delta-scan placeholder. Produced by the
    /// analyzer/planner when it recognizes the
    /// `__nr_ivm_delta('cat.ns.tbl', from, to)` table function. Codegen
    /// emits `TPlanNodeType::ICEBERG_DELTA_SCAN_NODE` with an explicit
    /// typed payload produced from refresh-time Iceberg metadata planning.
    /// Lowering consumes that payload and does not re-read connector catalog
    /// state or reconstruct full Iceberg table metadata.
    IcebergDeltaTable {
        table: IcebergTableInfo,
        from_snapshot_id: i64,
        to_snapshot_id: i64,
    },
    /// Refresh-only pinned Iceberg version scan placeholder. Produced by the
    /// IMV scan-binding rule for `Version(IcebergScan)`. Phase 1 keeps this
    /// variant non-executable: it is inspectable in rewrite tests and guarded
    /// at scan-range construction so it cannot silently read current snapshot.
    IcebergVersionTable {
        table: IcebergTableInfo,
        snapshot_id: i64,
    },
    /// IMV target-state scan placeholder. Produced by the analyzer when
    /// constructing an IMV refresh plan that reads the MV's own target state.
    /// This variant carries only metadata-level information (catalog identity,
    /// columns, and the aggregate/join logical contract) and has no codegen
    /// or runtime behavior in this task. Future tasks will implement the
    /// optimizer rewrite and execution path.
    IcebergMvTargetState(IcebergMvTargetStateScan),
    /// IMV target locator placeholder. Produced by the IMV rewrite pipeline
    /// after the change stream carries its logical apply key. Codegen resolves
    /// it through `IcebergMvRefreshContext` into an explicit target snapshot
    /// scan that emits physical apply key, `_file`, and `_pos`.
    IcebergMvTargetLocator(IcebergMvTargetLocatorScan),
}

#[derive(Clone, Debug)]
pub struct TableDef {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    /// Iceberg metadata pseudo-columns. `_file` and `_pos` are available for
    /// Iceberg row-identity scans; `_row_id` and
    /// `_last_updated_sequence_number` are exposed only when the table
    /// satisfies v3 row-lineage preconditions. The analyzer registers these
    /// into the per-relation scope as resolvable pseudo-columns but **not**
    /// into `SELECT *` expansion.
    pub iceberg_row_lineage_metadata_columns: Vec<ColumnDef>,
    pub source: ScanSource,
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::*;

    fn test_iceberg_table_info(schema: IcebergSchemaDef) -> IcebergTableInfo {
        IcebergTableInfo {
            catalog: "test_catalog".to_string(),
            namespace: "test_db".to_string(),
            table: "test_table".to_string(),
            table_uuid: Some("00000000-0000-0000-0000-000000000001".to_string()),
            current_snapshot_id: Some(7),
            schema_id: 1,
            location: "file:///tmp/test_table".to_string(),
            schema,
            serialized_metadata: None,
            serialized_metadata_rows: None,
        }
    }

    #[test]
    fn table_def_can_carry_iceberg_schema_metadata() {
        let iceberg = test_iceberg_table_info(IcebergSchemaDef {
            fields: vec![IcebergSchemaFieldDef {
                field_id: 10,
                name: "order_id".to_string(),
                initial_default: None,
                write_default: None,
                initial_default_json: None,
                write_default_json: None,
                children: vec![IcebergSchemaFieldDef {
                    field_id: 11,
                    name: "nested".to_string(),
                    initial_default: None,
                    write_default: None,
                    initial_default_json: None,
                    write_default_json: None,
                    children: vec![],
                }],
            }],
        });
        let table = TableDef {
            name: "orders".to_string(),
            columns: vec![ColumnDef {
                name: "order_id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                write_default: None,
                logical_type: None,
            }],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::IcebergDataFiles {
                table: iceberg,
                files: vec![],
                cloud_properties: BTreeMap::new(),
                binding: IcebergDataFileBinding::CurrentSnapshot,
            },
        };

        let ScanSource::IcebergDataFiles { table: iceberg, .. } = table.source else {
            panic!("expected iceberg data files");
        };
        assert_eq!(iceberg.location, "file:///tmp/test_table");
        assert_eq!(iceberg.schema.fields[0].field_id, 10);
        assert_eq!(iceberg.schema.fields[0].children[0].field_id, 11);
    }
}

#[cfg(test)]
mod imv_target_state_tests {
    use super::*;

    fn sample_columns() -> Vec<ColumnDef> {
        vec![
            ColumnDef {
                name: "region".to_string(),
                data_type: arrow::datatypes::DataType::Utf8,
                nullable: true,
                write_default: None,
                logical_type: None,
            },
            ColumnDef {
                name: "c".to_string(),
                data_type: arrow::datatypes::DataType::Int64,
                nullable: true,
                write_default: None,
                logical_type: None,
            },
        ]
    }

    #[test]
    fn iceberg_mv_target_state_scan_source_carries_logical_contract() {
        let source = ScanSource::IcebergMvTargetState(IcebergMvTargetStateScan {
            catalog: "ice".to_string(),
            database: "ns".to_string(),
            table: "mv_sales".to_string(),
            target_table_uuid: "target-uuid".to_string(),
            target_snapshot_id: Some(42),
            aggregate_state_layout_version: 1,
            columns: sample_columns(),
            group_key_names: vec!["region".to_string()],
            aggregate_state_names: vec!["c".to_string()],
            physical_column_names: vec!["region".to_string(), "c".to_string()],
            row_id_column_name: "__row_id__".to_string(),
            row_filter: IcebergMvTargetStateRowFilter::DeltaInputRowIds {
                row_id_column_name: "__row_id__".to_string(),
                branch_scope: None,
            },
            partition_constraint: IcebergMvTargetStatePartitionConstraint::Unpartitioned,
        });

        let ScanSource::IcebergMvTargetState(scan) = source else {
            panic!("expected target-state scan source");
        };
        assert_eq!(scan.fqn(), "ice.ns.mv_sales");
        assert_eq!(scan.group_key_names, vec!["region"]);
        assert_eq!(scan.aggregate_state_names, vec!["c"]);
        assert_eq!(scan.row_id_column_name, "__row_id__");
        assert!(
            scan.constraint_summary()
                .contains("row_filter=delta_input_row_ids(__row_id__)")
        );
    }

    #[test]
    fn target_state_row_filter_carries_branch_scope() {
        let filter = IcebergMvTargetStateRowFilter::DeltaInputRowIds {
            row_id_column_name: "__row_id__".to_string(),
            branch_scope: Some(BranchScope {
                branch_id_column_name: "__branch_id__".to_string(),
                branch_id: 2,
            }),
        };

        let IcebergMvTargetStateRowFilter::DeltaInputRowIds {
            branch_scope: Some(scope),
            ..
        } = filter
        else {
            panic!("expected branch scope");
        };
        assert_eq!(scope.branch_id_column_name, "__branch_id__");
        assert_eq!(scope.branch_id, 2);
    }
}
