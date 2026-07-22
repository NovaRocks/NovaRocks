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

use std::collections::{BTreeMap, BTreeSet};

use crate::connector::iceberg::scan_model::{
    IcebergDataFileBinding, IcebergDataFileInfo, IcebergTableInfo,
};
use crate::connector::scan_model::starrocks::StarRocksScanSourceDescriptor;
use crate::runtime::scan_range::ScanRangeParams;
use crate::sql::analysis::OutputColumn;
use crate::sql::column_id::ColumnId;
use crate::sql::planner::distributed::FragmentId;
use crate::sql::planner::payload::PlanScanNode;
use novarocks_catalog::schema::ColumnDef;

pub(crate) use super::iceberg_delta::IcebergDeltaScanRuntimePlan;

pub(crate) trait ScanBindingResolver: Send + Sync {
    fn resolve_scan(
        &self,
        node_id: i32,
        scan: &PlanScanNode,
    ) -> Result<Option<ResolvedScanExecution>, String>;
}

#[derive(Clone, Debug)]
pub(crate) enum ResolvedScanExecution {
    IcebergFiles(ResolvedIcebergFileScan),
    IcebergDelta(ResolvedIcebergDeltaScan),
}

#[derive(Clone, Debug)]
pub(crate) struct ResolvedIcebergFileScan {
    pub table: IcebergTableInfo,
    pub files: Vec<IcebergDataFileInfo>,
    pub cloud_properties: BTreeMap<String, String>,
    pub binding: IcebergDataFileBinding,
}

#[derive(Clone, Debug)]
pub(crate) struct ResolvedIcebergDeltaScan {
    pub runtime_plan: IcebergDeltaScanRuntimePlan,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ResolvedScanColumnKind {
    PhysicalTableColumn,
    IcebergMetadataColumn,
}

#[derive(Clone, Debug)]
pub(crate) struct ResolvedScanColumn {
    pub planner: OutputColumn,
    pub source: ColumnDef,
    pub kind: ResolvedScanColumnKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ResolvedReadReason {
    PlannerRequiredOrOutput,
    EqualityDeleteKey,
}

#[derive(Clone, Debug)]
pub(crate) struct ResolvedReadColumn {
    pub planner_column_id: Option<ColumnId>,
    pub source: ColumnDef,
    pub reason: ResolvedReadReason,
}

#[derive(Clone, Debug)]
pub(crate) struct ResolvedScanBinding {
    pub node_id: i32,
    pub execution: ResolvedScanExecution,
    pub physical_columns: Vec<ResolvedScanColumn>,
    pub required_reads: Vec<ResolvedReadColumn>,
}

impl ResolvedScanBinding {
    fn validate(&self) -> Result<(), String> {
        let mut physical_planner_ids = BTreeSet::new();
        let mut physical_source_names = BTreeSet::new();
        for column in &self.physical_columns {
            if !physical_planner_ids.insert(column.planner.column_id) {
                return Err(format!(
                    "scan binding node_id={} has duplicate physical planner column id {}",
                    self.node_id, column.planner.column_id
                ));
            }
            let source_key = column.source.name.to_ascii_lowercase();
            if !physical_source_names.insert(source_key) {
                return Err(format!(
                    "scan binding node_id={} has duplicate physical source name '{}'",
                    self.node_id, column.source.name
                ));
            }
            if column.planner.data_type != column.source.data_type {
                return Err(format!(
                    "scan binding node_id={} column planner='{}' source='{}' type mismatch: planner={:?}, source={:?}",
                    self.node_id,
                    column.planner.name,
                    column.source.name,
                    column.planner.data_type,
                    column.source.data_type
                ));
            }
            if column.planner.nullable != column.source.nullable {
                return Err(format!(
                    "scan binding node_id={} column planner='{}' source='{}' nullability mismatch: planner={}, source={}",
                    self.node_id,
                    column.planner.name,
                    column.source.name,
                    column.planner.nullable,
                    column.source.nullable
                ));
            }
        }

        let mut required_source_names = BTreeSet::new();
        for read in &self.required_reads {
            let source_key = read.source.name.to_ascii_lowercase();
            if !required_source_names.insert(source_key.clone()) {
                return Err(format!(
                    "scan binding node_id={} has duplicate required source name '{}'",
                    self.node_id, source_key
                ));
            }
            match (read.planner_column_id, read.reason) {
                (Some(column_id), ResolvedReadReason::EqualityDeleteKey) => {
                    return Err(format!(
                        "scan binding node_id={} required source '{}' uses EqualityDeleteKey with planner_column_id=Some({}); hidden equality reads require planner_column_id=None",
                        self.node_id, read.source.name, column_id
                    ));
                }
                (None, ResolvedReadReason::PlannerRequiredOrOutput) => {
                    return Err(format!(
                        "scan binding node_id={} required source '{}' has planner_column_id=None but PlannerRequiredOrOutput requires a planner column id",
                        self.node_id, read.source.name
                    ));
                }
                (Some(_), ResolvedReadReason::PlannerRequiredOrOutput)
                | (None, ResolvedReadReason::EqualityDeleteKey) => {}
            }
        }
        Ok(())
    }
}

#[derive(Default)]
pub(crate) struct ScanExecutionBindings {
    by_node_id: BTreeMap<i32, ResolvedScanBinding>,
    scan_ranges: BTreeMap<FragmentId, BTreeMap<i32, Vec<ScanRangeParams>>>,
    starrocks_sources: BTreeMap<i32, StarRocksScanSourceDescriptor>,
}

impl ScanExecutionBindings {
    pub(crate) fn insert_binding(&mut self, binding: ResolvedScanBinding) -> Result<(), String> {
        if self.by_node_id.contains_key(&binding.node_id) {
            return Err(format!(
                "duplicate scan binding node_id={}",
                binding.node_id
            ));
        }
        binding.validate()?;
        self.by_node_id.insert(binding.node_id, binding);
        Ok(())
    }

    pub(crate) fn binding(&self, node_id: i32) -> Option<&ResolvedScanBinding> {
        self.by_node_id.get(&node_id)
    }

    pub(super) fn binding_node_ids(&self) -> impl Iterator<Item = i32> + '_ {
        self.by_node_id.keys().copied()
    }

    pub(crate) fn insert_scan_ranges(
        &mut self,
        fragment_id: FragmentId,
        node_id: i32,
        ranges: Vec<ScanRangeParams>,
    ) -> Result<(), String> {
        let per_node = self.scan_ranges.entry(fragment_id).or_default();
        if per_node.contains_key(&node_id) {
            return Err(format!(
                "duplicate scan ranges fragment_id={fragment_id} node_id={node_id}"
            ));
        }
        per_node.insert(node_id, ranges);
        Ok(())
    }

    pub(crate) fn scan_ranges(
        &self,
        fragment_id: FragmentId,
        node_id: i32,
    ) -> Option<&[ScanRangeParams]> {
        self.scan_ranges
            .get(&fragment_id)
            .and_then(|per_node| per_node.get(&node_id))
            .map(Vec::as_slice)
    }

    pub(super) fn scan_range_keys(&self) -> impl Iterator<Item = (FragmentId, i32)> + '_ {
        self.scan_ranges
            .iter()
            .flat_map(|(&fragment_id, per_node)| {
                per_node.keys().map(move |&node_id| (fragment_id, node_id))
            })
    }

    pub(crate) fn insert_starrocks_source(
        &mut self,
        node_id: i32,
        source: StarRocksScanSourceDescriptor,
    ) -> Result<(), String> {
        if self.starrocks_sources.contains_key(&node_id) {
            return Err(format!("duplicate StarRocks scan source node_id={node_id}"));
        }
        self.starrocks_sources.insert(node_id, source);
        Ok(())
    }

    pub(crate) fn starrocks_source(&self, node_id: i32) -> Option<&StarRocksScanSourceDescriptor> {
        self.starrocks_sources.get(&node_id)
    }

    pub(super) fn starrocks_source_node_ids(&self) -> impl Iterator<Item = i32> + '_ {
        self.starrocks_sources.keys().copied()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use arrow::datatypes::DataType;

    use super::*;
    use novarocks_catalog::schema::SqlType;

    #[test]
    fn resolver_trait_object_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync + ?Sized>() {}

        assert_send_sync::<dyn ScanBindingResolver>();
    }

    fn planner_column(id: u32, name: &str, data_type: DataType, nullable: bool) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: name.to_string(),
            data_type,
            nullable,
            is_internal: false,
        }
    }

    fn source_column(
        name: &str,
        data_type: DataType,
        nullable: bool,
        logical_type: Option<SqlType>,
    ) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type,
            nullable,
            write_default: None,
            logical_type,
        }
    }

    fn delta_execution() -> ResolvedScanExecution {
        ResolvedScanExecution::IcebergDelta(ResolvedIcebergDeltaScan {
            runtime_plan: IcebergDeltaScanRuntimePlan {
                table_location: "s3://warehouse/db/table".to_string(),
                data_columns: Vec::new(),
                cloud_properties: BTreeMap::new(),
                change_files: Vec::new(),
                delete_side: None,
            },
        })
    }

    fn binding(
        node_id: i32,
        physical_columns: Vec<ResolvedScanColumn>,
        required_reads: Vec<ResolvedReadColumn>,
    ) -> ResolvedScanBinding {
        ResolvedScanBinding {
            node_id,
            execution: delta_execution(),
            physical_columns,
            required_reads,
        }
    }

    #[test]
    fn physical_mapping_preserves_planner_and_source_contracts() {
        let planner = planner_column(17, "payload", DataType::LargeBinary, true);
        let source = source_column("payload", DataType::LargeBinary, true, Some(SqlType::Json));
        let mut bindings = ScanExecutionBindings::default();

        bindings
            .insert_binding(binding(
                41,
                vec![ResolvedScanColumn {
                    planner,
                    source,
                    kind: ResolvedScanColumnKind::PhysicalTableColumn,
                }],
                Vec::new(),
            ))
            .expect("valid binding");

        let resolved = &bindings.binding(41).expect("binding").physical_columns[0];
        assert_eq!(resolved.planner.column_id, ColumnId::new_for_test(17));
        assert_eq!(resolved.planner.data_type, DataType::LargeBinary);
        assert!(resolved.planner.nullable);
        assert_eq!(resolved.source.logical_type, Some(SqlType::Json));
        assert_eq!(resolved.kind, ResolvedScanColumnKind::PhysicalTableColumn);
    }

    #[test]
    fn hidden_equality_reads_require_none_id_and_typed_reason() {
        let hidden = source_column("tenant_id", DataType::Int64, false, None);
        let mut bindings = ScanExecutionBindings::default();
        bindings
            .insert_binding(binding(
                42,
                Vec::new(),
                vec![ResolvedReadColumn {
                    planner_column_id: None,
                    source: hidden.clone(),
                    reason: ResolvedReadReason::EqualityDeleteKey,
                }],
            ))
            .expect("valid hidden equality read");

        let invalid_missing_id = bindings
            .insert_binding(binding(
                43,
                Vec::new(),
                vec![ResolvedReadColumn {
                    planner_column_id: None,
                    source: hidden.clone(),
                    reason: ResolvedReadReason::PlannerRequiredOrOutput,
                }],
            ))
            .expect_err("planner read without planner id");
        assert!(invalid_missing_id.contains("node_id=43"));
        assert!(invalid_missing_id.contains("tenant_id"));
        assert!(invalid_missing_id.contains("planner_column_id"));

        let invalid_visible_equality = bindings
            .insert_binding(binding(
                44,
                Vec::new(),
                vec![ResolvedReadColumn {
                    planner_column_id: Some(ColumnId::new_for_test(9)),
                    source: hidden,
                    reason: ResolvedReadReason::EqualityDeleteKey,
                }],
            ))
            .expect_err("equality-only reason with planner id");
        assert!(invalid_visible_equality.contains("node_id=44"));
        assert!(invalid_visible_equality.contains("tenant_id"));
        assert!(invalid_visible_equality.contains("EqualityDeleteKey"));
    }

    #[test]
    fn duplicate_node_binding_insertion_fails_fast() {
        let mut bindings = ScanExecutionBindings::default();
        bindings
            .insert_binding(binding(51, Vec::new(), Vec::new()))
            .expect("first binding");

        let err = bindings
            .insert_binding(binding(51, Vec::new(), Vec::new()))
            .expect_err("duplicate binding");

        assert!(err.contains("duplicate scan binding node_id=51"));
    }

    #[test]
    fn duplicate_physical_planner_column_id_fails_fast() {
        let mut bindings = ScanExecutionBindings::default();
        let err = bindings
            .insert_binding(binding(
                52,
                vec![
                    ResolvedScanColumn {
                        planner: planner_column(7, "first", DataType::Int64, false),
                        source: source_column("first", DataType::Int64, false, None),
                        kind: ResolvedScanColumnKind::PhysicalTableColumn,
                    },
                    ResolvedScanColumn {
                        planner: planner_column(7, "second", DataType::Int64, false),
                        source: source_column("second", DataType::Int64, false, None),
                        kind: ResolvedScanColumnKind::PhysicalTableColumn,
                    },
                ],
                Vec::new(),
            ))
            .expect_err("duplicate planner column id");

        assert!(err.contains("node_id=52"));
        assert!(err.contains("duplicate physical planner column id c7"));
    }

    #[test]
    fn duplicate_physical_source_name_fails_fast_case_insensitively() {
        let mut bindings = ScanExecutionBindings::default();
        let err = bindings
            .insert_binding(binding(
                56,
                vec![
                    ResolvedScanColumn {
                        planner: planner_column(7, "first", DataType::Int64, false),
                        source: source_column("Tenant_ID", DataType::Int64, false, None),
                        kind: ResolvedScanColumnKind::PhysicalTableColumn,
                    },
                    ResolvedScanColumn {
                        planner: planner_column(8, "second", DataType::Int64, false),
                        source: source_column("tenant_id", DataType::Int64, false, None),
                        kind: ResolvedScanColumnKind::PhysicalTableColumn,
                    },
                ],
                Vec::new(),
            ))
            .expect_err("duplicate physical source name");

        assert!(err.contains("node_id=56"), "{err}");
        assert!(err.contains("duplicate physical source name"), "{err}");
        assert!(err.contains("tenant_id"), "{err}");
    }

    #[test]
    fn duplicate_required_source_name_fails_fast_case_insensitively() {
        let mut bindings = ScanExecutionBindings::default();
        let err = bindings
            .insert_binding(binding(
                53,
                Vec::new(),
                vec![
                    ResolvedReadColumn {
                        planner_column_id: None,
                        source: source_column("Tenant_ID", DataType::Int64, false, None),
                        reason: ResolvedReadReason::EqualityDeleteKey,
                    },
                    ResolvedReadColumn {
                        planner_column_id: None,
                        source: source_column("tenant_id", DataType::Int64, false, None),
                        reason: ResolvedReadReason::EqualityDeleteKey,
                    },
                ],
            ))
            .expect_err("duplicate required source name");

        assert!(err.contains("node_id=53"));
        assert!(err.contains("duplicate required source name 'tenant_id'"));
    }

    #[test]
    fn physical_contract_mismatch_reports_node_name_and_both_contracts() {
        for (node_id, planner_type, planner_nullable, source_type, source_nullable, label) in [
            (
                54,
                DataType::Int64,
                false,
                DataType::Utf8,
                false,
                "type mismatch",
            ),
            (
                55,
                DataType::Int64,
                false,
                DataType::Int64,
                true,
                "nullability mismatch",
            ),
        ] {
            let mut bindings = ScanExecutionBindings::default();
            let err = bindings
                .insert_binding(binding(
                    node_id,
                    vec![ResolvedScanColumn {
                        planner: planner_column(
                            node_id as u32,
                            "contract_col",
                            planner_type,
                            planner_nullable,
                        ),
                        source: source_column("contract_col", source_type, source_nullable, None),
                        kind: ResolvedScanColumnKind::PhysicalTableColumn,
                    }],
                    Vec::new(),
                ))
                .expect_err(label);

            assert!(err.contains(&format!("node_id={node_id}")), "{err}");
            assert!(err.contains("contract_col"), "{err}");
            assert!(err.contains(label), "{err}");
            assert!(err.contains("planner="), "{err}");
            assert!(err.contains("source="), "{err}");
        }
    }

    #[test]
    fn range_and_starrocks_source_insertions_are_checked_and_read_only() {
        let mut bindings = ScanExecutionBindings::default();
        bindings
            .insert_scan_ranges(3, 61, Vec::new())
            .expect("first scan ranges");
        assert!(bindings.scan_ranges(3, 61).expect("scan ranges").is_empty());
        let range_err = bindings
            .insert_scan_ranges(3, 61, Vec::new())
            .expect_err("duplicate scan ranges");
        assert!(range_err.contains("fragment_id=3"));
        assert!(range_err.contains("node_id=61"));

        let source = crate::connector::scan_model::starrocks::StarRocksScanSourceDescriptor {
            catalog_name: "default_catalog".to_string(),
            db_id: 1,
            table_id: 2,
            schema_id: 3,
            storage_columns: Vec::new(),
            tablet_schema:
                crate::connector::scan_model::starrocks::test_starrocks_tablet_schema_descriptor(
                    3,
                    &[],
                ),
        };
        bindings
            .insert_starrocks_source(62, source)
            .expect("first StarRocks source");
        assert_eq!(
            bindings
                .starrocks_source(62)
                .expect("StarRocks source")
                .table_id,
            2
        );
        let duplicate = crate::connector::scan_model::starrocks::StarRocksScanSourceDescriptor {
            catalog_name: "default_catalog".to_string(),
            db_id: 1,
            table_id: 2,
            schema_id: 3,
            storage_columns: Vec::new(),
            tablet_schema:
                crate::connector::scan_model::starrocks::test_starrocks_tablet_schema_descriptor(
                    3,
                    &[],
                ),
        };
        let source_err = bindings
            .insert_starrocks_source(62, duplicate)
            .expect_err("duplicate StarRocks source");
        assert!(source_err.contains("node_id=62"));
    }
}
