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

use novarocks_spi::connector::{
    ConnectorBatchBudget, ConnectorControlPlanningLease, ConnectorExecutionDeclaration,
    ConnectorPredicateDisposition, ConnectorScan, ConnectorSplit, ConnectorSplitPlanningMetrics,
    ConnectorStaticPredicate,
};

use crate::engine::query_planning::bindings::QueryScanMaterialization;
use crate::runtime::scan_range::ScanRangeParams;
use crate::sql::analysis::OutputColumn;
use crate::sql::analysis::TypedExpr;
use crate::sql::column_id::ColumnId;
use crate::sql::planner::distributed::FragmentId;
use crate::sql::planner::payload::PlanScanNode;
use novarocks_catalog::schema::ColumnDef;

pub(crate) trait ScanBindingResolver: Send + Sync {
    fn resolve_scan(
        &self,
        node_id: i32,
        scan: &PlanScanNode,
    ) -> Result<Option<ResolvedScanExecution>, String>;

    /// Supplies the opaque connector execution read for a source whose
    /// metadata was resolved and pinned before generic preparation.  Normal
    /// query planning leaves this unimplemented; statistics collection uses
    /// it to guarantee that preparation does not perform another latest
    /// metadata lookup.
    fn resolve_connector_read(
        &self,
        _node_id: i32,
        _scan: &PlanScanNode,
    ) -> Result<Option<PlannedConnectorRead>, String> {
        Ok(None)
    }
}

#[derive(Clone, Debug)]
pub(crate) enum ResolvedScanExecution {
    ConnectorRead,
    /// A query-local opaque read admission.  Core may inspect only its SPI
    /// schema and selector while preparation asks the exact lease to plan it.
    AdmittedConnectorRead(QueryScanMaterialization),
    /// A provider-sealed scan admitted earlier by the application. Preparation
    /// validates its exact generation and selection, then plans opaque splits
    /// without decoding or recreating provider physical facts.
    SealedConnectorScan(ConnectorScan),
}

#[cfg(test)]
pub(crate) fn fixture_sealed_change_scan(
    instance_id: &str,
    from_snapshot_id: i64,
    to_snapshot_id: i64,
) -> ConnectorScan {
    fixture_sealed_change_scan_for_table(instance_id, "orders", from_snapshot_id, to_snapshot_id)
}

#[cfg(test)]
pub(crate) fn fixture_sealed_change_scan_for_table(
    instance_id: &str,
    table: &str,
    from_snapshot_id: i64,
    to_snapshot_id: i64,
) -> ConnectorScan {
    use std::sync::Arc;

    use novarocks_spi::connector::{
        ConnectorBeginScanRequest, ConnectorChangeWindow, ConnectorInstanceId,
        ConnectorReadPurpose, ConnectorScanSelection, ConnectorTableIdentity,
        ConnectorTableRequest, ConnectorTableResolution,
    };

    let lease = fixture_planning_lease(instance_id);
    let context = crate::connector::test_request_context();
    let metadata = lease
        .binding()
        .metadata()
        .load_table(ConnectorTableRequest {
            table: ConnectorTableIdentity {
                instance_id: ConnectorInstanceId::parse(instance_id)
                    .expect("fixture connector instance ID"),
                namespace: Arc::from("db"),
                table: Arc::from(table),
            },
            resolution: ConnectorTableResolution::StrictBaseTable,
            context: context.clone(),
        })
        .expect("fixture connector table metadata");
    let projection = (0..metadata.schema.fields().len()).collect();
    lease
        .binding()
        .planning()
        .begin_scan(
            &metadata.table,
            ConnectorBeginScanRequest {
                projection,
                static_predicates: Vec::new(),
                selection: ConnectorScanSelection::ChangeWindow(ConnectorChangeWindow::new(
                    from_snapshot_id,
                    to_snapshot_id,
                )),
                purpose: ConnectorReadPurpose::Query,
                limit: None,
                batch: ConnectorBatchBudget {
                    max_rows: std::num::NonZeroUsize::new(4096).expect("nonzero rows"),
                    max_bytes: std::num::NonZeroUsize::new(context.max_handle_payload_bytes())
                        .expect("nonzero bytes"),
                },
                context,
            },
        )
        .expect("fixture change-window scan")
}

#[cfg(test)]
pub(crate) fn fixture_planning_lease(instance_id: &str) -> ConnectorControlPlanningLease {
    use std::collections::HashMap;
    use std::sync::Arc;

    ConnectorControlPlanningLease::new(
        Arc::new(crate::connector::scan_model::planned_files_fixture_binding(
            instance_id,
            HashMap::new(),
            None,
        )),
        || {},
    )
}

#[cfg(test)]
pub(crate) fn fixture_query_scan_materialization(instance_id: &str) -> QueryScanMaterialization {
    use std::sync::Arc;

    use novarocks_spi::connector::{
        ConnectorInstanceId, ConnectorReadSelector, ConnectorTableIdentity, ConnectorTableRequest,
        ConnectorTableResolution,
    };

    let planning_lease = fixture_planning_lease(instance_id);
    let metadata = planning_lease
        .binding()
        .metadata()
        .load_table(ConnectorTableRequest {
            table: ConnectorTableIdentity {
                instance_id: ConnectorInstanceId::parse(instance_id)
                    .expect("fixture connector instance ID"),
                namespace: Arc::from("db"),
                table: Arc::from("orders"),
            },
            resolution: ConnectorTableResolution::StrictBaseTable,
            context: crate::connector::test_request_context(),
        })
        .expect("fixture connector read admission");
    QueryScanMaterialization {
        table: metadata.table,
        schema: metadata.schema,
        selector: ConnectorReadSelector::Current,
        statistics_pin: None,
        planning_lease,
    }
}

/// Provider-neutral result of planning an executable connector read.  Its
/// handles remain opaque to core: preparation owns only declaration delivery,
/// scheduling hints, and native-carrier assembly.
#[derive(Clone)]
pub(crate) struct PlannedConnectorRead {
    pub(crate) declaration: ConnectorExecutionDeclaration,
    pub(crate) scan: ConnectorScan,
    /// Stable provider field ordinals aligned 1:1 with `scan.output_schema`.
    /// These are frozen with the exact FE read and are the only authority for
    /// Scan-domain target encoding for pre-reader evaluation.
    pub(crate) provider_field_ordinals: Vec<u32>,
    pub(crate) splits: Vec<ConnectorSplit>,
    /// Provider split-planning evidence retained only in FE preparation.
    pub(crate) planning_metrics: ConnectorSplitPlanningMetrics,
    /// Submitted predicate requests and their normalized provider response.
    pub(crate) static_predicates: Vec<ConnectorStaticPredicate>,
    pub(crate) predicate_dispositions: Vec<ConnectorPredicateDisposition>,
    /// Ordered Core residuals after removing only negotiated `Exact` IDs.
    pub(crate) residual_predicates: Vec<TypedExpr>,
    pub(crate) batch: ConnectorBatchBudget,
    /// Keeps the exact FE control generation alive through the BE ensure
    /// barrier. It is never encoded into a fragment carrier.
    pub(crate) planning_lease: ConnectorControlPlanningLease,
    /// FE-local remote read ownership. This never enters a native carrier.
    pub(crate) read_session: Option<novarocks_spi::connector::ConnectorReadSessionLease>,
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
    connector_reads: BTreeMap<(FragmentId, i32), PlannedConnectorRead>,
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

    pub(crate) fn insert_connector_read(
        &mut self,
        fragment_id: FragmentId,
        node_id: i32,
        read: PlannedConnectorRead,
    ) -> Result<(), String> {
        if self.connector_reads.contains_key(&(fragment_id, node_id)) {
            return Err(format!(
                "duplicate connector read fragment_id={fragment_id} node_id={node_id}"
            ));
        }
        if read.scan.handle().owner() != &read.declaration.descriptor().instance_id {
            return Err(format!(
                "connector read fragment_id={fragment_id} node_id={node_id} has a scan handle owned by another instance"
            ));
        }
        if read.provider_field_ordinals.len() != read.scan.output_schema().fields().len() {
            return Err(format!(
                "connector read fragment_id={fragment_id} node_id={node_id} provider ordinal count {} does not match output schema field count {}",
                read.provider_field_ordinals.len(),
                read.scan.output_schema().fields().len(),
            ));
        }
        let mut provider_ordinals = BTreeSet::new();
        if read
            .provider_field_ordinals
            .iter()
            .any(|ordinal| !provider_ordinals.insert(*ordinal))
        {
            return Err(format!(
                "connector read fragment_id={fragment_id} node_id={node_id} has duplicate provider field ordinals"
            ));
        }
        if read
            .splits
            .iter()
            .any(|split| split.owner() != &read.declaration.descriptor().instance_id)
        {
            return Err(format!(
                "connector read fragment_id={fragment_id} node_id={node_id} has a split owned by another instance"
            ));
        }
        self.connector_reads.insert((fragment_id, node_id), read);
        Ok(())
    }

    pub(crate) fn connector_read(
        &self,
        fragment_id: FragmentId,
        node_id: i32,
    ) -> Option<&PlannedConnectorRead> {
        self.connector_reads.get(&(fragment_id, node_id))
    }

    pub(crate) fn connector_read_for_node(&self, node_id: i32) -> Option<&PlannedConnectorRead> {
        self.connector_reads
            .iter()
            .find_map(|(&(fragment_id, candidate), read)| {
                (candidate == node_id)
                    .then_some((fragment_id, read))
                    .map(|(_, read)| read)
            })
    }

    pub(crate) fn connector_reads(&self) -> impl Iterator<Item = &PlannedConnectorRead> {
        self.connector_reads.values()
    }

    pub(super) fn connector_read_keys(&self) -> impl Iterator<Item = (FragmentId, i32)> + '_ {
        self.connector_reads.keys().copied()
    }
}

#[cfg(test)]
mod tests {
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
        ResolvedScanExecution::SealedConnectorScan(fixture_sealed_change_scan("ice", 6, 7))
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
}
