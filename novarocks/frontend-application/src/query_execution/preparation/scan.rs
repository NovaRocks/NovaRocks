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
    ConnectorControlPlanningLease, ConnectorFrozenRewriteGroup, ConnectorInstanceId,
    ConnectorPinnedFileSet,
};

use crate::catalog_application::query_bindings::QueryScanMaterialization;
use crate::query_execution::preparation::typed_scan::PreparedTypedScan;
use novarocks_proto_codec::lifecycle::ScanRangeParams;
use novarocks_sql::plan_read::ColumnId;
use novarocks_sql::plan_read::FragmentId;
use novarocks_sql::plan_read::OutputColumn;
use novarocks_sql::plan_read::PlanScanNode;
use novarocks_types::schema::ColumnDef;

pub(crate) trait ScanBindingResolver: Send + Sync {
    fn resolve_scan(
        &self,
        node_id: i32,
        scan: &PlanScanNode,
    ) -> Result<Option<ResolvedScanExecution>, String>;
}

/// How one scan's relation was admitted, and by what.
///
/// Every variant is an admission, and the shared prefix says so on purpose:
/// preparation refuses a scan it cannot name an admission for, so "admitted"
/// is the property that makes a variant belong to this enum at all rather than
/// a word repeated for want of a better one.
#[derive(Clone, Debug)]
#[expect(
    clippy::enum_variant_names,
    reason = "Every variant is an admission; the prefix states the property that admits it to this enum."
)]
pub(crate) enum ResolvedScanExecution {
    /// A query-local opaque read admission.  Core may inspect only its SPI
    /// schema and selector while preparation asks the exact lease to plan it.
    AdmittedConnectorRead(QueryScanMaterialization),
    /// A query-local admission read as one system relation of its table.
    ///
    /// It carries the same admitted materialization as an ordinary read: the
    /// exact planning lease and table handle the statement froze. Which system
    /// relation it is comes from the SQL scan's own metadata table kind, and
    /// the connector resolves that name to a pinned metadata file.
    AdmittedSystemTable(QueryScanMaterialization),
    /// A query-local admission read as a change window over its relation.
    ///
    /// It carries the same admitted materialization as an ordinary read: the
    /// exact planning lease and table handle the statement froze. The window's
    /// two endpoints come from the SQL scan itself, and the connector freezes
    /// one change-window relation pinned to both of them.
    AdmittedChangeWindow(QueryScanMaterialization),
    /// One provider-frozen cohort read of a pinned file set.
    AdmittedPinnedFileSet(QueryPinnedFileSetRead),
    /// One provider-frozen cohort read of a distributed
    /// `ALTER TABLE ... EXECUTE` procedure's own relation.
    AdmittedTableExecute(QueryRewriteGroupRead),
}

impl ResolvedScanExecution {
    /// Project the transient admission onto the only execution distinction the
    /// immutable native plan needs. The admission itself owns Catalog and
    /// provider capabilities and must not cross into `PreparedFragmentSet`.
    pub(crate) const fn prepared_kind(&self) -> PreparedScanExecutionKind {
        match self {
            Self::AdmittedConnectorRead(_)
            | Self::AdmittedSystemTable(_)
            | Self::AdmittedPinnedFileSet(_)
            | Self::AdmittedTableExecute(_) => PreparedScanExecutionKind::AdmittedConnectorRead,
            Self::AdmittedChangeWindow(_) => PreparedScanExecutionKind::SealedConnectorScan,
        }
    }
}

/// Resource-free execution category retained by native preparation.
///
/// Exact table, snapshot, pinned-file, and rewrite-group semantics live in the
/// frozen typed handle. Per-attempt access and its Catalog generation guard
/// live in `ConnectorAttemptAccessPlan`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PreparedScanExecutionKind {
    AdmittedConnectorRead,
    SealedConnectorScan,
}

/// The exact facts one provider-frozen cohort read is planned from.
///
/// The file set was minted by the connector's own mutation or rewrite
/// preparation and is only carried here: the engine neither derives it nor may
/// narrow it, because the cohort's commit replaces precisely those files.
#[derive(Clone)]
pub(crate) struct QueryPinnedFileSetRead {
    /// The relation, the version, and exactly the files this cohort reads.
    /// All three are the connector's own; the SQL name that carries the read
    /// through planning is query-local and synthetic, so it names nothing the
    /// connector could resolve.
    pub(crate) pinned: ConnectorPinnedFileSet,
    /// The instance that pinned the set. It is the exact-owner witness for
    /// this read; the relation itself is frozen from the pinned set above.
    pub(crate) owner: ConnectorInstanceId,
    pub(crate) planning_lease: ConnectorControlPlanningLease,
}

impl std::fmt::Debug for QueryPinnedFileSetRead {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PinnedFileSetRead")
            .field("namespace", &self.pinned.namespace())
            .field("table", &self.pinned.table())
            .field("version_ordinal", &self.pinned.version_ordinal())
            .field("files", &self.pinned.files().len())
            .finish_non_exhaustive()
    }
}

/// The exact facts one distributed procedure's cohort read is planned from.
///
/// The group was minted by the connector's own rewrite preparation and is only
/// carried here. It names the artifacts the cohort reads, which are the same
/// artifacts its commit replaces: the engine neither derives that set nor could
/// -- re-deriving it from a rule would let the read and the commit disagree.
#[derive(Clone)]
pub(crate) struct QueryRewriteGroupRead {
    /// The relation and the immutable artifact this cohort's group lives in.
    pub(crate) group: ConnectorFrozenRewriteGroup,
    /// The group inside that artifact.
    pub(crate) group_digest: [u8; 32],
    /// The instance that minted the group. It is the exact-owner witness for
    /// this read; the SQL name that carries it through planning is query-local
    /// and synthetic, so it names nothing the connector could resolve.
    pub(crate) owner: ConnectorInstanceId,
    pub(crate) planning_lease: ConnectorControlPlanningLease,
}

impl std::fmt::Debug for QueryRewriteGroupRead {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RewriteGroupRead")
            .field("namespace", &self.group.schema_name())
            .field("table", &self.group.table_name())
            .field("artifact_location", &self.group.artifact_location())
            .finish_non_exhaustive()
    }
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
        catalog_handle: planning_lease
            .binding()
            .catalog_handle()
            .expect("fixture control binding has a catalog handle")
            .clone(),
        schema: metadata.schema,
        selector: ConnectorReadSelector::Current,
        statistics_pin: None,
        planning_lease,
    }
}

/// One SQL scan lowered onto the typed connector read stack.
///
/// It deliberately carries no split or provider capability. Enumeration is
/// lazy and owned by the execution round, which opens fresh attempt access
/// from the logical-query sidecar; anything that used to size itself from a
/// frozen split count must ask the live backend topology instead.
pub(crate) struct PreparedTypedConnectorScan {
    /// Pure typed scan description. It owns no provider capability or lease.
    pub(crate) prepared: PreparedTypedScan,
}

impl PreparedTypedConnectorScan {
    pub(super) fn new(prepared: PreparedTypedScan) -> Self {
        Self { prepared }
    }
}

impl std::fmt::Debug for PreparedTypedConnectorScan {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedTypedConnectorScan")
            .field("prepared", &self.prepared)
            .finish_non_exhaustive()
    }
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
    pub execution_kind: PreparedScanExecutionKind,
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
    // Immutable, already-materialized scheduling payload only. This map never
    // owns a split source, split manager, request context, or enumeration
    // cursor; typed connector scans register an empty entry because each
    // attempt enumerates through its sidecar access.
    scan_ranges: BTreeMap<FragmentId, BTreeMap<i32, Vec<ScanRangeParams>>>,
    typed_scans: BTreeMap<(FragmentId, i32), PreparedTypedConnectorScan>,
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

    /// Record one typed connector scan.
    ///
    /// The frozen typed relation and the immutable materialization input must
    /// name the same exact catalog content. A name-only match would permit a
    /// fragment to select another version after desired state changes.
    pub(crate) fn insert_typed_scan(
        &mut self,
        fragment_id: FragmentId,
        node_id: i32,
        scan: PreparedTypedConnectorScan,
    ) -> Result<(), String> {
        if self.typed_scans.contains_key(&(fragment_id, node_id)) {
            return Err(format!(
                "duplicate typed connector scan fragment_id={fragment_id} node_id={node_id}"
            ));
        }
        if scan.prepared.table_scan.plan_node_id() != node_id {
            return Err(format!(
                "typed connector scan fragment_id={fragment_id} node_id={node_id} carries plan node {}",
                scan.prepared.table_scan.plan_node_id()
            ));
        }
        self.typed_scans.insert((fragment_id, node_id), scan);
        Ok(())
    }

    pub(crate) fn typed_scan(
        &self,
        fragment_id: FragmentId,
        node_id: i32,
    ) -> Option<&PreparedTypedConnectorScan> {
        self.typed_scans.get(&(fragment_id, node_id))
    }

    pub(crate) fn typed_scan_for_node(&self, node_id: i32) -> Option<&PreparedTypedConnectorScan> {
        self.typed_scans
            .iter()
            .find_map(|(&(_, candidate), scan)| (candidate == node_id).then_some(scan))
    }

    pub(crate) fn typed_scan_keys(&self) -> impl Iterator<Item = (FragmentId, i32)> + '_ {
        self.typed_scans.keys().copied()
    }

    /// Every typed connector scan of the query, keyed by fragment and plan
    /// node. The execution round drives enumeration from these; preparation
    /// itself never calls a split manager.
    pub(crate) fn typed_scans(
        &self,
    ) -> impl Iterator<Item = (FragmentId, i32, &PreparedTypedConnectorScan)> + '_ {
        self.typed_scans
            .iter()
            .map(|(&(fragment_id, node_id), scan)| (fragment_id, node_id, scan))
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::*;
    use novarocks_types::schema::SqlType;

    #[test]
    fn resolver_trait_object_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync + ?Sized>() {}

        assert_send_sync::<dyn ScanBindingResolver>();
    }

    #[test]
    fn prepared_execution_kind_does_not_retain_the_admission_lease() {
        use std::collections::HashMap;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        let mut materialization = fixture_query_scan_materialization("ice");
        let released = Arc::new(AtomicBool::new(false));
        let released_on_drop = Arc::clone(&released);
        materialization.planning_lease = ConnectorControlPlanningLease::new(
            Arc::new(crate::connector::scan_model::planned_files_fixture_binding(
                "ice",
                HashMap::new(),
                None,
            )),
            move || released_on_drop.store(true, Ordering::SeqCst),
        );
        let admission = ResolvedScanExecution::AdmittedConnectorRead(materialization);

        let kind = admission.prepared_kind();
        drop(admission);

        assert_eq!(kind, PreparedScanExecutionKind::AdmittedConnectorRead);
        assert!(
            released.load(Ordering::SeqCst),
            "projecting native facts must not retain the transient Catalog lease"
        );
    }

    fn planner_column(id: u32, name: &str, data_type: DataType, nullable: bool) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId(id),
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

    fn binding(
        node_id: i32,
        physical_columns: Vec<ResolvedScanColumn>,
        required_reads: Vec<ResolvedReadColumn>,
    ) -> ResolvedScanBinding {
        ResolvedScanBinding {
            node_id,
            execution_kind: PreparedScanExecutionKind::SealedConnectorScan,
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
        assert_eq!(resolved.planner.column_id, ColumnId(17));
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
                    planner_column_id: Some(ColumnId(9)),
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
