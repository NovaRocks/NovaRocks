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

//! Collects per-table maintenance facts through the neutral connector surface:
//! snapshot list, current-snapshot summary counters, typed maintenance policy
//! facts, references, the provider-signed compaction count, and the
//! downstream-consumer floor that protects incremental MV lineage.
//!
//! Nothing here interprets a storage format. The provider signs every fact; the
//! frontend owns every policy decision made from them.

use std::collections::BTreeMap;
use std::sync::Arc;

use novarocks_spi::connector::{
    ConnectorInstanceId, ConnectorTableIdentity, ConnectorTableResolution,
};

use crate::mv::persistence::definition::StoredMvDefinition;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SnapshotInfo {
    pub snapshot_id: i64,
    pub timestamp_ms: i64,
}

/// Provider facts only. Frontend owns every policy decision and retry state.
#[derive(Clone, Debug, Default)]
pub struct TableMaintenanceStats {
    pub current_snapshot_id: Option<i64>,
    pub snapshots: Vec<SnapshotInfo>,
    pub total_data_files: Option<u64>,
    pub max_compactable_data_files: Option<u64>,
    pub total_files_size_bytes: Option<u64>,
    pub total_delete_files: Option<u64>,
    /// Typed maintenance policy facts declared by the table. `None` means the
    /// table declares no usable value for that key; defaults and clamping are
    /// policy and belong to the frontend, never to this fact layer.
    pub maintenance_enabled: Option<bool>,
    pub expire_max_snapshot_age_ms: Option<i64>,
    pub expire_min_snapshots_to_keep: Option<u32>,
    pub target_file_size_bytes: Option<i64>,
    pub non_default_reference_count: usize,
    pub downstream_floor_ts_ms: Option<i64>,
    pub downstream_floor_unknown: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DownstreamFloor {
    pub(crate) floor_ts_ms: Option<i64>,
    pub(crate) unknown: bool,
}

/// Minimum consumed-snapshot timestamp across all MV definitions that read
/// `table_fqn` incrementally (committed positions and in-flight pins). A
/// consumer pointing at a snapshot we cannot resolve marks the floor unknown,
/// which blocks expire for safety.
pub(crate) fn downstream_floor(
    definitions: &[StoredMvDefinition],
    table_fqn: &str,
    snapshot_ts_by_id: &BTreeMap<i64, i64>,
) -> DownstreamFloor {
    let mut floor_ts: Option<i64> = None;
    let mut unknown = false;
    let mut consider = |snapshot_id: i64| match snapshot_ts_by_id.get(&snapshot_id) {
        Some(ts) => floor_ts = Some(floor_ts.map_or(*ts, |f| f.min(*ts))),
        None => unknown = true,
    };
    for definition in definitions {
        if let Some(id) = definition.last_refresh_snapshots.get(table_fqn) {
            consider(*id);
        }
        if let Some(id) = definition.refresh_target_snapshots.get(table_fqn) {
            consider(*id);
        }
    }
    DownstreamFloor {
        floor_ts_ms: floor_ts,
        unknown,
    }
}

/// Read one MV storage table's maintenance facts through the neutral surface.
/// `definitions` is the full MV list from the same pass, used for the floor.
///
/// The compaction observation runs first on purpose. Answering it forces the
/// provider to discard its cached table and re-read the catalog, and the
/// provider repopulates that cache with what it read, so the metadata load
/// below observes the very table version the count was taken from. Reversing
/// the two would let the projected facts describe an older table than the
/// count, and would drop the forced refresh this pass has always performed.
/// Read one MV storage table's maintenance facts through explicit frontend
/// control and observation ports. Background policy must retain only these
/// leaves, never the aggregate standalone engine state.
pub fn collect_table_stats_with_ports(
    connector_control: &dyn novarocks_spi::connector::ConnectorControlRegistry,
    storage_observation: &dyn crate::mv::storage_observation::MvStorageObservationPort,
    catalog: &str,
    namespace: &str,
    table: &str,
    definitions: &[StoredMvDefinition],
) -> Result<TableMaintenanceStats, String> {
    let context = crate::connector::connector_request_context(
        None,
        Arc::new(std::sync::atomic::AtomicBool::new(false)),
    )?;
    let instance_id = ConnectorInstanceId::parse(catalog).map_err(|error| error.to_string())?;
    let identity = ConnectorTableIdentity {
        instance_id: instance_id.clone(),
        namespace: Arc::from(namespace),
        table: Arc::from(table),
    };

    let max_compactable_data_files =
        crate::connector::metadata_maintenance::read_max_compactable_data_files(
            connector_control,
            &instance_id,
            identity,
            context.clone(),
        )
        .map_err(|error| {
            format!(
                "observe {catalog}.{namespace}.{table} compaction groups for maintenance: {error}"
            )
        })?;

    let exact_lease =
        crate::connector::acquire_metadata_planning_lease(connector_control, catalog)?;
    let metadata = crate::connector::metadata_load_connector_table_with_planning_lease(
        &exact_lease,
        context.clone(),
        namespace,
        table,
        ConnectorTableResolution::StrictBaseTable,
    )?;
    let observed = storage_observation
        .observe_maintenance_metadata(&exact_lease, &metadata, context)
        .map_err(|error| {
            format!("observe {catalog}.{namespace}.{table} maintenance metadata: {error}")
        })?;

    let snapshots: Vec<SnapshotInfo> = observed
        .snapshots()
        .iter()
        .map(|snapshot| SnapshotInfo {
            snapshot_id: snapshot.snapshot_id,
            timestamp_ms: snapshot.timestamp_ms,
        })
        .collect();
    let snapshot_ts_by_id: BTreeMap<i64, i64> = snapshots
        .iter()
        .map(|snapshot| (snapshot.snapshot_id, snapshot.timestamp_ms))
        .collect();

    let fqn = format!("{catalog}.{namespace}.{table}");
    let floor = downstream_floor(definitions, &fqn, &snapshot_ts_by_id);
    let policy = *observed.policy();

    Ok(TableMaintenanceStats {
        current_snapshot_id: observed.current_snapshot_id(),
        snapshots,
        total_data_files: observed.total_data_files(),
        max_compactable_data_files,
        total_files_size_bytes: observed.total_files_size_bytes(),
        total_delete_files: observed.total_delete_files(),
        maintenance_enabled: policy.maintenance_enabled,
        expire_max_snapshot_age_ms: policy.expire_max_snapshot_age_ms,
        expire_min_snapshots_to_keep: policy.expire_min_snapshots_to_keep,
        target_file_size_bytes: policy.target_file_size_bytes,
        non_default_reference_count: observed.non_default_reference_count(),
        downstream_floor_ts_ms: floor.floor_ts_ms,
        downstream_floor_unknown: floor.unknown,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv::persistence::definition::{StoredMvDefinition, StoredMvRefreshPolicy};
    use std::collections::BTreeMap;

    fn definition_with_consumed(fqn: &str, snapshot_id: i64) -> StoredMvDefinition {
        let mut last_refresh_snapshots = BTreeMap::new();
        last_refresh_snapshots.insert(fqn.to_string(), snapshot_id);
        StoredMvDefinition {
            mv_id: 1,
            select_sql: "SELECT 1".to_string(),
            base_table_refs: vec![fqn.to_string()],
            primary_key_columns: vec![],
            storage_engine: "iceberg".to_string(),
            target_catalog: Some("ice".to_string()),
            target_namespace: Some("analytics".to_string()),
            target_table: Some("mv_x".to_string()),
            schema_contract: None,
            partition_spec: None,
            partition_state_complete: false,
            last_refresh_ms: None,
            last_refresh_rows: None,
            last_refresh_snapshots,
            last_refresh_table_uuids: BTreeMap::new(),
            last_refreshed_iceberg_snapshot_id: None,
            refresh_in_progress: false,
            active_refresh_id: None,
            refresh_target_snapshots: BTreeMap::new(),
            refresh_policy: StoredMvRefreshPolicy::Manual,
            refresh_paused: false,
            refresh_interval_ms: None,
            max_staleness_ms: None,
            last_scheduler_error: None,
            next_refresh_after_ms: None,
            created_at_ms: 0,
        }
    }

    #[test]
    fn floor_is_min_consumed_snapshot_timestamp() {
        let mut ts_by_id = BTreeMap::new();
        ts_by_id.insert(10, 1_000);
        ts_by_id.insert(20, 2_000);
        let defs = vec![
            definition_with_consumed("ice.sales.t", 20),
            definition_with_consumed("ice.sales.t", 10),
        ];
        let floor = downstream_floor(&defs, "ice.sales.t", &ts_by_id);
        assert_eq!(
            floor,
            DownstreamFloor {
                floor_ts_ms: Some(1_000),
                unknown: false
            }
        );
    }

    #[test]
    fn floor_is_none_without_consumers() {
        let defs = vec![definition_with_consumed("ice.sales.other", 10)];
        let floor = downstream_floor(&defs, "ice.sales.t", &BTreeMap::new());
        assert_eq!(
            floor,
            DownstreamFloor {
                floor_ts_ms: None,
                unknown: false
            }
        );
    }

    #[test]
    fn floor_unknown_when_consumed_snapshot_missing_from_metadata() {
        let defs = vec![definition_with_consumed("ice.sales.t", 99)];
        let floor = downstream_floor(&defs, "ice.sales.t", &BTreeMap::new());
        assert!(floor.unknown);
    }

    #[test]
    fn floor_considers_in_progress_refresh_pins() {
        let mut ts_by_id = BTreeMap::new();
        ts_by_id.insert(10, 1_000);
        let mut def = definition_with_consumed("ice.sales.other", 1);
        def.refresh_target_snapshots
            .insert("ice.sales.t".to_string(), 10);
        let floor = downstream_floor(&[def], "ice.sales.t", &ts_by_id);
        assert_eq!(
            floor,
            DownstreamFloor {
                floor_ts_ms: Some(1_000),
                unknown: false
            }
        );
    }

    #[test]
    fn floor_takes_min_across_both_maps_for_same_table() {
        let mut ts_by_id = BTreeMap::new();
        ts_by_id.insert(10, 1_000);
        ts_by_id.insert(20, 2_000);
        // One consumer: committed at snapshot 20, with an in-flight refresh
        // pinned at the older snapshot 10. The floor must reflect the older pin.
        let mut def = definition_with_consumed("ice.sales.t", 20);
        def.refresh_target_snapshots
            .insert("ice.sales.t".to_string(), 10);
        let floor = downstream_floor(&[def], "ice.sales.t", &ts_by_id);
        assert_eq!(
            floor,
            DownstreamFloor {
                floor_ts_ms: Some(1_000),
                unknown: false
            }
        );
    }

    // ---------------------------------------------------------------------
    // Consumption-side evidence over an injected fake provider.
    //
    // `collect_table_stats` reads no storage format itself: it asks the
    // provider for the compaction scalar, loads the table on a planning lease,
    // and projects the injected observation. All three inputs are injectable,
    // so these cases drive them directly and assert the wiring this layer
    // actually owns — verbatim projection, scalar wiring, call order, floor
    // derivation, and fail-closed propagation.
    //
    // Whether a real provider derives those facts correctly is proven where
    // that provider lives: the maintenance projection cases in
    // `novarocks/connector/iceberg/src/storage_inspector.rs` and
    // `novarocks/connector/iceberg/src/catalog_control/metadata_maintenance.rs`.
    // Re-deriving them here would only retest the provider through a longer
    // path.
    // ---------------------------------------------------------------------

    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorControlBinding, ConnectorControlPlanningLease, ConnectorControlRegistry,
        ConnectorError, ConnectorErrorKind, ConnectorExecutionBindingKey,
        ConnectorExecutionDeclaration, ConnectorExecutionDistribution, ConnectorInstanceDescriptor,
        ConnectorInstanceIncarnation, ConnectorMaxCompactableDataFiles,
        ConnectorMaxCompactableDataFilesRequest, ConnectorMetadata, ConnectorMetadataMaintenance,
        ConnectorProviderId, ConnectorRequestContext, ConnectorScanPlanning, ConnectorTableHandle,
        ConnectorTableMetadata, ConnectorTableRequest,
    };

    use crate::mv::storage_observation::{
        MvLakePackageObservation, MvMaintenanceMetadataObservation, MvObservedMaintenancePolicy,
        MvObservedSnapshot, MvRefreshBaseObservation, MvRefreshTargetObservation,
        MvSchemaValidationObservation, MvStorageObservationPort, MvTargetCreationObservation,
    };

    const TEST_CATALOG: &str = "ice";
    const TEST_NAMESPACE: &str = "sales";
    const TEST_TABLE: &str = "orders";

    /// One provider round trip, recorded in the order the pass made it.
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum ProviderCall {
        LoadTable,
        ReadMaxCompactableDataFiles,
    }

    #[derive(Default)]
    struct CallLog(std::sync::Mutex<Vec<ProviderCall>>);

    impl CallLog {
        fn record(&self, call: ProviderCall) {
            self.0.lock().expect("provider call log").push(call);
        }

        fn calls(&self) -> Vec<ProviderCall> {
            self.0.lock().expect("provider call log").clone()
        }
    }

    /// What the fake provider answers for the compaction scalar.
    #[derive(Clone, Copy, Debug)]
    enum CompactionAnswer {
        /// The provider signs a value. `None` means it exposes no such
        /// observation for this table; it is not a zero.
        Signed(Option<u64>),
        /// The provider cannot answer at all.
        Unsupported,
    }

    /// Answers exactly the two provider calls this pass makes, records their
    /// order, and refuses everything else.
    struct FakeProvider {
        descriptor: ConnectorInstanceDescriptor,
        key: ConnectorExecutionBindingKey,
        compaction: CompactionAnswer,
        calls: Arc<CallLog>,
    }

    impl ConnectorMetadata for FakeProvider {
        fn instance_id(&self) -> &ConnectorInstanceId {
            &self.descriptor.instance_id
        }

        fn namespace_exists(
            &self,
            _request: novarocks_spi::connector::ConnectorNamespaceRequest,
        ) -> Result<bool, ConnectorError> {
            unreachable!("a maintenance fact pass does not probe namespaces")
        }

        fn table_exists(&self, _request: ConnectorTableRequest) -> Result<bool, ConnectorError> {
            unreachable!("a maintenance fact pass does not probe table existence")
        }

        fn list_tables(
            &self,
            _request: novarocks_spi::connector::ConnectorListTablesRequest,
        ) -> Result<Vec<ConnectorTableIdentity>, ConnectorError> {
            unreachable!("a maintenance fact pass does not enumerate tables")
        }

        fn load_table(
            &self,
            request: ConnectorTableRequest,
        ) -> Result<ConnectorTableMetadata, ConnectorError> {
            self.calls.record(ProviderCall::LoadTable);
            assert_eq!(
                request.resolution,
                ConnectorTableResolution::StrictBaseTable,
                "maintenance must read the base table, never a provider read alias"
            );
            Ok(ConnectorTableMetadata {
                identity: request.table.clone(),
                schema: Arc::new(arrow::datatypes::Schema::empty()),
                planning_facts: novarocks_spi::connector::ConnectorTablePlanningFacts::empty(),
                definition_facts: novarocks_spi::connector::ConnectorTableDefinitionFacts::empty(),
                version: None,
                statistics_data_version: None,
                table: ConnectorTableHandle::try_new(
                    self.descriptor.instance_id.clone(),
                    Bytes::from_static(b"fake maintenance table"),
                )?,
            })
        }
    }

    impl ConnectorScanPlanning for FakeProvider {
        fn instance_id(&self) -> &ConnectorInstanceId {
            &self.descriptor.instance_id
        }

        fn begin_scan(
            &self,
            _table: &ConnectorTableHandle,
            _request: novarocks_spi::connector::ConnectorBeginScanRequest,
        ) -> Result<novarocks_spi::connector::ConnectorScan, ConnectorError> {
            unreachable!("a maintenance fact pass does not plan scans")
        }

        fn plan_splits(
            &self,
            _scan: &novarocks_spi::connector::ConnectorScanHandle,
            _request: novarocks_spi::connector::ConnectorSplitPlanningRequest,
        ) -> Result<novarocks_spi::connector::ConnectorSplitPlanningResult, ConnectorError>
        {
            unreachable!("a maintenance fact pass does not plan scans")
        }
    }

    impl ConnectorExecutionDistribution for FakeProvider {
        fn declaration(
            &self,
            _context: &ConnectorRequestContext,
        ) -> Result<ConnectorExecutionDeclaration, ConnectorError> {
            unreachable!("maintenance facts never cross the execution boundary")
        }
    }

    impl ConnectorMetadataMaintenance for FakeProvider {
        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }

        fn binding_key(&self) -> &ConnectorExecutionBindingKey {
            &self.key
        }

        fn plan_maintenance(
            &self,
            _request: novarocks_spi::connector::ConnectorMetadataMaintenancePlanningRequest,
        ) -> Result<novarocks_spi::connector::ConnectorMetadataMaintenancePlan, ConnectorError>
        {
            unreachable!("an observation must not plan")
        }

        fn execute(
            &self,
            _request: novarocks_spi::connector::ConnectorMetadataMaintenanceExecuteRequest,
        ) -> Result<
            novarocks_spi::connector::ExternalMutationOutcome<
                novarocks_spi::connector::ConnectorMetadataMaintenanceReceipt,
            >,
            ConnectorError,
        > {
            unreachable!("an observation must not execute")
        }

        fn reconcile(
            &self,
            _request: novarocks_spi::connector::ConnectorMetadataMaintenanceReconcileRequest,
        ) -> Result<
            novarocks_spi::connector::ExternalMutationOutcome<
                novarocks_spi::connector::ConnectorMetadataMaintenanceReceipt,
            >,
            ConnectorError,
        > {
            unreachable!("an observation must not reconcile")
        }

        fn read_max_compactable_data_files(
            &self,
            request: ConnectorMaxCompactableDataFilesRequest,
        ) -> Result<ConnectorMaxCompactableDataFiles, ConnectorError> {
            self.calls.record(ProviderCall::ReadMaxCompactableDataFiles);
            assert_eq!(request.table.owner(), &self.descriptor.instance_id);
            match self.compaction {
                CompactionAnswer::Signed(value) => Ok(ConnectorMaxCompactableDataFiles::new(value)),
                CompactionAnswer::Unsupported => Err(ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    "fake provider observes no compactable data file groups",
                )),
            }
        }
    }

    /// The exact facts the injected observation hands back, so a projection
    /// that silently drops, defaults, or clamps one cannot pass.
    #[derive(Clone, Debug, Default)]
    struct FakeObservation {
        current_snapshot_id: Option<i64>,
        snapshots: Vec<MvObservedSnapshot>,
        non_default_reference_count: usize,
        total_data_files: Option<u64>,
        total_delete_files: Option<u64>,
        total_files_size_bytes: Option<u64>,
        policy: MvObservedMaintenancePolicy,
    }

    impl MvStorageObservationPort for FakeObservation {
        fn observe_created_target(
            &self,
            _exact_lease: &ConnectorControlPlanningLease,
            _metadata: &ConnectorTableMetadata,
            _context: ConnectorRequestContext,
        ) -> Result<MvTargetCreationObservation, ConnectorError> {
            unreachable!("a maintenance fact pass observes maintenance metadata only")
        }

        fn observe_schema_validation(
            &self,
            _exact_lease: &ConnectorControlPlanningLease,
            _metadata: &ConnectorTableMetadata,
            _context: ConnectorRequestContext,
        ) -> Result<MvSchemaValidationObservation, ConnectorError> {
            unreachable!("a maintenance fact pass observes maintenance metadata only")
        }

        fn observe_lake_package(
            &self,
            _exact_lease: &ConnectorControlPlanningLease,
            _metadata: &ConnectorTableMetadata,
            _context: ConnectorRequestContext,
        ) -> Result<Option<MvLakePackageObservation>, ConnectorError> {
            unreachable!("a maintenance fact pass observes maintenance metadata only")
        }

        fn observe_refresh_base(
            &self,
            _exact_lease: &ConnectorControlPlanningLease,
            _metadata: &ConnectorTableMetadata,
            _context: ConnectorRequestContext,
        ) -> Result<MvRefreshBaseObservation, ConnectorError> {
            unreachable!("a maintenance fact pass observes maintenance metadata only")
        }

        fn observe_refresh_target(
            &self,
            _exact_lease: &ConnectorControlPlanningLease,
            _metadata: &ConnectorTableMetadata,
            _context: ConnectorRequestContext,
        ) -> Result<MvRefreshTargetObservation, ConnectorError> {
            unreachable!("a maintenance fact pass observes maintenance metadata only")
        }

        fn observe_maintenance_metadata(
            &self,
            _exact_lease: &ConnectorControlPlanningLease,
            _metadata: &ConnectorTableMetadata,
            context: ConnectorRequestContext,
        ) -> Result<MvMaintenanceMetadataObservation, ConnectorError> {
            MvMaintenanceMetadataObservation::try_new(
                self.current_snapshot_id,
                self.snapshots.clone(),
                self.non_default_reference_count,
                self.total_data_files,
                self.total_delete_files,
                self.total_files_size_bytes,
                self.policy,
                &context,
            )
        }
    }

    /// Compose explicit provider-control and observation leaves, and hand back
    /// the shared log both fakes write their call order into.
    fn fixture(
        observation: FakeObservation,
        compaction: CompactionAnswer,
    ) -> (
        Arc<crate::query_execution::compiler::TestConnectorControlRegistry>,
        Arc<FakeObservation>,
        Arc<CallLog>,
    ) {
        let instance_id = ConnectorInstanceId::parse(TEST_CATALOG).expect("fixture instance ID");
        let incarnation = ConnectorInstanceIncarnation::from_bytes([9; 16]);
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("mv-maintenance-fixture")
                .expect("fixture provider ID"),
            instance_id: instance_id.clone(),
        };
        let calls = Arc::new(CallLog::default());
        let provider = Arc::new(FakeProvider {
            descriptor: descriptor.clone(),
            key: ConnectorExecutionBindingKey {
                instance_id,
                incarnation,
            },
            compaction,
            calls: Arc::clone(&calls),
        });
        let binding =
            ConnectorControlBinding::try_new_with_all_capabilities_and_metadata_maintenance(
                descriptor,
                incarnation,
                provider.clone(),
                provider.clone(),
                provider.clone(),
                None,
                None,
                Some(provider),
                None,
                None,
            )
            .expect("fake control binding");
        let control =
            Arc::new(crate::query_execution::compiler::TestConnectorControlRegistry::default());
        control.register(binding).expect("register fake binding");
        (control, Arc::new(observation), calls)
    }

    #[test]
    fn collect_table_stats_projects_every_observed_fact_verbatim() {
        let (control, observation, _calls) = fixture(
            FakeObservation {
                current_snapshot_id: Some(20),
                snapshots: vec![
                    MvObservedSnapshot {
                        snapshot_id: 10,
                        timestamp_ms: 1_000,
                    },
                    MvObservedSnapshot {
                        snapshot_id: 20,
                        timestamp_ms: 7_000,
                    },
                ],
                non_default_reference_count: 3,
                total_data_files: Some(11),
                total_delete_files: Some(2),
                total_files_size_bytes: Some(4_096),
                policy: MvObservedMaintenancePolicy {
                    maintenance_enabled: Some(false),
                    expire_max_snapshot_age_ms: Some(3_600_000),
                    expire_min_snapshots_to_keep: Some(4),
                    target_file_size_bytes: Some(1_048_576),
                },
            },
            CompactionAnswer::Signed(Some(9)),
        );

        let stats = collect_table_stats_with_ports(
            control.as_ref(),
            observation.as_ref(),
            TEST_CATALOG,
            TEST_NAMESPACE,
            TEST_TABLE,
            &[],
        )
        .expect("collect maintenance stats");

        assert_eq!(stats.current_snapshot_id, Some(20));
        assert_eq!(
            stats.snapshots,
            vec![
                SnapshotInfo {
                    snapshot_id: 10,
                    timestamp_ms: 1_000,
                },
                SnapshotInfo {
                    snapshot_id: 20,
                    timestamp_ms: 7_000,
                },
            ]
        );
        assert_eq!(stats.non_default_reference_count, 3);
        assert_eq!(stats.total_data_files, Some(11));
        assert_eq!(stats.total_delete_files, Some(2));
        assert_eq!(stats.total_files_size_bytes, Some(4_096));
        assert_eq!(stats.maintenance_enabled, Some(false));
        assert_eq!(stats.expire_max_snapshot_age_ms, Some(3_600_000));
        assert_eq!(stats.expire_min_snapshots_to_keep, Some(4));
        assert_eq!(stats.target_file_size_bytes, Some(1_048_576));
        // The provider-signed scalar and the summary counter are different
        // facts. Each must arrive unchanged, and neither may be reconciled
        // against the other: the values here deliberately disagree.
        assert_eq!(stats.max_compactable_data_files, Some(9));
        assert_ne!(stats.max_compactable_data_files, stats.total_data_files);
        // No MV consumes this table, so nothing pins retention.
        assert_eq!(stats.downstream_floor_ts_ms, None);
        assert!(!stats.downstream_floor_unknown);
    }

    #[test]
    fn collect_table_stats_keeps_undeclared_facts_absent() {
        // A table that declares nothing and a provider that exposes no
        // compaction observation. Every value must stay absent: a default or a
        // zero injected here would be a policy decision made in a fact layer.
        let (control, observation, _calls) =
            fixture(FakeObservation::default(), CompactionAnswer::Signed(None));

        let stats = collect_table_stats_with_ports(
            control.as_ref(),
            observation.as_ref(),
            TEST_CATALOG,
            TEST_NAMESPACE,
            TEST_TABLE,
            &[],
        )
        .expect("collect maintenance stats");

        assert_eq!(stats.current_snapshot_id, None);
        assert!(stats.snapshots.is_empty());
        assert_eq!(stats.total_data_files, None);
        assert_eq!(stats.total_delete_files, None);
        assert_eq!(stats.total_files_size_bytes, None);
        assert_eq!(stats.max_compactable_data_files, None);
        assert_eq!(stats.maintenance_enabled, None);
        assert_eq!(stats.expire_max_snapshot_age_ms, None);
        assert_eq!(stats.expire_min_snapshots_to_keep, None);
        assert_eq!(stats.target_file_size_bytes, None);
        assert_eq!(stats.non_default_reference_count, 0);
    }

    #[test]
    fn collect_table_stats_observes_compaction_before_the_projected_metadata_load() {
        let (control, observation, calls) = fixture(
            FakeObservation::default(),
            CompactionAnswer::Signed(Some(2)),
        );

        let stats = collect_table_stats_with_ports(
            control.as_ref(),
            observation.as_ref(),
            TEST_CATALOG,
            TEST_NAMESPACE,
            TEST_TABLE,
            &[],
        )
        .expect("collect maintenance stats");
        assert_eq!(stats.max_compactable_data_files, Some(2));

        // Answering the observation forces the provider to discard its cached
        // table and re-read the catalog, and it repopulates that cache with
        // what it read. The metadata load that feeds every projected fact must
        // therefore come after it; reversing the two would let the facts
        // describe an older table than the count.
        let calls = calls.calls();
        assert_eq!(
            calls,
            vec![
                // Resolving the table handle the observation needs.
                ProviderCall::LoadTable,
                ProviderCall::ReadMaxCompactableDataFiles,
                // The planning-lease load whose metadata is projected.
                ProviderCall::LoadTable,
            ]
        );
        let observed = calls
            .iter()
            .position(|call| *call == ProviderCall::ReadMaxCompactableDataFiles)
            .expect("the compaction observation ran");
        let projected = calls
            .iter()
            .rposition(|call| *call == ProviderCall::LoadTable)
            .expect("the planning-lease load ran");
        assert!(
            observed < projected,
            "the compaction observation must precede the projected metadata load: {calls:?}"
        );
    }

    #[test]
    fn collect_table_stats_floors_retention_at_the_snapshot_an_mv_consumes() {
        // The two snapshots carry different timestamps, so a floor that
        // reported the oldest retained snapshot instead of the consumed one
        // would fail here rather than pass by coincidence.
        let observation = FakeObservation {
            current_snapshot_id: Some(20),
            snapshots: vec![
                MvObservedSnapshot {
                    snapshot_id: 10,
                    timestamp_ms: 1_000,
                },
                MvObservedSnapshot {
                    snapshot_id: 20,
                    timestamp_ms: 7_000,
                },
            ],
            ..FakeObservation::default()
        };
        let fqn = format!("{TEST_CATALOG}.{TEST_NAMESPACE}.{TEST_TABLE}");

        // A consumer committed at the newest snapshot floors retention there,
        // not at the oldest snapshot the table still retains.
        let (control, storage_observation, _calls) =
            fixture(observation.clone(), CompactionAnswer::Signed(None));
        let consumer = definition_with_consumed(&fqn, 20);
        let stats = collect_table_stats_with_ports(
            control.as_ref(),
            storage_observation.as_ref(),
            TEST_CATALOG,
            TEST_NAMESPACE,
            TEST_TABLE,
            std::slice::from_ref(&consumer),
        )
        .expect("collect maintenance stats");
        assert_eq!(stats.downstream_floor_ts_ms, Some(7_000));
        assert!(!stats.downstream_floor_unknown);

        // A consumer pinned to a snapshot this table cannot resolve leaves the
        // floor unknown, which is what blocks expire.
        let (control, storage_observation, _calls) =
            fixture(observation, CompactionAnswer::Signed(None));
        let missing = definition_with_consumed(&fqn, 99);
        let stats = collect_table_stats_with_ports(
            control.as_ref(),
            storage_observation.as_ref(),
            TEST_CATALOG,
            TEST_NAMESPACE,
            TEST_TABLE,
            std::slice::from_ref(&missing),
        )
        .expect("collect maintenance stats");
        assert!(stats.downstream_floor_unknown);
        assert_eq!(stats.downstream_floor_ts_ms, None);
    }

    #[test]
    fn collect_table_stats_fails_closed_when_the_provider_cannot_sign_the_scalar() {
        let (control, observation, calls) =
            fixture(FakeObservation::default(), CompactionAnswer::Unsupported);

        let error = collect_table_stats_with_ports(
            control.as_ref(),
            observation.as_ref(),
            TEST_CATALOG,
            TEST_NAMESPACE,
            TEST_TABLE,
            &[],
        )
        .expect_err("an unanswerable observation must not be downgraded to a fact");

        assert!(
            error.contains("compaction groups for maintenance"),
            "unexpected error: {error}"
        );
        // The pass stops at the failure. It never reaches the planning-lease
        // load, so it cannot report the remaining facts with a silently absent
        // `max_compactable_data_files`.
        assert_eq!(
            calls.calls(),
            vec![
                ProviderCall::LoadTable,
                ProviderCall::ReadMaxCompactableDataFiles,
            ]
        );
    }
}
