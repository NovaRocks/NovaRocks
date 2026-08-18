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

mod dispatch;
mod iceberg;
mod projection;

use std::collections::HashMap;
use std::num::NonZeroU64;
use std::sync::{Arc, Mutex};

use crate::query_execution::preparation::scan::{
    ResolvedReadReason, ResolvedScanExecution, ScanBindingResolver,
};
use novarocks::connector::ConnectorRegistry;
use novarocks::connector::scan_model::{FixtureDeleteFile, FixtureScanFile};
use novarocks_sql::plan_read::{
    DistributedNode, DistributedNodeKind, DistributedPlan, PlanScanNode,
};
use novarocks_sql::planning::catalog::{ConnectorReadTableFacts, materialize_connector_read_table};
use novarocks_sql::planning::query_execution::{
    SqlScanPreparationCategory, SqlScanPreparationFacts, scan_preparation_facts,
};
use novarocks_sql::test_support::{NativeScanFixture, native_scan_plan};

fn prepare_scan_bindings(
    plan: &DistributedPlan,
    connectors: &ConnectorRegistry,
    resolver: Option<&dyn ScanBindingResolver>,
) -> Result<crate::query_execution::preparation::scan::ScanExecutionBindings, String> {
    let controls = novarocks::connector::FixtureControlResolver::new(connectors.clone());
    prepare_scan_bindings_with_controls(plan, &controls, resolver)
}

/// Prepare a tokenized SQL scan against a caller-owned control resolver, for
/// tests that must hold the same resolver across admission and preparation.
fn prepare_scan_bindings_with_controls(
    plan: &DistributedPlan,
    controls: &novarocks::connector::FixtureControlResolver,
    resolver: Option<&dyn ScanBindingResolver>,
) -> Result<crate::query_execution::preparation::scan::ScanExecutionBindings, String> {
    let query_bindings = fixture_query_table_bindings(plan, controls);
    super::prepare_scan_bindings(
        plan,
        controls,
        &novarocks::connector::test_request_context(),
        Some(&query_bindings),
        resolver,
        super::ScanPreparationOptions::single_backend_fixture(),
    )
}

/// The shared fixture allocates the same token that the sealed SQL scan embeds.
/// Concrete SQL source construction remains in SQL test support; Core sees
/// only copied scan facts and supplies the provider admission beside that token.
fn fixture_query_table_bindings(
    plan: &DistributedPlan,
    controls: &novarocks::connector::FixtureControlResolver,
) -> crate::catalog_application::query_bindings::QueryTableBindingStore {
    use crate::catalog_application::query_bindings::{
        QueryScanMaterialization, QueryTableBinding, QueryTableBindingKey, QueryTableBindingStore,
    };
    use novarocks_spi::connector::{
        ConnectorControlResolver, ConnectorInstanceId, ConnectorReadSelector,
        ConnectorTableIdentity, ConnectorTablePlanningFacts, ConnectorTableRequest,
        ConnectorTableResolution,
    };

    fn collect(node: &DistributedNode, facts: &mut Vec<SqlScanPreparationFacts>) {
        if let DistributedNodeKind::Scan(scan) = &node.payload {
            facts.push(scan_preparation_facts(scan));
        }
        for child in &node.children {
            collect(child, facts);
        }
    }

    let mut fixture_facts = Vec::new();
    for fragment in plan.fragments() {
        collect(&fragment.root, &mut fixture_facts);
    }
    // One physical table binding can occur both as a current/locator read and
    // as one or more frozen reads. Preserve every frozen selector before the
    // per-binding fixture admission below coalesces repeated scan facts.
    let frozen_snapshot_ids = fixture_facts
        .iter()
        .filter_map(|facts| {
            facts
                .frozen_snapshot_id()
                .map(|snapshot_id| (facts.binding(), snapshot_id))
        })
        .collect::<Vec<_>>();
    fixture_facts.sort_by_key(|facts| facts.binding().ordinal().get());
    fixture_facts.dedup_by_key(|facts| facts.binding());
    let store = QueryTableBindingStore::try_new_with_scope_for_test(
        NonZeroU64::new(1).expect("fixture scope"),
    );
    for facts in fixture_facts {
        let binding_frozen_snapshot_ids = frozen_snapshot_ids
            .iter()
            .filter_map(|(binding, snapshot_id)| {
                (*binding == facts.binding()).then_some(*snapshot_id)
            })
            .collect::<Vec<_>>();
        if facts.category() == SqlScanPreparationCategory::ConnectorRead {
            // This source kind is supplied by its dedicated resolver tests;
            // no catalog admission is expected before resolver dispatch.
            continue;
        }
        let planning_lease = controls
            .acquire_current(
                &ConnectorInstanceId::parse(facts.identity().catalog())
                    .expect("fixture catalog must be a valid connector instance"),
            )
            .ok();
        if planning_lease.is_none() && facts.category() == SqlScanPreparationCategory::Delta {
            // Resolver-only negative tests deliberately omit connector admission so
            // they can assert the resolver error before generic read planning.
            continue;
        }
        store
        .resolve_or_insert_with_id(
            QueryTableBindingKey::strict_base(
                facts.identity().catalog(),
                facts.identity().namespace(),
                facts.identity().table(),
            ),
            |binding| {
                if binding != facts.binding() {
                    return Err("sealed scan fixture binding token must match Core fixture store".to_string());
                }
                let lease = planning_lease.clone().ok_or_else(|| {
                    "scan fixture must acquire an exact connector lease".to_string()
                })?;
                let metadata = lease
                    .binding()
                    .metadata()
                    .load_table(ConnectorTableRequest {
                        table: ConnectorTableIdentity {
                            instance_id: ConnectorInstanceId::parse(facts.identity().catalog())
                                .expect("fixture catalog must be valid"),
                            namespace: Arc::from(facts.identity().namespace()),
                            table: Arc::from(facts.identity().table()),
                        },
                        resolution: ConnectorTableResolution::StrictBaseTable,
                        context: novarocks::connector::test_request_context(),
                    })
                    .map_err(|error| error.to_string())?;
                let scan_materialization = QueryScanMaterialization {
                    table: metadata.table,
                    schema: metadata.schema,
                    selector: ConnectorReadSelector::Current,
                    statistics_pin: None,
                    planning_lease: lease.clone(),
                };
                let frozen_snapshot_materializations = binding_frozen_snapshot_ids
                    .into_iter()
                    .map(|snapshot_id| {
                        let lease = planning_lease.clone().ok_or_else(|| {
                            "frozen scan fixture must acquire an exact connector lease".to_string()
                        })?;
                        let metadata = lease
                            .binding()
                            .metadata()
                            .load_table(ConnectorTableRequest {
                                table: ConnectorTableIdentity {
                                    instance_id: ConnectorInstanceId::parse(facts.identity().catalog())
                                        .expect("fixture catalog must be valid"),
                                    namespace: Arc::from(facts.identity().namespace()),
                                    table: Arc::from(facts.identity().table()),
                                },
                                resolution: ConnectorTableResolution::StrictBaseTable,
                                context: novarocks::connector::test_request_context(),
                            })
                            .map_err(|error| error.to_string())?;
                        Ok((
                            snapshot_id,
                            QueryScanMaterialization {
                                table: metadata.table,
                                schema: metadata.schema,
                                selector: ConnectorReadSelector::SnapshotId(snapshot_id),
                                statistics_pin: None,
                                planning_lease: lease,
                            },
                        ))
                    })
                    .collect::<Result<std::collections::BTreeMap<_, _>, String>>()?;
                Ok(QueryTableBinding {
                    resolved: materialize_connector_read_table(ConnectorReadTableFacts {
                        catalog: facts.identity().catalog().to_string(),
                        namespace: facts.identity().namespace().to_string(),
                        table: facts.identity().table().to_string(),
                        columns: scan_materialization
                            .schema
                            .fields()
                            .iter()
                            .map(|field| novarocks_catalog::schema::ColumnDef {
                                name: field.name().to_string(),
                                data_type: field.data_type().clone(),
                                nullable: field.is_nullable(),
                                write_default: None,
                                logical_type: None,
                            })
                            .collect(),
                        iceberg_row_lineage_metadata_columns: Vec::new(),
                        schema: scan_materialization.schema.clone(),
                        binding,
                        selector: ConnectorReadSelector::Current,
                        planning_facts: ConnectorTablePlanningFacts::empty(),
                    })
                    .map_err(|error| format!("fixture SQL materialization: {error}"))?
                    .into_resolved_table(),
                    statistics_pin: None,
                    admission: planning_lease
                        .clone()
                        .map(crate::catalog_application::query_bindings::QueryTableBindingAdmission::Exact)
                        .unwrap_or(crate::catalog_application::query_bindings::QueryTableBindingAdmission::Local),
                    scan_materialization: Some(scan_materialization.clone()),
                    mv_target_read: match facts.mv_target() {
                        Some(target)
                            if matches!(
                                facts.category(),
                                SqlScanPreparationCategory::MvTargetState
                                    | SqlScanPreparationCategory::MvTargetLocator
                            ) => Some(
                            crate::catalog_application::query_bindings::MvTargetReadAdmission {
                                full: scan_materialization.clone(),
                                affected_partitions: scan_materialization.clone(),
                                target_table_uuid: target.target_table_uuid().to_string(),
                                frozen_snapshot_id: target.target_snapshot_id(),
                            },
                        ),
                        _ => None,
                    },
                    write_target_admission: None,
                    frozen_snapshot_materializations,
                    admitted_change_scans: std::collections::BTreeMap::new(),
                })
            },
        )
        .expect("fixture query binding");
    }
    store
}

struct StaticResolver {
    execution: ResolvedScanExecution,
}

impl ScanBindingResolver for StaticResolver {
    fn resolve_scan(
        &self,
        _node_id: i32,
        _scan: &PlanScanNode,
    ) -> Result<Option<ResolvedScanExecution>, String> {
        Ok(Some(self.execution.clone()))
    }
}

fn data_file(path: &str) -> FixtureScanFile {
    let mut file = FixtureScanFile::new(path);
    file.partition_spec_id = Some(0);
    file.sequence_number = Some(1);
    file
}

fn equality_delete_file(
    equality_column_names: Vec<&str>,
    equality_field_ids: Vec<i32>,
) -> FixtureDeleteFile {
    FixtureDeleteFile::equality(
        "s3://bucket/eq-delete.parquet",
        &equality_column_names,
        &equality_field_ids,
    )
}

fn registry(files: Vec<FixtureScanFile>) -> ConnectorRegistry {
    let registry = ConnectorRegistry::new();
    novarocks::connector::scan_model::register_planned_files_fixture(
        &registry,
        "test_catalog",
        files,
        None,
    );
    registry
}

/// Register read units per table name, so a test can plan a scan against a
/// table the fixture deliberately has no units for.
fn registry_for_tables(files_by_table: HashMap<String, Vec<FixtureScanFile>>) -> ConnectorRegistry {
    let registry = ConnectorRegistry::new();
    novarocks::connector::scan_model::register_planned_table_files_fixture(
        &registry,
        "test_catalog",
        files_by_table,
        None,
    );
    registry
}

fn recording_registry(
    files: Vec<FixtureScanFile>,
) -> (ConnectorRegistry, Arc<Mutex<Vec<Vec<usize>>>>) {
    let seen_projections = Arc::new(Mutex::new(Vec::new()));
    let registry = ConnectorRegistry::new();
    novarocks::connector::scan_model::register_planned_files_fixture(
        &registry,
        "test_catalog",
        files,
        Some(Arc::clone(&seen_projections)),
    );
    (registry, seen_projections)
}

/// Seal a change-window scan on the neutral read fixture, the way an
/// application does while it still holds the exact lease.
///
/// The scan is minted from its own binding of the same catalog. That is enough
/// for preparation to accept it, because the fixture pins one incarnation per
/// catalog, and it keeps the sealed handle decodable by whichever registration
/// the test later plans against.
fn fixture_sealed_change_scan(
    catalog: &str,
    table: &str,
    from_snapshot_id: i64,
    to_snapshot_id: i64,
) -> novarocks_spi::connector::ConnectorScan {
    use novarocks_spi::connector::{
        ConnectorBatchBudget, ConnectorBeginScanRequest, ConnectorChangeWindow,
        ConnectorControlPlanningLease, ConnectorInstanceId, ConnectorReadPurpose,
        ConnectorScanSelection, ConnectorTableIdentity, ConnectorTableRequest,
        ConnectorTableResolution,
    };

    let lease = ConnectorControlPlanningLease::new(
        Arc::new(
            novarocks::connector::scan_model::planned_files_fixture_binding(
                catalog,
                HashMap::new(),
                None,
            ),
        ),
        || {},
    );
    let context = novarocks::connector::test_request_context();
    let metadata = lease
        .binding()
        .metadata()
        .load_table(ConnectorTableRequest {
            table: ConnectorTableIdentity {
                instance_id: ConnectorInstanceId::parse(catalog).expect("fixture instance ID"),
                namespace: Arc::from("db"),
                table: Arc::from(table),
            },
            resolution: ConnectorTableResolution::StrictBaseTable,
            context: context.clone(),
        })
        .expect("fixture table metadata");
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

fn resolved_delta() -> ResolvedScanExecution {
    ResolvedScanExecution::SealedConnectorScan(fixture_sealed_change_scan(
        "test_catalog",
        "orders",
        6,
        7,
    ))
}

fn resolved_data_delta() -> ResolvedScanExecution {
    resolved_delta()
}
