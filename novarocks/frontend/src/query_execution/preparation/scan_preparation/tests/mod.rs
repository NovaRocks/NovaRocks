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

use std::num::NonZeroU64;
use std::sync::Arc;

use crate::connector::FixtureConnectorRegistry;
use crate::connector::scan_model::{FixtureDeleteFile, FixtureScanFile};
use crate::query_execution::preparation::scan::{
    ResolvedReadReason, ResolvedScanExecution, ScanBindingResolver,
};
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
    connectors: &FixtureConnectorRegistry,
    resolver: Option<&dyn ScanBindingResolver>,
) -> Result<crate::query_execution::preparation::scan::ScanExecutionBindings, String> {
    let controls = crate::connector::FixtureControlResolver::new(connectors.clone());
    prepare_scan_bindings_with_controls(plan, &controls, resolver)
}

/// Prepare a plan whose scans must offer the connector these runtime filters.
fn prepare_scan_bindings_with_runtime_filters(
    plan: &DistributedPlan,
    connectors: &FixtureConnectorRegistry,
    runtime_filter_scans: &[novarocks_sql::planning::query_execution::SqlRuntimeFilterSourceScanRequest],
) -> Result<crate::query_execution::preparation::scan::ScanExecutionBindings, String> {
    let controls = crate::connector::FixtureControlResolver::new(connectors.clone());
    let query_bindings = fixture_query_table_bindings(plan, &controls);
    let typed = fixture_control_role_host(plan, &controls);
    super::prepare_scan_bindings(
        plan,
        &controls,
        &crate::connector::test_request_context(),
        Some(&query_bindings),
        None,
        &fixture_scan_preparation_options(typed),
        runtime_filter_scans,
    )
}

/// Prepare a plan whose change-window lane resolves through the production
/// query-local resolver, exactly as a refresh does.
fn prepare_scan_bindings_with_delta_resolver(
    plan: &DistributedPlan,
    connectors: &FixtureConnectorRegistry,
) -> Result<crate::query_execution::preparation::scan::ScanExecutionBindings, String> {
    let controls = crate::connector::FixtureControlResolver::new(connectors.clone());
    let query_bindings = fixture_query_table_bindings(plan, &controls);
    let typed = fixture_control_role_host(plan, &controls);
    let resolver = crate::query_execution::planning::delta_scan::QueryTableBindingScanResolver::new(
        &query_bindings,
    );
    super::prepare_scan_bindings(
        plan,
        &controls,
        &crate::connector::test_request_context(),
        Some(&query_bindings),
        Some(&resolver),
        &fixture_scan_preparation_options(typed),
        &[],
    )
}

/// Prepare a tokenized SQL scan against a caller-owned control resolver, for
/// tests that must hold the same resolver across admission and preparation.
fn prepare_scan_bindings_with_controls(
    plan: &DistributedPlan,
    controls: &crate::connector::FixtureControlResolver,
    resolver: Option<&dyn ScanBindingResolver>,
) -> Result<crate::query_execution::preparation::scan::ScanExecutionBindings, String> {
    let query_bindings = fixture_query_table_bindings(plan, controls);
    let typed = fixture_control_role_host(plan, controls);
    super::prepare_scan_bindings(
        plan,
        controls,
        &crate::connector::test_request_context(),
        Some(&query_bindings),
        resolver,
        &fixture_scan_preparation_options(typed),
        &[],
    )
}

/// Fixture options carrying a typed control registry, mirroring how the
/// composition root hands one to production preparation.
fn fixture_scan_preparation_options(
    typed: Arc<crate::connector::ConnectorControlHost>,
) -> super::ScanPreparationOptions {
    super::ScanPreparationOptions::single_backend_fixture().with_typed_connector_control(
        typed,
        novarocks_spi::connector::read_stack::ConnectorSession::try_new(
            "fixture-query",
            "fixture-user",
            "UTC",
            "en_US",
            std::time::SystemTime::UNIX_EPOCH,
        )
        .expect("fixture connector session"),
    )
}

/// Install one typed control per catalog instance the sealed plan scans.
///
/// The handle is the exact desired-state identity the fixture lease already
/// froze, so the registry answers precisely what production would answer.
fn fixture_control_role_host(
    plan: &DistributedPlan,
    controls: &crate::connector::FixtureControlResolver,
) -> Arc<crate::connector::ConnectorControlHost> {
    use novarocks_spi::connector::{ConnectorControlResolver, ConnectorInstanceId};

    let registry = Arc::new(crate::connector::ConnectorControlHost::new());
    let mut facts = Vec::new();
    for fragment in plan.fragments() {
        fn collect(node: &DistributedNode, facts: &mut Vec<SqlScanPreparationFacts>) {
            if let DistributedNodeKind::Scan(scan) = &node.payload {
                facts.push(scan_preparation_facts(scan));
            }
            for child in &node.children {
                collect(child, facts);
            }
        }
        collect(&fragment.root, &mut facts);
    }
    let mut installed = std::collections::BTreeSet::new();
    for facts in facts {
        let catalog = facts.identity().catalog().to_string();
        if !installed.insert(catalog.clone()) {
            continue;
        }
        let Ok(instance_id) = ConnectorInstanceId::parse(&catalog) else {
            continue;
        };
        let Ok(lease) = controls.acquire_current(&instance_id) else {
            continue;
        };
        let catalog_handle = lease
            .binding()
            .catalog_handle()
            .expect("fixture control binding has a catalog handle")
            .clone();
        let control = Arc::new(FixtureTypedControl::new(catalog_handle.clone()));
        let adapter = Arc::new(
            novarocks_spi::connector::read_stack::adapter::ReadRuntimeAdapter::new(Arc::clone(
                &control,
            )),
        );
        let properties = novarocks_spi::connector::NormalizedCatalogProperties::try_new(
            lease
                .binding()
                .catalog_properties()
                .expect("fixture control binding has catalog properties")
                .clone(),
        )
        .expect("fixture normalized properties");
        let role = novarocks_spi::connector::ConnectorControlRoleBinding::try_new(
            properties,
            Arc::clone(lease.binding()),
            Some(novarocks_spi::connector::ConnectorControlReadBinding::new(
                Arc::clone(&adapter) as _,
                adapter as _,
                None,
                Arc::new(FixtureReadCodec),
            )),
            None,
        )
        .expect("fixture control role binding");
        registry
            .register_role_binding(role)
            .expect("fixture read control install");
    }
    registry
}

/// A provider-only type family used to exercise SPI planning without exposing
/// any protocol carrier to the frontend fixture.
#[derive(Clone, Debug)]
struct FixtureTable {
    name: novarocks_spi::connector::read_stack::SchemaTableName,
    snapshot_id: i64,
    pinned_paths: Option<Vec<String>>,
    change_window: Option<(i64, i64)>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct FixtureColumn {
    id: i32,
    name: String,
}

impl novarocks_spi::connector::read_stack::ColumnHandle for FixtureColumn {}

#[derive(Clone, Debug)]
struct FixtureTransaction;

/// A typed control that freezes relations in a private provider family and
/// binds fixture columns by name. It declines all pushdown and enumerates no
/// split, so a test observes exactly what preparation itself decided.
struct FixtureTypedControl {
    descriptor: novarocks_spi::connector::ConnectorInstanceDescriptor,
    catalog_handle: novarocks_spi::connector::CatalogHandle,
    pinned_requests: std::sync::Mutex<Vec<novarocks_spi::connector::ConnectorPinnedFileSet>>,
}

impl FixtureTypedControl {
    const COLUMN_NAMES: [&'static str; 14] = [
        "id",
        "category",
        "v",
        "agg",
        "extra",
        "k",
        "__branch_id__",
        "__nova_join_row_key",
        "__nova_base_row_id",
        "_file",
        "_pos",
        "_row_id",
        "_last_updated_sequence_number",
        "__change_op",
    ];

    fn new(catalog_handle: novarocks_spi::connector::CatalogHandle) -> Self {
        Self {
            descriptor: novarocks_spi::connector::ConnectorInstanceDescriptor {
                provider_id: novarocks_spi::connector::ConnectorProviderId::parse("fixture")
                    .expect("fixture provider ID"),
                instance_id: catalog_handle.catalog_name().clone(),
            },
            catalog_handle,
            pinned_requests: std::sync::Mutex::new(Vec::new()),
        }
    }

    fn table(
        name: &novarocks_spi::connector::read_stack::SchemaTableName,
        snapshot_id: i64,
    ) -> FixtureTable {
        FixtureTable {
            name: name.clone(),
            snapshot_id,
            pinned_paths: None,
            change_window: None,
        }
    }

    fn bindings()
    -> Vec<novarocks_spi::connector::read_stack::adapter::ProviderReadColumnBinding<FixtureColumn>>
    {
        Self::COLUMN_NAMES
            .iter()
            .enumerate()
            .map(|(ordinal, name)| {
                novarocks_spi::connector::read_stack::adapter::ProviderReadColumnBinding::new(
                    *name,
                    FixtureColumn {
                        id: ordinal as i32 + 1,
                        name: (*name).to_owned(),
                    },
                    false,
                )
            })
            .collect()
    }
}

impl novarocks_spi::connector::read_stack::adapter::ProviderReadRuntime for FixtureTypedControl {
    type Table = FixtureTable;
    type Column = FixtureColumn;
    type Transaction = FixtureTransaction;
    type Split = FixtureSplit;

    fn descriptor(&self) -> &novarocks_spi::connector::ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn catalog_handle(&self) -> &novarocks_spi::connector::CatalogHandle {
        &self.catalog_handle
    }

    fn transaction(&self) -> Self::Transaction {
        FixtureTransaction
    }
}

#[derive(Debug)]
struct FixtureSplit;

impl novarocks_spi::connector::read_stack::ConnectorSplit for FixtureSplit {
    fn retained_size_in_bytes(&self) -> u64 {
        0
    }
}

impl novarocks_spi::connector::read_stack::adapter::ProviderReadMetadata for FixtureTypedControl {
    fn get_table_handle(
        &self,
        _session: &novarocks_spi::connector::read_stack::ConnectorSession,
        name: &novarocks_spi::connector::read_stack::SchemaTableName,
        version: novarocks_spi::connector::read_stack::ConnectorReadRelationVersion,
        _reference: Option<&str>,
    ) -> Result<Option<FixtureTable>, novarocks_spi::connector::ConnectorError> {
        let snapshot_id = match version {
            novarocks_spi::connector::read_stack::ConnectorReadRelationVersion::Current
            | novarocks_spi::connector::read_stack::ConnectorReadRelationVersion::Reference => 1,
            novarocks_spi::connector::read_stack::ConnectorReadRelationVersion::SnapshotId(id) => {
                id
            }
        };
        Ok(Some(Self::table(name, snapshot_id)))
    }

    fn get_pinned_file_set_handle(
        &self,
        _session: &novarocks_spi::connector::read_stack::ConnectorSession,
        name: &novarocks_spi::connector::read_stack::SchemaTableName,
        pinned: &novarocks_spi::connector::ConnectorPinnedFileSet,
    ) -> Result<Option<FixtureTable>, novarocks_spi::connector::ConnectorError> {
        self.pinned_requests
            .lock()
            .expect("fixture pinned request lock")
            .push(pinned.clone());
        let mut table = Self::table(name, pinned.version_ordinal());
        table.pinned_paths = Some(pinned.files().iter().map(ToString::to_string).collect());
        Ok(Some(table))
    }

    fn get_column_bindings(
        &self,
        _session: &novarocks_spi::connector::read_stack::ConnectorSession,
        _table: &FixtureTable,
    ) -> Result<
        Vec<
            novarocks_spi::connector::read_stack::adapter::ProviderReadColumnBinding<FixtureColumn>,
        >,
        novarocks_spi::connector::ConnectorError,
    > {
        Ok(Self::bindings())
    }

    fn apply_filter(
        &self,
        _session: &novarocks_spi::connector::read_stack::ConnectorSession,
        _table: &FixtureTable,
        _constraint: &novarocks_spi::connector::read_stack::Constraint<FixtureColumn>,
    ) -> Result<
        Option<
            novarocks_spi::connector::read_stack::adapter::ProviderReadFilterApplication<
                FixtureTable,
                FixtureColumn,
            >,
        >,
        novarocks_spi::connector::ConnectorError,
    > {
        Ok(None)
    }

    fn apply_projection(
        &self,
        _session: &novarocks_spi::connector::read_stack::ConnectorSession,
        _table: &FixtureTable,
        _assignments: &[novarocks_spi::connector::read_stack::Assignment<FixtureColumn>],
    ) -> Result<Option<FixtureTable>, novarocks_spi::connector::ConnectorError> {
        Ok(None)
    }

    fn apply_limit(
        &self,
        _session: &novarocks_spi::connector::read_stack::ConnectorSession,
        _table: &FixtureTable,
        _limit: u64,
    ) -> Result<
        Option<
            novarocks_spi::connector::read_stack::adapter::ProviderReadLimitApplication<
                FixtureTable,
            >,
        >,
        novarocks_spi::connector::ConnectorError,
    > {
        Ok(None)
    }

    fn get_system_table_plan(
        &self,
        _session: &novarocks_spi::connector::read_stack::ConnectorSession,
        _name: &novarocks_spi::connector::read_stack::SchemaTableName,
    ) -> Result<
        Option<
            novarocks_spi::connector::read_stack::adapter::ProviderReadSystemTablePlan<
                FixtureTable,
            >,
        >,
        novarocks_spi::connector::ConnectorError,
    > {
        Ok(None)
    }

    fn get_change_window_plan(
        &self,
        _session: &novarocks_spi::connector::read_stack::ConnectorSession,
        name: &novarocks_spi::connector::read_stack::SchemaTableName,
        window: novarocks_spi::connector::read_stack::ConnectorReadChangeWindow,
    ) -> Result<Option<FixtureTable>, novarocks_spi::connector::ConnectorError> {
        let mut table = Self::table(name, window.to_snapshot_id());
        table.change_window = Some((window.from_snapshot_id(), window.to_snapshot_id()));
        Ok(Some(table))
    }

    fn get_table_execute_plan(
        &self,
        _session: &novarocks_spi::connector::read_stack::ConnectorSession,
        _name: &novarocks_spi::connector::read_stack::SchemaTableName,
        _procedure: novarocks_spi::connector::read_stack::ConnectorReadTableExecuteProcedure,
    ) -> Result<Option<FixtureTable>, novarocks_spi::connector::ConnectorError> {
        Ok(None)
    }
}

impl novarocks_spi::connector::read_stack::adapter::ProviderReadSplitManager
    for FixtureTypedControl
{
    fn get_splits(
        &self,
        _session: &novarocks_spi::connector::read_stack::ConnectorSession,
        _table: &FixtureTable,
        _columns: &[novarocks_spi::connector::read_stack::Assignment<FixtureColumn>],
        _dynamic_filter_columns: &std::collections::BTreeSet<FixtureColumn>,
        _constraint: &novarocks_spi::connector::read_stack::Constraint<FixtureColumn>,
    ) -> Result<
        Box<dyn novarocks_spi::connector::read_stack::adapter::ProviderReadSplitSource<Self>>,
        novarocks_spi::connector::ConnectorError,
    > {
        Err(novarocks_spi::connector::ConnectorError::new(
            novarocks_spi::connector::ConnectorErrorKind::Unsupported,
            "the fixture read control enumerates no split",
        ))
    }
}

/// The codec is intentionally inert: scan planning holds only SPI values and
/// must never invoke a codec before native egress.
struct FixtureReadCodec;

impl novarocks_spi::connector::ConnectorReadWireEncoder for FixtureReadCodec {
    fn owner(&self) -> &str {
        "fixture"
    }

    fn encode_relation_payload(
        &self,
        _relation: &novarocks_spi::connector::read_stack::ConnectorReadRelation,
    ) -> Result<
        novarocks_spi::connector::ConnectorReadRelationPayload,
        novarocks_spi::connector::ConnectorCodecError,
    > {
        unreachable!("scan preparation fixture must not encode wire relations")
    }

    fn encode_column_payload(
        &self,
        _column: &novarocks_spi::connector::read_stack::ConnectorReadColumnHandle,
    ) -> Result<
        novarocks_spi::connector::ConnectorEncodedPayload,
        novarocks_spi::connector::ConnectorCodecError,
    > {
        unreachable!("scan preparation fixture must not encode wire columns")
    }

    fn encode_transaction_payload(
        &self,
        _transaction: &novarocks_spi::connector::read_stack::ConnectorReadTransactionHandle,
    ) -> Result<
        novarocks_spi::connector::ConnectorEncodedPayload,
        novarocks_spi::connector::ConnectorCodecError,
    > {
        unreachable!("scan preparation fixture must not encode wire transactions")
    }

    fn encode_split_payload(
        &self,
        _split: &novarocks_spi::connector::read_stack::ConnectorReadSplit,
    ) -> Result<
        novarocks_spi::connector::ConnectorReadSplitPayload,
        novarocks_spi::connector::ConnectorCodecError,
    > {
        unreachable!("scan preparation fixture must not encode wire splits")
    }
}

/// The shared fixture allocates the same token that the sealed SQL scan embeds.
/// Concrete SQL source construction remains in SQL test support; Core sees
/// only copied scan facts and supplies the provider admission beside that token.
fn fixture_query_table_bindings(
    plan: &DistributedPlan,
    controls: &crate::connector::FixtureControlResolver,
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
                        context: crate::connector::test_request_context(),
                    })
                    .map_err(|error| error.to_string())?;
                let scan_materialization = QueryScanMaterialization {
                    table: metadata.table,
                    catalog_handle: lease
                        .binding()
                        .catalog_handle()
                        .map_err(|error| error.to_string())?
                        .clone(),
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
                                context: crate::connector::test_request_context(),
                            })
                            .map_err(|error| error.to_string())?;
                        Ok((
                            snapshot_id,
                            QueryScanMaterialization {
                                table: metadata.table,
                                catalog_handle: lease
                                    .binding()
                                    .catalog_handle()
                                    .map_err(|error| error.to_string())?
                                    .clone(),
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
                            .map(|field| novarocks_types::schema::ColumnDef {
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

fn registry(files: Vec<FixtureScanFile>) -> FixtureConnectorRegistry {
    let registry = FixtureConnectorRegistry::new();
    crate::connector::scan_model::register_planned_files_fixture(
        &registry,
        "test_catalog",
        files,
        None,
    );
    registry
}
