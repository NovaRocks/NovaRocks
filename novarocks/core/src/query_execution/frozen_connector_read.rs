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

//! Generic admission for one provider-frozen connector source.
//!
//! The caller already owns an opaque table handle and the exact control
//! generation that froze it. This module turns those facts into an ordinary
//! connector read without acquiring current metadata or any provider-specific
//! lifecycle capability.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Mutex;

use arrow::datatypes::SchemaRef;
use novarocks_spi::connector::{
    ConnectorBatchBudget, ConnectorBeginScanRequest, ConnectorControlPlanningLease, ConnectorError,
    ConnectorErrorKind, ConnectorExecutionBindingKey, ConnectorReadSelector,
    ConnectorRequestContext, ConnectorScanSelection, ConnectorSplitPlanningRequest,
    ConnectorTableHandle,
};

use crate::catalog_application::query_bindings::{
    QueryTableBinding, QueryTableBindingAdmission, QueryTableBindingKey, QueryTableBindingStore,
};
use crate::catalog_application::query_materializer::QueryLocalTableOverlay;
use crate::query_execution::backend::BackendTopologySnapshot;
use crate::query_execution::preparation::scan::PlannedConnectorRead;
use novarocks_sql::binding::SqlTableBindingId;
use novarocks_sql::planning::query_execution::{
    FrozenConnectorScanIdentity, FrozenConnectorScanPlan, build_frozen_connector_scan_plan,
    frozen_connector_resolved_analyzer_table, matches_frozen_connector_scan,
};

/// Plan one opaque frozen source through the exact generation that admitted it.
pub(crate) fn plan_frozen_connector_read(
    planning_lease: ConnectorControlPlanningLease,
    topology: &BackendTopologySnapshot,
    source: &ConnectorTableHandle,
    expected_schema: &SchemaRef,
    projection: Vec<usize>,
    context: ConnectorRequestContext,
) -> Result<PlannedConnectorRead, ConnectorError> {
    let binding = planning_lease.binding();
    let expected_owner = ConnectorExecutionBindingKey {
        instance_id: binding.descriptor().instance_id.clone(),
        incarnation: binding.incarnation(),
    };
    if source.owner() != &expected_owner.instance_id {
        return Err(invalid(
            "frozen connector source does not belong to the exact planning generation",
        ));
    }
    let target_parallelism = NonZeroUsize::new(topology.targets().len()).ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::Unavailable,
            "frozen connector read requires at least one live backend",
        )
    })?;
    let batch = ConnectorBatchBudget {
        max_rows: NonZeroUsize::new(4096).expect("frozen read batch rows are nonzero"),
        max_bytes: NonZeroUsize::new(context.max_handle_payload_bytes())
            .expect("validated connector payload budget is nonzero"),
    };
    let selection = ConnectorScanSelection::Snapshot(ConnectorReadSelector::Current);
    let provider_field_ordinals = if projection.is_empty() {
        (0..expected_schema.fields().len())
            .map(|ordinal| {
                u32::try_from(ordinal).map_err(|_| {
                    invalid("frozen connector schema has an ordinal outside the native carrier")
                })
            })
            .collect::<Result<Vec<_>, _>>()?
    } else {
        projection
            .iter()
            .map(|ordinal| {
                u32::try_from(*ordinal).map_err(|_| {
                    invalid("frozen connector projection has an ordinal outside the native carrier")
                })
            })
            .collect::<Result<Vec<_>, _>>()?
    };
    let scan = binding.planning().begin_scan(
        source,
        ConnectorBeginScanRequest {
            projection,
            static_predicates: Vec::new(),
            selection,
            purpose: novarocks_spi::connector::ConnectorReadPurpose::Query,
            limit: None,
            batch,
            context: context.clone(),
        },
    )?;
    scan.validate(&expected_owner, selection).map_err(|error| {
        corrupt(format!(
            "frozen connector provider returned a scan outside the exact admission: {error}"
        ))
    })?;
    if scan.output_schema().as_ref() != expected_schema.as_ref() {
        return Err(corrupt(
            "frozen connector scan output schema does not match its admitted schema",
        ));
    }
    if provider_field_ordinals.len() != scan.output_schema().fields().len() {
        return Err(invalid(
            "frozen connector projection width does not match its admitted schema",
        ));
    }
    let split_result = binding.planning().plan_splits(
        scan.handle(),
        ConnectorSplitPlanningRequest {
            target_parallelism,
            max_split_bytes: None,
            context: context.clone(),
        },
    )?;
    validate_split_owners(&split_result.splits, &expected_owner)?;
    let declaration = binding.execution_declaration(&context)?;
    validate_execution_declaration(&declaration, &expected_owner)?;
    Ok(PlannedConnectorRead {
        declaration,
        provider_field_ordinals,
        scan,
        splits: split_result.splits,
        planning_metrics: split_result.metrics,
        static_predicates: Vec::new(),
        predicate_dispositions: Vec::new(),
        residual_predicates: Vec::new(),
        batch,
        planning_lease,
        read_session: split_result.session,
    })
}

fn validate_split_owners(
    splits: &[novarocks_spi::connector::ConnectorSplit],
    expected_owner: &ConnectorExecutionBindingKey,
) -> Result<(), ConnectorError> {
    if splits
        .iter()
        .any(|split| split.owner() != &expected_owner.instance_id)
    {
        return Err(corrupt(
            "frozen connector read planned a split for another connector instance",
        ));
    }
    Ok(())
}

fn validate_execution_declaration(
    declaration: &novarocks_spi::connector::ConnectorExecutionDeclaration,
    expected_owner: &ConnectorExecutionBindingKey,
) -> Result<(), ConnectorError> {
    if declaration.binding_key() != *expected_owner {
        return Err(corrupt(
            "frozen connector execution declaration does not match the exact planning generation",
        ));
    }
    Ok(())
}

/// Admit a synthetic SQL binding for an already-planned frozen read.
pub(crate) fn admit_frozen_connector_scan_binding(
    bindings: &QueryTableBindingStore,
    identity: &FrozenConnectorScanIdentity,
    input_schema: &SchemaRef,
) -> Result<SqlTableBindingId, String> {
    bindings.resolve_or_insert_with_id(frozen_connector_binding_key(identity), |binding| {
        frozen_connector_query_table_binding(identity.clone(), input_schema.clone(), binding)
    })
}

/// Build the request-local catalog overlay used by SQL-shaped frozen reads.
/// The overlay and resolver must be created from the same identity and binding
/// store; neither is published to shared catalog state.
pub(crate) fn frozen_connector_query_local_overlay(
    identity: &FrozenConnectorScanIdentity,
    input_schema: &SchemaRef,
) -> QueryLocalTableOverlay {
    let identity = identity.clone();
    let schema = input_schema.clone();
    QueryLocalTableOverlay::new(
        identity.namespace().to_string(),
        identity.table().to_string(),
        frozen_connector_binding_key(&identity),
        move |binding| {
            frozen_connector_query_table_binding(identity.clone(), schema.clone(), binding)
        },
    )
}

fn frozen_connector_binding_key(identity: &FrozenConnectorScanIdentity) -> QueryTableBindingKey {
    QueryTableBindingKey::strict_base(identity.catalog(), identity.namespace(), identity.table())
}

fn frozen_connector_query_table_binding(
    identity: FrozenConnectorScanIdentity,
    input_schema: SchemaRef,
    binding: SqlTableBindingId,
) -> Result<QueryTableBinding, String> {
    Ok(QueryTableBinding {
        resolved: frozen_connector_resolved_analyzer_table(&identity, input_schema, binding),
        statistics_pin: None,
        admission: QueryTableBindingAdmission::Local,
        scan_materialization: None,
        mv_target_read: None,
        write_target_admission: None,
        frozen_snapshot_materializations: BTreeMap::new(),
        admitted_change_scans: BTreeMap::new(),
    })
}

/// Build the minimal physical scan carrier for one admitted frozen source.
pub(crate) fn frozen_connector_scan_physical_plan(
    identity: &FrozenConnectorScanIdentity,
    input_schema: &SchemaRef,
    binding: SqlTableBindingId,
) -> FrozenConnectorScanPlan {
    build_frozen_connector_scan_plan(identity, input_schema, binding)
}

/// One-shot injection of an already-planned connector read into preparation.
pub(crate) struct FrozenConnectorReadResolver {
    binding: SqlTableBindingId,
    identity: FrozenConnectorScanIdentity,
    read: Mutex<Option<PlannedConnectorRead>>,
}

impl FrozenConnectorReadResolver {
    pub(crate) fn new(
        binding: SqlTableBindingId,
        identity: FrozenConnectorScanIdentity,
        read: PlannedConnectorRead,
    ) -> Self {
        Self {
            binding,
            identity,
            read: Mutex::new(Some(read)),
        }
    }

    fn matches(&self, scan: &novarocks_sql::plan_read::PlanScanNode) -> bool {
        matches_frozen_connector_scan(scan, self.binding, &self.identity)
    }
}

impl crate::query_execution::preparation::scan::ScanBindingResolver
    for FrozenConnectorReadResolver
{
    fn resolve_scan(
        &self,
        _node_id: i32,
        scan: &novarocks_sql::plan_read::PlanScanNode,
    ) -> Result<Option<crate::query_execution::preparation::scan::ResolvedScanExecution>, String>
    {
        if !self.matches(scan) {
            return Ok(None);
        }
        Ok(Some(
            crate::query_execution::preparation::scan::ResolvedScanExecution::ConnectorRead,
        ))
    }

    fn resolve_connector_read(
        &self,
        _node_id: i32,
        scan: &novarocks_sql::plan_read::PlanScanNode,
    ) -> Result<Option<PlannedConnectorRead>, String> {
        if !self.matches(scan) {
            return Ok(None);
        }
        self.read
            .lock()
            .map_err(|_| "frozen connector read lock poisoned".to_string())
            .map(|mut read| {
                read.take().map(|mut read| {
                    // The opaque source was planned before SQL compilation, so
                    // none of the query's scan predicates could have been
                    // negotiated with the provider. Core must retain every one
                    // as an execution residual when the frozen read is injected.
                    read.residual_predicates = scan.predicates.clone();
                    read
                })
            })
    }
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

fn corrupt(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorControlPlanningLease, ConnectorExecutionDeclaration, ConnectorInstanceDescriptor,
        ConnectorInstanceId, ConnectorInstanceIncarnation, ConnectorProviderId, ConnectorSplit,
        ConnectorTableIdentity, ConnectorTableRequest, ConnectorTableResolution,
    };

    use super::*;
    use crate::connector::scan_model::FixtureScanFile;
    use crate::query_execution::backend::LiveBackendTarget;

    fn topology() -> BackendTopologySnapshot {
        BackendTopologySnapshot::try_new(
            7,
            vec![LiveBackendTarget::new(
                3,
                SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 19030),
                11,
            )],
        )
        .expect("fixture topology")
    }

    fn fixture() -> (
        ConnectorControlPlanningLease,
        novarocks_spi::connector::ConnectorTableMetadata,
        ConnectorRequestContext,
    ) {
        let binding = crate::connector::scan_model::planned_files_fixture_binding(
            "frozen-read",
            HashMap::from([(
                "orders".to_string(),
                vec![FixtureScanFile::new("opaque-unit")],
            )]),
            None,
        );
        let context = crate::connector::test_request_context();
        let metadata = binding
            .metadata()
            .load_table(ConnectorTableRequest {
                table: ConnectorTableIdentity {
                    instance_id: ConnectorInstanceId::parse("frozen-read")
                        .expect("fixture instance ID"),
                    namespace: Arc::from("db"),
                    table: Arc::from("orders"),
                },
                resolution: ConnectorTableResolution::StrictBaseTable,
                context: context.clone(),
            })
            .expect("fixture table metadata");
        (
            ConnectorControlPlanningLease::new(Arc::new(binding), || {}),
            metadata,
            context,
        )
    }

    fn planning_error(result: Result<PlannedConnectorRead, ConnectorError>) -> ConnectorError {
        match result {
            Ok(_) => panic!("expected frozen connector planning failure"),
            Err(error) => error,
        }
    }

    #[test]
    fn plans_frozen_read_from_exact_generation() {
        let (lease, metadata, context) = fixture();
        let projection = (0..metadata.schema.fields().len()).collect();
        let read = plan_frozen_connector_read(
            lease,
            &topology(),
            &metadata.table,
            &metadata.schema,
            projection,
            context,
        )
        .expect("plan frozen connector read");

        assert_eq!(read.scan.output_schema().as_ref(), metadata.schema.as_ref());
        assert_eq!(read.splits.len(), 1);
        assert_eq!(read.declaration.binding_key(), read.scan.owner().clone());
    }

    #[test]
    fn preserves_reordered_provider_projection_ordinals() {
        let (lease, metadata, context) = fixture();
        let expected = Arc::new(Schema::new(vec![
            metadata.schema.field(2).clone(),
            metadata.schema.field(0).clone(),
        ]));
        let read = plan_frozen_connector_read(
            lease,
            &topology(),
            &metadata.table,
            &expected,
            vec![2, 0],
            context,
        )
        .expect("plan projected frozen connector read");

        assert_eq!(read.scan.output_schema().as_ref(), expected.as_ref());
        assert_eq!(read.provider_field_ordinals, vec![2, 0]);
    }

    #[test]
    fn rejects_empty_topology_before_provider_planning() {
        let (lease, metadata, context) = fixture();
        let error = planning_error(plan_frozen_connector_read(
            lease,
            &BackendTopologySnapshot::empty(8),
            &metadata.table,
            &metadata.schema,
            Vec::new(),
            context,
        ));

        assert_eq!(error.kind(), ConnectorErrorKind::Unavailable);
        assert!(error.to_string().contains("at least one live backend"));
    }

    #[test]
    fn rejects_source_owned_by_another_connector() {
        let (lease, metadata, context) = fixture();
        let foreign = ConnectorTableHandle::try_new(
            ConnectorInstanceId::parse("another-instance").expect("foreign instance ID"),
            Bytes::from_static(b"foreign"),
        )
        .expect("foreign handle");
        let error = planning_error(plan_frozen_connector_read(
            lease,
            &topology(),
            &foreign,
            &metadata.schema,
            Vec::new(),
            context,
        ));

        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert!(error.to_string().contains("exact planning generation"));
    }

    #[test]
    fn rejects_provider_schema_different_from_admission() {
        let (lease, metadata, context) = fixture();
        let expected = Arc::new(Schema::new(vec![Field::new(
            "different",
            DataType::Utf8,
            false,
        )]));
        let projection = (0..metadata.schema.fields().len()).collect();
        let error = planning_error(plan_frozen_connector_read(
            lease,
            &topology(),
            &metadata.table,
            &expected,
            projection,
            context,
        ));

        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
        assert!(error.to_string().contains("output schema"));
    }

    #[test]
    fn rejects_foreign_split_owner() {
        let owner = ConnectorExecutionBindingKey {
            instance_id: ConnectorInstanceId::parse("frozen-read").expect("instance ID"),
            incarnation: ConnectorInstanceIncarnation::from_bytes([3; 16]),
        };
        let split = ConnectorSplit::try_new(
            ConnectorInstanceId::parse("another-instance").expect("foreign instance ID"),
            "foreign-split",
            Bytes::new(),
            None,
        )
        .expect("foreign split");
        let error = validate_split_owners(&[split], &owner).expect_err("foreign split must fail");

        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
        assert!(error.to_string().contains("another connector instance"));
    }

    #[test]
    fn rejects_declaration_from_another_generation() {
        let instance_id = ConnectorInstanceId::parse("frozen-read").expect("instance ID");
        let expected = ConnectorExecutionBindingKey {
            instance_id: instance_id.clone(),
            incarnation: ConnectorInstanceIncarnation::from_bytes([3; 16]),
        };
        let declaration = ConnectorExecutionDeclaration::try_new(
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("fixture").expect("provider ID"),
                instance_id,
            },
            ConnectorInstanceIncarnation::from_bytes([4; 16]),
            Bytes::new(),
        )
        .expect("foreign generation declaration");
        let error = validate_execution_declaration(&declaration, &expected)
            .expect_err("foreign declaration generation must fail");

        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
        assert!(error.to_string().contains("exact planning generation"));
    }

    #[test]
    fn generic_binding_and_physical_carrier_preserve_identity() {
        let bindings = QueryTableBindingStore::try_new().expect("binding store");
        let identity = FrozenConnectorScanIdentity::new("__frozen", "operation", "cohort_7");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            true,
        )]));
        let binding = admit_frozen_connector_scan_binding(&bindings, &identity, &schema)
            .expect("admit frozen binding");
        let plan = frozen_connector_scan_physical_plan(&identity, &schema, binding);

        assert!(matches_frozen_connector_scan(
            plan.scan(),
            binding,
            &identity
        ));
        assert_eq!(plan.output_column_count(), 1);
    }

    #[test]
    fn resolver_supplies_the_frozen_read_exactly_once() {
        let (lease, metadata, context) = fixture();
        let projection = (0..metadata.schema.fields().len()).collect();
        let read = plan_frozen_connector_read(
            lease,
            &topology(),
            &metadata.table,
            &metadata.schema,
            projection,
            context,
        )
        .expect("plan frozen connector read");
        let bindings = QueryTableBindingStore::try_new().expect("binding store");
        let identity = FrozenConnectorScanIdentity::new("__frozen", "operation", "cohort_once");
        let binding = admit_frozen_connector_scan_binding(&bindings, &identity, &metadata.schema)
            .expect("admit frozen binding");
        let plan = frozen_connector_scan_physical_plan(&identity, &metadata.schema, binding)
            .with_predicates(vec![novarocks_sql::plan_read::TypedExpr {
                kind: novarocks_sql::plan_read::ExprKind::Literal(
                    novarocks_sql::plan_read::LiteralValue::Bool(true),
                ),
                data_type: DataType::Boolean,
                nullable: false,
            }]);
        let wrong_identity =
            FrozenConnectorScanIdentity::new("__frozen", "operation", "wrong_cohort");
        let wrong_binding =
            admit_frozen_connector_scan_binding(&bindings, &wrong_identity, &metadata.schema)
                .expect("admit wrong frozen binding");
        let wrong_plan =
            frozen_connector_scan_physical_plan(&wrong_identity, &metadata.schema, wrong_binding);
        let resolver = FrozenConnectorReadResolver::new(binding, identity, read);

        assert!(
            crate::query_execution::preparation::scan::ScanBindingResolver::resolve_scan(
                &resolver,
                8,
                wrong_plan.scan(),
            )
            .expect("wrong scan resolver call")
            .is_none()
        );
        assert!(
            crate::query_execution::preparation::scan::ScanBindingResolver::resolve_connector_read(
                &resolver,
                8,
                wrong_plan.scan(),
            )
            .expect("wrong resolver call")
            .is_none()
        );

        let resolved =
            crate::query_execution::preparation::scan::ScanBindingResolver::resolve_connector_read(
                &resolver,
                9,
                plan.scan(),
            )
            .expect("first resolver call")
            .expect("frozen read");
        assert_eq!(resolved.residual_predicates.len(), 1);
        assert_eq!(
            format!("{:?}", resolved.residual_predicates),
            format!("{:?}", plan.scan().predicates)
        );
        assert!(
            crate::query_execution::preparation::scan::ScanBindingResolver::resolve_connector_read(
                &resolver,
                9,
                plan.scan(),
            )
            .expect("second resolver call")
            .is_none()
        );
    }
}
