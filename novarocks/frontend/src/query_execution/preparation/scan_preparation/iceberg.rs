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

use novarocks_spi::connector::{
    ConnectorBatchBudget, ConnectorBeginScanRequest, ConnectorPredicateDisposition,
    ConnectorPredicateDispositionKind, ConnectorReadPurpose, ConnectorSplitPlanningRequest,
    ConnectorStaticPredicate, normalize_predicate_dispositions,
};

use crate::query_execution::preparation::scan::PlannedConnectorRead;
use novarocks::catalog_application::query_bindings::QueryScanMaterialization;
use novarocks_sql::plan_read::PlanScanNode;
use novarocks_sql::plan_read::TypedExpr;

use super::projection::effective_scan_column_names;

/// Plan an admitted connector read without decoding or reconstructing a
/// provider handle.  Projection ordinals are derived exclusively from the
/// schema frozen by `ConnectorMetadata::load_table`; a missing or ambiguous
/// column is a preparation error rather than an opportunity for Core to map
/// provider field identities.
pub(super) fn plan_connector_read(
    context: novarocks_spi::connector::ConnectorRequestContext,
    scan: &PlanScanNode,
    materialization: &QueryScanMaterialization,
    purpose: ConnectorReadPurpose,
    static_predicates: Vec<ConnectorStaticPredicate>,
    target_parallelism: std::num::NonZeroUsize,
    max_split_bytes: Option<std::num::NonZeroU64>,
) -> Result<PlannedConnectorRead, String> {
    let QueryScanMaterialization {
        table,
        schema,
        selector,
        planning_lease,
        ..
    } = materialization;
    let projection_names = effective_scan_column_names(scan);
    let mut projection = Vec::with_capacity(projection_names.len());
    for name in projection_names {
        let mut matching = schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| field.name().eq_ignore_ascii_case(&name));
        let Some((ordinal, _)) = matching.next() else {
            return Err(format!(
                "connector read schema is missing projected column '{name}'"
            ));
        };
        if matching.next().is_some() {
            return Err(format!(
                "connector read schema has ambiguous projected column '{name}'"
            ));
        }
        projection.push(ordinal);
    }
    let binding = planning_lease.binding();
    if table.owner() != &binding.descriptor().instance_id {
        return Err(
            "connector read table handle owner does not match its planning lease".to_string(),
        );
    }
    let declaration = binding
        .execution_declaration(&context)
        .map_err(|error| error.to_string())?;
    let batch = ConnectorBatchBudget {
        max_rows: std::num::NonZeroUsize::new(4096).expect("batch rows are nonzero"),
        max_bytes: std::num::NonZeroUsize::new(
            novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        )
        .expect("batch bytes are nonzero"),
    };
    let connector_scan = binding
        .planning()
        .begin_scan(
            table,
            ConnectorBeginScanRequest {
                projection: projection.clone(),
                static_predicates: static_predicates.clone(),
                selection: novarocks_spi::connector::ConnectorScanSelection::Snapshot(*selector),
                purpose,
                limit: None,
                batch,
                context: context.clone(),
            },
        )
        .map_err(|error| error.to_string())?;
    connector_scan
        .validate(
            &novarocks_spi::connector::ConnectorExecutionBindingKey {
                instance_id: binding.descriptor().instance_id.clone(),
                incarnation: binding.incarnation(),
            },
            novarocks_spi::connector::ConnectorScanSelection::Snapshot(*selector),
        )
        .map_err(|error| error.to_string())?;
    let expected_fields = projection
        .iter()
        .map(|ordinal| schema.fields()[*ordinal].clone())
        .collect::<Vec<_>>();
    if connector_scan.output_schema().fields().as_ref() != expected_fields.as_slice() {
        return Err(
            "connector read returned a schema that does not match the admitted projection"
                .to_string(),
        );
    }
    let predicate_dispositions = normalize_predicate_dispositions(
        &static_predicates,
        connector_scan.predicate_dispositions(),
    )
    .map_err(|error| format!("connector static predicate response: {error}"))?;
    let residual_predicates = residual_predicates(&scan.predicates, &predicate_dispositions)?;
    let split_result = binding
        .planning()
        .plan_splits(
            connector_scan.handle(),
            ConnectorSplitPlanningRequest {
                target_parallelism,
                max_split_bytes,
                context,
            },
        )
        .map_err(|error| error.to_string())?;
    if split_result
        .splits
        .iter()
        .any(|split| split.owner() != &binding.descriptor().instance_id)
    {
        return Err("connector read planned a split for another instance".to_string());
    }
    let provider_field_ordinals = projection
        .iter()
        .map(|ordinal| {
            u32::try_from(*ordinal)
                .map_err(|_| "Iceberg provider field ordinal does not fit u32".to_string())
        })
        .collect::<Result<_, _>>()?;
    Ok(PlannedConnectorRead {
        declaration,
        scan: connector_scan,
        provider_field_ordinals,
        splits: split_result.splits,
        planning_metrics: split_result.metrics,
        static_predicates,
        predicate_dispositions,
        residual_predicates,
        batch,
        planning_lease: planning_lease.clone(),
        read_session: split_result.session,
    })
}

/// Plans a provider-sealed scan through the same opaque split contract used by
/// ordinary connector reads. The application admitted this scan while it held
/// the exact lease; preparation must not call `begin_scan` again.
pub(super) fn plan_sealed_connector_read(
    exact_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
    context: novarocks_spi::connector::ConnectorRequestContext,
    predicates: &[TypedExpr],
    connector_scan: novarocks_spi::connector::ConnectorScan,
    expected_selection: novarocks_spi::connector::ConnectorScanSelection,
    target_parallelism: std::num::NonZeroUsize,
    max_split_bytes: Option<std::num::NonZeroU64>,
) -> Result<PlannedConnectorRead, String> {
    let binding = exact_lease.binding();
    connector_scan
        .validate(
            &novarocks_spi::connector::ConnectorExecutionBindingKey {
                instance_id: binding.descriptor().instance_id.clone(),
                incarnation: binding.incarnation(),
            },
            expected_selection,
        )
        .map_err(|error| error.to_string())?;
    let declaration = binding
        .execution_declaration(&context)
        .map_err(|error| error.to_string())?;
    let split_result = binding
        .planning()
        .plan_splits(
            connector_scan.handle(),
            ConnectorSplitPlanningRequest {
                target_parallelism,
                max_split_bytes,
                context,
            },
        )
        .map_err(|error| error.to_string())?;
    if split_result
        .splits
        .iter()
        .any(|split| split.owner() != &binding.descriptor().instance_id)
    {
        return Err("sealed connector scan planned a split for another instance".to_string());
    }
    let provider_field_ordinals = (0..connector_scan.output_schema().fields().len())
        .map(|ordinal| {
            u32::try_from(ordinal)
                .map_err(|_| "connector provider field ordinal does not fit u32".to_string())
        })
        .collect::<Result<_, _>>()?;
    let batch = ConnectorBatchBudget {
        max_rows: std::num::NonZeroUsize::new(4096).expect("batch rows are nonzero"),
        max_bytes: std::num::NonZeroUsize::new(
            novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        )
        .expect("batch bytes are nonzero"),
    };
    Ok(PlannedConnectorRead {
        declaration,
        scan: connector_scan,
        provider_field_ordinals,
        splits: split_result.splits,
        planning_metrics: split_result.metrics,
        static_predicates: Vec::new(),
        predicate_dispositions: Vec::new(),
        residual_predicates: predicates.to_vec(),
        batch,
        planning_lease: exact_lease,
        read_session: split_result.session,
    })
}

fn residual_predicates(
    predicates: &[TypedExpr],
    dispositions: &[ConnectorPredicateDisposition],
) -> Result<Vec<TypedExpr>, String> {
    let exact = dispositions
        .iter()
        .filter(|disposition| disposition.kind == ConnectorPredicateDispositionKind::Exact)
        .map(|disposition| usize::try_from(disposition.predicate_id.0))
        .collect::<Result<std::collections::BTreeSet<_>, _>>()
        .map_err(|_| "connector predicate ID does not fit the local ordinal".to_string())?;
    Ok(predicates
        .iter()
        .enumerate()
        .filter(|(ordinal, _)| !exact.contains(ordinal))
        .map(|(_, predicate)| predicate.clone())
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_spi::connector::{ConnectorPredicateDisposition, ConnectorStaticPredicateId};
    use novarocks_sql::plan_read::DistributedNodeKind;
    use novarocks_sql::test_support::{NativeScanFixture, native_scan_plan};

    #[test]
    fn only_exact_dispositions_remove_ordered_core_residuals() {
        let plan = native_scan_plan(NativeScanFixture::OrdinaryIcebergWithIdEqualityPredicate)
            .expect("sealed predicate fixture");
        let DistributedNodeKind::Scan(scan) = &plan.fragments()[0].root.payload else {
            panic!("sealed predicate fixture root");
        };
        let predicates = scan.predicates.clone();
        let pruning_only = vec![ConnectorPredicateDisposition {
            predicate_id: ConnectorStaticPredicateId(0),
            kind: ConnectorPredicateDispositionKind::PruningOnly,
        }];
        let residual = residual_predicates(&predicates, &pruning_only).unwrap();
        assert_eq!(residual.len(), 1);
        assert_eq!(format!("{:?}", residual), format!("{:?}", predicates));
        let exact = vec![ConnectorPredicateDisposition {
            predicate_id: ConnectorStaticPredicateId(0),
            kind: ConnectorPredicateDispositionKind::Exact,
        }];
        assert!(
            residual_predicates(&predicates, &exact)
                .expect("exact predicate disposition")
                .is_empty()
        );
    }
}
