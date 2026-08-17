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

//! Application projection for SQL terminal write contracts.
//!
//! The compiler receives the resulting [`DmlWritePlanInput`] only. Provider
//! metadata never crosses this boundary: the request-local binding retains a
//! sealed write preparation and SQL sees only its Arrow layout and field
//! tokens.

use arrow::datatypes::{Schema, SchemaRef};
use novarocks_catalog::schema::ColumnDef;
use std::sync::Arc;

use crate::catalog_application::query_bindings::{
    QueryTableBinding, QueryTableBindingAdmission, QueryTableBindingKey, QueryTableBindingStore,
    QueryWriteTargetAdmission,
};
use novarocks_spi::connector::{
    ConnectorControlPlanningLease, ConnectorWriteInputShape, ConnectorWritePreparation,
};
use novarocks_sql::binding::SqlTableBindingId;
use novarocks_sql::planning::dml::{
    DmlWritePlanInput, DmlWriteSinkMode, DmlWriteTarget, DmlWriteTargetField,
};
use novarocks_sql::planning::query_execution::{
    FrozenConnectorScanIdentity, frozen_connector_write_target_resolved_analyzer_table,
};

/// Project an admitted write target into the opaque DML planning boundary.
///
/// This is intentionally separate from the legacy internal compiler helper:
/// new application entry points must not receive the private planner write
/// contract.  The request-local binding keeps the same exact planning lease
/// and provider preparation through terminal planning and fragment setup.
pub(crate) fn dml_write_plan_input_for_admitted_target(
    bindings: &QueryTableBindingStore,
    binding: SqlTableBindingId,
    mode: DmlWriteSinkMode,
    input: novarocks_sql::plan_read::ConnectorWriteInputBinding,
) -> Result<DmlWritePlanInput, String> {
    let captured = bindings.binding(binding)?;
    captured.admission.exact_planning_lease().map_err(|_| {
        "SQL write target binding is missing its admission planning lease".to_string()
    })?;
    let preparation = &admitted_write_target(&captured)?.preparation;
    preparation
        .validate()
        .map_err(|error| format!("validate SQL write preparation: {error}"))?;
    validate_mode(mode, preparation.input())?;
    let identity =
        novarocks_sql::planning::catalog::materialization_identity_facts(&captured.resolved);
    DmlWritePlanInput::try_new(
        mode,
        DmlWriteTarget {
            binding,
            catalog: identity.catalog().to_string(),
            namespace: identity.namespace().to_string(),
            table: identity.table().to_string(),
            fields: preparation
                .input()
                .fields()
                .into_iter()
                .map(|field| DmlWriteTargetField {
                    token: field.token(),
                    column: ColumnDef {
                        name: field.field().name().to_string(),
                        data_type: field.field().data_type().clone(),
                        nullable: field.field().is_nullable(),
                        write_default: None,
                        logical_type: None,
                    },
                    is_hidden: false,
                })
                .collect(),
        },
        admitted_write_input_columns(preparation)?,
        input,
    )
}

/// Reserve a SQL write token for a sealed Provider preparation.  The exact
/// planning lease and opaque table handle remain paired by the preparation;
/// this function does not inspect either provider-owned value.
pub(crate) fn admit_prepared_connector_write_target(
    bindings: &QueryTableBindingStore,
    identity: FrozenConnectorScanIdentity,
    preparation: ConnectorWritePreparation,
    planning_lease: ConnectorControlPlanningLease,
) -> Result<SqlTableBindingId, String> {
    preparation
        .validate()
        .map_err(|error| format!("validate connector write preparation: {error}"))?;
    let descriptor = planning_lease.binding().descriptor();
    if !descriptor
        .instance_id
        .as_str()
        .eq_ignore_ascii_case(preparation.table().owner().as_str())
    {
        return Err(
            "connector write preparation does not match its admission planning lease".to_string(),
        );
    }
    let key = QueryTableBindingKey::write_target(
        identity.catalog(),
        identity.namespace(),
        identity.table(),
        preparation.digest(),
    );
    bindings.resolve_or_insert_with_id(key, |binding| {
        Ok(QueryTableBinding {
            resolved: frozen_connector_write_target_resolved_analyzer_table(
                &identity,
                admitted_write_input_schema(&preparation),
                binding,
            ),
            statistics_pin: None,
            admission: QueryTableBindingAdmission::Exact(planning_lease),
            // This token represents a terminal write target, not a read
            // source.  Do not invent a synthetic Iceberg file scan merely to
            // prove admission; the provider-owned write table below is the
            // exact SQL write-target contract.
            scan_materialization: None,
            mv_target_read: None,
            write_target_admission: Some(QueryWriteTargetAdmission {
                preparation: preparation.clone(),
            }),
            frozen_snapshot_materializations: std::collections::BTreeMap::new(),
            admitted_change_scans: std::collections::BTreeMap::new(),
        })
    })
}

/// Reserve a terminal write token for a synthetic frozen connector identity.
/// The SQL-facing identity carries no provider handle; the preparation and
/// exact lease remain in the application-owned binding store.
pub(crate) fn admit_prepared_frozen_connector_write_target(
    bindings: &QueryTableBindingStore,
    identity: FrozenConnectorScanIdentity,
    preparation: ConnectorWritePreparation,
    planning_lease: ConnectorControlPlanningLease,
) -> Result<SqlTableBindingId, String> {
    admit_prepared_connector_write_target(bindings, identity, preparation, planning_lease)
}

fn admitted_write_target(
    binding: &QueryTableBinding,
) -> Result<&crate::catalog_application::query_bindings::QueryWriteTargetAdmission, String> {
    binding
        .write_target_admission
        .as_ref()
        .ok_or_else(|| "SQL write target binding is missing admitted write facts".to_string())
}

fn admitted_write_input_columns(
    preparation: &ConnectorWritePreparation,
) -> Result<Vec<ColumnDef>, String> {
    Ok(preparation
        .input()
        .fields()
        .into_iter()
        .map(|field| ColumnDef {
            name: field.field().name().to_string(),
            data_type: field.field().data_type().clone(),
            nullable: field.field().is_nullable(),
            write_default: None,
            logical_type: None,
        })
        .collect())
}

fn admitted_write_input_schema(preparation: &ConnectorWritePreparation) -> SchemaRef {
    Arc::new(Schema::new(
        preparation
            .input()
            .fields()
            .into_iter()
            .map(|field| field.field().clone())
            .collect::<Vec<_>>(),
    ))
}

fn validate_mode(mode: DmlWriteSinkMode, input: &ConnectorWriteInputShape) -> Result<(), String> {
    let matches = matches!(
        (mode, input),
        (
            DmlWriteSinkMode::Data,
            ConnectorWriteInputShape::Data { .. }
        ) | (
            DmlWriteSinkMode::RowLineageData,
            ConnectorWriteInputShape::RowLineage { .. }
        ) | (
            DmlWriteSinkMode::PositionDeletes,
            ConnectorWriteInputShape::PositionDelete { .. }
        ) | (
            DmlWriteSinkMode::DeletionVectors,
            ConnectorWriteInputShape::DeletionVector { .. }
        ) | (
            DmlWriteSinkMode::EqualityDeletes,
            ConnectorWriteInputShape::EqualityDelete { .. }
        )
    );
    matches.then_some(()).ok_or_else(|| {
        "SQL write sink mode does not match its Provider-signed input shape".to_string()
    })
}
