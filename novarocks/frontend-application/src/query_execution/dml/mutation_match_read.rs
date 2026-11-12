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

use novarocks_parser::{ast, build};
use novarocks_spi::connector::{
    CONNECTOR_FIELD_HIDDEN_FROM_SQL, ConnectorControlPlanningPin, ConnectorReadSelector,
    ConnectorRowMutationPreparation, ConnectorTableColumnRole, ConnectorTableMetadata,
    ConnectorTablePlanningFacts,
};
use novarocks_sql::planning::catalog::{ConnectorReadTableFacts, materialize_connector_read_table};
use novarocks_types::schema::ColumnDef;

use crate::catalog_application::query_bindings::{
    QueryScanMaterialization, QueryTableBinding, QueryTableBindingAdmission, QueryTableBindingKey,
};
use crate::catalog_application::query_materializer::QueryLocalTableOverlay;
use crate::catalog_application::resolver::TargetBackend;

/// Keep the signed target read under the same generation and original table
/// observation. Only the generated target occurrence is assigned this overlay.
pub(super) fn bind_mutation_match_read(
    target: &TargetBackend,
    target_factor: &mut ast::TableFactor,
    isolate_lookup: bool,
    preparation: &ConnectorRowMutationPreparation,
    original: &ConnectorTableMetadata,
    planning_pin: &ConnectorControlPlanningPin,
) -> Result<QueryLocalTableOverlay, String> {
    let instance = &planning_pin.binding().descriptor().instance_id;
    let source = original.source_read_facts().ok_or_else(|| {
        "mutation match target lost its original provider access provenance".to_string()
    })?;
    if preparation.table() != &original.table
        || &preparation.owner().instance_id != instance
        || preparation.owner().incarnation != planning_pin.binding().incarnation()
        || preparation.match_source().owner() != instance
        || original.identity.instance_id != *instance
        || original.identity.namespace.as_ref() != target.namespace
        || original.identity.table.as_ref() != target.table
    {
        return Err("mutation match target differs from its admitted provider observation".into());
    }
    source
        .validate_owner(preparation.owner())
        .map_err(|error| error.to_string())?;
    let ast::TableFactor::Table {
        name,
        alias,
        version,
        metadata,
        ..
    } = target_factor
    else {
        return Err("generated mutation match target is not a table".into());
    };
    if metadata.is_some()
        || name.parts.iter().map(|part| part.value.as_str()).ne([
            target.catalog.as_str(),
            target.namespace.as_str(),
            target.table.as_str(),
        ])
        || (isolate_lookup && alias.is_none())
    {
        return Err("generated mutation match target differs from its admitted table".into());
    }
    let table = if isolate_lookup {
        format!(
            "__nr_mutation_target_{}",
            hex::encode(preparation.operation_id().to_bytes())
        )
    } else {
        target.table.clone()
    };
    if isolate_lookup {
        *name = build::object_name(
            [&target.catalog, &target.namespace, &table]
                .into_iter()
                .map(|name| build::ident(name, Some('`')))
                .collect(),
        )
        .map_err(|error| error.to_string())?;
    }
    // The Provider's Current selector is relative to its signed match source.
    // A named SQL ref would ask the catalog to resolve the ref a second time.
    *version = None;

    let schema = preparation.match_source_schema().clone();
    let snapshot = preparation.base_version_ordinal();
    let (columns, hidden) = match_source_columns(preparation, original)?;
    let key = QueryTableBindingKey::strict_base(&target.catalog, &target.namespace, &table);
    let (catalog, namespace) = (target.catalog.clone(), target.namespace.clone());
    let original = original.clone();
    let planning_pin = planning_pin.clone();
    let match_source = preparation.match_source().clone();
    Ok(QueryLocalTableOverlay::for_catalog(
        catalog.clone(),
        namespace.clone(),
        table.clone(),
        key,
        move |binding| {
            let resolved = materialize_connector_read_table(ConnectorReadTableFacts {
                catalog: catalog.clone(),
                namespace: namespace.clone(),
                table: table.clone(),
                columns: columns.clone(),
                iceberg_row_lineage_metadata_columns: hidden.clone(),
                schema: schema.clone(),
                binding,
                selector: ConnectorReadSelector::Current,
                planning_facts: ConnectorTablePlanningFacts::empty(),
            })?
            .into_resolved_table();
            Ok(QueryTableBinding {
                resolved,
                statistics_pin: None,
                admission: QueryTableBindingAdmission::RowMutationMatch(
                    planning_pin.clone(),
                    snapshot,
                ),
                source_metadata: Some(original.clone()),
                scan_materialization: Some(QueryScanMaterialization {
                    table: match_source.clone(),
                    selector: ConnectorReadSelector::Current,
                    mv_partition_selection: None,
                    planning_pin: planning_pin.clone(),
                }),
                mv_target_read: None,
                write_target_admission: None,
                frozen_cohort_read: None,
                frozen_snapshot_materializations: Default::default(),
                prepared_source_reads: Default::default(),
            })
        },
    ))
}

fn match_source_columns(
    preparation: &ConnectorRowMutationPreparation,
    original: &ConnectorTableMetadata,
) -> Result<(Vec<ColumnDef>, Vec<ColumnDef>), String> {
    let contract = preparation.match_contract();
    let mut columns = Vec::new();
    let mut hidden = Vec::new();
    for field in preparation.match_source_schema().fields() {
        let original_ordinal = original.schema().fields().iter().position(|candidate| {
            candidate.as_ref() == field.as_ref() && candidate.metadata() == field.metadata()
        });
        let identity = contract
            .identity_fields()
            .iter()
            .find(|value| value.field().name() == field.name());
        let original_role = original_ordinal
            .and_then(|ordinal| original.planning_facts.column_facts().get(ordinal))
            .map(|fact| fact.role());
        let system =
            identity.is_some() || original_role == Some(ConnectorTableColumnRole::RowLineageSystem);
        let is_hidden = field
            .metadata()
            .get(CONNECTOR_FIELD_HIDDEN_FROM_SQL)
            .is_some_and(|value| value == "true");
        if is_hidden && !system {
            continue;
        }
        let column = if let Some(ordinal) = original_ordinal {
            crate::connector::sql_column_from_connector_binding(
                field.name(),
                original
                    .execution_schema()
                    .binding(ordinal)
                    .ok_or("original match schema has no value binding")?,
                &original.planning_facts,
                ordinal,
            )?
        } else {
            let signed = identity
                .and_then(|value| value.execution_type())
                .or_else(|| {
                    contract
                        .before_fields()
                        .iter()
                        .find(|value| value.field().name() == field.name())
                        .and_then(|value| value.execution_type())
                })
                .ok_or_else(|| {
                    format!(
                        "signed match field `{}` has no execution declaration",
                        field.name()
                    )
                })?;
            let ty = signed
                .with_nullable(field.is_nullable())
                .map_err(|error| error.to_string())?;
            ColumnDef::from_value_type(field.name().clone(), ty, None)
                .map_err(|error| error.to_string())?
        };
        if system {
            hidden.push(column);
        } else {
            columns.push(column);
        }
    }
    Ok((columns, hidden))
}

pub(super) fn mutation_match_select(
    query: &mut ast::Query,
) -> Result<&mut ast::TableWithJoins, String> {
    if let ast::SetExpr::Select(select) = query.body.as_mut()
        && let [from] = select.from.as_mut_slice()
    {
        Ok(from)
    } else {
        Err("generated mutation match query lost its target relation".into())
    }
}
