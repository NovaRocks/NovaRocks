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

//! Production preparation and binding for frontend-owned MV refresh writes.

use std::sync::Arc;

use crate::mv::domain::iceberg_refresh::IcebergMvCorePorts;
use crate::query_execution::mv_assembly::refresh_artifact::MvFirstRefreshLogicalContext;
use crate::query_execution::write_session::ConnectorWriteSession;
use novarocks_sql::compiler::SqlMvRelationOccurrenceId;
use novarocks_sql::planning::mv::first_refresh::{SqlMvSnapshotPin, SqlMvSnapshotPinOccurrence};

pub(crate) fn frozen_logical_context_from_rewrite(
    rewrite: &crate::mv::domain::rewrite::context::IcebergMvRewriteContext,
    affected_partitions: crate::mv::domain::model::AffectedTargetPartitions,
    frozen_base_overlays: Option<
        Vec<crate::catalog_application::query_materializer::QueryLocalTableOverlay>,
    >,
) -> Result<MvFirstRefreshLogicalContext, String> {
    let pin = ordered_current_sources(rewrite)?;
    // Validate the exact application facts against the SQL-owned occurrence
    // contract before the logical artifact can outlive this preparation step.
    // The richer source revisions remain in the application artifact because
    // activation must reconstruct the canonical context without decoding IDs.
    let _sql_pin = sql_snapshot_pin(&rewrite.mv_definition, &pin)?;
    Ok(MvFirstRefreshLogicalContext {
        mv_definition: (*rewrite.mv_definition).clone(),
        base_refs: rewrite
            .base_refs
            .iter()
            .map(|base| base.table.clone())
            .collect(),
        pin,
        previous: ordered_previous_sources(rewrite)?,
        analysis: rewrite.analysis_facts().clone(),
        target_table_uuid: rewrite.target_table_uuid.clone(),
        affected_partitions,
        frozen_base_overlays,
    })
}

fn ordered_current_sources(
    rewrite: &crate::mv::domain::rewrite::context::IcebergMvRewriteContext,
) -> Result<Vec<crate::mv::domain::rewrite::context::MvRewriteSourceSnapshot>, String> {
    let occurrences = &rewrite
        .mv_definition
        .facts
        .definition()
        .relation_occurrences;
    if occurrences.len() != rewrite.pin.len() {
        return Err("MV first-refresh source pins do not cover every D occurrence".to_string());
    }
    occurrences
        .iter()
        .map(|occurrence| {
            rewrite
                .pin
                .get(&SqlMvRelationOccurrenceId::new(occurrence.occurrence_id))
                .cloned()
                .ok_or_else(|| {
                    format!(
                        "MV first-refresh source pin is missing D occurrence {}",
                        occurrence.occurrence_id
                    )
                })
        })
        .collect()
}

fn ordered_previous_sources(
    rewrite: &crate::mv::domain::rewrite::context::IcebergMvRewriteContext,
) -> Result<Vec<crate::mv::domain::rewrite::context::MvRewriteSourceSnapshot>, String> {
    match rewrite.mv_definition.facts.publication() {
        None if rewrite.previous.is_empty() => Ok(Vec::new()),
        None => Err("never-published MV first-refresh artifact has source history".to_string()),
        Some(published) => published
            .inputs
            .iter()
            .map(|input| {
                rewrite
                    .previous
                    .iter()
                    .find(|source| source.occurrence_id.get() == input.relation_occurrence_id)
                    .cloned()
                    .ok_or_else(|| {
                        format!(
                            "MV first-refresh history is missing D occurrence {}",
                            input.relation_occurrence_id
                        )
                    })
            })
            .collect(),
    }
}

fn sql_snapshot_pin(
    projection: &novarocks_mv_application::persistence::keyed_documents::StoredKeyedMvProjection,
    sources: &[crate::mv::domain::rewrite::context::MvRewriteSourceSnapshot],
) -> Result<SqlMvSnapshotPin, String> {
    let occurrences = &projection.facts.definition().relation_occurrences;
    if occurrences.len() != sources.len() {
        return Err("MV first-refresh SQL pin does not cover every D occurrence".to_string());
    }
    SqlMvSnapshotPin::try_from_occurrences(
        occurrences
            .iter()
            .zip(sources)
            .map(|(occurrence, source)| {
                let occurrence_id = SqlMvRelationOccurrenceId::new(occurrence.occurrence_id);
                if source.occurrence_id != occurrence_id {
                    return Err(format!(
                        "MV first-refresh SQL pin order differs at D occurrence {}",
                        occurrence.occurrence_id
                    ));
                }
                SqlMvSnapshotPinOccurrence::try_new(
                    occurrence_id,
                    novarocks_types::naming::TableIdentity {
                        catalog: occurrence.catalog_at_binding.clone(),
                        namespace: occurrence.namespace_at_binding.clone(),
                        table: occurrence.relation_at_binding.clone(),
                    },
                    source.snapshot_id,
                    source.table_object_id.clone(),
                )
            })
            .collect::<Result<Vec<_>, String>>()?,
    )
}

/// The one logical target a first-refresh publication seals.
///
/// A publication that republishes rows wholesale has exactly one thing to do
/// with every row it is given, so it seals a single unrouted data branch. The
/// plan is compiled against that branch's ordinal, so a session that sealed a
/// different number of them is refused here rather than having its extra
/// branches written by nobody.
pub(crate) fn sole_publication_write_target(
    write_session: &ConnectorWriteSession,
) -> Result<&novarocks_spi::connector::write_stack::ConnectorWriteTargetPlan, String> {
    match write_session.targets() {
        [write_target] => Ok(write_target),
        targets => Err(format!(
            "MV first-refresh publication requires a write session with exactly one target, but the session sealed {}",
            targets.len()
        )),
    }
}

/// Validate the frozen request for original asynchronous activation.
/// This cannot resolve a new generation.
pub(crate) fn validate_frozen_mv_rewrite_identity(
    target_catalog: &str,
    target_namespace: &str,
    target_name: &str,
    facts: &MvFirstRefreshLogicalContext,
) -> Result<novarocks_types::naming::TableIdentity, String> {
    let persisted_target = facts.mv_definition.facts.target();
    let target_identity = novarocks_types::naming::TableIdentity {
        catalog: persisted_target
            .catalog()
            .ok_or_else(|| {
                "MV first-refresh logical artifact target has no connector catalog".to_string()
            })?
            .to_string(),
        namespace: persisted_target.namespace().to_string(),
        table: persisted_target.name().to_string(),
    };
    if target_identity.catalog != target_catalog
        || target_identity.namespace != target_namespace
        || target_identity.table != target_name
    {
        return Err(
            "MV refresh logical artifact target does not match its frozen write request"
                .to_string(),
        );
    }
    validate_frozen_join_base_facts(facts)?;
    Ok(target_identity)
}

/// Resume the frozen activation from its retained exact metadata binding.
/// Provider failures preserve their original cause.
pub(crate) fn finish_frozen_mv_rewrite_context_original(
    ports: &IcebergMvCorePorts,
    expected_target_snapshot_id: Option<i64>,
    facts: &MvFirstRefreshLogicalContext,
    target_binding: &crate::mv::domain::refresh::target_binding::MvTargetBinding,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<
    Arc<crate::mv::domain::rewrite::context::IcebergMvRewriteContext>,
    crate::mv::domain::application::MvApplicationError,
> {
    use super::refresh_preparation::io_gate::{contract_error, provider_error};
    if target_binding.table_uuid() != facts.target_table_uuid {
        return Err(contract_error(
            "MV refresh logical artifact target UUID drifted after preparation",
        ));
    }
    if target_binding.current_snapshot_id() != expected_target_snapshot_id {
        return Err(contract_error(
            "MV refresh logical artifact target snapshot drifted after preparation",
        ));
    }
    let target_schema = crate::mv::domain::storage_observation::observe_schema_validation(
        ports.storage_observation(),
        target_binding.lease(),
        target_binding.metadata(),
        connector_context.clone(),
    )
    .map_err(provider_error)?;
    if target_schema.table() != target_binding.identity() {
        return Err(contract_error(
            "MV first-refresh target schema and retained target binding differ",
        ));
    }
    crate::mv::domain::refresh::rewrite_context::build_neutral_refresh_rewrite_context(
        Arc::new(facts.mv_definition.clone()),
        facts.pin.clone(),
        facts.previous.clone(),
        facts.target_table_uuid.clone(),
        target_binding
            .physical_write_schema()
            .map_err(contract_error)?,
        target_binding
            .write_input_fields()
            .map_err(contract_error)?,
        target_schema.exact_schema().clone(),
        facts.analysis.clone(),
    )
    .map_err(contract_error)
}

fn validate_frozen_join_base_facts(facts: &MvFirstRefreshLogicalContext) -> Result<(), String> {
    if facts.base_refs.is_empty() || facts.pin.len() != facts.base_refs.len() {
        return Err(
            "MV first-refresh logical artifact has incomplete base snapshot pins".to_string(),
        );
    }
    let occurrences = &facts.mv_definition.facts.definition().relation_occurrences;
    if occurrences.len() != facts.pin.len()
        || occurrences
            .iter()
            .zip(&facts.pin)
            .any(|(occurrence, source)| {
                source.occurrence_id != SqlMvRelationOccurrenceId::new(occurrence.occurrence_id)
            })
    {
        return Err(
            "MV first-refresh logical artifact does not preserve D occurrence order".to_string(),
        );
    }
    // Production logical first-refresh artifacts retain the materializations
    // admitted during preparation.  Those overlays carry the exact lease,
    // table identity, and pinned input set that activation must use; asking
    // the catalog for the current base here would silently reintroduce a
    // latest-generation acquire.
    facts
        .frozen_base_overlays
        .as_ref()
        .map(|_| ())
        .ok_or_else(|| {
            "MV logical artifact is missing exact-generation frozen base overlays".to_string()
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv::domain::test_admission::KeyedProjectionFixture as ProjectionFixture;
    use bytes::Bytes;
    use novarocks_mv_application::persistence::keyed_documents::StoredKeyedMvProjection;
    use novarocks_mv_application::product::MvTarget;
    use novarocks_spi::connector::{
        ConnectorExactSemanticRevision, ConnectorProviderId, ConnectorTableObjectId,
    };

    fn projection() -> StoredKeyedMvProjection {
        StoredKeyedMvProjection {
            mv_id: 17,
            facts: ProjectionFixture::new(MvTarget::from_parts(Some("ice"), "sales", "mv"), None)
                .build()
                .unwrap(),
        }
    }

    fn source(occurrence_id: u32) -> crate::mv::domain::rewrite::context::MvRewriteSourceSnapshot {
        let object_id =
            ConnectorTableObjectId::try_new(Bytes::from_static(b"source-object")).unwrap();
        crate::mv::domain::rewrite::context::MvRewriteSourceSnapshot {
            occurrence_id: SqlMvRelationOccurrenceId::new(occurrence_id),
            snapshot_id: 12,
            table_object_id: object_id.clone(),
            semantic_revision: ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
                ConnectorProviderId::parse("iceberg").unwrap(),
                &object_id,
                Some(12),
            )
            .unwrap(),
        }
    }

    #[test]
    fn sql_pin_preserves_sparse_definition_occurrence_order() {
        let projection = projection();
        let pin = sql_snapshot_pin(&projection, &[source(7), source(8)]).unwrap();
        assert_eq!(pin.occurrences()[0].occurrence_id().get(), 7);
        assert_eq!(pin.occurrences()[1].occurrence_id().get(), 8);
        assert!(sql_snapshot_pin(&projection, &[source(8), source(7)]).is_err());
    }
}
