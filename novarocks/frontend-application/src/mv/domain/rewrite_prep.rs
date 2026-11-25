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

//! Application-side freezing for optional materialized-view rewrite.
//!
//! Repository enumeration and connector metadata reads belong here. The SQL
//! compiler receives the resulting immutable definition index and owns all
//! candidate parse/analyze/statistics/selection work.

use std::{collections::HashSet, sync::Arc};

use crate::catalog_application::query_bindings::{QueryTableBindingKey, QueryTableBindingStore};
use crate::mv::domain::readiness::MvCandidateReader;
use crate::mv::domain::refresh::definition::{read_stored_mv_query, stored_mv_identity};
use novarocks_mv_application::persistence::{
    codec::BindingDependency,
    exact_revision::{persist_exact_connector_revision, restore_exact_query_revision},
    keyed_documents::StoredKeyedMvProjection,
    keyed_documents::{MvKeyedObservedCurrentDocuments, decode_current_keyed_documents},
};
use novarocks_query_application::preparation::CatalogFactPort;
use novarocks_sql::compiler::{
    MaterializedViewFact, MaterializedViewNeed, SqlMvDefinitionResolutionContext,
    SqlMvRelationOccurrenceId, SqlMvRewriteBaseTableFacts, SqlMvRewriteDefinitionFacts,
    SqlMvRewritePublicationInput, SqlMvRewritePublicationRelation, SqlMvRewriteSelectionFacts,
    SqlMvRewriteSourceOccurrenceFacts,
};
use novarocks_types::naming::TableIdentity;

/// Freeze the optional rewrite facts for exactly one SQL completion need.
///
/// The repository can only enumerate a request-local inventory, so this
/// function filters it before any connector or storage observation. A
/// definition is relevant only when all of its base relations occur in the
/// need: SQL rejects a fact containing an unrequested base relation, and a
/// partial match would otherwise make that compiler contract ambiguous.
///
/// MV discovery and each candidate are optional accelerators. Their failures
/// therefore produce a missing or empty observed fact, never a query failure.
pub(crate) async fn freeze_materialized_view_fact_with_ports_async(
    need: &MaterializedViewNeed,
    candidate_reader: &MvCandidateReader,
    connector_control: &dyn novarocks_spi::connector::ConnectorControlResolver,
    bindings: &QueryTableBindingStore,
    catalog_facts: Option<&dyn CatalogFactPort>,
    context: &novarocks_spi::connector::ConnectorRequestContext,
) -> MaterializedViewFact {
    let definitions = match candidate_reader.list_candidate_definitions_async().await {
        Ok(definitions) => definitions,
        Err(error) => {
            tracing::debug!(error = %error, "skip MV rewrite discovery because the inventory is unavailable");
            return MaterializedViewFact::missing(
                need,
                "materialized-view rewrite inventory is unavailable",
            )
            .expect("a static MV inventory diagnostic is non-empty");
        }
    };
    let requested_relations = need
        .referenced_relations()
        .iter()
        .map(novarocks_types::naming::TableIdentity::fqn)
        .collect::<HashSet<_>>();
    let relevant = definitions.into_iter().filter(|definition| {
        candidate_base_tables_are_requested(
            &definition_base_names(definition),
            &requested_relations,
        )
    });
    let mut accepted = Vec::new();
    let views = bindings
        .rewrite_view_definitions(need.referenced_relations())
        .await;
    for definition in relevant {
        let identity = definition.mv_id.to_string();
        if let Some(facts) = accept_mv_rewrite_candidate(
            &identity,
            freeze_mv_rewrite_definition_async(
                candidate_reader,
                connector_control,
                bindings,
                &views,
                catalog_facts,
                definition,
                context,
            )
            .await,
        ) {
            accepted.push(facts);
        }
    }
    MaterializedViewFact::observed(need, accepted)
}

#[derive(Debug)]
enum MvRewriteDefinitionError {
    Unavailable(String),
    StoredDefinition(novarocks_sql::analyze_error::AnalyzeError),
}
impl From<String> for MvRewriteDefinitionError {
    fn from(message: String) -> Self {
        Self::Unavailable(message)
    }
}
impl From<&str> for MvRewriteDefinitionError {
    fn from(message: &str) -> Self {
        Self::Unavailable(message.into())
    }
}

fn accept_mv_rewrite_candidate(
    identity: &str,
    result: Result<SqlMvRewriteDefinitionFacts, MvRewriteDefinitionError>,
) -> Option<SqlMvRewriteDefinitionFacts> {
    match result {
        Ok(facts) => Some(facts),
        Err(MvRewriteDefinitionError::StoredDefinition(error)) => {
            let public = error.to_user_error(None);
            tracing::warn!(candidate = identity,
                mv = ?error.stored_definition().map(|definition| definition.name()),
                error_code = public.code().as_str(), error = public.message(),
                "skip invalid stored MV rewrite definition");
            None
        }
        Err(MvRewriteDefinitionError::Unavailable(error)) => {
            let diagnostic =
                novarocks_mv_application::candidate::CandidateDiagnostic::new(identity, error);
            tracing::debug!(
                candidate = diagnostic.identity(),
                error = diagnostic.message(),
                "skip unavailable MV rewrite candidate"
            );
            None
        }
    }
}

fn candidate_base_tables_are_requested(
    base_table_refs: &[String],
    requested_relations: &HashSet<String>,
) -> bool {
    !base_table_refs.is_empty()
        && base_table_refs
            .iter()
            .all(|base_table| requested_relations.contains(base_table))
}

fn definition_base_names(projection: &StoredKeyedMvProjection) -> Vec<String> {
    projection
        .facts
        .binding()
        .occurrences
        .iter()
        .map(occurrence_table)
        .map(|table| table.fqn())
        .collect()
}

fn occurrence_table(
    occurrence: &novarocks_mv_application::persistence::codec::BindingOccurrence,
) -> TableIdentity {
    TableIdentity {
        catalog: occurrence.catalog_at_binding.clone(),
        namespace: occurrence.namespace_at_binding.clone(),
        table: occurrence.relation_at_binding.clone(),
    }
}

async fn freeze_mv_rewrite_definition_async(
    candidate_reader: &MvCandidateReader,
    connector_control: &dyn novarocks_spi::connector::ConnectorControlResolver,
    bindings: &QueryTableBindingStore,
    views: &[Arc<novarocks_sql::planning::catalog::SqlViewDefinition>],
    catalog_facts: Option<&dyn CatalogFactPort>,
    projection: StoredKeyedMvProjection,
    context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<SqlMvRewriteDefinitionFacts, MvRewriteDefinitionError> {
    use novarocks_spi::connector::{
        ConnectorReadSelector, ConnectorTableIdentity, ConnectorTableObjectRebindRequest,
        ConnectorTableObjectSelector, ConnectorTableResolution,
    };
    let target = projection.facts.target();
    let target_catalog = target.catalog().ok_or("MV rewrite target has no catalog")?;
    let planning =
        crate::connector::acquire_metadata_planning_pin(connector_control, target_catalog)?;
    let context = crate::connector::context_for_planning_pin(&planning, context.clone())?;
    let documents = planning
        .derive_document_storage_lease()
        .map_err(|error| error.to_string())?;
    let request = crate::mv::domain::staged_create::keyed_request(
        &documents,
        ConnectorTableIdentity {
            instance_id: documents.owner().instance_id.clone(),
            namespace: Arc::from(target.namespace()),
            table: Arc::from(target.name()),
        },
        projection.facts.source_revision().target_object_id.clone(),
        context.clone(),
    );
    let frozen = candidate_reader
        .observe_current_async(&documents, request)
        .await?;
    let publication = frozen
        .publication
        .as_ref()
        .ok_or("MV rewrite target has no current publication")?;
    if frozen.versions.definition_revision()
        != projection.facts.source_revision().definition_revision
    {
        return Err(
            "MV rewrite discovery definition changed before its frozen publication read".into(),
        );
    }
    let target_binding = planning
        .binding()
        .metadata()
        .rebind_table_object_binding_async(ConnectorTableObjectRebindRequest {
            table: frozen.view.table.clone(),
            expected_object_id: frozen.view.object_id.clone(),
            resolution: ConnectorTableResolution::StrictBaseTable,
            selector: ConnectorTableObjectSelector::Current,
            context: context.clone(),
        })
        .await
        .map_err(|error| format!("bind frozen MV rewrite output object: {error}"))?;
    let snapshot = frozen
        .publication_snapshot
        .ok_or("MV rewrite output provider did not expose an exact snapshot selector")?;
    // The provider interprets its own typed output selector against the retained
    // object handle. Never decode output-version payloads or manufacture a fact.
    let target_revision = planning
        .binding()
        .metadata()
        .exact_semantic_revision(
            &target_binding.metadata.table,
            ConnectorReadSelector::SnapshotId(snapshot),
        )
        .map_err(|error| format!("resolve exact MV rewrite output revision: {error}"))?;
    let binding = &frozen.versions.binding_document;
    let mut nested = Vec::new();
    let mut sources = Vec::with_capacity(binding.occurrences.len());
    for (occurrence, input) in binding.occurrences.iter().zip(&publication.inputs) {
        if occurrence.occurrence_id != input.relation_occurrence_id {
            return Err("MV rewrite D/P occurrence order differs".into());
        }
        let table = occurrence_table(occurrence);
        let state = freeze_base_table_state(bindings, &table, &context, &mut nested)?;
        sources.push(SqlMvRewriteSourceOccurrenceFacts::try_new(
            SqlMvRelationOccurrenceId::new(occurrence.occurrence_id),
            table,
            occurrence.qualifier_at_binding.clone(),
            Some(
                restore_exact_query_revision(&input.object_id, &input.native_data_version)
                    .map_err(|error| format!("restore MV rewrite publication input: {error}"))?,
            ),
            state,
        )?);
    }
    let definition = &frozen.definition;
    let query = read_stored_mv_query(
        &definition.query.effective_sql,
        &stored_mv_identity(&projection),
    )
    .map_err(MvRewriteDefinitionError::StoredDefinition)?;
    let replayed_views;
    let views = if binding
        .dependencies
        .iter()
        .any(|dep| matches!(dep, BindingDependency::View { .. }))
    {
        let original =
            catalog_facts.ok_or("MV view dependency has no original catalog fact source")?;
        let resolution = &definition.query.resolution;
        let mut capture = novarocks_sql::compiler::SqlCapturedCatalogPreparation::try_new(
            query.clone(),
            Some(&resolution.default_catalog),
            &resolution.default_namespace,
            crate::query_execution::planning::sql_compile_control_from_connector_request(&context),
        )
        .map_err(|e| e.to_string())?;
        let mut requested = Vec::new();
        while !capture.needs().is_empty() {
            requested.extend(capture.needs().iter().map(|need| need.relation().clone()));
            let facts = original
                .resolve_relations(capture.needs())
                .await
                .map_err(|e| e.to_string())?;
            capture = capture
                .continue_catalog(facts.into_boxed_slice())
                .map_err(|e| e.to_string())?;
        }
        replayed_views = bindings.rewrite_view_definitions(&requested).await;
        &replayed_views
    } else {
        views
    };
    verify_rewrite_publication(&frozen, views, &nested)?;
    novarocks_mv_application::persistence::aggregate_bindings::validate_published_aggregate_contracts(&frozen.interpretation)?;
    let selection = freeze_mv_rewrite_selection(&frozen, target_revision)?;
    SqlMvRewriteDefinitionFacts::try_new(
        projection.mv_id,
        *frozen.versions.definition_revision().as_bytes(),
        query,
        SqlMvDefinitionResolutionContext::try_new(
            definition.query.resolution.default_catalog.clone(),
            definition.query.resolution.default_namespace.clone(),
        )?,
        planning
            .binding()
            .descriptor()
            .provider_id
            .as_str()
            .to_string(),
        Some(TableIdentity {
            catalog: target_catalog.to_string(),
            namespace: target.namespace().to_string(),
            table: target.name().to_string(),
        }),
        sources,
    )?
    .with_selection_facts(selection)
    .map_err(MvRewriteDefinitionError::from)
}

fn freeze_mv_rewrite_selection(
    frozen: &MvKeyedObservedCurrentDocuments,
    target_revision: novarocks_spi::connector::ConnectorExactSemanticRevision,
) -> Result<SqlMvRewriteSelectionFacts, String> {
    let publication = frozen
        .publication
        .as_ref()
        .ok_or("MV rewrite target has no current publication")?;
    let publication_id = publication
        .publication_id
        .as_bytes()
        .try_into()
        .map_err(|_| "MV SQL rewrite requires a 16-byte publication identity".to_string())?;
    let inputs = frozen
        .versions
        .binding_document
        .occurrences
        .iter()
        .zip(&publication.inputs)
        .map(|(occurrence, input)| {
            if occurrence.occurrence_id != input.relation_occurrence_id {
                return Err("MV rewrite D/P occurrence order differs".into());
            }
            SqlMvRewritePublicationInput::try_new(
                SqlMvRelationOccurrenceId::new(occurrence.occurrence_id),
                SqlMvRewritePublicationRelation::new(
                    occurrence_table(occurrence).fqn(),
                    restore_exact_query_revision(&input.object_id, &input.native_data_version)
                        .map_err(|error| format!("restore MV rewrite input: {error}"))?,
                )?,
            )
        })
        .collect::<Result<Vec<_>, String>>()?;
    let target = &frozen.view.table;
    SqlMvRewriteSelectionFacts::try_new_with_publication(
        publication_id,
        frozen.versions.binding.identity().to_bytes(),
        *frozen.versions.definition_revision().as_bytes(),
        *frozen.versions.interpretation_revision().as_bytes(),
        Arc::from(format!(
            "document:{}:output:{}",
            hex::encode(
                frozen
                    .publication_revision
                    .as_ref()
                    .ok_or("MV rewrite target has no P revision")?
                    .as_bytes()
            ),
            hex::encode(frozen.view.metadata_version.digest())
        )),
        inputs,
        SqlMvRewritePublicationRelation::new(
            format!(
                "{}.{}.{}",
                target.instance_id.as_str(),
                target.namespace,
                target.table
            ),
            target_revision,
        )?,
    )
}

fn freeze_base_table_state(
    bindings: &QueryTableBindingStore,
    table: &TableIdentity,
    context: &novarocks_spi::connector::ConnectorRequestContext,
    nested: &mut Vec<(
        novarocks_mv_application::persistence::identity::ObjectIdentity,
        novarocks_mv_application::persistence::identity::DocumentRevision,
    )>,
) -> Result<SqlMvRewriteBaseTableFacts, String> {
    let id = bindings
        .resolved_lookup(&QueryTableBindingKey::strict_base(
            &table.catalog,
            &table.namespace,
            &table.table,
        ))
        .and_then(Result::ok)
        .ok_or_else(|| {
            format!(
                "MV rewrite source {} has no original Current binding",
                table.fqn()
            )
        })?;
    let binding = bindings.binding(id)?;
    let metadata = binding
        .source_metadata
        .as_ref()
        .ok_or("MV rewrite source metadata is absent")?;
    let lease = binding.admission.exact_planning_pin()?;
    let context = crate::connector::context_for_planning_pin(&lease, context.clone())?;
    let provider = lease.binding().metadata();
    let revision = provider
        .exact_semantic_revision(
            &metadata.table,
            novarocks_spi::connector::ConnectorReadSelector::Current,
        )
        .map_err(|error| error.to_string())?;
    if let Some(view) = provider
        .project_current_keyed_documents(metadata, &lease, &context)
        .map_err(|error| error.to_string())?
    {
        let catalog = lease
            .binding()
            .catalog_handle()
            .map_err(|error| error.to_string())?
            .clone();
        let observed =
            decode_current_keyed_documents(view, catalog).map_err(|error| error.to_string())?;
        let publication = observed
            .publication
            .as_ref()
            .ok_or("MV rewrite upstream has no current publication")?;
        let markers = observed
            .decoded_markers()
            .map_err(|error| error.to_string())?;
        if novarocks_mv_application::persistence::codec::marker_eligibility(
            novarocks_mv_application::persistence::codec::PublicationEvidence::Current(
                &publication.publication_id,
            ),
            &markers,
        )
        .automatic_reuse_blocked
        {
            return Err("MV rewrite upstream publication is blocked".into());
        }
        let (object, _) =
            persist_exact_connector_revision(&revision).map_err(|error| error.to_string())?;
        nested.push((object, observed.versions.definition_revision()));
    }
    Ok(SqlMvRewriteBaseTableFacts::resolved(revision))
}

fn verify_rewrite_publication(
    frozen: &MvKeyedObservedCurrentDocuments,
    views: &[Arc<novarocks_sql::planning::catalog::SqlViewDefinition>],
    nested: &[(
        novarocks_mv_application::persistence::identity::ObjectIdentity,
        novarocks_mv_application::persistence::identity::DocumentRevision,
    )],
) -> Result<(), String> {
    use novarocks_mv_application::persistence::codec::{PublicationEvidence, marker_eligibility};

    verify_rewrite_dependencies(
        &frozen.versions.binding_document.dependencies,
        views,
        nested,
    )?;
    let publication = frozen
        .publication
        .as_ref()
        .ok_or("MV rewrite target has no current publication")?;
    let markers = frozen
        .decoded_markers()
        .map_err(|error| error.to_string())?;
    if marker_eligibility(
        PublicationEvidence::Current(&publication.publication_id),
        &markers,
    )
    .automatic_reuse_blocked
    {
        return Err("The current materialized-view publication has an active blocker".into());
    }
    Ok(())
}

fn verify_rewrite_dependencies(
    dependencies: &[BindingDependency],
    views: &[Arc<novarocks_sql::planning::catalog::SqlViewDefinition>],
    nested: &[(
        novarocks_mv_application::persistence::identity::ObjectIdentity,
        novarocks_mv_application::persistence::identity::DocumentRevision,
    )],
) -> Result<(), String> {
    use crate::mv::domain::persistence::source_bindings::view_binding_dependency;
    use novarocks_sql::planning::catalog::SqlMvViewDependency;

    for dependency in dependencies {
        let proven = match dependency {
            BindingDependency::View { depth, .. } => views.iter().any(|view| {
                SqlMvViewDependency::try_from_definition(view, *depth)
                    .ok()
                    .is_some_and(|fact| view_binding_dependency(&fact) == *dependency)
            }),
            BindingDependency::NestedMv {
                object_id,
                definition_revision,
                ..
            } => nested
                .iter()
                .any(|(object, revision)| object == object_id && revision == definition_revision),
        };
        if !proven {
            return Err("MV rewrite dependency lacks matching original statement evidence".into());
        }
    }
    if nested.iter().any(|(object, _)| {
        !dependencies.iter().any(|dependency| {
            matches!(
                dependency, BindingDependency::NestedMv { object_id, .. } if object_id == object
            )
        })
    }) {
        return Err("MV rewrite source acquired a materialized-view dependency".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, HashSet},
        sync::{Arc, Mutex},
    };

    use super::{
        MvRewriteDefinitionError, accept_mv_rewrite_candidate, candidate_base_tables_are_requested,
    };

    #[test]
    fn rewrite_requires_the_exact_original_view_declaration() {
        use super::{freeze_mv_rewrite_selection, verify_rewrite_publication};
        use crate::mv::domain::persistence::source_bindings::view_binding_dependency;
        use novarocks_mv_application::{
            persistence::{
                codec::{InvalidDefinitionKind, Marker, MarkerScope},
                test_support::{KeyedProjectionFixture, observed_current_keyed},
            },
            product::MvTarget,
        };
        use novarocks_spi::connector::{
            CatalogHandle, CatalogVersion, ConnectorExactSemanticRevision, ConnectorInstanceId,
            ConnectorProviderId,
        };
        use novarocks_sql::planning::catalog::{
            SqlMvViewDependency, SqlViewColumn, SqlViewDefinition, SqlViewDialect, SqlViewOrigin,
        };
        let stored = SqlViewDefinition {
            identity: novarocks_types::naming::TableIdentity::new("ice", "sales", "orders_view"),
            origin: SqlViewOrigin::Iceberg {
                uuid: [7; 16],
                version_id: 1,
            },
            dialect: SqlViewDialect::Trino,
            version: "exact-original-declaration".into(),
            raw_sql: "SELECT id FROM orders".into(),
            default_catalog: "ice".into(),
            default_database: "sales".into(),
            columns: Arc::from([SqlViewColumn {
                name: "id".into(),
                value_type: novarocks_type_contract::FunctionValueType::new(
                    arrow::datatypes::DataType::Int64,
                    false,
                ),
            }]),
        };
        let mut fixture =
            KeyedProjectionFixture::new(MvTarget::from_parts(Some("ice"), "sales", "mv"), Some(11));
        fixture.binding.dependencies.push(view_binding_dependency(
            &SqlMvViewDependency::try_from_definition(&stored, 1).unwrap(),
        ));
        let projection = fixture.build().unwrap();
        let catalog = CatalogHandle::new(
            ConnectorInstanceId::parse("ice").unwrap(),
            CatalogVersion::from_bytes([1; 32]),
        );
        let mut frozen = observed_current_keyed(&projection, catalog);
        assert!(verify_rewrite_publication(&frozen, &[Arc::new(stored.clone())], &[]).is_ok());
        let output = ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
            ConnectorProviderId::parse("iceberg").unwrap(),
            &frozen.view.object_id,
            frozen.publication_snapshot,
        )
        .unwrap();
        assert!(freeze_mv_rewrite_selection(&frozen, output).is_ok());
        assert!(verify_rewrite_publication(&frozen, &[], &[]).is_err());

        let changed_declaration = SqlViewDefinition {
            version: "exact-replacement-declaration".into(),
            raw_sql: "SELECT id FROM replacement_orders".into(),
            ..stored.clone()
        };
        let changed_version = SqlViewDefinition {
            origin: SqlViewOrigin::Iceberg {
                uuid: [7; 16],
                version_id: 2,
            },
            ..stored.clone()
        };
        for current in [changed_declaration, changed_version] {
            assert!(verify_rewrite_publication(&frozen, &[Arc::new(current)], &[]).is_err());
        }
        let (key, marker) = Marker::InvalidDefinition {
            scope: MarkerScope::Publication(
                frozen.publication.as_ref().unwrap().publication_id.clone(),
            ),
            kind: InvalidDefinitionKind::Resolution,
            diagnostic: "The publication's stored view could not be resolved".into(),
        }
        .encode()
        .unwrap();
        frozen.view.markers.insert(key, marker);
        assert!(verify_rewrite_publication(&frozen, &[Arc::new(stored)], &[]).is_err());
    }

    struct CapturedEvent {
        level: tracing::Level,
        fields: BTreeMap<String, String>,
    }

    #[derive(Clone, Default)]
    struct Capture(Arc<Mutex<Vec<CapturedEvent>>>);

    impl tracing::Subscriber for Capture {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }

        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}

        fn event(&self, event: &tracing::Event<'_>) {
            struct Fields(BTreeMap<String, String>);
            impl tracing::field::Visit for Fields {
                fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                    self.0.insert(field.name().into(), value.into());
                }

                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    self.0.insert(field.name().into(), format!("{value:?}"));
                }
            }
            let mut fields = Fields(BTreeMap::new());
            event.record(&mut fields);
            self.0.lock().unwrap().push(CapturedEvent {
                level: *event.metadata().level(),
                fields: fields.0,
            });
        }

        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
    }

    #[test]
    fn completion_need_filters_out_partial_mv_base_matches() {
        let requested = HashSet::from([
            "iceberg.sales.orders".to_string(),
            "iceberg.sales.customers".to_string(),
        ]);

        assert!(candidate_base_tables_are_requested(
            &[
                "iceberg.sales.orders".to_string(),
                "iceberg.sales.customers".to_string(),
            ],
            &requested,
        ));
        assert!(!candidate_base_tables_are_requested(
            &[
                "iceberg.sales.orders".to_string(),
                "iceberg.sales.lineitem".to_string(),
            ],
            &requested,
        ));
        assert!(!candidate_base_tables_are_requested(&[], &requested));
    }

    #[test]
    fn unreadable_stored_definition_is_skipped_with_a_typed_warn_event() {
        let source = "SELECT\n  1 + )";
        let target =
            novarocks_types::naming::TableIdentity::new("iceberg", "published", "broken_mv");
        let error = crate::mv::domain::refresh::definition::read_stored_mv_query(source, &target)
            .expect_err("stored query must fail in the real parser");
        let parser = novarocks_parser::parser::parse_query(source)
            .unwrap_err()
            .to_user_error(source);
        let location = parser.location().expect("parser source position");
        assert_eq!((location.line(), location.column()), (2, 7));

        let capture = Capture::default();
        tracing::subscriber::with_default(capture.clone(), || {
            assert!(
                accept_mv_rewrite_candidate(
                    "candidate-17",
                    Err(MvRewriteDefinitionError::StoredDefinition(error)),
                )
                .is_none()
            );
        });
        let events = capture.0.lock().unwrap();
        let [event] = events.as_slice() else {
            panic!("one diagnostic must describe the skipped candidate");
        };
        assert_eq!(event.level, tracing::Level::WARN);
        assert_eq!(event.fields["candidate"], "candidate-17");
        assert_eq!(event.fields["error_code"], "sql.analyze.invalid_view");
        assert!(event.fields["mv"].contains(&target.fqn()));
        assert!(event.fields["error"].contains(&target.fqn()));
        assert!(event.fields["error"].contains(parser.code().as_str()));
        assert!(event.fields["error"].contains(" at 2:7: "));
        assert!(event.fields["error"].contains(parser.message()));
    }

    #[test]
    fn unavailable_candidate_is_skipped_with_the_original_debug_diagnostic() {
        let capture = Capture::default();
        tracing::subscriber::with_default(capture.clone(), || {
            assert!(
                accept_mv_rewrite_candidate(
                    "candidate-18",
                    Err(MvRewriteDefinitionError::Unavailable(
                        "catalog binding temporarily unavailable".into(),
                    )),
                )
                .is_none()
            );
        });
        let events = capture.0.lock().unwrap();
        let [event] = events.as_slice() else {
            panic!("one diagnostic must describe the skipped candidate");
        };
        assert_eq!(event.level, tracing::Level::DEBUG);
        assert_eq!(event.fields["candidate"], "candidate-18");
        assert_eq!(
            event.fields["error"],
            "catalog binding temporarily unavailable"
        );
        assert!(!event.fields.contains_key("error_code"));
        assert!(
            event
                .fields
                .values()
                .all(|value| !value.contains("sql.analyze.invalid_view")),
        );
    }
}
