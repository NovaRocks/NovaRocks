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

//! Canonical documents frozen at the application-to-SQL rewrite boundary.
//! Runtime handles stay in the caller. Provider identities are never decoded.

use crate::mv::domain::refresh::planning::RefreshBaseRelationOccurrence;
use crate::mv::domain::refresh::target_binding::MvTargetBinding;
use crate::mv::domain::storage_observation::MvSchemaValidationObservation;
use crate::query_execution::mv_assembly::refresh_preparation::captured_sources::CapturedRefreshSources;
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use bytes::Bytes;
use novarocks_mv_application::persistence::{
    codec::{
        BindingDocument, InterpretationDocument, SourceFieldReference, StateRole,
        keyed::KeyedPublicationDocument,
    },
    exact_revision::{persist_exact_connector_revision, restore_exact_query_revision},
    identity::{DocumentRevision, PartitionSpecVersion},
    keyed_documents::{MvStrictRecoveryPublication, StoredKeyedMvProjection},
    runtime_bindings::{
        MvExactTargetSchemaFacts, MvPhysicalFieldFacts, MvRuntimeBindings,
        reconstruct_keyed_runtime_bindings, reconstruct_runtime_bindings,
    },
};
use novarocks_spi::connector::read_stack::{
    ConnectorReadChangeWindow, ConnectorReadSourceSelection,
};
use novarocks_spi::connector::{
    ConnectorChangeWindowFacts, ConnectorExactSemanticRevision, ConnectorTableObjectId,
};
use novarocks_sql::binding::SqlTableBindingId;
use novarocks_sql::compiler::{
    SqlImvAggregateContractFacts, SqlImvAggregateExecutionFacts,
    SqlImvAggregateExecutionStateColumnFacts, SqlImvAggregateStateColumnFacts,
    SqlImvAggregateStateRoleFacts, SqlImvAggregateVisibleColumnFacts, SqlImvBaseContractFacts,
    SqlImvBaseSnapshotFacts, SqlImvBranchContractFacts, SqlImvExpressionFacts,
    SqlImvExpressionKindFacts, SqlImvJoinContractFacts, SqlImvOutputColumnFacts,
    SqlImvPartitionFacts, SqlImvQualifiedFieldFacts, SqlImvRefreshHistoryFacts,
    SqlImvRewriteSnapshotBuilder, SqlImvRewriteSnapshotHandle, SqlImvSchemaContractFacts,
    SqlImvTargetColumnsFacts, SqlImvTargetContractFacts, SqlImvTargetVisibleColumnFacts,
    SqlMvRelationOccurrenceId,
};
use novarocks_sql::planning::mv::SqlMvAggregateCalls;
use novarocks_sql::planning::mv_aggregate_layout::SqlMvAggregatePhysicalLayout;
use novarocks_types::naming::TableIdentity;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

/// Facts from one provider observation of one D occurrence. Equal logical
/// names never merge two occurrences.
#[derive(Clone, Debug)]
pub struct MvRewriteSourceSnapshot {
    pub occurrence_id: SqlMvRelationOccurrenceId,
    pub snapshot_id: i64,
    pub table_object_id: ConnectorTableObjectId,
    pub semantic_revision: ConnectorExactSemanticRevision,
}

/// Analysis of this exact D under D's resolution context. Join predicates are
/// SQL facts; partition transforms must come from the same provider generation.
#[derive(Clone, Debug)]
pub struct MvRewriteAnalysisFacts {
    pub definition_revision: DocumentRevision,
    pub partition_spec_version: PartitionSpecVersion,
    pub join: Option<SqlImvJoinContractFacts>,
    pub partition: Option<SqlImvPartitionFacts>,
    pub aggregate: Option<MvRewriteAggregateAnalysis>,
    pub(crate) sql_source_fields: BTreeMap<u32, Vec<novarocks_sql::compiler::SqlImvBaseFieldFacts>>,
    pub(crate) direct_output_sources:
        crate::mv::domain::persistence::source_bindings::MvDirectOutputSources,
}

#[derive(Clone, Debug)]
pub struct MvRewriteAggregateAnalysis {
    pub calls: SqlMvAggregateCalls,
    pub layout: SqlMvAggregatePhysicalLayout,
}

#[derive(Debug)]
pub(crate) struct MvRewriteRebuildLayout {
    pub(crate) binding: BindingDocument,
    pub(crate) interpretation: InterpretationDocument,
}

#[derive(Debug)]
pub struct IcebergMvRewriteContext {
    pub target: TableIdentity,
    pub mv_id: i64,
    pub current_catalog: Option<String>,
    pub current_database: String,
    pub mv_definition: Arc<StoredKeyedMvProjection>,
    pub(crate) rebuild_layout: Option<Arc<MvRewriteRebuildLayout>>,
    pub canonical_select_query: Arc<novarocks_parser::ast::Query>,
    /// Ordered, one entry per base scan, in D's own occurrence order.
    pub base_refs: Arc<[RefreshBaseRelationOccurrence]>,
    pub pin: Arc<BTreeMap<SqlMvRelationOccurrenceId, MvRewriteSourceSnapshot>>,
    /// Retained complete history facts for activation; numeric projections
    /// below must never become an alternate source of publication identity.
    pub previous: Arc<[MvRewriteSourceSnapshot]>,
    pub(crate) recovery_publication: Option<Arc<MvStrictRecoveryPublication>>,
    pub previous_snapshot_ids: BTreeMap<SqlMvRelationOccurrenceId, i64>,
    pub previous_table_object_ids: BTreeMap<SqlMvRelationOccurrenceId, ConnectorTableObjectId>,
    pub(crate) equivalent_context_from: BTreeMap<SqlMvRelationOccurrenceId, i64>,
    pub target_snapshot_id: Option<i64>,
    pub target_table_uuid: String,
    pub(crate) target_write_fields: Arc<[novarocks_spi::connector::ConnectorWriteFieldRequest]>,
    pub runtime_bindings: MvRuntimeBindings,
    analysis: MvRewriteAnalysisFacts,
    sql_schema: SqlImvSchemaContractFacts,
}

impl IcebergMvRewriteContext {
    pub(crate) fn previous_publication(&self) -> Option<&KeyedPublicationDocument> {
        match &self.recovery_publication {
            Some(proof) => Some(proof.baseline_publication()),
            None => self.mv_definition.facts.publication(),
        }
    }

    pub(crate) fn content_join_contract(&self) -> Option<&SqlImvJoinContractFacts> {
        self.analysis.join.as_ref()
    }

    /// The exact revision each D occurrence was read at, keyed by D's own
    /// occurrence id. This is what P records as its input watermark, so the
    /// repeated occurrences of one self-joined object stay separate.
    pub fn exact_revisions_by_occurrence(
        &self,
    ) -> BTreeMap<u32, novarocks_spi::connector::ConnectorExactSemanticRevision> {
        self.pin
            .iter()
            .map(|(occurrence_id, source)| (occurrence_id.get(), source.semantic_revision.clone()))
            .collect()
    }

    pub(crate) fn from_parts(
        projection: Arc<StoredKeyedMvProjection>,
        rebuild_layout: Option<Arc<MvRewriteRebuildLayout>>,
        recovery_publication: Option<Arc<MvStrictRecoveryPublication>>,
        current: Vec<MvRewriteSourceSnapshot>,
        previous: Vec<MvRewriteSourceSnapshot>,
        target_binding: &MvTargetBinding,
        target_observation: &MvSchemaValidationObservation,
        analysis: MvRewriteAnalysisFacts,
    ) -> Result<Self, String> {
        let facts = &projection.facts;
        if let Some(proof) = &recovery_publication {
            if rebuild_layout.is_some() {
                return Err("MV strict replay cannot use a rebuild layout".into());
            }
            proof
                .validate_current_projection(facts)
                .map_err(|error| error.to_string())?;
        }
        let (binding, interpretation) = rebuild_layout
            .as_deref()
            .map(|layout| (&layout.binding, &layout.interpretation))
            .unwrap_or_else(|| (facts.binding(), facts.interpretation()));
        if analysis.definition_revision != facts.source_revision().definition_revision
            || analysis.partition_spec_version != interpretation.target.partition_spec_version
        {
            return Err("MV rewrite analysis belongs to a different D/L generation".into());
        }
        let target_table_uuid = target_binding.table_uuid().to_owned();
        if target_table_uuid.is_empty() {
            return Err("MV rewrite target UUID is absent".into());
        }
        if target_observation.table() != target_binding.identity() {
            return Err(
                "MV rewrite target write binding and schema observation name different tables"
                    .into(),
            );
        }
        // Keep the physical W fields and their complete declarations from the
        // same original Provider metadata as the independently observed L facts.
        let target_arrow_schema = target_binding.physical_write_schema()?;
        let target_write_fields = target_binding.write_input_fields()?;
        let exact_target = target_observation.exact_schema();
        if target_write_fields.len() != target_arrow_schema.fields().len()
            || target_write_fields
                .iter()
                .zip(target_arrow_schema.fields())
                .any(|(request, field)| {
                    request.execution_type().is_none()
                        || !novarocks_type_contract::arrow_fields_exact(
                            request.field(),
                            field.as_ref(),
                        )
                })
        {
            return Err("MV rewrite target fields lack their exact complete declarations".into());
        }
        let runtime_bindings = if rebuild_layout.is_some() {
            reconstruct_runtime_bindings(
                binding,
                interpretation,
                &facts.source_revision().target_object_id,
                &exact_target.metadata_version,
                exact_target,
            )?
        } else {
            reconstruct_keyed_runtime_bindings(facts, exact_target)?
        };
        validate_target_schema(&target_arrow_schema, exact_target)?;
        if rebuild_layout.is_none() && facts.publication().is_some() {
            novarocks_mv_application::persistence::aggregate_bindings::validate_published_aggregate_contracts(interpretation)?;
        }
        let pin = validate_source_pins(binding, current)?;
        let (previous_snapshot_ids, previous_table_object_ids) = if rebuild_layout.is_some() {
            if !previous.is_empty() {
                return Err("MV binding rebuild cannot reuse predecessor source pins".into());
            }
            (BTreeMap::new(), BTreeMap::new())
        } else {
            let publication = recovery_publication.as_ref().map_or_else(
                || facts.publication(),
                |proof| Some(proof.baseline_publication()),
            );
            validate_history(facts.binding(), publication, previous.clone())?
        };
        for (occurrence, previous_object) in &previous_table_object_ids {
            if pin.get(occurrence).map(|value| &value.table_object_id) != Some(previous_object) {
                return Err("MV rewrite source object changed since publication".into());
            }
        }
        validate_aggregate_analysis(&runtime_bindings, &analysis)?;
        let sql_schema = sql_schema_facts(binding, &runtime_bindings, &analysis)?;
        let definition = facts.definition();
        let canonical_select_query = Arc::new(
            crate::mv::domain::refresh::definition::read_stored_mv_query(
                &definition.query.effective_sql,
                &crate::mv::domain::refresh::definition::stored_mv_identity(&projection),
            )
            .map_err(|error| error.to_string())?,
        );
        let base_refs = binding
            .occurrences
            .iter()
            .map(|occurrence| RefreshBaseRelationOccurrence {
                occurrence_id: SqlMvRelationOccurrenceId::new(occurrence.occurrence_id),
                table: occurrence_table(occurrence),
            })
            .collect::<Vec<_>>();
        let target = facts.target();
        let target_snapshot_id = facts.metadata_version().snapshot_id();
        Ok(Self {
            target: TableIdentity {
                catalog: target
                    .catalog()
                    .ok_or("MV rewrite target has no catalog")?
                    .to_string(),
                namespace: target.namespace().to_string(),
                table: target.name().to_string(),
            },
            mv_id: projection.mv_id,
            current_catalog: Some(definition.query.resolution.default_catalog.clone()),
            current_database: definition.query.resolution.default_namespace.clone(),
            mv_definition: projection,
            rebuild_layout,
            canonical_select_query,
            base_refs: base_refs.into(),
            pin: Arc::new(pin),
            previous: previous.into(),
            recovery_publication,
            previous_snapshot_ids,
            previous_table_object_ids,
            equivalent_context_from: BTreeMap::new(),
            target_snapshot_id,
            target_table_uuid,
            target_write_fields,
            runtime_bindings,
            analysis,
            sql_schema,
        })
    }

    pub fn analysis_facts(&self) -> &MvRewriteAnalysisFacts {
        &self.analysis
    }

    pub(crate) fn binding(&self) -> &BindingDocument {
        self.rebuild_layout.as_deref().map_or_else(
            || self.mv_definition.facts.binding(),
            |layout| &layout.binding,
        )
    }

    pub(crate) fn interpretation(&self) -> &InterpretationDocument {
        self.rebuild_layout.as_deref().map_or_else(
            || self.mv_definition.facts.interpretation(),
            |layout| &layout.interpretation,
        )
    }

    pub(crate) fn retain_equivalent_source_contexts(
        &mut self,
        sources: &CapturedRefreshSources,
    ) -> Result<(), String> {
        if self.interpretation().is_aggregate() || self.previous.is_empty() {
            return Ok(());
        }
        for base in self.base_refs.iter() {
            let pin = self
                .pin
                .get(&base.occurrence_id)
                .ok_or("MV equivalent source context has no pinned source")?;
            let from = self.previous_snapshot_id(base)?;
            if from == pin.snapshot_id {
                continue;
            }
            let previous = self.previous_revision(base)?;
            let Some(source) = sources.by_table(&base.table) else {
                continue;
            };
            let Some(receipt) = source.receipt(Some(previous)) else {
                continue;
            };
            if !matches!(
                receipt.window_facts(),
                Some(ConnectorChangeWindowFacts::NetZero(_))
            ) {
                continue;
            }
            let request = receipt.request();
            let window = ConnectorReadChangeWindow::new(from, pin.snapshot_id);
            if request.to_revision() != &pin.semantic_revision
                || request.object_id() != &pin.table_object_id
                || request.selection() != ConnectorReadSourceSelection::ChangeWindow(window)
            {
                return Err("MV equivalent source context differs from its exact window".into());
            }
            self.equivalent_context_from
                .insert(base.occurrence_id, from);
        }
        Ok(())
    }

    /// The single D occurrence that names `table`.
    ///
    /// A definition may reference the same relation more than once. Those are
    /// distinct occurrences and must never be merged, so a locator-keyed
    /// lookup over a repeated relation is an explicit error rather than an
    /// arbitrary winner.
    /// Pinned current snapshot for one base relation occurrence.
    ///
    /// By occurrence rather than by table: a definition may read one table
    /// twice, and asking for "the pin for this table" would have to pick one
    /// of the two.
    pub(crate) fn pinned_snapshot_id(
        &self,
        base: &RefreshBaseRelationOccurrence,
    ) -> Result<i64, String> {
        self.pin
            .get(&base.occurrence_id)
            .map(|value| value.snapshot_id)
            .ok_or_else(|| format!("MV refresh pin has no entry for {}", base.display()))
    }

    /// Pinned current object identity for one base relation occurrence.
    pub(crate) fn pinned_table_object_id(
        &self,
        base: &RefreshBaseRelationOccurrence,
    ) -> Result<ConnectorTableObjectId, String> {
        self.pin
            .get(&base.occurrence_id)
            .map(|value| value.table_object_id.clone())
            .ok_or_else(|| format!("MV refresh pin has no entry for {}", base.display()))
    }

    /// Published predecessor snapshot for one base relation occurrence.
    pub(crate) fn previous_snapshot_id(
        &self,
        base: &RefreshBaseRelationOccurrence,
    ) -> Result<i64, String> {
        self.previous_snapshot_ids
            .get(&base.occurrence_id)
            .copied()
            .ok_or_else(|| {
                format!(
                    "MV refresh has no published predecessor snapshot for {}",
                    base.display()
                )
            })
    }

    pub(crate) fn previous_revision(
        &self,
        base: &RefreshBaseRelationOccurrence,
    ) -> Result<&ConnectorExactSemanticRevision, String> {
        let mut matches = self
            .previous
            .iter()
            .filter(|source| source.occurrence_id == base.occurrence_id);
        let source = matches
            .next()
            .ok_or_else(|| format!("MV refresh has no exact predecessor for {}", base.display()))?;
        if matches.next().is_some() {
            return Err(format!(
                "MV refresh repeats the exact predecessor for {}",
                base.display()
            ));
        }
        Ok(&source.semantic_revision)
    }

    /// Locator-keyed projections for the legacy publication intent. Both fail
    /// rather than merge when one relation occurs twice.
    /// What each occurrence was pinned at, with the table it names.
    ///
    /// Keyed by occurrence rather than by table: a definition may read one
    /// table twice, and each mention carries its own pin.
    pub(crate) fn pinned_snapshots_by_occurrence(
        &self,
    ) -> Result<BTreeMap<SqlMvRelationOccurrenceId, (TableIdentity, i64)>, String> {
        self.occurrence_keyed(|table, value| (table, value.snapshot_id))
    }

    pub(crate) fn pinned_objects_by_occurrence(
        &self,
    ) -> Result<BTreeMap<SqlMvRelationOccurrenceId, ConnectorTableObjectId>, String> {
        self.occurrence_keyed(|_, value| value.table_object_id.clone())
    }

    fn occurrence_keyed<T>(
        &self,
        project: impl Fn(TableIdentity, &MvRewriteSourceSnapshot) -> T,
    ) -> Result<BTreeMap<SqlMvRelationOccurrenceId, T>, String> {
        let mut values = BTreeMap::new();
        for occurrence in &self.binding().occurrences {
            let table = occurrence_table(occurrence);
            let occurrence_id = SqlMvRelationOccurrenceId::new(occurrence.occurrence_id);
            let pin = self
                .pin
                .get(&occurrence_id)
                .ok_or_else(|| format!("MV refresh pin has no entry for {}", table.fqn()))?;
            if values
                .insert(occurrence_id, project(table.clone(), pin))
                .is_some()
            {
                return Err(format!(
                    "MV definition records occurrence {} more than once",
                    occurrence.occurrence_id
                ));
            }
        }
        Ok(values)
    }

    pub(crate) fn to_sql_rewrite_snapshot_with_target_file_metadata(
        &self,
        target_binding: SqlTableBindingId,
        target_metadata: &novarocks_spi::connector::ConnectorTableMetadata,
    ) -> Result<SqlImvRewriteSnapshotHandle, String> {
        self.to_sql_rewrite_snapshot(target_binding)?
            .with_target_file_metadata(
                crate::query_execution::mv_assembly::iceberg_activation::target_file_metadata(
                    target_metadata,
                )?,
            )
    }

    pub(crate) fn content_target_fields(
        &self,
        target_metadata: &novarocks_spi::connector::ConnectorTableMetadata,
    ) -> Result<Vec<novarocks_sql::compiler::SqlImvContentField>, String> {
        use novarocks_spi::connector::{ConnectorTableColumnRole, ConnectorTableColumnVisibility};

        let mapping = &self.mv_definition.facts.mapping().columns;
        let outputs = &self.runtime_bindings.outputs;
        if self.interpretation().is_aggregate() || mapping.len() != outputs.len() {
            return Err("MV content target requires all mapped non-aggregate outputs".to_string());
        }
        let exact = target_metadata.execution_schema();
        let fields = exact.schema().fields();
        let declarations = exact.bindings();
        let column_facts = target_metadata.planning_facts.column_facts();
        mapping
            .iter()
            .zip(outputs)
            .enumerate()
            .map(|(ordinal, (mapped, (_, bound)))| {
                let storage_ordinal = usize::try_from(bound.ordinal)
                    .map_err(|_| "MV storage ordinal exceeds the original execution schema")?;
                let read = declarations
                    .get(storage_ordinal)
                    .ok_or("MV content target lost a signed read declaration")?;
                let field = fields
                    .get(storage_ordinal)
                    .ok_or("MV content target lost a signed read field")?;
                let fact = column_facts
                    .get(storage_ordinal)
                    .ok_or("MV content target lost a signed read role")?;
                if usize::try_from(mapped.output_ordinal).ok() != Some(ordinal)
                    || mapped.storage_field_id != bound.field_id
                    || mapped.storage_nullable != bound.nullable
                    || read.nullable() != mapped.storage_nullable
                    || field.name() != &bound.name
                    || fact.field_ordinal() != bound.ordinal
                    || fact.visibility() != ConnectorTableColumnVisibility::Sql
                    || fact.role() == ConnectorTableColumnRole::RowLineageSystem
                {
                    return Err(
                        "MV content read declaration differs from its exact mapped field".into(),
                    );
                }
                Ok(novarocks_sql::compiler::SqlImvContentField {
                    storage_field_id: bytes::Bytes::copy_from_slice(bound.field_id.as_bytes()),
                    read_column: novarocks_types::schema::ColumnDef::from_value_type(
                        bound.name.clone(),
                        read.clone(),
                        None,
                    )
                    .map_err(|cause| cause.to_string())?,
                })
            })
            .collect()
    }

    pub(crate) fn to_sql_content_rewrite_snapshot(
        &self,
        target_binding: SqlTableBindingId,
        target_metadata: &novarocks_spi::connector::ConnectorTableMetadata,
    ) -> Result<SqlImvRewriteSnapshotHandle, String> {
        use novarocks_mv_application::persistence::codec::MappingPushdownClass;
        let fields = self.content_target_fields(target_metadata)?;
        let demand_fields = self
            .mv_definition
            .facts
            .mapping()
            .columns
            .iter()
            .filter(|field| {
                matches!(
                    field.pushdown,
                    MappingPushdownClass::Identity | MappingPushdownClass::OrderPreservingInjective
                )
            })
            .map(|field| bytes::Bytes::copy_from_slice(field.storage_field_id.as_bytes()))
            .collect();
        self.to_sql_rewrite_snapshot_with_target_file_metadata(target_binding, target_metadata)?
            .with_content_target(fields)?
            .with_content_demand_fields(demand_fields)
    }

    pub fn to_sql_rewrite_snapshot(
        &self,
        target_binding: SqlTableBindingId,
    ) -> Result<SqlImvRewriteSnapshotHandle, String> {
        let mut builder =
            SqlImvRewriteSnapshotBuilder::try_new(self.target.clone(), target_binding, self.mv_id)?;
        for occurrence in &self.binding().occurrences {
            let id = SqlMvRelationOccurrenceId::new(occurrence.occurrence_id);
            let pin = self
                .pin
                .get(&id)
                .ok_or("MV rewrite occurrence pin is missing")?;
            let base = SqlImvBaseSnapshotFacts::try_new(
                id,
                occurrence_table(occurrence),
                occurrence.qualifier_at_binding.clone(),
                pin.snapshot_id,
                pin.table_object_id.clone(),
            )?;
            let base = match self.equivalent_context_from.get(&id) {
                Some(&from) => base.with_equivalent_context_from(from),
                None => base,
            };
            builder.add_base_snapshot(base)?;
        }
        builder.set_target_columns(SqlImvTargetColumnsFacts::try_new(
            self.target_write_fields
                .iter()
                .map(|field| {
                    let declared = field.execution_type().ok_or_else(|| {
                        "MV target has no complete execution declaration".to_string()
                    })?;
                    novarocks_types::schema::ColumnDef::from_value_type(
                        field.field().name().clone(),
                        declared.clone(),
                        None,
                    )
                    .map_err(|error| error.to_string())
                })
                .collect::<Result<Vec<_>, String>>()?,
        )?)?;
        builder.set_refresh_history(SqlImvRefreshHistoryFacts::try_new(
            self.previous_snapshot_ids.clone(),
            self.previous_table_object_ids.clone(),
            self.target_snapshot_id,
            self.target_table_uuid.clone(),
        )?)?;
        builder.set_schema_contract(self.sql_schema.clone())?;
        if let Some(analysis) = &self.analysis.aggregate {
            builder.set_aggregate_execution(aggregate_execution_facts(analysis)?)?;
        }
        builder.build()
    }
}

pub(in crate::mv::domain) fn occurrence_table(
    value: &novarocks_mv_application::persistence::codec::BindingOccurrence,
) -> TableIdentity {
    TableIdentity {
        catalog: value.catalog_at_binding.clone(),
        namespace: value.namespace_at_binding.clone(),
        table: value.relation_at_binding.clone(),
    }
}

fn validate_source_pins(
    definition: &BindingDocument,
    values: Vec<MvRewriteSourceSnapshot>,
) -> Result<BTreeMap<SqlMvRelationOccurrenceId, MvRewriteSourceSnapshot>, String> {
    let mut pins = BTreeMap::new();
    for value in values {
        if value.snapshot_id < 0 || pins.insert(value.occurrence_id, value).is_some() {
            return Err("MV rewrite source pins contain an invalid or repeated occurrence".into());
        }
    }
    if pins.len() != definition.occurrences.len() {
        return Err("MV rewrite source pins do not cover every D occurrence".into());
    }
    for occurrence in &definition.occurrences {
        let pin = pins
            .get(&SqlMvRelationOccurrenceId::new(occurrence.occurrence_id))
            .ok_or("MV rewrite source pin refers to an unknown occurrence")?;
        if pin.semantic_revision.object_identity().value().as_ref()
            != pin.table_object_id.as_bytes().as_ref()
        {
            return Err("MV rewrite source pin carries conflicting exact object facts".into());
        }
        let (persisted_object, _) = persist_exact_connector_revision(&pin.semantic_revision)
            .map_err(|error| format!("persist MV rewrite source identity: {error}"))?;
        if occurrence.object_id != persisted_object {
            return Err("MV rewrite pinned source object differs from D".into());
        }
    }
    Ok(pins)
}

type RefreshHistory = (
    BTreeMap<SqlMvRelationOccurrenceId, i64>,
    BTreeMap<SqlMvRelationOccurrenceId, ConnectorTableObjectId>,
);

fn validate_history(
    binding: &BindingDocument,
    publication: Option<&KeyedPublicationDocument>,
    previous: Vec<MvRewriteSourceSnapshot>,
) -> Result<RefreshHistory, String> {
    let published = match publication {
        None if previous.is_empty() => {
            return Ok((BTreeMap::new(), BTreeMap::new()));
        }
        None => {
            return Err("NeverPublished MV cannot have refresh history".into());
        }
        Some(published) => published,
    };
    let previous = validate_source_pins(binding, previous)?;
    let mut snapshots = BTreeMap::new();
    let mut objects = BTreeMap::new();
    for input in &published.inputs {
        let occurrence = SqlMvRelationOccurrenceId::new(input.relation_occurrence_id);
        let value = previous
            .get(&occurrence)
            .ok_or("MV rewrite history omits a publication occurrence")?;
        let expected = restore_exact_query_revision(&input.object_id, &input.native_data_version)
            .map_err(|error| format!("restore MV rewrite history revision: {error}"))?;
        if value.semantic_revision != expected {
            return Err(
                "MV rewrite history does not describe the exact published source revision".into(),
            );
        }
        snapshots.insert(occurrence, value.snapshot_id);
        objects.insert(occurrence, value.table_object_id.clone());
    }
    Ok((snapshots, objects))
}

fn validate_target_schema(
    schema: &SchemaRef,
    exact: &MvExactTargetSchemaFacts,
) -> Result<(), String> {
    if schema.fields().len() != exact.fields.len() {
        return Err("MV rewrite exact target fields do not cover its Arrow schema".into());
    }
    let mut ordered = exact.fields.iter().collect::<Vec<_>>();
    ordered.sort_by_key(|field| field.ordinal);
    for (ordinal, field) in ordered.into_iter().enumerate() {
        let arrow = schema.field(ordinal);
        if usize::try_from(field.ordinal).ok() != Some(ordinal)
            || arrow.name() != &field.name
            || arrow.is_nullable() != field.nullable
        {
            return Err(
                "MV rewrite target Arrow field differs from its exact provider fact".into(),
            );
        }
    }
    Ok(())
}

fn sql_lineage(
    definition: &BindingDocument,
    field: &SourceFieldReference,
) -> Result<SqlImvQualifiedFieldFacts, String> {
    let occurrence = definition
        .occurrences
        .iter()
        .find(|value| value.occurrence_id == field.occurrence_id)
        .ok_or("MV output references an unknown D occurrence")?;
    SqlImvQualifiedFieldFacts::try_new(
        SqlMvRelationOccurrenceId::new(field.occurrence_id),
        occurrence_table(occurrence).fqn(),
        occurrence.qualifier_at_binding.clone(),
        Bytes::copy_from_slice(field.field_id.as_bytes()),
    )
}

fn sql_schema_facts(
    definition: &BindingDocument,
    bindings: &MvRuntimeBindings,
    analysis: &MvRewriteAnalysisFacts,
) -> Result<SqlImvSchemaContractFacts, String> {
    let bases = definition
        .occurrences
        .iter()
        .map(|occurrence| {
            SqlImvBaseContractFacts::try_new(
                SqlMvRelationOccurrenceId::new(occurrence.occurrence_id),
                occurrence_table(occurrence).fqn(),
                Some(occurrence.qualifier_at_binding.clone()),
                analysis
                    .sql_source_fields
                    .get(&occurrence.occurrence_id)
                    .cloned()
                    .ok_or("MV rewrite lacks the prepared SQL fields for a D occurrence")?,
            )
        })
        .collect::<Result<_, String>>()?;
    let outputs = definition
        .outputs
        .iter()
        .map(|output| {
            Ok(SqlImvOutputColumnFacts::new(
                SqlImvExpressionFacts::try_new(
                    if analysis
                        .direct_output_sources
                        .contains_key(&output.identity())
                    {
                        SqlImvExpressionKindFacts::Column
                    } else {
                        SqlImvExpressionKindFacts::Mixed
                    },
                    output
                        .source_fields
                        .iter()
                        .map(|field| sql_lineage(definition, field))
                        .collect::<Result<_, _>>()?,
                )?,
            ))
        })
        .collect::<Result<_, String>>()?;
    let aggregate = if bindings.aggregates.is_empty() {
        None
    } else {
        let [apply_key] = bindings.apply_key.as_slice() else {
            return Err("SQL aggregate MV rewrite requires one physical apply key".into());
        };
        let Some(analyzed) = &analysis.aggregate else {
            return Err("MV rewrite lacks analyzed aggregate execution facts".into());
        };
        let states = aggregate_states_in_layout_order(bindings, analyzed)?
            .into_iter()
            .map(|(_, state)| {
                SqlImvAggregateStateColumnFacts::try_new(
                    state.physical.name.clone(),
                    state.physical.source_type.clone(),
                    state_role(state.role),
                )
            })
            .collect::<Result<_, _>>()?;
        // NativeColumnV1 is L's declared encoding, not an old contract version.
        Some(SqlImvAggregateContractFacts::try_new(
            1,
            apply_key.name.clone(),
            states,
        )?)
    };
    let branch = match bindings.branches.first() {
        None => None,
        Some((_, first)) => {
            if bindings
                .branches
                .iter()
                .any(|(_, field)| field.field_id != first.field_id)
            {
                return Err("SQL IMV rewrite requires a shared branch discriminator field".into());
            }
            Some(SqlImvBranchContractFacts::try_new(first.name.clone())?)
        }
    };
    let target = SqlImvTargetContractFacts::try_new(
        definition
            .outputs
            .iter()
            .zip(&bindings.outputs)
            .map(|(output, (_, field))| {
                SqlImvTargetVisibleColumnFacts::try_new(
                    output.name.clone(),
                    Bytes::copy_from_slice(field.field_id.as_bytes()),
                )
            })
            .collect::<Result<_, _>>()?,
        analysis.partition.clone(),
    )?;
    SqlImvSchemaContractFacts::try_new(
        bases,
        outputs,
        analysis.join.clone(),
        aggregate,
        branch,
        target,
    )
}

fn validate_aggregate_analysis(
    bindings: &MvRuntimeBindings,
    analysis: &MvRewriteAnalysisFacts,
) -> Result<(), String> {
    let Some(aggregate) = &analysis.aggregate else {
        return if bindings.aggregates.is_empty() {
            Ok(())
        } else {
            Err("MV rewrite lacks analyzed aggregate execution facts".into())
        };
    };
    let runtime = aggregate.layout.runtime_layout();
    if runtime.visible_columns().len() != bindings.outputs.len() {
        return Err("MV rewrite aggregate outputs differ from D/L".into());
    }
    for (column, (_, field)) in runtime.visible_columns().iter().zip(&bindings.outputs) {
        validate_physical_column(field, column.name(), column.data_type(), column.nullable())?;
    }
    let [apply_key] = bindings.apply_key.as_slice() else {
        return Err("MV rewrite aggregate requires one physical group row ID".into());
    };
    if aggregate.layout.row_id_column().column().name != apply_key.name {
        return Err("MV rewrite aggregate row ID differs from L".into());
    }
    for (column, state) in aggregate_states_in_layout_order(bindings, aggregate)? {
        validate_physical_column(
            &state.physical,
            column.name(),
            column.data_type(),
            column.nullable(),
        )?;
    }
    Ok(())
}

/// Pair each runtime state column with the L slot that holds it, in the
/// runtime layout's own column order.
///
/// A state slot describes storage -- which column, what type, which role --
/// so the column is what resolves it. That used to go through the aggregate
/// identity instead, on the premise that a slot belongs to one aggregate. It
/// does not: on a UNION view every branch has its own aggregate at an output
/// position and they all write the same column, so the column has several
/// aggregates and exactly one slot. Resolving by column says that directly,
/// and every persisted slot must still be consumed exactly once.
fn aggregate_states_in_layout_order<'a>(
    bindings: &'a MvRuntimeBindings,
    aggregate: &'a MvRewriteAggregateAnalysis,
) -> Result<
    Vec<(
        &'a novarocks_types::mv_aggregate_layout::MvAggregateStateColumn,
        &'a novarocks_mv_application::persistence::runtime_bindings::MvRuntimeStateBinding,
    )>,
    String,
> {
    let mut by_column: BTreeMap<
        &str,
        &novarocks_mv_application::persistence::runtime_bindings::MvRuntimeStateBinding,
    > = BTreeMap::new();
    for binding in &bindings.aggregates {
        for state in &binding.states {
            match by_column.insert(state.physical.name.as_str(), state) {
                None => {}
                // Branch aggregates share the column, so they share the slot
                // that describes it. Two different slots on one column would
                // be two descriptions of the same storage, and nothing could
                // say which one a read is looking at.
                Some(previous) if previous.slot_id == state.slot_id => {}
                Some(_) => {
                    return Err(
                        "MV interpretation describes one physical state column with two slots"
                            .into(),
                    );
                }
            }
        }
    }

    let mut used = BTreeSet::new();
    let mut ordered = Vec::new();
    for column in aggregate.layout.runtime_layout().state_columns() {
        let state = by_column.get(column.name()).ok_or_else(|| {
            format!(
                "MV interpretation has no aggregate state bound to physical column `{}`",
                column.name()
            )
        })?;
        if state_role(state.role) != runtime_state_role(column.state_role()) {
            return Err(format!(
                "MV interpretation binds physical column `{}` to a different state role",
                column.name()
            ));
        }
        used.insert(state.slot_id.clone());
        ordered.push((column, *state));
    }
    let persisted = bindings
        .aggregates
        .iter()
        .flat_map(|value| value.states.iter().map(|state| &state.slot_id))
        .collect::<BTreeSet<_>>();
    if used.len() != persisted.len() {
        return Err("MV rewrite aggregate analysis omits persisted state slots".into());
    }
    Ok(ordered)
}

fn validate_physical_column(
    field: &MvPhysicalFieldFacts,
    name: &str,
    data_type: &DataType,
    nullable: bool,
) -> Result<(), String> {
    let bound = arrow_type_from_provider_type(&field.source_type)?;
    if field.name != name
        || !denote_one_provider_type(&bound, data_type)
        || field.nullable != nullable
    {
        return Err(format!(
            "MV rewrite analyzed physical column differs from its L/provider binding: L has \
             name={} type={:?} nullable={}; the analysis wants name={} type={:?} nullable={}",
            field.name, field.source_type, field.nullable, name, data_type, nullable
        ));
    }
    Ok(())
}

/// Whether two Arrow types are the same provider type.
///
/// A contract type signature names the provider's type, and several Arrow
/// layouts can carry one of them: Iceberg has a single `binary`, which the
/// connector reads into either binary array width, and the aggregate state
/// column is written as the large one. Comparing the Arrow variants exactly
/// demands a coincidence of layouts the signature never promised, and the
/// aggregate layout's own validators already accept either.
fn denote_one_provider_type(left: &DataType, right: &DataType) -> bool {
    match (left, right) {
        (DataType::Binary | DataType::LargeBinary, DataType::Binary | DataType::LargeBinary) => {
            true
        }
        (DataType::Utf8 | DataType::LargeUtf8, DataType::Utf8 | DataType::LargeUtf8) => true,
        (left, right) => left == right,
    }
}

fn state_role(role: StateRole) -> SqlImvAggregateStateRoleFacts {
    match role {
        StateRole::Single => SqlImvAggregateStateRoleFacts::Single,
        StateRole::AvgSum => SqlImvAggregateStateRoleFacts::AvgSum,
        StateRole::AvgCount => SqlImvAggregateStateRoleFacts::AvgCount,
        StateRole::RetractionCount => SqlImvAggregateStateRoleFacts::RetractionCount,
    }
}
fn runtime_state_role(
    role: novarocks_types::mv_aggregate_layout::MvAggregateStateRole,
) -> SqlImvAggregateStateRoleFacts {
    use novarocks_types::mv_aggregate_layout::MvAggregateStateRole;
    match role {
        MvAggregateStateRole::Single => SqlImvAggregateStateRoleFacts::Single,
        MvAggregateStateRole::AvgSum => SqlImvAggregateStateRoleFacts::AvgSum,
        MvAggregateStateRole::AvgCount => SqlImvAggregateStateRoleFacts::AvgCount,
        MvAggregateStateRole::RetractionCount => SqlImvAggregateStateRoleFacts::RetractionCount,
    }
}
fn aggregate_execution_facts(
    analysis: &MvRewriteAggregateAnalysis,
) -> Result<SqlImvAggregateExecutionFacts, String> {
    let layout = analysis.layout.runtime_layout();
    SqlImvAggregateExecutionFacts::try_new(
        analysis.calls.group_keys.len(),
        analysis.calls.visible_outputs.clone(),
        analysis.layout.row_id_column().column().name.clone(),
        layout
            .visible_columns()
            .iter()
            .map(|column| {
                SqlImvAggregateVisibleColumnFacts::try_new(
                    column.name().to_string(),
                    column.value_type().clone(),
                )
            })
            .collect::<Result<_, _>>()?,
        layout
            .state_columns()
            .iter()
            .map(|column| {
                SqlImvAggregateExecutionStateColumnFacts::try_new(
                    column.name().to_string(),
                    column.value_type().clone(),
                    column.visible_source_index(),
                    column.aggregate_index(),
                    aggregate_function_kind(column.aggregate_kind()),
                    runtime_state_role(column.state_role()),
                    column.count_star(),
                )
            })
            .collect::<Result<_, _>>()?,
        layout.group_key_source_indexes().to_vec(),
        analysis
            .layout
            .physical_columns()
            .iter()
            .map(|column| column.column().name.clone())
            .collect(),
        layout.aggregate_input_types().to_vec(),
    )
}

fn aggregate_function_kind(
    kind: novarocks_types::mv_aggregate_layout::MvAggregateRuntimeKind,
) -> novarocks_sql::planning::mv::AggregateFunctionKind {
    use novarocks_sql::planning::mv::AggregateFunctionKind;
    use novarocks_types::mv_aggregate_layout::MvAggregateRuntimeKind;

    match kind {
        MvAggregateRuntimeKind::Count => AggregateFunctionKind::Count,
        MvAggregateRuntimeKind::Sum => AggregateFunctionKind::Sum,
        MvAggregateRuntimeKind::Avg => AggregateFunctionKind::Avg,
        MvAggregateRuntimeKind::Min => AggregateFunctionKind::Min,
        MvAggregateRuntimeKind::Max => AggregateFunctionKind::Max,
        MvAggregateRuntimeKind::BoolOr => AggregateFunctionKind::BoolOr,
        MvAggregateRuntimeKind::BoolAnd => AggregateFunctionKind::BoolAnd,
        MvAggregateRuntimeKind::CountDistinct => AggregateFunctionKind::CountDistinct,
        MvAggregateRuntimeKind::ApproxCountDistinct => AggregateFunctionKind::ApproxCountDistinct,
    }
}

/// The first UNION ALL branch as a standalone `Query` (keeps the branch's own
/// FROM — a scan, a join, or a fan-in union). Works off the AST so a composed
/// branch is not classified.
pub fn first_union_branch_query(
    query: &novarocks_parser::ast::Query,
) -> Result<novarocks_parser::ast::Query, String> {
    fn first_branch_body(
        body: &novarocks_parser::ast::SetExpr,
    ) -> Result<&novarocks_parser::ast::SetExpr, String> {
        match body {
            novarocks_parser::ast::SetExpr::SetOperation(operation) => {
                if operation.operator != novarocks_parser::ast::SetOperator::Union
                    || operation.distinct
                    || operation.corresponding.is_some()
                {
                    return Err(
                        "MV branch extraction requires UNION ALL without CORRESPONDING".into(),
                    );
                }
                let first = operation
                    .inputs
                    .first()
                    .ok_or_else(|| "MV UNION ALL has no input branches".to_string())?;
                first_branch_body(first)
            }
            novarocks_parser::ast::SetExpr::Query(subquery) => {
                let inner = &subquery.query;
                first_branch_body(inner.body.as_ref())
            }
            other => Ok(other),
        }
    }
    let mut branch = query.clone();
    branch.body =
        novarocks_parser::ast::AstBox::new(first_branch_body(query.body.as_ref())?.clone());
    Ok(branch)
}

pub(crate) fn arrow_type_from_provider_type(
    source_type: &novarocks_spi::connector::ProviderSourceType,
) -> Result<DataType, String> {
    use novarocks_spi::connector::ProviderSourceType;
    let ProviderSourceType::Iceberg(source_type) = source_type;
    let mut nodes = source_type.nodes().iter();
    provider_type_node(&mut nodes, 1)
}

fn provider_type_node(
    nodes: &mut std::slice::Iter<'_, novarocks_spi::connector::IcebergSourceTypeNode>,
    depth: usize,
) -> Result<DataType, String> {
    use arrow::datatypes::Field;
    use novarocks_spi::connector::{
        IcebergSourcePrimitive as Primitive, IcebergSourceTypeNode as Node,
    };
    if depth > novarocks_type_contract::MAX_VALUE_TYPE_DEPTH {
        return Err(novarocks_type_contract::ValueTypeError::TooDeep.to_string());
    }
    let error = || "MV source type has an invalid provider node".to_string();
    Ok(match nodes.next().ok_or_else(error)? {
        Node::Primitive(primitive) => match primitive {
            Primitive::Boolean => DataType::Boolean,
            Primitive::Int => DataType::Int32,
            Primitive::Long => DataType::Int64,
            Primitive::Float => DataType::Float32,
            Primitive::Double => DataType::Float64,
            Primitive::Date => DataType::Date32,
            Primitive::Time => DataType::Time64(TimeUnit::Microsecond),
            Primitive::Timestamp | Primitive::Timestamptz => {
                DataType::Timestamp(TimeUnit::Microsecond, None)
            }
            Primitive::TimestampNs | Primitive::TimestamptzNs => {
                DataType::Timestamp(TimeUnit::Nanosecond, None)
            }
            Primitive::String => DataType::Utf8,
            Primitive::Binary => DataType::Binary,
            Primitive::Uuid => DataType::FixedSizeBinary(16),
            Primitive::Variant => DataType::LargeBinary,
            Primitive::Fixed(width) => {
                DataType::FixedSizeBinary(i32::try_from(*width).map_err(|_| error())?)
            }
            Primitive::Decimal { precision, scale } => DataType::Decimal128(
                u8::try_from(*precision).map_err(|_| error())?,
                i8::try_from(*scale).map_err(|_| error())?,
            ),
        },
        Node::Struct { field_count } => {
            let fields = (0..*field_count)
                .map(|_| {
                    let Some(Node::Field { name, required, .. }) = nodes.next() else {
                        return Err(error());
                    };
                    Ok(Field::new(
                        name.as_ref(),
                        provider_type_node(nodes, depth + 1)?,
                        !required,
                    ))
                })
                .collect::<Result<Vec<_>, String>>()?;
            DataType::Struct(fields.into())
        }
        Node::List {
            element_required, ..
        } => DataType::List(Arc::new(Field::new(
            "item",
            provider_type_node(nodes, depth + 1)?,
            !element_required,
        ))),
        Node::Map { value_required, .. } => {
            let key = Field::new("key", provider_type_node(nodes, depth + 1)?, false);
            let value = Field::new(
                "value",
                provider_type_node(nodes, depth + 1)?,
                !value_required,
            );
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(vec![key, value].into()),
                    false,
                )),
                false,
            )
        }
        Node::Field { .. } => return Err(error()),
    })
}

#[cfg(test)]
mod tests;
