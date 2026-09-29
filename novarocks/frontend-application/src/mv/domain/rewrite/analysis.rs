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

//! Freeze one rewrite analysis from canonical D/L and one exact provider
//! observation.
//!
//! Every fact here is either a document fact or a provider fact from the same
//! generation L was validated against. Where the Connector contract does not
//! yet publish a typed fact the rewrite needs, this module fails closed: it
//! never decodes a provider-opaque identity, never rebuilds the retired
//! `MvSchemaContract`, and never degrades silently into wrong pruning.

use novarocks_mv_application::persistence::codec::{
    TargetPartitionFieldBinding, TargetPartitionTransform,
};
use novarocks_mv_application::persistence::projection::StoredMvProjection;
use novarocks_mv_application::persistence::runtime_bindings::MvRuntimeBindings;
use novarocks_mv_application::persistence::schema::MvPartitionContract;
use novarocks_sql::planning::mv::{
    SqlMvAggregateCalls, SqlMvJoinPredicateColumns, SqlMvRefreshJoinAnalysis,
};
use novarocks_sql::planning::mv_aggregate_layout::SqlMvAggregatePhysicalLayout;

use bytes::Bytes;
use novarocks_sql::compiler::{
    SqlImvJoinContractFacts, SqlImvJoinKindFacts, SqlImvJoinPredicateFacts, SqlImvPartitionFacts,
    SqlImvPartitionFieldFacts, SqlImvPartitionTransformFacts, SqlImvQualifiedFieldFacts,
    SqlMvRelationOccurrenceId,
};

use crate::mv::domain::rewrite::context::{
    MvRewriteAggregateAnalysis, MvRewriteAnalysisFacts, occurrence_table,
};

/// Everything one refresh attempt already froze before analysis is derived.
pub(crate) struct MvRewriteAnalysisInput<'a> {
    pub projection: &'a StoredMvProjection,
    /// Reconstructed against the exact target observation for this attempt.
    pub runtime_bindings: &'a MvRuntimeBindings,
    /// The provider's partition names and numeric scan-key spec from the same
    /// target generation. L owns the verified opaque IDs and typed transforms.
    pub observed_target_partition: &'a MvPartitionContract,
    /// D's explicit direct-join analysis, frozen from its effective query.
    pub join_analysis: SqlMvRefreshJoinAnalysis,
    /// SQL aggregate calls and the physical layout derived from D's query.
    pub aggregate: Option<(SqlMvAggregateCalls, SqlMvAggregatePhysicalLayout)>,
}

pub(crate) fn freeze_rewrite_analysis_facts(
    input: MvRewriteAnalysisInput<'_>,
) -> Result<MvRewriteAnalysisFacts, String> {
    let facts = &input.projection.facts;
    let definition = facts.definition();
    let interpretation = facts.interpretation();

    // A join rewrite needs each equality predicate expressed as a D occurrence
    // plus that occurrence's opaque source-field identity. Neither comes from
    // the analyzer -- its numeric field IDs are exactly the retired mapping
    // this contract removed. Both come from D: every occurrence carries the
    // qualifier the query gave it and, under it, each source field's opaque
    // provider identity beside the name it was bound under. Resolving the
    // predicate through the occurrence keeps a self-join's two references
    // apart, which an FQN map cannot.
    let join = match &input.join_analysis {
        SqlMvRefreshJoinAnalysis::NotDirectTwoTableJoin => None,
        SqlMvRefreshJoinAnalysis::ZeroKeyCrossJoin => {
            if input.aggregate.is_none() {
                return Err(
                    "zero-key CROSS JOIN refresh requires aggregate change-stream facts".into(),
                );
            }
            // Aggregate state merging consumes the independently bound logical
            // CROSS tree. It has no equality reachability/apply-key descriptor.
            None
        }
        SqlMvRefreshJoinAnalysis::InnerEquiJoin(predicates) => {
            Some(join_contract_facts(definition, predicates)?)
        }
    };

    // Affected-partition derivation needs the typed transform and the opaque
    // identity of each partition source field. The provider publishes the
    // transform typed, for this exact generation, in the same observation L
    // was validated against; the opaque identity is the target column's own,
    // reached through the physical column the observation names -- the same
    // bridge the aggregate state slots use. Pruning from anything weaker would
    // silently drop affected partitions, so an unresolvable source field fails
    // closed rather than narrowing the sweep.
    let partition = if interpretation.target.partition_fields.is_empty() {
        if !input.observed_target_partition.fields.is_empty() {
            return Err("MV target partition observation disagrees with canonical L".into());
        }
        None
    } else {
        Some(partition_facts(
            input.runtime_bindings,
            &interpretation.target.partition_fields,
            input.observed_target_partition,
        )?)
    };

    let aggregate = match input.aggregate {
        Some((calls, layout)) => {
            if interpretation.aggregates.is_empty() {
                return Err(
                    "MV rewrite analyzed aggregate calls for a definition with no aggregate \
                     interpretation"
                        .to_string(),
                );
            }
            Some(MvRewriteAggregateAnalysis { calls, layout })
        }
        None => {
            if !interpretation.aggregates.is_empty() {
                return Err(
                    "MV rewrite has no analyzed aggregate execution facts for an aggregate \
                     interpretation"
                        .to_string(),
                );
            }
            None
        }
    };

    Ok(MvRewriteAnalysisFacts {
        definition_revision: facts.source_revision().definition_revision,
        partition_spec_version: interpretation.target.partition_spec_version.clone(),
        join,
        partition,
        aggregate,
    })
}

/// Resolve a definition's join predicates into the occurrence-qualified
/// provider field identities the rewrite contract is stated in.
///
/// The qualifier is the only thing that separates two occurrences of one
/// relation, so it is matched exactly and must name exactly one occurrence;
/// the column is matched against the name that occurrence's field was bound
/// under, which is what D recorded beside the field's opaque identity.
fn join_contract_facts(
    definition: &novarocks_mv_application::persistence::codec::DefinitionDocument,
    predicates: &[SqlMvJoinPredicateColumns],
) -> Result<SqlImvJoinContractFacts, String> {
    let resolved = predicates
        .iter()
        .map(|predicate| {
            SqlImvJoinPredicateFacts::try_new(
                join_predicate_side(definition, &predicate.left)?,
                join_predicate_side(definition, &predicate.right)?,
            )
        })
        .collect::<Result<Vec<_>, String>>()?;
    SqlImvJoinContractFacts::try_new(SqlImvJoinKindFacts::InnerEquiJoin, resolved)
}

fn join_predicate_side(
    definition: &novarocks_mv_application::persistence::codec::DefinitionDocument,
    column: &novarocks_sql::planning::mv::SqlMvJoinColumnRef,
) -> Result<SqlImvQualifiedFieldFacts, String> {
    let mut matched = definition
        .relation_occurrences
        .iter()
        .filter(|occurrence| occurrence.qualifier_at_binding == column.qualifier);
    let occurrence = matched.next().ok_or_else(|| {
        format!(
            "MV join predicate names `{}`, which is no relation occurrence of this definition",
            column.qualifier
        )
    })?;
    if matched.next().is_some() {
        return Err(format!(
            "MV join predicate qualifier `{}` names more than one relation occurrence",
            column.qualifier
        ));
    }
    let field = occurrence
        .fields
        .iter()
        .find(|field| field.name_at_binding == column.column)
        .ok_or_else(|| {
            format!(
                "MV join predicate names `{}`.`{}`, which that occurrence did not bind",
                column.qualifier, column.column
            )
        })?;
    SqlImvQualifiedFieldFacts::try_new(
        SqlMvRelationOccurrenceId::new(occurrence.occurrence_id),
        occurrence_table(occurrence).fqn(),
        occurrence.qualifier_at_binding.clone(),
        Bytes::copy_from_slice(field.field_id.as_bytes()),
    )
}

/// Project the provider's typed partition observation into the rewrite's
/// partition contract, naming each source by the target column's own opaque
/// identity.
fn partition_facts(
    bindings: &MvRuntimeBindings,
    canonical: &[TargetPartitionFieldBinding],
    observed: &MvPartitionContract,
) -> Result<SqlImvPartitionFacts, String> {
    if canonical.len() != observed.fields.len() {
        return Err("MV target partition observation disagrees with canonical L".into());
    }
    let fields = canonical
        .iter()
        .zip(&observed.fields)
        .map(|(binding, field)| {
            let physical = target_physical_field(bindings, &binding.source_target_field_id)?;
            if physical.name != field.source_column_name
                || !partition_transform_matches(&binding.transform, &field.transform)
            {
                return Err("MV target partition observation disagrees with canonical L".into());
            }
            SqlImvPartitionFieldFacts::try_new(
                field.partition_field_name.clone(),
                Bytes::copy_from_slice(binding.source_target_field_id.as_bytes()),
                partition_transform_facts(&binding.transform),
            )
        })
        .collect::<Result<Vec<_>, String>>()?;
    SqlImvPartitionFacts::try_new(observed.target_spec_id, fields)
}

/// The one physical target column of this exact generation with that name.
fn target_physical_field<'a>(
    bindings: &'a MvRuntimeBindings,
    id: &novarocks_mv_application::persistence::identity::FieldIdentity,
) -> Result<&'a novarocks_mv_application::persistence::runtime_bindings::MvPhysicalFieldFacts, String>
{
    let mut matched = bindings
        .outputs
        .iter()
        .map(|(_, field)| field)
        .chain(bindings.apply_key.iter())
        .chain(bindings.branches.iter().map(|(_, field)| field))
        .chain(
            bindings
                .aggregates
                .iter()
                .flat_map(|aggregate| aggregate.states.iter().map(|state| &state.physical)),
        )
        .filter(|field| field.field_id == *id);
    let field = matched.next().ok_or_else(|| {
        "MV canonical partition source is no physical column of this target".to_string()
    })?;
    if matched.any(|other| other.name != field.name || other.field_id != field.field_id) {
        return Err("MV canonical partition source has conflicting physical bindings".into());
    }
    Ok(field)
}

fn partition_transform_facts(
    transform: &TargetPartitionTransform,
) -> SqlImvPartitionTransformFacts {
    use TargetPartitionTransform as Observed;
    match transform {
        Observed::Identity => SqlImvPartitionTransformFacts::Identity,
        Observed::Year => SqlImvPartitionTransformFacts::Year,
        Observed::Month => SqlImvPartitionTransformFacts::Month,
        Observed::Day => SqlImvPartitionTransformFacts::Day,
        Observed::Hour => SqlImvPartitionTransformFacts::Hour,
        Observed::Bucket { num_buckets } => SqlImvPartitionTransformFacts::Bucket {
            num_buckets: *num_buckets,
        },
        Observed::Truncate { width } => SqlImvPartitionTransformFacts::Truncate { width: *width },
        Observed::Void => SqlImvPartitionTransformFacts::Void,
    }
}

fn partition_transform_matches(
    binding: &TargetPartitionTransform,
    observed: &novarocks_mv_application::persistence::schema::MvPartitionTransformContract,
) -> bool {
    use novarocks_mv_application::persistence::schema::MvPartitionTransformContract as Observed;
    matches!(
        (binding, observed),
        (TargetPartitionTransform::Identity, Observed::Identity)
            | (TargetPartitionTransform::Year, Observed::Year)
            | (TargetPartitionTransform::Month, Observed::Month)
            | (TargetPartitionTransform::Day, Observed::Day)
            | (TargetPartitionTransform::Hour, Observed::Hour)
            | (TargetPartitionTransform::Void, Observed::Void)
    ) || match (binding, observed) {
        (
            TargetPartitionTransform::Bucket { num_buckets: left },
            Observed::Bucket { num_buckets: right },
        ) => left == right,
        (
            TargetPartitionTransform::Truncate { width: left },
            Observed::Truncate { width: right },
        ) => left == right,
        _ => false,
    }
}
