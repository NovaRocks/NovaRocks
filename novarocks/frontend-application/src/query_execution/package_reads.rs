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

//! The frozen provider reads a compiled package carries.
//!
//! A package states each scan's read as one complete frozen value: the
//! provider relation and columns the plan addresses, the assignments and
//! predicate responsibilities the freeze negotiated, and the public facts the
//! provider published when it froze the read. The plan holds the first; the
//! freeze kept the rest, keyed by the scan occurrence it froze. This joins the
//! two and defaults nothing: every value comes from exactly one of them, and a
//! value neither holds is a refusal.
//!
//! What it authors is checked twice more by owners that do not trust it: the
//! package validator compares the read against the physical scan, and the
//! provider's pure compiler compares it against the private relation the
//! provider froze.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use novarocks_connector_contract::{
    Bound, ConnectorReadDistribution, ConnectorReadMetadataKind, ConnectorReadOrderingKey,
    ConnectorReadProperties, ConnectorReadPublicFacts, ConnectorReadRelationRecipeDraft,
    ConnectorReadStaticFacts, ConnectorValue, ConnectorValueType, Domain, FrozenConnectorRead,
    FrozenConnectorScan, Range, ScanColumnId, StaticScanAssignment, StaticScanDynamicFilter,
    TupleDomain, ValueSet,
};
use novarocks_physical_plan::{
    Fragment, NodeKind, PhysicalNode, PhysicalPlan, ProviderReadOccurrenceId, Relation,
    RuntimeFilterApplyPoint, RuntimeFilterId, ValueId,
};
use novarocks_spi::connector::read_stack::ConnectorReadColumnHandle;
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, PureCompileControl};

use crate::query_execution::package_freeze::PackageFreezeError;
use crate::query_execution::provider_read_facts::FrozenReadEncoding;

/// Every scan's complete frozen read, keyed by the occurrence the plan's scan
/// names.
///
/// Each scan of the plan must find the freeze of its occurrence, and each
/// freeze must belong to exactly one scan; either miss is a pairing defect,
/// refused rather than skipped.
pub(crate) fn author_frozen_reads(
    plan: &PhysicalPlan,
    encodings: &BTreeMap<ProviderReadOccurrenceId, FrozenReadEncoding>,
    control: &dyn PureCompileControl,
) -> Result<BTreeMap<ProviderReadOccurrenceId, FrozenConnectorRead>, PackageFreezeError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)
        .map_err(PackageFreezeError::Control)?;
    let result = author_all(plan, encodings, &mut work);
    // A primary interruption is final; an ordinary refusal still observes its
    // completed tail before it is reported.
    if matches!(&result, Err(PackageFreezeError::Control(_))) {
        return result;
    }
    work.finish().map_err(PackageFreezeError::Control)?;
    result
}

fn author_all(
    plan: &PhysicalPlan,
    encodings: &BTreeMap<ProviderReadOccurrenceId, FrozenReadEncoding>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<BTreeMap<ProviderReadOccurrenceId, FrozenConnectorRead>, PackageFreezeError> {
    let mut reads = BTreeMap::new();
    for fragment in plan.fragments().values() {
        for node in fragment.nodes().values() {
            work.step().map_err(PackageFreezeError::Control)?;
            let NodeKind::Scan { occurrence, .. } = &node.kind else {
                continue;
            };
            let encoding = encodings.get(occurrence).ok_or_else(|| {
                PackageFreezeError::Facts(format!(
                    "scan node {} reads provider occurrence {} with no frozen read",
                    node.id.get(),
                    occurrence.get()
                ))
            })?;
            let read = author_read(plan, fragment, node, encoding, work)?;
            if reads.insert(*occurrence, read).is_some() {
                return Err(PackageFreezeError::Facts(format!(
                    "provider read occurrence {} is scanned by more than one node",
                    occurrence.get()
                )));
            }
        }
    }
    if let Some(unscanned) = encodings
        .keys()
        .find(|occurrence| !reads.contains_key(occurrence))
    {
        return Err(PackageFreezeError::Facts(format!(
            "frozen provider read occurrence {} is scanned by no node",
            unscanned.get()
        )));
    }
    Ok(reads)
}

/// One scan's complete frozen read.
fn author_read(
    plan: &PhysicalPlan,
    fragment: &Fragment,
    node: &PhysicalNode,
    encoding: &FrozenReadEncoding,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FrozenConnectorRead, PackageFreezeError> {
    let NodeKind::Scan {
        relation,
        provider_outputs,
        ..
    } = &node.kind
    else {
        return Err(PackageFreezeError::Facts(format!(
            "node {} authors a frozen read but is not a scan",
            node.id.get()
        )));
    };
    let scan = node.id.get();
    let refused = |detail: String| PackageFreezeError::Read(format!("scan node {scan}: {detail}"));

    // The plan's relation columns and the freeze's assignments are the same
    // request ordinals, paired once at freeze. Re-pair them here only to prove
    // nothing reordered either side since.
    let fields = relation.schema();
    if fields.len() != encoding.assignments.len()
        || fields.len() != encoding.columns.len()
        || fields.len() != provider_outputs.len()
    {
        return Err(refused(format!(
            "relation has {} columns but the freeze assigned {} of {} named columns and the \
             scan produces {}",
            fields.len(),
            encoding.assignments.len(),
            encoding.columns.len(),
            provider_outputs.len()
        )));
    }
    for (ordinal, (field, (frozen, _))) in fields.iter().zip(encoding.columns.iter()).enumerate() {
        work.step().map_err(PackageFreezeError::Control)?;
        if field.column != *frozen || provider_outputs[ordinal].0 != *frozen {
            return Err(refused(format!(
                "relation column {ordinal} is not the provider column its freeze assigned"
            )));
        }
    }
    // The recipe draft is exactly what the plan addresses: the package
    // validator compares it field by field with the relation.
    let read = relation.read();
    let recipe = ConnectorReadRelationRecipeDraft::try_new(
        read.binding.clone(),
        read.relation.clone(),
        fields
            .iter()
            .map(|field| field.column.column_payload.clone())
            .collect(),
    )
    .map_err(|error| refused(format!("relation recipe: {error}")))?;

    // A provider column assigned more than once is one column: each predicate
    // about it is stated once, at the first ordinal that assigns it, which
    // every later ordinal reads the same values as.
    let mut ordinals = BTreeMap::<&ConnectorReadColumnHandle, ScanColumnId>::new();
    let mut assignments = Vec::with_capacity(encoding.assignments.len());
    for (ordinal, assignment) in encoding.assignments.iter().enumerate() {
        work.step().map_err(PackageFreezeError::Control)?;
        ordinals
            .entry(assignment.column())
            .or_insert(ScanColumnId::new(ordinal));
        assignments.push(StaticScanAssignment::new(
            Arc::from(assignment.variable()),
            assignment.value_type(),
        ));
    }
    let enforced = scan_domain(&encoding.enforced_predicate, &ordinals, Some(&assignments))
        .map_err(|detail| refused(format!("enforced predicate {detail}")))?;
    let unenforced = scan_domain(&encoding.unenforced_predicate, &ordinals, None)
        .map_err(|detail| refused(format!("unenforced predicate {detail}")))?;
    let dynamic_filters =
        scan_dynamic_filters(plan, fragment, node, provider_outputs, encoding).map_err(refused)?;
    let frozen_scan = FrozenConnectorScan::try_new(
        recipe,
        assignments,
        enforced,
        unenforced,
        encoding.remaining_expression.clone(),
        dynamic_filters,
    )
    .map_err(|error| refused(error.to_string()))?;

    // The public facts are the provider's own: its static facts as it froze
    // them and the fields it published, re-addressed by assignment ordinal.
    let schema = encoding.public_schema.as_ref().map_err(|error| {
        refused(format!(
            "the provider published no public read schema: {error}"
        ))
    })?;
    let source = scan_static_facts(&encoding.static_facts, &ordinals)
        .map_err(|detail| refused(format!("provider static facts {detail}")))?;
    let metadata_kind = match relation.as_ref() {
        Relation::Data(_) => None,
        Relation::Metadata(metadata) => Some(
            ConnectorReadMetadataKind::try_new(metadata.kind.as_str())
                .map_err(|error| refused(format!("metadata relation kind: {error}")))?,
        ),
    };
    work.flush().map_err(PackageFreezeError::Control)?;
    let execution = schema
        .execution_schema()
        .ok_or_else(|| refused("the provider published no complete execution schema".into()))?;
    let source_schema = schema
        .source_schema_facts()
        .ok_or_else(|| refused("the provider published no frozen source schema facts".into()))?;
    let public = ConnectorReadPublicFacts::try_new_typed_with_control(
        source,
        metadata_kind,
        execution,
        source_schema,
        work.control(),
    )
    .map_err(|error| match error {
        novarocks_connector_contract::PureProviderCompileError::Control(cause) => {
            PackageFreezeError::Control(cause)
        }
        novarocks_connector_contract::PureProviderCompileError::Provider(error) => {
            refused(format!("public read facts: {error}"))
        }
    })?;
    work.step().map_err(PackageFreezeError::Control)?;
    FrozenConnectorRead::try_new(frozen_scan, public).map_err(|error| refused(error.to_string()))
}

/// One predicate domain, addressed by assignment ordinal instead of provider
/// column. A domain over a column the read does not assign cannot be stated
/// about any ordinal, so it is refused rather than dropped.
fn scan_domain(
    domain: &TupleDomain<ConnectorReadColumnHandle>,
    ordinals: &BTreeMap<&ConnectorReadColumnHandle, ScanColumnId>,
    exact_assignments: Option<&[StaticScanAssignment]>,
) -> Result<TupleDomain<ScanColumnId>, String> {
    let Some(domains) = domain.domains() else {
        return Ok(TupleDomain::none());
    };
    let mut by_ordinal = BTreeMap::new();
    for (column, value) in domains {
        let ordinal = ordinals
            .get(column)
            .ok_or_else(|| "names a provider column the read does not assign".to_string())?;
        let exact = exact_assignments
            .and_then(|assignments| assignments.get(ordinal.index()))
            .and_then(|assignment| declared_temporal_domain(value, assignment.value_type()));
        by_ordinal.insert(*ordinal, exact.unwrap_or_else(|| value.clone()));
    }
    TupleDomain::with_column_domains(by_ordinal).map_err(|error| error.to_string())
}

/// Exact temporal offers can carry widened comparison bounds. Retag them
/// only when every endpoint is already a value of the exact assignment
/// declaration. The value and bound kind are unchanged, so membership on
/// the declared column is unchanged. Other mismatches face the original
/// frozen scan gate.
fn declared_temporal_domain(domain: &Domain, declared: ConnectorValueType) -> Option<Domain> {
    let (ConnectorValueType::Temporal(declared_type), ConnectorValueType::Temporal(offered_type)) =
        (declared, domain.value_type())
    else {
        return None;
    };
    if declared_type == offered_type || !declared_type.is_lossless_widening_to(offered_type) {
        return None;
    }
    let literal = |source: &ConnectorValue| -> Option<ConnectorValue> {
        let ConnectorValue::Temporal { data_type, value } = source else {
            return None;
        };
        if *data_type != offered_type {
            return None;
        }
        Some(ConnectorValue::Temporal {
            data_type: declared_type,
            value: value
                .validate(offered_type)
                .ok()?
                .validate(declared_type)
                .ok()?,
        })
    };
    let bound = |bound: &Bound| -> Option<Bound> {
        Some(match bound {
            Bound::Unbounded => Bound::Unbounded,
            Bound::Inclusive(value) => Bound::Inclusive(literal(value)?),
            Bound::Exclusive(value) => Bound::Exclusive(literal(value)?),
        })
    };
    let mut ranges = Vec::with_capacity(domain.values().ranges().len());
    for range in domain.values().ranges() {
        ranges.push(Range::try_new(declared, bound(range.low())?, bound(range.high())?).ok()?);
    }
    Some(Domain::new(
        ValueSet::of_ranges(declared, ranges).ok()?,
        domain.null_allowed(),
    ))
}

/// The provider's own static facts, with every column it names addressed by
/// assignment ordinal. Nothing is adjusted: the distribution is the
/// provider's answer, not the one the freeze stated to the plan.
fn scan_static_facts(
    facts: &ConnectorReadStaticFacts<ConnectorReadColumnHandle>,
    ordinals: &BTreeMap<&ConnectorReadColumnHandle, ScanColumnId>,
) -> Result<ConnectorReadStaticFacts<ScanColumnId>, String> {
    let ordinal = |column: &ConnectorReadColumnHandle| {
        ordinals
            .get(column)
            .copied()
            .ok_or_else(|| "name a provider column the read does not assign".to_string())
    };
    let keys = |keys: &[ConnectorReadColumnHandle]| {
        keys.iter()
            .map(ordinal)
            .collect::<Result<Vec<_>, String>>()
            .map(Arc::<[ScanColumnId]>::from)
    };
    let properties = facts.properties();
    let distribution = match properties.distribution() {
        ConnectorReadDistribution::Unconstrained => ConnectorReadDistribution::Unconstrained,
        ConnectorReadDistribution::Singleton => ConnectorReadDistribution::Singleton,
        ConnectorReadDistribution::RoundRobin => ConnectorReadDistribution::RoundRobin,
        ConnectorReadDistribution::Hash {
            keys: columns,
            partition_space,
            admissible,
            algorithm,
        } => ConnectorReadDistribution::Hash {
            keys: keys(&columns[..])?,
            partition_space: *partition_space,
            admissible: *admissible,
            algorithm: *algorithm,
        },
        ConnectorReadDistribution::BucketShuffle {
            keys: columns,
            partition_space,
            bucket_count,
            hash,
            layout,
            ordinal_domain_evidence,
        } => ConnectorReadDistribution::BucketShuffle {
            keys: keys(&columns[..])?,
            partition_space: *partition_space,
            bucket_count: *bucket_count,
            hash: *hash,
            layout: *layout,
            ordinal_domain_evidence: *ordinal_domain_evidence,
        },
    };
    let ordering = properties
        .ordering()
        .iter()
        .map(|key| {
            Ok(ConnectorReadOrderingKey::new(
                ordinal(key.column())?,
                key.direction(),
                key.null_ordering(),
            ))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let properties = ConnectorReadProperties::try_new(distribution, ordering)
        .map_err(|error| error.to_string())?;
    ConnectorReadStaticFacts::try_new(
        facts.input_version().clone(),
        facts.selection_digest(),
        properties,
        facts.artifact_coverage().clone(),
        facts.coverage_evidence().to_vec(),
    )
    .map_err(|error| error.to_string())
}

/// The runtime filters this scan applies at its source, one per filter, each
/// bound to the assignment that produces the value the filter constrains.
///
/// They come from the plan's own runtime-filter consumers, the same facts the
/// package validator checks them against, not from any wire encoding.
fn scan_dynamic_filters(
    plan: &PhysicalPlan,
    fragment: &Fragment,
    node: &PhysicalNode,
    provider_outputs: &[(novarocks_physical_plan::ProviderColumnReference, ValueId)],
    encoding: &FrozenReadEncoding,
) -> Result<Vec<StaticScanDynamicFilter>, String> {
    let mut values = BTreeMap::<RuntimeFilterId, BTreeSet<ValueId>>::new();
    for filter in plan.runtime_filters().values() {
        for consumer in &filter.consumers {
            if consumer.endpoint.fragment != fragment.id()
                || consumer.endpoint.node != node.id
                || consumer.apply_point != RuntimeFilterApplyPoint::ScanSource
            {
                continue;
            }
            let [value] = consumer.endpoint.values.as_ref() else {
                return Err(format!(
                    "runtime filter {} constrains {} values at the scan source",
                    filter.id.get(),
                    consumer.endpoint.values.len()
                ));
            };
            values.entry(filter.id).or_default().insert(*value);
        }
    }
    values
        .into_iter()
        .map(|(filter, values)| {
            let mut values = values.into_iter();
            let (Some(value), None) = (values.next(), values.next()) else {
                return Err(format!(
                    "runtime filter {} constrains more than one scan value",
                    filter.get()
                ));
            };
            let ordinal = provider_outputs
                .iter()
                .position(|(_, output)| *output == value)
                .ok_or_else(|| {
                    format!(
                        "runtime filter {} constrains a value the scan does not read",
                        filter.get()
                    )
                })?;
            Ok(StaticScanDynamicFilter::new(
                filter.get(),
                Arc::from(encoding.assignments[ordinal].variable()),
            ))
        })
        .collect()
}

#[cfg(test)]
mod tests;
