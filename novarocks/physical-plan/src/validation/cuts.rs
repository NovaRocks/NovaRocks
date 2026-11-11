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

use super::*;

pub(crate) fn inbound_cut_output_properties(
    cut: &crate::InboundFragmentCut,
) -> crate::PhysicalProperties {
    crate::PhysicalProperties {
        distribution: cut.partitioning.destination.clone(),
        row_multiplicity: cut.partitioning.destination_multiplicity,
        ordering: Box::default(),
    }
}

pub(crate) fn inbound_cut_matches_exchange_source(
    cut: &crate::InboundFragmentCut,
    node: &crate::PhysicalNode,
) -> bool {
    matches!(&node.kind, NodeKind::ExchangeSource { edge, imports }
        if *edge == cut.edge && imports.len() == cut.imports.len()
            && imports.iter().zip(&cut.imports).all(|((source, destination), cut)|
                *source == cut.source.value && *destination == cut.destination))
}

use std::collections::{BTreeMap, BTreeSet};

use crate::resource::{
    CutResourcePreflight, CutResourceUsage, MAX_PLAN_DERIVED_CUT_BYTES, MAX_PLAN_DERIVED_CUT_ITEMS,
};
use crate::{
    CutImport, CutValue, Edge, EdgeId, Fragment, FragmentCuts, FragmentId, FragmentSink,
    InboundFragmentCut, NodeId, NodeKind, OutboundFragmentCut, PhysicalPlan,
    RuntimeFilterBindingCut, RuntimeFilterBindingRole, ValueOrigin,
};

/// Derive the explicit cut contract used to validate one fragment without the
/// rest of the plan graph.
pub fn fragment_cuts(plan: &PhysicalPlan, fragment_id: FragmentId) -> Option<FragmentCuts> {
    let derivation = FragmentCutDerivation::new(plan, &PlanLimits::FROZEN)?;
    derivation.derive(plan, fragment_id)
}

/// Derive every independently verifiable fragment cut in one indexed pass.
pub fn derive_fragment_cuts(plan: &PhysicalPlan) -> Option<BTreeMap<FragmentId, FragmentCuts>> {
    let mut errors = ValidationContext::new();
    let derivation = FragmentCutDerivation::new(plan, &PlanLimits::FROZEN)?;
    let mut total_items = 0usize;
    let mut total_bytes = 0usize;
    for fragment in plan.fragments().keys().copied() {
        let usage = preflight_fragment_cut_resources(plan, fragment, &derivation, &mut errors)?;
        total_items = total_items.saturating_add(usage.items);
        total_bytes = total_bytes.saturating_add(usage.bytes);
    }
    if !errors.is_empty()
        || total_items > MAX_PLAN_DERIVED_CUT_ITEMS
        || total_bytes > MAX_PLAN_DERIVED_CUT_BYTES
    {
        return None;
    }
    plan.fragments()
        .keys()
        .copied()
        .map(|fragment| Some((fragment, derivation.derive_preflighted(plan, fragment)?)))
        .collect()
}

pub(crate) struct FragmentCutDerivation {
    pub(crate) inbound: BTreeMap<FragmentId, Vec<EdgeId>>,
    pub(crate) outbound: BTreeMap<FragmentId, Vec<EdgeId>>,
    pub(crate) change_stream_writers: BTreeMap<EdgeId, crate::ChangeStreamWriterCut>,
    /// Each fragment's slice of the plan-global binding numbering.
    pub(crate) runtime_filter_bindings: BTreeMap<FragmentId, Vec<RuntimeFilterBindingCut>>,
}

impl FragmentCutDerivation {
    pub(crate) fn new(plan: &PhysicalPlan, _limits: &PlanLimits) -> Option<Self> {
        let mut inbound = BTreeMap::<FragmentId, Vec<EdgeId>>::new();
        let mut outbound = BTreeMap::<FragmentId, Vec<EdgeId>>::new();
        for edge in plan.edges().values() {
            inbound
                .entry(edge.destination.fragment)
                .or_default()
                .push(edge.id);
            outbound
                .entry(edge.source.fragment)
                .or_default()
                .push(edge.id);
        }
        let mut change_stream_writers = BTreeMap::new();
        for fragment in plan.fragments().values() {
            let FragmentSink::Router { routes, .. } = fragment.sink() else {
                continue;
            };
            for route in routes {
                let Some(edge) = plan.edges().get(&route.edge) else {
                    continue;
                };
                if edge.kind != crate::EdgeKind::ChangeStreamRouter
                    || edge.source.fragment != fragment.id()
                {
                    continue;
                }
                if let Some(proof) = change_stream_writer_cut(route, edge) {
                    change_stream_writers.insert(edge.id, proof);
                }
            }
        }
        // The numbering is plan-global, so it is minted once here and every
        // fragment's cuts take their own slice of it.
        let mut runtime_filter_bindings = BTreeMap::<FragmentId, Vec<_>>::new();
        for binding in crate::runtime_filter_bindings(plan).ok()? {
            runtime_filter_bindings
                .entry(binding.fragment)
                .or_default()
                .push(binding.cut());
        }
        Some(Self {
            inbound,
            outbound,
            change_stream_writers,
            runtime_filter_bindings,
        })
    }

    pub(crate) fn derive(
        &self,
        plan: &PhysicalPlan,
        fragment_id: FragmentId,
    ) -> Option<FragmentCuts> {
        let mut errors = ValidationContext::new();
        preflight_fragment_cut_resources(plan, fragment_id, self, &mut errors)?;
        if !errors.is_empty() {
            return None;
        }
        self.derive_preflighted(plan, fragment_id)
    }

    pub(crate) fn derive_preflighted(
        &self,
        plan: &PhysicalPlan,
        fragment_id: FragmentId,
    ) -> Option<FragmentCuts> {
        fragment_cuts_from_edges(
            plan,
            fragment_id,
            self.inbound
                .get(&fragment_id)
                .map(Vec::as_slice)
                .unwrap_or_default(),
            self.outbound
                .get(&fragment_id)
                .map(Vec::as_slice)
                .unwrap_or_default(),
            self,
        )
    }

    pub(crate) fn change_stream_writer(
        &self,
        edge: EdgeId,
    ) -> Option<crate::ChangeStreamWriterCut> {
        self.change_stream_writers.get(&edge).cloned()
    }

    pub(crate) fn runtime_filter_bindings(
        &self,
        fragment: FragmentId,
    ) -> &[RuntimeFilterBindingCut] {
        self.runtime_filter_bindings
            .get(&fragment)
            .map(Vec::as_slice)
            .unwrap_or_default()
    }
}

pub(crate) fn preflight_fragment_cut_resources(
    plan: &PhysicalPlan,
    fragment_id: FragmentId,
    derivation: &FragmentCutDerivation,
    errors: &mut ValidationContext,
) -> Option<CutResourceUsage> {
    let fragment = plan.fragments().get(&fragment_id)?;
    let inbound = derivation
        .inbound
        .get(&fragment_id)
        .map(Vec::as_slice)
        .unwrap_or_default();
    let outbound = derivation
        .outbound
        .get(&fragment_id)
        .map(Vec::as_slice)
        .unwrap_or_default();
    let path = format!("fragments[{}].cuts.preflight", fragment_id.get());
    let mut usage = CutResourcePreflight::new();
    usage.add_items(inbound.len() + outbound.len());
    for (edge_id, is_outbound) in inbound
        .iter()
        .map(|edge| (edge, false))
        .chain(outbound.iter().map(|edge| (edge, true)))
    {
        let edge = plan.edges().get(edge_id)?;
        let source = plan.fragments().get(&edge.source.fragment)?;
        if let crate::OutputRole::Summary(kind) = edge.role {
            usage.add_items(kind.fields().len() * 2);
        }
        usage.add_items(edge.destination.receive_mapping.len() * if is_outbound { 2 } else { 1 });
        usage.add_distribution(&edge.partitioning.source);
        usage.add_distribution(&edge.partitioning.destination);
        for (source_value, _) in &edge.destination.receive_mapping {
            let ty = &source.values().get(source_value)?.ty;
            usage.add_value_type(ty, &path, errors);
            if is_outbound {
                usage.add_value_type(ty, &path, errors);
            }
        }
        if let Some(proof) = derivation.change_stream_writer(edge.id) {
            usage.add_items(proof.fields.len());
        }
        if let Some(proof) = writer_result_cut(plan, edge) {
            usage.add_items(proof.fields.len());
            for field in &proof.fields {
                usage.add_bytes(field.name.len());
                usage.add_value_type(&field.ty, &path, errors);
            }
        }
    }
    usage.add_items(fragment.runtime_filters().len());
    for filter in fragment.runtime_filters() {
        usage.add_filter(plan.runtime_filters().get(filter)?, &path, errors);
    }
    usage.add_items(derivation.runtime_filter_bindings(fragment_id).len());
    Some(usage.validate(&format!("{path}.resources"), errors))
}

pub(crate) fn fragment_cuts_from_edges(
    plan: &PhysicalPlan,
    fragment_id: FragmentId,
    inbound_edges: &[EdgeId],
    outbound_edges: &[EdgeId],
    derivation: &FragmentCutDerivation,
) -> Option<FragmentCuts> {
    let fragment = plan.fragments().get(&fragment_id)?;
    let inbound = inbound_edges
        .iter()
        .map(|edge| plan.edges().get(edge))
        .collect::<Option<Vec<_>>>()?
        .into_iter()
        .map(|edge| {
            let source = plan.fragments().get(&edge.source.fragment)?;
            let imports = edge
                .destination
                .receive_mapping
                .iter()
                .map(|(source_value, destination)| {
                    Some(CutImport {
                        source: CutValue {
                            value: *source_value,
                            ty: source.values().get(source_value)?.ty.clone(),
                        },
                        destination: *destination,
                    })
                })
                .collect::<Option<Vec<_>>>()?;
            Some(InboundFragmentCut {
                edge: edge.id,
                kind: edge.kind,
                source_fragment: edge.source.fragment,
                destination_node: edge.destination.node,
                imports: imports.into_boxed_slice(),
                partitioning: edge.partitioning.clone(),
                source_port: edge.source.port,
                delivery: edge.delivery,
                payload: edge_payload(plan, edge, derivation)?,
            })
        })
        .collect::<Option<Vec<_>>>()?;
    let outbound = outbound_edges
        .iter()
        .map(|edge| plan.edges().get(edge))
        .collect::<Option<Vec<_>>>()?
        .into_iter()
        .map(|edge| {
            let projection = edge
                .source
                .projection
                .iter()
                .map(|value| {
                    Some(CutValue {
                        value: *value,
                        ty: fragment.values().get(value)?.ty.clone(),
                    })
                })
                .collect::<Option<Vec<_>>>()?;
            Some(OutboundFragmentCut {
                edge: edge.id,
                kind: edge.kind,
                destination_fragment: edge.destination.fragment,
                destination_node: edge.destination.node,
                projection: projection.into_boxed_slice(),
                destination_imports: edge
                    .destination
                    .receive_mapping
                    .iter()
                    .map(|(source, destination)| {
                        Some(CutImport {
                            source: CutValue {
                                value: *source,
                                ty: fragment.values().get(source)?.ty.clone(),
                            },
                            destination: *destination,
                        })
                    })
                    .collect::<Option<Vec<_>>>()?
                    .into_boxed_slice(),
                partitioning: edge.partitioning.clone(),
                source_port: edge.source.port,
                delivery: edge.delivery,
                payload: edge_payload(plan, edge, derivation)?,
            })
        })
        .collect::<Option<Vec<_>>>()?;
    let runtime_filters = fragment
        .runtime_filters()
        .iter()
        .map(|id| plan.runtime_filters().get(id).cloned())
        .collect::<Option<Vec<_>>>()?;
    Some(FragmentCuts {
        inbound: inbound.into_boxed_slice(),
        outbound: outbound.into_boxed_slice(),
        runtime_filters: runtime_filters.into_boxed_slice(),
        runtime_filter_bindings: Box::from(derivation.runtime_filter_bindings(fragment_id)),
    })
}

fn edge_payload(
    plan: &PhysicalPlan,
    edge: &Edge,
    derivation: &FragmentCutDerivation,
) -> Option<crate::ExchangePayloadContract> {
    use crate::ExchangePayloadContract as Payload;
    match edge.role {
        crate::OutputRole::Relational => Some(Payload::Relational),
        crate::OutputRole::ProviderRoute { target } => {
            let proof = derivation.change_stream_writer(edge.id)?;
            if proof.write_target_ordinal != target {
                return None;
            }
            let destination = plan.fragments().get(&edge.destination.fragment)?;
            let root = destination.nodes().get(&destination.root())?;
            match &root.kind {
                NodeKind::PositionShard(spec) => Some(Payload::PositionContribution {
                    group: spec.group,
                    route: proof,
                }),
                _ => Some(Payload::ProviderRoute(proof)),
            }
        }
        crate::OutputRole::PositionShards { group, target } => {
            Some(Payload::PositionShards { group, target })
        }
        crate::OutputRole::WriterResults => {
            writer_result_cut(plan, edge).map(Payload::WriterResults)
        }
        crate::OutputRole::Summary(kind) => {
            let source = plan.fragments().get(&edge.source.fragment)?;
            let summary = source
                .summaries()
                .iter()
                .find(|summary| summary.kind() == kind && summary.edge == edge.id)?;
            if edge.destination.receive_mapping.len() != summary.columns.values().len()
                || !edge
                    .destination
                    .receive_mapping
                    .iter()
                    .map(|(s, _)| s)
                    .eq(summary.columns.values())
            {
                return None;
            }
            let mapping = &edge.destination.receive_mapping;
            let destination = match kind {
                crate::SummaryKind::WriteCounts => {
                    crate::SummaryColumns::WriteCounts([mapping[0].1, mapping[1].1, mapping[2].1])
                }
                crate::SummaryKind::DeleteVerification => {
                    crate::SummaryColumns::DeleteVerification([
                        mapping[0].1,
                        mapping[1].1,
                        mapping[2].1,
                        mapping[3].1,
                    ])
                }
            };
            Some(Payload::Summary(crate::SummaryCut {
                source: summary.columns,
                destination,
            }))
        }
    }
}

pub(crate) fn change_stream_writer_cut(
    route: &crate::ChangeStreamRoute,
    edge: &Edge,
) -> Option<crate::ChangeStreamWriterCut> {
    let proof = super::io_cuts::change_stream_writer_cut_ref(route, edge)?;
    Some(crate::ChangeStreamWriterCut {
        route_id: proof.route_id,
        write_target_ordinal: proof.write_target_ordinal,
        fields: proof.fields().collect(),
    })
}

pub(crate) fn writer_result_cut(
    plan: &PhysicalPlan,
    edge: &Edge,
) -> Option<crate::WriterResultCut> {
    let proof = super::io_cuts::writer_result_cut_ref(plan, edge)?;
    Some(crate::WriterResultCut {
        write_target_ordinal: proof.write_target_ordinal,
        schema_revision: proof.schema_revision,
        fields: proof
            .fields()
            .map(|field| crate::WriterResultCutField {
                source: field.source,
                destination: field.destination,
                name: field.name.into(),
                ty: field.ty.clone(),
                role: field.role,
            })
            .collect(),
    })
}

pub(crate) fn validate_fragment_cuts_into(
    fragment: &Fragment,
    cuts: &FragmentCuts,
    errors: &mut ValidationContext,
) {
    let path = format!("fragments[{}].cuts", fragment.id().get());
    bounded_count(
        errors,
        &format!("{path}.inbound"),
        cuts.inbound.len(),
        errors.limits().plan_edges,
    );
    bounded_count(
        errors,
        &format!("{path}.outbound"),
        cuts.outbound.len(),
        errors.limits().plan_edges,
    );
    bounded_count(
        errors,
        &format!("{path}.runtime_filters"),
        cuts.runtime_filters.len(),
        errors.limits().plan_runtime_filters,
    );
    bounded_count(
        errors,
        &format!("{path}.runtime_filter_bindings"),
        cuts.runtime_filter_bindings.len(),
        errors
            .limits()
            .plan_runtime_filters
            .saturating_mul(errors.limits().runtime_filter_endpoints),
    );
    let mut inbound_ids = BTreeSet::new();
    for cut in &cuts.inbound {
        validate_summary_cut(
            fragment,
            cut.source_port,
            &cut.payload,
            cut.delivery,
            cut.kind,
            &cut.partitioning,
            &cut.imports,
            false,
            errors,
        );
        bounded_count(
            errors,
            &format!("{path}.inbound.imports"),
            cut.imports.len(),
            errors.limits().fragment_values,
        );
        if !inbound_ids.insert(cut.edge) {
            errors.push(ValidationError::new(&path, "duplicate inbound edge"));
        }
        if cut.source_fragment == fragment.id() {
            errors.push(ValidationError::new(
                &path,
                "inbound cut has an invalid peer identity",
            ));
        }
        match fragment.nodes().get(&cut.destination_node) {
            Some(node) if inbound_cut_matches_exchange_source(cut, node) => {
                if node.output_properties != inbound_cut_output_properties(cut) {
                    errors.push(ValidationError::new(
                        &path,
                        "exchange source properties differ from its inbound cut",
                    ));
                }
            }
            Some(_) => errors.push(ValidationError::new(
                &path,
                "inbound cut does not match its exchange source node",
            )),
            None => errors.push(ValidationError::new(
                &path,
                "inbound cut destination node is not defined",
            )),
        }
        validate_distribution(
            fragment,
            &cut.partitioning.destination,
            "inbound_cut.destination_partitioning",
            errors,
        );
        validate_mapped_partitioning(
            &cut.partitioning,
            &cut.imports
                .iter()
                .map(|import| (import.source.value, import.destination))
                .collect::<Vec<_>>(),
            &path,
            errors,
        );
        for import in &cut.imports {
            match fragment.values().get(&import.destination) {
                // The imported column may admit null the sender never writes;
                // it is declared by the statement's column layout, not by the
                // value that fills it. It may not declare the reverse.
                Some(value)
                    if super::value_bindings_match_ignoring_root_nullability(
                        &value.ty,
                        &import.source.ty,
                    ) && (value.ty.nullable() || !import.source.ty.nullable())
                        && import_origin_matches(
                            &value.origin,
                            cut.edge,
                            cut.kind,
                            cut.source_fragment,
                            import.source.value,
                        ) => {}
                Some(_) => errors.push(ValidationError::new(
                    &path,
                    "inbound cut type or destination origin is inconsistent",
                )),
                None => errors.push(ValidationError::new(
                    &path,
                    "inbound cut destination value is not defined",
                )),
            }
        }
        if matches!(
            cut.payload,
            crate::ExchangePayloadContract::PositionContribution { .. }
        ) {
            validate_inbound_position_contribution(fragment, cut, &path, errors);
        } else {
            validate_inbound_change_stream_writer(fragment, cut, &path, errors);
        }
        validate_inbound_position_shards(fragment, cut, &path, errors);
        validate_inbound_writer_result_structure(fragment, cut, &path, errors);
    }
    let expected_inbound_list = fragment
        .nodes()
        .values()
        .filter_map(|node| match node.kind {
            NodeKind::ExchangeSource { edge, .. } => Some(edge),
            _ => None,
        })
        .collect::<Vec<_>>();
    let expected_inbound = expected_inbound_list
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    if expected_inbound.len() != expected_inbound_list.len() {
        errors.push(ValidationError::new(
            &path,
            "more than one exchange source claims the same inbound edge",
        ));
    }
    if inbound_ids != expected_inbound {
        errors.push(ValidationError::new(
            &path,
            "inbound cuts differ from the fragment exchange sources",
        ));
    }
    let mut inbound_indexes = BTreeMap::new();
    for cut in &cuts.inbound {
        inbound_indexes.entry(cut.edge).or_insert_with(|| {
            (
                cut,
                ValueMappingIndex::from_pairs_iter(
                    cut.imports
                        .iter()
                        .map(|import| (import.source.value, import.destination)),
                ),
            )
        });
    }
    for value in fragment.values().values() {
        let found = match value.origin {
            ValueOrigin::ExchangeImport { edge, source_value } => {
                inbound_indexes.get(&edge).is_some_and(|(cut, imports)| {
                    cut.kind != crate::EdgeKind::CteMulticast
                        && imports.contains(source_value, value.id)
                })
            }
            ValueOrigin::CteImport {
                edge,
                producer_fragment,
                producer_value,
            } => inbound_indexes.get(&edge).is_some_and(|(cut, imports)| {
                cut.kind == crate::EdgeKind::CteMulticast
                    && cut.source_fragment == producer_fragment
                    && imports.contains(producer_value, value.id)
            }),
            _ => continue,
        };
        if !found {
            errors.push(ValidationError::new(
                &path,
                "cross-fragment import is absent from the inbound cuts",
            ));
        }
    }

    let root_values = fragment
        .nodes()
        .get(&fragment.root())
        .map(|root| ValuePortIndex::new(&root.output.columns));
    let root_multiplicity = fragment
        .nodes()
        .get(&fragment.root())
        .map(|root| root.output_properties.row_multiplicity);
    let mut outbound_ids = BTreeSet::new();
    for cut in &cuts.outbound {
        validate_summary_cut(
            fragment,
            cut.source_port,
            &cut.payload,
            cut.delivery,
            cut.kind,
            &cut.partitioning,
            &cut.destination_imports,
            true,
            errors,
        );
        bounded_count(
            errors,
            &format!("{path}.outbound.projection"),
            cut.projection.len(),
            errors.limits().fragment_values,
        );
        if !outbound_ids.insert(cut.edge) {
            errors.push(ValidationError::new(&path, "duplicate outbound edge"));
        }
        if cut.destination_fragment == fragment.id() {
            errors.push(ValidationError::new(
                &path,
                "outbound cut has an invalid peer",
            ));
        }
        for projected in &cut.projection {
            match fragment.values().get(&projected.value) {
                Some(value) if value.ty != projected.ty => errors.push(ValidationError::new(
                    &path,
                    "outbound cut type differs from its source value",
                )),
                Some(_) => {}
                None => errors.push(ValidationError::new(
                    &path,
                    "outbound cut source value is not defined",
                )),
            }
        }
        if cut.destination_imports.len() != cut.projection.len()
            || cut
                .projection
                .iter()
                .zip(&cut.destination_imports)
                .any(|(projected, import)| projected != &import.source)
        {
            errors.push(ValidationError::new(
                &path,
                "outbound cut projection differs from its destination import mapping",
            ));
        }
        validate_mapped_partitioning(
            &cut.partitioning,
            &cut.destination_imports
                .iter()
                .map(|import| (import.source.value, import.destination))
                .collect::<Vec<_>>(),
            &path,
            errors,
        );
        if cut.source_port == crate::FragmentOutputPort::Root
            && root_values.as_ref().is_some_and(|root_values| {
                cut.projection
                    .iter()
                    .any(|projected| !root_values.contains(&projected.value))
            })
        {
            errors.push(ValidationError::new(
                &path,
                "outbound cut projects a value absent from the fragment root output",
            ));
        }
        if cut.source_port == crate::FragmentOutputPort::Root
            && root_multiplicity
                .is_some_and(|multiplicity| multiplicity != cut.partitioning.source_multiplicity)
        {
            errors.push(ValidationError::new(
                &path,
                "outbound cut row multiplicity differs from the fragment root",
            ));
        }
        validate_outbound_writer_result(fragment, cut, &path, errors);
        validate_outbound_position_cut(fragment, cut, &path, errors);
        validate_distribution(
            fragment,
            &cut.partitioning.source,
            "outbound_cut.source_partitioning",
            errors,
        );
        if cut.source_port == crate::FragmentOutputPort::Root
            && root_values.as_ref().is_some_and(|root_values| {
                distribution_values(&cut.partitioning.source)
                    .any(|value| !root_values.contains(&value))
            })
        {
            errors.push(ValidationError::new(
                &path,
                "outbound partition key is absent from the fragment root output",
            ));
        }
    }
    let mut sink_edges = match fragment.sink() {
        FragmentSink::Stream { edge } => vec![*edge],
        FragmentSink::Multicast { edges } => edges.to_vec(),
        FragmentSink::Router { routes, .. } => routes.iter().map(|route| route.edge).collect(),
        FragmentSink::Result | FragmentSink::RootResult(_) | FragmentSink::Noop => Vec::new(),
    };
    sink_edges.extend(fragment.summaries().iter().map(|summary| summary.edge));
    let sink_edge_ids = sink_edges.iter().copied().collect::<BTreeSet<_>>();
    if sink_edge_ids.len() != sink_edges.len() {
        errors.push(ValidationError::new(
            &path,
            "fragment sink destinations contain duplicate edge occurrences",
        ));
    }
    if sink_edge_ids != outbound_ids {
        errors.push(ValidationError::new(
            &path,
            "outbound cuts differ from the fragment sink destinations",
        ));
    }
    let outbound_by_edge = cuts
        .outbound
        .iter()
        .map(|cut| (cut.edge, cut))
        .collect::<BTreeMap<_, _>>();
    for summary in fragment.summaries() {
        let valid = outbound_by_edge.get(&summary.edge).is_some_and(|cut| {
            cut.source_port == crate::FragmentOutputPort::Summary(summary.kind())
                && cut.delivery == crate::DeliveryRequirement::MustDrain
                && cut.kind == crate::EdgeKind::Stream
                && cut.partitioning.destination == crate::Distribution::Singleton
                && cut.partitioning.source_multiplicity == crate::RowMultiplicity::SingleCopy
                && cut.partitioning.destination_multiplicity == crate::RowMultiplicity::SingleCopy
                && matches!(cut.payload, crate::ExchangePayloadContract::Summary(proof) if proof.source == summary.columns)
                && cut.projection.iter().map(|value| value.value).eq(summary.columns.values().iter().copied())
        });
        if !valid {
            errors.push(ValidationError::new(
                &path,
                "summary cut differs from its exact independent output contract",
            ));
        }
    }
    if let FragmentSink::Router { routes, .. } = fragment.sink() {
        for route in routes {
            let cut = outbound_by_edge.get(&route.edge).copied();
            if cut.is_none_or(|cut| {
                !cut.projection
                    .iter()
                    .map(|value| value.value)
                    .eq(route.input_mapping.iter().map(|(_, value)| *value))
            }) {
                errors.push(ValidationError::new(
                    &path,
                    "router edge projection differs from its exact route input sequence",
                ));
            }
            if let Some(cut) = cut {
                validate_router_partitioning(route, &cut.partitioning.source, &path, errors);
                let proof_matches = cut.change_stream_writer().is_some_and(|proof| {
                    proof.route_id == route.route_id
                        && proof.write_target_ordinal == route.write_target_ordinal
                        && proof.fields.len() == route.input_mapping.len()
                        && proof.fields.len() == cut.destination_imports.len()
                        && proof
                            .fields
                            .iter()
                            .zip(&route.input_mapping)
                            .zip(&cut.destination_imports)
                            .all(|((proof, (token, source)), import)| {
                                proof.token == *token
                                    && proof.source == *source
                                    && proof.source == import.source.value
                                    && proof.destination == import.destination
                            })
                });
                if !proof_matches {
                    errors.push(ValidationError::new(
                        &path,
                        "router outbound cut lacks its exact destination writer proof",
                    ));
                }
            }
        }
    }
    for cut in &cuts.outbound {
        if cut.kind != crate::EdgeKind::ChangeStreamRouter && cut.change_stream_writer().is_some() {
            errors.push(ValidationError::new(
                &path,
                "non-router outbound cut carries a change-stream writer proof",
            ));
        }
    }
    if let Some(root) = fragment.nodes().get(&fragment.root())
        && let NodeKind::PositionShard(spec) = &root.kind
        && (cuts.inbound.len() != spec.inputs.len()
            || cuts.outbound.len() != 1
            || cuts.inbound.iter().any(|cut| {
                !matches!(
                    cut.payload,
                    crate::ExchangePayloadContract::PositionContribution { .. }
                )
            })
            || !matches!(
                cuts.outbound[0].payload,
                crate::ExchangePayloadContract::PositionShards { .. }
            ))
    {
        errors.push(ValidationError::new(
            &path,
            "position aggregation must consume its complete contribution group and emit one shard stream",
        ));
    }
    validate_fragment_writer_results(fragment, cuts, &path, errors);
    validate_fragment_summary_inputs(fragment, cuts, errors);
    validate_fragment_runtime_filter_cuts(fragment, cuts, &path, errors);
    validate_fragment_runtime_filter_binding_cuts(
        fragment,
        cuts,
        &format!("{path}.runtime_filter_bindings"),
        errors,
    );
}

fn validate_inbound_position_contribution(
    fragment: &Fragment,
    cut: &InboundFragmentCut,
    path: &str,
    errors: &mut ValidationContext,
) {
    let crate::ExchangePayloadContract::PositionContribution { group, route } = &cut.payload else {
        return;
    };
    let matched = fragment.nodes().get(&fragment.root()).is_some_and(|root| {
        let NodeKind::PositionShard(spec) = &root.kind else {
            return false;
        };
        spec.group == *group
            && spec.target == route.write_target_ordinal
            && root.inputs.iter().zip(&spec.inputs).any(|(node, input)| {
                *node == cut.destination_node
                    && input.route == route.route_id
                    && input
                        .columns()
                        .into_iter()
                        .eq(cut.imports.iter().map(|import| import.destination))
                    && file_hash(&cut.partitioning.destination, input.file)
            })
    });
    if cut.kind != crate::EdgeKind::ChangeStreamRouter
        || cut.delivery != crate::DeliveryRequirement::MustDrain
        || group.to_bytes() == [0; 32]
        || !matched
        || !route
            .fields
            .iter()
            .map(|field| (field.source, field.destination))
            .eq(cut
                .imports
                .iter()
                .map(|import| (import.source.value, import.destination)))
    {
        errors.push(ValidationError::new(
            path,
            "inbound cut differs from its position contribution",
        ));
    }
}

fn file_hash(distribution: &crate::Distribution, file: crate::ValueId) -> bool {
    matches!(distribution, crate::Distribution::Hash { keys, scheme }
        if keys.as_ref() == [file]
            && scheme.definition.algorithm
                == novarocks_type_contract::PartitionHashAlgorithm::NativeHierarchicalV1)
}

fn validate_inbound_position_shards(
    fragment: &Fragment,
    cut: &InboundFragmentCut,
    path: &str,
    errors: &mut ValidationContext,
) {
    let crate::ExchangePayloadContract::PositionShards { group, target } = cut.payload else {
        return;
    };
    let dop = fragment.dop_domain();
    let writer_matches = fragment.nodes().get(&fragment.root()).is_some_and(|node| {
        matches!(&node.kind, NodeKind::TableWriter { target: writer }
            if node.inputs.as_ref() == [cut.destination_node]
                && writer.write_target_ordinal == target
                && writer.required_distribution == crate::Distribution::Singleton
                && writer.input.iter().copied().eq(cut.imports.iter().map(|i| i.destination)))
    });
    if !writer_matches
        || group.to_bytes() == [0; 32]
        || cut.kind != crate::EdgeKind::Stream
        || cut.delivery != crate::DeliveryRequirement::MustDrain
        || cut.partitioning.destination != crate::Distribution::Singleton
        || cut.partitioning.destination_multiplicity != crate::RowMultiplicity::SingleCopy
        || *fragment.stage_placement() != crate::StagePlacementPolicy::Singleton
        || dop.min != 1
        || dop.max != 1
        || !has_position_shard_layout(cut.imports.iter().map(|import| &import.source.ty))
    {
        errors.push(ValidationError::new(
            path,
            "position shards require their exact direct singleton writer at DOP 1",
        ));
    }
}

fn has_position_shard_layout<'a>(
    types: impl ExactSizeIterator<Item = &'a crate::ValueType>,
) -> bool {
    use arrow_schema::DataType;
    types.len() == 9
        && types.enumerate().all(|(ordinal, ty)| {
            let data_type = ty.data_type();
            if ordinal == 2 {
                return ty.nullable() && matches!(data_type, DataType::Struct(_));
            }
            !ty.nullable()
                && match ordinal {
                    0 => data_type == &DataType::Utf8,
                    1 | 4 | 5 => data_type == &DataType::Int32,
                    3 | 6 | 7 => data_type == &DataType::Int64,
                    8 => data_type == &DataType::Binary,
                    _ => false,
                }
        })
}

fn validate_outbound_position_cut(
    fragment: &Fragment,
    cut: &OutboundFragmentCut,
    path: &str,
    errors: &mut ValidationContext,
) {
    let valid = match &cut.payload {
        crate::ExchangePayloadContract::PositionContribution { group, route } => {
            group.to_bytes() != [0; 32]
                && cut.kind == crate::EdgeKind::ChangeStreamRouter
                && cut.delivery == crate::DeliveryRequirement::MustDrain
                && matches!(fragment.sink(), FragmentSink::Router { routes, .. }
                if routes.iter().any(|r| r.edge == cut.edge && r.route_id == route.route_id
                    && r.accepted_effects.iter().all(|effect| {
                        !matches!(effect, novarocks_connector_contract::ConnectorRowMutationEffect::Insert)
                    })))
                && route.fields.len() == 4
                && file_hash(&cut.partitioning.source, route.fields[0].source)
        }
        crate::ExchangePayloadContract::PositionShards { group, target } => {
            cut.kind == crate::EdgeKind::Stream
                && cut.delivery == crate::DeliveryRequirement::MustDrain
                && cut.partitioning.destination == crate::Distribution::Singleton
                && fragment.nodes().get(&fragment.root()).is_some_and(|node| {
                    matches!(&node.kind, NodeKind::PositionShard(spec)
                        if spec.group == *group && spec.target == *target
                            && spec.output.columns().into_iter()
                                .eq(cut.projection.iter().map(|column| column.value)))
                })
        }
        _ => true,
    };
    if !valid {
        errors.push(ValidationError::new(
            path,
            "outbound cut differs from its position shard contract",
        ));
    }
}

pub(crate) fn validate_inbound_change_stream_writer(
    fragment: &Fragment,
    cut: &InboundFragmentCut,
    path: &str,
    errors: &mut ValidationContext,
) {
    if cut.kind != crate::EdgeKind::ChangeStreamRouter {
        if cut.change_stream_writer().is_some() {
            errors.push(ValidationError::new(
                path,
                "non-router inbound cut carries a change-stream writer proof",
            ));
        }
        return;
    }
    let Some(proof) = &cut.change_stream_writer() else {
        errors.push(ValidationError::new(
            path,
            "router inbound cut lacks its destination writer proof",
        ));
        return;
    };
    let writer = fragment.nodes().get(&fragment.root());
    let target = writer.and_then(|writer| match &writer.kind {
        NodeKind::TableWriter { target } if writer.inputs.as_ref() == [cut.destination_node] => {
            Some(target)
        }
        _ => None,
    });
    let Some(target) = target else {
        errors.push(ValidationError::new(
            path,
            "router inbound cut receiver is not the direct root table writer input",
        ));
        return;
    };
    if proof.route_id == crate::ConnectorWriteRouteId::from_bytes([0; 32])
        || proof.write_target_ordinal != target.write_target_ordinal
        || proof.fields.len() != cut.imports.len()
        || proof.fields.len() != target.target_fields.len()
        || !proof
            .fields
            .iter()
            .zip(&cut.imports)
            .zip(&target.target_fields)
            .all(|((proof, import), target)| {
                proof.source == import.source.value
                    && proof.destination == import.destination
                    && proof.token == target.token
                    && proof.destination == target.input
            })
    {
        errors.push(ValidationError::new(
            path,
            "router inbound cut proof differs from its exact table writer contract",
        ));
    }
}

pub(crate) fn validate_inbound_writer_result_structure(
    fragment: &Fragment,
    cut: &InboundFragmentCut,
    path: &str,
    errors: &mut ValidationContext,
) {
    if cut.kind != crate::EdgeKind::Stream {
        if cut.writer_result().is_some() {
            errors.push(ValidationError::new(
                path,
                "non-stream inbound cut carries a writer result proof",
            ));
        }
        return;
    }
    let Some(proof) = &cut.writer_result() else {
        return;
    };
    let matches = proof.schema_revision == crate::WRITER_MULTIPLEX_SCHEMA_REVISION
        && proof.fields.len() == cut.imports.len()
        && proof
            .fields
            .iter()
            .zip(&cut.imports)
            .all(|(field, import)| {
                field.source == import.source.value
                    && field.destination == import.destination
                    && field.ty == import.source.ty
                    && fragment
                        .values()
                        .get(&field.destination)
                        .is_some_and(|value| value.ty == field.ty)
            });
    if !matches {
        errors.push(ValidationError::new(
            path,
            "inbound writer result proof differs from its stream import contract",
        ));
    }
}

pub(crate) fn validate_outbound_writer_result(
    fragment: &Fragment,
    cut: &OutboundFragmentCut,
    path: &str,
    errors: &mut ValidationContext,
) {
    if cut.kind != crate::EdgeKind::Stream {
        if cut.writer_result().is_some() {
            errors.push(ValidationError::new(
                path,
                "non-stream outbound cut carries a writer result proof",
            ));
        }
        return;
    }
    let root = fragment.nodes().get(&fragment.root());
    let target = root.and_then(|root| match &root.kind {
        NodeKind::TableWriter { target } => Some(target),
        _ => None,
    });
    match (target, &cut.writer_result()) {
        (None, None) => {}
        (None, Some(_)) => errors.push(ValidationError::new(
            path,
            "non-writer stream carries a writer result proof",
        )),
        (Some(_), None) => errors.push(ValidationError::new(
            path,
            "table writer stream lacks its writer result proof",
        )),
        (Some(target), Some(proof)) => {
            let fields_match = proof.write_target_ordinal == target.write_target_ordinal
                && proof.schema_revision == target.output_schema.revision
                && proof.fields.len() == target.output_schema.fields.len()
                && proof.fields.len() == cut.destination_imports.len()
                && proof
                    .fields
                    .iter()
                    .zip(&target.output_schema.fields)
                    .zip(&cut.destination_imports)
                    .all(|((proof, field), import)| {
                        proof.source == field.value
                            && proof.destination == import.destination
                            && import.source.value == field.value
                            && proof.name == field.name
                            && proof.ty == field.ty
                            && proof.ty == import.source.ty
                            && proof.role == field.role
                    });
            if !fields_match {
                errors.push(ValidationError::new(
                    path,
                    "outbound writer result proof differs from its exact table writer schema",
                ));
            }
        }
    }
}

pub(crate) fn validate_fragment_writer_results(
    fragment: &Fragment,
    cuts: &FragmentCuts,
    path: &str,
    errors: &mut ValidationContext,
) {
    let inbound_by_edge = cuts
        .inbound
        .iter()
        .map(|cut| (cut.edge, cut))
        .collect::<BTreeMap<_, _>>();
    let mut consumed_inbound = BTreeMap::<EdgeId, usize>::new();
    let mut consumed_writers = BTreeMap::<NodeId, usize>::new();
    if let Some(root) = fragment.nodes().get(&fragment.root())
        && matches!(root.kind, NodeKind::TableWriter { .. })
    {
        for cut in &cuts.outbound {
            if cut.writer_result().is_some() {
                *consumed_writers.entry(root.id).or_default() += 1;
            }
        }
    }
    for finish_node in fragment
        .nodes()
        .values()
        .filter(|node| matches!(node.kind, NodeKind::TableFinish(_)))
    {
        if finish_node.id != fragment.root() {
            errors.push(ValidationError::new(
                path,
                "table finish must be the root of its fragment",
            ));
        }
        let NodeKind::TableFinish(finish) = &finish_node.kind else {
            unreachable!();
        };
        let finish_values = finish
            .input_schema
            .fields
            .iter()
            .map(|field| field.value)
            .collect::<Box<[_]>>();
        let mut pending = finish_node
            .inputs
            .get(finish.inputs.writer_results as usize)
            .into_iter()
            .map(|input| (*input, finish_values.clone()))
            .collect::<Vec<_>>();
        let mut visited = BTreeSet::new();
        let mut ordinals = Vec::new();
        while let Some((node_id, expected_values)) = pending.pop() {
            if !visited.insert(node_id) {
                errors.push(ValidationError::new(
                    path,
                    "writer relation reaches table finish through more than one local path",
                ));
                continue;
            }
            let Some(node) = fragment.nodes().get(&node_id) else {
                continue;
            };
            match &node.kind {
                NodeKind::TableWriter { target } => {
                    *consumed_writers.entry(node.id).or_default() += 1;
                    ordinals.push(target.write_target_ordinal);
                    if !writer_schema_matches_finish_values(
                        &target.output_schema,
                        &finish.input_schema,
                        &expected_values,
                    ) {
                        errors.push(ValidationError::new(
                            path,
                            "local table writer fields do not map exactly to its table finish input roles",
                        ));
                    }
                }
                NodeKind::ExchangeSource { edge, .. } => {
                    let proof = inbound_by_edge
                        .get(edge)
                        .and_then(|cut| cut.writer_result());
                    let Some(proof) = proof else {
                        errors.push(ValidationError::new(
                            path,
                            "table finish stream lacks an upstream writer result proof",
                        ));
                        continue;
                    };
                    *consumed_inbound.entry(*edge).or_default() += 1;
                    ordinals.push(proof.write_target_ordinal);
                    if !writer_result_proof_matches_finish(
                        proof,
                        &finish.input_schema,
                        &expected_values,
                    ) {
                        errors.push(ValidationError::new(
                            path,
                            "upstream writer result fields do not map exactly to its table finish input roles",
                        ));
                    }
                }
                NodeKind::SetOp {
                    kind: crate::SetOperationKind::UnionAll,
                    input_mappings,
                } => {
                    if node.output.columns.as_ref() != expected_values.as_ref()
                        || input_mappings.len() != node.inputs.len()
                        || input_mappings
                            .iter()
                            .any(|mapping| mapping.len() != expected_values.len())
                    {
                        errors.push(ValidationError::new(
                            path,
                            "writer UnionAll does not preserve the exact finish field occurrences",
                        ));
                        continue;
                    }
                    pending.extend(
                        node.inputs
                            .iter()
                            .copied()
                            .zip(input_mappings.iter().cloned()),
                    );
                }
                _ => errors.push(ValidationError::new(
                    path,
                    "table finish input contains a non-preserving writer relation node",
                )),
            }
        }
        ordinals.sort_unstable();
        if ordinals.as_slice() != finish.expected_target_ordinals.as_ref() {
            errors.push(ValidationError::new(
                path,
                "table finish expected targets differ from its fragment cut writer proofs",
            ));
        }
    }
    for cut in &cuts.inbound {
        if cut.writer_result().is_some() && consumed_inbound.get(&cut.edge).copied() != Some(1) {
            errors.push(ValidationError::new(
                path,
                "inbound writer result proof must feed exactly one local table finish",
            ));
        }
    }
    for writer in fragment
        .nodes()
        .values()
        .filter(|node| matches!(node.kind, NodeKind::TableWriter { .. }))
    {
        if consumed_writers.get(&writer.id).copied() != Some(1) {
            errors.push(ValidationError::new(
                path,
                "table writer must feed exactly one local finish or writer result stream",
            ));
        }
    }
}

pub(crate) fn validate_fragment_runtime_filter_cuts(
    fragment: &Fragment,
    cuts: &FragmentCuts,
    path: &str,
    errors: &mut ValidationContext,
) {
    let mut parent_proof_work = SemanticTraceWorkBudget::new(errors.limits());
    let inbound = cuts
        .inbound
        .iter()
        .map(|cut| (cut.edge, cut))
        .collect::<BTreeMap<_, _>>();
    let outbound = cuts
        .outbound
        .iter()
        .map(|cut| (cut.edge, cut))
        .collect::<BTreeMap<_, _>>();
    let mut lineage_indexes = RuntimeFilterLineageIndexes::default();
    let inbound_edges = cuts
        .inbound
        .iter()
        .map(|cut| cut.edge)
        .collect::<BTreeSet<_>>();
    let expected = fragment
        .runtime_filters()
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    if expected.len() != fragment.runtime_filters().len() {
        errors.push(ValidationError::new(
            path,
            "fragment has duplicate runtime-filter identities",
        ));
    }
    let supplied = cuts
        .runtime_filters
        .iter()
        .map(|filter| filter.id)
        .collect::<BTreeSet<_>>();
    if supplied.len() != cuts.runtime_filters.len() || supplied != expected {
        errors.push(ValidationError::new(
            path,
            "runtime filters in fragment cuts differ from fragment attachments",
        ));
    }
    for filter in &cuts.runtime_filters {
        if !validate_runtime_filter_shape(filter, path, errors) {
            continue;
        }
        let witnesses = runtime_filter_witness_index(&filter.equality_witnesses);
        for witness in &filter.equality_witnesses {
            if witness.fragment == fragment.id() {
                validate_runtime_filter_equality_witness(
                    fragment,
                    witness,
                    &filter.domain,
                    path,
                    errors,
                );
            }
        }
        let mut local_endpoint_count = 0_usize;
        for producer in &filter.producers {
            if producer.endpoint.fragment == fragment.id() {
                local_endpoint_count += 1;
                validate_runtime_filter_endpoint_in_fragment(
                    fragment,
                    &producer.endpoint,
                    &filter.domain,
                    path,
                    errors,
                );
                validate_apply_point_in_fragment(
                    fragment,
                    &mut lineage_indexes,
                    &producer.endpoint,
                    producer.apply_point,
                    path,
                    errors,
                );
                validate_runtime_filter_producer_target(
                    fragment,
                    &witnesses,
                    producer,
                    &filter.domain,
                    filter.reduction,
                    path,
                    errors,
                    &mut parent_proof_work,
                );
                validate_runtime_filter_join_coverage(fragment, filter, producer, path, errors);
                validate_runtime_filter_producer_progress(
                    fragment,
                    producer,
                    &inbound_edges,
                    &mut lineage_indexes,
                    path,
                    errors,
                );
            }
        }
        for consumer in &filter.consumers {
            validate_local_runtime_filter_consumer_lineage(
                fragment,
                &inbound,
                &outbound,
                &witnesses,
                &filter.producers,
                consumer,
                path,
                &mut lineage_indexes,
                errors,
            );
            if consumer.endpoint.fragment == fragment.id() {
                local_endpoint_count += 1;
                validate_runtime_filter_endpoint_in_fragment(
                    fragment,
                    &consumer.endpoint,
                    &filter.domain,
                    path,
                    errors,
                );
                validate_apply_point_in_fragment(
                    fragment,
                    &mut lineage_indexes,
                    &consumer.endpoint,
                    consumer.apply_point,
                    path,
                    errors,
                );
                validate_runtime_filter_consumer_semantics(
                    fragment,
                    &witnesses,
                    &filter.producers,
                    &filter.domain,
                    consumer,
                    &mut lineage_indexes,
                    path,
                    errors,
                );
            }
        }
        if local_endpoint_count == 0 {
            errors.push(ValidationError::new(
                path,
                "attached runtime filter has no endpoint in this fragment",
            ));
        }
    }
}

/// Refuse a binding table that is not exactly this fragment's slice of the
/// plan-global runtime-filter numbering.
///
/// The identities are plan-global and cannot be recomputed here. The
/// numbering, however, mints one fragment's bindings as consecutive
/// identities in the order of its attachments, producers before consumers,
/// each role in endpoint order. So every local endpoint must have exactly one
/// binding, no binding may name anything else, and the table must list them
/// in that order under consecutive nonzero identities: any other table names
/// an endpoint by an identity the plan never gave it.
pub(crate) fn validate_fragment_runtime_filter_binding_cuts(
    fragment: &Fragment,
    cuts: &FragmentCuts,
    path: &str,
    errors: &mut ValidationContext,
) {
    let filters = cuts
        .runtime_filters
        .iter()
        .map(|filter| (filter.id, filter))
        .collect::<BTreeMap<_, _>>();
    let mut attached = BTreeSet::new();
    let mut local = Vec::new();
    for id in fragment.runtime_filters() {
        // Duplicate or unsupplied attachments are refused by the
        // runtime-filter cut law; each attached filter is expected once here.
        if !attached.insert(*id) {
            continue;
        }
        let Some(filter) = filters.get(id) else {
            continue;
        };
        for (index, producer) in filter.producers.iter().enumerate() {
            if producer.endpoint.fragment == fragment.id() {
                local.push((filter.id, RuntimeFilterBindingRole::Producer(index)));
            }
        }
        for (index, consumer) in filter.consumers.iter().enumerate() {
            if consumer.endpoint.fragment == fragment.id() {
                local.push((filter.id, RuntimeFilterBindingRole::Consumer(index)));
            }
        }
    }
    let previous_errors = errors.len();
    let mut identities = BTreeSet::new();
    let mut bound = BTreeSet::new();
    for (ordinal, binding) in cuts.runtime_filter_bindings.iter().enumerate() {
        let entry = format!("{path}[{ordinal}]");
        if binding.binding_id == 0 {
            errors.push(ValidationError::new(
                &entry,
                "runtime-filter binding identity 0 is never numbered",
            ));
        }
        if !identities.insert(binding.binding_id) {
            errors.push(ValidationError::new(
                &entry,
                format!(
                    "runtime-filter binding identity {} is not unique",
                    binding.binding_id
                ),
            ));
        }
        let endpoint = filters
            .get(&binding.filter)
            .and_then(|filter| match binding.role {
                RuntimeFilterBindingRole::Producer(index) => filter
                    .producers
                    .get(index)
                    .map(|producer| &producer.endpoint),
                RuntimeFilterBindingRole::Consumer(index) => filter
                    .consumers
                    .get(index)
                    .map(|consumer| &consumer.endpoint),
            });
        match endpoint {
            None => errors.push(ValidationError::new(
                &entry,
                format!(
                    "runtime-filter binding names {} of runtime filter {}, which the fragment cuts do not define",
                    binding_role_text(binding.role),
                    binding.filter.get()
                ),
            )),
            Some(endpoint) if endpoint.fragment != fragment.id() => {
                errors.push(ValidationError::new(
                    &entry,
                    format!(
                        "runtime-filter binding names remote {} of runtime filter {} in fragment {}",
                        binding_role_text(binding.role),
                        binding.filter.get(),
                        endpoint.fragment.get()
                    ),
                ))
            }
            Some(_) => {
                if !bound.insert((binding.filter, binding.role)) {
                    errors.push(ValidationError::new(
                        &entry,
                        format!(
                            "local {} of runtime filter {} has more than one binding",
                            binding_role_text(binding.role),
                            binding.filter.get()
                        ),
                    ));
                }
            }
        }
        if errors.is_saturated() {
            errors.mark_truncated();
            return;
        }
    }
    for (filter, role) in &local {
        if !bound.contains(&(*filter, *role)) {
            errors.push(ValidationError::new(
                path,
                format!(
                    "local {} of runtime filter {} has no binding",
                    binding_role_text(*role),
                    filter.get()
                ),
            ));
            if errors.is_saturated() {
                errors.mark_truncated();
                return;
            }
        }
    }
    if errors.len() != previous_errors {
        return;
    }
    // The table is now a bijection onto the local endpoints. Only the
    // numbering order and consecutive identities remain to be proven.
    let first = cuts
        .runtime_filter_bindings
        .first()
        .map(|binding| binding.binding_id);
    for (ordinal, (binding, (filter, role))) in
        cuts.runtime_filter_bindings.iter().zip(&local).enumerate()
    {
        let expected_id = first.and_then(|first| {
            u32::try_from(ordinal)
                .ok()
                .and_then(|offset| first.checked_add(offset))
        });
        if binding.filter != *filter || binding.role != *role {
            errors.push(ValidationError::new(
                format!("{path}[{ordinal}]"),
                format!(
                    "runtime-filter binding is out of numbering order: expected {} of runtime filter {}",
                    binding_role_text(*role),
                    filter.get()
                ),
            ));
            return;
        }
        if expected_id != Some(binding.binding_id) {
            errors.push(ValidationError::new(
                format!("{path}[{ordinal}]"),
                "runtime-filter binding identities of one fragment are not consecutive",
            ));
            return;
        }
    }
}

fn binding_role_text(role: RuntimeFilterBindingRole) -> String {
    match role {
        RuntimeFilterBindingRole::Producer(index) => format!("producer {index}"),
        RuntimeFilterBindingRole::Consumer(index) => format!("consumer {index}"),
    }
}

pub(crate) fn validate_runtime_filter_join_coverage(
    fragment: &Fragment,
    filter: &crate::RuntimeFilter,
    producer: &crate::RuntimeFilterProducer,
    path: &str,
    errors: &mut ValidationContext,
) {
    if !matches!(
        producer.target,
        crate::RuntimeFilterProducerTarget::JoinBuildKey { .. }
    ) || producer.contribution_kinds.as_ref()
        != [
            crate::RuntimeFilterContributionKind::ValueDomainDelta,
            crate::RuntimeFilterContributionKind::ProducerClosed,
        ]
    {
        return;
    }
    let Some(node) = fragment.nodes().get(&producer.endpoint.node) else {
        return;
    };
    let NodeKind::HashJoin { distribution, .. } = node.kind else {
        return;
    };
    let matches_shape = |coverage: &crate::RuntimeFilterCoverage| match distribution {
        crate::JoinDistribution::BroadcastBuild => matches!(
            coverage.nodes.as_ref(),
            [crate::RuntimeFilterCoverageNode::Witness(witness), crate::RuntimeFilterCoverageNode::AnyOf { children }]
                if *witness == producer.witness && children.as_ref() == [0] && coverage.root == 1
        ),
        crate::JoinDistribution::Partitioned => matches!(
            coverage.nodes.as_ref(),
            [crate::RuntimeFilterCoverageNode::Witness(witness), crate::RuntimeFilterCoverageNode::AllOf { children }]
                if *witness == producer.witness && children.as_ref() == [0] && coverage.root == 1
        ),
        crate::JoinDistribution::Colocated | crate::JoinDistribution::Singleton => matches!(
            coverage.nodes.as_ref(),
            [crate::RuntimeFilterCoverageNode::Witness(witness)]
                if *witness == producer.witness && coverage.root == 0
        ),
    };
    if !matches_shape(&filter.availability_coverage) || !matches_shape(&filter.terminal_coverage) {
        errors.push(ValidationError::new(
            path,
            "runtime filter coverage shape differs from its exact join execution mode",
        ));
    }
}
