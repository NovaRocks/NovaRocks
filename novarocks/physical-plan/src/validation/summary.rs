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

//! Exact, kind-specific summary declarations, provenance and strict inputs.

use super::*;
use crate::{
    DeliveryRequirement, ExchangePayloadContract, FragmentOutputPort, FragmentSink, PerKind,
    SummaryAuthor, SummaryColumns, SummaryField, SummaryKind, SummaryOutput, SummaryWireType,
    ValueDef, ValueOrigin,
};
use arrow_schema::DataType;

pub(super) fn field_type_matches(value: &crate::ValueType, field: SummaryField) -> bool {
    let data_type = match field.wire_type() {
        SummaryWireType::Int64 => DataType::Int64,
        SummaryWireType::FixedSizeBinary(width) => DataType::FixedSizeBinary(i32::from(width)),
    };
    super::value_binding_is_primitive(value, &data_type, false)
}
pub(super) fn distinct(columns: &SummaryColumns) -> bool {
    columns
        .values()
        .iter()
        .enumerate()
        .all(|(index, id)| !columns.values()[..index].contains(id))
}

pub(crate) fn validate_summary_value(
    fragment: &Fragment,
    value: &ValueDef,
    field: SummaryField,
    errors: &mut ValidationContext,
) {
    let path = format!(
        "fragments[{}].values[{}].origin",
        fragment.id().get(),
        value.id.get()
    );
    let declared = fragment.summaries().iter().any(|summary| {
        summary.kind() == field.kind()
            && summary
                .columns
                .fields()
                .any(|(role, id)| role == field && id == value.id)
    });
    if !declared || !field_type_matches(&value.ty, field) {
        errors.push(ValidationError::new(
            &path,
            "summary origin requires its exact declared non-null field type",
        ));
    }
}

pub(crate) fn validate_fragment_summary_outputs(
    fragment: &Fragment,
    errors: &mut ValidationContext,
) {
    let mut seen = PerKind::<crate::EdgeId>::empty();
    for summary in fragment.summaries() {
        let path = format!(
            "fragments[{}].summaries[{}]",
            fragment.id().get(),
            summary.kind()
        );
        if seen.get(summary.kind()).is_some() {
            errors.push(ValidationError::new(
                &path,
                "fragment declares a summary kind more than once",
            ));
        }
        if seen.iter().any(|(_, edge)| *edge == summary.edge) {
            errors.push(ValidationError::new(
                &path,
                "summary outputs must have distinct edges",
            ));
        }
        seen.insert(summary.kind(), summary.edge);
        validate_summary_output(fragment, summary, errors);
        if errors.is_saturated() {
            break;
        }
    }
}

pub(crate) fn validate_summary_output(
    fragment: &Fragment,
    summary: &SummaryOutput,
    errors: &mut ValidationContext,
) {
    let path = format!(
        "fragments[{}].summaries[{}]",
        fragment.id().get(),
        summary.kind()
    );
    let author_matches = match summary.author {
        SummaryAuthor::RouterSink => {
            summary.kind() == SummaryKind::WriteCounts
                && matches!(fragment.sink(), FragmentSink::Router { .. })
        }
        SummaryAuthor::Node(id) => {
            summary.kind() != SummaryKind::WriteCounts
                && fragment.nodes().get(&id).is_some_and(|node| {
                    node.kind.summary_authorship() == Some(summary.kind())
                        && summary
                            .columns
                            .values()
                            .iter()
                            .all(|id| node.output.columns.contains(id))
                })
        }
    };
    if !author_matches {
        errors.push(ValidationError::new(
            &path,
            "summary kind has no exact legal author in its fragment",
        ));
    }
    if !distinct(&summary.columns) {
        errors.push(ValidationError::new(
            &path,
            "summary field identities must be distinct",
        ));
    }
    for (field, id) in summary.columns.fields() {
        if !fragment.values().get(&id).is_some_and(|value| {
            value.origin == (ValueOrigin::Summary { field }) && field_type_matches(&value.ty, field)
        }) {
            errors.push(ValidationError::new(
                &path,
                "summary output requires its own exact ordered non-null field origins",
            ));
        }
    }
    if summary.author == SummaryAuthor::RouterSink
        && fragment.nodes().get(&fragment.root()).is_some_and(|node| {
            node.output
                .columns
                .iter()
                .any(|id| summary.columns.values().contains(id))
        })
    {
        errors.push(ValidationError::new(
            &path,
            "summary fields cannot be relational root outputs",
        ));
    }
}

pub(crate) fn validate_summary_edge(
    fragment: &Fragment,
    summary: &SummaryOutput,
    edge: &crate::Edge,
    errors: &mut ValidationContext,
) {
    let path = format!(
        "fragments[{}].summaries[{}]",
        fragment.id().get(),
        summary.kind()
    );
    if edge.id != summary.edge
        || edge.source.fragment != fragment.id()
        || edge.source.port != FragmentOutputPort::Summary(summary.kind())
        || edge.role != crate::OutputRole::Summary(summary.kind())
        || edge.delivery != DeliveryRequirement::MustDrain
        || edge.kind != crate::EdgeKind::Stream
        || edge.partitioning.source != crate::Distribution::Singleton
        || edge.partitioning.destination != crate::Distribution::Singleton
        || edge.partitioning.source_multiplicity != crate::RowMultiplicity::SingleCopy
        || edge.partitioning.destination_multiplicity != crate::RowMultiplicity::SingleCopy
        || edge.source.projection.as_ref() != summary.columns.values()
    {
        errors.push(ValidationError::new(&path,
            "summary edge must project its exact kind and independent fields with MustDrain delivery"));
    }
}

pub(crate) fn validate_summary_cut(
    fragment: &Fragment,
    source_port: FragmentOutputPort,
    payload: &ExchangePayloadContract,
    delivery: DeliveryRequirement,
    edge_kind: crate::EdgeKind,
    partitioning: &crate::EdgePartitioning,
    imports: &[crate::CutImport],
    outbound: bool,
    errors: &mut ValidationContext,
) {
    let path = format!("fragments[{}].cuts.summary", fragment.id().get());
    if !matches!(payload, ExchangePayloadContract::Relational)
        && delivery != DeliveryRequirement::MustDrain
    {
        errors.push(ValidationError::new(
            &path,
            "write output roles require MustDrain delivery",
        ));
    }
    let ExchangePayloadContract::Summary(proof) = payload else {
        if matches!(source_port, FragmentOutputPort::Summary(_)) {
            errors.push(ValidationError::new(
                &path,
                "summary source port requires its exact summary payload",
            ));
        }
        return;
    };
    if source_port != FragmentOutputPort::Summary(proof.kind())
        || delivery != DeliveryRequirement::MustDrain
        || proof.destination.kind() != proof.kind()
    {
        errors.push(ValidationError::new(
            &path,
            "summary cut must preserve one exact kind, source port and MustDrain delivery",
        ));
    }
    if edge_kind != crate::EdgeKind::Stream
        || partitioning.source != crate::Distribution::Singleton
        || partitioning.destination != crate::Distribution::Singleton
        || partitioning.source_multiplicity != crate::RowMultiplicity::SingleCopy
        || partitioning.destination_multiplicity != crate::RowMultiplicity::SingleCopy
    {
        errors.push(ValidationError::new(
            &path,
            "summary requires an unpartitioned single-copy gather stream",
        ));
    }
    if !distinct(&proof.source) || !distinct(&proof.destination) {
        errors.push(ValidationError::new(
            &path,
            "summary cut field identities must be distinct at both endpoints",
        ));
    }
    if imports.len() != proof.source.values().len()
        || imports.len() != proof.destination.values().len()
        || imports
            .iter()
            .zip(
                proof
                    .source
                    .fields()
                    .zip(proof.destination.values().iter().copied()),
            )
            .any(|(import, ((field, source), destination))| {
                import.source.value != source
                    || import.destination != destination
                    || !field_type_matches(&import.source.ty, field)
            })
    {
        errors.push(ValidationError::new(
            &path,
            "summary cut mapping differs from its ordered non-null field contract",
        ));
    }
    let columns = if outbound {
        proof.source
    } else {
        proof.destination
    };
    for (field, id) in columns.fields() {
        if fragment
            .values()
            .get(&id)
            .is_none_or(|value| !field_type_matches(&value.ty, field))
        {
            errors.push(ValidationError::new(
                &path,
                "summary local field is absent or has an inexact type",
            ));
        }
    }
}

pub(crate) fn validate_fragment_summary_inputs(
    fragment: &Fragment,
    cuts: &crate::FragmentCuts,
    errors: &mut ValidationContext,
) {
    let path = format!("fragments[{}].cuts.summary", fragment.id().get());
    let mut used = std::collections::BTreeSet::new();
    for node in fragment.nodes().values() {
        let crate::NodeKind::TableFinish(finish) = &node.kind else {
            continue;
        };
        for (kind, port) in finish.inputs.summaries.iter() {
            let exact = node
                .inputs
                .get(*port as usize)
                .and_then(|id| fragment.nodes().get(id))
                .and_then(|source| {
                    let crate::NodeKind::ExchangeSource { edge, .. } = source.kind else {
                        return None;
                    };
                    let cut = cuts
                        .inbound
                        .iter()
                        .find(|cut| cut.edge == edge && cut.destination_node == source.id)?;
                    let ExchangePayloadContract::Summary(proof) = &cut.payload else {
                        return None;
                    };
                    (proof.kind() == kind && proof.destination.kind() == kind)
                        .then_some((edge, proof.destination))
                });
            match exact {
                Some((edge, columns))
                    if finish.summaries.get(kind) == Some(&columns) && used.insert(edge) => {}
                _ => errors.push(ValidationError::new(
                    &path,
                    format!("finish requires one exact independently bound {kind} source"),
                )),
            }
        }
    }
    for cut in &cuts.inbound {
        if matches!(cut.payload, ExchangePayloadContract::Summary(_)) && !used.contains(&cut.edge) {
            errors.push(ValidationError::new(
                &path,
                "summary cut is not consumed exactly once by a finish",
            ));
        }
    }
}
