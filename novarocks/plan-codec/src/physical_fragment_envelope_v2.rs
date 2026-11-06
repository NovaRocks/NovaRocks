// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Fragment envelope vocabulary only; no Fragment/Package certification.
//! The caller invoices the entire original owner. Known floors here cover only
//! borrowed header/sink backing; other components retain their original gates.

use crate::physical_change_event_v2::{decode_mutation_effect, encode_mutation_effect};
use crate::physical_node_v2::*;
use novarocks_connector_contract::{
    ConnectorRowMutationEffect, ConnectorWriteFieldToken, ConnectorWriteRouteId, WriteTargetOrdinal,
};
use novarocks_physical_plan as p;
use novarocks_proto_models::{physical_control_v2::Empty, physical_package_v2 as wire};
#[cfg(test)]
use novarocks_type_contract::CompilePhase;
use novarocks_type_contract::{CompileCheckpoints, CompileControlError, PureCompileControl};
use std::mem::size_of;

pub(crate) use crate::physical_node_v2::{
    NodeProjectionFacts as FragmentEnvelopeProjectionFacts,
    NodeProjectionLimits as FragmentEnvelopeProjectionLimits,
};
type Error = NodeCodecError;

/// These owned fields must be combined with all other original component owners.
/// A sink vocabulary projection never proves its routes, DOP or references legal.
#[derive(Debug, PartialEq)]
pub(crate) struct DecodedFragmentEnvelope {
    pub(crate) id: p::FragmentId,
    pub(crate) root: p::NodeId,
    pub(crate) sink: p::FragmentSink,
    pub(crate) summaries: Box<[p::SummaryOutput]>,
    pub(crate) dop_domain: p::PipelineDopDomain,
    pub(crate) stage_placement: p::StagePlacementPolicy,
    pub(crate) runtime_filters: Box<[p::RuntimeFilterId]>,
}
#[derive(Debug, PartialEq)]
pub(crate) struct EncodedFragmentEnvelope {
    pub(crate) id: u32,
    pub(crate) root_node_id: u32,
    pub(crate) sink: wire::FragmentSink,
    pub(crate) summaries: Vec<wire::SummaryOutput>,
    pub(crate) dop_domain: wire::PipelineDopDomain,
    pub(crate) stage_placement: wire::StagePlacementPolicy,
    pub(crate) runtime_filter_ids: Vec<u32>,
}
fn required<T>(
    value: Option<T>,
    text: &'static str,
    w: &mut CompileCheckpoints<'_>,
) -> Result<T, Error> {
    let result = value.ok_or_else(|| invalid(text));
    w.step()?;
    result
}
fn token32(bytes: &[u8], w: &mut CompileCheckpoints<'_>) -> Result<[u8; 32], Error> {
    let correct_length = bytes.len() == 32;
    w.step()?;
    if !correct_length {
        return Err(invalid(
            "fragment route/field token must be exactly 32 bytes",
        ));
    }
    let mut output = [0; 32];
    for (target, source) in output.iter_mut().zip(bytes) {
        *target = *source;
        w.step()?;
    }
    Ok(output)
}
fn ordinal(raw: u32, w: &mut CompileCheckpoints<'_>) -> Result<WriteTargetOrdinal, Error> {
    let result = WriteTargetOrdinal::try_new(raw).map_err(Error::Identity);
    w.step()?;
    result
}
fn list<T, U>(
    input: &[T],
    w: &mut CompileCheckpoints<'_>,
    mut convert: impl FnMut(&T, &mut CompileCheckpoints<'_>) -> Result<U, Error>,
) -> Result<Vec<U>, Error> {
    let mut output = reserve(input.len(), w)?;
    for value in input {
        output.push(convert(value, w)?);
        w.step()?;
    }
    Ok(output)
}
fn token_vec(bytes: [u8; 32], w: &mut CompileCheckpoints<'_>) -> Result<Vec<u8>, Error> {
    list(&bytes, w, |v, _| Ok(*v))
}
fn physical_placement_sources(input: &p::StagePlacementPolicy) -> usize {
    match input {
        p::StagePlacementPolicy::FollowSources { sources } => sources.len(),
        _ => 0,
    }
}
fn wire_placement_sources(input: Option<&wire::StagePlacementPolicy>) -> usize {
    match input.and_then(|value| value.kind.as_ref()) {
        Some(wire::stage_placement_policy::Kind::FollowSources(value)) => value.fragment_ids.len(),
        _ => 0,
    }
}
fn wire_placement_capacity(input: Option<&wire::StagePlacementPolicy>) -> usize {
    match input.and_then(|value| value.kind.as_ref()) {
        Some(wire::stage_placement_policy::Kind::FollowSources(value)) => {
            value.fragment_ids.capacity()
        }
        _ => 0,
    }
}
fn encode_placement(
    value: &p::StagePlacementPolicy,
    w: &mut CompileCheckpoints<'_>,
) -> Result<wire::StagePlacementPolicy, Error> {
    use wire::stage_placement_policy::Kind;
    let kind = match value {
        p::StagePlacementPolicy::Singleton => Kind::Singleton(Empty {}),
        p::StagePlacementPolicy::ScanWork => Kind::ScanWork(Empty {}),
        p::StagePlacementPolicy::Spread { max_instances } => {
            Kind::Spread(wire::SpreadStagePlacement {
                max_instances: max_instances.map(|n| n.get()),
            })
        }
        p::StagePlacementPolicy::FollowSources { sources } => {
            Kind::FollowSources(wire::FollowSourcesStagePlacement {
                fragment_ids: list(sources, w, |id, _| Ok(id.get()))?,
            })
        }
    };
    w.step()?;
    Ok(wire::StagePlacementPolicy { kind: Some(kind) })
}
fn decode_placement(
    value: &wire::StagePlacementPolicy,
    w: &mut CompileCheckpoints<'_>,
) -> Result<p::StagePlacementPolicy, Error> {
    use wire::stage_placement_policy::Kind;
    Ok(
        match required(value.kind.as_ref(), "stage placement kind is absent", w)? {
            Kind::Singleton(_) => p::StagePlacementPolicy::Singleton,
            Kind::ScanWork(_) => p::StagePlacementPolicy::ScanWork,
            Kind::Spread(value) => p::StagePlacementPolicy::Spread {
                max_instances: value
                    .max_instances
                    .map(|n| {
                        std::num::NonZeroU32::new(n)
                            .ok_or_else(|| invalid("stage instance cap must be positive"))
                    })
                    .transpose()?,
            },
            Kind::FollowSources(value) => p::StagePlacementPolicy::FollowSources {
                sources: boxed(
                    list(&value.fragment_ids, w, |id, _| Ok(p::FragmentId::new(*id)))?,
                    w,
                )?,
            },
        },
    )
}
fn physical_outer(input: &p::Fragment) -> Result<usize, Error> {
    add(
        add(
            add(input.runtime_filters().len(), input.summaries().len())?,
            physical_placement_sources(input.stage_placement()),
        )?,
        match input.sink() {
            p::FragmentSink::Multicast { edges } => edges.len(),
            p::FragmentSink::Router { routes, .. } => routes.len(),
            _ => 0,
        },
    )
}
fn raw_kind<'a>(
    input: &'a wire::Fragment,
    w: &mut CompileCheckpoints<'_>,
) -> Result<&'a wire::fragment_sink::Kind, Error> {
    required(input.root_node_id, "fragment root ID is absent", w)?;
    let placement = required(
        input.stage_placement.as_ref(),
        "fragment stage placement is absent",
        w,
    )?;
    required(placement.kind.as_ref(), "stage placement kind is absent", w)?;
    required(
        input.dop_domain.as_ref(),
        "fragment DOP domain is absent",
        w,
    )?;
    let sink = required(input.sink.as_ref(), "fragment sink is absent", w)?;
    required(sink.kind.as_ref(), "fragment sink kind is absent", w)
}
fn wire_outer(input: &wire::Fragment, kind: &wire::fragment_sink::Kind) -> Result<usize, Error> {
    add(
        add(
            add(input.runtime_filter_ids.len(), input.summaries.len())?,
            wire_placement_sources(input.stage_placement.as_ref()),
        )?,
        match kind {
            wire::fragment_sink::Kind::Multicast(v) => v.edge_ids.len(),
            wire::fragment_sink::Kind::Router(v) => v.routes.len(),
            _ => 0,
        },
    )
}
type EnvelopeAdmit<'a> =
    dyn FnMut(&FragmentEnvelopeProjectionFacts) -> Result<(), CompileControlError> + 'a;
struct EnvelopeAdmission<'a, 'callback> {
    parent: Option<&'a mut EnvelopeAdmit<'callback>>,
}
impl EnvelopeAdmission<'_, '_> {
    fn observed(&self) -> bool {
        self.parent.is_some()
    }
    fn numeric<T>(&self, result: Result<T, Error>) -> Result<T, Error> {
        match result {
            Err(Error::InvalidShape(_)) if self.observed() => {
                Err(CompileControlError::ResourceExhausted.into())
            }
            result => result,
        }
    }
    fn sum(&self, left: usize, right: usize) -> Result<usize, Error> {
        self.numeric(add(left, right))
    }
    fn request<T>(&self, model: &mut Model, n: usize, copies: usize) -> Result<(), Error> {
        self.numeric(model.request::<T>(n, copies))
    }
    fn gate(
        &mut self,
        model: &Model,
        source: usize,
        limits: FragmentEnvelopeProjectionLimits,
    ) -> Result<(), Error> {
        if self.observed() {
            let facts = self.numeric(model.numerical_facts(source, 0, limits))?;
            (self.parent.as_mut().expect("observed parent"))(&facts)?;
        }
        Ok(())
    }
    fn facts(
        &mut self,
        model: &Model,
        source: usize,
        limits: FragmentEnvelopeProjectionLimits,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<FragmentEnvelopeProjectionFacts, Error> {
        self.gate(model, source, limits)?;
        self.numeric(model.facts(source, 0, limits, work))
    }
}
// These are the original root request authors. Observed preparation consumes
// them before its first completed callback; Plain keeps its original order.
fn encode_sink_requests(
    sink: &p::FragmentSink,
    model: &mut Model,
    admission: &EnvelopeAdmission<'_, '_>,
) -> Result<(), Error> {
    match sink {
        p::FragmentSink::RootResult(_) => {
            admission.request::<u8>(model, root_projection_request_bytes()?, 1)?;
            model.delegated_work = admission.sum(model.delegated_work, root_projection_work()?)?;
        }
        p::FragmentSink::Multicast { edges } => admission.request::<u32>(model, edges.len(), 1)?,
        p::FragmentSink::Router { routes, .. } => {
            model.refs = admission.sum(model.refs, 1)?;
            admission.request::<wire::ChangeStreamRoute>(model, routes.len(), 1)?;
        }
        _ => {}
    }
    Ok(())
}
fn decode_sink_requests(
    kind: Option<&wire::fragment_sink::Kind>,
    model: &mut Model,
    admission: &EnvelopeAdmission<'_, '_>,
) -> Result<(), Error> {
    match kind {
        Some(wire::fragment_sink::Kind::RootResult(_)) => {
            admission.request::<u8>(model, root_projection_request_bytes()?, 2)?;
            model.delegated_work = admission.sum(model.delegated_work, root_projection_work()?)?;
        }
        Some(wire::fragment_sink::Kind::Multicast(v)) => {
            admission.request::<p::EdgeId>(model, v.edge_ids.len(), 2)?;
        }
        Some(wire::fragment_sink::Kind::Router(v)) => {
            model.refs = admission.sum(model.refs, 1)?;
            admission.request::<p::ChangeStreamRoute>(model, v.routes.len(), 2)?;
        }
        _ => {}
    }
    Ok(())
}
fn physical_route_keys(route: &p::ChangeStreamRoute) -> Result<usize, Error> {
    if matches!(&route.requirements.colocation, p::WriteColocation::ByKey(keys) if keys.is_empty())
        || matches!(&route.requirements.layout, p::WriteLayoutPreference::ClusterBy(keys) if keys.is_empty())
    {
        return Err(invalid("write requirement key must be nonempty"));
    }
    add(
        route.requirements.colocation_keys().len(),
        route.requirements.layout_keys().len(),
    )
}
fn wire_requirement_lists(
    value: Option<&wire::WriteRouteRequirements>,
) -> Result<(&[u32], &[u32]), Error> {
    let value = value.ok_or_else(|| invalid("write route requirements are absent"))?;
    let colocation = match value
        .colocation
        .as_ref()
        .ok_or_else(|| invalid("write colocation is absent"))?
    {
        wire::write_route_requirements::Colocation::NoColocation(_)
        | wire::write_route_requirements::Colocation::Singleton(_) => &[][..],
        wire::write_route_requirements::Colocation::ByKey(keys) if !keys.value_ids.is_empty() => {
            keys.value_ids.as_slice()
        }
        _ => return Err(invalid("write ByKey colocation is empty")),
    };
    let layout = match value
        .layout
        .as_ref()
        .ok_or_else(|| invalid("write layout is absent"))?
    {
        wire::write_route_requirements::Layout::NoLayout(_) => &[][..],
        wire::write_route_requirements::Layout::ClusterBy(keys) if !keys.value_ids.is_empty() => {
            keys.value_ids.as_slice()
        }
        _ => return Err(invalid("write ClusterBy layout is empty")),
    };
    Ok((colocation, layout))
}
fn wire_route_keys(route: &wire::ChangeStreamRoute) -> Result<usize, Error> {
    let (colocation, layout) = wire_requirement_lists(route.requirements.as_ref())?;
    add(colocation.len(), layout.len())
}
fn wire_requirement_backing(value: Option<&wire::WriteRouteRequirements>) -> Result<usize, Error> {
    wire_requirement_lists(value)?;
    let value = value.expect("checked requirement presence");
    let colocation = match &value.colocation {
        Some(wire::write_route_requirements::Colocation::ByKey(keys)) => keys.value_ids.capacity(),
        _ => 0,
    };
    let layout = match &value.layout {
        Some(wire::write_route_requirements::Layout::ClusterBy(keys)) => keys.value_ids.capacity(),
        _ => 0,
    };
    add(bytes::<u32>(colocation)?, bytes::<u32>(layout)?)
}
fn encode_requirements(
    value: &p::WriteRouteRequirements,
    w: &mut CompileCheckpoints<'_>,
) -> Result<wire::WriteRouteRequirements, Error> {
    let keys = |ids: &[p::ValueId],
                w: &mut CompileCheckpoints<'_>|
     -> Result<wire::WriteRouteKeyFields, Error> {
        if ids.is_empty() {
            return Err(invalid("write requirement key must be nonempty"));
        }
        Ok(wire::WriteRouteKeyFields {
            value_ids: list(ids, w, |id, _| Ok(id.get()))?,
        })
    };
    Ok(wire::WriteRouteRequirements {
        colocation: Some(match &value.colocation {
            p::WriteColocation::None => {
                wire::write_route_requirements::Colocation::NoColocation(Empty {})
            }
            p::WriteColocation::Singleton => {
                wire::write_route_requirements::Colocation::Singleton(Empty {})
            }
            p::WriteColocation::ByKey(ids) => {
                wire::write_route_requirements::Colocation::ByKey(keys(ids, w)?)
            }
        }),
        layout: Some(match &value.layout {
            p::WriteLayoutPreference::None => {
                wire::write_route_requirements::Layout::NoLayout(Empty {})
            }
            p::WriteLayoutPreference::ClusterBy(ids) => {
                wire::write_route_requirements::Layout::ClusterBy(keys(ids, w)?)
            }
        }),
    })
}
fn decode_requirements(
    value: Option<&wire::WriteRouteRequirements>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<p::WriteRouteRequirements, Error> {
    let (colocation, layout) = wire_requirement_lists(value)?;
    let value = value.expect("checked requirement presence");
    let colocation = match value.colocation.as_ref().expect("checked colocation") {
        wire::write_route_requirements::Colocation::NoColocation(_) => p::WriteColocation::None,
        wire::write_route_requirements::Colocation::Singleton(_) => p::WriteColocation::Singleton,
        wire::write_route_requirements::Colocation::ByKey(_) => p::WriteColocation::ByKey(boxed(
            list(colocation, w, |id, _| Ok(p::ValueId::new(*id)))?,
            w,
        )?),
    };
    let layout = match value.layout.as_ref().expect("checked layout") {
        wire::write_route_requirements::Layout::NoLayout(_) => p::WriteLayoutPreference::None,
        wire::write_route_requirements::Layout::ClusterBy(_) => {
            p::WriteLayoutPreference::ClusterBy(boxed(
                list(layout, w, |id, _| Ok(p::ValueId::new(*id)))?,
                w,
            )?)
        }
    };
    w.step()?;
    Ok(p::WriteRouteRequirements { colocation, layout })
}
fn encode_route_requests(
    route: &p::ChangeStreamRoute,
    model: &mut Model,
    admission: &EnvelopeAdmission<'_, '_>,
) -> Result<(), Error> {
    admission.request::<u8>(model, 32, 1)?;
    admission.request::<i32>(model, route.accepted_effects.len(), 1)?;
    admission.request::<wire::WriteInputMapping>(model, route.input_mapping.len(), 1)?;
    admission.request::<u32>(model, route.requirements.colocation_keys().len(), 1)?;
    admission.request::<u32>(model, route.requirements.layout_keys().len(), 1)?;
    admission.request::<u8>(model, 32, route.input_mapping.len())
}
fn decode_route_requests(
    route: &wire::ChangeStreamRoute,
    model: &mut Model,
    admission: &EnvelopeAdmission<'_, '_>,
) -> Result<(), Error> {
    admission.request::<ConnectorRowMutationEffect>(model, route.accepted_effects.len(), 2)?;
    admission.request::<(ConnectorWriteFieldToken, p::ValueId)>(
        model,
        route.input_mapping.len(),
        2,
    )?;
    let (colocation, layout) = wire_requirement_lists(route.requirements.as_ref())?;
    admission.request::<p::ValueId>(model, colocation.len(), 2)?;
    admission.request::<p::ValueId>(model, layout.len(), 2)
}
fn preflight_encode(
    input: &p::Fragment,
    source: usize,
    l: FragmentEnvelopeProjectionLimits,
    admission: &mut EnvelopeAdmission<'_, '_>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<FragmentEnvelopeProjectionFacts, Error> {
    let outer = admission.numeric(physical_outer(input))?;
    let mut model = Model {
        items: outer,
        ..Model::default()
    };
    if admission.observed() {
        admission.request::<u32>(&mut model, input.runtime_filters().len(), 1)?;
        admission.request::<u32>(
            &mut model,
            physical_placement_sources(input.stage_placement()),
            1,
        )?;
        admission.request::<wire::SummaryOutput>(&mut model, input.summaries().len(), 1)?;
        encode_sink_requests(input.sink(), &mut model, admission)?;
        admission.gate(&model, source, l)?;
    }
    let mut known = add(
        size_of::<p::Fragment>(),
        add(
            bytes::<p::RuntimeFilterId>(input.runtime_filters().len())?,
            bytes::<p::FragmentId>(physical_placement_sources(input.stage_placement()))?,
        )?,
    )?;
    known = add(
        known,
        match input.sink() {
            p::FragmentSink::Multicast { edges } => bytes::<p::EdgeId>(edges.len())?,
            p::FragmentSink::Router { routes, .. } => bytes::<p::ChangeStreamRoute>(routes.len())?,
            _ => 0,
        },
    )?;
    known = add(known, bytes::<p::SummaryOutput>(input.summaries().len())?)?;
    count_prefix(0, outer, source, known, l, w)?;
    if !admission.observed() {
        admission.request::<wire::SummaryOutput>(&mut model, input.summaries().len(), 1)?;
        admission.request::<u32>(&mut model, input.runtime_filters().len(), 1)?;
        admission.request::<u32>(
            &mut model,
            physical_placement_sources(input.stage_placement()),
            1,
        )?;
    }
    admission.facts(&model, source, l, w)?;
    if !admission.observed() {
        encode_sink_requests(input.sink(), &mut model, admission)?;
    }
    for summary in input.summaries() {
        let n = summary.columns.values().len();
        model.items = admission.sum(model.items, n)?;
        let author_refs = usize::from(matches!(summary.author, p::SummaryAuthor::Node(_)));
        model.refs = admission.sum(model.refs, admission.sum(n, 1 + author_refs)?)?;
        admission.request::<u32>(&mut model, n, 1)?;
        admission.facts(&model, source, l, w)?;
        for _ in 0..n {
            w.step()?;
        }
    }
    if let p::FragmentSink::Router { routes, .. } = input.sink() {
        admission.facts(&model, source, l, w)?;
        for route in routes {
            let inner = admission.sum(
                route.accepted_effects.len(),
                admission.sum(route.input_mapping.len(), physical_route_keys(route)?)?,
            )?;
            model.items = admission.sum(model.items, inner)?;
            let references = || {
                admission.sum(
                    model.refs,
                    admission.sum(route.input_mapping.len(), physical_route_keys(route)?)?,
                )
            };
            if admission.observed() {
                model.refs = references()?;
                encode_route_requests(route, &mut model, admission)?;
                admission.gate(&model, source, l)?;
            }
            count_prefix(0, model.items, source, known, l, w)?;
            if !admission.observed() {
                model.refs = admission.sum(
                    model.refs,
                    admission.sum(route.input_mapping.len(), physical_route_keys(route)?)?,
                )?;
            }
            cap(model.refs, l.max_value_references, w)?;
            known = add(
                known,
                add(
                    bytes::<ConnectorRowMutationEffect>(route.accepted_effects.len())?,
                    add(
                        bytes::<(ConnectorWriteFieldToken, p::ValueId)>(route.input_mapping.len())?,
                        bytes::<p::ValueId>(physical_route_keys(route)?)?,
                    )?,
                )?,
            )?;
            if !admission.observed() {
                encode_route_requests(route, &mut model, admission)?;
            }
            admission.facts(&model, source, l, w)?;
            floor(source, known, w)?;
            w.step()?;
        }
    }
    floor(source, known, w)?;
    admission.facts(&model, source, l, w)
}
fn preflight_decode(
    input: &wire::Fragment,
    source: usize,
    l: FragmentEnvelopeProjectionLimits,
    admission: &mut EnvelopeAdmission<'_, '_>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<FragmentEnvelopeProjectionFacts, Error> {
    let mut initial = None;
    if admission.observed() {
        // Presence belongs to raw_kind below. These are only requests already
        // exposed by the actual borrowed raw header; no sink is synthesized.
        let captured = input.sink.as_ref().and_then(|sink| sink.kind.as_ref());
        let outer = match captured {
            Some(kind) => admission.numeric(wire_outer(input, kind))?,
            None => admission.sum(input.runtime_filter_ids.len(), input.summaries.len())?,
        };
        let mut model = Model {
            items: outer,
            ..Model::default()
        };
        admission.request::<p::RuntimeFilterId>(&mut model, input.runtime_filter_ids.len(), 2)?;
        admission.request::<p::FragmentId>(
            &mut model,
            wire_placement_sources(input.stage_placement.as_ref()),
            2,
        )?;
        admission.request::<p::SummaryOutput>(&mut model, input.summaries.len(), 2)?;
        decode_sink_requests(captured, &mut model, admission)?;
        admission.gate(&model, source, l)?;
        initial = Some(model);
    }
    let kind = raw_kind(input, w)?;
    let outer = admission.numeric(wire_outer(input, kind))?;
    let mut known = add(
        size_of::<wire::Fragment>(),
        add(
            bytes::<u32>(input.runtime_filter_ids.capacity())?,
            bytes::<u32>(wire_placement_capacity(input.stage_placement.as_ref()))?,
        )?,
    )?;
    known = add(
        known,
        match kind {
            wire::fragment_sink::Kind::Multicast(v) => bytes::<u32>(v.edge_ids.capacity())?,
            wire::fragment_sink::Kind::Router(v) => {
                bytes::<wire::ChangeStreamRoute>(v.routes.capacity())?
            }
            _ => 0,
        },
    )?;
    known = add(
        known,
        bytes::<wire::SummaryOutput>(input.summaries.capacity())?,
    )?;
    count_prefix(0, outer, source, known, l, w)?;
    let mut model = initial.unwrap_or(Model {
        items: outer,
        ..Model::default()
    });
    if !admission.observed() {
        admission.request::<p::SummaryOutput>(&mut model, input.summaries.len(), 2)?;
        admission.request::<p::RuntimeFilterId>(&mut model, input.runtime_filter_ids.len(), 2)?;
        admission.request::<p::FragmentId>(
            &mut model,
            wire_placement_sources(input.stage_placement.as_ref()),
            2,
        )?;
    }
    admission.facts(&model, source, l, w)?;
    for summary in &input.summaries {
        let columns = summary
            .columns
            .as_ref()
            .ok_or_else(|| invalid("summary columns are absent"))?;
        let kind = crate::physical_summary_v2::decode_kind(columns.kind).map_err(invalid)?;
        if columns.value_ids.len() != kind.fields().len() {
            return Err(invalid("summary field count differs from its kind"));
        }
        known = add(known, bytes::<u32>(columns.value_ids.capacity())?)?;
        model.items = admission.sum(model.items, columns.value_ids.len())?;
        let author_refs = usize::from(matches!(
            summary
                .author
                .as_ref()
                .and_then(|author| author.kind.as_ref()),
            Some(wire::summary_author::Kind::NodeId(_))
        ));
        model.refs = admission.sum(
            model.refs,
            admission.sum(columns.value_ids.len(), 1 + author_refs)?,
        )?;
        admission.facts(&model, source, l, w)?;
        floor(source, known, w)?;
        crate::physical_summary_v2::decode_output(summary, w)?;
    }
    match kind {
        wire::fragment_sink::Kind::RootResult(_) => {
            if !admission.observed() {
                decode_sink_requests(Some(kind), &mut model, admission)?;
            }
        }
        wire::fragment_sink::Kind::Multicast(_) => {
            if !admission.observed() {
                decode_sink_requests(Some(kind), &mut model, admission)?;
            }
        }
        wire::fragment_sink::Kind::Router(v) => {
            required(v.effect_value_id, "router effect ValueId is absent", w)?;
            if !admission.observed() {
                decode_sink_requests(Some(kind), &mut model, admission)?;
            }
            admission.facts(&model, source, l, w)?;
            for route in &v.routes {
                let inner = admission.sum(
                    route.accepted_effects.len(),
                    admission.sum(route.input_mapping.len(), wire_route_keys(route)?)?,
                )?;
                model.items = admission.sum(model.items, inner)?;
                let references = || {
                    admission.sum(
                        model.refs,
                        admission.sum(route.input_mapping.len(), wire_route_keys(route)?)?,
                    )
                };
                if admission.observed() {
                    model.refs = references()?;
                    decode_route_requests(route, &mut model, admission)?;
                    admission.gate(&model, source, l)?;
                }
                count_prefix(0, model.items, source, known, l, w)?;
                if !admission.observed() {
                    model.refs = admission.sum(
                        model.refs,
                        admission.sum(route.input_mapping.len(), wire_route_keys(route)?)?,
                    )?;
                }
                cap(model.refs, l.max_value_references, w)?;
                known = add(
                    known,
                    add(
                        route.route_id.capacity(),
                        add(
                            bytes::<i32>(route.accepted_effects.capacity())?,
                            add(
                                bytes::<wire::WriteInputMapping>(route.input_mapping.capacity())?,
                                wire_requirement_backing(route.requirements.as_ref())?,
                            )?,
                        )?,
                    )?,
                )?;
                if !admission.observed() {
                    decode_route_requests(route, &mut model, admission)?;
                }
                admission.facts(&model, source, l, w)?;
                floor(source, known, w)?;
                token32(&route.route_id, w)?;
                ordinal(route.write_target_ordinal, w)?;
                required(route.edge_id, "router route edge ID is absent", w)?;
                for effect in &route.accepted_effects {
                    decode_mutation_effect(*effect, w)?;
                }
                for mapping in &route.input_mapping {
                    known = add(known, mapping.field_token.capacity())?;
                    floor(source, known, w)?;
                    token32(&mapping.field_token, w)?;
                    required(mapping.value_id, "router mapping ValueId is absent", w)?;
                    w.step()?;
                }
                w.step()?;
            }
        }
        _ => {}
    }
    floor(source, known, w)?;
    admission.facts(&model, source, l, w)
}
fn emit_encode(
    input: &p::Fragment,
    w: &mut CompileCheckpoints<'_>,
) -> Result<EncodedFragmentEnvelope, Error> {
    use wire::fragment_sink::Kind;
    let kind = match input.sink() {
        p::FragmentSink::Result => Kind::Result(Empty {}),
        p::FragmentSink::RootResult(contract) => {
            w.flush()?;
            let value = novarocks_proto_codec::root_result::encode_root_contract(contract);
            w.flush()?;
            Kind::RootResult(value)
        }
        p::FragmentSink::Noop => Kind::Noop(Empty {}),
        p::FragmentSink::Stream { edge } => Kind::StreamEdgeId(edge.get()),
        p::FragmentSink::Multicast { edges } => Kind::Multicast(wire::MulticastSink {
            edge_ids: list(edges, w, |id, _| Ok(id.get()))?,
        }),
        p::FragmentSink::Router { effect, routes } => Kind::Router(wire::RouterSink {
            effect_value_id: Some(effect.get()),
            routes: list(routes, w, |route, w| {
                Ok(wire::ChangeStreamRoute {
                    route_id: token_vec(route.route_id.to_bytes(), w)?,
                    write_target_ordinal: route.write_target_ordinal.get(),
                    accepted_effects: list(&route.accepted_effects, w, |effect, _| {
                        Ok(encode_mutation_effect(*effect))
                    })?,
                    input_mapping: list(&route.input_mapping, w, |(token, value), w| {
                        Ok(wire::WriteInputMapping {
                            field_token: token_vec(token.to_bytes(), w)?,
                            value_id: Some(value.get()),
                        })
                    })?,
                    requirements: Some(encode_requirements(&route.requirements, w)?),
                    edge_id: Some(route.edge.get()),
                })
            })?,
        }),
    };
    let dop = input.dop_domain();
    let output = EncodedFragmentEnvelope {
        id: input.id().get(),
        root_node_id: input.root().get(),
        stage_placement: encode_placement(input.stage_placement(), w)?,
        sink: wire::FragmentSink { kind: Some(kind) },
        summaries: list(input.summaries(), w, |output, w| {
            crate::physical_summary_v2::encode_output(*output, w)
        })?,
        dop_domain: wire::PipelineDopDomain {
            min: dop.min,
            max: dop.max,
            requires_power_of_two: dop.requires_power_of_two,
        },
        runtime_filter_ids: list(input.runtime_filters(), w, |id, _| Ok(id.get()))?,
    };
    w.step()?;
    Ok(output)
}
fn emit_decode(
    input: &wire::Fragment,
    w: &mut CompileCheckpoints<'_>,
) -> Result<DecodedFragmentEnvelope, Error> {
    use wire::fragment_sink::Kind;
    let kind = raw_kind(input, w)?;
    let sink = match kind {
        Kind::Result(_) => p::FragmentSink::Result,
        Kind::RootResult(contract) => {
            let root = required(input.root_node_id, "fragment root ID is absent", w)?;
            let mut columns = None;
            for node in &input.nodes {
                if node.id == root {
                    columns = Some(
                        required(node.output.as_ref(), "root output port is absent", w)?
                            .value_ids
                            .len(),
                    );
                }
                w.step()?;
            }
            let columns = required(columns, "root result node is absent", w)?;
            w.flush()?;
            let value = novarocks_proto_codec::root_result::decode_semantic_root_contract(
                contract,
                columns,
                novarocks_proto_codec::FieldPath::root("fragment.sink.root_result"),
            )
            .map_err(Error::Root)?;
            w.flush()?;
            p::FragmentSink::RootResult(Box::new(value))
        }
        Kind::Noop(_) => p::FragmentSink::Noop,
        Kind::StreamEdgeId(id) => p::FragmentSink::Stream {
            edge: p::EdgeId::new(*id),
        },
        Kind::Multicast(v) => p::FragmentSink::Multicast {
            edges: boxed(list(&v.edge_ids, w, |id, _| Ok(p::EdgeId::new(*id)))?, w)?,
        },
        Kind::Router(v) => p::FragmentSink::Router {
            effect: p::ValueId::new(required(
                v.effect_value_id,
                "prepared router effect ID is absent",
                w,
            )?),
            routes: boxed(
                list(&v.routes, w, |route, w| {
                    let route_id = ConnectorWriteRouteId::from_bytes(token32(&route.route_id, w)?);
                    let write_target_ordinal = ordinal(route.write_target_ordinal, w)?;
                    let accepted_effects = boxed(
                        list(&route.accepted_effects, w, |v, w| {
                            decode_mutation_effect(*v, w)
                        })?,
                        w,
                    )?;
                    let input_mapping = boxed(
                        list(&route.input_mapping, w, |mapping, w| {
                            let token = ConnectorWriteFieldToken::from_bytes(token32(
                                &mapping.field_token,
                                w,
                            )?);
                            let id = p::ValueId::new(required(
                                mapping.value_id,
                                "prepared router mapping ID is absent",
                                w,
                            )?);
                            Ok((token, id))
                        })?,
                        w,
                    )?;
                    let requirements = decode_requirements(route.requirements.as_ref(), w)?;
                    Ok(p::ChangeStreamRoute {
                        route_id,
                        write_target_ordinal,
                        accepted_effects,
                        input_mapping,
                        requirements,
                        edge: p::EdgeId::new(required(
                            route.edge_id,
                            "prepared router edge ID is absent",
                            w,
                        )?),
                    })
                })?,
                w,
            )?,
        },
    };
    let dop = required(input.dop_domain.as_ref(), "prepared DOP is absent", w)?;
    let output = DecodedFragmentEnvelope {
        id: p::FragmentId::new(input.id),
        stage_placement: decode_placement(
            required(
                input.stage_placement.as_ref(),
                "fragment stage placement is absent",
                w,
            )?,
            w,
        )?,
        root: p::NodeId::new(required(
            input.root_node_id,
            "prepared fragment root is absent",
            w,
        )?),
        sink,
        summaries: boxed(
            list(
                &input.summaries,
                w,
                crate::physical_summary_v2::decode_output,
            )?,
            w,
        )?,
        dop_domain: p::PipelineDopDomain {
            min: dop.min,
            max: dop.max,
            requires_power_of_two: dop.requires_power_of_two,
        },
        runtime_filters: boxed(
            list(&input.runtime_filter_ids, w, |id, _| {
                Ok(p::RuntimeFilterId::new(*id))
            })?,
            w,
        )?,
    };
    w.step()?;
    Ok(output)
}

pub(crate) struct PreparedFragmentEnvelopeEncode<'a> {
    input: &'a p::Fragment,
    control: &'a dyn PureCompileControl,
    facts: FragmentEnvelopeProjectionFacts,
}
impl PreparedFragmentEnvelopeEncode<'_> {
    #[cfg(test)]
    pub(crate) fn facts(&self) -> &FragmentEnvelopeProjectionFacts {
        &self.facts
    }
    #[cfg(test)]
    pub(crate) fn emit(
        self,
    ) -> Result<(EncodedFragmentEnvelope, FragmentEnvelopeProjectionFacts), Error> {
        let mut w = CompileCheckpoints::try_new(self.control, CompilePhase::Encode)?;
        let result = emit_encode(self.input, &mut w).map(|output| (output, self.facts));
        finish(w, result)
    }

    /// Consume this exact token in the caller's scope. Preparation and emission
    /// replace one contribution; the source invoice and requests are not added twice.
    pub(crate) fn emit_in(
        self,
        admit: &mut impl FnMut(&FragmentEnvelopeProjectionFacts) -> Result<(), CompileControlError>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(EncodedFragmentEnvelope, FragmentEnvelopeProjectionFacts), Error> {
        if !std::ptr::addr_eq(self.control, work.control()) {
            return Err(invalid(
                "fragment envelope emission has a different original control",
            ));
        }
        admit(&self.facts)?;
        emit_encode(self.input, work).map(|output| (output, self.facts))
    }
}
#[cfg(test)]
pub(crate) fn prepare_fragment_envelope_encode<'a>(
    input: &'a p::Fragment,
    source_retained_bytes: usize,
    limits: FragmentEnvelopeProjectionLimits,
    control: &'a dyn PureCompileControl,
) -> Result<PreparedFragmentEnvelopeEncode<'a>, Error> {
    let mut w = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = preflight_encode(
        input,
        source_retained_bytes,
        limits,
        &mut EnvelopeAdmission { parent: None },
        &mut w,
    )
    .map(|facts| PreparedFragmentEnvelopeEncode {
        input,
        control,
        facts,
    });
    finish(w, result)
}
pub(crate) struct PreparedFragmentEnvelopeDecode<'a> {
    input: &'a wire::Fragment,
    control: &'a dyn PureCompileControl,
    facts: FragmentEnvelopeProjectionFacts,
}
impl PreparedFragmentEnvelopeDecode<'_> {
    #[cfg(test)]
    pub(crate) fn facts(&self) -> &FragmentEnvelopeProjectionFacts {
        &self.facts
    }
    #[cfg(test)]
    pub(crate) fn emit(
        self,
    ) -> Result<(DecodedFragmentEnvelope, FragmentEnvelopeProjectionFacts), Error> {
        let mut w = CompileCheckpoints::try_new(self.control, CompilePhase::Decode)?;
        let result = emit_decode(self.input, &mut w).map(|output| (output, self.facts));
        finish(w, result)
    }

    /// Consume this exact token in the caller's scope. Preparation and emission
    /// replace one contribution; the source invoice and requests are not added twice.
    pub(crate) fn emit_in(
        self,
        admit: &mut impl FnMut(&FragmentEnvelopeProjectionFacts) -> Result<(), CompileControlError>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(DecodedFragmentEnvelope, FragmentEnvelopeProjectionFacts), Error> {
        if !std::ptr::addr_eq(self.control, work.control()) {
            return Err(invalid(
                "fragment envelope emission has a different original control",
            ));
        }
        admit(&self.facts)?;
        emit_decode(self.input, work).map(|output| (output, self.facts))
    }
}
#[cfg(test)]
pub(crate) fn prepare_fragment_envelope_decode<'a>(
    input: &'a wire::Fragment,
    source_retained_bytes: usize,
    limits: FragmentEnvelopeProjectionLimits,
    control: &'a dyn PureCompileControl,
) -> Result<PreparedFragmentEnvelopeDecode<'a>, Error> {
    let mut w = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = preflight_decode(
        input,
        source_retained_bytes,
        limits,
        &mut EnvelopeAdmission { parent: None },
        &mut w,
    )
    .map(|facts| PreparedFragmentEnvelopeDecode {
        input,
        control,
        facts,
    });
    finish(w, result)
}

/// Borrow the exact original header and the caller's control/work owner.
pub(crate) fn prepare_fragment_envelope_encode_in<'a, 'control: 'a>(
    input: &'a p::Fragment,
    source_retained_bytes: usize,
    limits: FragmentEnvelopeProjectionLimits,
    admit: &mut impl FnMut(&FragmentEnvelopeProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<PreparedFragmentEnvelopeEncode<'a>, Error> {
    let facts = preflight_encode(
        input,
        source_retained_bytes,
        limits,
        &mut EnvelopeAdmission {
            parent: Some(admit),
        },
        work,
    )?;
    Ok(PreparedFragmentEnvelopeEncode {
        input,
        control: work.control(),
        facts,
    })
}

/// Borrow the exact original header and the caller's control/work owner.
pub(crate) fn prepare_fragment_envelope_decode_in<'a, 'control: 'a>(
    input: &'a wire::Fragment,
    source_retained_bytes: usize,
    limits: FragmentEnvelopeProjectionLimits,
    admit: &mut impl FnMut(&FragmentEnvelopeProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<PreparedFragmentEnvelopeDecode<'a>, Error> {
    let facts = preflight_decode(
        input,
        source_retained_bytes,
        limits,
        &mut EnvelopeAdmission {
            parent: Some(admit),
        },
        work,
    )?;
    Ok(PreparedFragmentEnvelopeDecode {
        input,
        control: work.control(),
        facts,
    })
}
#[cfg(test)]
#[path = "physical_fragment_envelope_v2/tests.rs"]
mod tests;

// The original M07 codec owns schema shape/validation. This conservative
// request envelope covers its flat field tables, all named-child tables,
// root tree, seen state and temporary vectors together. Every schema field
// uses one validated profile node; Vec push growth needs at most twice that
// count. All text backing is bounded by the original profile itself.
pub(crate) fn root_projection_request_bytes() -> Result<usize, Error> {
    use novarocks_proto_models::result as wire;
    use novarocks_result_contract::RootProfileV1 as P;
    let nodes = mul(P::SCHEMA_TYPE_NODES, 2)?;
    let per_node = add(
        size_of::<wire::RenderField>(),
        add(
            size_of::<wire::ScalarField>(),
            add(
                size_of::<wire::NamedRenderField>(),
                add(size_of::<wire::NamedScalarField>(), size_of::<usize>() * 4)?,
            )?,
        )?,
    )?;
    add(
        mul(P::SCHEMA_BACKING_BYTES, 2)?,
        add(
            mul(nodes, per_node)?,
            mul(P::MAX_COLUMNS, size_of::<wire::RenderColumn>() * 2)?,
        )?,
    )
}
fn root_projection_work() -> Result<usize, Error> {
    use novarocks_result_contract::RootProfileV1 as P;
    mul(P::SCHEMA_TYPE_NODES, P::MAX_DEPTH)
}
