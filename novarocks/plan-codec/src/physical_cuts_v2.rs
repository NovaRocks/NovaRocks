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

//! Complete cut representation projection. Remote identities are preserved;
//! topology, coverage legality and runtime-filter laws remain Package duties.
//! The caller admits the truthful source union and opaque host allocations,
//! lends one scope, and owns its entry and ordinary/success footer.
use crate::{
    borrowed_type_resources::verify_type_binding_admitted,
    physical_binding_v2::{BindingProjectionLimits, MaterializationModel},
    physical_node_v2::{self as r, NodeCodecError, NodeProjectionFacts, NodeProjectionLimits},
    physical_properties_v2 as props,
    physical_relational_nodes_v2::{decode_join_side, encode_join_side},
    physical_result_v2::{copy_box, copy_string},
    physical_topn_node_v2::{decode_comparator, decode_phase, encode_comparator, encode_phase},
    physical_type_v2::{DecodedTypeTable, EncodedTypeTable, clone_value_type_observed},
    physical_writer_schema_v2::{decode_role, role},
};
use novarocks_connector_contract::{
    ConnectorWriteFieldToken, ConnectorWriteRouteId, WriteTargetOrdinal,
};
use novarocks_physical_plan as p;
use novarocks_proto_models::{physical_control_v2::Empty, physical_package_v2 as wire};
use novarocks_type_contract::{CompileCheckpoints, CompileControlError, FunctionValueType};
use std::mem::size_of;
pub type CutsCodecError = NodeCodecError;
#[derive(Clone, Copy, Debug)]
pub struct CutsProjectionLimits {
    pub node: NodeProjectionLimits,
    pub binding: BindingProjectionLimits,
}
/// Ordered type IDs for every actual CutValue, WriterResult field, then runtime
/// filter domain occurrence. Repeated occurrences are not interned.
pub struct CutsTypeIds<'a> {
    source: &'a p::FragmentCuts,
    ids: &'a [u32],
}
impl<'a> CutsTypeIds<'a> {
    pub fn new(source: &'a p::FragmentCuts, ids: &'a [u32]) -> Self {
        Self { source, ids }
    }
}
/// All exact sender loans and explicit caller ceilings. No source/control is
/// acquired by this context, and no type roots are copied.
pub struct EncodedCutsContext<'loan, 'source> {
    pub types: &'loan EncodedTypeTable<'source>,
    pub type_ids: &'loan CutsTypeIds<'loan>,
    pub source_retained_bytes: usize,
    pub limits: CutsProjectionLimits,
}
type E = CutsCodecError;
fn shape(s: &'static str) -> E {
    r::invalid(s)
}
fn req<T>(v: Option<T>) -> Result<T, E> {
    v.ok_or_else(|| shape("cut required field is absent"))
}
fn exact32(v: &[u8]) -> Result<[u8; 32], E> {
    v.try_into()
        .map_err(|_| shape("cut token or route identity must have 32 bytes"))
}
fn ordinal(v: u32) -> Result<WriteTargetOrdinal, E> {
    WriteTargetOrdinal::try_new(v).map_err(|_| shape("cut write target ordinal is invalid"))
}
macro_rules! closed {
 ($enc:ident,$dec:ident,$p:ident,$w:ident,$($v:ident),+ $(,)?) => {
 fn $enc(v:p::$p)->i32 { (match v {$(p::$p::$v=>wire::$w::$v),+}) as i32 }
 fn $dec(v:i32)->Result<p::$p,E> { match wire::$w::try_from(v) {$(Ok(wire::$w::$v)=>Ok(p::$p::$v),)+ _=>Err(shape("cut enum is unknown or unspecified"))} }
 };
}
closed!(
    edge,
    read_edge,
    EdgeKind,
    EdgeKind,
    Stream,
    CteMulticast,
    ChangeStreamRouter
);
closed!(
    filter_kind,
    read_filter_kind,
    RuntimeFilterKind,
    RuntimeFilterKind,
    Bloom,
    MinMax,
    InList
);
fn null_sem(p: p::RuntimeFilterNullSemantics) -> i32 {
    match p {
        p::RuntimeFilterNullSemantics::NeverMatches => {
            wire::RuntimeFilterNullSemantics::NeverMatches as i32
        }
        p::RuntimeFilterNullSemantics::NullSafeEqual => {
            wire::RuntimeFilterNullSemantics::NullSafeEqual as i32
        }
    }
}
fn read_null_sem(p: i32) -> Result<p::RuntimeFilterNullSemantics, E> {
    match wire::RuntimeFilterNullSemantics::try_from(p) {
        Ok(wire::RuntimeFilterNullSemantics::NeverMatches) => {
            Ok(p::RuntimeFilterNullSemantics::NeverMatches)
        }
        Ok(wire::RuntimeFilterNullSemantics::NullSafeEqual) => {
            Ok(p::RuntimeFilterNullSemantics::NullSafeEqual)
        }
        _ => Err(shape("cut null semantics is unknown or unspecified")),
    }
}
closed!(
    lifecycle,
    read_lifecycle,
    RuntimeFilterLifecycle,
    RuntimeFilterLifecycle,
    CompleteOnce,
    MonotonicUpdates
);
closed!(
    reduction,
    read_reduction,
    RuntimeFilterReduction,
    RuntimeFilterReduction,
    SetUnion,
    TightenOrderedBound,
    UnionOrderedHull
);
closed!(
    contribution,
    read_contribution,
    RuntimeFilterContributionKind,
    RuntimeFilterContributionKind,
    ValueDomainDelta,
    FinalDomainShard,
    OrderedBoundUpdate,
    FinalOrderedHullShard,
    ProducerClosed
);
closed!(
    completion,
    read_completion,
    RuntimeFilterCompletion,
    RuntimeFilterCompletion,
    ProducerClosed,
    FencedCommittedDomain
);
closed!(
    capability,
    read_capability,
    RuntimeFilterArtifactCapability,
    RuntimeFilterArtifactCapability,
    Membership,
    OrderedRange,
    EmptyDomain
);
closed!(
    granularity,
    read_granularity,
    LateApplyGranularity,
    LateApplyGranularity,
    Row,
    Batch,
    RowGroup,
    Split,
    File
);
fn vec_map<T, U>(
    xs: &[T],
    w: &mut CompileCheckpoints<'_>,
    mut f: impl FnMut(&T, &mut CompileCheckpoints<'_>) -> Result<U, E>,
) -> Result<Vec<U>, E> {
    let mut out = r::reserve(xs.len(), w)?;
    for x in xs {
        out.push(f(x, w)?);
        w.step()?;
    }
    Ok(out)
}
fn box_map<T, U>(
    xs: &[T],
    w: &mut CompileCheckpoints<'_>,
    f: impl FnMut(&T, &mut CompileCheckpoints<'_>) -> Result<U, E>,
) -> Result<Box<[U]>, E> {
    r::boxed(vec_map(xs, w, f)?, w)
}
fn bytes_copy(xs: &[u8], w: &mut CompileCheckpoints<'_>) -> Result<Vec<u8>, E> {
    vec_map(xs, w, |x, _| Ok(*x))
}
struct Count<'admit> {
    admit: &'admit mut dyn FnMut(&NodeProjectionFacts) -> Result<(), CompileControlError>,
    model: r::Model,
    source: usize,
    known: usize,
    limits: CutsProjectionLimits,
    encode: bool,
    types: usize,
    lookup_work: usize,
}
impl Count<'_> {
    fn gate(&mut self) -> Result<(), E> {
        if self.known > self.source {
            return Err(shape("cuts source invoice omits retained source"));
        }
        let facts = self
            .model
            .numerical_facts(self.source, 0, self.limits.node)?;
        (self.admit)(&facts)?;
        Ok(())
    }
    fn list<P, W>(
        &mut self,
        n: usize,
        cap: usize,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<(), E> {
        self.model.items = r::add(self.model.items, n)?;
        self.known = r::add(
            self.known,
            if self.encode {
                r::bytes::<P>(n)?
            } else {
                r::bytes::<W>(cap)?
            },
        )?;
        if self.encode {
            self.model.request::<W>(n, 1)?
        } else {
            self.model.request::<P>(n, 2)?
        };
        self.gate()?;
        w.step()?;
        Ok(())
    }
    fn id(&mut self, w: &mut CompileCheckpoints<'_>) -> Result<(), E> {
        self.model.refs = r::add(self.model.refs, 1)?;
        self.gate()?;
        w.step()?;
        Ok(())
    }
    fn ty(&mut self, w: &mut CompileCheckpoints<'_>) -> Result<(), E> {
        self.types = r::add(self.types, 1)?;
        r::check_cap(self.types, self.limits.binding.max_type_references)?;
        // Only the original namespace lookup is known from this header.
        // Recursive comparison work arrives through its sole admitted author.
        self.model.delegated_work = r::add(self.model.delegated_work, self.lookup_work)?;
        self.id(w)
    }
    fn string(&mut self, n: usize, cap: usize, w: &mut CompileCheckpoints<'_>) -> Result<(), E> {
        self.known = r::add(self.known, cap)?;
        self.model
            .request::<u8>(n, if self.encode { 1 } else { 2 })?;
        self.gate()?;
        w.step()?;
        Ok(())
    }
    fn distribution(
        &mut self,
        physical: Option<&p::Distribution>,
        raw: Option<&wire::Distribution>,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<(), E> {
        let (f, backing) = if let Some(p) = physical {
            (
                props::distribution_encode_numerical_facts(p, self.source)?,
                props::distribution_encode_source_backing_floor(p)?,
            )
        } else {
            let raw = req(raw)?;
            (
                props::distribution_decode_numerical_facts(raw, self.source)?,
                props::distribution_decode_source_backing_floor(raw)?,
            )
        };
        self.known = r::add(self.known, backing)?;
        self.model.property(f)?;
        self.gate()?;
        if let Some(p) = physical {
            props::preflight_distribution_encode_observed(
                p,
                self.source,
                self.limits.node.properties,
                w,
            )?;
        } else {
            let p = req(raw)?;
            props::preflight_distribution_decode_observed(
                p,
                self.source,
                self.limits.node.properties,
                w,
            )?;
            props::validate_distribution_source_observed(p, w)?;
        }
        Ok(())
    }
}
fn count_partition(
    c: &mut Count,
    p: Option<&p::EdgePartitioning>,
    raw: Option<&wire::EdgePartitioning>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), E> {
    if let Some(p) = p {
        c.distribution(Some(&p.source), None, w)?;
        c.distribution(Some(&p.destination), None, w)?;
    } else {
        let p = req(raw)?;
        props::decode_multiplicity(p.source_multiplicity)?;
        props::decode_multiplicity(p.destination_multiplicity)?;
        c.distribution(None, p.source.as_ref(), w)?;
        c.distribution(None, p.destination.as_ref(), w)?;
    }
    Ok(())
}
fn source_port(port: p::FragmentOutputPort) -> i32 {
    crate::physical_summary_v2::encode_port(port)
}
fn read_source_port(raw: i32) -> Result<p::FragmentOutputPort, E> {
    crate::physical_summary_v2::decode_port(raw).map_err(shape)
}
closed!(
    delivery,
    read_delivery,
    DeliveryRequirement,
    DeliveryRequirement,
    DemandDriven,
    MustDrain
);

#[derive(Clone, Copy)]
enum RawPayload<'a> {
    Relational,
    ProviderRoute(&'a wire::ChangeStreamWriterCut),
    WriterResults(&'a wire::WriterResultCut),
    Summary(&'a wire::SummaryCut),
}
fn raw_inbound_payload(
    value: Option<&wire::inbound_fragment_cut::Payload>,
) -> Result<RawPayload<'_>, E> {
    use wire::inbound_fragment_cut::Payload as P;
    Ok(match req(value)? {
        P::Relational(_) => RawPayload::Relational,
        P::ChangeStreamWriter(v) => RawPayload::ProviderRoute(v),
        P::WriterResult(v) => RawPayload::WriterResults(v),
        P::Summary(v) => RawPayload::Summary(v),
    })
}
fn raw_outbound_payload(
    value: Option<&wire::outbound_fragment_cut::Payload>,
) -> Result<RawPayload<'_>, E> {
    use wire::outbound_fragment_cut::Payload as P;
    Ok(match req(value)? {
        P::Relational(_) => RawPayload::Relational,
        P::ChangeStreamWriter(v) => RawPayload::ProviderRoute(v),
        P::WriterResult(v) => RawPayload::WriterResults(v),
        P::Summary(v) => RawPayload::Summary(v),
    })
}
fn decode_summary(
    value: &wire::SummaryCut,
    w: &mut CompileCheckpoints<'_>,
) -> Result<p::SummaryCut, E> {
    Ok(p::SummaryCut {
        source: crate::physical_summary_v2::decode_columns(req(value.source.as_ref())?, w)?,
        destination: crate::physical_summary_v2::decode_columns(
            req(value.destination.as_ref())?,
            w,
        )?,
    })
}
fn count_summary_columns(
    c: &mut Count,
    physical: Option<&p::SummaryColumns>,
    raw: Option<&wire::SummaryColumns>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), E> {
    let n = if let Some(columns) = physical {
        let n = columns.values().len();
        c.model.request::<u32>(n, 1)?;
        n
    } else {
        let columns = req(raw)?;
        let kind = crate::physical_summary_v2::decode_kind(columns.kind).map_err(shape)?;
        if columns.value_ids.len() != kind.fields().len() {
            return Err(shape("summary field count differs from its kind"));
        }
        c.known = r::add(c.known, r::bytes::<u32>(columns.value_ids.capacity())?)?;
        columns.value_ids.len()
    };
    c.model.items = r::add(c.model.items, n)?;
    c.gate()?;
    for _ in 0..n {
        c.id(w)?;
    }
    Ok(())
}
fn count_payload(
    c: &mut Count,
    physical: Option<&p::ExchangePayloadContract>,
    raw: Option<RawPayload<'_>>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), E> {
    let (change, result, rawchange, rawresult, summary) = match (physical, raw) {
        (Some(p::ExchangePayloadContract::Relational), None)
        | (None, Some(RawPayload::Relational)) => (None, None, None, None, false),
        (Some(p::ExchangePayloadContract::ProviderRoute(v)), None) => {
            (Some(v), None, None, None, false)
        }
        (Some(p::ExchangePayloadContract::WriterResults(v)), None) => {
            (None, Some(v), None, None, false)
        }
        (Some(p::ExchangePayloadContract::Summary(v)), None) => {
            count_summary_columns(c, Some(&v.source), None, w)?;
            count_summary_columns(c, Some(&v.destination), None, w)?;
            (None, None, None, None, true)
        }
        (None, Some(RawPayload::ProviderRoute(v))) => (None, None, Some(v), None, false),
        (None, Some(RawPayload::WriterResults(v))) => (None, None, None, Some(v), false),
        (None, Some(RawPayload::Summary(v))) => {
            count_summary_columns(c, None, Some(req(v.source.as_ref())?), w)?;
            count_summary_columns(c, None, Some(req(v.destination.as_ref())?), w)?;
            (None, None, None, None, true)
        }
        _ => return Err(shape("cut payload counting has no exact source")),
    };
    let _ = summary;
    count_extra(c, change, result, rawchange, rawresult, w)
}
fn count_extra(
    c: &mut Count,
    change: Option<&p::ChangeStreamWriterCut>,
    result: Option<&p::WriterResultCut>,
    rawchange: Option<&wire::ChangeStreamWriterCut>,
    rawresult: Option<&wire::WriterResultCut>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), E> {
    if c.encode {
        if let Some(p) = change {
            c.string(32, 0, w)?;
            c.list::<p::ChangeStreamWriterCutField, wire::ChangeStreamWriterCutField>(
                p.fields.len(),
                p.fields.len(),
                w,
            )?;
            for _ in &p.fields {
                c.string(32, 0, w)?;
                c.id(w)?;
                c.id(w)?;
            }
        }
        if let Some(p) = result {
            c.list::<p::WriterResultCutField, wire::WriterResultCutField>(
                p.fields.len(),
                p.fields.len(),
                w,
            )?;
            for f in &p.fields {
                c.string(f.name.len(), f.name.len(), w)?;
                c.id(w)?;
                c.id(w)?;
                c.ty(w)?;
            }
        }
    } else {
        if let Some(p) = rawchange {
            exact32(&p.route_id)?;
            ordinal(p.write_target_ordinal)?;
            c.known = r::add(c.known, p.route_id.capacity())?;
            c.list::<p::ChangeStreamWriterCutField, wire::ChangeStreamWriterCutField>(
                p.fields.len(),
                p.fields.capacity(),
                w,
            )?;
            for f in &p.fields {
                exact32(&f.field_token)?;
                req(f.source_value_id)?;
                req(f.destination_value_id)?;
                c.known = r::add(c.known, f.field_token.capacity())?;
                c.gate()?;
                c.id(w)?;
                c.id(w)?;
            }
        }
        if let Some(p) = rawresult {
            ordinal(p.write_target_ordinal)?;
            c.list::<p::WriterResultCutField, wire::WriterResultCutField>(
                p.fields.len(),
                p.fields.capacity(),
                w,
            )?;
            for f in &p.fields {
                decode_role(f.role).map_err(|_| shape("cut writer role is unknown"))?;
                req(f.source_value_id)?;
                req(f.destination_value_id)?;
                req(f.value_type_id)?;
                c.string(f.name.len(), f.name.capacity(), w)?;
                c.id(w)?;
                c.id(w)?;
                c.ty(w)?;
            }
        }
    }
    Ok(())
}
fn count_coverage(
    c: &mut Count,
    p: Option<&p::RuntimeFilterCoverage>,
    raw: Option<&wire::RuntimeFilterCoverage>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), E> {
    if let Some(p) = p {
        c.list::<p::RuntimeFilterCoverageNode, wire::RuntimeFilterCoverageNode>(
            p.nodes.len(),
            p.nodes.len(),
            w,
        )?;
        for n in &p.nodes {
            match n {
                p::RuntimeFilterCoverageNode::Witness(_) => c.id(w)?,
                p::RuntimeFilterCoverageNode::AllOf { children }
                | p::RuntimeFilterCoverageNode::AnyOf { children } => {
                    c.list::<u32, u32>(children.len(), children.len(), w)?
                }
            }
        }
    } else {
        let p = req(raw)?;
        req(p.root_index)?;
        c.list::<p::RuntimeFilterCoverageNode, wire::RuntimeFilterCoverageNode>(
            p.nodes.len(),
            p.nodes.capacity(),
            w,
        )?;
        for n in &p.nodes {
            use wire::runtime_filter_coverage_node::Kind as K;
            match req(n.kind.as_ref())? {
                K::WitnessId(_) => c.id(w)?,
                K::AllOf(x) | K::AnyOf(x) => {
                    c.list::<u32, u32>(x.node_indices.len(), x.node_indices.capacity(), w)?
                }
            }
        }
    }
    Ok(())
}
fn count_endpoint(
    c: &mut Count,
    p: Option<&p::RuntimeFilterEndpoint>,
    raw: Option<&wire::RuntimeFilterEndpoint>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), E> {
    if let Some(p) = p {
        c.list::<p::ValueId, u32>(p.values.len(), p.values.len(), w)?;
        for _ in &p.values {
            c.id(w)?;
        }
    } else {
        let p = req(raw)?;
        req(p.fragment_id)?;
        req(p.node_id)?;
        c.list::<p::ValueId, u32>(p.value_ids.len(), p.value_ids.capacity(), w)?;
        for _ in &p.value_ids {
            c.id(w)?;
        }
    }
    Ok(())
}
fn count_lineage(
    c: &mut Count,
    p: Option<&[p::RuntimeFilterLineageStep]>,
    raw: Option<&Vec<wire::RuntimeFilterLineageStep>>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), E> {
    if let Some(p) = p {
        c.list::<p::RuntimeFilterLineageStep, wire::RuntimeFilterLineageStep>(p.len(), p.len(), w)?;
        for _ in p {
            c.id(w)?;
        }
    } else {
        let p = req(raw)?;
        c.list::<p::RuntimeFilterLineageStep, wire::RuntimeFilterLineageStep>(
            p.len(),
            p.capacity(),
            w,
        )?;
        for x in p {
            validate_lineage(x, w)?;
            c.id(w)?;
        }
    }
    Ok(())
}
fn count_filters(
    c: &mut Count,
    p: Option<&[p::RuntimeFilter]>,
    raw: Option<&Vec<wire::RuntimeFilter>>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), E> {
    if let Some(xs) = p {
        for x in xs {
            c.ty(w)?;
            count_coverage(c, Some(&x.availability_coverage), None, w)?;
            count_coverage(c, Some(&x.terminal_coverage), None, w)?;
            c.list::<p::RuntimeFilterEqualityWitness, wire::RuntimeFilterEqualityWitness>(
                x.equality_witnesses.len(),
                x.equality_witnesses.len(),
                w,
            )?;
            c.list::<p::RuntimeFilterProducer, wire::RuntimeFilterProducer>(
                x.producers.len(),
                x.producers.len(),
                w,
            )?;
            for p in &x.producers {
                count_endpoint(c, Some(&p.endpoint), None, w)?;
                c.list::<p::RuntimeFilterContributionKind, i32>(
                    p.contribution_kinds.len(),
                    p.contribution_kinds.len(),
                    w,
                )?;
                c.list::<p::EdgeId, u32>(
                    p.progress.build_edges.len(),
                    p.progress.build_edges.len(),
                    w,
                )?;
                c.list::<p::EdgeId, u32>(
                    p.progress.non_build_edges.len(),
                    p.progress.non_build_edges.len(),
                    w,
                )?;
            }
            c.list::<p::RuntimeFilterConsumer, wire::RuntimeFilterConsumer>(
                x.consumers.len(),
                x.consumers.len(),
                w,
            )?;
            for p in &x.consumers {
                count_endpoint(c, Some(&p.endpoint), None, w)?;
                c.list::<p::RuntimeFilterArtifactCapability, i32>(
                    p.capabilities.len(),
                    p.capabilities.len(),
                    w,
                )?;
                match &p.target {
                    p::RuntimeFilterConsumerTarget::JoinProbeKey { .. } => (),
                    p::RuntimeFilterConsumerTarget::ScanField { lineage, .. }
                    | p::RuntimeFilterConsumerTarget::AggregateTopNScanField { lineage, .. } => {
                        count_lineage(c, Some(lineage), None, w)?
                    }
                }
            }
            w.step()?;
        }
    } else {
        let xs = req(raw)?;
        for x in xs {
            req(x.id)?;
            read_filter_kind(x.kind)?;
            read_lifecycle(x.lifecycle)?;
            read_reduction(x.reduction)?;
            req(x.policy.as_ref())?;
            use wire::runtime_filter_domain::Kind as D;
            match req(req(x.domain.as_ref())?.kind.as_ref())? {
                D::Membership(d) => {
                    req(d.value_type_id)?;
                    read_null_sem(d.null_semantics)?;
                }
                D::Ordered(d) => {
                    let k = req(d.key.as_ref())?;
                    req(k.value_type_id)?;
                    props::decode_direction(k.direction)?;
                    props::decode_nulls(k.null_ordering)?;
                    decode_comparator(d.comparator)?;
                }
            }
            c.ty(w)?;
            count_coverage(c, None, x.availability_coverage.as_ref(), w)?;
            count_coverage(c, None, x.terminal_coverage.as_ref(), w)?;
            c.list::<p::RuntimeFilterEqualityWitness, wire::RuntimeFilterEqualityWitness>(
                x.equality_witnesses.len(),
                x.equality_witnesses.capacity(),
                w,
            )?;
            for p in &x.equality_witnesses {
                req(p.id)?;
                req(p.fragment_id)?;
                req(p.join_node_id)?;
                decode_join_side(p.domain_side, w)?;
            }
            c.list::<p::RuntimeFilterProducer, wire::RuntimeFilterProducer>(
                x.producers.len(),
                x.producers.capacity(),
                w,
            )?;
            for p in &x.producers {
                req(p.witness_id)?;
                count_endpoint(c, None, p.endpoint.as_ref(), w)?;
                read_apply(req(p.apply_point.as_ref())?)?;
                read_completion(p.completion)?;
                c.list::<p::RuntimeFilterContributionKind, i32>(
                    p.contribution_kinds.len(),
                    p.contribution_kinds.capacity(),
                    w,
                )?;
                for k in &p.contribution_kinds {
                    read_contribution(*k)?;
                    w.step()?;
                }
                let progress = req(p.progress.as_ref())?;
                c.list::<p::EdgeId, u32>(
                    progress.build_edge_ids.len(),
                    progress.build_edge_ids.capacity(),
                    w,
                )?;
                c.list::<p::EdgeId, u32>(
                    progress.non_build_edge_ids.len(),
                    progress.non_build_edge_ids.capacity(),
                    w,
                )?;
                read_producer_target(req(p.target.as_ref())?)?;
            }
            c.list::<p::RuntimeFilterConsumer, wire::RuntimeFilterConsumer>(
                x.consumers.len(),
                x.consumers.capacity(),
                w,
            )?;
            for p in &x.consumers {
                count_endpoint(c, None, p.endpoint.as_ref(), w)?;
                read_apply(req(p.apply_point.as_ref())?)?;
                read_activation(req(p.activation.as_ref())?)?;
                c.list::<p::RuntimeFilterArtifactCapability, i32>(
                    p.capabilities.len(),
                    p.capabilities.capacity(),
                    w,
                )?;
                for k in &p.capabilities {
                    read_capability(*k)?;
                    w.step()?;
                }
                use wire::runtime_filter_consumer_target::Kind as T;
                match req(req(p.target.as_ref())?.kind.as_ref())? {
                    T::JoinProbeEqualityId(_) => (),
                    T::ScanField(t) => {
                        req(t.equality_id)?;
                        count_lineage(c, None, Some(&t.lineage), w)?;
                    }
                    T::AggregateTopnScanField(t) => {
                        req(t.producer_witness_id)?;
                        count_lineage(c, None, Some(&t.lineage), w)?;
                    }
                }
            }
            w.step()?;
        }
    }
    Ok(())
}
/// Each binding is one fixed-size entry with one runtime-filter reference.
/// Its closed shape is checked here, before any materialization; whether it
/// names exactly the local endpoints remains a Package law.
fn count_bindings(
    c: &mut Count,
    p: Option<&[p::RuntimeFilterBindingCut]>,
    raw: Option<&Vec<wire::RuntimeFilterBindingCut>>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), E> {
    if let Some(xs) = p {
        for x in xs {
            encode_binding(x)?;
            c.id(w)?;
        }
    } else {
        for x in req(raw)? {
            decode_binding(x)?;
            c.id(w)?;
        }
    }
    Ok(())
}
fn count(
    c: &mut Count,
    p: Option<&p::FragmentCuts>,
    raw: Option<&wire::FragmentCuts>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), E> {
    // All four root extents are O(1) and known together before any callback.
    let ([a, b, d, g], [ac, bc, dc, gc]) = if let Some(p) = p {
        let n = [
            p.inbound.len(),
            p.outbound.len(),
            p.runtime_filters.len(),
            p.runtime_filter_bindings.len(),
        ];
        (n, n)
    } else {
        let p = req(raw)?;
        (
            [
                p.inbound.len(),
                p.outbound.len(),
                p.runtime_filters.len(),
                p.runtime_filter_bindings.len(),
            ],
            [
                p.inbound.capacity(),
                p.outbound.capacity(),
                p.runtime_filters.capacity(),
                p.runtime_filter_bindings.capacity(),
            ],
        )
    };
    c.model.inputs = r::add(r::add(r::add(a, b)?, d)?, g)?;
    c.model.items = c.model.inputs;
    if c.encode {
        c.model.request::<wire::InboundFragmentCut>(a, 1)?;
        c.model.request::<wire::OutboundFragmentCut>(b, 1)?;
        c.model.request::<wire::RuntimeFilter>(d, 1)?;
        c.model.request::<wire::RuntimeFilterBindingCut>(g, 1)?;
        c.known = r::add(
            c.known,
            r::add(
                r::bytes::<p::InboundFragmentCut>(a)?,
                r::add(
                    r::bytes::<p::OutboundFragmentCut>(b)?,
                    r::add(
                        r::bytes::<p::RuntimeFilter>(d)?,
                        r::bytes::<p::RuntimeFilterBindingCut>(g)?,
                    )?,
                )?,
            )?,
        )?;
    } else {
        c.model.request::<p::InboundFragmentCut>(a, 2)?;
        c.model.request::<p::OutboundFragmentCut>(b, 2)?;
        c.model.request::<p::RuntimeFilter>(d, 2)?;
        c.model.request::<p::RuntimeFilterBindingCut>(g, 2)?;
        c.known = r::add(
            c.known,
            r::add(
                r::bytes::<wire::InboundFragmentCut>(ac)?,
                r::add(
                    r::bytes::<wire::OutboundFragmentCut>(bc)?,
                    r::add(
                        r::bytes::<wire::RuntimeFilter>(dc)?,
                        r::bytes::<wire::RuntimeFilterBindingCut>(gc)?,
                    )?,
                )?,
            )?,
        )?;
    }
    c.gate()?;
    w.step()?;
    if let Some(p) = p {
        for x in &p.inbound {
            c.list::<p::CutImport, wire::CutImport>(x.imports.len(), x.imports.len(), w)?;
            for _ in &x.imports {
                c.id(w)?;
                c.id(w)?;
                c.ty(w)?;
            }
            count_partition(c, Some(&x.partitioning), None, w)?;
            count_payload(c, Some(&x.payload), None, w)?;
        }

        for x in &p.outbound {
            c.list::<p::CutValue, wire::CutValue>(x.projection.len(), x.projection.len(), w)?;
            for _ in &x.projection {
                c.id(w)?;
                c.ty(w)?;
            }
            c.list::<p::CutImport, wire::CutImport>(
                x.destination_imports.len(),
                x.destination_imports.len(),
                w,
            )?;
            for _ in &x.destination_imports {
                c.id(w)?;
                c.id(w)?;
                c.ty(w)?;
            }
            count_partition(c, Some(&x.partitioning), None, w)?;
            count_payload(c, Some(&x.payload), None, w)?;
        }
        count_filters(c, Some(&p.runtime_filters), None, w)?;
        count_bindings(c, Some(&p.runtime_filter_bindings), None, w)?;
    } else {
        let p = req(raw)?;
        for x in &p.inbound {
            req(x.edge_id)?;
            req(x.source_fragment_id)?;
            req(x.destination_node_id)?;
            read_edge(x.kind)?;
            read_source_port(x.source_port)?;
            read_delivery(x.delivery)?;
            c.list::<p::CutImport, wire::CutImport>(x.imports.len(), x.imports.capacity(), w)?;
            for v in &x.imports {
                let t = req(v.source.as_ref())?;
                req(t.value_id)?;
                req(t.value_type_id)?;
                req(v.destination_value_id)?;
                c.id(w)?;
                c.id(w)?;
                c.ty(w)?;
            }
            count_partition(c, None, x.partitioning.as_ref(), w)?;
            count_payload(c, None, Some(raw_inbound_payload(x.payload.as_ref())?), w)?;
        }

        for x in &p.outbound {
            req(x.edge_id)?;
            req(x.destination_fragment_id)?;
            read_edge(x.kind)?;
            read_source_port(x.source_port)?;
            read_delivery(x.delivery)?;
            c.list::<p::CutValue, wire::CutValue>(x.projection.len(), x.projection.capacity(), w)?;
            for v in &x.projection {
                req(v.value_id)?;
                req(v.value_type_id)?;
                c.id(w)?;
                c.ty(w)?;
            }
            c.list::<p::CutImport, wire::CutImport>(
                x.destination_imports.len(),
                x.destination_imports.capacity(),
                w,
            )?;
            for v in &x.destination_imports {
                let t = req(v.source.as_ref())?;
                req(t.value_id)?;
                req(t.value_type_id)?;
                req(v.destination_value_id)?;
                c.id(w)?;
                c.id(w)?;
                c.ty(w)?;
            }
            count_partition(c, None, x.partitioning.as_ref(), w)?;
            count_payload(c, None, Some(raw_outbound_payload(x.payload.as_ref())?), w)?;
        }
        count_filters(c, None, Some(&p.runtime_filters), w)?;
        count_bindings(c, None, Some(&p.runtime_filter_bindings), w)?;
    }
    Ok(())
}
fn types_physical(
    p: &p::FragmentCuts,
    mut f: impl FnMut(&FunctionValueType) -> Result<(), E>,
) -> Result<(), E> {
    for x in &p.inbound {
        for v in &x.imports {
            f(&v.source.ty)?;
        }
        if let p::ExchangePayloadContract::WriterResults(r) = &x.payload {
            for v in &r.fields {
                f(&v.ty)?;
            }
        }
    }
    for x in &p.outbound {
        for v in &x.projection {
            f(&v.ty)?;
        }
        for v in &x.destination_imports {
            f(&v.source.ty)?;
        }
        if let p::ExchangePayloadContract::WriterResults(r) = &x.payload {
            for v in &r.fields {
                f(&v.ty)?;
            }
        }
    }
    for x in &p.runtime_filters {
        f(match &x.domain {
            p::RuntimeFilterDomain::Membership { ty, .. } => ty,
            p::RuntimeFilterDomain::Ordered { key, .. } => &key.ty,
        })?;
    }
    Ok(())
}
fn types_wire(p: &wire::FragmentCuts, mut f: impl FnMut(u32) -> Result<(), E>) -> Result<(), E> {
    for x in &p.inbound {
        for v in &x.imports {
            f(req(req(v.source.as_ref())?.value_type_id)?)?;
        }
        if let RawPayload::WriterResults(r) = raw_inbound_payload(x.payload.as_ref())? {
            for v in &r.fields {
                f(req(v.value_type_id)?)?;
            }
        }
    }
    for x in &p.outbound {
        for v in &x.projection {
            f(req(v.value_type_id)?)?;
        }
        for v in &x.destination_imports {
            f(req(req(v.source.as_ref())?.value_type_id)?)?;
        }
        if let RawPayload::WriterResults(r) = raw_outbound_payload(x.payload.as_ref())? {
            for v in &r.fields {
                f(req(v.value_type_id)?)?;
            }
        }
    }
    for x in &p.runtime_filters {
        use wire::runtime_filter_domain::Kind as D;
        f(match req(req(x.domain.as_ref())?.kind.as_ref())? {
            D::Membership(d) => req(d.value_type_id)?,
            D::Ordered(d) => req(req(d.key.as_ref())?.value_type_id)?,
        })?;
    }
    Ok(())
}
struct Encode<'a> {
    ids: &'a [u32],
    at: usize,
}
impl Encode<'_> {
    fn ty(&mut self) -> Result<u32, E> {
        let id = *self
            .ids
            .get(self.at)
            .ok_or_else(|| shape("cut type occurrence is absent"))?;
        self.at += 1;
        Ok(id)
    }
}
fn decode_ty(
    types: &DecodedTypeTable,
    id: u32,
    w: &mut CompileCheckpoints<'_>,
) -> Result<FunctionValueType, E> {
    w.flush()?;
    let t = types.value_type(id);
    w.step()?;
    w.flush()?;
    clone_value_type_observed(t.ok_or_else(|| shape("cut value type is unknown"))?, w)
        .map_err(E::from)
}
/// Explicit original source and occurrence-ID loans; no entry or footer.
pub fn encode_fragment_cuts_observed(
    source: &p::FragmentCuts,
    context: EncodedCutsContext<'_, '_>,
    admit: &mut impl FnMut(&NodeProjectionFacts) -> Result<(), CompileControlError>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(wire::FragmentCuts, NodeProjectionFacts), E> {
    let EncodedCutsContext {
        types,
        type_ids: ids,
        source_retained_bytes,
        limits,
    } = context;
    if !std::ptr::eq(source, ids.source) {
        return Err(shape("cut type view belongs to a different source"));
    }
    let mut c = Count {
        admit,
        model: r::Model::default(),
        source: source_retained_bytes,
        known: r::add(
            size_of::<p::FragmentCuts>(),
            size_of::<EncodedTypeTable<'_>>(),
        )?,
        limits,
        encode: true,
        types: 0,
        lookup_work: r::add(types.source_counts().0, 4)?,
    };
    count(&mut c, Some(source), None, w)?;
    if ids.ids.len() != c.types {
        return Err(shape("cut type occurrence count differs"));
    }
    c.gate()?;
    let mut at = 0;
    types_physical(source, |ty| {
        let id = ids.ids[at];
        at += 1;
        let root = types
            .value_type_observed(id, w)?
            .ok_or_else(|| shape("cut value type is unknown"))?;
        let base = c.model.delegated_work;
        let proof = verify_type_binding_admitted::<E>(
            ty,
            root,
            source_retained_bytes,
            limits.node.max_work,
            &mut |facts| {
                c.model.delegated_work = r::add(base, facts.work_upper_bound())?;
                c.gate()
            },
            w,
        )?;
        if !proof.matches() {
            return Err(shape("cut type ID does not preserve full value type"));
        }
        Ok(())
    })?;
    let f = c
        .model
        .numerical_facts(source_retained_bytes, 0, limits.node)?;
    (c.admit)(&f)?;
    let f = c.model.facts(source_retained_bytes, 0, limits.node, w)?;
    let mut e = Encode {
        ids: ids.ids,
        at: 0,
    };
    let out = wire::FragmentCuts {
        inbound: vec_map(&source.inbound, w, |x, w| encode_inbound(x, &mut e, w))?,
        outbound: vec_map(&source.outbound, w, |x, w| encode_outbound(x, &mut e, w))?,
        runtime_filters: vec_map(&source.runtime_filters, w, |x, w| {
            encode_filter(x, &mut e, w)
        })?,
        runtime_filter_bindings: vec_map(&source.runtime_filter_bindings, w, |x, _| {
            encode_binding(x)
        })?,
    };
    Ok((out, f))
}
/// The original sparse decoded namespace supplies complete FVTs. This token is
/// not a type/package/provider proof; the caller retains the source association.
pub fn decode_fragment_cuts_observed(
    source: &wire::FragmentCuts,
    types: &DecodedTypeTable,
    source_retained_bytes: usize,
    limits: CutsProjectionLimits,
    admit: &mut impl FnMut(&NodeProjectionFacts) -> Result<(), CompileControlError>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(p::FragmentCuts, NodeProjectionFacts), E> {
    let mut c = Count {
        admit,
        model: r::Model::default(),
        source: source_retained_bytes,
        known: r::add(
            size_of::<wire::FragmentCuts>(),
            size_of::<DecodedTypeTable>(),
        )?,
        limits,
        encode: false,
        types: 0,
        lookup_work: r::add(types.value_types().len(), 4)?,
    };
    count(&mut c, None, Some(source), w)?;
    let delegate = 0;
    c.gate()?;
    let mut child = MaterializationModel::for_composition(0, 0, source_retained_bytes, c.known);
    child.facts.type_reference_count = c.types;
    child.compose_in_node(c.model, 0, limits.node, limits.binding)?;
    let initial = child.node_numerical_facts(delegate)?;
    (c.admit)(&initial)?;
    types_wire(source, |id| {
        w.flush()?;
        let ty = types.value_type(id);
        if let Some(ty) = ty {
            // The actual lookup has captured this clone source. Its known
            // request prefix wins before the completed lookup observation.
            child.count_owned_type_clone_admitted(
                ty,
                limits.binding,
                &mut |facts| (c.admit)(facts),
                w,
            )?;
            w.step()?;
            w.flush()?;
        } else {
            w.step()?;
            w.flush()?;
            return Err(shape("cut value type is unknown"));
        }
        Ok(())
    })?;
    let f = child.node_numerical_facts(delegate)?;
    (c.admit)(&f)?;
    let f = child.node_facts(delegate, w)?;
    let out = p::FragmentCuts {
        inbound: box_map(&source.inbound, w, |x, w| decode_inbound(x, types, w))?,
        outbound: box_map(&source.outbound, w, |x, w| decode_outbound(x, types, w))?,
        runtime_filters: box_map(&source.runtime_filters, w, |x, w| {
            decode_filter(x, types, w)
        })?,
        runtime_filter_bindings: box_map(&source.runtime_filter_bindings, w, |x, _| {
            decode_binding(x)
        })?,
    };
    Ok((out, f))
}
fn encode_partition(
    p: &p::EdgePartitioning,
    w: &mut CompileCheckpoints<'_>,
) -> Result<wire::EdgePartitioning, E> {
    Ok(wire::EdgePartitioning {
        source: Some(props::emit_distribution_observed(&p.source, w)?),
        source_multiplicity: props::encode_multiplicity(p.source_multiplicity),
        destination: Some(props::emit_distribution_observed(&p.destination, w)?),
        destination_multiplicity: props::encode_multiplicity(p.destination_multiplicity),
    })
}
fn decode_partition(
    p: &wire::EdgePartitioning,
    w: &mut CompileCheckpoints<'_>,
) -> Result<p::EdgePartitioning, E> {
    Ok(p::EdgePartitioning {
        source: props::materialize_distribution_source_observed(req(p.source.as_ref())?, w)?,
        source_multiplicity: props::decode_multiplicity(p.source_multiplicity)?,
        destination: props::materialize_distribution_source_observed(
            req(p.destination.as_ref())?,
            w,
        )?,
        destination_multiplicity: props::decode_multiplicity(p.destination_multiplicity)?,
    })
}
fn encode_value(p: &p::CutValue, e: &mut Encode<'_>) -> Result<wire::CutValue, E> {
    Ok(wire::CutValue {
        value_id: Some(p.value.get()),
        value_type_id: Some(e.ty()?),
    })
}
fn decode_value(
    p: &wire::CutValue,
    t: &DecodedTypeTable,
    w: &mut CompileCheckpoints<'_>,
) -> Result<p::CutValue, E> {
    Ok(p::CutValue {
        value: p::ValueId::new(req(p.value_id)?),
        ty: decode_ty(t, req(p.value_type_id)?, w)?,
    })
}
fn encode_import(p: &p::CutImport, e: &mut Encode<'_>) -> Result<wire::CutImport, E> {
    Ok(wire::CutImport {
        source: Some(encode_value(&p.source, e)?),
        destination_value_id: Some(p.destination.get()),
    })
}
fn decode_import(
    p: &wire::CutImport,
    t: &DecodedTypeTable,
    w: &mut CompileCheckpoints<'_>,
) -> Result<p::CutImport, E> {
    Ok(p::CutImport {
        source: decode_value(req(p.source.as_ref())?, t, w)?,
        destination: p::ValueId::new(req(p.destination_value_id)?),
    })
}
fn encode_change(
    p: &p::ChangeStreamWriterCut,
    w: &mut CompileCheckpoints<'_>,
) -> Result<wire::ChangeStreamWriterCut, E> {
    Ok(wire::ChangeStreamWriterCut {
        route_id: bytes_copy(&p.route_id.to_bytes(), w)?,
        write_target_ordinal: p.write_target_ordinal.get(),
        fields: vec_map(&p.fields, w, |f, w| {
            Ok(wire::ChangeStreamWriterCutField {
                field_token: bytes_copy(&f.token.to_bytes(), w)?,
                source_value_id: Some(f.source.get()),
                destination_value_id: Some(f.destination.get()),
            })
        })?,
    })
}
fn decode_change(
    p: &wire::ChangeStreamWriterCut,
    w: &mut CompileCheckpoints<'_>,
) -> Result<p::ChangeStreamWriterCut, E> {
    Ok(p::ChangeStreamWriterCut {
        route_id: ConnectorWriteRouteId::from_bytes(exact32(&p.route_id)?),
        write_target_ordinal: ordinal(p.write_target_ordinal)?,
        fields: box_map(&p.fields, w, |f, _| {
            Ok(p::ChangeStreamWriterCutField {
                token: ConnectorWriteFieldToken::from_bytes(exact32(&f.field_token)?),
                source: p::ValueId::new(req(f.source_value_id)?),
                destination: p::ValueId::new(req(f.destination_value_id)?),
            })
        })?,
    })
}
fn encode_result(
    p: &p::WriterResultCut,
    e: &mut Encode<'_>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<wire::WriterResultCut, E> {
    Ok(wire::WriterResultCut {
        write_target_ordinal: p.write_target_ordinal.get(),
        schema_revision: p.schema_revision,
        fields: vec_map(&p.fields, w, |f, w| {
            Ok(wire::WriterResultCutField {
                source_value_id: Some(f.source.get()),
                destination_value_id: Some(f.destination.get()),
                name: copy_string(&f.name, w)?,
                value_type_id: Some(e.ty()?),
                role: role(f.role),
            })
        })?,
    })
}
fn decode_result(
    p: &wire::WriterResultCut,
    t: &DecodedTypeTable,
    w: &mut CompileCheckpoints<'_>,
) -> Result<p::WriterResultCut, E> {
    Ok(p::WriterResultCut {
        write_target_ordinal: ordinal(p.write_target_ordinal)?,
        schema_revision: p.schema_revision,
        fields: box_map(&p.fields, w, |f, w| {
            let name = copy_box(&f.name, w)?;
            Ok(p::WriterResultCutField {
                source: p::ValueId::new(req(f.source_value_id)?),
                destination: p::ValueId::new(req(f.destination_value_id)?),
                name,
                ty: decode_ty(t, req(f.value_type_id)?, w)?,
                role: decode_role(f.role).map_err(|_| shape("cut writer role is unknown"))?,
            })
        })?,
    })
}
fn decode_payload(
    value: RawPayload<'_>,
    t: &DecodedTypeTable,
    w: &mut CompileCheckpoints<'_>,
) -> Result<p::ExchangePayloadContract, E> {
    Ok(match value {
        RawPayload::Relational => p::ExchangePayloadContract::Relational,
        RawPayload::ProviderRoute(v) => {
            p::ExchangePayloadContract::ProviderRoute(decode_change(v, w)?)
        }
        RawPayload::WriterResults(v) => {
            p::ExchangePayloadContract::WriterResults(decode_result(v, t, w)?)
        }
        RawPayload::Summary(v) => p::ExchangePayloadContract::Summary(decode_summary(v, w)?),
    })
}
macro_rules! encode_payload {
    ($value:expr, $module:ident, $e:expr, $w:expr) => {{
        use wire::$module::Payload as P;
        match $value {
            p::ExchangePayloadContract::Relational => P::Relational(Empty {}),
            p::ExchangePayloadContract::ProviderRoute(v) => {
                P::ChangeStreamWriter(encode_change(v, $w)?)
            }
            p::ExchangePayloadContract::WriterResults(v) => {
                P::WriterResult(encode_result(v, $e, $w)?)
            }
            p::ExchangePayloadContract::Summary(v) => P::Summary(wire::SummaryCut {
                source: Some(crate::physical_summary_v2::encode_columns(v.source, $w)?),
                destination: Some(crate::physical_summary_v2::encode_columns(
                    v.destination,
                    $w,
                )?),
            }),
        }
    }};
}
fn encode_inbound(
    p: &p::InboundFragmentCut,
    e: &mut Encode<'_>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<wire::InboundFragmentCut, E> {
    Ok(wire::InboundFragmentCut {
        edge_id: Some(p.edge.get()),
        kind: edge(p.kind),
        source_fragment_id: Some(p.source_fragment.get()),
        destination_node_id: Some(p.destination_node.get()),
        imports: vec_map(&p.imports, w, |p, _| encode_import(p, e))?,
        partitioning: Some(encode_partition(&p.partitioning, w)?),
        source_port: source_port(p.source_port),
        delivery: delivery(p.delivery),
        payload: Some(encode_payload!(&p.payload, inbound_fragment_cut, e, w)),
    })
}
fn decode_inbound(
    p: &wire::InboundFragmentCut,
    t: &DecodedTypeTable,
    w: &mut CompileCheckpoints<'_>,
) -> Result<p::InboundFragmentCut, E> {
    Ok(p::InboundFragmentCut {
        edge: p::EdgeId::new(req(p.edge_id)?),
        kind: read_edge(p.kind)?,
        source_fragment: p::FragmentId::new(req(p.source_fragment_id)?),
        destination_node: p::NodeId::new(req(p.destination_node_id)?),
        imports: box_map(&p.imports, w, |p, w| decode_import(p, t, w))?,
        partitioning: decode_partition(req(p.partitioning.as_ref())?, w)?,
        source_port: read_source_port(p.source_port)?,
        delivery: read_delivery(p.delivery)?,
        payload: decode_payload(raw_inbound_payload(p.payload.as_ref())?, t, w)?,
    })
}
fn encode_outbound(
    p: &p::OutboundFragmentCut,
    e: &mut Encode<'_>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<wire::OutboundFragmentCut, E> {
    Ok(wire::OutboundFragmentCut {
        edge_id: Some(p.edge.get()),
        kind: edge(p.kind),
        destination_fragment_id: Some(p.destination_fragment.get()),
        destination_node_id: Some(p.destination_node.get()),
        projection: vec_map(&p.projection, w, |p, _| encode_value(p, e))?,
        destination_imports: vec_map(&p.destination_imports, w, |p, _| encode_import(p, e))?,
        partitioning: Some(encode_partition(&p.partitioning, w)?),
        source_port: source_port(p.source_port),
        delivery: delivery(p.delivery),
        payload: Some(encode_payload!(&p.payload, outbound_fragment_cut, e, w)),
    })
}
fn decode_outbound(
    p: &wire::OutboundFragmentCut,
    t: &DecodedTypeTable,
    w: &mut CompileCheckpoints<'_>,
) -> Result<p::OutboundFragmentCut, E> {
    Ok(p::OutboundFragmentCut {
        edge: p::EdgeId::new(req(p.edge_id)?),
        kind: read_edge(p.kind)?,
        destination_fragment: p::FragmentId::new(req(p.destination_fragment_id)?),
        destination_node: p::NodeId::new(req(p.destination_node_id)?),
        projection: box_map(&p.projection, w, |p, w| decode_value(p, t, w))?,
        destination_imports: box_map(&p.destination_imports, w, |p, w| decode_import(p, t, w))?,
        partitioning: decode_partition(req(p.partitioning.as_ref())?, w)?,
        source_port: read_source_port(p.source_port)?,
        delivery: read_delivery(p.delivery)?,
        payload: decode_payload(raw_outbound_payload(p.payload.as_ref())?, t, w)?,
    })
}
fn encode_coverage(
    p: &p::RuntimeFilterCoverage,
    w: &mut CompileCheckpoints<'_>,
) -> Result<wire::RuntimeFilterCoverage, E> {
    use wire::runtime_filter_coverage_node::Kind as K;
    Ok(wire::RuntimeFilterCoverage {
        nodes: vec_map(&p.nodes, w, |n, w| {
            Ok(wire::RuntimeFilterCoverageNode {
                kind: Some(match n {
                    p::RuntimeFilterCoverageNode::Witness(id) => K::WitnessId(id.get()),
                    p::RuntimeFilterCoverageNode::AllOf { children } => {
                        K::AllOf(wire::CoverageChildren {
                            node_indices: vec_map(children, w, |x, _| Ok(*x))?,
                        })
                    }
                    p::RuntimeFilterCoverageNode::AnyOf { children } => {
                        K::AnyOf(wire::CoverageChildren {
                            node_indices: vec_map(children, w, |x, _| Ok(*x))?,
                        })
                    }
                }),
            })
        })?,
        root_index: Some(p.root),
    })
}
fn decode_coverage(
    p: &wire::RuntimeFilterCoverage,
    w: &mut CompileCheckpoints<'_>,
) -> Result<p::RuntimeFilterCoverage, E> {
    use wire::runtime_filter_coverage_node::Kind as K;
    Ok(p::RuntimeFilterCoverage {
        nodes: box_map(&p.nodes, w, |n, w| {
            Ok(match req(n.kind.as_ref())? {
                K::WitnessId(id) => {
                    p::RuntimeFilterCoverageNode::Witness(p::RuntimeFilterWitnessId::new(*id))
                }
                K::AllOf(x) => p::RuntimeFilterCoverageNode::AllOf {
                    children: box_map(&x.node_indices, w, |x, _| Ok(*x))?,
                },
                K::AnyOf(x) => p::RuntimeFilterCoverageNode::AnyOf {
                    children: box_map(&x.node_indices, w, |x, _| Ok(*x))?,
                },
            })
        })?,
        root: req(p.root_index)?,
    })
}
fn encode_endpoint(
    p: &p::RuntimeFilterEndpoint,
    w: &mut CompileCheckpoints<'_>,
) -> Result<wire::RuntimeFilterEndpoint, E> {
    Ok(wire::RuntimeFilterEndpoint {
        fragment_id: Some(p.fragment.get()),
        node_id: Some(p.node.get()),
        value_ids: vec_map(&p.values, w, |x, _| Ok(x.get()))?,
    })
}
fn decode_endpoint(
    p: &wire::RuntimeFilterEndpoint,
    w: &mut CompileCheckpoints<'_>,
) -> Result<p::RuntimeFilterEndpoint, E> {
    Ok(p::RuntimeFilterEndpoint {
        fragment: p::FragmentId::new(req(p.fragment_id)?),
        node: p::NodeId::new(req(p.node_id)?),
        values: box_map(&p.value_ids, w, |x, _| Ok(p::ValueId::new(*x)))?,
    })
}
fn encode_apply(p: p::RuntimeFilterApplyPoint) -> wire::RuntimeFilterApplyPoint {
    use wire::runtime_filter_apply_point::Kind as K;
    wire::RuntimeFilterApplyPoint {
        kind: Some(match p {
            p::RuntimeFilterApplyPoint::NodeInput { input_ordinal } => {
                K::NodeInputOrdinal(input_ordinal)
            }
            p::RuntimeFilterApplyPoint::NodeOutput => K::NodeOutput(Empty {}),
            p::RuntimeFilterApplyPoint::ScanSource => K::ScanSource(Empty {}),
        }),
    }
}
fn read_apply(p: &wire::RuntimeFilterApplyPoint) -> Result<p::RuntimeFilterApplyPoint, E> {
    use wire::runtime_filter_apply_point::Kind as K;
    Ok(match req(p.kind.as_ref())? {
        K::NodeInputOrdinal(n) => p::RuntimeFilterApplyPoint::NodeInput { input_ordinal: *n },
        K::NodeOutput(_) => p::RuntimeFilterApplyPoint::NodeOutput,
        K::ScanSource(_) => p::RuntimeFilterApplyPoint::ScanSource,
    })
}
fn encode_activation(
    p: p::RuntimeFilterConsumerActivation,
) -> wire::RuntimeFilterConsumerActivation {
    use wire::runtime_filter_consumer_activation::Kind as K;
    wire::RuntimeFilterConsumerActivation {
        kind: Some(match p {
            p::RuntimeFilterConsumerActivation::BlockingSnapshot => K::BlockingSnapshot(Empty {}),
            p::RuntimeFilterConsumerActivation::StartUnfilteredThenApplyComplete { late_apply } => {
                K::StartUnfilteredThenApplyComplete(granularity(late_apply))
            }
            p::RuntimeFilterConsumerActivation::NonBlockingLive { late_apply } => {
                K::NonBlockingLive(granularity(late_apply))
            }
        }),
    }
}
fn read_activation(
    p: &wire::RuntimeFilterConsumerActivation,
) -> Result<p::RuntimeFilterConsumerActivation, E> {
    use wire::runtime_filter_consumer_activation::Kind as K;
    Ok(match req(p.kind.as_ref())? {
        K::BlockingSnapshot(_) => p::RuntimeFilterConsumerActivation::BlockingSnapshot,
        K::StartUnfilteredThenApplyComplete(n) => {
            p::RuntimeFilterConsumerActivation::StartUnfilteredThenApplyComplete {
                late_apply: read_granularity(*n)?,
            }
        }
        K::NonBlockingLive(n) => p::RuntimeFilterConsumerActivation::NonBlockingLive {
            late_apply: read_granularity(*n)?,
        },
    })
}
fn encode_producer_target(p: &p::RuntimeFilterProducerTarget) -> wire::RuntimeFilterProducerTarget {
    use wire::runtime_filter_producer_target::Kind as K;
    wire::RuntimeFilterProducerTarget {
        kind: Some(match *p {
            p::RuntimeFilterProducerTarget::JoinBuildKey { equality } => {
                K::JoinBuildEqualityId(equality.get())
            }
            p::RuntimeFilterProducerTarget::AggregateTopNKey {
                group_key_ordinal,
                topn,
                phase,
                order_key_ordinal,
                limit,
                offset,
                direction,
                null_ordering,
            } => K::AggregateTopnKey(wire::RuntimeFilterAggregateTopNKey {
                group_key_ordinal,
                topn_node_id: Some(topn.get()),
                phase: Some(encode_phase(phase)),
                order_key_ordinal,
                limit,
                offset,
                direction: props::encode_direction(direction),
                null_ordering: props::encode_nulls(null_ordering),
            }),
        }),
    }
}
fn read_producer_target(
    p: &wire::RuntimeFilterProducerTarget,
) -> Result<p::RuntimeFilterProducerTarget, E> {
    use wire::runtime_filter_producer_target::Kind as K;
    Ok(match req(p.kind.as_ref())? {
        K::JoinBuildEqualityId(id) => p::RuntimeFilterProducerTarget::JoinBuildKey {
            equality: p::RuntimeFilterEqualityWitnessId::new(*id),
        },
        K::AggregateTopnKey(p) => p::RuntimeFilterProducerTarget::AggregateTopNKey {
            group_key_ordinal: p.group_key_ordinal,
            topn: p::NodeId::new(req(p.topn_node_id)?),
            phase: decode_phase(p.phase.as_ref())?,
            order_key_ordinal: p.order_key_ordinal,
            limit: p.limit,
            offset: p.offset,
            direction: props::decode_direction(p.direction)?,
            null_ordering: props::decode_nulls(p.null_ordering)?,
        },
    })
}
fn encode_lineage(p: &p::RuntimeFilterLineageStep) -> wire::RuntimeFilterLineageStep {
    use wire::runtime_filter_lineage_step::Kind as K;
    wire::RuntimeFilterLineageStep {
        kind: Some(match *p {
            p::RuntimeFilterLineageStep::FilterPassThrough {
                fragment,
                node,
                input_ordinal,
            } => K::FilterPassThrough(wire::FragmentInputLineage {
                fragment_id: Some(fragment.get()),
                node_id: Some(node.get()),
                input_ordinal,
            }),
            p::RuntimeFilterLineageStep::SortPassThrough {
                fragment,
                node,
                input_ordinal,
            } => K::SortPassThrough(wire::FragmentInputLineage {
                fragment_id: Some(fragment.get()),
                node_id: Some(node.get()),
                input_ordinal,
            }),
            p::RuntimeFilterLineageStep::JoinOutputPassThrough {
                fragment,
                node,
                input_ordinal,
            } => K::JoinOutputPassThrough(wire::FragmentInputLineage {
                fragment_id: Some(fragment.get()),
                node_id: Some(node.get()),
                input_ordinal,
            }),
            p::RuntimeFilterLineageStep::ProjectIdentity {
                fragment,
                node,
                output_ordinal,
            } => K::ProjectIdentity(wire::ProjectIdentityLineage {
                fragment_id: Some(fragment.get()),
                node_id: Some(node.get()),
                output_ordinal,
            }),
            p::RuntimeFilterLineageStep::AggregateGroupKey {
                fragment,
                node,
                group_key_ordinal,
            } => K::AggregateGroupKey(wire::AggregateGroupKeyLineage {
                fragment_id: Some(fragment.get()),
                node_id: Some(node.get()),
                group_key_ordinal,
            }),
            p::RuntimeFilterLineageStep::UnionAllBranch {
                fragment,
                node,
                input_ordinal,
                output_ordinal,
            } => K::UnionAllBranch(wire::UnionAllBranchLineage {
                fragment_id: Some(fragment.get()),
                node_id: Some(node.get()),
                input_ordinal,
                output_ordinal,
            }),
            p::RuntimeFilterLineageStep::JoinEquality {
                fragment,
                node,
                key_ordinal,
                source_side,
                target_side,
            } => K::JoinEquality(wire::JoinEqualityLineage {
                fragment_id: Some(fragment.get()),
                node_id: Some(node.get()),
                key_ordinal,
                source_side: encode_join_side(source_side),
                target_side: encode_join_side(target_side),
            }),
            p::RuntimeFilterLineageStep::ExchangeMapping {
                edge,
                mapping_ordinal,
            } => K::ExchangeMapping(wire::ExchangeMappingLineage {
                edge_id: Some(edge.get()),
                mapping_ordinal,
            }),
        }),
    }
}
fn read_lineage(
    p: &wire::RuntimeFilterLineageStep,
    w: &mut CompileCheckpoints<'_>,
) -> Result<p::RuntimeFilterLineageStep, E> {
    use wire::runtime_filter_lineage_step::Kind as K;
    Ok(match req(p.kind.as_ref())? {
        K::FilterPassThrough(p) => p::RuntimeFilterLineageStep::FilterPassThrough {
            fragment: p::FragmentId::new(req(p.fragment_id)?),
            node: p::NodeId::new(req(p.node_id)?),
            input_ordinal: p.input_ordinal,
        },
        K::SortPassThrough(p) => p::RuntimeFilterLineageStep::SortPassThrough {
            fragment: p::FragmentId::new(req(p.fragment_id)?),
            node: p::NodeId::new(req(p.node_id)?),
            input_ordinal: p.input_ordinal,
        },
        K::JoinOutputPassThrough(p) => p::RuntimeFilterLineageStep::JoinOutputPassThrough {
            fragment: p::FragmentId::new(req(p.fragment_id)?),
            node: p::NodeId::new(req(p.node_id)?),
            input_ordinal: p.input_ordinal,
        },
        K::ProjectIdentity(p) => p::RuntimeFilterLineageStep::ProjectIdentity {
            fragment: p::FragmentId::new(req(p.fragment_id)?),
            node: p::NodeId::new(req(p.node_id)?),
            output_ordinal: p.output_ordinal,
        },
        K::AggregateGroupKey(p) => p::RuntimeFilterLineageStep::AggregateGroupKey {
            fragment: p::FragmentId::new(req(p.fragment_id)?),
            node: p::NodeId::new(req(p.node_id)?),
            group_key_ordinal: p.group_key_ordinal,
        },
        K::UnionAllBranch(p) => p::RuntimeFilterLineageStep::UnionAllBranch {
            fragment: p::FragmentId::new(req(p.fragment_id)?),
            node: p::NodeId::new(req(p.node_id)?),
            input_ordinal: p.input_ordinal,
            output_ordinal: p.output_ordinal,
        },
        K::JoinEquality(p) => p::RuntimeFilterLineageStep::JoinEquality {
            fragment: p::FragmentId::new(req(p.fragment_id)?),
            node: p::NodeId::new(req(p.node_id)?),
            key_ordinal: p.key_ordinal,
            source_side: decode_join_side(p.source_side, w)?,
            target_side: decode_join_side(p.target_side, w)?,
        },
        K::ExchangeMapping(p) => p::RuntimeFilterLineageStep::ExchangeMapping {
            edge: p::EdgeId::new(req(p.edge_id)?),
            mapping_ordinal: p.mapping_ordinal,
        },
    })
}
fn validate_lineage(
    p: &wire::RuntimeFilterLineageStep,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), E> {
    read_lineage(p, w).map(|_| ())
}
fn encode_consumer_target(
    p: &p::RuntimeFilterConsumerTarget,
    w: &mut CompileCheckpoints<'_>,
) -> Result<wire::RuntimeFilterConsumerTarget, E> {
    use wire::runtime_filter_consumer_target::Kind as K;
    Ok(wire::RuntimeFilterConsumerTarget {
        kind: Some(match p {
            p::RuntimeFilterConsumerTarget::JoinProbeKey { equality } => {
                K::JoinProbeEqualityId(equality.get())
            }
            p::RuntimeFilterConsumerTarget::ScanField { equality, lineage } => {
                K::ScanField(wire::RuntimeFilterScanField {
                    equality_id: Some(equality.get()),
                    lineage: vec_map(lineage, w, |p, _| Ok(encode_lineage(p)))?,
                })
            }
            p::RuntimeFilterConsumerTarget::AggregateTopNScanField { producer, lineage } => {
                K::AggregateTopnScanField(wire::RuntimeFilterAggregateTopNScanField {
                    producer_witness_id: Some(producer.get()),
                    lineage: vec_map(lineage, w, |p, _| Ok(encode_lineage(p)))?,
                })
            }
        }),
    })
}
fn decode_consumer_target(
    p: &wire::RuntimeFilterConsumerTarget,
    w: &mut CompileCheckpoints<'_>,
) -> Result<p::RuntimeFilterConsumerTarget, E> {
    use wire::runtime_filter_consumer_target::Kind as K;
    Ok(match req(p.kind.as_ref())? {
        K::JoinProbeEqualityId(id) => p::RuntimeFilterConsumerTarget::JoinProbeKey {
            equality: p::RuntimeFilterEqualityWitnessId::new(*id),
        },
        K::ScanField(p) => p::RuntimeFilterConsumerTarget::ScanField {
            equality: p::RuntimeFilterEqualityWitnessId::new(req(p.equality_id)?),
            lineage: box_map(&p.lineage, w, read_lineage)?,
        },
        K::AggregateTopnScanField(p) => p::RuntimeFilterConsumerTarget::AggregateTopNScanField {
            producer: p::RuntimeFilterWitnessId::new(req(p.producer_witness_id)?),
            lineage: box_map(&p.lineage, w, read_lineage)?,
        },
    })
}
fn encode_filter(
    p: &p::RuntimeFilter,
    e: &mut Encode<'_>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<wire::RuntimeFilter, E> {
    use wire::runtime_filter_domain::Kind as D;
    let domain = wire::RuntimeFilterDomain {
        kind: Some(match &p.domain {
            p::RuntimeFilterDomain::Membership { null_semantics, .. } => {
                D::Membership(wire::RuntimeFilterMembershipDomain {
                    value_type_id: Some(e.ty()?),
                    null_semantics: null_sem(*null_semantics),
                })
            }
            p::RuntimeFilterDomain::Ordered {
                key,
                inclusive,
                comparator,
            } => D::Ordered(wire::RuntimeFilterOrderedDomain {
                key: Some(wire::RuntimeFilterOrderKey {
                    value_type_id: Some(e.ty()?),
                    direction: props::encode_direction(key.direction),
                    null_ordering: props::encode_nulls(key.null_ordering),
                }),
                inclusive: *inclusive,
                comparator: encode_comparator(*comparator),
            }),
        }),
    };
    Ok(wire::RuntimeFilter {
        id: Some(p.id.get()),
        kind: filter_kind(p.kind),
        domain: Some(domain),
        lifecycle: lifecycle(p.lifecycle),
        reduction: reduction(p.reduction),
        availability_coverage: Some(encode_coverage(&p.availability_coverage, w)?),
        terminal_coverage: Some(encode_coverage(&p.terminal_coverage, w)?),
        equality_witnesses: vec_map(&p.equality_witnesses, w, |p, _| {
            Ok(wire::RuntimeFilterEqualityWitness {
                id: Some(p.id.get()),
                fragment_id: Some(p.fragment.get()),
                join_node_id: Some(p.join.get()),
                key_ordinal: p.key_ordinal,
                domain_side: encode_join_side(p.domain_side),
            })
        })?,
        producers: vec_map(&p.producers, w, |p, w| {
            Ok(wire::RuntimeFilterProducer {
                witness_id: Some(p.witness.get()),
                endpoint: Some(encode_endpoint(&p.endpoint, w)?),
                apply_point: Some(encode_apply(p.apply_point)),
                contribution_kinds: vec_map(&p.contribution_kinds, w, |k, _| Ok(contribution(*k)))?,
                completion: completion(p.completion),
                progress: Some(wire::RuntimeFilterProducerProgress {
                    build_edge_ids: vec_map(&p.progress.build_edges, w, |e, _| Ok(e.get()))?,
                    non_build_edge_ids: vec_map(
                        &p.progress.non_build_edges,
                        w,
                        |e, _| Ok(e.get()),
                    )?,
                }),
                target: Some(encode_producer_target(&p.target)),
            })
        })?,
        consumers: vec_map(&p.consumers, w, |p, w| {
            Ok(wire::RuntimeFilterConsumer {
                endpoint: Some(encode_endpoint(&p.endpoint, w)?),
                apply_point: Some(encode_apply(p.apply_point)),
                capabilities: vec_map(&p.capabilities, w, |k, _| Ok(capability(*k)))?,
                activation: Some(encode_activation(p.activation)),
                target: Some(encode_consumer_target(&p.target, w)?),
            })
        })?,
        policy: Some(wire::RuntimeFilterPolicy {
            max_contribution_bytes: p.policy.max_contribution_bytes,
            max_artifact_bytes: p.policy.max_artifact_bytes,
            deadline_ms: p.policy.deadline_ms,
            max_retries: p.policy.max_retries,
        }),
    })
}
fn encode_binding(p: &p::RuntimeFilterBindingCut) -> Result<wire::RuntimeFilterBindingCut, E> {
    use wire::runtime_filter_binding_cut::Role as R;
    let index = |index: usize| {
        u32::try_from(index).map_err(|_| shape("cut runtime-filter binding index exceeds u32"))
    };
    Ok(wire::RuntimeFilterBindingCut {
        binding_id: Some(p.binding_id),
        runtime_filter_id: Some(p.filter.get()),
        role: Some(match p.role {
            p::RuntimeFilterBindingRole::Producer(i) => R::ProducerIndex(index(i)?),
            p::RuntimeFilterBindingRole::Consumer(i) => R::ConsumerIndex(index(i)?),
        }),
    })
}
fn decode_binding(p: &wire::RuntimeFilterBindingCut) -> Result<p::RuntimeFilterBindingCut, E> {
    use wire::runtime_filter_binding_cut::Role as R;
    let index = |index: u32| {
        usize::try_from(index).map_err(|_| shape("cut runtime-filter binding index exceeds usize"))
    };
    Ok(p::RuntimeFilterBindingCut {
        binding_id: req(p.binding_id)?,
        filter: p::RuntimeFilterId::new(req(p.runtime_filter_id)?),
        role: match req(p.role.as_ref())? {
            R::ProducerIndex(i) => p::RuntimeFilterBindingRole::Producer(index(*i)?),
            R::ConsumerIndex(i) => p::RuntimeFilterBindingRole::Consumer(index(*i)?),
        },
    })
}
fn decode_filter(
    p: &wire::RuntimeFilter,
    t: &DecodedTypeTable,
    w: &mut CompileCheckpoints<'_>,
) -> Result<p::RuntimeFilter, E> {
    use wire::runtime_filter_domain::Kind as D;
    let domain = match req(req(p.domain.as_ref())?.kind.as_ref())? {
        D::Membership(p) => p::RuntimeFilterDomain::Membership {
            ty: decode_ty(t, req(p.value_type_id)?, w)?,
            null_semantics: read_null_sem(p.null_semantics)?,
        },
        D::Ordered(p) => {
            let k = req(p.key.as_ref())?;
            p::RuntimeFilterDomain::Ordered {
                key: p::RuntimeFilterOrderKey {
                    ty: decode_ty(t, req(k.value_type_id)?, w)?,
                    direction: props::decode_direction(k.direction)?,
                    null_ordering: props::decode_nulls(k.null_ordering)?,
                },
                inclusive: p.inclusive,
                comparator: decode_comparator(p.comparator)?,
            }
        }
    };
    let policy = req(p.policy.as_ref())?;
    Ok(p::RuntimeFilter {
        id: p::RuntimeFilterId::new(req(p.id)?),
        kind: read_filter_kind(p.kind)?,
        domain,
        lifecycle: read_lifecycle(p.lifecycle)?,
        reduction: read_reduction(p.reduction)?,
        availability_coverage: decode_coverage(req(p.availability_coverage.as_ref())?, w)?,
        terminal_coverage: decode_coverage(req(p.terminal_coverage.as_ref())?, w)?,
        equality_witnesses: box_map(&p.equality_witnesses, w, |p, w| {
            Ok(p::RuntimeFilterEqualityWitness {
                id: p::RuntimeFilterEqualityWitnessId::new(req(p.id)?),
                fragment: p::FragmentId::new(req(p.fragment_id)?),
                join: p::NodeId::new(req(p.join_node_id)?),
                key_ordinal: p.key_ordinal,
                domain_side: decode_join_side(p.domain_side, w)?,
            })
        })?,
        producers: box_map(&p.producers, w, |p, w| {
            let progress = req(p.progress.as_ref())?;
            Ok(p::RuntimeFilterProducer {
                witness: p::RuntimeFilterWitnessId::new(req(p.witness_id)?),
                endpoint: decode_endpoint(req(p.endpoint.as_ref())?, w)?,
                apply_point: read_apply(req(p.apply_point.as_ref())?)?,
                contribution_kinds: box_map(&p.contribution_kinds, w, |k, _| {
                    read_contribution(*k)
                })?,
                completion: read_completion(p.completion)?,
                progress: p::RuntimeFilterProducerProgress {
                    build_edges: box_map(&progress.build_edge_ids, w, |e, _| {
                        Ok(p::EdgeId::new(*e))
                    })?,
                    non_build_edges: box_map(&progress.non_build_edge_ids, w, |e, _| {
                        Ok(p::EdgeId::new(*e))
                    })?,
                },
                target: read_producer_target(req(p.target.as_ref())?)?,
            })
        })?,
        consumers: box_map(&p.consumers, w, |p, w| {
            Ok(p::RuntimeFilterConsumer {
                endpoint: decode_endpoint(req(p.endpoint.as_ref())?, w)?,
                apply_point: read_apply(req(p.apply_point.as_ref())?)?,
                capabilities: box_map(&p.capabilities, w, |k, _| read_capability(*k))?,
                activation: read_activation(req(p.activation.as_ref())?)?,
                target: decode_consumer_target(req(p.target.as_ref())?, w)?,
            })
        })?,
        policy: p::RuntimeFilterPolicy {
            max_contribution_bytes: policy.max_contribution_bytes,
            max_artifact_bytes: policy.max_artifact_bytes,
            deadline_ms: policy.deadline_ms,
            max_retries: policy.max_retries,
        },
    })
}
#[cfg(test)]
#[path = "physical_cuts_v2/tests.rs"]
mod tests;

#[cfg(test)]
mod summary_contract_tests {
    use super::*;
    #[test]
    fn exchange_payload_port_delivery_and_counter_presence_have_no_defaults() {
        assert!(raw_inbound_payload(None).is_err());
        assert!(raw_outbound_payload(None).is_err());
        for value in [0, -1, i32::MAX] {
            assert!(read_source_port(value).is_err());
            assert!(read_delivery(value).is_err());
        }
        let columns = wire::SummaryColumns {
            kind: 1,
            value_ids: vec![0, u32::MAX, 7],
        };
        assert_eq!(
            crate::physical_summary_v2::decode_columns_shape(&columns)
                .unwrap()
                .values(),
            [
                p::ValueId::new(0),
                p::ValueId::new(u32::MAX),
                p::ValueId::new(7)
            ]
        );
        assert!(
            crate::physical_summary_v2::decode_columns_shape(&wire::SummaryColumns {
                kind: 1,
                value_ids: vec![0, 7],
            })
            .is_err()
        );
        assert!(read_source_port(2).is_err());
        assert_eq!(
            read_source_port(3).unwrap(),
            p::FragmentOutputPort::Summary(p::SummaryKind::WriteCounts)
        );
        assert_eq!(
            read_source_port(4).unwrap(),
            p::FragmentOutputPort::Summary(p::SummaryKind::DeleteVerification)
        );
        struct Control;
        impl novarocks_type_contract::PureCompileControl for Control {
            fn checkpoint(
                &self,
                _: novarocks_type_contract::CompilePhase,
                _: u32,
            ) -> Result<(), CompileControlError> {
                Ok(())
            }
        }
        let mut work =
            CompileCheckpoints::try_new(&Control, novarocks_type_contract::CompilePhase::Decode)
                .unwrap();
        assert!(
            decode_summary(
                &wire::SummaryCut {
                    source: None,
                    destination: None
                },
                &mut work
            )
            .is_err()
        );
    }
}
