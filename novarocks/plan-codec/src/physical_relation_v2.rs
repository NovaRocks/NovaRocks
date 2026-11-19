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

//! Complete Relation vocabulary through original sealed read/type sources.
//! The read owner supplies the original control. TypeTable currently retains
//! original type sources but does NOT capture an original control; borrowing
//! it here is type authority, not a fabricated control-origin attestation.
//! Full Fragment reference/predicate/provider/property validation remains
//! mandatory. Source invoices cover all backing; checked floors are LOWER only.
use crate::{
    allocation_exit_v2::reserve_exit,
    binding_index_v2::BindingIndex,
    borrowed_type_resources::verify_type_binding,
    physical_binding_v2::BindingCodecError,
    physical_connector_payload_v2::{ConnectorPayloadCodecError, bytes_shared_upper},
    physical_properties_v2::{self, PhysicalPropertyCodecError, PhysicalPropertyProjectionLimits},
    physical_provider_read_v2::{
        DecodedProviderReads, EncodedProviderReads, ProviderReadCodecError,
    },
    physical_type_v2::{self, DecodedTypeTable, EncodedTypeTable, TypeCodecError},
};
use novarocks_physical_plan as p;
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};
use std::{alloc::Layout, fmt, mem::size_of};

#[derive(Clone, Copy)]
pub struct RelationSource<'a> {
    pub id: u32,
    pub relation: &'a p::Relation,
    pub value_type_ids: &'a [u32],
}
#[derive(Clone, Copy, Debug)]
pub struct RelationProjectionLimits {
    pub max_definitions: usize,
    pub max_schema_fields: usize,
    pub max_predicate_guarantees: usize,
    pub max_metadata_kind_bytes: usize,
    pub max_coverage_bytes: usize,
    pub max_allocation_requests: usize,
    pub max_allocation_request_bytes: usize,
    pub max_coexisting_source_and_request_bytes: usize,
    pub max_work: usize,
    pub properties: PhysicalPropertyProjectionLimits,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RelationProjectionFacts {
    pub definition_count: usize,
    pub schema_field_count: usize,
    pub predicate_guarantee_count: usize,
    pub metadata_kind_bytes: usize,
    pub coverage_bytes: usize,
    pub allocation_requests_upper_bound: usize,
    pub allocation_request_bytes_upper_bound: usize,
    pub coexisting_source_and_request_bytes_upper_bound: usize,
    pub cumulative_work_upper_bound: usize,
}
#[derive(Debug)]
pub enum RelationCodecError {
    Control(CompileControlError),
    Read(ProviderReadCodecError),
    Payload(ConnectorPayloadCodecError),
    Type(TypeCodecError),
    Properties(PhysicalPropertyCodecError),
    Identity(p::IdentityError),
    Index(BindingCodecError),
    InvalidShape(&'static str),
}
impl From<CompileControlError> for RelationCodecError {
    fn from(e: CompileControlError) -> Self {
        Self::Control(e)
    }
}
impl From<ProviderReadCodecError> for RelationCodecError {
    fn from(e: ProviderReadCodecError) -> Self {
        match e {
            ProviderReadCodecError::Control(c) => Self::Control(c),
            e => Self::Read(e),
        }
    }
}
impl From<ConnectorPayloadCodecError> for RelationCodecError {
    fn from(e: ConnectorPayloadCodecError) -> Self {
        match e {
            ConnectorPayloadCodecError::Control(c) => Self::Control(c),
            e => Self::Payload(e),
        }
    }
}
impl From<TypeCodecError> for RelationCodecError {
    fn from(e: TypeCodecError) -> Self {
        match e {
            TypeCodecError::Control(c) => Self::Control(c),
            e => Self::Type(e),
        }
    }
}
impl From<PhysicalPropertyCodecError> for RelationCodecError {
    fn from(e: PhysicalPropertyCodecError) -> Self {
        match e {
            PhysicalPropertyCodecError::Control(c) => Self::Control(c),
            e => Self::Properties(e),
        }
    }
}
impl From<BindingCodecError> for RelationCodecError {
    fn from(e: BindingCodecError) -> Self {
        match e {
            BindingCodecError::Control(c) => Self::Control(c),
            e => Self::Index(e),
        }
    }
}
impl fmt::Display for RelationCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(e) => e.fmt(f),
            Self::Read(e) => e.fmt(f),
            Self::Payload(e) => e.fmt(f),
            Self::Type(e) => e.fmt(f),
            Self::Properties(e) => e.fmt(f),
            Self::Identity(e) => e.fmt(f),
            Self::Index(e) => e.fmt(f),
            Self::InvalidShape(s) => f.write_str(s),
        }
    }
}
impl std::error::Error for RelationCodecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(source) => Some(source),
            Self::Read(source) => Some(source),
            Self::Payload(source) => Some(source),
            Self::Type(source) => Some(source),
            Self::Properties(source) => Some(source),
            Self::Identity(source) => Some(source),
            Self::Index(source) => Some(source),
            _ => None,
        }
    }
}
type Error = RelationCodecError;
fn invalid(s: &'static str) -> Error {
    Error::InvalidShape(s)
}
fn add(a: usize, b: usize) -> Result<usize, Error> {
    a.checked_add(b)
        .ok_or_else(|| invalid("relation resource sum overflow"))
}
fn mul(a: usize, b: usize) -> Result<usize, Error> {
    a.checked_mul(b)
        .ok_or_else(|| invalid("relation resource product overflow"))
}
fn bytes<T>(n: usize) -> Result<usize, Error> {
    Layout::array::<T>(n)
        .map(|v| v.size())
        .map_err(|_| invalid("relation allocation layout is unrepresentable"))
}
fn cap(n: usize, max: usize, w: &mut CompileCheckpoints<'_>) -> Result<(), Error> {
    let yes = n <= max;
    w.step()?;
    if yes {
        Ok(())
    } else {
        Err(invalid("relation projection envelope exceeded"))
    }
}
fn floor(source: usize, known: usize, w: &mut CompileCheckpoints<'_>) -> Result<(), Error> {
    let yes = source >= known;
    w.step()?;
    if yes {
        Ok(())
    } else {
        Err(invalid("relation source invoice omits original backing"))
    }
}
// Only directly owned structural backing is included. Shared Field/Bytes and
// other source owners remain in the caller's complete invoice. Distinct IDs
// can borrow the same relation, so the namespace uses the maximum individual
// footprint rather than charging these allocations per occurrence.
#[cfg(test)]
fn individual_relation_floor(
    relation: &p::Relation,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(usize, usize), Error> {
    individual_relation_floor_in(relation, &mut Model::default(), w)
}
fn individual_relation_floor_in(
    relation: &p::Relation,
    model: &mut Model<'_>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(usize, usize), Error> {
    let properties = relation.provided_properties();
    let keys = match &properties.distribution {
        p::Distribution::Hash { keys, .. }
        | p::Distribution::ConditionalKey { keys, .. }
        | p::Distribution::BucketShuffle { keys, .. } => keys.len(),
        p::Distribution::Unconstrained
        | p::Distribution::Singleton
        | p::Distribution::RoundRobin
        | p::Distribution::Broadcast => 0,
    };
    let mut known = add(
        size_of::<p::Relation>(),
        add(
            bytes::<p::RelationField>(relation.schema().len())?,
            bytes::<p::PredicateGuarantee>(relation.predicate_guarantees().len())?,
        )?,
    )?;
    known = add(
        known,
        add(
            bytes::<p::ValueId>(keys)?,
            bytes::<p::OrderingKey>(properties.ordering.len())?,
        )?,
    )?;
    if let p::Relation::Metadata(metadata) = relation {
        known = add(
            known,
            add(
                metadata.kind.as_str().len(),
                metadata.coverage_evidence.len(),
            )?,
        )?;
    }
    let mut extra_work = 0;
    for field in relation.schema() {
        // The same topology author describes the already retained root-owned
        // Dictionary Boxes here. FieldRef descendants remain shared; encoding
        // does not clone this source or add these bytes to output requests.
        let topology = if model.observed() {
            model.clone_prefix(&field.ty, w, false)?
        } else {
            physical_type_v2::preflight_value_type_clone(&field.ty, w)?
        };
        known = add(known, topology.allocation_request_bytes_upper_bound())?;
        extra_work = add(extra_work, topology.work_upper_bound())?;
        w.step()?;
        extra_work = add(extra_work, 1)?;
    }
    w.step()?;
    Ok((known, extra_work))
}
fn finish<T>(r: Result<T, Error>, w: CompileCheckpoints<'_>) -> Result<T, Error> {
    if matches!(&r, Err(Error::Control(_))) {
        return r;
    }
    w.finish()?;
    r
}
fn reserve<T>(n: usize, w: &mut CompileCheckpoints<'_>) -> Result<Vec<T>, Error> {
    bytes::<T>(n)?;
    w.flush()?;
    let mut v = Vec::new();
    let r = v.try_reserve_exact(n);
    reserve_exit::<Error>(r, w)?;
    Ok(v)
}
fn copy(v: &[u8], w: &mut CompileCheckpoints<'_>) -> Result<Vec<u8>, Error> {
    let mut out = reserve(v.len(), w)?;
    for c in v.chunks(1024) {
        out.extend_from_slice(c);
        w.step()?;
    }
    Ok(out)
}
fn boxed<T>(v: Vec<T>, w: &mut CompileCheckpoints<'_>) -> Result<Box<[T]>, Error> {
    w.flush()?;
    let result = v.into_boxed_slice();
    w.flush()?;
    Ok(result)
}
fn required(id: Option<u32>, w: &mut CompileCheckpoints<'_>) -> Result<u32, Error> {
    w.step()?;
    id.ok_or_else(|| invalid("relation required reference is absent"))
}
const fn encode_guarantee(v: p::PredicateGuaranteeKind) -> wire::PredicateGuaranteeKind {
    match v {
        p::PredicateGuaranteeKind::Exact => wire::PredicateGuaranteeKind::Exact,
        p::PredicateGuaranteeKind::PruningOnly => wire::PredicateGuaranteeKind::PruningOnly,
    }
}
fn decode_guarantee(v: i32) -> Result<p::PredicateGuaranteeKind, Error> {
    Ok(
        match wire::PredicateGuaranteeKind::try_from(v)
            .map_err(|_| invalid("unknown predicate guarantee kind"))?
        {
            wire::PredicateGuaranteeKind::Exact => p::PredicateGuaranteeKind::Exact,
            wire::PredicateGuaranteeKind::PruningOnly => p::PredicateGuaranteeKind::PruningOnly,
            wire::PredicateGuaranteeKind::Unspecified => {
                return Err(invalid("unspecified predicate guarantee kind"));
            }
        },
    )
}
#[derive(Default)]
struct Model<'a> {
    admission: Option<Admission<'a>>,
    fields: usize,
    guarantees: usize,
    kinds: usize,
    coverage: usize,
    requests: usize,
    bytes: usize,
    work: usize,
    base_work: usize,
}
type Admit<'a> = dyn FnMut(&RelationProjectionFacts) -> Result<(), CompileControlError> + 'a;
struct Admission<'a> {
    definitions: usize,
    source: usize,
    limits: RelationProjectionLimits,
    callback: &'a mut Admit<'a>,
}
impl Model<'_> {
    fn sum(&self, left: usize, right: usize) -> Result<usize, Error> {
        if self.observed() {
            left.checked_add(right)
                .ok_or_else(|| CompileControlError::ResourceExhausted.into())
        } else {
            add(left, right)
        }
    }
    fn product(&self, left: usize, right: usize) -> Result<usize, Error> {
        if self.observed() {
            left.checked_mul(right)
                .ok_or_else(|| CompileControlError::ResourceExhausted.into())
        } else {
            mul(left, right)
        }
    }
    fn buffer_bytes<T>(&self, count: usize) -> Result<usize, Error> {
        if self.observed() {
            Layout::array::<T>(count)
                .map(|layout| layout.size())
                .map_err(|_| Error::Control(CompileControlError::ResourceExhausted))
        } else {
            bytes::<T>(count)
        }
    }
    fn prefix(&mut self) -> Result<(), Error> {
        if let Some(admission) = self.admission.as_ref() {
            let facts = self.numerical_facts(admission.definitions, admission.source)?;
            let l = admission.limits;
            for (value, maximum) in [
                (facts.definition_count, l.max_definitions),
                (facts.schema_field_count, l.max_schema_fields),
                (facts.predicate_guarantee_count, l.max_predicate_guarantees),
                (facts.metadata_kind_bytes, l.max_metadata_kind_bytes),
                (facts.coverage_bytes, l.max_coverage_bytes),
                (
                    facts.allocation_requests_upper_bound,
                    l.max_allocation_requests,
                ),
                (
                    facts.allocation_request_bytes_upper_bound,
                    l.max_allocation_request_bytes,
                ),
                (
                    facts.coexisting_source_and_request_bytes_upper_bound,
                    l.max_coexisting_source_and_request_bytes,
                ),
                (facts.cumulative_work_upper_bound, l.max_work),
            ] {
                if value > maximum {
                    return Err(CompileControlError::ResourceExhausted.into());
                }
            }
            (self.admission.as_mut().expect("admission exists").callback)(&facts)?;
        }
        Ok(())
    }
    fn observed(&self) -> bool {
        self.admission.is_some()
    }
    fn numerical_facts(&self, n: usize, source: usize) -> Result<RelationProjectionFacts, Error> {
        Ok(RelationProjectionFacts {
            definition_count: n,
            schema_field_count: self.fields,
            predicate_guarantee_count: self.guarantees,
            metadata_kind_bytes: self.kinds,
            coverage_bytes: self.coverage,
            allocation_requests_upper_bound: self.requests,
            allocation_request_bytes_upper_bound: self.bytes,
            coexisting_source_and_request_bytes_upper_bound: self.sum(source, self.bytes)?,
            cumulative_work_upper_bound: self.sum(
                self.work,
                self.sum(self.product(self.bytes, 4)?, self.requests)?,
            )?,
        })
    }

    fn own_prefix(
        &mut self,
        n: usize,
        reads: usize,
        payloads: usize,
        types: usize,
    ) -> Result<(), Error> {
        if self.observed() {
            let next = own_work_mode(
                n,
                self.fields,
                self.guarantees,
                reads,
                payloads,
                types,
                true,
            )?;
            self.work = self.sum(
                self.work,
                next.checked_sub(self.base_work)
                    .ok_or_else(|| invalid("relation work prefix decreased"))?,
            )?;
            self.base_work = next;
            self.prefix()?;
        }
        Ok(())
    }
    fn clone_prefix(
        &mut self,
        ty: &novarocks_type_contract::FunctionValueType,
        w: &mut CompileCheckpoints<'_>,
        copying: bool,
    ) -> Result<physical_type_v2::ValueTypeCloneFacts, Error> {
        let mut previous = (0, 0, 0);
        physical_type_v2::preflight_value_type_clone_admitted(
            ty,
            &mut |facts, _| {
                let next = (
                    facts.allocation_requests_upper_bound(),
                    facts.allocation_request_bytes_upper_bound(),
                    facts.work_upper_bound(),
                );
                if copying {
                    self.requests = self.sum(
                        self.requests,
                        next.0
                            .checked_sub(previous.0)
                            .ok_or_else(|| invalid("relation clone requests decreased"))?,
                    )?;
                    self.bytes = self.sum(
                        self.bytes,
                        next.1
                            .checked_sub(previous.1)
                            .ok_or_else(|| invalid("relation clone bytes decreased"))?,
                    )?;
                }
                self.work = self.sum(
                    self.work,
                    self.product(
                        next.2
                            .checked_sub(previous.2)
                            .ok_or_else(|| invalid("relation clone work decreased"))?,
                        if copying { 2 } else { 1 },
                    )?,
                )?;
                previous = next;
                self.prefix()
            },
            w,
        )
    }
    fn request(&mut self, n: usize) -> Result<(), Error> {
        if n != 0 {
            self.requests = self.sum(self.requests, 1)?;
            self.bytes = self.sum(self.bytes, n)?;
        }
        self.prefix()
    }
    fn twice<T>(&mut self, n: usize) -> Result<(), Error> {
        self.request(self.buffer_bytes::<T>(n)?)?;
        self.request(self.buffer_bytes::<T>(n)?)?;
        Ok(())
    }
    fn properties(
        &mut self,
        f: physical_properties_v2::PhysicalPropertyProjectionFacts,
    ) -> Result<(), Error> {
        if let Some(admission) = self.admission.as_ref() {
            let l = admission.limits.properties;
            for (value, maximum) in [
                (f.value_reference_count, l.max_value_references),
                (f.allocation_requests_upper_bound, l.max_allocation_requests),
                (
                    f.allocation_request_bytes_upper_bound,
                    l.max_allocation_request_bytes,
                ),
                (
                    f.coexisting_source_and_request_bytes_upper_bound,
                    l.max_coexisting_source_and_request_bytes,
                ),
                (f.cumulative_work_upper_bound, l.max_work),
            ] {
                if value > maximum {
                    return Err(CompileControlError::ResourceExhausted.into());
                }
            }
        }
        self.requests = self.sum(self.requests, f.allocation_requests_upper_bound)?;
        self.bytes = self.sum(self.bytes, f.allocation_request_bytes_upper_bound)?;
        self.work = self.sum(self.work, self.product(f.cumulative_work_upper_bound, 2)?)?;
        self.prefix()
    }
    fn gate(
        &self,
        n: usize,
        source: usize,
        l: RelationProjectionLimits,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<RelationProjectionFacts, Error> {
        if self.observed() {
            // All known axes have already been synchronously admitted. These
            // are the original completed facts checks, not another scope.
            for _ in 0..8 {
                w.step()?;
            }
            return self.numerical_facts(n, source);
        }
        cap(self.fields, l.max_schema_fields, w)?;
        cap(self.guarantees, l.max_predicate_guarantees, w)?;
        cap(self.kinds, l.max_metadata_kind_bytes, w)?;
        cap(self.coverage, l.max_coverage_bytes, w)?;
        cap(self.requests, l.max_allocation_requests, w)?;
        cap(self.bytes, l.max_allocation_request_bytes, w)?;
        let coexist = self.sum(source, self.bytes)?;
        cap(coexist, l.max_coexisting_source_and_request_bytes, w)?;
        let work = self.sum(
            self.work,
            self.sum(self.product(self.bytes, 4)?, self.requests)?,
        )?;
        cap(work, l.max_work, w)?;
        Ok(RelationProjectionFacts {
            definition_count: n,
            schema_field_count: self.fields,
            predicate_guarantee_count: self.guarantees,
            metadata_kind_bytes: self.kinds,
            coverage_bytes: self.coverage,
            allocation_requests_upper_bound: self.requests,
            allocation_request_bytes_upper_bound: self.bytes,
            coexisting_source_and_request_bytes_upper_bound: coexist,
            cumulative_work_upper_bound: work,
        })
    }
}
fn own_work(
    n: usize,
    fields: usize,
    guarantees: usize,
    read_count: usize,
    payload_count: usize,
    type_count: usize,
) -> Result<usize, Error> {
    own_work_mode(
        n,
        fields,
        guarantees,
        read_count,
        payload_count,
        type_count,
        false,
    )
}
fn own_work_mode(
    n: usize,
    fields: usize,
    guarantees: usize,
    read_count: usize,
    payload_count: usize,
    type_count: usize,
    observed: bool,
) -> Result<usize, Error> {
    let sum = |left: usize, right: usize| {
        if observed {
            left.checked_add(right)
                .ok_or_else(|| CompileControlError::ResourceExhausted.into())
        } else {
            add(left, right)
        }
    };
    let product = |left: usize, right: usize| {
        if observed {
            left.checked_mul(right)
                .ok_or_else(|| CompileControlError::ResourceExhausted.into())
        } else {
            mul(left, right)
        }
    };
    let h = (usize::BITS - n.leading_zeros()) as usize;
    // Two source/ref lookup passes (preflight + emission), prior read floor,
    // and the sole index sort. Linear source-count bounds also conservatively
    // cover opaque decoded BTreeMap lookups; no inside-library quantum claim.
    sum(
        1024,
        sum(
            product(
                n,
                sum(128, sum(product(h + 1, 16)?, product(read_count, 4)?)?)?,
            )?,
            sum(
                product(
                    fields,
                    sum(128, product(sum(payload_count, type_count)?, 4)?)?,
                )?,
                sum(product(guarantees, 16)?, product(read_count, 16)?)?,
            )?,
        )?,
    )
}

pub struct EncodedRelations<'loan, 'source, 'control> {
    inputs: &'loan [RelationSource<'source>],
    reads: &'loan EncodedProviderReads<'source, 'control>,
    types: &'loan EncodedTypeTable<'source>,
    wire: Vec<wire::RelationDefinition>,
    indices: BindingIndex,
    facts: RelationProjectionFacts,
    original_source_bytes: usize,
}
impl<'loan, 'source, 'control> EncodedRelations<'loan, 'source, 'control> {
    pub fn as_wire(&self) -> &[wire::RelationDefinition] {
        &self.wire
    }
    pub fn into_wire(self) -> Vec<wire::RelationDefinition> {
        self.wire
    }
    pub fn facts(&self) -> &RelationProjectionFacts {
        &self.facts
    }
    pub fn source_count(&self) -> usize {
        self.inputs.len()
    }
    pub fn reads(&self) -> &EncodedProviderReads<'source, 'control> {
        self.reads
    }
    pub fn types(&self) -> &EncodedTypeTable<'source> {
        self.types
    }
    pub(crate) fn original_control(&self) -> &'control dyn PureCompileControl {
        self.reads.original_control()
    }
    pub fn relation(&self, id: u32) -> Result<Option<&'source p::Relation>, Error> {
        let mut w = CompileCheckpoints::try_new(self.original_control(), CompilePhase::Encode)?;
        let r = self.relation_observed(id, &mut w);
        finish(r, w)
    }
    pub fn relation_in(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&'source p::Relation>, Error> {
        check_control(self.original_control(), work)?;
        self.relation_observed(id, work)
    }
    pub fn source_id_in(
        &self,
        source: &p::Relation,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<u32, Error> {
        check_control(self.original_control(), work)?;
        self.source_id_observed(source, work)
    }
    pub fn retained_invoice_floor_in(
        &self,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<usize, Error> {
        check_control(self.original_control(), work)?;
        self.retained_floor_observed(work)
    }
    pub(crate) fn relation_observed(
        &self,
        id: u32,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&'source p::Relation>, Error> {
        Ok(self
            .indices
            .find(id, |at| self.inputs[at].id, w)?
            .map(|at| self.inputs[at].relation))
    }
    pub fn source_id(&self, source: &p::Relation) -> Result<u32, Error> {
        let mut w = CompileCheckpoints::try_new(self.original_control(), CompilePhase::Encode)?;
        let r = self.source_id_observed(source, &mut w);
        finish(r, w)
    }
    pub(crate) fn source_id_observed(
        &self,
        source: &p::Relation,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<u32, Error> {
        let mut found = None;
        for input in self.inputs {
            let same = std::ptr::eq(input.relation, source);
            w.step()?;
            if same {
                if found.is_some() {
                    return Err(invalid("relation source association is ambiguous"));
                }
                found = Some(input.id);
            }
        }
        found.ok_or_else(|| invalid("relation source owner is not in this namespace"))
    }
    pub fn retained_invoice_floor(&self) -> Result<usize, Error> {
        let mut w = CompileCheckpoints::try_new(self.original_control(), CompilePhase::Encode)?;
        let r = self.retained_floor_observed(&mut w);
        finish(r, w)
    }
    pub(crate) fn retained_floor_observed(
        &self,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<usize, Error> {
        let mut n = add(
            self.original_source_bytes,
            add(
                size_of::<Self>(),
                add(
                    self.indices.backing_bytes()?,
                    bytes::<wire::RelationDefinition>(self.wire.capacity())?,
                )?,
            )?,
        )?;
        w.step()?;
        for def in &self.wire {
            let raw = raw(def, w)?;
            n = add(n, raw.known_backing()?)?;
            w.step()?;
        }
        Ok(n)
    }
}
pub struct DecodedRelations<'loan, 'wire, 'control> {
    wire: &'wire [wire::RelationDefinition],
    reads: &'loan DecodedProviderReads<'wire, 'control>,
    types: &'loan DecodedTypeTable,
    relations: Vec<p::Relation>,
    indices: BindingIndex,
    facts: RelationProjectionFacts,
    original_source_bytes: usize,
}
impl<'loan, 'wire, 'control> DecodedRelations<'loan, 'wire, 'control> {
    pub fn as_wire(&self) -> &'wire [wire::RelationDefinition] {
        self.wire
    }
    pub fn facts(&self) -> &RelationProjectionFacts {
        &self.facts
    }
    pub fn source_count(&self) -> usize {
        self.wire.len()
    }
    pub fn reads(&self) -> &DecodedProviderReads<'wire, 'control> {
        self.reads
    }
    pub fn types(&self) -> &DecodedTypeTable {
        self.types
    }
    pub(crate) fn original_control(&self) -> &'control dyn PureCompileControl {
        self.reads.original_control()
    }
    pub fn relation(&self, id: u32) -> Result<Option<&p::Relation>, Error> {
        let mut w = CompileCheckpoints::try_new(self.original_control(), CompilePhase::Decode)?;
        let r = self.relation_observed(id, &mut w);
        finish(r, w)
    }
    pub fn relation_in(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&p::Relation>, Error> {
        check_control(self.original_control(), work)?;
        self.relation_observed(id, work)
    }
    pub fn retained_invoice_floor_in(
        &self,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<usize, Error> {
        check_control(self.original_control(), work)?;
        self.retained_floor_observed(work)
    }
    pub(crate) fn relation_observed(
        &self,
        id: u32,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&p::Relation>, Error> {
        Ok(self
            .indices
            .find(id, |at| self.wire[at].id, w)?
            .map(|at| &self.relations[at]))
    }
    /// Actual structural lower floor. Shared Field/Bytes backing and property
    /// internals are still covered by the complete caller source invoice.
    pub fn retained_invoice_floor(&self) -> Result<usize, Error> {
        let mut w = CompileCheckpoints::try_new(self.original_control(), CompilePhase::Decode)?;
        let result = self.retained_floor_observed(&mut w);
        finish(result, w)
    }
    pub(crate) fn retained_floor_observed(
        &self,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<usize, Error> {
        let mut n = add(
            self.original_source_bytes,
            add(
                size_of::<Self>(),
                add(
                    self.indices.backing_bytes()?,
                    bytes::<p::Relation>(self.relations.capacity())?,
                )?,
            )?,
        )?;
        w.step()?;
        for relation in &self.relations {
            n = add(
                n,
                add(
                    bytes::<p::RelationField>(relation.schema().len())?,
                    bytes::<p::PredicateGuarantee>(relation.predicate_guarantees().len())?,
                )?,
            )?;
            if let p::Relation::Metadata(v) = relation {
                n = add(n, add(v.kind.as_str().len(), v.coverage_evidence.len())?)?;
            }
            w.step()?;
        }
        Ok(n)
    }
}
struct Raw<'a> {
    read: Option<u32>,
    digest: &'a Vec<u8>,
    schema: &'a [wire::RelationField],
    schema_capacity: usize,
    guarantees: &'a [wire::PredicateGuarantee],
    guarantee_capacity: usize,
    properties: &'a Option<wire::PhysicalProperties>,
    metadata: Option<(&'a String, &'a Vec<u8>)>,
}
impl Raw<'_> {
    fn known_backing(&self) -> Result<usize, Error> {
        let mut n = add(
            self.digest.capacity(),
            add(
                bytes::<wire::RelationField>(self.schema_capacity)?,
                bytes::<wire::PredicateGuarantee>(self.guarantee_capacity)?,
            )?,
        )?;
        if let Some((kind, coverage)) = self.metadata {
            n = add(n, add(kind.capacity(), coverage.capacity())?)?;
        }
        Ok(n)
    }
}
fn raw<'a>(
    d: &'a wire::RelationDefinition,
    w: &mut CompileCheckpoints<'_>,
) -> Result<Raw<'a>, Error> {
    let result = raw_source(d);
    w.step()?;
    result
}
fn raw_source(d: &wire::RelationDefinition) -> Result<Raw<'_>, Error> {
    let k = d
        .kind
        .as_ref()
        .ok_or_else(|| invalid("relation kind is absent"))?;
    Ok(match k {
        wire::relation_definition::Kind::Data(v) => Raw {
            read: v.read_reference_id,
            digest: &v.selection_digest,
            schema: &v.schema,
            schema_capacity: v.schema.capacity(),
            guarantees: &v.predicate_guarantees,
            guarantee_capacity: v.predicate_guarantees.capacity(),
            properties: &v.provided_properties,
            metadata: None,
        },
        wire::relation_definition::Kind::Metadata(v) => Raw {
            read: v.read_reference_id,
            digest: &v.selection_digest,
            schema: &v.schema,
            schema_capacity: v.schema.capacity(),
            guarantees: &v.predicate_guarantees,
            guarantee_capacity: v.predicate_guarantees.capacity(),
            properties: &v.provided_properties,
            metadata: Some((&v.kind, &v.coverage_evidence)),
        },
    })
}
fn properties<'a>(
    r: &Raw<'a>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<&'a wire::PhysicalProperties, Error> {
    let p = r.properties.as_ref();
    w.step()?;
    p.ok_or_else(|| invalid("relation provided properties are absent"))
}
fn typed<'a>(
    types: &'a DecodedTypeTable,
    id: u32,
    w: &mut CompileCheckpoints<'_>,
) -> Result<&'a p::ValueType, Error> {
    w.flush()?;
    let t = types.value_type(id);
    w.flush()?;
    t.ok_or_else(|| invalid("relation value type ID is unknown"))
}

fn check_control(
    original: &dyn PureCompileControl,
    work: &CompileCheckpoints<'_>,
) -> Result<(), Error> {
    // The same owner may have distinct trait vtables in different codegen
    // units. Its retained object address, not the vtable address, is identity.
    if std::ptr::addr_eq(original, work.control()) {
        Ok(())
    } else {
        Err(invalid("relation control loan differs"))
    }
}
/// Compose the original relation author in the caller's scope. Each callback
/// replaces this component's previous cumulative facts; source is the complete
/// original union once. Allocation/host execution admission remains external.
pub fn encode_relations_in<'loan, 'source, 'control>(
    inputs: &'loan [RelationSource<'source>],
    reads: &'loan EncodedProviderReads<'source, 'control>,
    types: &'loan EncodedTypeTable<'source>,
    source: usize,
    limits: RelationProjectionLimits,
    admit: &mut impl FnMut(&RelationProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<EncodedRelations<'loan, 'source, 'control>, Error> {
    if !std::ptr::addr_eq(work.control(), reads.original_control()) {
        return Err(invalid("relation control loan differs"));
    }
    encode_core(inputs, reads, types, source, limits, Some(admit), work)
}
/// Receiving uses the same mapper and checked namespaces, without a private
/// entry/footer, guessed provider facts, or reconstruction of type authority.
pub fn decode_relations_in<'loan, 'wire, 'control>(
    definitions: &'wire [wire::RelationDefinition],
    reads: &'loan DecodedProviderReads<'wire, 'control>,
    types: &'loan DecodedTypeTable,
    source: usize,
    limits: RelationProjectionLimits,
    admit: &mut impl FnMut(&RelationProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DecodedRelations<'loan, 'wire, 'control>, Error> {
    if !std::ptr::addr_eq(work.control(), reads.original_control()) {
        return Err(invalid("relation control loan differs"));
    }
    decode_core(definitions, reads, types, source, limits, Some(admit), work)
}
pub fn encode_relations<'loan, 'source, 'control>(
    inputs: &'loan [RelationSource<'source>],
    reads: &'loan EncodedProviderReads<'source, 'control>,
    types: &'loan EncodedTypeTable<'source>,
    source: usize,
    limits: RelationProjectionLimits,
) -> Result<EncodedRelations<'loan, 'source, 'control>, Error> {
    let mut w = CompileCheckpoints::try_new(reads.original_control(), CompilePhase::Encode)?;
    let r = encode_core(inputs, reads, types, source, limits, None, &mut w);
    finish(r, w)
}
fn encode_core<'loan, 'source, 'control, 'admit>(
    inputs: &'loan [RelationSource<'source>],
    reads: &'loan EncodedProviderReads<'source, 'control>,
    types: &'loan EncodedTypeTable<'source>,
    source: usize,
    l: RelationProjectionLimits,
    admit: Option<&'admit mut Admit<'admit>>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<EncodedRelations<'loan, 'source, 'control>, Error> {
    let mut m = Model::default();
    if let Some(callback) = admit {
        m.admission = Some(Admission {
            definitions: inputs.len(),
            source,
            limits: l,
            callback,
        });
        m.own_prefix(
            inputs.len(),
            reads.source_count(),
            reads.payloads().source_count(),
            types.source_counts().0,
        )?;
        m.prefix()?;
    }
    let root = bytes::<RelationSource<'_>>(inputs.len())?;
    let mut largest_ids = 0;
    let mut largest_relation = 0;
    m.request(m.buffer_bytes::<usize>(inputs.len())?)?;
    m.request(m.buffer_bytes::<wire::RelationDefinition>(inputs.len())?)?;
    cap(inputs.len(), l.max_definitions, w)?;
    for input in inputs {
        let r = input.relation;
        let valid = r.schema().len() == input.value_type_ids.len();
        if !m.observed() || !valid {
            w.step()?;
        }
        if !valid {
            return Err(invalid("relation schema type ID count differs"));
        }
        m.fields = m.sum(m.fields, r.schema().len())?;
        m.guarantees = m.sum(m.guarantees, r.predicate_guarantees().len())?;
        // Different root entries may borrow the SAME ID slice. This is a
        // known lower floor, not a per-occurrence source backing charge.
        largest_ids = largest_ids.max(bytes::<u32>(input.value_type_ids.len())?);
        m.request(m.buffer_bytes::<wire::RelationField>(r.schema().len())?)?;
        m.request(m.buffer_bytes::<wire::PredicateGuarantee>(r.predicate_guarantees().len())?)?;
        m.request(32)?;
        if let p::Relation::Metadata(v) = r {
            m.kinds = m.sum(m.kinds, v.kind.as_str().len())?;
            m.coverage = m.sum(m.coverage, v.coverage_evidence.len())?;
            m.request(m.buffer_bytes::<u8>(v.kind.as_str().len())?)?;
            m.request(m.buffer_bytes::<u8>(v.coverage_evidence.len())?)?;
        }
        if m.observed() {
            m.properties(physical_properties_v2::properties_encode_numerical_facts(
                r.provided_properties(),
                source,
            )?)?;
        }
        m.own_prefix(
            inputs.len(),
            reads.source_count(),
            reads.payloads().source_count(),
            types.source_counts().0,
        )?;
        if m.observed() {
            w.step()?;
        }
        let (known_relation, extra_work) = individual_relation_floor_in(r, &mut m, w)?;
        largest_relation = largest_relation.max(known_relation);
        if !m.observed() {
            m.work = m.sum(m.work, extra_work)?;
        }
        m.prefix()?;
        w.step()?;
    }
    if !m.observed() {
        m.work = m.sum(
            m.work,
            own_work(
                inputs.len(),
                m.fields,
                m.guarantees,
                reads.source_count(),
                reads.payloads().source_count(),
                types.source_counts().0,
            )?,
        )?;
    }
    m.prefix()?;
    cap(m.work, l.max_work, w)?;
    floor(source, add(root, largest_ids)?, w)?;
    floor(source, largest_relation, w)?;
    floor(source, reads.retained_floor_observed(w)?, w)?;
    for input in inputs {
        let r = input.relation;
        reads.source_id_observed(r.read(), w)?;
        for (field, id) in r.schema().iter().zip(input.value_type_ids) {
            reads
                .payloads()
                .source_id_observed(&field.column.column_payload, w)?;
            let ty = types
                .value_type_observed(*id, w)?
                .ok_or_else(|| invalid("relation value type ID is unknown"))?;
            let checked = if m.observed() {
                let mut previous = 0usize;
                crate::borrowed_type_resources::verify_type_binding_admitted(
                    &field.ty,
                    ty,
                    source,
                    l.max_work,
                    &mut |facts| {
                        let next = facts.work_upper_bound();
                        m.work = m.sum(
                            m.work,
                            next.checked_sub(previous)
                                .ok_or_else(|| invalid("relation comparison work decreased"))?,
                        )?;
                        previous = next;
                        m.prefix()
                    },
                    w,
                )?
            } else {
                let checked = verify_type_binding(
                    &field.ty,
                    ty,
                    source,
                    l.max_work
                        .checked_sub(m.work)
                        .ok_or_else(|| invalid("relation work envelope exhausted"))?,
                    w,
                )?;
                m.work = m.sum(m.work, checked.work_upper_bound())?;
                checked
            };
            if !checked.matches() {
                return Err(invalid("relation full source value type differs"));
            }
            w.step()?;
        }
        let pf = physical_properties_v2::preflight_encode_observed(
            r.provided_properties(),
            source,
            l.properties,
            w,
        )?;
        if !m.observed() {
            m.properties(pf)?;
        }
        cap(m.work, l.max_work, w)?;
        w.step()?;
    }
    let facts = m.gate(inputs.len(), source, l, w)?;
    let indices = BindingIndex::prepare(inputs.len(), |at| inputs[at].id, w)?;
    let mut output = reserve(inputs.len(), w)?;
    for input in inputs {
        let r = input.relation;
        let read_id = reads.source_id_observed(r.read(), w)?;
        let mut schema = reserve(r.schema().len(), w)?;
        for (f, id) in r.schema().iter().zip(input.value_type_ids) {
            schema.push(wire::RelationField {
                column_payload_id: Some(
                    reads
                        .payloads()
                        .source_id_observed(&f.column.column_payload, w)?,
                ),
                value_type_id: Some(*id),
            });
            w.step()?;
        }
        let mut guarantees = reserve(r.predicate_guarantees().len(), w)?;
        for g in r.predicate_guarantees() {
            guarantees.push(wire::PredicateGuarantee {
                predicate_root: Some(g.predicate.get()),
                kind: encode_guarantee(g.kind) as i32,
            });
            w.step()?;
        }
        let (props, _) = physical_properties_v2::encode_observed(
            r.provided_properties(),
            source,
            l.properties,
            w,
        )?;
        let digest = copy(&r.selection_digest(), w)?;
        let kind = match r {
            p::Relation::Data(_) => wire::relation_definition::Kind::Data(wire::DataRelation {
                read_reference_id: Some(read_id),

                selection_digest: digest,
                schema,
                predicate_guarantees: guarantees,
                provided_properties: Some(props),
            }),
            p::Relation::Metadata(v) => {
                let bytes = copy(v.kind.as_str().as_bytes(), w)?;
                w.flush()?;
                let kind = String::from_utf8(bytes);
                w.flush()?;
                let kind = kind.map_err(|_| invalid("authored metadata kind is not UTF-8"))?;
                wire::relation_definition::Kind::Metadata(wire::MetadataRelation {
                    kind,
                    read_reference_id: Some(read_id),

                    selection_digest: digest,
                    schema,
                    predicate_guarantees: guarantees,
                    provided_properties: Some(props),
                    coverage_evidence: copy(&v.coverage_evidence, w)?,
                })
            }
        };
        output.push(wire::RelationDefinition {
            id: input.id,
            kind: Some(kind),
        });
        w.step()?;
    }
    Ok(EncodedRelations {
        inputs,
        reads,
        types,
        wire: output,
        indices,
        facts,
        original_source_bytes: source,
    })
}
pub fn decode_relations<'loan, 'wire, 'control>(
    defs: &'wire [wire::RelationDefinition],
    reads: &'loan DecodedProviderReads<'wire, 'control>,
    types: &'loan DecodedTypeTable,
    source: usize,
    limits: RelationProjectionLimits,
) -> Result<DecodedRelations<'loan, 'wire, 'control>, Error> {
    let mut w = CompileCheckpoints::try_new(reads.original_control(), CompilePhase::Decode)?;
    let r = decode_core(defs, reads, types, source, limits, None, &mut w);
    finish(r, w)
}
fn preflight_decode_observed<'admit>(
    defs: &[wire::RelationDefinition],
    reads: &DecodedProviderReads<'_, '_>,
    types: &DecodedTypeTable,
    source: usize,
    l: RelationProjectionLimits,
    admit: Option<&'admit mut Admit<'admit>>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<RelationProjectionFacts, Error> {
    let observed = admit.is_some();
    if observed && defs.len() > l.max_definitions {
        return Err(CompileControlError::ResourceExhausted.into());
    }
    let mut m = Model::default();
    if let Some(callback) = admit {
        m.admission = Some(Admission {
            definitions: defs.len(),
            source,
            limits: l,
            callback,
        });
        m.own_prefix(
            defs.len(),
            reads.source_count(),
            reads.payloads().source_count(),
            types.value_types().len(),
        )?;
    }
    let mut known = bytes::<wire::RelationDefinition>(defs.len())?;
    m.request(m.buffer_bytes::<usize>(defs.len())?)?;
    m.request(m.buffer_bytes::<p::Relation>(defs.len())?)?;
    cap(defs.len(), l.max_definitions, w)?;
    for def in defs {
        let result = raw_source(def);
        if !m.observed() {
            w.step()?;
        }
        let r = result?;
        m.fields = m.sum(m.fields, r.schema.len())?;
        m.guarantees = m.sum(m.guarantees, r.guarantees.len())?;
        known = add(known, r.known_backing()?)?;
        m.twice::<p::RelationField>(r.schema.len())?;
        m.twice::<p::PredicateGuarantee>(r.guarantees.len())?;
        if m.observed() {
            // Promotable Bytes sharing is bounded before namespace lookup:
            // each column plus table/view can request at most one Shared.
            // Empty payloads need no request, but this conservative bound
            // avoids moving an already-known ceiling past a lookup callback.
            for _ in 0..m.sum(r.schema.len(), 2)? {
                m.request(bytes_shared_upper()?)?;
            }
        }
        if let Some((kind, cov)) = r.metadata {
            m.kinds = m.sum(m.kinds, kind.len())?;
            m.coverage = m.sum(m.coverage, cov.len())?;
            m.request(m.buffer_bytes::<u8>(kind.len())?)?;
            m.twice::<u8>(cov.len())?;
        }
        m.own_prefix(
            defs.len(),
            reads.source_count(),
            reads.payloads().source_count(),
            types.value_types().len(),
        )?;
        if m.observed() {
            if let Some(props) = r.properties.as_ref() {
                m.properties(physical_properties_v2::properties_decode_numerical_facts(
                    props, source,
                )?)?;
            }
            for field in r.schema {
                if let Some(id) = field.value_type_id {
                    // std BTree lookup is already covered by own_prefix. A
                    // captured type's requests are admitted before its first
                    // clone-preflight callback, never after lookup completion.
                    w.flush()?;
                    let found = types.value_type(id);
                    if let Some(ty) = found {
                        m.clone_prefix(ty, w, true)?;
                    }
                    w.flush()?;
                }
            }
            w.step()?;
        }
        w.step()?;
    }
    if !m.observed() {
        m.work = own_work(
            defs.len(),
            m.fields,
            m.guarantees,
            reads.source_count(),
            reads.payloads().source_count(),
            types.value_types().len(),
        )?;
    }
    m.prefix()?;
    cap(m.work, l.max_work, w)?;
    floor(source, known, w)?;
    floor(source, reads.retained_floor_observed(w)?, w)?;
    for def in defs {
        let r = raw(def, w)?;
        let id = required(r.read, w)?;
        let read = reads
            .read_observed(id, w)?
            .ok_or_else(|| invalid("relation read reference ID is unknown"))?;
        let valid = r.digest.len() == 32;
        w.step()?;
        if !valid {
            return Err(invalid("relation selection digest is not exactly 32 bytes"));
        }
        for f in r.schema {
            let id = required(f.column_payload_id, w)?;
            let payload = reads
                .payloads()
                .payload_observed(id, w)?
                .ok_or_else(|| invalid("relation column payload ID is unknown"))?;
            if !m.observed() && !payload.payload().is_empty() {
                m.request(bytes_shared_upper()?)?;
            }
            let id = required(f.value_type_id, w)?;
            let ty = typed(types, id, w)?;
            if !m.observed() {
                let clone = physical_type_v2::preflight_value_type_clone(ty, w)?;
                m.requests = m.sum(m.requests, clone.allocation_requests_upper_bound())?;
                m.bytes = m.sum(m.bytes, clone.allocation_request_bytes_upper_bound())?;
                m.work = m.sum(m.work, mul(clone.work_upper_bound(), 2)?)?;
            }
            w.step()?;
        }
        // Cloning a checked read shares its version Arc and binding Arcs. Only
        // two promotable Bytes clones can request new Shared blocks.
        for payload in [read.relation.table(), read.relation.view()] {
            if !m.observed() && !payload.payload().is_empty() {
                m.request(bytes_shared_upper()?)?;
            }
            w.step()?;
        }
        for g in r.guarantees {
            required(g.predicate_root, w)?;
            let kind = decode_guarantee(g.kind);
            w.step()?;
            kind?;
        }
        let pf = physical_properties_v2::preflight_decode_observed(
            properties(&r, w)?,
            source,
            l.properties,
            w,
        )?;
        if !m.observed() {
            m.properties(pf)?;
        }
        cap(m.work, l.max_work, w)?;
        w.step()?;
    }
    m.gate(defs.len(), source, l, w)
}
fn decode_core<'loan, 'wire, 'control, 'admit>(
    defs: &'wire [wire::RelationDefinition],
    reads: &'loan DecodedProviderReads<'wire, 'control>,
    types: &'loan DecodedTypeTable,
    source: usize,
    l: RelationProjectionLimits,
    admit: Option<&'admit mut Admit<'admit>>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<DecodedRelations<'loan, 'wire, 'control>, Error> {
    let facts = preflight_decode_observed(defs, reads, types, source, l, admit, w)?;
    let indices = BindingIndex::prepare(defs.len(), |at| defs[at].id, w)?;
    let mut output = reserve(defs.len(), w)?;
    for def in defs {
        let r = raw(def, w)?;
        let read = reads
            .read_observed(required(r.read, w)?, w)?
            .ok_or_else(|| invalid("relation read reference ID is unknown"))?;
        let digest: [u8; 32] = r
            .digest
            .as_slice()
            .try_into()
            .map_err(|_| invalid("relation selection digest is not exactly 32 bytes"))?;
        let mut schema = reserve(r.schema.len(), w)?;
        for f in r.schema {
            let column = reads
                .payloads()
                .payload_observed(required(f.column_payload_id, w)?, w)?
                .ok_or_else(|| invalid("relation column payload ID is unknown"))?;
            let ty = physical_type_v2::clone_value_type_observed(
                typed(types, required(f.value_type_id, w)?, w)?,
                w,
            )?;
            w.flush()?;
            let column = p::ProviderColumnReference {
                column_payload: column.clone(),
            };
            w.flush()?;
            schema.push(p::RelationField { column, ty });
            w.step()?;
        }
        let schema = boxed(schema, w)?;
        let mut guarantees = reserve(r.guarantees.len(), w)?;
        for g in r.guarantees {
            guarantees.push(p::PredicateGuarantee {
                predicate: novarocks_expression_contract::RootOrdinal::new(required(
                    g.predicate_root,
                    w,
                )?),
                kind: decode_guarantee(g.kind)?,
            });
            w.step()?;
        }
        let guarantees = boxed(guarantees, w)?;
        let (provided_properties, _) =
            physical_properties_v2::decode_observed(properties(&r, w)?, source, l.properties, w)?;
        w.flush()?;
        let read = read.clone();
        w.flush()?;
        let relation = if let Some((kind, cov)) = r.metadata {
            w.flush()?;
            let result = p::MetadataRelationKind::try_new(kind);
            w.flush()?;
            let kind = result.map_err(Error::Identity)?;
            p::Relation::Metadata(p::MetadataRelation {
                kind,
                read,

                selection_digest: digest,
                schema,
                predicate_guarantees: guarantees,
                provided_properties,
                coverage_evidence: boxed(copy(cov, w)?, w)?,
            })
        } else {
            p::Relation::Data(p::DataRelation {
                read,

                selection_digest: digest,
                schema,
                predicate_guarantees: guarantees,
                provided_properties,
            })
        };
        output.push(relation);
        w.step()?;
    }
    Ok(DecodedRelations {
        wire: defs,
        reads,
        types,
        relations: output,
        indices,
        facts,
        original_source_bytes: source,
    })
}
#[cfg(test)]
mod tests;

mod materialization;
pub(crate) use materialization::prepare_observed as prepare_relation_materialization_observed;
pub use materialization::{
    PreparedRelationMaterialization, materialize_relation, prepare_relation_materialization,
    prepare_relation_materialization_in,
};
