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

//! Complete writer schema/target-field representation through the original
//! checked type roots. IDs and writer semantics remain Fragment-owned. Types
//! do not retain control provenance; the caller supplies its original control.

use crate::{
    borrowed_type_resources::{
        preflight_type_binding, preflight_type_binding_admitted_in,
        type_binding_prefix_work_upper_bound_in, verify_type_binding,
        verify_type_binding_admitted_in,
    },
    physical_node_v2::{self as resources, Model, NodeProjectionLimits},
    physical_properties_v2::PhysicalPropertyProjectionLimits,
    physical_type_v2::{self, DecodedTypeTable, EncodedTypeTable, TypeCodecError},
};
use novarocks_connector_contract::ConnectorWriteFieldToken;
use novarocks_physical_plan as p;
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
};
use std::{fmt, mem::size_of};

#[derive(Clone, Copy, Debug)]
pub struct WriterSchemaProjectionLimits {
    pub max_fields: usize,
    pub max_name_bytes: usize,
    pub max_type_references: usize,
    pub max_allocation_requests: usize,
    pub max_allocation_request_bytes: usize,
    pub max_coexisting_source_and_request_bytes: usize,
    pub max_work: usize,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WriterSchemaProjectionFacts {
    pub field_count: usize,
    pub name_bytes: usize,
    pub type_reference_count: usize,
    pub allocation_requests_upper_bound: usize,
    pub allocation_request_bytes_upper_bound: usize,
    pub coexisting_source_and_request_bytes_upper_bound: usize,
    pub cumulative_work_upper_bound: usize,
}
#[derive(Debug)]
pub enum WriterSchemaCodecError {
    Control(CompileControlError),
    Type(TypeCodecError),
    Resources(resources::NodeCodecError),
    InvalidShape(&'static str),
}
impl From<CompileControlError> for WriterSchemaCodecError {
    fn from(e: CompileControlError) -> Self {
        Self::Control(e)
    }
}
impl From<TypeCodecError> for WriterSchemaCodecError {
    fn from(e: TypeCodecError) -> Self {
        match e {
            TypeCodecError::Control(c) => Self::Control(c),
            e => Self::Type(e),
        }
    }
}
impl From<resources::NodeCodecError> for WriterSchemaCodecError {
    fn from(e: resources::NodeCodecError) -> Self {
        match e {
            resources::NodeCodecError::Control(c) => Self::Control(c),
            e => Self::Resources(e),
        }
    }
}
impl From<WriterSchemaCodecError> for resources::NodeCodecError {
    fn from(error: WriterSchemaCodecError) -> Self {
        match error {
            WriterSchemaCodecError::Control(cause) => Self::Control(cause),
            WriterSchemaCodecError::Type(error) => error.into(),
            WriterSchemaCodecError::Resources(error) => error,
            WriterSchemaCodecError::InvalidShape(message) => Self::InvalidShape(message),
        }
    }
}
impl fmt::Display for WriterSchemaCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(e) => e.fmt(f),
            Self::Type(e) => e.fmt(f),
            Self::Resources(e) => e.fmt(f),
            Self::InvalidShape(s) => f.write_str(s),
        }
    }
}
impl std::error::Error for WriterSchemaCodecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(e) => Some(e),
            Self::Type(e) => Some(e),
            Self::Resources(e) => Some(e),
            Self::InvalidShape(_) => None,
        }
    }
}
type Error = WriterSchemaCodecError;
fn shape(s: &'static str) -> Error {
    Error::InvalidShape(s)
}
fn finish<T>(w: CompileCheckpoints<'_>, result: Result<T, Error>) -> Result<T, Error> {
    if matches!(&result, Err(Error::Control(_))) {
        return result;
    }
    w.finish()?;
    result
}
fn required(id: Option<u32>, w: &mut CompileCheckpoints<'_>) -> Result<u32, Error> {
    let result = id.ok_or_else(|| shape("writer field required reference is absent"));
    w.step()?;
    result
}
pub(crate) fn role(value: p::WriterRelationFieldRole) -> i32 {
    use wire::WriterRelationFieldRole as W;
    (match value {
        p::WriterRelationFieldRole::Kind => W::Kind,
        p::WriterRelationFieldRole::TargetOrdinal => W::TargetOrdinal,
        p::WriterRelationFieldRole::RowCount => W::RowCount,
        p::WriterRelationFieldRole::CommitFragment => W::CommitFragment,
        p::WriterRelationFieldRole::Auxiliary => W::Auxiliary,
    }) as i32
}
pub(crate) fn decode_role(value: i32) -> Result<p::WriterRelationFieldRole, Error> {
    use wire::WriterRelationFieldRole as W;
    match W::try_from(value) {
        Ok(W::Kind) => Ok(p::WriterRelationFieldRole::Kind),
        Ok(W::TargetOrdinal) => Ok(p::WriterRelationFieldRole::TargetOrdinal),
        Ok(W::RowCount) => Ok(p::WriterRelationFieldRole::RowCount),
        Ok(W::CommitFragment) => Ok(p::WriterRelationFieldRole::CommitFragment),
        Ok(W::Auxiliary) => Ok(p::WriterRelationFieldRole::Auxiliary),
        Ok(W::Unspecified) | Err(_) => Err(shape("unknown writer relation field role")),
    }
}
impl WriterSchemaProjectionLimits {
    // This private adapter borrows the shared request/work algebra; it does
    // not expose a second caller wallet. No properties or node inputs exist.
    fn resources(self) -> NodeProjectionLimits {
        NodeProjectionLimits {
            max_input_nodes: self.max_fields,
            max_value_references: self.max_type_references,
            max_list_items: self.max_fields,
            max_allocation_requests: self.max_allocation_requests,
            max_allocation_request_bytes: self.max_allocation_request_bytes,
            max_coexisting_source_and_request_bytes: self.max_coexisting_source_and_request_bytes,
            max_work: self.max_work,
            properties: PhysicalPropertyProjectionLimits {
                max_value_references: self.max_type_references,
                max_allocation_requests: self.max_allocation_requests,
                max_allocation_request_bytes: self.max_allocation_request_bytes,
                max_coexisting_source_and_request_bytes: self
                    .max_coexisting_source_and_request_bytes,
                max_work: self.max_work,
            },
        }
    }
}
/// The original containing node before this schema's contribution. Type IDs
/// stay on the schema axis; the caller charges actual Value occurrences in base.
#[derive(Clone, Copy)]
pub(crate) struct WriterSchemaNodeAdmission {
    pub(crate) base: Model,
    pub(crate) values: usize,
    pub(crate) limits: Option<NodeProjectionLimits>,
}
impl WriterSchemaNodeAdmission {
    /// Commit one completed contribution before lending this same node to the
    /// next schema. Source bytes are supplied once to the original node model.
    pub(crate) fn merge(
        self,
        child: WriterSchemaProjectionFacts,
    ) -> Result<Model, resources::NodeCodecError> {
        let mut merged = self.base;
        merged.items = resources::add(merged.items, child.field_count)?;
        merged.requests = resources::add(merged.requests, child.allocation_requests_upper_bound)?;
        merged.requested =
            resources::add(merged.requested, child.allocation_request_bytes_upper_bound)?;
        merged.delegated_work =
            resources::add(merged.delegated_work, child.cumulative_work_upper_bound)?;
        Ok(merged)
    }
    fn remaining(
        self,
        source: usize,
        child: WriterSchemaProjectionFacts,
    ) -> Result<Option<usize>, Error> {
        let merged = self.merge(child)?;
        let facts = merged.numerical_facts(source, self.values, self.limits)?;
        self.limits
            .map(|limits| {
                limits
                    .max_work
                    .checked_sub(facts.cumulative_work_upper_bound)
                    .ok_or_else(|| CompileControlError::ResourceExhausted.into())
            })
            .transpose()
    }
}
#[derive(Clone, Copy)]
struct ProjectionContext {
    source: usize,
    limits: WriterSchemaProjectionLimits,
    parent: Option<WriterSchemaNodeAdmission>,
}
type SchemaAdmit<'a> =
    dyn FnMut(&WriterSchemaProjectionFacts) -> Result<(), CompileControlError> + 'a;
struct Count<'a> {
    model: Model,
    names: usize,
    work_peak: usize,
    admit: Option<&'a mut SchemaAdmit<'a>>,
    parent: Option<WriterSchemaNodeAdmission>,
}
impl<'a> Count<'a> {
    fn new(n: usize) -> Self {
        Self {
            model: Model {
                items: n,
                refs: n,
                ..Model::default()
            },
            names: 0,
            work_peak: 0,
            admit: None,
            parent: None,
        }
    }
    fn numerical_facts(
        &mut self,
        source: usize,
        types: usize,
        limits: WriterSchemaProjectionLimits,
    ) -> Result<WriterSchemaProjectionFacts, Error> {
        if self.names > limits.max_name_bytes {
            return Err(CompileControlError::ResourceExhausted.into());
        }
        let f = self
            .model
            .numerical_facts(source, types, limits.resources())?;
        let mut facts = WriterSchemaProjectionFacts {
            field_count: f.list_item_count,
            name_bytes: self.names,
            type_reference_count: f.value_reference_count,
            allocation_requests_upper_bound: f.allocation_requests_upper_bound,
            allocation_request_bytes_upper_bound: f.allocation_request_bytes_upper_bound,
            coexisting_source_and_request_bytes_upper_bound: f
                .coexisting_source_and_request_bytes_upper_bound,
            cumulative_work_upper_bound: f.cumulative_work_upper_bound,
        };
        if self.admit.is_some() {
            self.work_peak = self.work_peak.max(facts.cumulative_work_upper_bound);
            resources::check_cap(self.work_peak, limits.max_work)?;
            facts.cumulative_work_upper_bound = self.work_peak;
        }
        if let Some(parent) = self.parent {
            let _ = parent.remaining(source, facts)?;
        }
        if let Some(admit) = self.admit.as_mut() {
            admit(&facts)?;
        }
        Ok(facts)
    }
    fn comparison_prefix(
        &mut self,
        left: &FunctionValueType,
        right: &FunctionValueType,
        source: usize,
        roots: usize,
        limits: WriterSchemaProjectionLimits,
    ) -> Result<(), Error> {
        let prefix = type_binding_prefix_work_upper_bound_in(left, right, source)?;
        let mut current = self.model;
        current.delegated_work = resources::add(current.delegated_work, prefix.work_upper_bound())?;
        let facts = current.numerical_facts(source, roots, limits.resources())?;
        self.work_peak = self.work_peak.max(facts.cumulative_work_upper_bound);
        self.numerical_facts(source, roots, limits)?;
        Ok(())
    }
    fn clone_facts(
        &mut self,
        ty: &FunctionValueType,
        charge: bool,
        request_base: Option<Model>,
        source: usize,
        roots: usize,
        limits: WriterSchemaProjectionLimits,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<physical_type_v2::ValueTypeCloneFacts, Error> {
        if self.admit.is_none() {
            return Ok(physical_type_v2::preflight_value_type_clone(ty, w)?);
        }
        let base = request_base.unwrap_or(self.model);
        physical_type_v2::preflight_value_type_clone_admitted(
            ty,
            &mut |facts, _| {
                if charge {
                    self.model.requests =
                        resources::add(base.requests, facts.allocation_requests_upper_bound())?;
                    self.model.requested = resources::add(
                        base.requested,
                        facts.allocation_request_bytes_upper_bound(),
                    )?;
                }
                if facts.work_upper_bound()
                    > physical_type_v2::value_type_clone_preflight_work_upper_bound()
                {
                    return Err(CompileControlError::ResourceExhausted.into());
                }
                self.numerical_facts(source, roots, limits)?;
                Ok(())
            },
            w,
        )
    }
    fn facts(
        &mut self,
        source: usize,
        types: usize,
        limits: WriterSchemaProjectionLimits,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<WriterSchemaProjectionFacts, Error> {
        // All originating numerical facts precede any completed gate work.
        // Keep the original successful name step and seven node steps.
        let facts = self.numerical_facts(source, types, limits)?;
        resources::cap(self.names, limits.max_name_bytes, w)?;
        self.model.facts(source, types, limits.resources(), w)?;
        Ok(facts)
    }
    fn remaining(
        &mut self,
        source: usize,
        types: usize,
        limits: WriterSchemaProjectionLimits,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<usize, Error> {
        let raw_work = if self.admit.is_some() {
            Some(
                self.model
                    .numerical_facts(source, types, limits.resources())?
                    .cumulative_work_upper_bound,
            )
        } else {
            None
        };
        let mut f = self.facts(source, types, limits, w)?;
        if let Some(raw_work) = raw_work {
            // The captured comparison prefix is a replacement peak. The same
            // delegated comparison retains its room without deducting it twice.
            f.cumulative_work_upper_bound = raw_work;
        }
        let child = limits
            .max_work
            .checked_sub(f.cumulative_work_upper_bound)
            .ok_or(CompileControlError::ResourceExhausted)?;
        Ok(match self.parent {
            Some(parent) => parent
                .remaining(source, f)?
                .map_or(child, |limit| child.min(limit)),
            None => child,
        })
    }
}
#[derive(Clone, Copy)]
enum Source<'a> {
    Schema(&'a p::WriterRelationSchema),
    Targets(&'a [p::WriterTargetField]),
}
impl<'a> Source<'a> {
    fn len(self) -> usize {
        match self {
            Self::Schema(s) => s.fields.len(),
            Self::Targets(s) => s.len(),
        }
    }
    fn types(self) -> SourceTypes<'a> {
        SourceTypes {
            source: self,
            next: 0,
            pending: None,
        }
    }
    fn storage(self, i: usize) -> Option<&'a FunctionValueType> {
        match self {
            Self::Schema(_) => None,
            Self::Targets(fields) => fields[i].storage_type.as_ref(),
        }
    }
    fn name(self, i: usize) -> &'a str {
        match self {
            Self::Schema(s) => &s.fields[i].name,
            Self::Targets(s) => &s[i].provider_name,
        }
    }
    fn floor(self) -> Result<usize, Error> {
        Ok(match self {
            Self::Schema(s) => resources::add(
                size_of::<p::WriterRelationSchema>(),
                resources::bytes::<p::WriterRelationField>(s.fields.len())?,
            )?,
            Self::Targets(s) => resources::add(
                size_of::<&[p::WriterTargetField]>(),
                resources::bytes::<p::WriterTargetField>(s.len())?,
            )?,
        })
    }
}
/// W followed immediately by its optional T, without rescanning prior fields.
struct SourceTypes<'a> {
    source: Source<'a>,
    next: usize,
    pending: Option<&'a FunctionValueType>,
}
impl<'a> Iterator for SourceTypes<'a> {
    type Item = &'a FunctionValueType;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(ty) = self.pending.take() {
            return Some(ty);
        }
        let i = self.next;
        let ty = match self.source {
            Source::Schema(schema) => &schema.fields.get(i)?.ty,
            Source::Targets(fields) => {
                let field = fields.get(i)?;
                self.pending = field.storage_type.as_ref();
                &field.ty
            }
        };
        self.next += 1;
        Some(ty)
    }
}
#[derive(Clone, Copy)]
enum Raw<'a> {
    Schema(&'a wire::WriterRelationSchema),
    Targets(&'a Vec<wire::WriterTargetField>),
}
impl<'a> Raw<'a> {
    fn len(self) -> usize {
        match self {
            Self::Schema(s) => s.fields.len(),
            Self::Targets(s) => s.len(),
        }
    }
    fn name(self, i: usize) -> &'a String {
        match self {
            Self::Schema(s) => &s.fields[i].name,
            Self::Targets(s) => &s[i].provider_name,
        }
    }
    fn type_id(self, i: usize) -> Option<u32> {
        match self {
            Self::Schema(s) => s.fields[i].value_type_id,
            Self::Targets(s) => s[i].value_type_id,
        }
    }
    fn storage_id(self, i: usize) -> Option<u32> {
        match self {
            Self::Schema(_) => None,
            Self::Targets(fields) => fields[i].storage_value_type_id,
        }
    }
    fn type_ids(self) -> RawTypeIds<'a> {
        RawTypeIds {
            raw: self,
            next: 0,
            pending: None,
        }
    }
    fn floor(self) -> Result<usize, Error> {
        Ok(match self {
            Self::Schema(s) => resources::add(
                size_of::<wire::WriterRelationSchema>(),
                resources::bytes::<wire::WriterRelationField>(s.fields.capacity())?,
            )?,
            Self::Targets(s) => resources::add(
                size_of::<Vec<wire::WriterTargetField>>(),
                resources::bytes::<wire::WriterTargetField>(s.capacity())?,
            )?,
        })
    }
}
struct RawTypeIds<'a> {
    raw: Raw<'a>,
    next: usize,
    pending: Option<u32>,
}
impl Iterator for RawTypeIds<'_> {
    type Item = Option<u32>;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(id) = self.pending.take() {
            return Some(Some(id));
        }
        if self.next >= self.raw.len() {
            return None;
        }
        let i = self.next;
        self.next += 1;
        self.pending = self.raw.storage_id(i);
        Some(self.raw.type_id(i))
    }
}
fn typed<'a>(
    types: &'a DecodedTypeTable,
    id: u32,
    w: &mut CompileCheckpoints<'_>,
) -> Result<&'a FunctionValueType, Error> {
    w.flush()?;
    let result = types.value_type(id);
    w.flush()?;
    w.step()?;
    result.ok_or_else(|| shape("writer field type reference is unknown"))
}
fn preflight_encode(
    source: Source<'_>,
    ids: &[u32],
    types: &EncodedTypeTable<'_>,
    invoice: usize,
    l: WriterSchemaProjectionLimits,
    w: &mut CompileCheckpoints<'_>,
) -> Result<WriterSchemaProjectionFacts, Error> {
    preflight_encode_context(
        source,
        ids,
        types,
        ProjectionContext {
            source: invoice,
            limits: l,
            parent: None,
        },
        None,
        w,
    )
}
fn preflight_encode_context<'admit>(
    source: Source<'_>,
    ids: &[u32],
    types: &EncodedTypeTable<'_>,
    context: ProjectionContext,
    admit: Option<&'admit mut SchemaAdmit<'admit>>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<WriterSchemaProjectionFacts, Error> {
    let ProjectionContext {
        source: invoice,
        limits: l,
        parent,
    } = context;
    let n = source.len();
    let roots = types.source_counts().0;
    let mut c = Count::new(n);
    c.parent = parent;
    c.admit = admit;
    // Original linear root lookups run once here; emitted IDs need no lookup.
    // Admit the sole clone preflight for every actual source occurrence.
    let occurrence_work = resources::add(
        resources::add(roots, 8)?,
        physical_type_v2::value_type_clone_preflight_work_upper_bound(),
    )?;
    c.model.delegated_work = resources::mul(n, occurrence_work)?;
    if c.admit.is_some() {
        match source {
            Source::Schema(_) => c.model.request::<wire::WriterRelationField>(n, 1)?,
            Source::Targets(_) => c.model.request::<wire::WriterTargetField>(n, 1)?,
        }
    }
    c.facts(invoice, roots, l, w)?;
    if let Source::Targets(_) = source {
        for i in 0..n {
            if source.storage(i).is_some() {
                c.model.refs = resources::add(c.model.refs, 1)?;
                c.model.delegated_work = resources::add(c.model.delegated_work, occurrence_work)?;
                c.numerical_facts(invoice, roots, l)?;
            }
            w.step()?;
        }
    }
    let mut known = resources::add(source.floor()?, resources::bytes::<u32>(ids.len())?)?;
    resources::floor(
        invoice,
        resources::add(known, resources::bytes::<FunctionValueType>(roots)?)?,
        w,
    )?;
    let valid = ids.len() == c.model.refs;
    w.step()?;
    if !valid {
        return Err(shape(
            "writer field type ID count differs from original fields",
        ));
    }
    if c.admit.is_none() {
        match source {
            Source::Schema(_) => c.model.request::<wire::WriterRelationField>(n, 1)?,
            Source::Targets(_) => c.model.request::<wire::WriterTargetField>(n, 1)?,
        }
    }
    c.numerical_facts(invoice, roots, l)?;
    for i in 0..n {
        let bytes = source.name(i).len();
        c.names = resources::add(c.names, bytes)?;
        known = resources::add(known, bytes)?;
        c.model.request::<u8>(bytes, 1)?;
        if let Source::Targets(_) = source {
            c.model.request::<u8>(32, 1)?;
        }
        c.numerical_facts(invoice, roots, l)?;
        w.step()?;
        resources::floor(invoice, known, w)?;
        c.facts(invoice, roots, l, w)?;
    }
    for (source_ty, id) in source.types().zip(ids) {
        let bound = if c.admit.is_some() {
            let mut captured_bound = None;
            let captured = types.value_type_captured(
                *id,
                &mut |ty, work| {
                    c.comparison_prefix(source_ty, ty, invoice, roots, l)?;
                    let remaining = c.remaining(invoice, roots, l, work)?;
                    let base = c.model.delegated_work;
                    captured_bound = Some(verify_type_binding_admitted_in(
                        source_ty,
                        ty,
                        invoice,
                        remaining,
                        &mut |facts| {
                            c.model.delegated_work =
                                resources::add(base, facts.work_upper_bound())?;
                            c.numerical_facts(invoice, roots, l)?;
                            Ok::<(), Error>(())
                        },
                        work,
                    )?);
                    Ok::<(), Error>(())
                },
                w,
            )?;
            if captured.is_none() {
                return Err(shape("writer field type reference is unknown"));
            }
            captured_bound.ok_or_else(|| shape("writer field type capture is absent"))?
        } else {
            let ty = types
                .value_type_observed(*id, w)?
                .ok_or_else(|| shape("writer field type reference is unknown"))?;
            let bound = verify_type_binding(
                source_ty,
                ty,
                invoice,
                c.remaining(invoice, roots, l, w)?,
                w,
            )?;
            c.model.delegated_work =
                resources::add(c.model.delegated_work, bound.work_upper_bound())?;
            bound
        };
        c.numerical_facts(invoice, roots, l)?;
        w.step()?;
        if !bound.matches() {
            return Err(shape(
                "writer field full type differs from authored type root",
            ));
        }
        // Root-owned Dictionary Boxes are retained even when encoding; reuse
        // the sole clone topology for their necessary source backing floor.
        let clone = c.clone_facts(source_ty, false, None, invoice, roots, l, w)?;
        known = resources::add(known, clone.allocation_request_bytes_upper_bound())?;
        // The sole grammar's ceiling was admitted for every occurrence before
        // any type walk, and remains in final facts for exact-envelope replay.
        if clone.work_upper_bound()
            > physical_type_v2::value_type_clone_preflight_work_upper_bound()
        {
            return Err(CompileControlError::ResourceExhausted.into());
        }
        c.numerical_facts(invoice, roots, l)?;
        resources::floor(invoice, known, w)?;
        c.facts(invoice, roots, l, w)?;
    }
    c.facts(invoice, roots, l, w)
}
fn preflight_decode(
    raw: Raw<'_>,
    types: &DecodedTypeTable,
    invoice: usize,
    l: WriterSchemaProjectionLimits,
    w: &mut CompileCheckpoints<'_>,
) -> Result<WriterSchemaProjectionFacts, Error> {
    preflight_decode_context(
        raw,
        types,
        ProjectionContext {
            source: invoice,
            limits: l,
            parent: None,
        },
        None,
        w,
    )
}
fn preflight_decode_context<'admit>(
    raw: Raw<'_>,
    types: &DecodedTypeTable,
    context: ProjectionContext,
    admit: Option<&'admit mut SchemaAdmit<'admit>>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<WriterSchemaProjectionFacts, Error> {
    let ProjectionContext {
        source: invoice,
        limits: l,
        parent,
    } = context;
    let n = raw.len();
    let roots = types.value_types().len();
    let mut c = Count::new(n);
    c.parent = parent;
    c.admit = admit;
    let lookups = crate::btree_resources_v2::lookup_work(roots).map_err(shape)?;
    // Each actual occurrence has a lookup and clone in prepare and in emit.
    // Keep both original clone ceilings rather than narrowing final facts.
    let occurrence_work = resources::mul(
        resources::add(
            lookups,
            physical_type_v2::value_type_clone_preflight_work_upper_bound(),
        )?,
        2,
    )?;
    c.model.delegated_work = resources::mul(n, occurrence_work)?;
    if c.admit.is_some() {
        match raw {
            Raw::Schema(_) => c.model.request::<p::WriterRelationField>(n, 2)?,
            Raw::Targets(_) => c.model.request::<p::WriterTargetField>(n, 2)?,
        }
    }
    c.facts(invoice, roots, l, w)?;
    if let Raw::Targets(_) = raw {
        for i in 0..n {
            if raw.storage_id(i).is_some() {
                c.model.refs = resources::add(c.model.refs, 1)?;
                c.model.delegated_work = resources::add(c.model.delegated_work, occurrence_work)?;
                c.numerical_facts(invoice, roots, l)?;
            }
            w.step()?;
        }
    }
    let mut known = raw.floor()?;
    resources::floor(
        invoice,
        resources::add(known, resources::bytes::<FunctionValueType>(roots)?)?,
        w,
    )?;
    if c.admit.is_none() {
        match raw {
            Raw::Schema(_) => c.model.request::<p::WriterRelationField>(n, 2)?,
            Raw::Targets(_) => c.model.request::<p::WriterTargetField>(n, 2)?,
        }
    }
    c.numerical_facts(invoice, roots, l)?;
    for i in 0..n {
        let name = raw.name(i);
        c.names = resources::add(c.names, name.len())?;
        known = resources::add(known, name.capacity())?;
        c.model.request::<u8>(name.len(), 2)?;
        c.numerical_facts(invoice, roots, l)?;
        match raw {
            Raw::Schema(s) => {
                required(s.fields[i].value_id, w)?;
                let result = decode_role(s.fields[i].role);
                w.step()?;
                result?;
            }
            Raw::Targets(s) => {
                required(s[i].input_value_id, w)?;
                required(s[i].projected_value_id, w)?;
                known = resources::add(known, s[i].token.capacity())?;
                let valid = s[i].token.len() == 32;
                w.step()?;
                if !valid {
                    return Err(shape("writer field token is not exactly 32 bytes"));
                }
            }
        }
        required(raw.type_id(i), w)?;
        w.step()?;
        resources::floor(invoice, known, w)?;
        c.facts(invoice, roots, l, w)?;
    }
    for id in raw.type_ids() {
        let id = required(id, w)?;
        let request_base = c.model;
        let ty = if c.admit.is_some() {
            w.flush()?;
            let captured = types.value_type(id);
            if let Some(ty) = captured {
                let root = physical_type_v2::value_type_clone_root_facts(ty)?;
                c.model.requests = resources::add(
                    request_base.requests,
                    root.allocation_requests_upper_bound(),
                )?;
                c.model.requested = resources::add(
                    request_base.requested,
                    root.allocation_request_bytes_upper_bound(),
                )?;
                c.comparison_prefix(ty, ty, invoice, roots, l)?;
            }
            w.flush()?;
            w.step()?;
            captured.ok_or_else(|| shape("writer field type reference is unknown"))?
        } else {
            typed(types, id, w)?
        };
        // Borrowed fixed-scratch admission supplies an existing full-type
        // numerical bound before the sole owned-Dictionary clone topology.
        let remaining = c.remaining(invoice, roots, l, w)?;
        if c.admit.is_some() {
            let base = c.model.delegated_work;
            preflight_type_binding_admitted_in(
                ty,
                ty,
                invoice,
                remaining,
                &mut |facts| {
                    c.model.delegated_work = resources::add(base, facts.work_upper_bound())?;
                    c.numerical_facts(invoice, roots, l)?;
                    Ok::<(), Error>(())
                },
                w,
            )?;
        } else {
            let bound = preflight_type_binding(ty, ty, invoice, remaining, w)?;
            c.model.delegated_work =
                resources::add(c.model.delegated_work, bound.work_upper_bound())?;
        }
        c.numerical_facts(invoice, roots, l)?;
        let clone = c.clone_facts(ty, true, Some(request_base), invoice, roots, l, w)?;
        if c.admit.is_none() {
            c.model.requests =
                resources::add(c.model.requests, clone.allocation_requests_upper_bound())?;
            c.model.requested = resources::add(
                c.model.requested,
                clone.allocation_request_bytes_upper_bound(),
            )?;
        }
        if clone.work_upper_bound()
            > physical_type_v2::value_type_clone_preflight_work_upper_bound()
        {
            return Err(CompileControlError::ResourceExhausted.into());
        }
        c.numerical_facts(invoice, roots, l)?;
        w.step()?;
        c.facts(invoice, roots, l, w)?;
    }
    c.facts(invoice, roots, l, w)
}
fn text(input: &str, w: &mut CompileCheckpoints<'_>) -> Result<String, Error> {
    let mut bytes = resources::reserve::<u8>(input.len(), w)?;
    for byte in input.as_bytes() {
        bytes.push(*byte);
        w.step()?;
    }
    w.flush()?;
    let result = String::from_utf8(bytes);
    w.flush()?;
    result.map_err(|_| shape("copied writer field name is invalid UTF8"))
}
fn name(input: &str, w: &mut CompileCheckpoints<'_>) -> Result<Box<str>, Error> {
    let text = text(input, w)?;
    w.flush()?;
    let output = text.into_boxed_str();
    w.flush()?;
    Ok(output)
}
pub(crate) fn emit_schema(
    source: &p::WriterRelationSchema,
    ids: &[u32],
    w: &mut CompileCheckpoints<'_>,
) -> Result<wire::WriterRelationSchema, Error> {
    let mut fields = resources::reserve(source.fields.len(), w)?;
    for (f, id) in source.fields.iter().zip(ids) {
        fields.push(wire::WriterRelationField {
            value_id: Some(f.value.get()),
            name: text(&f.name, w)?,
            value_type_id: Some(*id),
            role: role(f.role),
        });
        w.step()?;
    }
    Ok(wire::WriterRelationSchema {
        revision: source.revision,
        fields,
    })
}
pub(crate) fn emit_targets(
    source: &[p::WriterTargetField],
    ids: &[u32],
    w: &mut CompileCheckpoints<'_>,
) -> Result<Vec<wire::WriterTargetField>, Error> {
    let mut fields = resources::reserve(source.len(), w)?;
    let mut ids = ids.iter();
    for f in source {
        let id = ids
            .next()
            .ok_or_else(|| shape("prepared writer input type ID is absent"))?;
        let storage_value_type_id = if f.storage_type.is_some() {
            Some(
                *ids.next()
                    .ok_or_else(|| shape("prepared writer storage type ID is absent"))?,
            )
        } else {
            None
        };
        let mut token = resources::reserve(32, w)?;
        for byte in f.token.to_bytes() {
            token.push(byte);
            w.step()?;
        }
        fields.push(wire::WriterTargetField {
            storage_value_type_id,
            token,
            provider_name: text(&f.provider_name, w)?,
            input_value_id: Some(f.input.get()),
            projected_value_id: Some(f.projected.get()),
            value_type_id: Some(*id),
            hidden: f.hidden,
        });
        w.step()?;
    }
    Ok(fields)
}
pub(crate) fn read_schema(
    raw: &wire::WriterRelationSchema,
    types: &DecodedTypeTable,
    w: &mut CompileCheckpoints<'_>,
) -> Result<p::WriterRelationSchema, Error> {
    let mut fields = resources::reserve(raw.fields.len(), w)?;
    for f in &raw.fields {
        fields.push(p::WriterRelationField {
            value: p::ValueId::new(required(f.value_id, w)?),
            name: name(&f.name, w)?,
            ty: physical_type_v2::clone_value_type_observed(
                typed(types, required(f.value_type_id, w)?, w)?,
                w,
            )?,
            role: decode_role(f.role)?,
        });
        w.step()?;
    }
    Ok(p::WriterRelationSchema {
        revision: raw.revision,
        fields: resources::boxed(fields, w)?,
    })
}
pub(crate) fn read_targets(
    raw: &[wire::WriterTargetField],
    types: &DecodedTypeTable,
    w: &mut CompileCheckpoints<'_>,
) -> Result<Box<[p::WriterTargetField]>, Error> {
    let mut fields = resources::reserve(raw.len(), w)?;
    for f in raw {
        let token: [u8; 32] = f
            .token
            .as_slice()
            .try_into()
            .map_err(|_| shape("prepared writer field token is not 32 bytes"))?;
        w.step()?;
        let provider_name = name(&f.provider_name, w)?;
        let input = p::ValueId::new(required(f.input_value_id, w)?);
        let projected = p::ValueId::new(required(f.projected_value_id, w)?);
        let ty = physical_type_v2::clone_value_type_observed(
            typed(types, required(f.value_type_id, w)?, w)?,
            w,
        )?;
        let storage_type = f
            .storage_value_type_id
            .map(|id| -> Result<_, Error> {
                Ok(physical_type_v2::clone_value_type_observed(
                    typed(types, id, w)?,
                    w,
                )?)
            })
            .transpose()?;
        fields.push(p::WriterTargetField {
            token: ConnectorWriteFieldToken::from_bytes(token),
            provider_name,
            input,
            projected,
            ty,
            storage_type,
            hidden: f.hidden,
        });
        w.step()?;
    }
    Ok(resources::boxed(fields, w)?)
}

/// Same-meter schema projection. The caller retains the exact type/source loan
/// and owns entry, header admission and final publication.
pub(crate) struct WriterSchemaProjection {
    pub(crate) source: usize,
    pub(crate) limits: WriterSchemaProjectionLimits,
    pub(crate) parent: WriterSchemaNodeAdmission,
}
impl WriterSchemaProjection {
    fn context(self) -> ProjectionContext {
        ProjectionContext {
            source: self.source,
            limits: self.limits,
            parent: Some(self.parent),
        }
    }
}
pub(crate) fn preflight_schema_encode_observed(
    input: &p::WriterRelationSchema,
    ids: &[u32],
    types: &EncodedTypeTable<'_>,
    projection: WriterSchemaProjection,
    w: &mut CompileCheckpoints<'_>,
) -> Result<WriterSchemaProjectionFacts, Error> {
    preflight_encode_context(
        Source::Schema(input),
        ids,
        types,
        projection.context(),
        None,
        w,
    )
}
pub(crate) fn preflight_targets_encode_observed(
    input: &[p::WriterTargetField],
    ids: &[u32],
    types: &EncodedTypeTable<'_>,
    projection: WriterSchemaProjection,
    w: &mut CompileCheckpoints<'_>,
) -> Result<WriterSchemaProjectionFacts, Error> {
    preflight_encode_context(
        Source::Targets(input),
        ids,
        types,
        projection.context(),
        None,
        w,
    )
}
pub(crate) fn preflight_schema_decode_observed(
    input: &wire::WriterRelationSchema,
    types: &DecodedTypeTable,
    projection: WriterSchemaProjection,
    w: &mut CompileCheckpoints<'_>,
) -> Result<WriterSchemaProjectionFacts, Error> {
    preflight_decode_context(Raw::Schema(input), types, projection.context(), None, w)
}
pub(crate) fn preflight_targets_decode_observed(
    input: &Vec<wire::WriterTargetField>,
    types: &DecodedTypeTable,
    projection: WriterSchemaProjection,
    w: &mut CompileCheckpoints<'_>,
) -> Result<WriterSchemaProjectionFacts, Error> {
    preflight_decode_context(Raw::Targets(input), types, projection.context(), None, w)
}
pub(crate) fn preflight_schema_encode_in(
    input: &p::WriterRelationSchema,
    ids: &[u32],
    types: &EncodedTypeTable<'_>,
    projection: WriterSchemaProjection,
    admit: &mut resources::NodeAdmit<'_>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<WriterSchemaProjectionFacts, Error> {
    let parent = projection.parent;
    let source = projection.source;
    let mut update = |facts: &WriterSchemaProjectionFacts| {
        let merged = parent.merge(*facts).map_err(|error| match error {
            resources::NodeCodecError::Control(cause) => cause,
            _ => CompileControlError::ResourceExhausted,
        })?;
        let next = merged
            .numerical_facts(source, parent.values, parent.limits)
            .map_err(|error| match error {
                resources::NodeCodecError::Control(cause) => cause,
                _ => CompileControlError::ResourceExhausted,
            })?;
        admit(&next)
    };
    preflight_encode_context(
        Source::Schema(input),
        ids,
        types,
        projection.context(),
        Some(&mut update),
        w,
    )
}
pub(crate) fn preflight_targets_encode_in(
    input: &[p::WriterTargetField],
    ids: &[u32],
    types: &EncodedTypeTable<'_>,
    projection: WriterSchemaProjection,
    admit: &mut resources::NodeAdmit<'_>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<WriterSchemaProjectionFacts, Error> {
    let parent = projection.parent;
    let source = projection.source;
    let mut update = |facts: &WriterSchemaProjectionFacts| {
        let merged = parent.merge(*facts).map_err(|error| match error {
            resources::NodeCodecError::Control(cause) => cause,
            _ => CompileControlError::ResourceExhausted,
        })?;
        let next = merged
            .numerical_facts(source, parent.values, parent.limits)
            .map_err(|error| match error {
                resources::NodeCodecError::Control(cause) => cause,
                _ => CompileControlError::ResourceExhausted,
            })?;
        admit(&next)
    };
    preflight_encode_context(
        Source::Targets(input),
        ids,
        types,
        projection.context(),
        Some(&mut update),
        w,
    )
}
pub(crate) fn preflight_schema_decode_in(
    input: &wire::WriterRelationSchema,
    types: &DecodedTypeTable,
    projection: WriterSchemaProjection,
    admit: &mut resources::NodeAdmit<'_>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<WriterSchemaProjectionFacts, Error> {
    let parent = projection.parent;
    let source = projection.source;
    let mut update = |facts: &WriterSchemaProjectionFacts| {
        let merged = parent.merge(*facts).map_err(|error| match error {
            resources::NodeCodecError::Control(cause) => cause,
            _ => CompileControlError::ResourceExhausted,
        })?;
        let next = merged
            .numerical_facts(source, parent.values, parent.limits)
            .map_err(|error| match error {
                resources::NodeCodecError::Control(cause) => cause,
                _ => CompileControlError::ResourceExhausted,
            })?;
        admit(&next)
    };
    preflight_decode_context(
        Raw::Schema(input),
        types,
        projection.context(),
        Some(&mut update),
        w,
    )
}
pub(crate) fn preflight_targets_decode_in(
    input: &Vec<wire::WriterTargetField>,
    types: &DecodedTypeTable,
    projection: WriterSchemaProjection,
    admit: &mut resources::NodeAdmit<'_>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<WriterSchemaProjectionFacts, Error> {
    let parent = projection.parent;
    let source = projection.source;
    let mut update = |facts: &WriterSchemaProjectionFacts| {
        let merged = parent.merge(*facts).map_err(|error| match error {
            resources::NodeCodecError::Control(cause) => cause,
            _ => CompileControlError::ResourceExhausted,
        })?;
        let next = merged
            .numerical_facts(source, parent.values, parent.limits)
            .map_err(|error| match error {
                resources::NodeCodecError::Control(cause) => cause,
                _ => CompileControlError::ResourceExhausted,
            })?;
        admit(&next)
    };
    preflight_decode_context(
        Raw::Targets(input),
        types,
        projection.context(),
        Some(&mut update),
        w,
    )
}
macro_rules! encoding_api {
    ($token:ident,$prepare:ident,$encode:ident,$input:ty,$output:ty,$variant:ident,$emit:ident) => {
        pub struct $token<'input, 'loan, 'source, 'control> {
            input: &'input $input,
            type_ids: &'input [u32],
            types: &'loan EncodedTypeTable<'source>,
            control: &'control dyn PureCompileControl,
            facts: WriterSchemaProjectionFacts,
        }
        impl $token<'_, '_, '_, '_> {
            pub fn facts(&self) -> &WriterSchemaProjectionFacts {
                &self.facts
            }
            pub fn emit(self) -> Result<($output, WriterSchemaProjectionFacts), Error> {
                let mut work = CompileCheckpoints::try_new(self.control, CompilePhase::Encode)?;
                let result =
                    $emit(self.input, self.type_ids, &mut work).map(|output| (output, self.facts));
                finish(work, result)
            }
            pub fn types(&self) -> &EncodedTypeTable<'_> {
                self.types
            }
        }
        pub fn $prepare<'input, 'loan, 'source, 'control>(
            input: &'input $input,
            type_ids: &'input [u32],
            types: &'loan EncodedTypeTable<'source>,
            source_retained_bytes: usize,
            limits: WriterSchemaProjectionLimits,
            control: &'control dyn PureCompileControl,
        ) -> Result<$token<'input, 'loan, 'source, 'control>, Error> {
            let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
            let result = preflight_encode(
                Source::$variant(input),
                type_ids,
                types,
                source_retained_bytes,
                limits,
                &mut work,
            );
            let facts = finish(work, result)?;
            Ok($token {
                input,
                type_ids,
                types,
                control,
                facts,
            })
        }
        pub fn $encode(
            input: &$input,
            type_ids: &[u32],
            types: &EncodedTypeTable<'_>,
            source_retained_bytes: usize,
            limits: WriterSchemaProjectionLimits,
            control: &dyn PureCompileControl,
        ) -> Result<($output, WriterSchemaProjectionFacts), Error> {
            let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
            let result = (|| {
                let facts = preflight_encode(
                    Source::$variant(input),
                    type_ids,
                    types,
                    source_retained_bytes,
                    limits,
                    &mut work,
                )?;
                let output = $emit(input, type_ids, &mut work)?;
                Ok((output, facts))
            })();
            finish(work, result)
        }
    };
}
encoding_api!(
    PreparedWriterSchemaEncode,
    prepare_writer_schema_encode,
    encode_writer_schema,
    p::WriterRelationSchema,
    wire::WriterRelationSchema,
    Schema,
    emit_schema
);
encoding_api!(
    PreparedWriterTargetFieldsEncode,
    prepare_writer_target_fields_encode,
    encode_writer_target_fields,
    [p::WriterTargetField],
    Vec<wire::WriterTargetField>,
    Targets,
    emit_targets
);

macro_rules! decoding_api {
    ($token:ident,$prepare:ident,$decode:ident,$input:ty,$output:ty,$variant:ident,$emit:ident) => {
        pub struct $token<'input, 'types, 'control> {
            input: &'input $input,
            types: &'types DecodedTypeTable,
            control: &'control dyn PureCompileControl,
            facts: WriterSchemaProjectionFacts,
        }
        impl $token<'_, '_, '_> {
            pub fn facts(&self) -> &WriterSchemaProjectionFacts {
                &self.facts
            }
            pub fn types(&self) -> &DecodedTypeTable {
                self.types
            }
            pub fn emit(self) -> Result<($output, WriterSchemaProjectionFacts), Error> {
                let mut work = CompileCheckpoints::try_new(self.control, CompilePhase::Decode)?;
                let result =
                    $emit(self.input, self.types, &mut work).map(|output| (output, self.facts));
                finish(work, result)
            }
        }
        pub fn $prepare<'input, 'types, 'control>(
            input: &'input $input,
            types: &'types DecodedTypeTable,
            source_retained_bytes: usize,
            limits: WriterSchemaProjectionLimits,
            control: &'control dyn PureCompileControl,
        ) -> Result<$token<'input, 'types, 'control>, Error> {
            let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
            let result = preflight_decode(
                Raw::$variant(input),
                types,
                source_retained_bytes,
                limits,
                &mut work,
            );
            let facts = finish(work, result)?;
            Ok($token {
                input,
                types,
                control,
                facts,
            })
        }
        pub fn $decode(
            input: &$input,
            types: &DecodedTypeTable,
            source_retained_bytes: usize,
            limits: WriterSchemaProjectionLimits,
            control: &dyn PureCompileControl,
        ) -> Result<($output, WriterSchemaProjectionFacts), Error> {
            let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
            let result = (|| {
                let facts = preflight_decode(
                    Raw::$variant(input),
                    types,
                    source_retained_bytes,
                    limits,
                    &mut work,
                )?;
                let output = $emit(input, types, &mut work)?;
                Ok((output, facts))
            })();
            finish(work, result)
        }
    };
}
decoding_api!(
    PreparedWriterSchemaDecode,
    prepare_writer_schema_decode,
    decode_writer_schema,
    wire::WriterRelationSchema,
    p::WriterRelationSchema,
    Schema,
    read_schema
);
decoding_api!(
    PreparedWriterTargetFieldsDecode,
    prepare_writer_target_fields_decode,
    decode_writer_target_fields,
    Vec<wire::WriterTargetField>,
    Box<[p::WriterTargetField]>,
    Targets,
    read_targets
);

#[cfg(test)]
#[path = "physical_writer_schema_v2/tests.rs"]
mod tests;
