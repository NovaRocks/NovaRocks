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

//! Deterministic occurrence definitions, without type interning or source
//! normalization. The entire projection envelope is checked before allocating
//! output definitions or copying strings. Scratch work is not a MEM grant.

use super::encode_resources::Model;
use super::{
    FieldRootSources, PackageTypeProjectionFacts, PackageTypeProjectionLimits, TypeCodecError,
    TypeProjectionLimits, ValueRootSources, WriterTypeSource,
    semantic::{self, Children},
    validate_field, validate_type_visit,
};
use crate::allocation_exit_v2::reserve_exit;
use crate::arrow_metadata_v2::{copy_string, encode_metadata};
use crate::value_type_table::{
    LogicalTypeRoot, LogicalTypeTableBuilder, resources as shared_resources,
};
use arrow::datatypes::{DataType, Field, UnionMode};
use novarocks_proto_models::{physical_type_v2 as wire, plan, value_type as logical_wire};
use novarocks_type_contract::{
    ArrowTypeProfile, CompileCheckpoints, CompileControlError, FunctionValueType, ValueTypeVisit,
    validate_arrow_carrier_parameters_observed,
    validate_arrow_layout_structure_with_profile_observed,
};
use std::alloc::Layout;
use std::{collections::BTreeSet, sync::Arc};
use wire::carrier_type_definition::Kind;

type Error = TypeCodecError;
type HostError<H> = crate::host_projection_v2::ProjectionFailure<TypeCodecError, H>;
type BorrowedRootBindings<'source> = (Vec<(u32, &'source FunctionValueType)>, Vec<(u32, u32)>);
use crate::host_projection_v2::{AdmissionRefusal, ProjectionFailure};

/// The only non-strict selector carries an actual checked recipe source.
/// It is never constructed from a DTO, nominal carrier or caller boolean.
#[derive(Clone, Copy)]
pub(super) enum SourceLaw<'source> {
    Strict,
    Writer(&'source novarocks_connector_contract::ConnectorWriteRecipeDraft),
}
impl SourceLaw<'_> {
    pub(super) fn is_strict(self) -> bool {
        match self {
            Self::Strict => true,
            Self::Writer(recipe) => {
                // Keep the real source loan through every nested occurrence.
                let _ = recipe.input();
                false
            }
        }
    }
}

struct Admission<'a, H> {
    model: Model,
    limits: PackageTypeProjectionLimits,
    admit: &'a mut dyn FnMut(&PackageTypeProjectionFacts) -> Result<(), AdmissionRefusal<H>>,
}
impl<H> Admission<'_, H> {
    fn gate(&mut self) -> Result<(), HostError<H>> {
        self.model.gate_with_host(self.limits, self.admit)
    }
}

struct Counts<'a, H> {
    definitions: usize,
    expanded: usize,
    strings: usize,
    carriers: usize,
    fields: usize,
    limits: TypeProjectionLimits,
    resources: Option<Admission<'a, H>>,
}

fn add(total: &mut usize, count: usize, bound: usize) -> Result<(), Error> {
    *total = total
        .checked_add(count)
        .filter(|total| *total <= bound)
        .ok_or(Error::InvalidShape(
            "type projection exceeds its admitted envelope",
        ))?;
    Ok(())
}

impl<H> Counts<'_, H> {
    fn definition(&mut self) -> Result<(), HostError<H>> {
        add(&mut self.definitions, 1, self.limits.max_definitions)
            .map_err(|error| self.envelope_error(error))?;
        if let Some(resources) = &mut self.resources {
            resources.model.definition(1)?;
            resources.gate()?;
        }
        Ok(())
    }
    fn expansion(&mut self, count: usize) -> Result<(), HostError<H>> {
        add(&mut self.expanded, count, self.limits.max_expanded_nodes)
            .map_err(|error| self.envelope_error(error))?;
        if let Some(resources) = &mut self.resources {
            resources.model.expansion(count)?;
            resources.gate()?;
        }
        Ok(())
    }
    fn string(&mut self, length: usize) -> Result<(), HostError<H>> {
        add(&mut self.strings, length, self.limits.max_string_bytes)
            .map_err(|error| self.envelope_error(error))
    }
    fn envelope_error(&self, error: Error) -> HostError<H> {
        if self.resources.is_some() {
            CompileControlError::ResourceExhausted.into()
        } else {
            error.into()
        }
    }
    fn carrier(&mut self) -> Result<(), HostError<H>> {
        self.definition()?;
        // Separate namespaces allocate by occurrence count, never a source ID.
        u32::try_from(self.carriers)
            .map_err(|_| Error::InvalidShape("carrier definition IDs are exhausted"))?;
        self.carriers = self
            .carriers
            .checked_add(1)
            .ok_or(Error::InvalidShape("carrier definition count overflow"))?;
        Ok(())
    }
    fn field(&mut self) -> Result<(), HostError<H>> {
        self.definition()?;
        u32::try_from(self.fields)
            .map_err(|_| Error::InvalidShape("field definition IDs are exhausted"))?;
        self.fields = self
            .fields
            .checked_add(1)
            .ok_or(Error::InvalidShape("field definition count overflow"))?;
        Ok(())
    }
}

fn add_child(nodes: &mut usize, child: usize) -> Result<(), Error> {
    *nodes = nodes
        .checked_add(child)
        .ok_or(Error::InvalidShape("expanded carrier node count overflow"))?;
    Ok(())
}

// Validation precedes this recursion: Strict roots retain their explicit depth/4096
// nodes; immutable Writer recipes retain their original 32-level/schema law.
// Counts include each
// definition's full referenced subtree, including repeated FieldRef uses.
fn count_type<H>(
    ty: &DataType,
    counts: &mut Counts<'_, H>,
    work: &mut CompileCheckpoints<'_>,
    law: SourceLaw<'_>,
) -> Result<usize, HostError<H>> {
    if let Some(resources) = &mut counts.resources {
        resources.model.carrier(ty)?;
        resources.gate()?;
        counts.carrier()?;
        work.step()?;
    } else {
        work.step()?;
        counts.carrier()?;
    }
    if !law.is_strict() {
        validate_arrow_carrier_parameters_observed(ty, || work.step().map_err(Error::from))?;
    }
    let mut nodes = 1usize;
    match ty {
        DataType::Timestamp(_, Some(zone)) => counts.string(zone.len())?,
        DataType::List(field)
        | DataType::ListView(field)
        | DataType::LargeList(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => add_child(&mut nodes, count_field(field, counts, work, law)?)?,
        DataType::Struct(fields) => {
            for field in fields {
                add_child(&mut nodes, count_field(field, counts, work, law)?)?;
            }
        }
        DataType::Union(fields, _) => {
            for (_, field) in fields.iter() {
                add_child(&mut nodes, count_field(field, counts, work, law)?)?;
            }
        }
        DataType::Dictionary(key, value) => {
            add_child(&mut nodes, count_type(key, counts, work, law)?)?;
            add_child(&mut nodes, count_type(value, counts, work, law)?)?;
        }
        DataType::RunEndEncoded(ends, values) => {
            add_child(&mut nodes, count_field(ends, counts, work, law)?)?;
            add_child(&mut nodes, count_field(values, counts, work, law)?)?;
        }
        _ => {}
    }
    counts.expansion(nodes)?;
    Ok(nodes)
}

fn count_field<H>(
    field: &Field,
    counts: &mut Counts<'_, H>,
    work: &mut CompileCheckpoints<'_>,
    law: SourceLaw<'_>,
) -> Result<usize, HostError<H>> {
    if let Some(resources) = &mut counts.resources {
        resources.model.field(field)?;
        resources.gate()?;
        counts.field()?;
        work.step()?;
    } else {
        work.step()?;
        counts.field()?;
    }
    #[allow(deprecated)]
    let dictionary_id = field.dict_id();
    if !law.is_strict() && dictionary_id.is_some() != field.dict_is_ordered().is_some() {
        return Err(Error::InvalidShape("incomplete field dictionary attributes").into());
    }
    counts.string(field.name().len())?;
    let observed = counts.resources.is_some();
    if observed {
        work.flush()?;
    }
    let mut metadata = field.metadata().iter();
    if observed {
        work.step()?;
        work.flush()?;
    }
    loop {
        if observed {
            work.flush()?;
        }
        let entry = metadata.next();
        if let Some((key, value)) = entry {
            // The actual iterator result makes these request bounds known.
            // Numeric refusal stays primary before its opaque-exit callback.
            if let Some(resources) = &mut counts.resources {
                resources.model.string(key.len())?;
                resources.model.string(value.len())?;
                resources.gate()?;
            }
        }
        if observed {
            work.step()?;
            work.flush()?;
        }
        let Some((key, value)) = entry else {
            break;
        };
        work.step()?;
        counts.string(key.len())?;
        counts.string(value.len())?;
    }
    let nodes = count_type(field.data_type(), counts, work, law)?;
    counts.expansion(nodes)?;
    Ok(nodes)
}

/// Admit `count` actual nodes of one strict validator walk before it reaches
/// them. Without an admission (the unmetered DTO-only API) nothing is owed.
fn admit_strict_nodes<H>(
    resources: &mut Option<Admission<'_, H>>,
    count: usize,
) -> Result<(), HostError<H>> {
    if count != 0
        && let Some(resources) = resources
    {
        resources.model.strict_nodes(count)?;
        resources.gate()?;
    }
    Ok(())
}

/// Exactly the children the strict validator pushes when it visits `ty`.
pub(super) fn strict_children(ty: &DataType) -> usize {
    match ty {
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::FixedSizeList(_, _)
        | DataType::Map(_, _) => 1,
        DataType::Struct(fields) => fields.len(),
        DataType::Union(fields, _) => fields.len(),
        DataType::Dictionary(_, _) | DataType::RunEndEncoded(_, _) => 2,
        _ => 0,
    }
}

/// The original strict validator walk of one root whose own node the caller
/// has admitted. Each visited carrier admits its direct children before the
/// walker visits their Fields or pushes them, so the charge follows the
/// actual type rather than the per-type node maximum.
fn validate_strict_type<H>(
    ty: &DataType,
    profile: ArrowTypeProfile,
    resources: &mut Option<Admission<'_, H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), HostError<H>> {
    validate_arrow_layout_structure_with_profile_observed(ty, profile, |visit| {
        if let Some(resources) = resources {
            match visit {
                ValueTypeVisit::TypeNode(node) => resources.model.strict_carrier(node)?,
                ValueTypeVisit::Field(field) => resources.model.strict_field(field)?,
                ValueTypeVisit::ChildEdge(_) => {}
            }
            resources.gate()?;
        }
        if let ValueTypeVisit::TypeNode(node) = visit {
            admit_strict_nodes(resources, strict_children(node))?;
        }
        validate_type_visit(visit, work).map_err(Into::into)
    })
}

fn preflight<'a, H>(
    values: ValueRootSources<'_>,
    fields: FieldRootSources<'_>,
    writers: &[WriterTypeSource<'_>],
    limits: TypeProjectionLimits,
    resources: Option<Admission<'a, H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(Counts<'a, H>, BTreeSet<u32>), HostError<H>> {
    let root_count = values
        .len()
        .checked_add(fields.len())
        .and_then(|count| {
            writers.iter().try_fold(count, |count, source| {
                count.checked_add(source.field_ids.len())
            })
        })
        .ok_or(Error::InvalidShape("root definition count overflow"))?;
    if root_count > limits.max_definitions {
        return Err(Error::InvalidShape("type projection exceeds its admitted envelope").into());
    }
    let observed = resources.is_some();
    let mut reserved_fields = BTreeSet::new();
    for (id, _) in fields.iter() {
        if observed {
            work.flush()?;
        } else {
            work.step()?;
        }
        let unique = reserved_fields.insert(id);
        if observed {
            work.step()?;
            work.flush()?;
        }
        if !unique {
            return Err(Error::InvalidShape("duplicate field definition ID").into());
        }
    }
    for source in writers {
        if source.field_ids.len() != source.recipe.input().field_count() {
            return Err(Error::InvalidShape(
                "writer field IDs do not cover the original input occurrences",
            )
            .into());
        }
        for id in source.field_ids {
            work.flush()?;
            let unique = reserved_fields.insert(*id);
            work.step()?;
            work.flush()?;
            if !unique {
                return Err(Error::InvalidShape("duplicate field definition ID").into());
            }
        }
    }
    let mut counts = Counts {
        definitions: 0,
        expanded: 0,
        strings: 0,
        carriers: 0,
        fields: 0,
        limits,
        resources,
    };
    // Root definitions alone already consume this many entries; reject before
    // allocating duplicate-ID scratch or visiting a source tree.
    add(
        &mut counts.definitions,
        values.len(),
        limits.max_definitions,
    )?;
    if let Some(resources) = &mut counts.resources {
        resources.model.definition(values.len())?;
        resources.gate()?;
    }
    let mut ids = BTreeSet::new();
    for (id, value) in values.iter() {
        // The root node of this strict walk, before its first callback.
        admit_strict_nodes(&mut counts.resources, 1)?;
        if observed {
            work.flush()?;
        } else {
            work.step()?;
        }
        let unique = ids.insert(id);
        if observed {
            work.step()?;
            work.flush()?;
        }
        if !unique {
            return Err(Error::InvalidShape("duplicate value type definition ID").into());
        }
        value.logical_type().validate_carrier(value.data_type())?;
        validate_strict_type(
            value.data_type(),
            ArrowTypeProfile::CanonicalValue,
            &mut counts.resources,
            work,
        )?;
        let nodes = count_type(value.data_type(), &mut counts, work, SourceLaw::Strict)?;
        counts.expansion(nodes)?;
    }
    for (ordinal, (_, field)) in fields.iter().enumerate() {
        // The root node, whose visit pair is this root Field and its carrier.
        admit_strict_nodes(&mut counts.resources, 1)?;
        work.step()?;
        if let Some(resources) = &mut counts.resources {
            resources.model.strict_field(field)?;
            resources.gate()?;
        }
        validate_field(field, work)?;
        validate_strict_type(
            field.data_type(),
            if fields.value_binding(ordinal).is_some() {
                ArrowTypeProfile::CanonicalValue
            } else {
                ArrowTypeProfile::Physical
            },
            &mut counts.resources,
            work,
        )?;
        #[allow(deprecated)]
        let dictionary_id = field.dict_id();
        if dictionary_id.is_some() != field.dict_is_ordered().is_some() {
            return Err(Error::InvalidShape("incomplete field dictionary attributes").into());
        }
        count_field(field, &mut counts, work, SourceLaw::Strict)?;
    }
    // The immutable Draft already passed the original complete ordered-role
    // Writer law. Borrow every one of those original occurrences, rather than
    // manufacturing a second Field/FVT owner or restarting a per-field quota.
    for source in writers {
        for binding in source.recipe.input().fields_iter() {
            count_field(
                binding.field(),
                &mut counts,
                work,
                SourceLaw::Writer(source.recipe),
            )?;
        }
    }
    Ok((counts, reserved_fields))
}

struct FieldIds {
    reserved: BTreeSet<u32>,
    cursor: u64,
    observed: bool,
}
impl FieldIds {
    fn next(&mut self, work: &mut CompileCheckpoints<'_>) -> Result<u32, Error> {
        // The cursor never resets: across the whole emission, it tests at
        // most reserved-root-count + automatic-field-count identities. MAX
        // reservations do not allocate a sparse array or force a large scan.
        loop {
            work.step()?;
            let id = u32::try_from(self.cursor)
                .map_err(|_| Error::InvalidShape("field definition IDs are exhausted"))?;
            self.cursor += 1;
            if self.observed {
                work.flush()?;
            }
            let reserved = self.reserved.contains(&id);
            if self.observed {
                work.step()?;
                work.flush()?;
            }
            if !reserved {
                return Ok(id);
            }
        }
    }
}

fn output_vec<T>(
    n: usize,
    observed: bool,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<T>, Error> {
    if !observed {
        return Ok(Vec::with_capacity(n));
    }
    let mut output = Vec::new();
    work.flush()?;
    let result = output.try_reserve_exact(n);
    if result.is_ok() {
        work.step()?;
    }
    reserve_exit::<Error>(result, work)?;
    Ok(output)
}

#[derive(Clone, Copy)]
enum Projection {
    Source,
    Node(u32),
    MapEntries(u32, u32),
    EncodingValues(u32),
}
impl Projection {
    fn id(self) -> Option<u32> {
        match self {
            Self::Node(id) => Some(id),
            _ => None,
        }
    }
    fn semantic_id(self) -> Option<u32> {
        match self {
            Self::Node(id) | Self::EncodingValues(id) => Some(id),
            _ => None,
        }
    }
    fn children<'a>(
        self,
        defs: &'a [logical_wire::LogicalTypeDefinition],
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Children<'a>, Error> {
        match self {
            Self::Source => Ok(Children::Scalar),
            Self::MapEntries(key, value) => Ok(Children::Map(key, value)),
            Self::Node(id) | Self::EncodingValues(id) => {
                semantic::children(semantic::definition(defs, id, work)?)
            }
        }
    }
}
fn request<H>(
    resources: &mut Option<Admission<'_, H>>,
    allocation: Layout,
) -> Result<(), AdmissionRefusal<H>> {
    if let Some(resources) = resources {
        resources.model.request(allocation, 1)?;
        let facts = resources.model.facts(resources.limits)?;
        (resources.admit)(&facts)?;
    }
    Ok(())
}
fn borrowed_roots<'source, H>(
    values: ValueRootSources<'source>,
    writers: &[WriterTypeSource<'source>],
    resources: &mut Option<Admission<'_, H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<BorrowedRootBindings<'source>, HostError<H>> {
    let mut writer_count = 0usize;
    for source in writers {
        work.step()?;
        for binding in source.recipe.input().fields_iter() {
            work.step()?;
            if binding.execution_type().is_some() {
                writer_count = writer_count
                    .checked_add(1)
                    .ok_or(CompileControlError::ResourceExhausted)?;
            }
        }
    }
    let total = values
        .len()
        .checked_add(writer_count)
        .ok_or(CompileControlError::ResourceExhausted)?;
    if let Some(resources) = resources {
        resources.model.additional_value_roots(writer_count)?;
        resources.gate()?;
    }
    let mut callback = |allocation| request(resources, allocation);
    let mut context = shared_resources::Context {
        observe_scope: None,
        request: &mut callback,
        work,
    };
    let mut all = context
        .vector::<(u32, &FunctionValueType)>(total)
        .map_err(semantic::failure)?;
    let mut reserved = context
        .vector::<u32>(values.len())
        .map_err(semantic::failure)?;
    for (id, value) in values.iter() {
        context.step().map_err(semantic::failure)?;
        all.push((id, value));
        reserved.push(id);
    }
    shared_resources::sort_by_id(&mut reserved, |id| *id, &mut context)
        .map_err(semantic::failure)?;
    let mut pairs = context
        .vector::<(u32, u32)>(writer_count)
        .map_err(semantic::failure)?;
    let mut next = 0u64;
    for source in writers {
        for (field_id, binding) in source
            .field_ids
            .iter()
            .zip(source.recipe.input().fields_iter())
        {
            context.step().map_err(semantic::failure)?;
            if let Some(value) = binding.execution_type() {
                let id = loop {
                    let id = u32::try_from(next)
                        .map_err(|_| Error::InvalidShape("writer binding IDs exhausted"))?;
                    next += 1;
                    context.step().map_err(semantic::failure)?;
                    if shared_resources::find(&reserved, id, |id| *id, context.work)?.is_none() {
                        break id;
                    }
                };
                all.push((id, value));
                pairs.push((*field_id, id));
            }
        }
    }
    shared_resources::sort_by_id(&mut pairs, |pair| pair.0, &mut context)
        .map_err(semantic::failure)?;
    Ok((all, pairs))
}

/// Resolve explicit associations against an entry-count index of the real
/// value roots. Neither sparse IDs nor repeated source fields allocate by ID.
fn associated_roots<'source, H>(
    values: ValueRootSources<'source>,
    fields: FieldRootSources<'source>,
    resources: &mut Option<Admission<'_, H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<(u32, &'source FunctionValueType)>, HostError<H>> {
    let mut count = 0usize;
    for ordinal in 0..fields.len() {
        work.step()?;
        count += usize::from(fields.value_binding(ordinal).is_some());
    }
    if count == 0 {
        return Ok(Vec::new());
    }
    let mut callback = |layout| request(resources, layout);
    let mut context = shared_resources::Context {
        observe_scope: None,
        request: &mut callback,
        work,
    };
    let mut indexed = context
        .vector::<(u32, &FunctionValueType)>(values.len())
        .map_err(semantic::failure)?;
    for root in values.iter() {
        context.step().map_err(semantic::failure)?;
        indexed.push(root);
    }
    shared_resources::sort_by_id(&mut indexed, |root| root.0, &mut context)
        .map_err(semantic::failure)?;
    Ok(indexed)
}

fn validate_associated_field<H>(
    field: &Field,
    value: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), HostError<H>> {
    work.step()?;
    #[allow(deprecated)]
    if field.is_nullable() != value.nullable()
        || !field.metadata().is_empty()
        || field.dict_id().is_some()
        || field.dict_is_ordered().is_some()
    {
        return Err(
            Error::InvalidShape("execution field differs from its explicit value binding").into(),
        );
    }
    if !novarocks_type_contract::arrow_data_types_exact_borrowed_with_profile_observed::<Error>(
        field.data_type(),
        value.data_type(),
        ArrowTypeProfile::CanonicalValue,
        || work.step().map_err(Into::into),
    )? {
        return Err(Error::InvalidShape(
            "execution field layout differs from its explicit value binding",
        )
        .into());
    }
    Ok(())
}

/// The actual output DTO under construction. Source attributes are emitted
/// directly beside this shape, never into an intermediate legacy TypeTable.
struct EmitTarget {
    shape: wire::TypeTable,
    source_facts: Option<Vec<wire::SourceArrowFieldFacts>>,
}
impl std::ops::Deref for EmitTarget {
    type Target = wire::TypeTable;
    fn deref(&self) -> &Self::Target {
        &self.shape
    }
}
impl std::ops::DerefMut for EmitTarget {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.shape
    }
}

fn emit_field(
    field: &Field,
    explicit_id: Option<u32>,
    table: &mut EmitTarget,
    ids: &mut FieldIds,
    work: &mut CompileCheckpoints<'_>,
    law: SourceLaw<'_>,
    projection: Projection,
    definitions: &[logical_wire::LogicalTypeDefinition],
) -> Result<u32, Error> {
    #[allow(deprecated)]
    if table.source_facts.is_none()
        && (!field.metadata().is_empty()
            || field.dict_id().is_some()
            || field.dict_is_ordered().is_some())
    {
        return Err(Error::InvalidShape(
            "execution field cannot carry source Arrow attributes",
        ));
    }
    work.step()?;
    let carrier_type_id = emit_type(
        field.data_type(),
        table,
        ids,
        work,
        law,
        projection,
        definitions,
    )?;
    let id = match explicit_id {
        Some(id) => id,
        None => ids.next(work)?,
    };
    if let Some(source_facts) = table.source_facts.as_mut() {
        if projection.id().is_some() {
            return Err(Error::InvalidShape(
                "source field cannot reference execution identity",
            ));
        }
        #[allow(deprecated)]
        let dictionary_id = field.dict_id();
        let facts = wire::SourceArrowFieldFacts {
            field_id: Some(id),
            metadata: encode_metadata(field.metadata(), work)?,
            dictionary_id,
            dictionary_is_ordered: field.dict_is_ordered(),
        };
        source_facts.push(facts);
    }
    table.fields.push(wire::FieldDefinition {
        id,
        name: copy_string(field.name(), work)?,
        nullable: field.is_nullable(),
        carrier_type_id: Some(carrier_type_id),
        logical_type_id: projection.id(),
        value_binding_id: None,
    });
    Ok(id)
}

fn emit_type(
    ty: &DataType,
    table: &mut EmitTarget,
    ids: &mut FieldIds,
    work: &mut CompileCheckpoints<'_>,
    law: SourceLaw<'_>,
    projection: Projection,
    definitions: &[logical_wire::LogicalTypeDefinition],
) -> Result<u32, Error> {
    work.step()?;
    let position = table.carriers.len();
    let id = u32::try_from(position)
        .map_err(|_| Error::InvalidShape("carrier definition IDs are exhausted"))?;
    // Reserve this occurrence's ID before visiting its ordered children. An
    // unfinished entry is private and discarded on any projection failure.
    table
        .carriers
        .push(wire::CarrierTypeDefinition { id, kind: None });
    let children = projection.children(definitions, work)?;
    let item = match children {
        Children::Item(id) => Projection::Node(id),
        _ => Projection::Source,
    };
    let kind = match ty {
        DataType::List(field) => Kind::ListFieldId(emit_field(
            field,
            None,
            table,
            ids,
            work,
            law,
            item,
            definitions,
        )?),
        DataType::ListView(field) => Kind::ListViewFieldId(emit_field(
            field,
            None,
            table,
            ids,
            work,
            law,
            item,
            definitions,
        )?),
        DataType::LargeList(field) => Kind::LargeListFieldId(emit_field(
            field,
            None,
            table,
            ids,
            work,
            law,
            item,
            definitions,
        )?),
        DataType::LargeListView(field) => Kind::LargeListViewFieldId(emit_field(
            field,
            None,
            table,
            ids,
            work,
            law,
            item,
            definitions,
        )?),
        DataType::FixedSizeList(field, length) => Kind::FixedSizeList(wire::FixedSizeList {
            item_field_id: Some(emit_field(
                field,
                None,
                table,
                ids,
                work,
                law,
                item,
                definitions,
            )?),
            length: *length,
        }),
        DataType::Struct(fields) => {
            let mut field_ids = output_vec(fields.len(), ids.observed, work)?;
            for (ordinal, field) in fields.iter().enumerate() {
                let child = match &children {
                    Children::Struct(fields) => Projection::Node(
                        fields[ordinal]
                            .value_type_id
                            .ok_or(Error::InvalidShape("logical named child absent"))?,
                    ),
                    Children::Map(key, value) => {
                        Projection::Node(if ordinal == 0 { *key } else { *value })
                    }
                    _ => Projection::Source,
                };
                field_ids.push(emit_field(
                    field,
                    None,
                    table,
                    ids,
                    work,
                    law,
                    child,
                    definitions,
                )?);
            }
            Kind::StructType(wire::StructFields { field_ids })
        }
        DataType::Union(fields, mode) => {
            let mut output = output_vec(fields.len(), ids.observed, work)?;
            for (ordinal, (type_id, field)) in fields.iter().enumerate() {
                let child = match &children {
                    Children::Union(fields) => Projection::Node(
                        fields[ordinal]
                            .field
                            .as_ref()
                            .and_then(|field| field.value_type_id)
                            .ok_or(Error::InvalidShape("logical union child absent"))?,
                    ),
                    _ => Projection::Source,
                };
                output.push(wire::UnionField {
                    type_id: i32::from(type_id),
                    field_id: Some(emit_field(
                        field,
                        None,
                        table,
                        ids,
                        work,
                        law,
                        child,
                        definitions,
                    )?),
                });
            }
            Kind::UnionType(wire::UnionFields {
                mode: match mode {
                    UnionMode::Sparse => plan::ArrowUnionMode::Sparse as i32,
                    UnionMode::Dense => plan::ArrowUnionMode::Dense as i32,
                },
                fields: output,
            })
        }
        DataType::Dictionary(key, value) => Kind::Dictionary(wire::DictionaryTypes {
            key_type_id: Some(emit_type(
                key,
                table,
                ids,
                work,
                law,
                Projection::Source,
                definitions,
            )?),
            value_type_id: Some(emit_type(
                value,
                table,
                ids,
                work,
                law,
                projection,
                definitions,
            )?),
        }),
        DataType::Map(entries, ordered) => Kind::Map(wire::MapField {
            entries_field_id: Some(emit_field(
                entries,
                None,
                table,
                ids,
                work,
                law,
                match children {
                    Children::Map(k, v) => Projection::MapEntries(k, v),
                    _ => Projection::Source,
                },
                definitions,
            )?),
            ordered: *ordered,
        }),
        DataType::RunEndEncoded(ends, values) => Kind::RunEndEncoded(wire::RunEndEncodedFields {
            run_ends_field_id: Some(emit_field(
                ends,
                None,
                table,
                ids,
                work,
                law,
                Projection::Source,
                definitions,
            )?),
            values_field_id: Some(emit_field(
                values,
                None,
                table,
                ids,
                work,
                law,
                match projection.semantic_id() {
                    Some(id) => Projection::EncodingValues(id),
                    None => Projection::Source,
                },
                definitions,
            )?),
        }),
        _ => if ids.observed {
            super::scalars::encode_scalar_from_source(ty, law, work)?
        } else {
            super::scalars::encode_scalar(ty)?
        }
        .ok_or(Error::InvalidShape("Arrow type has no v2 carrier variant"))?,
    };
    table.carriers[position].kind = Some(kind);
    Ok(id)
}

pub(super) fn encode_with_fields(
    values: &[(u32, FunctionValueType)],
    fields: &[(u32, Arc<Field>)],
    limits: TypeProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<wire::TypeTable, Error> {
    encode_roots::<std::convert::Infallible>(
        ValueRootSources::Owned(values),
        FieldRootSources::Owned(fields),
        &[],
        limits,
        None,
        work,
        &mut None,
    )
    .map_err(crate::host_projection_v2::ProjectionFailure::without_host)
}

fn encode_roots<'a, H>(
    values: ValueRootSources<'_>,
    fields: FieldRootSources<'_>,
    writers: &[WriterTypeSource<'_>],
    limits: TypeProjectionLimits,
    mut resources: Option<Admission<'a, H>>,
    work: &mut CompileCheckpoints<'_>,
    source_output: &mut Option<Vec<wire::SourceArrowFieldFacts>>,
) -> Result<wire::TypeTable, HostError<H>> {
    let source_mode = source_output.is_some();
    let (all_values, writer_pairs) = borrowed_roots(
        values,
        if source_mode { &[] } else { writers },
        &mut resources,
        work,
    )?;
    let values = ValueRootSources::Borrowed(&all_values);
    let associated_values = associated_roots(values, fields, &mut resources, work)?;
    let (mut counts, reserved) = preflight(values, fields, writers, limits, resources, work)?;
    for (ordinal, (_, field)) in fields.iter().enumerate() {
        work.step()?;
        if let Some(binding_id) = fields.value_binding(ordinal) {
            let position =
                shared_resources::find(&associated_values, binding_id, |root| root.0, work)?
                    .ok_or(Error::InvalidShape(
                        "dangling execution field value binding",
                    ))?;
            let value = associated_values[position].1;
            let mut comparison = semantic::Profile::default();
            comparison.value(value.value_type(), work)?;
            if let Some(resources) = &mut counts.resources {
                resources.model.associated_field_work(comparison.work)?;
                resources.gate()?;
            }
            validate_associated_field(field, value, work)?;
        }
    }
    if let Some(resources) = &mut counts.resources {
        resources
            .model
            .output(counts.carriers, counts.fields, values.len())?;
        if source_mode {
            resources.model.source_output(counts.fields)?;
        }
        resources.gate()?;
    }
    let mut profile = semantic::Profile::default();
    for (_, value) in values.iter() {
        profile.value(value.value_type(), work)?;
    }
    add(
        &mut counts.definitions,
        profile.definitions,
        limits.max_definitions,
    )?;
    add(
        &mut counts.expanded,
        profile.expanded,
        limits.max_expanded_nodes,
    )?;
    add(
        &mut counts.strings,
        profile.strings,
        limits.max_string_bytes,
    )?;
    if let Some(resources) = &mut counts.resources {
        resources.model.semantic_profile(&profile)?;
        resources.gate()?;
    }
    let mut callback = |allocation| request(&mut counts.resources, allocation);
    let mut context = shared_resources::Context {
        observe_scope: None,
        request: &mut callback,
        work,
    };
    let mut roots = context
        .vector::<LogicalTypeRoot>(values.len())
        .map_err(semantic::failure)?;
    for (id, value) in values.iter() {
        context.step().map_err(semantic::failure)?;
        roots.push(LogicalTypeRoot {
            id,
            value: value.value_type(),
        });
    }
    let logical_types = LogicalTypeTableBuilder::new(&roots)
        .encode_observed(
            semantic::limits(limits),
            &mut |layout| (context.request)(layout),
            context.work,
        )
        .map_err(semantic::failure)?;
    work.flush()?;
    let mut ids = FieldIds {
        reserved,
        cursor: 0,
        observed: counts.resources.is_some(),
    };
    let mut table = EmitTarget {
        shape: wire::TypeTable {
            carriers: output_vec(counts.carriers, ids.observed, work)?,
            fields: output_vec(counts.fields, ids.observed, work)?,
            value_types: output_vec(values.len(), ids.observed, work)?,
            logical_types: Vec::new(),
        },
        source_facts: if source_mode {
            Some(output_vec(counts.fields, ids.observed, work)?)
        } else {
            None
        },
    };
    for (id, value) in values.iter() {
        work.step()?;
        let carrier_type_id = emit_type(
            value.data_type(),
            &mut table,
            &mut ids,
            work,
            SourceLaw::Strict,
            Projection::Node(id),
            &logical_types,
        )?;
        table.value_types.push(wire::ValueTypeDefinition {
            id,
            carrier_type_id: Some(carrier_type_id),

            logical_type_id: Some(id),
        });
    }
    for (ordinal, (id, field)) in fields.iter().enumerate() {
        work.step()?;
        let binding_id = fields.value_binding(ordinal);
        let projection = match binding_id {
            Some(binding_id) => {
                let position =
                    shared_resources::find(&associated_values, binding_id, |root| root.0, work)?
                        .ok_or(Error::InvalidShape(
                            "dangling execution field value binding",
                        ))?;
                validate_associated_field(field, associated_values[position].1, work)?;
                Projection::Node(binding_id)
            }
            None => Projection::Source,
        };
        let emitted = emit_field(
            field,
            Some(id),
            &mut table,
            &mut ids,
            work,
            SourceLaw::Strict,
            projection,
            &logical_types,
        )?;
        if let Some(binding_id) = binding_id {
            let output = table
                .fields
                .last_mut()
                .expect("just emitted associated field");
            debug_assert_eq!(output.id, emitted);
            output.value_binding_id = Some(binding_id);
        }
    }
    for source in writers {
        for (id, binding) in source
            .field_ids
            .iter()
            .zip(source.recipe.input().fields_iter())
        {
            let projection = if source_mode {
                Projection::Source
            } else {
                match shared_resources::find(&writer_pairs, *id, |pair| pair.0, work)? {
                    Some(position) => Projection::Node(writer_pairs[position].1),
                    None => Projection::Source,
                }
            };
            let emitted = emit_field(
                binding.field(),
                Some(*id),
                &mut table,
                &mut ids,
                work,
                SourceLaw::Writer(source.recipe),
                projection,
                &logical_types,
            )?;
            if let Some(binding_id) = projection.id() {
                // The field facts and the explicitly supplied execution binding
                // are separate roots with an exact source-owned pairing.
                let field = table.fields.last_mut().expect("just emitted writer field");
                debug_assert_eq!(field.id, emitted);
                field.value_binding_id = Some(binding_id);
            }
        }
    }
    table.logical_types = logical_types;
    *source_output = table.source_facts;
    Ok(table.shape)
}

pub(super) fn encode_associated_sources_pure_in(
    values: &[(u32, &FunctionValueType)],
    fields: &[super::BorrowedFieldTypeRoot<'_>],
    limits: TypeProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<wire::TypeTable, Error> {
    encode_roots::<std::convert::Infallible>(
        ValueRootSources::Borrowed(values),
        FieldRootSources::Associated(fields),
        &[],
        limits,
        None,
        work,
        &mut None,
    )
    .map_err(crate::host_projection_v2::ProjectionFailure::without_host)
}

/// A distinct source-only owner uses the same borrowed field emitter. Its
/// attributes never create logical/value definitions or projection references.
pub(super) fn encode_source_fields_pure_in(
    fields: &[(u32, &Arc<Field>)],
    limits: TypeProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<wire::SourceFieldTable, Error> {
    let mut facts = Some(Vec::new());
    let shape = encode_roots::<std::convert::Infallible>(
        ValueRootSources::Borrowed(&[]),
        FieldRootSources::Borrowed(fields),
        &[],
        limits,
        None,
        work,
        &mut facts,
    )
    .map_err(crate::host_projection_v2::ProjectionFailure::without_host)?;
    Ok(wire::SourceFieldTable {
        shape: Some(shape),
        field_facts: facts.expect("source facts emitted in the sole body"),
    })
}

pub(super) fn encode_source_fields_with_creator<H>(
    fields: &[(u32, &Arc<Field>)],
    source: usize,
    limits: PackageTypeProjectionLimits,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<wire::SourceFieldTable, HostError<H>> {
    let mut previous = 0usize;
    let mut facts = Some(Vec::new());
    let shape = encode_writer_sources_with_host_core(
        ValueRootSources::Borrowed(&[]),
        FieldRootSources::Borrowed(fields),
        &[],
        source,
        limits,
        &mut |facts| {
            let additional = facts
                .allocation_request_bytes_upper_bound
                .checked_sub(previous)
                .ok_or(AdmissionRefusal::Control(
                    CompileControlError::ResourceExhausted,
                ))?;
            if additional != 0 {
                creation
                    .observe_opaque_upper_bound(additional)
                    .map_err(AdmissionRefusal::Host)?;
            }
            previous = facts.allocation_request_bytes_upper_bound;
            Ok(())
        },
        work,
        &mut facts,
    )?;
    Ok(wire::SourceFieldTable {
        shape: Some(shape),
        field_facts: facts.expect("source facts emitted in the sole body"),
    })
}

pub(super) fn encode_writer_sources(
    values: ValueRootSources<'_>,
    fields: FieldRootSources<'_>,
    writers: &[WriterTypeSource<'_>],
    source_retained_bytes: usize,
    limits: PackageTypeProjectionLimits,
    admit: &mut impl FnMut(&PackageTypeProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<wire::TypeTable, Error> {
    encode_writer_sources_with_host(
        values,
        fields,
        writers,
        source_retained_bytes,
        limits,
        &mut |facts| admit(facts).map_err(AdmissionRefusal::<std::convert::Infallible>::Control),
        work,
    )
    .map_err(crate::host_projection_v2::ProjectionFailure::without_host)
}

pub(super) fn encode_writer_sources_with_host<H>(
    values: ValueRootSources<'_>,
    fields: FieldRootSources<'_>,
    writers: &[WriterTypeSource<'_>],
    source_retained_bytes: usize,
    limits: PackageTypeProjectionLimits,
    admit: &mut impl FnMut(&PackageTypeProjectionFacts) -> Result<(), AdmissionRefusal<H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<wire::TypeTable, HostError<H>> {
    encode_writer_sources_with_host_core(
        values,
        fields,
        writers,
        source_retained_bytes,
        limits,
        admit,
        work,
        &mut None,
    )
}

fn encode_writer_sources_with_host_core<H>(
    values: ValueRootSources<'_>,
    fields: FieldRootSources<'_>,
    writers: &[WriterTypeSource<'_>],
    source_retained_bytes: usize,
    limits: PackageTypeProjectionLimits,
    admit: &mut impl FnMut(&PackageTypeProjectionFacts) -> Result<(), AdmissionRefusal<H>>,
    work: &mut CompileCheckpoints<'_>,
    source_output: &mut Option<Vec<wire::SourceArrowFieldFacts>>,
) -> Result<wire::TypeTable, HostError<H>> {
    let writer_fields = writers
        .iter()
        .try_fold(0usize, |count, source| {
            count.checked_add(source.field_ids.len())
        })
        .ok_or(CompileControlError::ResourceExhausted)?;
    let model = Model::new_sources(
        source_retained_bytes,
        values,
        fields,
        writers.len(),
        writer_fields,
        limits,
    )?;
    let mut resources = Admission {
        model,
        limits,
        admit,
    };
    // Numerical roots/source/scratch admission precedes the first source
    // traversal, opaque lookup or allocation, even on a pending caller meter.
    resources.gate()?;
    encode_roots(
        values,
        fields,
        writers,
        TypeProjectionLimits {
            max_definitions: limits.max_definitions,
            max_expanded_nodes: limits.max_expanded_nodes,
            max_string_bytes: limits.max_string_bytes,
        },
        Some(resources),
        work,
        source_output,
    )
}

pub(super) fn encode_source_writer_sources(
    fields: &[(u32, &Arc<Field>)],
    writers: &[WriterTypeSource<'_>],
    source: usize,
    limits: PackageTypeProjectionLimits,
    admit: &mut impl FnMut(&PackageTypeProjectionFacts) -> Result<(), Error>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<wire::SourceFieldTable, Error> {
    let mut facts = Some(Vec::new());
    let shape = encode_writer_sources_with_host_core(
        ValueRootSources::Borrowed(&[]),
        FieldRootSources::Borrowed(fields),
        writers,
        source,
        limits,
        &mut |f| admit(f).map_err(AdmissionRefusal::Host),
        work,
        &mut facts,
    )
    .map_err(|error| match error {
        ProjectionFailure::Codec(error) | ProjectionFailure::Host(error) => error,
    })?;
    Ok(wire::SourceFieldTable {
        shape: Some(shape),
        field_facts: facts.expect("source emitter publishes actual facts"),
    })
}
