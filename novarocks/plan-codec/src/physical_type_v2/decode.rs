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

//! Checked sparse DAG projection. Scratch indexes and Arrow materialization
//! are bounded separately; neither is a host allocation grant.

use super::{
    DecodedTypeTable, Materialized, PackageTypeProjectionFacts, PackageTypeProjectionLimits,
    PackageTypeRootDomain, PackageTypeRootSource, PreparedPackageTypeGraph, TypeCodecError,
    TypeProjectionLimits, decode_resources::TypeDecodeModel, observe_bytes, semantic,
    validate_type_with_profile,
};
use arrow::datatypes::{DataType, Field, UnionFields, UnionMode};
use novarocks_proto_models::{physical_type_v2 as wire, plan};
use novarocks_type_contract::owned_resources::metadata_materialization::MaterializedMetadataMap;
use novarocks_type_contract::{
    ArrowTypeProfile, CompileCheckpoints, CompileControlError, FunctionValueType,
    MAX_ARROW_FIELD_METADATA_BYTES, MAX_ARROW_FIELD_METADATA_ENTRIES,
    MAX_ARROW_FIELD_METADATA_KEY_BYTES, MAX_ARROW_FIELD_METADATA_VALUE_BYTES,
    MAX_ARROW_FIELD_NAME_BYTES, MAX_ARROW_TIMESTAMP_TIMEZONE_BYTES, MAX_VALUE_TYPE_DEPTH,
    MAX_VALUE_TYPE_NODES, TypedValueBinding, ValueLayout, ValueSchemaCreationScope, ValueTypeError,
    ValueTypeStorageLifetime,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use wire::carrier_type_definition::Kind;

use super::graph::{Index, Node, add, required};
use crate::{
    host_projection_v2::{AdmissionRefusal, ProjectionFailure},
    value_type_table::{self, resources as shared_resources},
};
use std::{alloc::Layout, convert::Infallible};

type E = TypeCodecError;
type HostError<H> = ProjectionFailure<E, H>;
type ReceiverAdmit<'a, H> =
    dyn FnMut(&PackageTypeProjectionFacts, usize) -> Result<(), HostError<H>> + 'a;
type PureTypeAdmit<'a> =
    dyn FnMut(&PackageTypeProjectionFacts) -> Result<(), CompileControlError> + 'a;
type TypeGraphTopology = (Vec<Node>, usize, usize, BTreeMap<Node, Summary>);
impl<H> From<crate::value_type_table::LogicalTypeCodecError> for HostError<H> {
    fn from(error: crate::value_type_table::LogicalTypeCodecError) -> Self {
        Self::Codec(error.into())
    }
}
impl<H> From<novarocks_type_contract::ExecutionValueTypeError> for HostError<H> {
    fn from(error: novarocks_type_contract::ExecutionValueTypeError) -> Self {
        Self::Codec(error.into())
    }
}
impl<H> From<novarocks_type_contract::owned_resources::layout::LayoutResourceError>
    for HostError<H>
{
    fn from(error: novarocks_type_contract::owned_resources::layout::LayoutResourceError) -> Self {
        use novarocks_type_contract::owned_resources::layout::LayoutResourceError;
        match error {
            LayoutResourceError::SourceModel => {
                E::ResourceSource("type backing lifetime holder source model drift").into()
            }
            _ => CompileControlError::ResourceExhausted.into(),
        }
    }
}
fn flatten<H>(error: ProjectionFailure<E, HostError<H>>) -> HostError<H> {
    match error {
        ProjectionFailure::Codec(error) => ProjectionFailure::Codec(error),
        ProjectionFailure::Host(error) => error,
    }
}

#[derive(Clone, Copy, Default)]
struct Summary {
    nodes: usize,
    depth: usize,
    logical_depth: usize,
    encoding_depth: usize,
    dictionary_boxes: usize,
    shared_backing: bool,
}

struct ParentFactory<'a, F>(&'a F);
struct ParentScope<S>(S);
impl<H, S: novarocks_type_contract::ValueSchemaCreationScope<H>>
    novarocks_type_contract::ValueSchemaCreationScope<HostError<H>> for ParentScope<S>
{
    fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
        self.0.storage_lifetime()
    }
    fn request_layout(&mut self, layout: Layout) -> Result<(), HostError<H>> {
        self.0
            .request_layout(layout)
            .map_err(ProjectionFailure::Host)
    }
    fn observe_opaque_upper_bound(&mut self, bytes: usize) -> Result<(), HostError<H>> {
        self.0
            .observe_opaque_upper_bound(bytes)
            .map_err(ProjectionFailure::Host)
    }
}
impl<H, F: novarocks_type_contract::ValueSchemaCreationFactory<H>>
    novarocks_type_contract::ValueSchemaCreationFactory<HostError<H>> for ParentFactory<'_, F>
{
    type Scope<'a>
        = ParentScope<F::Scope<'a>>
    where
        Self: 'a;
    fn begin_scope(&self) -> Result<Self::Scope<'_>, HostError<H>> {
        self.0
            .begin_scope()
            .map(ParentScope)
            .map_err(ProjectionFailure::Host)
    }
    fn begin_scope_observed<O>(
        &self,
        observe: &mut dyn FnMut(Layout) -> Result<(), O>,
    ) -> Result<
        Self::Scope<'_>,
        novarocks_type_contract::ValueSchemaScopeBeginFailure<O, HostError<H>>,
    > {
        use novarocks_type_contract::ValueSchemaScopeBeginFailure as Failure;
        self.0
            .begin_scope_observed(observe)
            .map(ParentScope)
            .map_err(|error| match error {
                Failure::Observer(error) => Failure::Observer(error),
                Failure::Creator(error) => Failure::Creator(ProjectionFailure::Host(error)),
            })
    }
}

struct Receiver<'graph, 'source, 'caller, H> {
    graph: Option<&'graph PreparedPackageTypeGraph<'source>>,
    model: TypeDecodeModel,
    limits: Option<PackageTypeProjectionLimits>,
    admit: &'caller mut ReceiverAdmit<'caller, H>,
    scoped_request_bytes: usize,
}
impl<H> Receiver<'_, '_, '_, H> {
    fn gate(&mut self) -> Result<(), HostError<H>> {
        (self.admit)(
            &self.model.facts_in(self.limits)?,
            self.scoped_request_bytes
                .checked_add(self.model.entity_bytes())
                .ok_or(CompileControlError::ResourceExhausted)?,
        )?;
        Ok(())
    }
}
fn strict<H>(
    receiver: Option<&Receiver<'_, '_, '_, H>>,
    node: Node,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, E> {
    match receiver {
        None => Ok(true),
        Some(receiver) => match receiver.graph {
            Some(graph) => Ok(graph.domain(node, work)? != PackageTypeRootDomain::Writer),
            None => Ok(true),
        },
    }
}
// A package chooses from actual occurrences. The standalone/source decoder
// retains its original physical-tree policy; a generic carrier is not a value.
fn depth_profile<H>(
    receiver: Option<&Receiver<'_, '_, '_, H>>,
    node: Node,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<ArrowTypeProfile>, E> {
    match receiver.and_then(|receiver| receiver.graph) {
        Some(graph) => graph.depth_profile(node, work),
        None => Ok(Some(ArrowTypeProfile::Physical)),
    }
}

fn check_summary_depth(summary: Summary, profile: ArrowTypeProfile) -> Result<(), E> {
    profile.check_physical_depth(summary.depth)?;
    if profile != ArrowTypeProfile::Physical && summary.logical_depth > MAX_VALUE_TYPE_DEPTH {
        return Err(ValueTypeError::TooDeep.into());
    }
    match profile {
        ArrowTypeProfile::CanonicalValue if summary.encoding_depth != 0 => Err(
            ValueTypeError::InvalidType("canonical value layout contains runtime encoding").into(),
        ),
        _ => Ok(()),
    }
}

// The old standalone path builds its original Index; a package path borrows
// the very Index whose source domains were prepared, never a second namespace.
enum ReceiverIndex<'source, 'graph> {
    Owned(Index<'source>),
    Package(&'graph Index<'source>),
}
impl<'source> std::ops::Deref for ReceiverIndex<'source, '_> {
    type Target = Index<'source>;
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Owned(index) => index,
            Self::Package(index) => index,
        }
    }
}
fn opaque<T>(
    package: bool,
    work: &mut CompileCheckpoints<'_>,
    operation: impl FnOnce(&mut CompileCheckpoints<'_>) -> Result<T, E>,
) -> Result<T, E> {
    if package {
        work.flush()?;
    }
    let result = operation(work);
    if package && !matches!(&result, Err(E::Control(_))) {
        work.step()?;
        work.flush()?;
    }
    result
}

struct FieldAttributes<'a> {
    metadata: &'a [plan::ArrowFieldMetadataEntry],
    dictionary_id: Option<i64>,
    dictionary_is_ordered: Option<bool>,
}
struct SourceFactsIndex<'source> {
    rows: Vec<(u32, &'source wire::SourceArrowFieldFacts)>,
}
impl<'source> SourceFactsIndex<'source> {
    fn prepare<H>(
        facts: &'source [wire::SourceArrowFieldFacts],
        shape: &wire::TypeTable,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, HostError<H>> {
        // The sole TypeDecodeModel pre-admits this exact Vec and observed
        // sorting work before this constructor is entered.
        work.flush()?;
        let mut rows = Vec::with_capacity(facts.len());
        for facts in facts {
            work.step()?;
            let id = required(facts.field_id)?;
            rows.push((id, facts));
        }
        shared_resources::sort_by_id_work_observed(&mut rows, |row| row.0, &mut || {
            work.step().map_err(E::from)
        })?;
        for pair in rows.windows(2) {
            work.step()?;
            if pair[0].0 == pair[1].0 {
                return Err(E::InvalidShape("duplicate source field facts identity").into());
            }
        }
        if rows.len() != shape.fields.len() {
            return Err(E::InvalidShape(
                "source facts do not cover exactly the actual field definitions",
            )
            .into());
        }
        Ok(Self { rows })
    }
    fn field(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<FieldAttributes<'source>, E> {
        let position = shared_resources::find(&self.rows, id, |row| row.0, work)
            .map_err(E::from)?
            .ok_or(E::InvalidShape("missing source field facts"))?;
        let facts = self.rows[position].1;
        Ok(FieldAttributes {
            metadata: &facts.metadata,
            dictionary_id: facts.dictionary_id,
            dictionary_is_ordered: facts.dictionary_is_ordered,
        })
    }
}
fn field_attributes<'a>(
    field: &'a wire::FieldDefinition,
    source: Option<&SourceFactsIndex<'a>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FieldAttributes<'a>, E> {
    work.step()?;
    match source {
        Some(source) => source.field(field.id, work),
        None => Ok(FieldAttributes {
            metadata: &[],
            dictionary_id: None,
            dictionary_is_ordered: None,
        }),
    }
}

fn preflight<'a, 'graph, H>(
    table: &'a wire::TypeTable,
    limits: TypeProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
    receiver: &mut Option<Receiver<'graph, 'a, '_, H>>,
    source_facts: Option<&SourceFactsIndex<'a>>,
) -> Result<(ReceiverIndex<'a, 'graph>, usize), HostError<H>> {
    let mut definitions = 0;
    add(
        &mut definitions,
        table.carriers.len(),
        limits.max_definitions,
    )?;
    add(&mut definitions, table.fields.len(), limits.max_definitions)?;
    add(
        &mut definitions,
        table.value_types.len(),
        limits.max_definitions,
    )?;
    add(
        &mut definitions,
        table.logical_types.len(),
        limits.max_definitions,
    )?;
    let mut index = match receiver.as_ref() {
        Some(receiver) if receiver.graph.is_some() => {
            ReceiverIndex::Package(receiver.graph.unwrap().index())
        }
        _ => ReceiverIndex::Owned(Index {
            table,
            carriers: BTreeMap::new(),
            fields: BTreeMap::new(),
        }),
    };
    let mut bytes = 0;
    for carrier in &table.carriers {
        work.step()?;
        if match &mut index {
            ReceiverIndex::Owned(index) => index.carriers.insert(carrier.id, carrier).is_some(),
            ReceiverIndex::Package(_) => false,
        } {
            return Err(E::InvalidShape("duplicate carrier type identity").into());
        }
        let kind = carrier
            .kind
            .as_ref()
            .ok_or(E::InvalidShape("missing carrier kind"))?;
        let is_strict = strict(receiver.as_ref(), Node::Carrier(carrier.id), work)?;
        if let Kind::Timestamp(timestamp) = kind
            && let Some(zone) = &timestamp.timezone
        {
            if is_strict && zone.len() > MAX_ARROW_TIMESTAMP_TIMEZONE_BYTES {
                return Err(E::InvalidShape("Arrow timestamp zone exceeds its owner bound").into());
            }
            add(&mut bytes, zone.len(), limits.max_string_bytes)?;
            observe_bytes(zone.as_bytes(), work)?;
        }
        match kind {
            Kind::StructType(fields)
                if is_strict && fields.field_ids.len() >= MAX_VALUE_TYPE_NODES =>
            {
                return Err(ValueTypeError::TooManyNodes.into());
            }
            Kind::UnionType(fields) => {
                if fields.fields.len() > 128 {
                    return Err(E::InvalidShape("too many union fields").into());
                }
                union_mode(fields.mode)?;
                let mut seen = [false; 128];
                for field in &fields.fields {
                    work.step()?;
                    let id = usize::try_from(field.type_id)
                        .ok()
                        .filter(|id| *id < 128)
                        .ok_or(E::InvalidShape("invalid union type identity"))?;
                    if seen[id] {
                        return Err(E::InvalidShape("duplicate union type identity").into());
                    }
                    seen[id] = true;
                }
            }
            Kind::FixedSizeBinary(length)
            | Kind::FixedSizeList(wire::FixedSizeList { length, .. })
                if *length < 0
                    || (is_strict && *length > novarocks_physical_plan::MAX_FIXED_SIZE_LENGTH) =>
            {
                return Err(E::InvalidShape("Arrow fixed size exceeds its owner bound").into());
            }
            _ => {}
        }
    }
    for field in &table.fields {
        work.step()?;
        if match &mut index {
            ReceiverIndex::Owned(index) => index.fields.insert(field.id, field).is_some(),
            ReceiverIndex::Package(_) => false,
        } {
            return Err(E::InvalidShape("duplicate field identity").into());
        }
        let is_strict = strict(receiver.as_ref(), Node::Field(field.id), work)?;
        let attributes = field_attributes(field, source_facts, work)?;
        if is_strict
            && (field.name.len() > MAX_ARROW_FIELD_NAME_BYTES
                || attributes.metadata.len() > MAX_ARROW_FIELD_METADATA_ENTRIES)
        {
            return Err(E::InvalidShape("Arrow field attributes exceed their owner bounds").into());
        }
        add(&mut bytes, field.name.len(), limits.max_string_bytes)?;
        observe_bytes(field.name.as_bytes(), work)?;
        let mut metadata_bytes = 0;
        let mut previous: Option<&str> = None;
        for entry in attributes.metadata {
            work.step()?;
            if is_strict
                && (entry.key.len() > MAX_ARROW_FIELD_METADATA_KEY_BYTES
                    || entry.value.len() > MAX_ARROW_FIELD_METADATA_VALUE_BYTES)
            {
                return Err(
                    E::InvalidShape("Arrow field metadata entry exceeds its owner bound").into(),
                );
            }
            add(
                &mut metadata_bytes,
                entry.key.len(),
                if is_strict {
                    MAX_ARROW_FIELD_METADATA_BYTES
                } else {
                    usize::MAX
                },
            )?;
            add(
                &mut metadata_bytes,
                entry.value.len(),
                if is_strict {
                    MAX_ARROW_FIELD_METADATA_BYTES
                } else {
                    usize::MAX
                },
            )?;
            add(&mut bytes, entry.key.len(), limits.max_string_bytes)?;
            add(&mut bytes, entry.value.len(), limits.max_string_bytes)?;
            observe_bytes(entry.key.as_bytes(), work)?;
            observe_bytes(entry.value.as_bytes(), work)?;
            // Keep the original owner bounds and diagnostic order; strict
            // metadata order is supplied by the sole observed key author.
            if !crate::arrow_metadata_v2::ordered_key(previous, &entry.key, work)? {
                return Err(E::InvalidShape("field metadata must be sorted and unique").into());
            }
            previous = Some(&entry.key);
        }
    }
    let mut values = BTreeMap::new();
    for value in &table.value_types {
        work.step()?;
        let duplicate = opaque(receiver.is_some(), work, |_| {
            Ok(values.insert(value.id, ()).is_some())
        })?;
        if duplicate {
            return Err(E::InvalidShape("duplicate value type identity").into());
        }
        index.carrier(required(value.carrier_type_id)?)?;
        required(value.logical_type_id)?;
    }
    for field in &table.fields {
        work.step()?;
        let attributes = field_attributes(field, source_facts, work)?;
        let dictionary = matches!(
            index.kind(required(field.carrier_type_id)?)?,
            Kind::Dictionary(_)
        );
        if source_facts.is_some()
            && if dictionary {
                attributes.dictionary_id.is_none() || attributes.dictionary_is_ordered.is_none()
            } else {
                attributes.dictionary_id.is_some() || attributes.dictionary_is_ordered.is_some()
            }
        {
            return Err(
                E::InvalidShape("field dictionary attributes do not match its carrier").into(),
            );
        }
    }
    Ok((index, bytes))
}

fn summary_overflow(package: bool, message: &'static str) -> E {
    if package {
        CompileControlError::ResourceExhausted.into()
    } else {
        E::InvalidShape(message)
    }
}

struct Frame {
    node: Node,
    next: usize,
    summary: Summary,
}

// Fields are graph vertices, but not additional Arrow DataType nodes or depth.
// Physical summaries count every carrier; logical summaries omit only the
// scaffold on this Map occurrence and actual encoding wrappers. Repeated
// references contribute again; a shared Struct never changes its own role.
fn topology<H>(
    index: &Index<'_>,
    table: &wire::TypeTable,
    limits: TypeProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
    receiver: &mut Option<Receiver<'_, '_, '_, H>>,
) -> Result<TypeGraphTopology, HostError<H>> {
    let mut summaries = BTreeMap::<Node, Summary>::new();
    let mut active = BTreeMap::<Node, bool>::new();
    let mut order = Vec::new();
    let mut stack = Vec::<Frame>::new();
    let package = receiver.is_some();
    let vertices = table
        .carriers
        .len()
        .checked_add(table.fields.len())
        .ok_or(CompileControlError::ResourceExhausted)?;
    if package {
        opaque(true, work, |_| {
            order
                .try_reserve_exact(vertices)
                .map_err(|_| CompileControlError::ResourceExhausted)?;
            stack
                .try_reserve_exact(vertices)
                .map_err(|_| CompileControlError::ResourceExhausted)?;
            Ok(())
        })?;
    }
    for root in table
        .carriers
        .iter()
        .map(|entry| Node::Carrier(entry.id))
        .chain(table.fields.iter().map(|entry| Node::Field(entry.id)))
    {
        work.step()?;
        if opaque(package, work, |_| Ok(summaries.contains_key(&root)))? {
            continue;
        }
        opaque(package, work, |_| {
            active.insert(root, true);
            Ok(())
        })?;
        // The original raw physical policy remains independent of value
        // declarations. Value and Writer paths use the admitted graph's actual
        // vertices; physical wrappers do not introduce another rejection quota.
        let max_path_vertices = match depth_profile(receiver.as_ref(), root, work)?
            .and_then(ArrowTypeProfile::physical_depth_limit)
        {
            Some(limit) => limit
                .checked_mul(2)
                .ok_or(CompileControlError::ResourceExhausted)?,
            None => vertices,
        };
        stack.push(Frame {
            node: root,
            next: 0,
            summary: Summary::default(),
        });
        while let Some(frame) = stack.last_mut() {
            work.step()?;
            let node = frame.node;
            if frame.next < opaque(package, work, |_| index.child_count(node))? {
                let child = opaque(package, work, |_| index.child(node, frame.next))?;
                work.step()?;
                if let Some(summary) =
                    opaque(package, work, |_| Ok(summaries.get(&child).copied()))?
                {
                    frame.summary.nodes = frame
                        .summary
                        .nodes
                        .checked_add(summary.nodes)
                        .ok_or_else(|| summary_overflow(package, "expanded type size overflow"))?;
                    frame.summary.depth = frame.summary.depth.max(summary.depth);
                    frame.summary.logical_depth =
                        frame.summary.logical_depth.max(summary.logical_depth);
                    frame.summary.encoding_depth =
                        frame.summary.encoding_depth.max(summary.encoding_depth);
                    frame.summary.shared_backing |= summary.shared_backing;
                    frame.summary.dictionary_boxes = frame
                        .summary
                        .dictionary_boxes
                        .checked_add(summary.dictionary_boxes)
                        .ok_or(CompileControlError::ResourceExhausted)?;
                    frame.next += 1;
                    continue;
                }
                if opaque(package, work, |_| Ok(active.insert(child, true)))? == Some(true) {
                    return Err(E::InvalidShape("cyclic type table references").into());
                }
                if stack.len() >= max_path_vertices || (package && stack.len() >= vertices) {
                    return Err(ValueTypeError::TooDeep.into());
                }
                stack.push(Frame {
                    node: child,
                    next: 0,
                    summary: Summary::default(),
                });
            } else {
                let mut summary = frame.summary;
                if matches!(node, Node::Carrier(_)) {
                    summary.nodes = summary
                        .nodes
                        .checked_add(1)
                        .ok_or_else(|| summary_overflow(package, "expanded type size overflow"))?;
                    summary.depth = summary
                        .depth
                        .checked_add(1)
                        .ok_or_else(|| summary_overflow(package, "expanded type depth overflow"))?;
                }
                if let Node::Carrier(id) = node {
                    match index.kind(id)? {
                        Kind::Map(map) => {
                            // Validate this edge before removing its entries
                            // scaffold from semantic depth. The same Struct
                            // reached through any ordinary edge keeps its level.
                            let field = index.field(required(map.entries_field_id)?)?;
                            let Kind::StructType(entries) =
                                index.kind(required(field.carrier_type_id)?)?
                            else {
                                return Err(E::InvalidShape("Map entries must be Struct").into());
                            };
                            if entries.field_ids.len() != 2 {
                                return Err(E::InvalidShape(
                                    "Map entries must contain key and value",
                                )
                                .into());
                            }
                        }
                        Kind::Dictionary(_) | Kind::RunEndEncoded(_) => {
                            summary.encoding_depth = summary
                                .encoding_depth
                                .checked_add(1)
                                .ok_or(CompileControlError::ResourceExhausted)?;
                        }
                        _ => {
                            summary.logical_depth = summary
                                .logical_depth
                                .checked_add(1)
                                .ok_or(CompileControlError::ResourceExhausted)?;
                        }
                    }
                    summary.dictionary_boxes = if matches!(index.kind(id)?, Kind::Dictionary(_)) {
                        summary
                            .dictionary_boxes
                            .checked_add(2)
                            .ok_or(CompileControlError::ResourceExhausted)?
                    } else {
                        0
                    };
                }
                // Freeze the original clone law alongside its topology
                // summary: Dictionary copies Boxes, all other nested carriers
                // clone their original Field/Fields/UnionFields/timezone Arcs.
                match node {
                    Node::Field(_) => summary.shared_backing = true,
                    Node::Carrier(id) => match index.kind(id)? {
                        Kind::StructType(_) | Kind::UnionType(_) => summary.shared_backing = true,
                        Kind::Timestamp(timestamp) => {
                            summary.shared_backing = timestamp.timezone.is_some()
                        }
                        _ => {}
                    },
                }
                if let Some(receiver) = receiver.as_mut() {
                    receiver
                        .model
                        .expanded(summary.nodes, summary.dictionary_boxes)?;
                    receiver.gate()?;
                }
                let is_strict = strict(receiver.as_ref(), node, work)?;
                if is_strict
                    && matches!(node, Node::Carrier(_))
                    && let Some(receiver) = receiver.as_mut()
                {
                    receiver.model.validation(summary.nodes)?;
                    receiver.gate()?;
                }
                if is_strict && summary.nodes > MAX_VALUE_TYPE_NODES {
                    return Err(ValueTypeError::TooManyNodes.into());
                }
                if is_strict {
                    let profile = depth_profile(receiver.as_ref(), node, work)?
                        .ok_or(E::InvalidShape("strict type has no depth profile"))?;
                    check_summary_depth(summary, profile)?;
                }
                if package && !is_strict {
                    // The original Writer depth author protects recursive
                    // clones before Arrow construction. Attributes and exact
                    // per-recipe charges remain with the original field law.
                    let mut charge = 0;
                    novarocks_connector_contract::charge_write_type_header(
                        summary.depth,
                        None,
                        &mut charge,
                    )?;
                }
                opaque(package, work, |_| {
                    summaries.insert(node, summary);
                    Ok(())
                })?;
                opaque(package, work, |_| {
                    if package {
                        active.insert(node, false);
                    } else {
                        active.remove(&node);
                    }
                    Ok(())
                })?;
                order.push(node);
                stack.pop();
            }
        }
    }
    let mut expanded = 0;
    let mut maximum = 0;
    let mut iterator = summaries.values();
    loop {
        let next = opaque(package, work, |_| Ok(iterator.next()))?;
        let Some(summary) = next else {
            break;
        };
        work.step()?;
        maximum = maximum.max(summary.nodes);
        add(&mut expanded, summary.nodes, limits.max_expanded_nodes)?;
    }
    for value in &table.value_types {
        work.step()?;
        let id = required(value.carrier_type_id)?;
        let summary = opaque(package, work, |_| {
            summaries
                .get(&Node::Carrier(id))
                .ok_or(E::InvalidShape("missing carrier summary"))
        })?;
        if let Some(receiver) = receiver.as_mut() {
            receiver
                .model
                .expanded(summary.nodes, summary.dictionary_boxes)?;
            receiver.gate()?;
        }
        add(&mut expanded, summary.nodes, limits.max_expanded_nodes)?;
    }
    Ok((order, maximum, expanded, summaries))
}

fn union_mode(mode: i32) -> Result<UnionMode, E> {
    match plan::ArrowUnionMode::try_from(mode) {
        Ok(plan::ArrowUnionMode::Sparse) => Ok(UnionMode::Sparse),
        Ok(plan::ArrowUnionMode::Dense) => Ok(UnionMode::Dense),
        _ => Err(E::InvalidShape("unknown or unspecified union mode")),
    }
}

fn field_ref(
    fields: &BTreeMap<u32, Materialized<Arc<Field>>>,
    id: u32,
    work: &mut CompileCheckpoints<'_>,
    package: bool,
) -> Result<Arc<Field>, E> {
    work.step()?;
    opaque(package, work, |_| {
        fields
            .get(&id)
            .map(|field| field.value.clone())
            .ok_or(E::InvalidShape("field topology was not materialized"))
    })
}

fn carrier_ref(
    carriers: &BTreeMap<u32, Materialized<DataType>>,
    id: u32,
    work: &mut CompileCheckpoints<'_>,
    package: bool,
) -> Result<DataType, E> {
    let carrier = opaque(package, work, |_| {
        carriers
            .get(&id)
            .ok_or(E::InvalidShape("carrier topology was not materialized"))
    })?;
    super::clone_carrier(&carrier.value, work)
}

fn materialize(
    kind: &Kind,
    carriers: &BTreeMap<u32, Materialized<DataType>>,
    fields: &BTreeMap<u32, Materialized<Arc<Field>>>,
    work: &mut CompileCheckpoints<'_>,
    package: Option<(&PreparedPackageTypeGraph<'_>, u32)>,
) -> Result<DataType, E> {
    let scalar = match package {
        None => super::scalars::decode_scalar(kind)?,
        Some((graph, id)) => super::scalars::decode_scalar_in_package(kind, graph, id, work)?,
    };
    if let Some(ty) = scalar {
        return Ok(ty);
    }
    Ok(match kind {
        Kind::ListFieldId(id) => DataType::List(field_ref(fields, *id, work, package.is_some())?),
        Kind::ListViewFieldId(id) => {
            DataType::ListView(field_ref(fields, *id, work, package.is_some())?)
        }
        Kind::LargeListFieldId(id) => {
            DataType::LargeList(field_ref(fields, *id, work, package.is_some())?)
        }
        Kind::LargeListViewFieldId(id) => {
            DataType::LargeListView(field_ref(fields, *id, work, package.is_some())?)
        }
        Kind::FixedSizeList(value) => DataType::FixedSizeList(
            field_ref(
                fields,
                required(value.item_field_id)?,
                work,
                package.is_some(),
            )?,
            value.length,
        ),
        Kind::StructType(value) => {
            let mut output = Vec::with_capacity(value.field_ids.len());
            for id in &value.field_ids {
                output.push(field_ref(fields, *id, work, package.is_some())?);
            }
            DataType::Struct(opaque(package.is_some(), work, |_| Ok(output.into()))?)
        }
        Kind::UnionType(value) => {
            let mut ids = Vec::with_capacity(value.fields.len());
            let mut output = Vec::with_capacity(value.fields.len());
            for field in &value.fields {
                work.step()?;
                ids.push(
                    i8::try_from(field.type_id)
                        .map_err(|_| E::InvalidShape("invalid union type identity"))?,
                );
                output.push(field_ref(
                    fields,
                    required(field.field_id)?,
                    work,
                    package.is_some(),
                )?);
            }
            DataType::Union(
                opaque(package.is_some(), work, |_| {
                    UnionFields::try_new(ids, output)
                        .map_err(|_| E::InvalidShape("invalid union fields"))
                })?,
                union_mode(value.mode)?,
            )
        }
        Kind::Dictionary(value) => DataType::Dictionary(
            Box::new(carrier_ref(
                carriers,
                required(value.key_type_id)?,
                work,
                package.is_some(),
            )?),
            Box::new(carrier_ref(
                carriers,
                required(value.value_type_id)?,
                work,
                package.is_some(),
            )?),
        ),
        Kind::Map(value) => DataType::Map(
            field_ref(
                fields,
                required(value.entries_field_id)?,
                work,
                package.is_some(),
            )?,
            value.ordered,
        ),
        Kind::RunEndEncoded(value) => DataType::RunEndEncoded(
            field_ref(
                fields,
                required(value.run_ends_field_id)?,
                work,
                package.is_some(),
            )?,
            field_ref(
                fields,
                required(value.values_field_id)?,
                work,
                package.is_some(),
            )?,
        ),
        _ => return Err(E::InvalidShape("scalar carrier was not decoded")),
    })
}

pub(super) fn decode(
    table: &wire::TypeTable,
    limits: TypeProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DecodedTypeTable, E> {
    decode_body::<Infallible, shared_resources::NoFactory>(
        table,
        limits,
        ValueTypeStorageLifetime::default(),
        work,
        None::<Receiver<'_, '_, '_, Infallible>>,
        None,
        None,
        &mut None,
    )
    .map_err(ProjectionFailure::without_host)
}

fn request<H>(
    receiver: &mut Option<Receiver<'_, '_, '_, H>>,
    allocation: Layout,
) -> Result<(), AdmissionRefusal<HostError<H>>> {
    if let Some(receiver) = receiver {
        receiver
            .model
            .request(allocation, 1)
            .map_err(AdmissionRefusal::Control)?;
        receiver.gate().map_err(|error| match error {
            ProjectionFailure::Codec(E::Control(cause)) => AdmissionRefusal::Control(cause),
            other => AdmissionRefusal::Host(other),
        })?;
    }
    Ok(())
}

fn begin_physical_scope<'a, H, F: novarocks_type_contract::ValueSchemaCreationFactory<H>>(
    factory: &'a F,
    receiver: &mut Option<Receiver<'_, '_, '_, H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<F::Scope<'a>, HostError<H>> {
    work.flush()?;
    factory
        .begin_scope_observed(&mut |layout| {
            let receiver = receiver.as_mut().ok_or(E::ResourceSource(
                "scoped physical constructor lacks receiver",
            ))?;
            receiver.model.entity_request(layout, 1)?;
            receiver.gate()
        })
        .map_err(|error| match error {
            novarocks_type_contract::ValueSchemaScopeBeginFailure::Observer(error) => error,
            novarocks_type_contract::ValueSchemaScopeBeginFailure::Creator(error) => {
                HostError::Host(error)
            }
        })
}
fn scoped_request<H>(
    scope: &mut impl ValueSchemaCreationScope<H>,
    layout: Layout,
    receiver: &mut Option<Receiver<'_, '_, '_, H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), HostError<H>> {
    let receiver = receiver.as_mut().ok_or(E::ResourceSource(
        "scoped physical constructor lacks receiver",
    ))?;
    receiver.model.entity_request(layout, 1)?;
    receiver.gate()?;
    work.flush()?;
    scope.request_layout(layout).map_err(HostError::Host)
}
fn borrowed_physical_child<H>(
    node: Node,
    index: &Index<'_>,
    carriers: &BTreeMap<u32, Materialized<DataType>>,
    fields: &BTreeMap<u32, Materialized<Arc<Field>>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ValueTypeStorageLifetime, HostError<H>> {
    if index.child_count(node)? == 0 {
        return Ok(ValueTypeStorageLifetime::pure());
    }
    if index.child_count(node)? != 1 {
        return Err(E::ResourceSource(
            "multiple physical aliases require their actual loan holder",
        )
        .into());
    }
    work.step()?;
    Ok(match index.child(node, 0)? {
        Node::Carrier(id) => {
            &carriers
                .get(&id)
                .ok_or(E::InvalidShape("physical child was not materialized"))?
                .clone_storage
        }
        Node::Field(id) => {
            &fields
                .get(&id)
                .ok_or(E::InvalidShape("physical Field child was not materialized"))?
                .storage
        }
    }
    .clone())
}
fn retain_physical_children<H>(
    node: Node,
    index: &Index<'_>,
    carriers: &BTreeMap<u32, Materialized<DataType>>,
    fields: &BTreeMap<u32, Materialized<Arc<Field>>>,
    scope: &mut impl ValueSchemaCreationScope<H>,
    receiver: &mut Option<Receiver<'_, '_, '_, H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ValueTypeStorageLifetime, HostError<H>> {
    let children = (0..index.child_count(node)?).map(|ordinal| {
        let child = index.child(node, ordinal)?;
        match child {
            Node::Carrier(id) => carriers.get(&id).map(|entry| &entry.clone_storage),
            Node::Field(id) => fields.get(&id).map(|entry| &entry.storage),
        }
        .ok_or_else(|| E::InvalidShape("physical child backing was not materialized").into())
    });
    scope.storage_lifetime().retaining_flat_observed(
        children,
        |layout| {
            let receiver = receiver.as_mut().ok_or(E::ResourceSource(
                "scoped physical constructor lacks receiver",
            ))?;
            receiver.model.entity_request(layout, 1)?;
            receiver.gate()?;
            scope.request_layout(layout).map_err(HostError::Host)
        },
        work,
    )
}

fn layout_cache_request<H>(
    scope: Option<&mut impl ValueSchemaCreationScope<H>>,
    allocation: Layout,
    receiver: &mut Option<Receiver<'_, '_, '_, H>>,
) -> Result<(), HostError<H>> {
    match scope {
        Some(scope) => {
            let parent = receiver
                .as_mut()
                .ok_or(E::ResourceSource("layout scope lacks receiver"))?;
            parent.model.entity_request(allocation, 1)?;
            parent.gate()?;
            scope.request_layout(allocation).map_err(HostError::Host)
        }
        None => request(receiver, allocation).map_err(|error| match error {
            AdmissionRefusal::Control(error) => error.into(),
            AdmissionRefusal::Host(error) => error,
        }),
    }
}

/// Freeze only the existing value-root closure in the original Carrier map.
/// Physical/source-only vertices retain their original role and no layout is
/// invented for unused encodings. The sparse Index remains the sole grammar.
fn materialized_layout<H, F: novarocks_type_contract::ValueSchemaCreationFactory<H>>(
    id: u32,
    index: &Index<'_>,
    carriers: &mut BTreeMap<u32, Materialized<DataType>>,
    factory: Option<&F>,
    original_storage: &ValueTypeStorageLifetime,
    receiver: &mut Option<Receiver<'_, '_, '_, H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ValueLayout, HostError<H>> {
    work.step()?;
    if let Some(layout) = opaque(receiver.is_some(), work, |_| {
        Ok(carriers
            .get(&id)
            .ok_or(E::InvalidShape("value carrier was not materialized"))?
            .layout
            .clone())
    })? {
        return Ok(layout);
    }
    let node = Node::Carrier(id);
    let map = matches!(index.kind(id)?, Kind::Map(_));
    let count = layout_child_count(index, node)?;
    let mut scope = match factory {
        Some(factory) => Some(begin_physical_scope(factory, receiver, work)?),
        None => None,
    };
    let mut children = {
        let mut callback = |allocation| {
            layout_cache_request(scope.as_mut(), allocation, receiver)
                .map_err(AdmissionRefusal::Host)
        };
        let mut context = shared_resources::Context {
            request: &mut callback,
            work,
            observe_scope: None,
        };
        context
            .vector::<ValueLayout>(count)
            .map_err(semantic::failure)
            .map_err(flatten)?
    };
    for ordinal in 0..count {
        work.step()?;
        let child = if map {
            let entries = index.child(node, 0)?;
            let entries = index.child(entries, 0)?;
            index.child(entries, ordinal)?
        } else {
            index.child(node, ordinal)?
        };
        let child = match child {
            Node::Carrier(id) => id,
            Node::Field(_) => match index.child(child, 0)? {
                Node::Carrier(id) => id,
                Node::Field(_) => {
                    return Err(E::InvalidShape("Field child is not a Carrier").into());
                }
            },
        };
        children.push(materialized_layout(
            child,
            index,
            carriers,
            factory,
            original_storage,
            receiver,
            work,
        )?);
    }
    let children = {
        let mut callback = |allocation| {
            layout_cache_request(scope.as_mut(), allocation, receiver)
                .map_err(AdmissionRefusal::Host)
        };
        let mut context = shared_resources::Context {
            request: &mut callback,
            work,
            observe_scope: None,
        };
        context
            .boxed(children)
            .map_err(semantic::failure)
            .map_err(flatten)?
    };
    let original = carriers
        .get(&id)
        .ok_or(E::InvalidShape("value Carrier backing is absent"))?
        .clone_storage
        .clone();
    let storage = if let Some(scope) = scope.as_mut() {
        scope
            .storage_lifetime()
            .retaining_observed(&original, |allocation| {
                scoped_request(scope, allocation, receiver, work)
            })?
    } else {
        original_storage.clone()
    };
    work.flush()?;
    layout_cache_request(
        scope.as_mut(),
        ValueLayout::storage_allocation_layout()?,
        receiver,
    )?;
    let data_type = carrier_ref(carriers, id, work, receiver.is_some())?;
    work.flush()?;
    let layout = ValueLayout::try_from_normalized_arrow_with_children_observed(
        data_type,
        children,
        original,
        storage,
        |units| {
            if let Some(parent) = receiver.as_mut() {
                parent.model.layout_cache_work(units)?;
                parent.gate()?;
            }
            for _ in 0..units {
                work.step()?;
            }
            Ok::<_, HostError<H>>(())
        },
    )?;
    // Existing map entry only: no new tree allocation, new graph or source
    // authority is published. The cached child owners are immutable.
    opaque(receiver.is_some(), work, |_| {
        carriers
            .get_mut(&id)
            .ok_or(E::InvalidShape("value Carrier cache entry is absent"))?
            .layout = Some(layout.clone());
        Ok(())
    })?;
    Ok(layout)
}

fn source_projection_nodes<H>(
    table: &wire::TypeTable,
    index: &Index<'_>,
    receiver: &mut Option<Receiver<'_, '_, '_, H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<BTreeSet<Node>, HostError<H>> {
    let mut paired = false;
    for field in &table.fields {
        work.step()?;
        paired |= field.value_binding_id.is_some();
    }
    let mut seen = BTreeSet::new();
    if !paired {
        return Ok(seen);
    }
    let vertices = index.vertex_count()?;
    let mut edges = 0usize;
    for carrier in &table.carriers {
        work.step()?;
        edges = edges
            .checked_add(index.child_count(Node::Carrier(carrier.id))?)
            .ok_or(CompileControlError::ResourceExhausted)?;
    }
    edges = edges
        .checked_add(table.fields.len())
        .ok_or(CompileControlError::ResourceExhausted)?;
    if let Some(receiver) = receiver.as_mut() {
        receiver.model.source_projection_graph(vertices, edges)?;
        receiver.gate()?;
    }
    let package = receiver.is_some();
    let mut stack = Vec::new();
    opaque(package, work, |_| {
        stack
            .try_reserve_exact(vertices)
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        Ok(())
    })?;
    for field in &table.fields {
        work.step()?;
        if field.value_binding_id.is_some() {
            let root = Node::Field(field.id);
            if opaque(package, work, |_| Ok(seen.insert(root)))? {
                stack.push(root);
            }
        }
    }
    while let Some(node) = stack.pop() {
        work.step()?;
        for ordinal in 0..opaque(package, work, |_| index.child_count(node))? {
            work.step()?;
            let child = opaque(package, work, |_| index.child(node, ordinal))?;
            if opaque(package, work, |_| Ok(seen.insert(child)))? {
                stack.push(child);
            }
        }
    }
    Ok(seen)
}

/// One propagation over the already validated physical DAG. This records
/// wire reference presence only; attributes, carrier vocabulary and value
/// definitions never manufacture semantic meaning for a source field role.
fn source_role_flags<H>(
    index: &Index<'_>,
    order: &[Node],
    receiver: &mut Option<Receiver<'_, '_, '_, H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<(u32, bool)>, HostError<H>> {
    let vertices = index.vertex_count()?;
    let mut edges = 0usize;
    for node in order {
        work.step()?;
        edges = edges
            .checked_add(index.child_count(*node)?)
            .ok_or(CompileControlError::ResourceExhausted)?;
    }
    if let Some(receiver) = receiver.as_mut() {
        receiver.model.source_role_work(vertices, edges)?;
        receiver.gate()?;
    }
    let (mut marks, mut fields) = {
        let mut callback = |layout| request(receiver, layout);
        let mut context = shared_resources::Context {
            observe_scope: None,
            request: &mut callback,
            work,
        };
        let marks = context
            .vector::<(Node, bool)>(vertices)
            .map_err(semantic::failure)
            .map_err(flatten)?;
        let fields = context
            .vector::<(u32, bool)>(index.fields.len())
            .map_err(semantic::failure)
            .map_err(flatten)?;
        (marks, fields)
    };
    // The two existing sparse indices already author stable sorted keys.
    for id in index.carriers.keys() {
        work.step()?;
        marks.push((Node::Carrier(*id), false));
    }
    for (id, field) in &index.fields {
        work.step()?;
        marks.push((
            Node::Field(*id),
            field.logical_type_id.is_some() || field.value_binding_id.is_some(),
        ));
    }
    fn position<H>(
        marks: &[(Node, bool)],
        node: Node,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<usize, HostError<H>> {
        let (mut low, mut high) = (0, marks.len());
        while low < high {
            let middle = low + (high - low) / 2;
            work.step()?;
            match marks[middle].0.cmp(&node) {
                std::cmp::Ordering::Less => low = middle + 1,
                std::cmp::Ordering::Greater => high = middle,
                std::cmp::Ordering::Equal => return Ok(middle),
            }
        }
        Err(E::InvalidShape("validated source graph node is absent").into())
    }
    for node in order {
        work.step()?;
        let own = position::<H>(&marks, *node, work)?;
        for ordinal in 0..index.child_count(*node)? {
            work.step()?;
            let child = index.child(*node, ordinal)?;
            let child = position::<H>(&marks, child, work)?;
            let child_has_refs = marks[child].1;
            marks[own].1 |= child_has_refs;
        }
    }
    for (node, has_refs) in marks {
        work.step()?;
        if let Node::Field(id) = node {
            fields.push((id, has_refs));
        }
    }
    Ok(fields)
}

/// Forecast only requests introduced by the existing physical materializer
/// after the final numeric snapshot. This grants no capacity and mutates none
/// of the original receiver/creator counters. Layout caching visits each
/// declared Carrier at most once; unused definitions are a finite conservative
/// upper, never a policy cap or another decode/materialization pass.
fn physical_tail_funding_facts<H, F: novarocks_type_contract::ValueSchemaCreationFactory<H>>(
    mut facts: super::PackageTypeProjectionFacts,
    table: &wire::TypeTable,
    index: &Index<'_>,
    order: &[Node],
    summaries: &BTreeMap<Node, Summary>,
    factory: Option<&F>,
) -> Result<super::PackageTypeProjectionFacts, HostError<H>> {
    let original_requests = facts.allocation_requests_upper_bound;
    let mut add_request = |allocation: Layout, count: usize| -> Result<(), HostError<H>> {
        if allocation.size() == 0 || count == 0 {
            return Ok(());
        }
        let bytes = allocation
            .size()
            .checked_mul(count)
            .ok_or(CompileControlError::ResourceExhausted)?;
        facts.allocation_requests_upper_bound = facts
            .allocation_requests_upper_bound
            .checked_add(count)
            .ok_or(CompileControlError::ResourceExhausted)?;
        facts.allocation_request_bytes_upper_bound = facts
            .allocation_request_bytes_upper_bound
            .checked_add(bytes)
            .ok_or(CompileControlError::ResourceExhausted)?;
        facts.coexisting_source_and_request_bytes_upper_bound = facts
            .coexisting_source_and_request_bytes_upper_bound
            .checked_add(bytes)
            .ok_or(CompileControlError::ResourceExhausted)?;
        let work_upper = TypeDecodeModel::request_work_upper(allocation, count)?;
        facts.cumulative_work_upper_bound = facts
            .cumulative_work_upper_bound
            .checked_add(work_upper)
            .ok_or(CompileControlError::ResourceExhausted)?;
        Ok(())
    };
    for &node in order {
        let children = index.child_count(node)?;
        if let Some(factory) = factory {
            let summary = summaries
                .get(&node)
                .ok_or(E::InvalidShape("physical funding summary is absent"))?;
            let (own_scope, loan_scope, dictionary) = match node {
                Node::Field(_) => (true, false, false),
                Node::Carrier(id) => {
                    let kind = index.kind(id)?;
                    let upper =
                        TypeDecodeModel::carrier_entity_upper(kind, summary.dictionary_boxes)?;
                    let dictionary = matches!(kind, Kind::Dictionary(_));
                    (
                        upper != 0,
                        summary.shared_backing && (dictionary || (upper == 0 && children > 1)),
                        dictionary,
                    )
                }
            };
            if own_scope {
                factory.visit_scope_allocation_layouts(&mut |layout| add_request(layout, 1))?;
            }
            if loan_scope {
                factory.visit_scope_allocation_layouts(&mut |layout| add_request(layout, 1))?;
            }
            if loan_scope || (!dictionary && own_scope) {
                ValueTypeStorageLifetime::retaining_flat_allocation_layouts_upper(
                    children,
                    &mut |layout| add_request(layout, 1),
                )?;
            }
            if dictionary && own_scope {
                add_request(ValueTypeStorageLifetime::retaining_allocation_layout()?, 1)?;
            }
        }
        if let Node::Carrier(_) = node
            && !table.value_types.is_empty()
        {
            // The sole cache author owns one scope, Vec/possible trim, shared
            // Arrow loan, and ValueLayout holder per reached Carrier. Map's
            // key/value count is the same frozen projection as that author.
            let child_layouts = layout_child_count(index, node)?;
            if let Some(factory) = factory {
                factory.visit_scope_allocation_layouts(&mut |layout| add_request(layout, 1))?;
                add_request(ValueTypeStorageLifetime::retaining_allocation_layout()?, 1)?;
            }
            add_request(
                Layout::array::<ValueLayout>(child_layouts)
                    .map_err(|_| CompileControlError::ResourceExhausted)?,
                2,
            )?;
            add_request(ValueLayout::storage_allocation_layout()?, 1)?;
            let summary = summaries
                .get(&node)
                .ok_or(E::InvalidShape("layout funding summary is absent"))?;
            // clone_carrier copies only Dictionary Boxes; other descendants
            // share their original Field/Fields/UnionFields/timezone Arcs.
            add_request(Layout::new::<DataType>(), summary.dictionary_boxes)?;
        }
    }
    if factory.is_some() {
        ValueTypeStorageLifetime::retaining_flat_allocation_layouts_upper(
            order.len(),
            &mut |layout| add_request(layout, 1),
        )?;
    }
    facts.cumulative_work_upper_bound = facts
        .cumulative_work_upper_bound
        .checked_add(TypeDecodeModel::funding_projection_work(
            order.len(),
            facts
                .allocation_requests_upper_bound
                .checked_sub(original_requests)
                .ok_or(CompileControlError::ResourceExhausted)?,
        )?)
        .ok_or(CompileControlError::ResourceExhausted)?;
    Ok(facts)
}

fn layout_child_count(index: &Index<'_>, node: Node) -> Result<usize, E> {
    match node {
        Node::Carrier(id) if matches!(index.kind(id)?, Kind::Map(_)) => Ok(2),
        _ => index.child_count(node),
    }
}

fn decode_body<'source, H, F: novarocks_type_contract::ValueSchemaCreationFactory<H>>(
    table: &'source wire::TypeTable,
    limits: TypeProjectionLimits,
    storage: ValueTypeStorageLifetime,
    work: &mut CompileCheckpoints<'_>,
    mut receiver: Option<Receiver<'_, 'source, '_, H>>,
    factory: Option<&F>,
    source_facts: Option<&SourceFactsIndex<'source>>,
    funding: &mut Option<&mut dyn super::PackageTypeFundingScope<H>>,
) -> Result<DecodedTypeTable, HostError<H>> {
    let (index, physical_strings) = preflight(table, limits, work, &mut receiver, source_facts)?;
    // Unique facts IDs + equal cardinality + lookup of every actual Field
    // prove complete coverage, including unused vertices and dangling rows.
    let (order, maximum, physical_expanded, summaries) =
        topology(&index, table, limits, work, &mut receiver)?;
    let field_execution_refs = source_role_flags(&index, &order, &mut receiver, work)?;
    let numeric = receiver
        .as_ref()
        .and_then(|receiver| receiver.limits)
        .map_or(limits, |limits| TypeProjectionLimits {
            max_definitions: limits.max_definitions,
            max_expanded_nodes: limits.max_expanded_nodes,
            max_string_bytes: limits.max_string_bytes,
        });
    let physical_definitions = table
        .carriers
        .len()
        .checked_add(table.fields.len())
        .and_then(|n| n.checked_add(table.value_types.len()))
        .ok_or(CompileControlError::ResourceExhausted)?;
    let logical_limits = value_type_table::LogicalTypeTableLimits {
        max_definitions: numeric
            .max_definitions
            .checked_sub(physical_definitions)
            .ok_or(CompileControlError::ResourceExhausted)?,
        max_expanded_nodes: numeric
            .max_expanded_nodes
            .checked_sub(physical_expanded)
            .ok_or(CompileControlError::ResourceExhausted)?,
        max_string_bytes: numeric
            .max_string_bytes
            .checked_sub(physical_strings)
            .ok_or(CompileControlError::ResourceExhausted)?,
    };
    let logical = if let Some(factory) = factory {
        let parent = ParentFactory(factory);
        let mut observe = |allocation: Layout| {
            let receiver = receiver.as_mut().ok_or_else(|| {
                shared_resources::invalid("scoped package lacks its original numeric receiver")
            })?;
            receiver.model.request(allocation, 1).map_err(|cause| {
                crate::host_projection_v2::ProjectionFailure::Host(HostError::from(cause))
            })?;
            receiver.scoped_request_bytes = receiver
                .scoped_request_bytes
                .checked_add(allocation.size())
                .ok_or_else(|| {
                    crate::host_projection_v2::ProjectionFailure::Host(HostError::from(
                        CompileControlError::ResourceExhausted,
                    ))
                })?;
            // The original caps observe all operations, including the exact
            // scope holder, before its actual independent creator request.
            receiver.model.facts_in(receiver.limits).map_err(|cause| {
                crate::host_projection_v2::ProjectionFailure::Host(HostError::from(cause))
            })?;
            Ok(())
        };
        value_type_table::decode_logical_type_table_with_factory_observed_in(
            &table.logical_types,
            logical_limits,
            &parent,
            &mut observe,
            work,
        )
        .map_err(semantic::failure)
        .map_err(flatten)?
    } else {
        let mut callback = |allocation| request(&mut receiver, allocation);
        value_type_table::decode_logical_type_table_observed(
            &table.logical_types,
            logical_limits,
            storage.clone(),
            &mut callback,
            work,
        )
        .map_err(semantic::failure)
        .map_err(flatten)?
    };
    let definitions = {
        let mut callback = |allocation| request(&mut receiver, allocation);
        let mut context = shared_resources::Context {
            observe_scope: None,
            request: &mut callback,
            work,
        };
        semantic::DefinitionIndex::new(&table.logical_types, &mut context)
            .map_err(semantic::failure)
            .map_err(flatten)?
    };
    let profile = semantic::Profile::decoded(&logical, &table.logical_types, work)?;
    let mut expanded = physical_expanded;
    add(&mut expanded, profile.expanded, limits.max_expanded_nodes)?;
    let mut strings = physical_strings;
    add(&mut strings, profile.strings, limits.max_string_bytes)?;
    if let Some(receiver) = receiver.as_mut() {
        receiver.model.semantic_profile(&profile)?;
        receiver.gate()?;
    }
    for value in &table.value_types {
        work.step()?;
        let id = required(value.logical_type_id)?;
        definitions.get(id, work)?;
        semantic::validate_projection(
            &index,
            &definitions,
            required(value.carrier_type_id)?,
            id,
            false,
            work,
        )?;
    }
    let binding_index = {
        let mut callback = |allocation| request(&mut receiver, allocation);
        let mut context = shared_resources::Context {
            observe_scope: None,
            request: &mut callback,
            work,
        };
        let mut bindings = context
            .vector::<(u32, &wire::ValueTypeDefinition)>(table.value_types.len())
            .map_err(semantic::failure)
            .map_err(flatten)?;
        for binding in &table.value_types {
            context.step().map_err(semantic::failure).map_err(flatten)?;
            bindings.push((binding.id, binding));
        }
        shared_resources::sort_by_id(&mut bindings, |entry| entry.0, &mut context)
            .map_err(semantic::failure)
            .map_err(flatten)?;
        bindings
    };
    for field in &table.fields {
        work.step()?;
        if let Some(id) = field.logical_type_id
            && definitions.get(id, work)?.nullable != field.nullable
        {
            return Err(E::InvalidShape("field nullability conflicts with logical owner").into());
        }
        if let Some(binding_id) = field.value_binding_id {
            // Explicit binding identity selects physical layout independently
            // of the semantic identity, including shared semantic definitions.
            let position =
                shared_resources::find(&binding_index, binding_id, |entry| entry.0, work)?
                    .ok_or(E::InvalidShape("dangling field value-binding reference"))?;
            let binding = binding_index[position].1;
            if field.logical_type_id != binding.logical_type_id
                || field.nullable
                    != definitions
                        .get(required(binding.logical_type_id)?, work)?
                        .nullable
            {
                return Err(E::InvalidShape("source field projection references conflict").into());
            }
            semantic::validate_projection(
                &index,
                &definitions,
                required(field.carrier_type_id)?,
                required(field.logical_type_id)?,
                true,
                work,
            )?;
        }
    }

    let source_nodes = source_projection_nodes(table, &index, &mut receiver, work)?;
    for field in &table.fields {
        work.step()?;
        if let Some(logical_id) = field.logical_type_id {
            let covered = opaque(receiver.is_some(), work, |_| {
                Ok(source_nodes.contains(&Node::Field(field.id)))
            })?;
            if !covered {
                semantic::validate_projection(
                    &index,
                    &definitions,
                    required(field.carrier_type_id)?,
                    logical_id,
                    false,
                    work,
                )?;
            }
        }
    }

    let package = receiver.is_some();
    if let Some(receiver) = receiver.as_mut() {
        let writer_roots = receiver
            .graph
            .map_or(0, |graph| graph.roots().facts().writer_field_root_count);
        // An unfolded Arrow tree has at most one Field per child type edge,
        // plus its root Field. Count both occurrence kinds, including repeats.
        let occurrences = writer_roots
            .checked_mul(maximum)
            .and_then(|n| n.checked_mul(2))
            .ok_or(CompileControlError::ResourceExhausted)?;
        receiver.model.writer_roots(
            writer_roots,
            occurrences,
            receiver
                .graph
                .map_or(0, |graph| graph.roots().facts().cumulative_work_upper_bound),
        )?;
        receiver.gate()?;
    }
    // No original physical carrier or field metadata was allocated above. Observe
    // the admitted graph tail before the first materialization.
    work.flush()?;
    let namespace = if source_facts.is_some() {
        super::TypeMaterializationNamespace::Source
    } else {
        super::TypeMaterializationNamespace::ExecutionPhysical
    };
    let funding_facts = if funding.is_some() {
        receiver
            .as_ref()
            .map(|receiver| receiver.model.facts_in(receiver.limits))
            .transpose()?
            .map(|facts| {
                physical_tail_funding_facts(facts, table, &index, &order, &summaries, factory)
            })
            .transpose()?
    } else {
        None
    };
    let mut original_body = Some(|| -> Result<DecodedTypeTable, HostError<H>> {
        let mut carriers = BTreeMap::new();
        let mut fields = BTreeMap::new();
        let mut values = BTreeMap::new();
        let mut field_loans = if package {
            let mut loans = Vec::new();
            loans
                .try_reserve_exact(table.fields.len())
                .map_err(|_| E::from(CompileControlError::ResourceExhausted))?;
            work.step()?;
            Some(loans)
        } else {
            None
        };
        for node in order {
            work.step()?;
            match node {
                Node::Carrier(id) => {
                    let summary = summaries
                        .get(&node)
                        .ok_or(E::InvalidShape("physical carrier summary is absent"))?;
                    let kind = index.kind(id)?;
                    let upper =
                        TypeDecodeModel::carrier_entity_upper(kind, summary.dictionary_boxes)?;
                    let mut scope = match factory {
                        Some(factory) if upper != 0 => {
                            Some(begin_physical_scope(factory, &mut receiver, work)?)
                        }
                        _ => None,
                    };
                    if let Some(scope) = scope.as_mut() {
                        work.flush()?;
                        scope
                            .observe_opaque_upper_bound(upper)
                            .map_err(HostError::Host)?;
                    }
                    let dictionary = matches!(kind, Kind::Dictionary(_));
                    let separate_loan = summary.shared_backing
                        && (dictionary || (upper == 0 && index.child_count(node)? > 1));
                    let mut loan_scope = match factory {
                        Some(factory) if separate_loan => {
                            Some(begin_physical_scope(factory, &mut receiver, work)?)
                        }
                        _ => None,
                    };
                    let cloned_backing = if factory.is_none() {
                        storage.clone()
                    } else if let Some(loan) = loan_scope.as_mut() {
                        retain_physical_children(
                            node,
                            &index,
                            &carriers,
                            &fields,
                            loan,
                            &mut receiver,
                            work,
                        )?
                    } else if !dictionary && let Some(scope) = scope.as_mut() {
                        retain_physical_children(
                            node,
                            &index,
                            &carriers,
                            &fields,
                            scope,
                            &mut receiver,
                            work,
                        )?
                    } else if dictionary {
                        // No shared descendant exists; the copied Boxes belong
                        // only to the new Dictionary constructor, not the source.
                        ValueTypeStorageLifetime::pure()
                    } else {
                        borrowed_physical_child(node, &index, &carriers, &fields, work)?
                    };
                    let backing = if dictionary && let Some(scope) = scope.as_mut() {
                        scope
                            .storage_lifetime()
                            .retaining_observed(&cloned_backing, |layout| {
                                scoped_request(scope, layout, &mut receiver, work)
                            })?
                    } else {
                        cloned_backing.clone()
                    };
                    let graph = receiver
                        .as_ref()
                        .and_then(|r| r.graph.map(|graph| (graph, id)));
                    let ty = opaque(package, work, |work| {
                        materialize(index.kind(id)?, &carriers, &fields, work, graph)
                    })?;
                    if strict(receiver.as_ref(), node, work)? {
                        let profile = depth_profile(receiver.as_ref(), node, work)?
                            .ok_or(E::InvalidShape("strict type has no depth profile"))?;
                        validate_type_with_profile(&ty, profile, work)?;
                    } else {
                        novarocks_type_contract::validate_arrow_carrier_parameters_observed(
                            &ty,
                            || work.step().map_err(E::from),
                        )?;
                    }
                    opaque(package, work, |_| {
                        carriers.insert(
                            id,
                            Materialized {
                                layout: None,
                                value: ty,
                                storage: backing,
                                clone_storage: cloned_backing,
                            },
                        );
                        Ok(())
                    })?;
                }
                Node::Field(id) => {
                    let source = index.field(id)?;
                    let attributes = field_attributes(source, source_facts, work)?;
                    let mut scope = match factory {
                        Some(factory) => Some(begin_physical_scope(factory, &mut receiver, work)?),
                        None => None,
                    };
                    let backing = if let Some(scope) = scope.as_mut() {
                        let summary = summaries
                            .get(&node)
                            .ok_or(E::InvalidShape("physical Field summary is absent"))?;
                        let upper = TypeDecodeModel::field_entity_upper(
                            source,
                            attributes.metadata,
                            summary.dictionary_boxes,
                        )?;
                        work.flush()?;
                        scope
                            .observe_opaque_upper_bound(upper)
                            .map_err(HostError::Host)?;
                        retain_physical_children(
                            node,
                            &index,
                            &carriers,
                            &fields,
                            scope,
                            &mut receiver,
                            work,
                        )?
                    } else {
                        storage.clone()
                    };
                    let ty =
                        carrier_ref(&carriers, required(source.carrier_type_id)?, work, package)?;
                    observe_bytes(source.name.as_bytes(), work)?;
                    let field = opaque(package, work, |_| {
                        Ok(
                            match (attributes.dictionary_id, attributes.dictionary_is_ordered) {
                                #[allow(deprecated)]
                                (Some(id), Some(ordered)) => Field::new_dict(
                                    source.name.clone(),
                                    ty,
                                    source.nullable,
                                    id,
                                    ordered,
                                ),
                                (None, None) => {
                                    Field::new(source.name.clone(), ty, source.nullable)
                                }
                                _ => {
                                    return Err(E::InvalidShape(
                                        "incomplete field dictionary attributes",
                                    ));
                                }
                            },
                        )
                    })?;
                    let mut metadata = opaque(package, work, |_| {
                        Ok(MaterializedMetadataMap::with_capacity(
                            attributes.metadata.len(),
                        ))
                    })?;
                    for entry in attributes.metadata {
                        work.step()?;
                        observe_bytes(entry.key.as_bytes(), work)?;
                        observe_bytes(entry.value.as_bytes(), work)?;
                        opaque(package, work, |_| {
                            metadata.insert(entry.key.clone(), entry.value.clone());
                            Ok(())
                        })?;
                    }
                    let field = metadata.into_field(field);
                    opaque(package, work, |_| {
                        let field = field.into_shared();
                        if let Some(loans) = field_loans.as_mut() {
                            loans.push(field.loan());
                        }
                        fields.insert(
                            id,
                            Materialized {
                                layout: None,
                                value: field.into_original_field_ref(),
                                clone_storage: backing.clone(),
                                storage: backing,
                            },
                        );
                        Ok(())
                    })?;
                }
            }
        }
        for source in &table.value_types {
            work.step()?;
            let logical_id = required(source.logical_type_id)?;
            let carrier_id = required(source.carrier_type_id)?;
            let layout = materialized_layout(
                carrier_id,
                &index,
                &mut carriers,
                factory,
                &storage,
                &mut receiver,
                work,
            )?;
            work.step()?;
            work.flush()?;
            let value_type = logical
                .get_observed(logical_id, work)?
                .ok_or(E::InvalidShape("logical root was not materialized"))?;
            work.flush()?;
            let value_type = value_type.clone();
            work.step()?;
            work.flush()?;
            let binding = TypedValueBinding::try_new_observed(value_type, layout, |units| {
                for _ in 0..units {
                    work.step()?;
                }
                Ok::<_, E>(())
            })?;
            let value = FunctionValueType::try_from_typed_binding(binding)?;
            opaque(package, work, |_| {
                values.insert(source.id, value);
                Ok(())
            })?;
        }
        for source in &table.fields {
            work.step()?;
            if let Some(logical_id) = source.logical_type_id {
                let covered = opaque(package, work, |_| {
                    Ok(source_nodes.contains(&Node::Field(source.id)))
                })?;
                if !covered {
                    let value_type = logical
                        .get_observed(logical_id, work)?
                        .ok_or(E::InvalidShape("logical field was not materialized"))?;
                    let field = opaque(package, work, |_| {
                        fields.get(&source.id).ok_or(E::InvalidShape(
                            "logical field carrier was not materialized",
                        ))
                    })?;
                    novarocks_type_contract::validate_value_layout_observed(
                        value_type,
                        field.data_type(),
                        |units| {
                            for _ in 0..units {
                                work.step()?;
                            }
                            Ok::<_, E>(())
                        },
                    )?;
                }
            }
        }
        let mut field_values = BTreeMap::new();
        for source in &table.fields {
            work.step()?;
            if let Some(binding_id) = source.value_binding_id {
                let value = opaque(package, work, |_| {
                    values.get(&binding_id).ok_or(E::InvalidShape(
                        "paired execution binding was not materialized",
                    ))
                })?;
                let field = opaque(package, work, |_| {
                    fields
                        .get(&source.id)
                        .ok_or(E::InvalidShape("paired source field was not materialized"))
                })?;
                if !novarocks_connector_contract::write_field_matches_execution_type_observed::<E>(
                    field,
                    value,
                    |units| {
                        for _ in 0..units {
                            work.step()?;
                        }
                        Ok(())
                    },
                )? {
                    return Err(E::InvalidShape(
                        "source field conflicts with explicitly paired execution layout",
                    )
                    .into());
                }
                let value = value.clone_observed(|| work.step().map_err(E::from))?;
                opaque(package, work, |_| {
                    field_values.insert(source.id, value);
                    Ok(())
                })?;
            }
        }
        if let Some(graph) = receiver.as_ref().and_then(|receiver| receiver.graph) {
            let mut previous_recipe = None;
            let mut decoded_bytes = 0;
            graph.roots().visit::<E>(
            &mut |root, work| {
                if let PackageTypeRootSource::WriterField {
                    recipe, binding, ..
                } = root
                {
                    if previous_recipe.is_none_or(|previous| !std::ptr::eq(previous, recipe)) {
                        previous_recipe = Some(recipe);
                        decoded_bytes = 0;
                    }
                    let id = required(binding.source_field_id)?;
                    let field = opaque(true, work, |_| {
                        fields.get(&id).ok_or(E::InvalidShape(
                            "writer field topology was not materialized",
                        ))
                    })?;
                    let mut observe =
                        |event: novarocks_connector_contract::WriteSchemaVisit<'_>| match event {
                            novarocks_connector_contract::WriteSchemaVisit::Completed => {
                                work.step()?;
                                work.flush().map_err(E::from)
                            }
                            _ => work.flush().map_err(E::from),
                        };
                    required(binding.value_type_id)?;
                    novarocks_connector_contract::validate_write_source_field_schema_events::<E>(
                        field,
                        1,
                        &mut decoded_bytes,
                        &mut observe,
                    )?;
                }
                Ok(())
            },
            work,
        )?;
        }
        let metadata_namespace = match field_loans {
            Some(loans) => {
                // TypeDecodeModel admitted the exact source record Vec/shrink/Arc
                // before original materialization. No decoded Field is cloned.
                work.flush()?;
                let loans = loans.into_boxed_slice();
                work.step()?;
                work.flush()?;
                let loans = loans.into();
                work.step()?;
                work.flush()?;
                Some(novarocks_type_contract::owned_resources::metadata_materialization::MaterializedFieldNamespace::from_original_loans(loans))
            }
            None => None,
        };
        let mut table_storage = storage;
        if factory.is_some() {
            // Existing schema/source exports borrow raw Arrow handles together
            // with the complete namespace marker. Preserve that custody until
            // those actual callers can consume per-field loans. Complete FVTs
            // above never borrow this wrapper/main responsibility.
            let backing = carriers
                .values()
                .map(|entry| Ok(&entry.storage))
                .chain(fields.values().map(|entry| Ok(&entry.storage)));
            table_storage = table_storage.retaining_flat_observed(
                backing,
                |layout| {
                    request(&mut receiver, layout).map_err(|error| match error {
                        AdmissionRefusal::Control(cause) => HostError::from(cause),
                        AdmissionRefusal::Host(cause) => cause,
                    })
                },
                work,
            )?;
        }
        Ok(DecodedTypeTable {
            source_fields: None,
            field_execution_refs,
            creation_requests: None,
            metadata_namespace,
            storage: table_storage,
            carriers,
            fields,
            values,
            field_values,
        })
    });
    match (funding.as_deref_mut(), funding_facts.as_ref()) {
        (Some(funding), Some(facts)) => funding.materialize(namespace, facts, &mut || {
            original_body
                .take()
                .expect("materialization continuation runs once")()
        }),
        _ => original_body
            .take()
            .expect("unfunded original continuation")(),
    }
}

/// Compose the two original model prefixes on their original package limits.
/// The source invoice enters coexistence once, never as a new capacity grant.
struct PackageDecodeResources<'host, 'creation, 'callback, H> {
    creation: Option<&'host mut novarocks_type_contract::ValueSchemaCreation<'creation, H>>,
    pure_admit: Option<&'callback mut PureTypeAdmit<'callback>>,
    limits: PackageTypeProjectionLimits,
    source: usize,
    completed: PackageTypeProjectionFacts,
    completed_scoped: usize,
    current: PackageTypeProjectionFacts,
    current_scoped: usize,
    previous_non_scoped: usize,
    total: usize,
}
impl<H> PackageDecodeResources<'_, '_, '_, H> {
    fn gate(
        &mut self,
        child: &PackageTypeProjectionFacts,
        scoped: usize,
    ) -> Result<(), HostError<H>> {
        let plus = |a: usize, b: usize| {
            a.checked_add(b)
                .ok_or(CompileControlError::ResourceExhausted)
        };
        let combined = super::combine_namespace_facts(self.completed, *child, self.source)?;
        super::validate_namespace_facts(&combined, self.limits)?;
        if let Some(admit) = self.pure_admit.as_deref_mut() {
            admit(&combined)?;
        }
        let combined_scoped = plus(self.completed_scoped, scoped)?;
        if let Some(creation) = self.creation.as_deref_mut() {
            let non_scoped = combined
                .allocation_request_bytes_upper_bound
                .checked_sub(combined_scoped)
                .ok_or(E::ResourceSource(
                    "package scoped request classification exceeds its original total",
                ))?;
            let additional =
                non_scoped
                    .checked_sub(self.previous_non_scoped)
                    .ok_or(E::ResourceSource(
                        "package non-scoped request prefix decreased",
                    ))?;
            if additional != 0 {
                creation
                    .observe_opaque_upper_bound(additional)
                    .map_err(ProjectionFailure::Host)?;
            }
            self.previous_non_scoped = non_scoped;
        }
        self.current = *child;
        self.current_scoped = scoped;
        self.total = combined.allocation_request_bytes_upper_bound;
        Ok(())
    }
    fn finish_namespace(&mut self) -> Result<(), HostError<H>> {
        self.completed = super::combine_namespace_facts(self.completed, self.current, self.source)?;
        self.completed_scoped = self
            .completed_scoped
            .checked_add(self.current_scoped)
            .ok_or(CompileControlError::ResourceExhausted)?;
        self.current = PackageTypeProjectionFacts::default();
        self.current_scoped = 0;
        Ok(())
    }
    fn exact(&mut self, layout: Layout) -> Result<(), HostError<H>> {
        if layout.size() != 0
            && let Some(creation) = self.creation.as_deref_mut()
        {
            creation
                .request_layout(layout)
                .map_err(ProjectionFailure::Host)?;
        }
        Ok(())
    }
}

// A short, linear loan of the real package context. It erases only its
// callback lifetimes; it neither copies the ledger nor fabricates a creator.
trait SourceParentAdmission<H> {
    fn gate(
        &mut self,
        facts: &PackageTypeProjectionFacts,
        exact: usize,
    ) -> Result<(), HostError<H>>;
    fn exact(&mut self, layout: Layout) -> Result<(), HostError<H>>;
    fn has_creator(&self) -> bool;
}
impl<H> SourceParentAdmission<H> for PackageDecodeResources<'_, '_, '_, H> {
    fn gate(
        &mut self,
        facts: &PackageTypeProjectionFacts,
        exact: usize,
    ) -> Result<(), HostError<H>> {
        PackageDecodeResources::gate(self, facts, exact)
    }
    fn exact(&mut self, layout: Layout) -> Result<(), HostError<H>> {
        PackageDecodeResources::exact(self, layout)
    }
    fn has_creator(&self) -> bool {
        self.creation.is_some()
    }
}

/// A linear actual request port plus the original parent numerical gate.
/// Pure decoding has None; it does not manufacture a successful resource port.
struct SourceAdmission<'host, 'creation, 'gate, H> {
    creation: Option<&'host mut novarocks_type_contract::ValueSchemaCreation<'creation, H>>,
    parent: Option<&'gate mut dyn SourceParentAdmission<H>>,
    previous_non_exact: usize,
    total: usize,
}
impl<H> SourceAdmission<'_, '_, '_, H> {
    fn gate(
        &mut self,
        facts: &PackageTypeProjectionFacts,
        exact: usize,
    ) -> Result<(), HostError<H>> {
        if let Some(parent) = self.parent.as_deref_mut() {
            // The parent already applies the caller's actual opaque port to
            // the combined namespace prefix. Never charge that port twice.
            parent.gate(facts, exact)?;
        } else if let Some(creation) = self.creation.as_deref_mut() {
            let non_exact = facts
                .allocation_request_bytes_upper_bound
                .checked_sub(exact)
                .ok_or(E::ResourceSource(
                    "source exact request classification exceeds total",
                ))?;
            let additional = non_exact
                .checked_sub(self.previous_non_exact)
                .ok_or(E::ResourceSource("source decoder request prefix decreased"))?;
            if additional != 0 {
                creation
                    .observe_opaque_upper_bound(additional)
                    .map_err(ProjectionFailure::Host)?;
            }
            self.previous_non_exact = non_exact;
        }
        self.total = facts.allocation_request_bytes_upper_bound;
        Ok(())
    }
    fn exact(&mut self, layout: Layout) -> Result<(), HostError<H>> {
        if let Some(parent) = self.parent.as_deref_mut() {
            return parent.exact(layout);
        }
        if layout.size() != 0
            && let Some(creation) = self.creation.as_deref_mut()
        {
            creation
                .request_layout(layout)
                .map_err(ProjectionFailure::Host)?;
        }
        Ok(())
    }
}

/// Source-only owner: no execution identity is manufactured from source attributes.
pub(super) fn decode_source_fields_with_creator<H>(
    source_table: &wire::SourceFieldTable,
    source: usize,
    limits: PackageTypeProjectionLimits,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<super::DecodedSourceFieldTable, HostError<H>> {
    let storage = creation.storage_lifetime();
    decode_source_fields_core::<H, shared_resources::NoFactory>(
        source_table,
        source,
        Some(limits),
        storage,
        None,
        Some(creation),
        None,
        None,
        work,
        &mut None,
    )
}

pub(super) fn decode_source_fields_input_derived_with_creator<H>(
    source_table: &wire::SourceFieldTable,
    source: usize,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<super::DecodedSourceFieldTable, HostError<H>> {
    let storage = creation.storage_lifetime();
    decode_source_fields_core::<H, shared_resources::NoFactory>(
        source_table,
        source,
        None,
        storage,
        None,
        Some(creation),
        None,
        None,
        work,
        &mut None,
    )
}

pub(super) fn decode_source_fields_input_derived_with_factory<
    H,
    F: novarocks_type_contract::ValueSchemaCreationFactory<H>,
>(
    source_table: &wire::SourceFieldTable,
    source: usize,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
    factory: &F,
    work: &mut CompileCheckpoints<'_>,
) -> Result<super::DecodedSourceFieldTable, HostError<H>> {
    let storage = creation.storage_lifetime();
    decode_source_fields_core(
        source_table,
        source,
        None,
        storage,
        None,
        Some(creation),
        None,
        Some(factory),
        work,
        &mut None,
    )
}

fn decode_source_fields_core<H, F: novarocks_type_contract::ValueSchemaCreationFactory<H>>(
    source_table: &wire::SourceFieldTable,
    source: usize,
    limits: Option<PackageTypeProjectionLimits>,
    storage: ValueTypeStorageLifetime,
    package: Option<&novarocks_proto_models::physical_package_v2::FragmentPackage>,
    creation: Option<&mut novarocks_type_contract::ValueSchemaCreation<'_, H>>,
    parent: Option<&mut dyn SourceParentAdmission<H>>,
    factory: Option<&F>,
    work: &mut CompileCheckpoints<'_>,
    funding: &mut Option<&mut dyn super::PackageTypeFundingScope<H>>,
) -> Result<super::DecodedSourceFieldTable, HostError<H>> {
    let has_creator = creation.is_some() || parent.as_ref().is_some_and(|p| p.has_creator());
    let table = source_table
        .shape
        .as_ref()
        .ok_or(E::InvalidShape("source field shape is absent"))?;
    let mut model = TypeDecodeModel::new::<Summary, Frame>(table, source)?;
    if factory.is_some() {
        model.enable_entity_scopes()?;
    }
    model.source_facts_work(source_table.field_facts.len())?;
    if package.is_none() {
        model.source_indexes(table)?;
    }
    for carrier in &table.carriers {
        if let Some(kind) = &carrier.kind {
            model.carrier(kind)?;
        }
    }
    // Name/header and original metadata table/strings are one actual Field
    // constructor contribution, even though their wire owners are separate.
    for field in &table.fields {
        model.field_header(field)?;
    }
    for facts in &source_table.field_facts {
        // The raw fact row's map constructor has no name copy of its own.
        model.field_metadata(&facts.metadata)?;
        for entry in &facts.metadata {
            model.entry(entry)?;
        }
    }
    let mut admission = SourceAdmission {
        creation,
        parent,
        previous_non_exact: 0,
        total: 0,
    };
    admission.gate(&model.facts_in(limits)?, model.entity_bytes())?;
    work.flush()?;
    if !table.value_types.is_empty() || !table.logical_types.is_empty() {
        return Err(
            E::InvalidShape("source field owner cannot contain execution definitions").into(),
        );
    }
    for field in &table.fields {
        work.step()?;
        if field.logical_type_id.is_some() || field.value_binding_id.is_some() {
            return Err(
                E::InvalidShape("source field owner cannot reference execution identity").into(),
            );
        }
    }
    // This exact new scratch request joins the original numerical model at
    // its real construction point; it is not charged through the opaque port.
    let index_layout =
        Layout::array::<(u32, &wire::SourceArrowFieldFacts)>(source_table.field_facts.len())
            .map_err(|_| E::ResourceSource("source facts index layout cannot be represented"))?;
    model.request(index_layout, 1)?;
    let exact_index_bytes = index_layout.size();
    admission.gate(
        &model.facts_in(limits)?,
        exact_index_bytes
            .checked_add(model.entity_bytes())
            .ok_or(CompileControlError::ResourceExhausted)?,
    )?;
    admission.exact(index_layout)?;
    let source_facts = SourceFactsIndex::prepare::<H>(&source_table.field_facts, table, work)?;
    // Every actual source graph vertex is a possible root, including unused
    // definitions. The same per-root checked Arrow grammar supplies finite
    // expansion/name bounds; this is not a request or memory grant.
    let structural = match limits {
        Some(limits) => TypeProjectionLimits {
            max_definitions: limits.max_definitions,
            max_expanded_nodes: limits.max_expanded_nodes,
            max_string_bytes: limits.max_string_bytes,
        },
        None => TypeProjectionLimits::for_validated_source_root_count(
            table
                .carriers
                .len()
                .checked_add(table.fields.len())
                .ok_or(CompileControlError::ResourceExhausted)?,
        )?,
    };
    let graph = if let Some(package) = package {
        Some(
            super::package_graph::prepare_package_type_graph_namespace_with_host(
                package,
                structural,
                super::root_sources::TypeNamespace::Source,
                source,
                &mut |facts| {
                    model.graph(*facts)?;
                    admission.gate(
                        &model.facts_in(limits)?,
                        exact_index_bytes
                            .checked_add(model.entity_bytes())
                            .ok_or(CompileControlError::ResourceExhausted)?,
                    )
                },
                work,
            )?,
        )
    } else {
        None
    };
    let mut decoded = decode_body::<H, F>(
        table,
        structural,
        storage.clone(),
        work,
        Some(Receiver {
            graph: graph.as_ref(),
            model,
            limits,
            admit: &mut |facts, scoped| admission.gate(facts, scoped),
            scoped_request_bytes: exact_index_bytes,
        }),
        factory,
        Some(&source_facts),
        funding,
    )?;
    decoded.creation_requests = has_creator.then_some(admission.total);
    Ok(super::DecodedSourceFieldTable {
        fields: decoded.fields,
        metadata_namespace: decoded.metadata_namespace,
        creation_requests: decoded.creation_requests,
        storage: decoded.storage,
    })
}

pub(super) fn decode_package(
    package: &novarocks_proto_models::physical_package_v2::FragmentPackage,
    source: usize,
    limits: PackageTypeProjectionLimits,
    admit: &mut impl FnMut(&PackageTypeProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DecodedTypeTable, E> {
    decode_package_with_storage(
        package,
        source,
        ValueTypeStorageLifetime::default(),
        limits,
        admit,
        work,
    )
}

pub(super) fn decode_package_with_storage(
    package: &novarocks_proto_models::physical_package_v2::FragmentPackage,
    source: usize,
    storage: ValueTypeStorageLifetime,
    limits: PackageTypeProjectionLimits,
    admit: &mut impl FnMut(&PackageTypeProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DecodedTypeTable, E> {
    let mut resources = PackageDecodeResources {
        creation: None,
        pure_admit: Some(admit),
        limits,
        source,
        completed: PackageTypeProjectionFacts::default(),
        completed_scoped: 0,
        current: PackageTypeProjectionFacts::default(),
        current_scoped: 0,
        previous_non_scoped: 0,
        total: 0,
    };
    decode_package_core::<Infallible, shared_resources::NoFactory>(
        package,
        source,
        storage,
        limits,
        &mut resources,
        work,
        None,
        &mut None,
    )
    .map_err(ProjectionFailure::without_host)
}

/// One real package root author, same graph and materialization body.
pub(super) fn decode_package_with_creator<H>(
    package: &novarocks_proto_models::physical_package_v2::FragmentPackage,
    source: usize,
    limits: PackageTypeProjectionLimits,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DecodedTypeTable, HostError<H>> {
    decode_package_created_core::<H, shared_resources::NoFactory>(
        package, source, limits, creation, None, work, &mut None,
    )
}

pub(super) fn decode_package_with_factory<
    H,
    F: novarocks_type_contract::ValueSchemaCreationFactory<H>,
>(
    package: &novarocks_proto_models::physical_package_v2::FragmentPackage,
    source: usize,
    limits: PackageTypeProjectionLimits,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
    factory: &F,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DecodedTypeTable, HostError<H>> {
    decode_package_created_core(
        package,
        source,
        limits,
        creation,
        Some(factory),
        work,
        &mut None,
    )
}

fn decode_package_created_core<H, F: novarocks_type_contract::ValueSchemaCreationFactory<H>>(
    package: &novarocks_proto_models::physical_package_v2::FragmentPackage,
    source: usize,
    limits: PackageTypeProjectionLimits,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
    factory: Option<&F>,
    work: &mut CompileCheckpoints<'_>,
    funding: &mut Option<&mut dyn super::PackageTypeFundingScope<H>>,
) -> Result<DecodedTypeTable, HostError<H>> {
    let storage = creation.storage_lifetime();
    let mut resources = PackageDecodeResources {
        creation: Some(creation),
        pure_admit: None,
        limits,
        source,
        completed: PackageTypeProjectionFacts::default(),
        completed_scoped: 0,
        current: PackageTypeProjectionFacts::default(),
        current_scoped: 0,
        previous_non_scoped: 0,
        total: 0,
    };
    let mut decoded = decode_package_core(
        package,
        source,
        storage,
        limits,
        &mut resources,
        work,
        factory,
        funding,
    )?;
    decoded.creation_requests = Some(resources.total);
    Ok(decoded)
}

fn decode_package_core<H, F: novarocks_type_contract::ValueSchemaCreationFactory<H>>(
    package: &novarocks_proto_models::physical_package_v2::FragmentPackage,
    source: usize,
    storage: ValueTypeStorageLifetime,
    limits: PackageTypeProjectionLimits,
    resources: &mut PackageDecodeResources<'_, '_, '_, H>,
    work: &mut CompileCheckpoints<'_>,
    factory: Option<&F>,
    funding: &mut Option<&mut dyn super::PackageTypeFundingScope<H>>,
) -> Result<DecodedTypeTable, HostError<H>> {
    let table = package
        .types
        .as_ref()
        .ok_or(E::InvalidShape("package type table is absent"))?;
    let mut model = TypeDecodeModel::new::<Summary, Frame>(table, source)?;
    if factory.is_some() {
        model.enable_entity_scopes()?;
    }
    // All header-known output/scratch requests precede source steps and late
    // control checks. Child graph caps must not replace this numerical cause.
    resources.gate(&model.facts(limits)?, model.entity_bytes())?;
    // Pure numerical header projection: only kinds, lengths and layouts are
    // read, with CPU already covered by the original B/N numerical ceiling.
    // No attribute validation, byte traversal, graph edge, output allocation
    // or synthetic cooperative work occurs in this opaque calculation.
    for carrier in &table.carriers {
        if let Some(kind) = &carrier.kind {
            model.carrier(kind)?;
        }
    }
    for field in &table.fields {
        model.field(field)?;
    }
    // Known output requests must refuse before the graph's first source step,
    // including when a late caller refusal is pending at 254/255 units.
    resources.gate(&model.facts(limits)?, model.entity_bytes())?;
    let graph_limits = TypeProjectionLimits {
        max_definitions: usize::MAX,
        max_expanded_nodes: usize::MAX,
        max_string_bytes: usize::MAX,
    };
    let graph = super::package_graph::prepare_package_type_graph_with_host(
        package,
        graph_limits,
        source,
        &mut |facts| {
            model.graph(*facts)?;
            resources.gate(&model.facts(limits)?, model.entity_bytes())
        },
        work,
    )?;
    let table = graph.table_for(package)?;
    let mut decoded = decode_body(
        table,
        graph_limits,
        storage.clone(),
        work,
        Some(Receiver {
            graph: Some(&graph),
            model,
            limits: Some(limits),
            admit: &mut |facts, scoped| resources.gate(facts, scoped),
            scoped_request_bytes: 0,
        }),
        factory,
        None,
        funding,
    )?;
    resources.finish_namespace()?;
    let source_table = package
        .source_fields
        .as_ref()
        .ok_or(E::InvalidShape("package source fields are absent"))?;
    let source_fields = decode_source_fields_core(
        source_table,
        source,
        Some(limits),
        storage.clone(),
        Some(package),
        None,
        Some(resources),
        factory,
        work,
        funding,
    )?;
    resources.finish_namespace()?;
    decoded.source_fields = Some(source_fields);
    Ok(decoded)
}

pub(super) fn decode_package_with_funding<H>(
    package: &novarocks_proto_models::physical_package_v2::FragmentPackage,
    source: usize,
    limits: PackageTypeProjectionLimits,
    admit: &mut impl FnMut(&PackageTypeProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
    host: &mut dyn super::PackageTypeFundingScope<H>,
) -> Result<DecodedTypeTable, HostError<H>> {
    let mut resources = PackageDecodeResources {
        creation: None,
        pure_admit: Some(admit),
        limits,
        source,
        completed: PackageTypeProjectionFacts::default(),
        completed_scoped: 0,
        current: PackageTypeProjectionFacts::default(),
        current_scoped: 0,
        previous_non_scoped: 0,
        total: 0,
    };
    decode_package_core::<H, shared_resources::NoFactory>(
        package,
        source,
        ValueTypeStorageLifetime::pure(),
        limits,
        &mut resources,
        work,
        None,
        &mut Some(host),
    )
}

pub(super) fn decode_package_with_factory_and_funding<
    H,
    F: novarocks_type_contract::ValueSchemaCreationFactory<H>,
>(
    package: &novarocks_proto_models::physical_package_v2::FragmentPackage,
    source: usize,
    limits: PackageTypeProjectionLimits,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
    factory: &F,
    work: &mut CompileCheckpoints<'_>,
    funding: &mut dyn super::PackageTypeFundingScope<H>,
) -> Result<DecodedTypeTable, HostError<H>> {
    decode_package_created_core(
        package,
        source,
        limits,
        creation,
        Some(factory),
        work,
        &mut Some(funding),
    )
}

#[cfg(test)]
mod funding_forecast_tests {
    use super::*;
    use novarocks_type_contract::{ValueSchemaCreationFactory, ValueSchemaScopeBeginFailure};

    struct UnqualifiedFactory;
    struct UnopenedScope;
    impl ValueSchemaCreationScope<Infallible> for UnopenedScope {
        fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
            unreachable!("numeric forecast must not construct a scope")
        }
        fn request_layout(&mut self, _: Layout) -> Result<(), Infallible> {
            unreachable!("numeric forecast must not request capacity")
        }
        fn observe_opaque_upper_bound(&mut self, _: usize) -> Result<(), Infallible> {
            unreachable!("numeric forecast must not request capacity")
        }
    }
    impl ValueSchemaCreationFactory<Infallible> for UnqualifiedFactory {
        type Scope<'a> = UnopenedScope;
        fn begin_scope(&self) -> Result<Self::Scope<'_>, Infallible> {
            unreachable!("numeric forecast must not construct a scope")
        }
        fn begin_scope_observed<O>(
            &self,
            _: &mut dyn FnMut(Layout) -> Result<(), O>,
        ) -> Result<Self::Scope<'_>, ValueSchemaScopeBeginFailure<O, Infallible>> {
            unreachable!("numeric forecast must not construct a scope")
        }
    }
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
    #[test]
    fn physical_tail_forecast_rejects_absent_scope_geometry_without_opening_scope() {
        let table = wire::TypeTable {
            carriers: vec![wire::CarrierTypeDefinition {
                id: 1,
                kind: Some(Kind::Primitive(plan::ArrowPrimitiveType::Int64 as i32)),
            }],
            fields: vec![wire::FieldDefinition {
                id: 2,
                name: "original".into(),
                nullable: false,
                carrier_type_id: Some(1),
                ..Default::default()
            }],
            ..Default::default()
        };
        let limits = TypeProjectionLimits {
            max_definitions: 8,
            max_expanded_nodes: 32,
            max_string_bytes: 1024,
        };
        let mut work =
            CompileCheckpoints::try_new(&Control, novarocks_type_contract::CompilePhase::Decode)
                .unwrap();
        let mut receiver: Option<Receiver<'_, '_, '_, Infallible>> = None;
        let (index, _) = preflight(&table, limits, &mut work, &mut receiver, None).unwrap();
        let (order, _, _, summaries) =
            topology(&index, &table, limits, &mut work, &mut receiver).unwrap();
        let result = physical_tail_funding_facts(
            super::super::PackageTypeProjectionFacts::default(),
            &table,
            &index,
            &order,
            &summaries,
            Some(&UnqualifiedFactory),
        );
        assert!(matches!(
            result,
            Err(ProjectionFailure::Codec(E::ResourceSource(
                "type backing lifetime holder source model drift"
            )))
        ));
    }
}

#[cfg(test)]
mod depth_summary_tests {
    use super::*;
    use novarocks_type_contract::{CompilePhase, PureCompileControl};
    struct Control;
    impl PureCompileControl for Control {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            Ok(())
        }
    }
    #[test]
    fn shared_struct_has_its_ordinary_level_and_map_scaffold_is_occurrence_local() {
        let table = wire::TypeTable {
            carriers: vec![
                wire::CarrierTypeDefinition {
                    id: 0,
                    kind: Some(Kind::Primitive(plan::ArrowPrimitiveType::Int64 as i32)),
                },
                wire::CarrierTypeDefinition {
                    id: 1,
                    kind: Some(Kind::StructType(wire::StructFields {
                        field_ids: vec![0, 1],
                    })),
                },
                wire::CarrierTypeDefinition {
                    id: 2,
                    kind: Some(Kind::Map(wire::MapField {
                        entries_field_id: Some(2),
                        ordered: false,
                    })),
                },
                wire::CarrierTypeDefinition {
                    id: 3,
                    kind: Some(Kind::StructType(wire::StructFields { field_ids: vec![2] })),
                },
            ],
            fields: (0..3)
                .map(|id| wire::FieldDefinition {
                    id,
                    carrier_type_id: Some(if id == 2 { 1 } else { 0 }),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        };
        let index = Index {
            table: &table,
            carriers: table.carriers.iter().map(|v| (v.id, v)).collect(),
            fields: table.fields.iter().map(|v| (v.id, v)).collect(),
        };
        let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Decode).unwrap();
        let (_, _, _, summaries) = topology::<Infallible>(
            &index,
            &table,
            TypeProjectionLimits {
                max_definitions: 100,
                max_expanded_nodes: 100,
                max_string_bytes: 100,
            },
            &mut work,
            &mut None,
        )
        .unwrap();
        assert_eq!(summaries[&Node::Carrier(1)].logical_depth, 2);
        assert_eq!(summaries[&Node::Carrier(2)].logical_depth, 2);
        assert_eq!(summaries[&Node::Carrier(3)].logical_depth, 3);
        assert_eq!(summaries[&Node::Carrier(2)].nodes, 4);
        assert_eq!(summaries[&Node::Carrier(3)].nodes, 4);
    }
}
