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
    DecodedTypeTable, PackageTypeProjectionFacts, PackageTypeProjectionLimits,
    PackageTypeRootDomain, PackageTypeRootSource, PreparedPackageTypeGraph, TypeCodecError,
    TypeProjectionLimits, decode_logical, decode_resources::TypeDecodeModel, observe_bytes,
    validate_type,
};
use arrow::datatypes::{DataType, Field, UnionFields, UnionMode};
use novarocks_proto_models::{physical_type_v2 as wire, plan};
use novarocks_type_contract::owned_resources::metadata_materialization::MaterializedMetadataMap;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, FunctionValueType, MAX_ARROW_FIELD_METADATA_BYTES,
    MAX_ARROW_FIELD_METADATA_ENTRIES, MAX_ARROW_FIELD_METADATA_KEY_BYTES,
    MAX_ARROW_FIELD_METADATA_VALUE_BYTES, MAX_ARROW_FIELD_NAME_BYTES,
    MAX_ARROW_TIMESTAMP_TIMEZONE_BYTES, MAX_VALUE_TYPE_DEPTH, MAX_VALUE_TYPE_NODES, ValueTypeError,
    field_logical_type,
};
use std::{collections::BTreeMap, sync::Arc};
use wire::carrier_type_definition::Kind;

use super::graph::{Index, Node, add, required};

type E = TypeCodecError;

#[derive(Clone, Copy, Default)]
struct Summary {
    nodes: usize,
    depth: usize,
    dictionary_boxes: usize,
}

struct Receiver<'graph, 'source, 'caller> {
    graph: &'graph PreparedPackageTypeGraph<'source>,
    model: TypeDecodeModel,
    limits: PackageTypeProjectionLimits,
    admit: &'caller mut dyn FnMut(&PackageTypeProjectionFacts) -> Result<(), CompileControlError>,
}
impl Receiver<'_, '_, '_> {
    fn gate(&mut self) -> Result<(), E> {
        (self.admit)(&self.model.facts(self.limits)?)?;
        Ok(())
    }
}
fn strict(
    receiver: Option<&Receiver<'_, '_, '_>>,
    node: Node,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, E> {
    match receiver {
        None => Ok(true),
        Some(receiver) => Ok(receiver.graph.domain(node, work)? != PackageTypeRootDomain::Writer),
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

fn preflight<'a, 'graph>(
    table: &'a wire::TypeTable,
    limits: TypeProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
    receiver: &mut Option<Receiver<'graph, 'a, '_>>,
) -> Result<ReceiverIndex<'a, 'graph>, E> {
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
    let mut index = match receiver.as_ref() {
        Some(receiver) => ReceiverIndex::Package(receiver.graph.index()),
        None => ReceiverIndex::Owned(Index {
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
            return Err(E::InvalidShape("duplicate carrier type identity"));
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
                return Err(E::InvalidShape(
                    "Arrow timestamp zone exceeds its owner bound",
                ));
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
                    return Err(E::InvalidShape("too many union fields"));
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
                        return Err(E::InvalidShape("duplicate union type identity"));
                    }
                    seen[id] = true;
                }
            }
            Kind::FixedSizeBinary(length)
            | Kind::FixedSizeList(wire::FixedSizeList { length, .. })
                if *length < 0
                    || (is_strict && *length > novarocks_physical_plan::MAX_FIXED_SIZE_LENGTH) =>
            {
                return Err(E::InvalidShape("Arrow fixed size exceeds its owner bound"));
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
            return Err(E::InvalidShape("duplicate field identity"));
        }
        let is_strict = strict(receiver.as_ref(), Node::Field(field.id), work)?;
        if is_strict
            && (field.name.len() > MAX_ARROW_FIELD_NAME_BYTES
                || field.metadata.len() > MAX_ARROW_FIELD_METADATA_ENTRIES)
        {
            return Err(E::InvalidShape(
                "Arrow field attributes exceed their owner bounds",
            ));
        }
        add(&mut bytes, field.name.len(), limits.max_string_bytes)?;
        observe_bytes(field.name.as_bytes(), work)?;
        let mut metadata_bytes = 0;
        let mut previous: Option<&str> = None;
        for entry in &field.metadata {
            work.step()?;
            if is_strict
                && (entry.key.len() > MAX_ARROW_FIELD_METADATA_KEY_BYTES
                    || entry.value.len() > MAX_ARROW_FIELD_METADATA_VALUE_BYTES)
            {
                return Err(E::InvalidShape(
                    "Arrow field metadata entry exceeds its owner bound",
                ));
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
                return Err(E::InvalidShape("field metadata must be sorted and unique"));
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
            return Err(E::InvalidShape("duplicate value type identity"));
        }
        index.carrier(required(value.carrier_type_id)?)?;
        decode_logical(value.logical_type)?;
    }
    for field in &table.fields {
        work.step()?;
        let dictionary = matches!(
            index.kind(required(field.carrier_type_id)?)?,
            Kind::Dictionary(_)
        );
        if if dictionary {
            field.dictionary_id.is_none() || field.dictionary_is_ordered.is_none()
        } else {
            field.dictionary_id.is_some() || field.dictionary_is_ordered.is_some()
        } {
            return Err(E::InvalidShape(
                "field dictionary attributes do not match its carrier",
            ));
        }
    }
    Ok(index)
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
// A carrier adds one to both; each repeated reference contributes again.
fn topology(
    index: &Index<'_>,
    table: &wire::TypeTable,
    limits: TypeProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
    receiver: &mut Option<Receiver<'_, '_, '_>>,
) -> Result<(Vec<Node>, usize), E> {
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
                    frame.summary.dictionary_boxes = frame
                        .summary
                        .dictionary_boxes
                        .checked_add(summary.dictionary_boxes)
                        .ok_or(CompileControlError::ResourceExhausted)?;
                    frame.next += 1;
                    continue;
                }
                if opaque(package, work, |_| Ok(active.insert(child, true)))? == Some(true) {
                    return Err(E::InvalidShape("cyclic type table references"));
                }
                // A valid carrier path has at most 64 carriers and 64 fields.
                // Bound the explicit DFS scratch before pushing another vertex.
                if (!package && stack.len() >= MAX_VALUE_TYPE_DEPTH * 2)
                    || (package && stack.len() >= vertices)
                {
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
                    summary.dictionary_boxes = if matches!(index.kind(id)?, Kind::Dictionary(_)) {
                        summary
                            .dictionary_boxes
                            .checked_add(2)
                            .ok_or(CompileControlError::ResourceExhausted)?
                    } else {
                        0
                    };
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
                if is_strict && summary.depth > MAX_VALUE_TYPE_DEPTH {
                    return Err(ValueTypeError::TooDeep.into());
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
    Ok((order, maximum))
}

fn union_mode(mode: i32) -> Result<UnionMode, E> {
    match plan::ArrowUnionMode::try_from(mode) {
        Ok(plan::ArrowUnionMode::Sparse) => Ok(UnionMode::Sparse),
        Ok(plan::ArrowUnionMode::Dense) => Ok(UnionMode::Dense),
        _ => Err(E::InvalidShape("unknown or unspecified union mode")),
    }
}

fn field_ref(
    fields: &BTreeMap<u32, Arc<Field>>,
    id: u32,
    work: &mut CompileCheckpoints<'_>,
    package: bool,
) -> Result<Arc<Field>, E> {
    work.step()?;
    opaque(package, work, |_| {
        fields
            .get(&id)
            .cloned()
            .ok_or(E::InvalidShape("field topology was not materialized"))
    })
}

fn carrier_ref(
    carriers: &BTreeMap<u32, DataType>,
    id: u32,
    work: &mut CompileCheckpoints<'_>,
    package: bool,
) -> Result<DataType, E> {
    let carrier = opaque(package, work, |_| {
        carriers
            .get(&id)
            .ok_or(E::InvalidShape("carrier topology was not materialized"))
    })?;
    super::clone_carrier(carrier, work)
}

fn materialize(
    kind: &Kind,
    carriers: &BTreeMap<u32, DataType>,
    fields: &BTreeMap<u32, Arc<Field>>,
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
    decode_body(
        table,
        limits,
        work,
        None,
        &mut super::DirectTypeMaterialization,
    )
    .map_err(crate::host_projection_v2::ProjectionFailure::without_host)
}

fn decode_body<'source, H: super::PackageTypeMaterializationScope>(
    table: &'source wire::TypeTable,
    limits: TypeProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
    mut receiver: Option<Receiver<'_, 'source, '_>>,
    host: &mut H,
) -> Result<DecodedTypeTable, crate::host_projection_v2::ProjectionFailure<E, H::HostError>> {
    let index = preflight(table, limits, work, &mut receiver)?;
    let (order, maximum) = topology(&index, table, limits, work, &mut receiver)?;
    if let Some(receiver) = receiver.as_mut() {
        let writer_roots = receiver.graph.roots().facts().writer_field_root_count;
        // An unfolded Arrow tree has at most one Field per child type edge,
        // plus its root Field. Count both occurrence kinds, including repeats.
        let occurrences = writer_roots
            .checked_mul(maximum)
            .and_then(|n| n.checked_mul(2))
            .ok_or(CompileControlError::ResourceExhausted)?;
        receiver.model.writer_roots(
            writer_roots,
            occurrences,
            receiver.graph.roots().facts().cumulative_work_upper_bound,
        )?;
        receiver.gate()?;
    }
    // No Arrow type, field, metadata or timezone was allocated above. Observe
    // the admitted graph tail before the first materialization.
    work.flush()?;
    match receiver.as_ref() {
        Some(receiver) => {
            let facts = receiver.model.facts(receiver.limits)?;
            host.materialize(&facts, || {
                materialize_body(table, index, order, work, Some(receiver))
            })
        }
        None => materialize_body(table, index, order, work, None)
            .map_err(crate::host_projection_v2::ProjectionFailure::Codec),
    }
}

/// The original Arrow author, shared by both host and direct adapters.
fn materialize_body(
    table: &wire::TypeTable,
    index: ReceiverIndex<'_, '_>,
    order: Vec<Node>,
    work: &mut CompileCheckpoints<'_>,
    receiver: Option<&Receiver<'_, '_, '_>>,
) -> Result<DecodedTypeTable, E> {
    let package = receiver.is_some();
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
                let graph = receiver.as_ref().map(|r| (r.graph, id));
                let ty = opaque(package, work, |work| {
                    materialize(index.kind(id)?, &carriers, &fields, work, graph)
                })?;
                if strict(receiver, node, work)? {
                    validate_type(&ty, work)?;
                } else {
                    novarocks_type_contract::validate_arrow_carrier_parameters_observed(
                        &ty,
                        || work.step().map_err(E::from),
                    )?;
                }
                opaque(package, work, |_| {
                    carriers.insert(id, ty);
                    Ok(())
                })?;
            }
            Node::Field(id) => {
                let source = index.field(id)?;
                let ty = carrier_ref(&carriers, required(source.carrier_type_id)?, work, package)?;
                observe_bytes(source.name.as_bytes(), work)?;
                let field = opaque(package, work, |_| {
                    Ok(match (source.dictionary_id, source.dictionary_is_ordered) {
                        #[allow(deprecated)]
                        (Some(id), Some(ordered)) => {
                            Field::new_dict(source.name.clone(), ty, source.nullable, id, ordered)
                        }
                        (None, None) => Field::new(source.name.clone(), ty, source.nullable),
                        _ => return Err(E::InvalidShape("incomplete field dictionary attributes")),
                    })
                })?;
                let mut metadata = opaque(package, work, |_| {
                    Ok(MaterializedMetadataMap::with_capacity(
                        source.metadata.len(),
                    ))
                })?;
                for entry in &source.metadata {
                    work.step()?;
                    observe_bytes(entry.key.as_bytes(), work)?;
                    observe_bytes(entry.value.as_bytes(), work)?;
                    opaque(package, work, |_| {
                        metadata.insert(entry.key.clone(), entry.value.clone());
                        Ok(())
                    })?;
                }
                let field = metadata.into_field(field);
                if strict(receiver, node, work)? {
                    opaque(package, work, |_| {
                        field_logical_type(field.field()).map_err(E::from)
                    })?;
                }
                opaque(package, work, |_| {
                    let field = field.into_shared();
                    if let Some(loans) = field_loans.as_mut() {
                        loans.push(field.loan());
                    }
                    fields.insert(id, field.into_original_field_ref());
                    Ok(())
                })?;
            }
        }
    }
    for source in &table.value_types {
        work.step()?;
        let logical = decode_logical(source.logical_type)?;
        let ty = carrier_ref(&carriers, required(source.carrier_type_id)?, work, package)?;
        logical.validate_carrier(&ty)?;
        // Every carrier subtree already passed the shared observed owner.
        // Preserve that exact carrier and authored root domain without an
        // additional unobserved recursive constructor traversal.
        opaque(package, work, |_| {
            values.insert(
                source.id,
                FunctionValueType {
                    data_type: ty,
                    nullable: source.nullable,
                    logical_type: logical,
                },
            );
            Ok(())
        })?;
    }
    if let Some(receiver) = receiver.as_ref() {
        let mut previous_recipe = None;
        let mut decoded_bytes = 0;
        receiver.graph.roots().visit::<E>(
            &mut |root, work| {
                if let PackageTypeRootSource::WriterField {
                    recipe, binding, ..
                } = root
                {
                    if previous_recipe.is_none_or(|previous| !std::ptr::eq(previous, recipe)) {
                        previous_recipe = Some(recipe);
                        decoded_bytes = 0;
                    }
                    let id = required(binding.field_id)?;
                    let field = opaque(true, work, |_| {
                        fields.get(&id).ok_or(E::InvalidShape(
                            "writer field topology was not materialized",
                        ))
                    })?;
                    novarocks_connector_contract::validate_write_field_schema_events::<E>(
                        field,
                        1,
                        &mut decoded_bytes,
                        |event| match event {
                            novarocks_connector_contract::WriteSchemaVisit::Completed => {
                                work.step()?;
                                work.flush().map_err(E::from)
                            }
                            _ => work.flush().map_err(E::from),
                        },
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
    Ok(DecodedTypeTable {
        metadata_namespace,
        carriers,
        fields,
        values,
    })
}

pub(super) fn decode_package(
    package: &novarocks_proto_models::physical_package_v2::FragmentPackage,
    source: usize,
    limits: PackageTypeProjectionLimits,
    admit: &mut impl FnMut(&PackageTypeProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DecodedTypeTable, E> {
    decode_package_with_host(
        package,
        source,
        limits,
        admit,
        work,
        &mut super::DirectTypeMaterialization,
    )
    .map_err(crate::host_projection_v2::ProjectionFailure::without_host)
}

pub(super) fn decode_package_with_host<H: super::PackageTypeMaterializationScope>(
    package: &novarocks_proto_models::physical_package_v2::FragmentPackage,
    source: usize,
    limits: PackageTypeProjectionLimits,
    admit: &mut impl FnMut(&PackageTypeProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
    host: &mut H,
) -> Result<DecodedTypeTable, crate::host_projection_v2::ProjectionFailure<E, H::HostError>> {
    let table = package
        .types
        .as_ref()
        .ok_or(E::InvalidShape("package type table is absent"))?;
    let mut model = TypeDecodeModel::new::<Summary, Frame>(table, source)?;
    // All header-known output/scratch requests precede source steps and late
    // control checks. Child graph caps must not replace this numerical cause.
    admit(&model.facts(limits)?)?;
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
        for entry in &field.metadata {
            model.entry(entry)?;
        }
    }
    // Known output requests must refuse before the graph's first source step,
    // including when a late caller refusal is pending at 254/255 units.
    admit(&model.facts(limits)?)?;
    let graph_limits = TypeProjectionLimits {
        max_definitions: usize::MAX,
        max_expanded_nodes: usize::MAX,
        max_string_bytes: usize::MAX,
    };
    let graph = super::prepare_package_type_graph(
        package,
        graph_limits,
        source,
        &mut |facts| {
            model.graph(*facts)?;
            admit(&model.facts(limits)?)
        },
        work,
    )?;
    let table = graph.table_for(package)?;
    decode_body(
        table,
        graph_limits,
        work,
        Some(Receiver {
            graph: &graph,
            model,
            limits,
            admit,
        }),
        host,
    )
}
