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

//! Reachability classifications from actual borrowed package root sources.
//! This is not acyclicity, Arrow/type legality, a Writer-law grant, materialized
//! output, complete package validation or an allocation/MEM grant.

use super::{
    TypeCodecError,
    graph::{Index, Node},
    root_sources::{PackageTypeRootSource, PackageTypeRootSources},
};
use crate::btree_resources_v2::{self as btree, BTreeResourceError, InsertionOnlyFacts};
use novarocks_proto_models::{physical_package_v2 as package_wire, physical_type_v2 as wire};
use novarocks_type_contract::{ArrowTypeProfile, CompileCheckpoints, CompileControlError};
use std::{alloc::Layout, collections::BTreeMap};

const STRICT: u8 = 1;
const WRITER: u8 = 2;
// Original strict roots are recorded without enqueueing until W is complete.
const STRICT_ROOT: u8 = 4;
// Profiles follow actual root occurrences, independently of attribute laws.
// A shared physical occurrence retains the original stricter physical policy.
const TYPED: u8 = 8;
const PHYSICAL: u8 = 16;
const TYPED_ROOT: u8 = 32;
const PHYSICAL_ROOT: u8 = 64;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct TypeGraphDomainFacts {
    pub(super) definitions: usize,
    pub(super) graph_edges: usize,
    pub(super) root_occurrences: usize,
    pub(super) allocation_requests_upper_bound: usize,
    pub(super) request_bytes_upper_bound: usize,
    pub(super) coexistence_bytes_upper_bound: usize,
    /// Own classification plus one original root-source prepare/visit bound.
    /// A parent that already admits that source contribution must merge it
    /// once, not add it again. Visitor/type/Writer validation is not included.
    pub(super) cumulative_work_upper_bound: usize,
}

type DomainAdmission<'a, H> = dyn FnMut(
        &TypeGraphDomainFacts,
    ) -> Result<(), crate::host_projection_v2::ProjectionFailure<TypeCodecError, H>>
    + 'a;

pub(super) struct RootDomains<'source> {
    table: &'source wire::TypeTable,
    flags: BTreeMap<Node, u8>,
    facts: TypeGraphDomainFacts,
}
impl RootDomains<'_> {
    pub(super) fn facts(&self) -> TypeGraphDomainFacts {
        self.facts
    }
    fn bits(&self, index: &Index<'_>, node: Node) -> Result<u8, TypeCodecError> {
        if !std::ptr::eq(self.table, index.table) {
            return Err(TypeCodecError::InvalidShape(
                "type graph domains belong to another table",
            ));
        }
        self.flags
            .get(&node)
            .copied()
            .ok_or(TypeCodecError::InvalidShape(
                "type graph domain node is absent",
            ))
    }
    pub(super) fn strict(&self, index: &Index<'_>, node: Node) -> Result<bool, TypeCodecError> {
        Ok(self.bits(index, node)? & STRICT != 0)
    }
    pub(super) fn depth_profile(
        &self,
        index: &Index<'_>,
        node: Node,
    ) -> Result<Option<ArrowTypeProfile>, TypeCodecError> {
        let bits = self.bits(index, node)?;
        Ok(if bits & PHYSICAL != 0 {
            Some(ArrowTypeProfile::Physical)
        } else if bits & TYPED != 0 {
            Some(ArrowTypeProfile::CanonicalValue)
        } else {
            None
        })
    }
    pub(super) fn writer(&self, index: &Index<'_>, node: Node) -> Result<bool, TypeCodecError> {
        Ok(self.bits(index, node)? & WRITER != 0)
    }
}
fn add(left: usize, right: usize) -> Result<usize, CompileControlError> {
    left.checked_add(right)
        .ok_or(CompileControlError::ResourceExhausted)
}
fn mul(left: usize, right: usize) -> Result<usize, CompileControlError> {
    left.checked_mul(right)
        .ok_or(CompileControlError::ResourceExhausted)
}

/// Sole pure discovery scratch layout, available before source observation.
/// Neither the layouts nor their request bounds allocate scratch or grant MEM.
#[derive(Clone, Copy)]
pub(super) struct DomainScratchFacts {
    definitions: usize,
    insertion: InsertionOnlyFacts,
    lookup: usize,
    stack_layout: Layout,
}
impl DomainScratchFacts {
    pub(super) fn prepare(definitions: usize) -> Result<Self, TypeCodecError> {
        Ok(Self {
            definitions,
            insertion: btree::insertion_only::<Node, u8>(definitions).map_err(tree_error)?,
            lookup: btree::lookup_work_typed(definitions).map_err(tree_error)?,
            stack_layout: Layout::array::<Node>(definitions)
                .map_err(|_| CompileControlError::ResourceExhausted)?,
        })
    }
    pub(super) fn projection_facts(
        &self,
        edges: usize,
        roots: usize,
        source_bytes: usize,
        root_work: usize,
    ) -> Result<TypeGraphDomainFacts, CompileControlError> {
        let requested = add(
            self.insertion.request_bytes_upper_bound,
            self.stack_layout.size(),
        )?;
        Ok(TypeGraphDomainFacts {
            definitions: self.definitions,
            graph_edges: edges,
            root_occurrences: roots,
            allocation_requests_upper_bound: add(
                self.insertion.allocation_requests_upper_bound,
                usize::from(self.stack_layout.size() != 0),
            )?,
            request_bytes_upper_bound: requested,
            coexistence_bytes_upper_bound: add(source_bytes, requested)?,
            cumulative_work_upper_bound: domain_work(
                self.definitions,
                edges,
                roots,
                source_bytes,
                root_work,
                &self.insertion,
                self.lookup,
            )?,
        })
    }
}

fn domain_work(
    n: usize,
    edges: usize,
    roots: usize,
    source_bytes: usize,
    root_work: usize,
    insertion: &InsertionOnlyFacts,
    lookup: usize,
) -> Result<usize, CompileControlError> {
    // Five child-count passes, four edge walks, discovery flags and strict
    // and profile fallback. All ordered/repeated roots incur their own sparse lookups.
    let lookups = add(add(mul(n, 64)?, mul(edges, 64)?)?, mul(roots, 32)?)?;
    let inline = mul(add(add(mul(n, 8)?, mul(edges, 4)?)?, roots)?, 256)?;
    // Two original Index iterations are opaque. The truthful union invoice
    // bounds their actual backing; occupied entries are never capacity.
    let source_iterations = mul(source_bytes, 32)?;
    add(
        add(add(256, root_work)?, insertion.cumulative_work_upper_bound)?,
        add(add(mul(lookups, lookup)?, inline)?, source_iterations)?,
    )
}
fn tree_error(error: BTreeResourceError) -> TypeCodecError {
    match error {
        BTreeResourceError::Arithmetic(_) => CompileControlError::ResourceExhausted.into(),
        BTreeResourceError::SourceModel(message) => TypeCodecError::InvalidShape(message),
    }
}

struct Classification<'index, 'source, 'admit, H> {
    index: &'index Index<'source>,
    flags: BTreeMap<Node, u8>,
    stack: Vec<Node>,
    facts: TypeGraphDomainFacts,
    source_bytes: usize,
    lookup: usize,
    insertion: InsertionOnlyFacts,
    root_work: usize,
    admit: &'admit mut DomainAdmission<'admit, H>,
}
impl<H> Classification<'_, '_, '_, H> {
    fn gate(
        &mut self,
    ) -> Result<(), crate::host_projection_v2::ProjectionFailure<TypeCodecError, H>> {
        self.facts.cumulative_work_upper_bound = domain_work(
            self.facts.definitions,
            self.facts.graph_edges,
            self.facts.root_occurrences,
            self.source_bytes,
            self.root_work,
            &self.insertion,
            self.lookup,
        )?;
        (self.admit)(&self.facts)?;
        Ok(())
    }
    fn valid_node(
        &mut self,
        node: Node,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), crate::host_projection_v2::ProjectionFailure<TypeCodecError, H>> {
        self.gate()?;
        work.flush()?;
        let found = match node {
            Node::Carrier(id) => self.index.carrier(id).map(|_| ()),
            Node::Field(id) => self.index.field(id).map(|_| ()),
        };
        work.step()?;
        work.flush()?;
        found.map_err(Into::into)
    }
    fn bits(
        &mut self,
        node: Node,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<u8, crate::host_projection_v2::ProjectionFailure<TypeCodecError, H>> {
        self.gate()?;
        work.flush()?;
        let bits = self.flags.get(&node).copied().unwrap_or(0);
        work.step()?;
        work.flush()?;
        Ok(bits)
    }
    fn mark(
        &mut self,
        node: Node,
        bit: u8,
        enqueue: bool,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), crate::host_projection_v2::ProjectionFailure<TypeCodecError, H>> {
        self.valid_node(node, work)?;
        let previous = self.bits(node, work)?;
        if previous & bit == 0 {
            self.gate()?;
            work.flush()?;
            self.flags.insert(node, previous | bit);
            work.step()?;
            work.flush()?;
            if enqueue {
                // A vertex is marked before pushing, and each domain is
                // drained separately. Never more than N distinct pending nodes.
                if self.stack.len() >= self.facts.definitions {
                    return Err(CompileControlError::ResourceExhausted.into());
                }
                self.stack.push(node);
                work.step()?;
            }
        }
        Ok(())
    }
    fn drain(
        &mut self,
        domain: u8,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), crate::host_projection_v2::ProjectionFailure<TypeCodecError, H>> {
        while let Some(node) = self.stack.pop() {
            work.step()?;
            self.gate()?;
            work.flush()?;
            let count = self.index.child_count(node);
            work.step()?;
            work.flush()?;
            for ordinal in 0..count? {
                self.gate()?;
                work.flush()?;
                let child = self.index.child(node, ordinal);
                work.step()?;
                work.flush()?;
                self.mark(child?, domain, true, work)?;
            }
        }
        Ok(())
    }
}

/// Classify actual source roots using the original child grammar. The Index's
/// own allocations/work are a separate caller contribution. This component
/// admits only its discovery flags/one reusable stack plus enumeration work;
/// the caller supplies a complete retained union invoice and owns all tails.
pub(super) fn prepare_domains_with_host<'source, H>(
    index: &Index<'source>,
    roots: &PackageTypeRootSources<'source>,
    package: &'source package_wire::FragmentPackage,
    source_bytes: usize,
    admit: &mut DomainAdmission<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<RootDomains<'source>, crate::host_projection_v2::ProjectionFailure<TypeCodecError, H>> {
    let definitions = index.vertex_count()?;
    let scratch = DomainScratchFacts::prepare(definitions)?;
    let insertion = scratch.insertion;
    let lookup = scratch.lookup;
    let root_facts = roots.facts();
    let facts = scratch.projection_facts(
        0,
        add(
            add(
                root_facts.value_root_count,
                root_facts.strict_field_root_count,
            )?,
            root_facts.writer_field_root_count,
        )?,
        source_bytes,
        root_facts.cumulative_work_upper_bound,
    )?;
    // Necessary occupied keys/borrowed handles and actual raw definition Vecs,
    // not any estimate of private retained BTree node capacity.
    let occupied = add(
        mul(
            index.carriers.len(),
            add(size_of::<u32>(), size_of::<&wire::CarrierTypeDefinition>())?,
        )?,
        mul(
            index.fields.len(),
            add(size_of::<u32>(), size_of::<&wire::FieldDefinition>())?,
        )?,
    )?;
    let raw_arrays = add(
        mul(
            index.table.carriers.capacity(),
            size_of::<wire::CarrierTypeDefinition>(),
        )?,
        mul(
            index.table.fields.capacity(),
            size_of::<wire::FieldDefinition>(),
        )?,
    )?;
    let source_floor = add(
        add(root_facts.source_floor, size_of::<Index<'_>>())?,
        add(occupied, raw_arrays)?,
    )?;
    let mut state = Classification {
        index,
        flags: BTreeMap::new(),
        stack: Vec::new(),
        facts,
        source_bytes,
        lookup,
        insertion,
        root_work: root_facts.cumulative_work_upper_bound,
        admit,
    };
    state.gate()?;
    let table = roots.table_for(package);
    work.step()?;
    let table = table?;
    let same_table = std::ptr::eq(table, index.table);
    work.step()?;
    if !same_table {
        return Err(TypeCodecError::InvalidShape(
            "type graph roots and index have different tables",
        )
        .into());
    }
    if source_floor > source_bytes {
        return Err(TypeCodecError::InvalidShape(
            "type graph domain source invoice is understated",
        )
        .into());
    }
    state.gate()?;
    // Admit each newly exposed edge count before the completed header callback.
    for node in index
        .carriers
        .keys()
        .map(|&id| Node::Carrier(id))
        .chain(index.fields.keys().map(|&id| Node::Field(id)))
    {
        state.gate()?;
        work.flush()?;
        let children = index.child_count(node);
        if let Ok(children) = &children {
            state.facts.graph_edges = add(state.facts.graph_edges, *children)?;
        }
        state.gate()?;
        work.step()?;
        work.flush()?;
        children?;
    }
    state.gate()?;
    work.flush()?;
    let reservation = state.stack.try_reserve_exact(definitions);
    reservation.map_err(|_| CompileControlError::ResourceExhausted)?;
    work.step()?;
    work.flush()?;
    roots.visit::<crate::host_projection_v2::ProjectionFailure<TypeCodecError, H>>(
        &mut |source, work| {
            let root =
                match source {
                    PackageTypeRootSource::Value(value) => {
                        value.carrier_type_id.map(Node::Carrier).ok_or(
                            TypeCodecError::InvalidShape("missing value root carrier reference"),
                        )
                    }
                    PackageTypeRootSource::SchemaField { field_id, .. }
                    | PackageTypeRootSource::IpcField { field_id, .. } => Ok(Node::Field(field_id)),
                    PackageTypeRootSource::WriterField { binding, .. } => {
                        binding.source_field_id.map(Node::Field).ok_or(
                            TypeCodecError::InvalidShape("missing writer root field reference"),
                        )
                    }
                };
            work.step()?;
            let node = root?;
            match source {
                PackageTypeRootSource::WriterField { .. } => state.mark(node, WRITER, true, work),
                _ => {
                    state.mark(node, STRICT_ROOT, false, work)?;
                    // A complete Value binding authorizes semantic depth. A
                    // bare carrier, logical tag or IPC schema does not.
                    let typed = match source {
                        PackageTypeRootSource::Value(_) => true,
                        PackageTypeRootSource::SchemaField { field_id, .. } => {
                            state.gate()?;
                            work.flush()?;
                            let field = state.index.field(field_id);
                            work.step()?;
                            work.flush()?;
                            field?.value_binding_id.is_some()
                        }
                        _ => false,
                    };
                    state.mark(
                        node,
                        if typed { TYPED_ROOT } else { PHYSICAL_ROOT },
                        false,
                        work,
                    )
                }
            }
        },
        work,
    )?;
    state.drain(WRITER, work)?;
    for node in index
        .carriers
        .keys()
        .map(|&id| Node::Carrier(id))
        .chain(index.fields.keys().map(|&id| Node::Field(id)))
    {
        if state.bits(node, work)? & TYPED_ROOT != 0 {
            state.mark(node, TYPED, true, work)?;
        }
    }
    state.drain(TYPED, work)?;
    for node in index
        .carriers
        .keys()
        .map(|&id| Node::Carrier(id))
        .chain(index.fields.keys().map(|&id| Node::Field(id)))
    {
        let bits = state.bits(node, work)?;
        // Uncovered definitions cannot borrow a semantic profile from an
        // unrelated declaration. Writer-only definitions keep their own law.
        if bits & PHYSICAL_ROOT != 0 || bits & (TYPED | WRITER) == 0 {
            state.mark(node, PHYSICAL, true, work)?;
        }
    }
    state.drain(PHYSICAL, work)?;
    // Every uncovered definition is a strict root; no unreferenced definition
    // is dropped. Descendants shared with Writer-only roots become intersection.
    for node in index
        .carriers
        .keys()
        .map(|&id| Node::Carrier(id))
        .chain(index.fields.keys().map(|&id| Node::Field(id)))
    {
        let bits = state.bits(node, work)?;
        if bits & STRICT_ROOT != 0 || bits & WRITER == 0 {
            state.mark(node, STRICT, true, work)?;
        }
    }
    state.drain(STRICT, work)?;
    let facts = state.facts;
    Ok(RootDomains {
        table,
        flags: state.flags,
        facts,
    })
}

#[cfg(test)]
pub(super) fn prepare_domains<'source>(
    index: &Index<'source>,
    roots: &PackageTypeRootSources<'source>,
    package: &'source package_wire::FragmentPackage,
    source_bytes: usize,
    admit: &mut impl FnMut(&TypeGraphDomainFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<RootDomains<'source>, TypeCodecError> {
    prepare_domains_with_host::<std::convert::Infallible>(
        index,
        roots,
        package,
        source_bytes,
        &mut |facts| admit(facts).map_err(Into::into),
        work,
    )
    .map_err(crate::host_projection_v2::ProjectionFailure::without_host)
}

#[cfg(test)]
mod tests {
    use super::super::{
        TypeProjectionLimits,
        root_sources::{TypeNamespace, prepare_package_type_root_sources_namespace_with_host},
    };
    use super::*;
    use novarocks_type_contract::{CompilePhase, PureCompileControl};
    use package_wire::connector_write_input_shape::Kind;
    use std::sync::Mutex;
    const SOURCE: usize = 1 << 26;
    const CAUSES: [CompileControlError; 3] = [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ];
    #[derive(Default)]
    struct Control {
        trace: Mutex<Vec<u32>>,
        refusal: Option<(usize, CompileControlError)>,
    }
    impl Control {
        fn refusing(at: usize, cause: CompileControlError) -> Self {
            Self {
                trace: Mutex::new(Vec::new()),
                refusal: Some((at, cause)),
            }
        }
        fn trace(&self) -> Vec<u32> {
            self.trace.lock().unwrap().clone()
        }
    }
    impl PureCompileControl for Control {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            assert_eq!(phase, CompilePhase::Decode);
            assert!(units <= 256);
            let mut trace = self.trace.lock().unwrap();
            let at = trace.len();
            trace.push(units);
            if let Some((refusal, cause)) = self.refusal.clone()
                && refusal == at
            {
                return Err(cause);
            }
            Ok(())
        }
    }
    fn primitive(id: u32) -> wire::CarrierTypeDefinition {
        wire::CarrierTypeDefinition {
            id,
            kind: Some(wire::carrier_type_definition::Kind::Primitive(
                novarocks_proto_models::plan::ArrowPrimitiveType::Int64 as i32,
            )),
        }
    }
    fn field(id: u32, carrier: u32) -> wire::FieldDefinition {
        wire::FieldDefinition {
            id,
            carrier_type_id: Some(carrier),
            ..Default::default()
        }
    }
    fn binding(id: u32) -> package_wire::ConnectorWriteFieldBinding {
        package_wire::ConnectorWriteFieldBinding {
            source_field_id: Some(id),
            field_token: vec![],
            value_type_id: None,
        }
    }
    fn recipe(kind: Kind) -> package_wire::FrozenWriterRecipe {
        package_wire::FrozenWriterRecipe {
            input: Some(package_wire::ConnectorWriteInputShape { kind: Some(kind) }),
            ..Default::default()
        }
    }
    fn data(
        fields: Vec<package_wire::ConnectorWriteFieldBinding>,
    ) -> package_wire::FrozenWriterRecipe {
        recipe(Kind::Data(package_wire::ConnectorWriteDataInput { fields }))
    }
    fn fixture() -> package_wire::FragmentPackage {
        package_wire::FragmentPackage {
            source_fields: Some(wire::SourceFieldTable {
                shape: Some(wire::TypeTable {
                    logical_types: Vec::new(),
                    carriers: vec![primitive(0), primitive(u32::MAX)],
                    fields: vec![field(0, 0), field(u32::MAX, u32::MAX)],
                    value_types: vec![],
                }),
                field_facts: vec![
                    wire::SourceArrowFieldFacts {
                        field_id: Some(0),
                        ..Default::default()
                    },
                    wire::SourceArrowFieldFacts {
                        field_id: Some(u32::MAX),
                        ..Default::default()
                    },
                ],
            }),
            writes: vec![data(vec![binding(0)])],
            ..Default::default()
        }
    }
    fn prerequisites<'s>(
        package: &'s package_wire::FragmentPackage,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(Index<'s>, PackageTypeRootSources<'s>), TypeCodecError> {
        let roots =
            prepare_package_type_root_sources_namespace_with_host::<std::convert::Infallible>(
                package,
                TypeNamespace::Source,
                SOURCE,
                &mut |_| Ok(()),
                work,
            )
            .map_err(crate::host_projection_v2::ProjectionFailure::without_host)?;
        let index = Index::prepare_observed(
            package
                .source_fields
                .as_ref()
                .unwrap()
                .shape
                .as_ref()
                .unwrap(),
            TypeProjectionLimits {
                max_definitions: 20_000,
                max_expanded_nodes: 100_000,
                max_string_bytes: 1 << 25,
            },
            SOURCE,
            &mut |_| Ok(()),
            work,
        )?;
        Ok((index, roots))
    }
    fn invoke(
        package: &package_wire::FragmentPackage,
        control: &Control,
    ) -> Result<TypeGraphDomainFacts, TypeCodecError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
        let result = (|| {
            let (index, roots) = prerequisites(package, &mut work)?;
            let domains =
                prepare_domains(&index, &roots, package, SOURCE, &mut |_| Ok(()), &mut work)?;
            Ok(domains.facts())
        })();
        if matches!(result, Err(TypeCodecError::Control(_))) {
            return result;
        }
        work.finish()?;
        result
    }
    fn assert_control(error: TypeCodecError, cause: CompileControlError) {
        assert!(matches!(error, TypeCodecError::Control(actual) if actual == cause));
    }

    #[test]
    fn five_writer_roles_strict_roots_intersection_and_unused_sparse_definitions() {
        let mut package = fixture();
        let table = package
            .source_fields
            .as_mut()
            .unwrap()
            .shape
            .as_mut()
            .unwrap();
        table.fields = (0..8)
            .map(|id| field(id, 0))
            .chain([field(u32::MAX, u32::MAX)])
            .collect();
        // Source shapes contain no execution Value roots.
        let field_facts = table
            .fields
            .iter()
            .map(|field| wire::SourceArrowFieldFacts {
                field_id: Some(field.id),
                ..Default::default()
            })
            .collect();
        package.source_fields.as_mut().unwrap().field_facts = field_facts;
        package.writes = vec![
            data(vec![binding(0)]),
            recipe(Kind::RowLineage(
                package_wire::ConnectorWriteRowLineageInput {
                    data_fields: vec![binding(1)],
                    row_identity_fields: vec![binding(2)],
                },
            )),
            recipe(Kind::PositionDelete(
                package_wire::ConnectorWritePositionDeleteInput {
                    identity_fields: vec![binding(3)],
                    partition_source_fields: vec![binding(4)],
                },
            )),
            recipe(Kind::DeletionVector(
                package_wire::ConnectorWriteDeletionVectorInput {
                    identity_fields: vec![binding(5)],
                    partition_source_fields: vec![binding(6)],
                },
            )),
            recipe(Kind::EqualityDelete(
                package_wire::ConnectorWriteEqualityDeleteInput {
                    equality_fields: vec![binding(7)],
                },
            )),
        ];
        package.schemas = vec![package_wire::SchemaDefinition {
            id: 0,
            field_ids: vec![2, 2],
        }];
        package.constants = vec![
            package_wire::IpcConstantPool {
                source_field_id: Some(2),
                ..Default::default()
            },
            package_wire::IpcConstantPool {
                source_field_id: Some(2),
                ..Default::default()
            },
            package_wire::IpcConstantPool {
                source_field_id: Some(4),
                ..Default::default()
            },
            package_wire::IpcConstantPool::default(),
        ];
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let (index, roots) = prerequisites(&package, &mut work).unwrap();
        let domains =
            prepare_domains(&index, &roots, &package, SOURCE, &mut |_| Ok(()), &mut work).unwrap();
        assert_eq!(domains.facts().root_occurrences, 11);
        assert_eq!(domains.facts().graph_edges, 9);
        for id in 0..8 {
            assert!(domains.writer(&index, Node::Field(id)).unwrap());
            assert_eq!(
                domains.strict(&index, Node::Field(id)).unwrap(),
                id == 2 || id == 4
            );
        }
        assert!(domains.strict(&index, Node::Carrier(0)).unwrap());
        assert!(domains.writer(&index, Node::Carrier(0)).unwrap());
        for node in [Node::Carrier(u32::MAX), Node::Field(u32::MAX)] {
            assert!(domains.strict(&index, node).unwrap());
            assert!(!domains.writer(&index, node).unwrap());
        }
        work.finish().unwrap();
    }

    #[test]
    fn writer_wide_5000_is_reachability_and_metadata_20000_is_not_a_law_grant() {
        let mut package = fixture();
        let table = package
            .source_fields
            .as_mut()
            .unwrap()
            .shape
            .as_mut()
            .unwrap();
        table.carriers = vec![
            wire::CarrierTypeDefinition {
                id: 0,
                kind: Some(wire::carrier_type_definition::Kind::StructType(
                    wire::StructFields {
                        field_ids: (1..=5000).collect(),
                    },
                )),
            },
            primitive(7),
            primitive(u32::MAX),
        ];
        table.fields = (1..=5000)
            .map(|id| field(id, 7))
            .chain([field(0, 0)])
            .collect();
        let field_facts = table
            .fields
            .iter()
            .map(|field| wire::SourceArrowFieldFacts {
                field_id: Some(field.id),
                metadata: if field.id == 0 {
                    vec![novarocks_proto_models::plan::ArrowFieldMetadataEntry {
                        key: "metadata".into(),
                        value: "x".repeat(20_000),
                    }]
                } else {
                    vec![]
                },
                ..Default::default()
            })
            .collect();
        package.source_fields.as_mut().unwrap().field_facts = field_facts;
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let (index, roots) = prerequisites(&package, &mut work).unwrap();
        let only_writer =
            prepare_domains(&index, &roots, &package, SOURCE, &mut |_| Ok(()), &mut work).unwrap();
        assert_eq!(only_writer.facts().graph_edges, 10001);
        for node in [
            Node::Field(0),
            Node::Field(5000),
            Node::Carrier(0),
            Node::Carrier(7),
        ] {
            assert!(only_writer.writer(&index, node).unwrap());
            assert!(!only_writer.strict(&index, node).unwrap());
        }
        assert!(only_writer.strict(&index, Node::Carrier(u32::MAX)).unwrap());
        // The same Source Field becomes strict via an actual IPC source occurrence.
        let mut intersection_package = package.clone();
        intersection_package
            .constants
            .push(package_wire::IpcConstantPool {
                id: 0,
                source_field_id: Some(0),
                ..Default::default()
            });
        let (index, roots) = prerequisites(&intersection_package, &mut work).unwrap();
        let both = prepare_domains(
            &index,
            &roots,
            &intersection_package,
            SOURCE,
            &mut |_| Ok(()),
            &mut work,
        )
        .unwrap();
        assert!(both.strict(&index, Node::Field(5000)).unwrap());
        assert!(both.writer(&index, Node::Field(5000)).unwrap());
        work.finish().unwrap();
    }

    #[test]
    fn cycles_terminate_without_acyclic_certification_and_dangling_roots_refuse() {
        let mut package = fixture();
        package
            .source_fields
            .as_mut()
            .unwrap()
            .shape
            .as_mut()
            .unwrap()
            .carriers[0]
            .kind = Some(wire::carrier_type_definition::Kind::ListFieldId(0));
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let (index, roots) = prerequisites(&package, &mut work).unwrap();
        let domains =
            prepare_domains(&index, &roots, &package, SOURCE, &mut |_| Ok(()), &mut work).unwrap();
        assert!(domains.writer(&index, Node::Field(0)).unwrap());
        assert!(!domains.strict(&index, Node::Carrier(0)).unwrap());
        assert_eq!(domains.facts().graph_edges, 3);
        let mut dangling = package.clone();
        dangling
            .source_fields
            .as_mut()
            .unwrap()
            .shape
            .as_mut()
            .unwrap()
            .carriers[0]
            .kind = Some(wire::carrier_type_definition::Kind::ListFieldId(9));
        assert!(matches!(
            invoke(&dangling, &Control::default()),
            Err(TypeCodecError::InvalidShape("dangling field reference"))
        ));
        let mut missing = fixture();
        if let Some(Kind::Data(input)) = missing.writes[0].input.as_mut().unwrap().kind.as_mut() {
            input.fields[0].source_field_id = None;
        }
        assert!(matches!(
            invoke(&missing, &Control::default()),
            Err(TypeCodecError::InvalidShape(
                "missing writer root field reference"
            ))
        ));
        // A present IPC source ID must resolve in the Source field namespace.
        let mut missing_ipc = fixture();
        missing_ipc.constants.push(package_wire::IpcConstantPool {
            source_field_id: Some(99),
            ..Default::default()
        });
        assert!(matches!(
            invoke(&missing_ipc, &Control::default()),
            Err(TypeCodecError::InvalidShape("dangling field reference"))
        ));
        // Preserve the original missing Value carrier negative in its actual
        // Execution namespace rather than placing semantic entries in Source.
        let missing_value = package_wire::FragmentPackage {
            types: Some(wire::TypeTable {
                carriers: vec![primitive(0)],
                fields: vec![],
                value_types: vec![wire::ValueTypeDefinition::default()],
                logical_types: vec![],
            }),
            ..Default::default()
        };
        let value_roots =
            prepare_package_type_root_sources_namespace_with_host::<std::convert::Infallible>(
                &missing_value,
                TypeNamespace::Execution,
                SOURCE,
                &mut |_| Ok(()),
                &mut work,
            )
            .map_err(crate::host_projection_v2::ProjectionFailure::without_host)
            .unwrap();
        let value_index = Index::prepare_observed(
            missing_value.types.as_ref().unwrap(),
            TypeProjectionLimits {
                max_definitions: 20000,
                max_expanded_nodes: 100000,
                max_string_bytes: 1 << 25,
            },
            SOURCE,
            &mut |_| Ok(()),
            &mut work,
        )
        .unwrap();
        assert!(matches!(
            prepare_domains(
                &value_index,
                &value_roots,
                &missing_value,
                SOURCE,
                &mut |_| Ok(()),
                &mut work
            ),
            Err(TypeCodecError::InvalidShape(
                "missing value root carrier reference"
            ))
        ));
        work.finish().unwrap();
    }

    #[test]
    fn actual_package_and_table_loans_refuse_equal_foreign_owners() {
        let package = fixture();
        let foreign = package.clone();
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let (index, roots) = prerequisites(&package, &mut work).unwrap();
        assert!(matches!(
            prepare_domains(&index, &roots, &foreign, SOURCE, &mut |_| Ok(()), &mut work),
            Err(TypeCodecError::InvalidShape(
                "package type roots belong to another raw package"
            ))
        ));
        let (foreign_index, foreign_roots) = prerequisites(&foreign, &mut work).unwrap();
        assert!(matches!(
            prepare_domains(
                &index,
                &foreign_roots,
                &foreign,
                SOURCE,
                &mut |_| Ok(()),
                &mut work
            ),
            Err(TypeCodecError::InvalidShape(
                "type graph roots and index have different tables"
            ))
        ));
        let domains =
            prepare_domains(&index, &roots, &package, SOURCE, &mut |_| Ok(()), &mut work).unwrap();
        assert!(matches!(
            domains.strict(&foreign_index, Node::Field(0)),
            Err(TypeCodecError::InvalidShape(
                "type graph domains belong to another table"
            ))
        ));
        assert!(matches!(
            prepare_domains(&index, &roots, &package, 0, &mut |_| Ok(()), &mut work),
            Err(TypeCodecError::InvalidShape(
                "type graph domain source invoice is understated"
            ))
        ));
        work.finish().unwrap();
    }

    #[test]
    fn independent_flags_and_stack_layout_golden_exact_and_one_under_axes() {
        let package = fixture();
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let (index, roots) = prerequisites(&package, &mut work).unwrap();
        let domains =
            prepare_domains(&index, &roots, &package, SOURCE, &mut |_| Ok(()), &mut work).unwrap();
        let facts = domains.facts();
        assert_eq!(facts.definitions, 4);
        assert_eq!(facts.graph_edges, 2);
        assert_eq!(facts.root_occurrences, 1);
        // Independent locked B=6 field-layout upper, not a call to the
        // production BTree author or a claim of actual retained capacity.
        let pointer = Layout::new::<Option<std::ptr::NonNull<()>>>();
        let align = pointer
            .align()
            .max(align_of::<Node>())
            .max(align_of::<u8>());
        let internal = pointer.size()
            + 4
            + 11 * (size_of::<Node>() + size_of::<u8>())
            + 5 * (align - 1)
            + 12 * pointer.size()
            + (align - 1);
        let one_node = internal.div_ceil(align) * align;
        let bytes = 4 * one_node + 4 * size_of::<Node>();
        assert_eq!(facts.allocation_requests_upper_bound, 5);
        assert_eq!(facts.request_bytes_upper_bound, bytes);
        assert_eq!(facts.coexistence_bytes_upper_bound, SOURCE + bytes);
        for axis in 0..4 {
            let metric = |facts: &TypeGraphDomainFacts| match axis {
                0 => facts.allocation_requests_upper_bound,
                1 => facts.request_bytes_upper_bound,
                2 => facts.coexistence_bytes_upper_bound,
                _ => facts.cumulative_work_upper_bound,
            };
            for (cap, accepted) in [(metric(&facts), true), (metric(&facts) - 1, false)] {
                let result = prepare_domains(
                    &index,
                    &roots,
                    &package,
                    SOURCE,
                    &mut |f| {
                        if metric(f) > cap {
                            Err(CompileControlError::ResourceExhausted)
                        } else {
                            Ok(())
                        }
                    },
                    &mut work,
                );
                if accepted {
                    assert_eq!(result.unwrap().facts(), facts);
                } else {
                    assert_control(
                        result.err().unwrap(),
                        CompileControlError::ResourceExhausted,
                    );
                }
            }
        }
        // The three scalar counters are independently caller-admittable too.
        for axis in 0..3 {
            let result = prepare_domains(
                &index,
                &roots,
                &package,
                SOURCE,
                &mut |f| {
                    let (actual, cap) = match axis {
                        0 => (f.definitions, 3),
                        1 => (f.graph_edges, 1),
                        _ => (f.root_occurrences, 0),
                    };
                    if actual > cap {
                        Err(CompileControlError::ResourceExhausted)
                    } else {
                        Ok(())
                    }
                },
                &mut work,
            );
            assert_control(
                result.err().unwrap(),
                CompileControlError::ResourceExhausted,
            );
        }
        work.finish().unwrap();
    }

    #[test]
    fn pending_real_copy_known_numeric_refusal_precedes_every_late_control() {
        let package = fixture();
        let setup = Control::default();
        let mut setup_work = CompileCheckpoints::try_new(&setup, CompilePhase::Decode).unwrap();
        let (index, roots) = prerequisites(&package, &mut setup_work).unwrap();
        for cause in CAUSES {
            let control = Control::refusing(1, cause);
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
            // Caller-owned actual byte copy supplies pending255; this is a
            // private composition seam, not a complete Package-path oracle.
            let mut copied = [0u8; 255];
            for byte in &mut copied {
                *byte = 7;
                work.step().unwrap();
            }
            assert_eq!(copied, [7; 255]);
            let result = prepare_domains(
                &index,
                &roots,
                &package,
                SOURCE,
                &mut |f| {
                    if f.allocation_requests_upper_bound > 0 {
                        Err(CompileControlError::ResourceExhausted)
                    } else {
                        Ok(())
                    }
                },
                &mut work,
            );
            assert_control(
                result.err().unwrap(),
                CompileControlError::ResourceExhausted,
            );
            assert_eq!(control.trace(), vec![0]);
        }
        setup_work.finish().unwrap();
    }

    #[test]
    fn every_small_callback_preserves_three_causes_and_wide_repeated_roots_dedup_storage() {
        let package = fixture();
        let success = Control::default();
        let facts = invoke(&package, &success).unwrap();
        let success_trace = success.trace();
        assert!(success_trace.iter().any(|&units| units > 0));
        let mut ordinary = fixture();
        ordinary
            .source_fields
            .as_mut()
            .unwrap()
            .shape
            .as_mut()
            .unwrap()
            .fields[0]
            .carrier_type_id = Some(99);
        let failure = Control::default();
        assert!(matches!(
            invoke(&ordinary, &failure),
            Err(TypeCodecError::InvalidShape(
                "dangling carrier type reference"
            ))
        ));
        for (source, baseline) in [(&package, success_trace), (&ordinary, failure.trace())] {
            for at in 0..baseline.len() {
                for cause in CAUSES {
                    let control = Control::refusing(at, cause.clone());
                    assert_control(invoke(source, &control).err().unwrap(), cause);
                    assert_eq!(control.trace(), baseline[..=at]);
                }
            }
        }
        let mut wide = fixture();
        wide.writes = vec![data((0..320).map(|_| binding(0)).collect())];
        let control = Control::default();
        let wide_facts = invoke(&wide, &control).unwrap();
        assert_eq!(wide_facts.definitions, facts.definitions);
        assert_eq!(wide_facts.root_occurrences, 320);
        assert_eq!(
            wide_facts.allocation_requests_upper_bound,
            facts.allocation_requests_upper_bound
        );
        assert_eq!(
            wide_facts.request_bytes_upper_bound,
            facts.request_bytes_upper_bound
        );
        let trace = control.trace();
        assert!(trace.iter().all(|&units| units <= 256));
        // Opaque sparse lookups flush frequently; admit honest bounded units,
        // without pretending this component guarantees an internal256 quantum.
        assert!(trace.iter().any(|&units| units > 0));
        for at in [0, trace.len() / 2, trace.len() - 1] {
            for cause in CAUSES {
                let control = Control::refusing(at, cause.clone());
                assert_control(invoke(&wide, &control).err().unwrap(), cause);
                assert_eq!(control.trace(), trace[..=at]);
            }
        }
    }
}
