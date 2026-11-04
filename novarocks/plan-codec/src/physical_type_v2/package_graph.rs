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

//! One package-owned preparation of roots, the original sparse graph and its
//! intersecting domains. No Arrow values or validated Writer recipes are made.

use super::{
    TypeCodecError, TypeProjectionLimits,
    graph::{Index, IndexProjectionFacts, Node},
    graph_domains::{DomainScratchFacts, RootDomains, TypeGraphDomainFacts},
    root_sources::PackageTypeRootSources,
};
use crate::btree_resources_v2::{self as btree, BTreeResourceError};
use novarocks_proto_models::{physical_package_v2 as raw, physical_type_v2 as wire};
use novarocks_type_contract::{CompileCheckpoints, CompileControlError};

/// Cumulative bounds for one preparation and one definition visit. A parent
/// merges this contribution once with subsequent validation/materialization;
/// it supplies a truthful invoice for the entire original package union.
/// These upper bounds do not certify actual private capacity or grant MEM.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PackageTypeGraphFacts {
    pub definition_count: usize,
    pub graph_edges: usize,
    pub root_occurrences: usize,
    pub allocation_requests_upper_bound: usize,
    pub request_bytes_upper_bound: usize,
    pub coexistence_bytes_upper_bound: usize,
    pub cumulative_work_upper_bound: usize,
}

/// Reachability from actual package roots, not permission to skip a type law.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PackageTypeRootDomain {
    Strict,
    Writer,
    Intersection,
}

#[derive(Clone, Copy, Debug)]
pub enum PackageTypeGraphDefinition<'source> {
    Carrier {
        definition: &'source wire::CarrierTypeDefinition,
        domain: PackageTypeRootDomain,
    },
    Field {
        definition: &'source wire::FieldDefinition,
        domain: PackageTypeRootDomain,
    },
}

/// Borrows the exact package and original definitions. Cycles may have been
/// classified; acyclicity, attributes, scalar parameters and Writer/Value
/// occurrence laws must pass before publication. A receiver must admit its
/// topology and all Arrow requests before materialization; classification alone
/// does not certify Writer attributes or occurrence charges.
pub struct PreparedPackageTypeGraph<'source> {
    roots: PackageTypeRootSources<'source>,
    index: Index<'source>,
    domains: RootDomains<'source>,
    facts: PackageTypeGraphFacts,
}

fn add(left: usize, right: usize) -> Result<usize, CompileControlError> {
    left.checked_add(right)
        .ok_or(CompileControlError::ResourceExhausted)
}
fn mul(left: usize, right: usize) -> Result<usize, CompileControlError> {
    left.checked_mul(right)
        .ok_or(CompileControlError::ResourceExhausted)
}
fn tree_error(error: BTreeResourceError) -> TypeCodecError {
    match error {
        BTreeResourceError::Arithmetic(_) => CompileControlError::ResourceExhausted.into(),
        BTreeResourceError::SourceModel(message) => TypeCodecError::InvalidShape(message),
    }
}

// Both Index iteration and the two domain lookups per output are opaque, and
// bracketed in visit. This bounds one visit including trivial scratch cleanup;
// caller visitor work and repeated visits are separate contributions.
fn visit_work(
    vertices: usize,
    lookup: usize,
    source_union_upper: usize,
) -> Result<usize, CompileControlError> {
    add(
        128,
        add(
            mul(vertices, add(256, mul(lookup, 4)?)?)?,
            mul(source_union_upper, 16)?,
        )?,
    )
}

fn merged(
    definitions: usize,
    roots: usize,
    source: usize,
    root_work: usize,
    index: Option<IndexProjectionFacts>,
    domains: Option<TypeGraphDomainFacts>,
    graph_shape: (usize, usize),
) -> Result<PackageTypeGraphFacts, CompileControlError> {
    let index_requests = index.map_or(0, |f| f.allocation_requests_upper_bound);
    let index_bytes = index.map_or(0, |f| f.request_bytes_upper_bound);
    let domain_requests = domains.map_or(0, |f| f.allocation_requests_upper_bound);
    let domain_bytes = domains.map_or(0, |f| f.request_bytes_upper_bound);
    let requested = add(index_bytes, domain_bytes)?;
    let index_source = add(
        index.map_or(source, |f| f.coexistence_bytes_upper_bound),
        size_of::<PreparedPackageTypeGraph<'_>>(),
    )?;
    // The domain author's work already includes the original source prepare
    // and its actual root visit. Add root_work only before that author exists.
    let classification_work = domains.map_or(root_work, |f| f.cumulative_work_upper_bound);
    Ok(PackageTypeGraphFacts {
        definition_count: definitions,
        graph_edges: domains.map_or(0, |f| f.graph_edges),
        root_occurrences: roots,
        allocation_requests_upper_bound: add(index_requests, domain_requests)?,
        request_bytes_upper_bound: requested,
        coexistence_bytes_upper_bound: domains
            .map_or(index_source, |f| f.coexistence_bytes_upper_bound),
        cumulative_work_upper_bound: add(
            add(
                index.map_or(0, |f| f.cumulative_work_upper_bound),
                classification_work,
            )?,
            visit_work(graph_shape.0, graph_shape.1, index_source)?,
        )?,
    })
}

/// Prepare one sparse graph from its actual package roots. The caller lends
/// its existing checkpoint scope and pure admission callback, owns entry and
/// ordinary/success footer, and accounts for complete retained source B.
/// Materialization, full package validation and host MEM remain separate.
pub fn prepare_package_type_graph<'source>(
    package: &'source raw::FragmentPackage,
    limits: TypeProjectionLimits,
    source_retained_bytes: usize,
    admit: &mut impl FnMut(&PackageTypeGraphFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PreparedPackageTypeGraph<'source>, TypeCodecError> {
    prepare_package_type_graph_with_host::<std::convert::Infallible>(
        package,
        limits,
        source_retained_bytes,
        &mut |facts| admit(facts).map_err(Into::into),
        work,
    )
    .map_err(crate::host_projection_v2::ProjectionFailure::without_host)
}

pub(super) fn prepare_package_type_graph_with_host<'source, H>(
    package: &'source raw::FragmentPackage,
    limits: TypeProjectionLimits,
    source_retained_bytes: usize,
    admit: &mut impl FnMut(
        &PackageTypeGraphFacts,
    ) -> Result<
        (),
        crate::host_projection_v2::ProjectionFailure<TypeCodecError, H>,
    >,
    work: &mut CompileCheckpoints<'_>,
) -> Result<
    PreparedPackageTypeGraph<'source>,
    crate::host_projection_v2::ProjectionFailure<TypeCodecError, H>,
> {
    prepare_package_type_graph_namespace_with_host(
        package,
        limits,
        super::root_sources::TypeNamespace::Execution,
        source_retained_bytes,
        admit,
        work,
    )
}

pub(super) fn prepare_package_type_graph_namespace_with_host<'source, H>(
    package: &'source raw::FragmentPackage,
    limits: TypeProjectionLimits,
    namespace: super::root_sources::TypeNamespace,
    source_retained_bytes: usize,
    admit: &mut impl FnMut(
        &PackageTypeGraphFacts,
    ) -> Result<
        (),
        crate::host_projection_v2::ProjectionFailure<TypeCodecError, H>,
    >,
    work: &mut CompileCheckpoints<'_>,
) -> Result<
    PreparedPackageTypeGraph<'source>,
    crate::host_projection_v2::ProjectionFailure<TypeCodecError, H>,
> {
    // Only scalar headers are read here. All exposed counts precede callbacks.
    let (vertices, definitions) = match namespace.table(package) {
        Ok(table) => {
            let vertices = add(table.carriers.len(), table.fields.len())?;
            (
                vertices,
                add(
                    add(vertices, table.value_types.len())?,
                    table.logical_types.len(),
                )?,
            )
        }
        Err(_) => (0, 0),
    };
    if definitions > limits.max_definitions {
        return Err(TypeCodecError::InvalidShape("type projection exceeds caller limit").into());
    }
    let graph_shape = (
        vertices,
        btree::lookup_work_typed(vertices).map_err(tree_error)?,
    );
    // All scratch layouts and fixed traversal work are already known from
    // these scalar headers. Reuse the original pure authors before the first
    // source step; a later callback must not precede a known resource refusal.
    let known_index = Some(Index::projection_facts(
        namespace.table(package)?,
        limits,
        source_retained_bytes,
    )?);
    let known_scratch = DomainScratchFacts::prepare(vertices)?;
    let index_source = add(
        add(
            source_retained_bytes,
            known_index.map_or(0, |f| f.request_bytes_upper_bound),
        )?,
        size_of::<PreparedPackageTypeGraph<'_>>(),
    )?;
    let mut facts = PackageTypeGraphFacts::default();
    let roots = super::root_sources::prepare_package_type_root_sources_namespace_with_host(
        package,
        namespace,
        source_retained_bytes,
        &mut |root| {
            let count = add(
                add(root.value_root_count, root.strict_field_root_count)?,
                root.writer_field_root_count,
            )?;
            let known_domains = known_scratch.projection_facts(
                0,
                count,
                index_source,
                root.cumulative_work_upper_bound,
            )?;
            facts = merged(
                definitions,
                count,
                source_retained_bytes,
                root.cumulative_work_upper_bound,
                known_index,
                Some(known_domains),
                graph_shape,
            )?;
            admit(&facts)
        },
        work,
    )?;
    let root_facts = roots.facts();
    let table = roots.table_for(package)?;
    let mut index_facts = None;
    let index = Index::prepare_observed_with_host(
        table,
        limits,
        source_retained_bytes,
        &mut |contribution| {
            // Defense against a changed author header, with no new namespace.
            if contribution.definition_count != definitions {
                return Err(CompileControlError::ResourceExhausted.into());
            }
            let known_domains = known_scratch.projection_facts(
                0,
                facts.root_occurrences,
                index_source,
                root_facts.cumulative_work_upper_bound,
            )?;
            facts = merged(
                definitions,
                facts.root_occurrences,
                source_retained_bytes,
                root_facts.cumulative_work_upper_bound,
                Some(*contribution),
                Some(known_domains),
                graph_shape,
            )?;
            admit(&facts)?;
            index_facts = Some(*contribution);
            Ok(())
        },
        work,
    )?;
    let index_facts = index_facts.ok_or(TypeCodecError::InvalidShape(
        "package type graph index facts are absent",
    ))?;
    // The sparse Index is now a real retained source. Its established request
    // upper bounds cover its backing without guessing any private capacity.
    // Count the original package once; no hidden child wallet/control exists.
    let domains = super::graph_domains::prepare_domains_with_host(
        &index,
        &roots,
        package,
        index_source,
        &mut |contribution| {
            facts = merged(
                definitions,
                contribution.root_occurrences,
                source_retained_bytes,
                root_facts.cumulative_work_upper_bound,
                Some(index_facts),
                Some(*contribution),
                graph_shape,
            )?;
            admit(&facts)
        },
        work,
    )?;
    // All stages completed under the same parent scope. No new validation or
    // request is performed by assembling these already borrowed components.
    debug_assert_eq!(domains.facts().root_occurrences, facts.root_occurrences);
    Ok(PreparedPackageTypeGraph {
        roots,
        index,
        domains,
        facts,
    })
}

impl<'source> PreparedPackageTypeGraph<'source> {
    pub fn facts(&self) -> PackageTypeGraphFacts {
        self.facts
    }
    pub fn table_for(
        &self,
        package: &raw::FragmentPackage,
    ) -> Result<&'source wire::TypeTable, TypeCodecError> {
        self.roots.table_for(package)
    }
    pub(super) fn index(&self) -> &Index<'source> {
        &self.index
    }
    pub(super) fn roots(&self) -> &PackageTypeRootSources<'source> {
        &self.roots
    }
    pub(super) fn depth_profile(
        &self,
        node: Node,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<novarocks_type_contract::ArrowTypeProfile>, TypeCodecError> {
        work.flush()?;
        let profile = self.domains.depth_profile(&self.index, node);
        work.step()?;
        work.flush()?;
        profile
    }
    pub(super) fn domain(
        &self,
        node: Node,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<PackageTypeRootDomain, TypeCodecError> {
        work.flush()?;
        let strict = self.domains.strict(&self.index, node);
        work.step()?;
        work.flush()?;
        let writer = self.domains.writer(&self.index, node);
        work.step()?;
        work.flush()?;
        match (strict?, writer?) {
            (true, true) => Ok(PackageTypeRootDomain::Intersection),
            (true, false) => Ok(PackageTypeRootDomain::Strict),
            (false, true) => Ok(PackageTypeRootDomain::Writer),
            (false, false) => Err(TypeCodecError::InvalidShape(
                "package type graph definition has no root domain",
            )),
        }
    }
    /// Visit original borrowed definitions in sparse namespace/key order.
    /// The preparation bound includes one visit; visitor work or repeat visits
    /// need additional caller admission. Generic visitor errors stay intact.
    pub fn visit<E: From<TypeCodecError>>(
        &self,
        visitor: &mut impl FnMut(
            PackageTypeGraphDefinition<'source>,
            &mut CompileCheckpoints<'_>,
        ) -> Result<(), E>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), E> {
        work.flush().map_err(TypeCodecError::from)?;
        let mut carriers = self.index.carriers.iter();
        loop {
            work.flush().map_err(TypeCodecError::from)?;
            let next = carriers.next();
            work.step().map_err(TypeCodecError::from)?;
            let Some((&id, &definition)) = next else {
                break;
            };
            let domain = self.domain(Node::Carrier(id), work)?;
            visitor(
                PackageTypeGraphDefinition::Carrier { definition, domain },
                work,
            )?;
        }
        work.flush().map_err(TypeCodecError::from)?;
        let mut fields = self.index.fields.iter();
        loop {
            work.flush().map_err(TypeCodecError::from)?;
            let next = fields.next();
            work.step().map_err(TypeCodecError::from)?;
            let Some((&id, &definition)) = next else {
                break;
            };
            let domain = self.domain(Node::Field(id), work)?;
            visitor(
                PackageTypeGraphDefinition::Field { definition, domain },
                work,
            )?;
        }
        Ok(())
    }
}
