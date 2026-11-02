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

//! Complete public frozen reads and a separate flat provider expression algebra.
//! Original constructors retain their laws. This component neither seals provider
//! private facts nor admits a whole FragmentPackage or a host allocation grant.

use crate::host_projection_v2::ProjectionFailure;
use crate::{
    allocation_exit_v2::reserve_exit,
    binding_index_v2::{BindingIndex, lookup_work_upper_bound, prepare_work_upper_bound},
    physical_binding_v2::BindingCodecError,
    physical_connector_payload_v2::{
        ConnectorPayloadCodecError, DecodedConnectorPayloads, EncodedConnectorPayloads,
    },
    physical_node_v2::NodeCodecError,
    physical_provider_binding_v2::{
        DecodedProviderBindings, EncodedProviderBindings, ProviderBindingCodecError,
    },
    physical_schema_v2::{DecodedSchemas, EncodedSchemas, SchemaCodecError},
    physical_type_v2::TypeCodecError,
};
use novarocks_connector_contract as c;
use novarocks_physical_plan::NodeId;
use novarocks_proto_codec::{FieldPath, ProtocolError, connector_read as codec};
use novarocks_proto_models::{connector_read as cv, physical_package_v2 as w};
use novarocks_type_contract::ValueSchemaCreation;
use novarocks_type_contract::{CompileCheckpoints, CompileControlError};
use std::{
    alloc::Layout, collections::BTreeMap, convert::Infallible, fmt, num::NonZeroU64, sync::Arc,
};

type E = ReadScanCodecError;
#[derive(Debug)]
pub enum ReadScanCodecError {
    Control(CompileControlError),
    Provider(c::ConnectorError),
    Scan(c::StaticConnectorScanError),
    Recipe(c::ConnectorReadRelationRecipeError),
    Protocol(ProtocolError),
    Domain(codec::DomainCodecError),
    Binding(ProviderBindingCodecError),
    Payload(ConnectorPayloadCodecError),
    Schema(SchemaCodecError),
    Type(TypeCodecError),
    Index(BindingCodecError),
    Node(NodeCodecError),
    SourceModel(&'static str),
    InvalidShape(&'static str),
    ConstructionResource(novarocks_type_contract::owned_resources::layout::LayoutResourceError),
    InvalidSourceSchema(crate::physical_public_source_schema_v2::PublicSourceSchemaCodecError),
}
impl fmt::Display for E {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(e) => e.fmt(f),
            Self::InvalidSourceSchema(e) => e.fmt(f),
            Self::ConstructionResource(e) => write!(f, "read schema construction: {e:?}"),
            Self::Provider(e) => e.fmt(f),
            Self::Scan(e) => e.fmt(f),
            Self::Recipe(e) => e.fmt(f),
            Self::Protocol(e) => e.fmt(f),
            Self::Domain(e) => e.fmt(f),
            Self::Binding(e) => e.fmt(f),
            Self::Payload(e) => e.fmt(f),
            Self::Schema(e) => e.fmt(f),
            Self::Type(e) => e.fmt(f),
            Self::Index(e) => e.fmt(f),
            Self::Node(e) => e.fmt(f),
            Self::SourceModel(s) | Self::InvalidShape(s) => f.write_str(s),
        }
    }
}
impl std::error::Error for ReadScanCodecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(source) => Some(source),
            Self::Provider(source) => Some(source),
            Self::Scan(source) => Some(source),
            Self::Recipe(source) => Some(source),
            Self::Protocol(source) => Some(source),
            Self::Domain(source) => Some(source),
            Self::Binding(source) => Some(source),
            Self::Payload(source) => Some(source),
            Self::Schema(source) => Some(source),
            Self::Type(source) => Some(source),
            Self::Index(source) => Some(source),
            Self::Node(source) => Some(source),
            Self::ConstructionResource(source) => Some(source),
            Self::InvalidSourceSchema(source) => Some(source),
            _ => None,
        }
    }
}
impl From<crate::physical_public_source_schema_v2::PublicSourceSchemaCodecError> for E {
    fn from(error: crate::physical_public_source_schema_v2::PublicSourceSchemaCodecError) -> Self {
        match error {
            crate::physical_public_source_schema_v2::PublicSourceSchemaCodecError::Control(
                error,
            ) => Self::Control(error),
            error => Self::InvalidSourceSchema(error),
        }
    }
}
impl From<novarocks_type_contract::owned_resources::layout::LayoutResourceError> for E {
    fn from(error: novarocks_type_contract::owned_resources::layout::LayoutResourceError) -> Self {
        Self::ConstructionResource(error)
    }
}
impl From<CompileControlError> for E {
    fn from(e: CompileControlError) -> Self {
        Self::Control(e)
    }
}
impl From<c::ConnectorError> for E {
    fn from(e: c::ConnectorError) -> Self {
        Self::Provider(e)
    }
}
impl From<c::StaticConnectorScanError> for E {
    fn from(e: c::StaticConnectorScanError) -> Self {
        Self::Scan(e)
    }
}
impl From<c::ConnectorReadRelationRecipeError> for E {
    fn from(e: c::ConnectorReadRelationRecipeError) -> Self {
        Self::Recipe(e)
    }
}
impl From<ProtocolError> for E {
    fn from(e: ProtocolError) -> Self {
        match e.kind() {
            novarocks_proto_codec::ProtocolErrorKind::CompileControl(c) => Self::Control(c.clone()),
            _ => Self::Protocol(e),
        }
    }
}
macro_rules! controlled_error {
    ($ty:path,$variant:ident) => {
        impl From<$ty> for E {
            fn from(e: $ty) -> Self {
                use $ty as OriginalError;
                match e {
                    OriginalError::Control(c) => Self::Control(c),
                    e => Self::$variant(e),
                }
            }
        }
    };
}
controlled_error!(ProviderBindingCodecError, Binding);
controlled_error!(ConnectorPayloadCodecError, Payload);
controlled_error!(SchemaCodecError, Schema);
controlled_error!(TypeCodecError, Type);
controlled_error!(BindingCodecError, Index);
controlled_error!(NodeCodecError, Node);
controlled_error!(codec::DomainCodecError, Domain);
impl From<c::PureProviderCompileError<c::ConnectorError>> for E {
    fn from(e: c::PureProviderCompileError<c::ConnectorError>) -> Self {
        match e {
            c::PureProviderCompileError::Control(c) => Self::Control(c),
            c::PureProviderCompileError::Provider(e) => Self::Provider(e),
        }
    }
}
fn add(a: usize, b: usize) -> Result<usize, E> {
    a.checked_add(b)
        .ok_or(E::Control(CompileControlError::ResourceExhausted))
}
fn mul(a: usize, b: usize) -> Result<usize, E> {
    a.checked_mul(b)
        .ok_or(E::Control(CompileControlError::ResourceExhausted))
}
fn layout<T>(n: usize) -> Result<Layout, E> {
    Layout::array::<T>(n).map_err(|_| E::Control(CompileControlError::ResourceExhausted))
}
fn arc_layout(payload: Layout) -> Result<Layout, E> {
    use novarocks_type_contract::owned_resources::layout::{LayoutResourceError, arc_layout};
    arc_layout(payload).map_err(|e| match e {
        LayoutResourceError::SourceModel => {
            E::SourceModel("read Arc source profile is unsupported")
        }
        _ => E::Control(CompileControlError::ResourceExhausted),
    })
}
fn observed<T>(r: Result<T, E>, work: &mut CompileCheckpoints<'_>) -> Result<T, E> {
    if matches!(&r, Err(E::Control(_))) {
        return r;
    }
    work.step()?;
    r
}
fn required<T>(
    r: Option<T>,
    what: &'static str,
    work: &mut CompileCheckpoints<'_>,
) -> Result<T, E> {
    observed(r.ok_or(E::InvalidShape(what)), work)
}
fn opaque<T>(work: &mut CompileCheckpoints<'_>, f: impl FnOnce() -> Result<T, E>) -> Result<T, E> {
    work.flush()?;
    let r = f();
    let r = observed(r, work)?;
    work.flush()?;
    Ok(r)
}
fn reserve<T>(n: usize, work: &mut CompileCheckpoints<'_>) -> Result<Vec<T>, E> {
    layout::<T>(n)?;
    work.flush()?;
    let mut out = Vec::new();
    let r = out.try_reserve_exact(n);
    if r.is_ok() {
        work.step()?;
    }
    reserve_exit::<E>(r, work)?;
    Ok(out)
}
fn copy(input: &[u8], work: &mut CompileCheckpoints<'_>) -> Result<Vec<u8>, E> {
    let mut out = reserve(input.len(), work)?;
    for chunk in input.chunks(256) {
        out.extend_from_slice(chunk);
        work.step()?;
    }
    Ok(out)
}
fn text(input: &str, work: &mut CompileCheckpoints<'_>) -> Result<String, E> {
    crate::physical_result_v2::copy_string(input, work).map_err(E::from)
}
fn array32(
    input: &[u8],
    what: &'static str,
    work: &mut CompileCheckpoints<'_>,
) -> Result<[u8; 32], E> {
    observed(input.try_into().map_err(|_| E::InvalidShape(what)), work)
}
fn ordinal(n: usize) -> Result<u32, E> {
    u32::try_from(n).map_err(|_| E::Control(CompileControlError::ResourceExhausted))
}

#[derive(Clone, Copy, Debug)]
pub struct ReadScanProjectionLimits {
    pub max_scans: usize,
    pub max_items: usize,
    pub max_scalar_bytes: usize,
    pub max_allocation_requests: usize,
    pub max_allocation_request_bytes: usize,
    pub max_coexisting_source_and_request_bytes: usize,
    pub max_work: usize,
}
pub type DecodedReadScans = (
    Vec<(NodeId, c::FrozenConnectorRead)>,
    ReadScanProjectionFacts,
);

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReadScanProjectionFacts {
    pub scan_count: usize,
    pub item_count: usize,
    pub scalar_bytes: usize,
    pub allocation_requests_upper_bound: usize,
    pub allocation_request_bytes_upper_bound: usize,
    pub coexisting_source_and_request_bytes_upper_bound: usize,
    pub cumulative_work_upper_bound: usize,
}
/// Residual IDs are caller-authored preorder occurrence IDs, not engine ExprIds.
/// Empty IDs are required exactly when the original read has no residual.
pub struct ReadScanSource<'a> {
    pub node: NodeId,
    pub read: &'a c::FrozenConnectorRead,
    pub schema_id: u32,
    pub source_schema_id: u32,
    pub expression_ids: &'a [u32],
}
#[derive(Clone, Copy)]
pub struct ReadScanEncodeContext<'a, 's, 'c> {
    pub bindings: &'a EncodedProviderBindings<'s, 'c>,
    pub payloads: &'a EncodedConnectorPayloads<'s, 'c>,
    pub schemas: &'a EncodedSchemas<'a, 's, 'c>,
    pub source_schemas:
        &'a [crate::physical_public_source_schema_v2::SourceSchemaFactsSource<'s>],
}
#[derive(Clone, Copy)]
pub struct ReadScanDecodeContext<'a, 'w, 'c> {
    pub bindings: &'a DecodedProviderBindings<'w, 'c>,
    pub payloads: &'a DecodedConnectorPayloads<'w, 'c>,
    pub schemas: &'a DecodedSchemas<'a, 'c>,
    pub source_schemas:
        Option<&'a crate::physical_public_source_schema_v2::DecodedPublicSourceSchemas>,
}
struct Model {
    source: usize,
    facts: ReadScanProjectionFacts,
    completed: c::WriterOwnedResourceFacts,
}
impl Model {
    fn new<T>(scans: usize, source: usize) -> Result<Self, E> {
        let mut m = Self {
            source,
            facts: ReadScanProjectionFacts {
                scan_count: scans,
                cumulative_work_upper_bound: add(256, mul(source, 16)?)?,
                ..Default::default()
            },
            completed: Default::default(),
        };
        // The original public read-facts/function/read-assembly constructors
        // return one short static ConnectorError diagnostic on an ordinary
        // failure. These are not covered by source B or an output byte buffer.
        // The component stops at that first error; no formatting or cloning of
        // the error is performed here. Nested owner ports count their own work.
        m.request::<u8>(256, 1)?;
        m.request::<T>(scans, 1)?;
        m.request::<usize>(scans, 1)?;
        m.work(prepare_work_upper_bound(scans)?)?;
        Ok(m)
    }
    fn request<T>(&mut self, n: usize, copies: usize) -> Result<(), E> {
        self.request_layout(layout::<T>(n)?, copies)
    }
    fn request_layout(&mut self, l: Layout, copies: usize) -> Result<(), E> {
        if l.size() == 0 || copies == 0 {
            return Ok(());
        }
        let bytes = mul(l.size(), copies)?;
        self.facts.allocation_requests_upper_bound =
            add(self.facts.allocation_requests_upper_bound, copies)?;
        self.facts.allocation_request_bytes_upper_bound =
            add(self.facts.allocation_request_bytes_upper_bound, bytes)?;
        self.work(add(mul(bytes, 4)?, mul(copies, 128)?)?)
    }
    fn domain(&mut self, f: codec::DomainResourceFacts) -> Result<(), E> {
        self.items(f.range_count)?;
        self.facts.scalar_bytes = add(self.facts.scalar_bytes, f.scalar_bytes)?;
        self.facts.allocation_requests_upper_bound = add(
            self.facts.allocation_requests_upper_bound,
            f.allocation_requests_upper_bound,
        )?;
        self.facts.allocation_request_bytes_upper_bound = add(
            self.facts.allocation_request_bytes_upper_bound,
            f.allocation_request_bytes_upper_bound,
        )?;
        self.work(f.cumulative_work_upper_bound)
    }
    fn domain_prefix(
        &mut self,
        current: codec::DomainResourceFacts,
        previous: &mut codec::DomainResourceFacts,
    ) -> Result<(), E> {
        // Only this original author's monotone contribution is replaced. No
        // other child/source B or parent facts are subtracted or reset.
        let delta = codec::DomainResourceFacts {
            range_count: current
                .range_count
                .checked_sub(previous.range_count)
                .ok_or(E::InvalidShape("Domain range prefix decreased"))?,
            scalar_bytes: current
                .scalar_bytes
                .checked_sub(previous.scalar_bytes)
                .ok_or(E::InvalidShape("Domain byte prefix decreased"))?,
            allocation_requests_upper_bound: current
                .allocation_requests_upper_bound
                .checked_sub(previous.allocation_requests_upper_bound)
                .ok_or(E::InvalidShape("Domain request prefix decreased"))?,
            allocation_request_bytes_upper_bound: current
                .allocation_request_bytes_upper_bound
                .checked_sub(previous.allocation_request_bytes_upper_bound)
                .ok_or(E::InvalidShape("Domain request byte prefix decreased"))?,
            cumulative_work_upper_bound: current
                .cumulative_work_upper_bound
                .checked_sub(previous.cumulative_work_upper_bound)
                .ok_or(E::InvalidShape("Domain work prefix decreased"))?,
        };
        self.domain(delta)?;
        *previous = current;
        Ok(())
    }
    fn work(&mut self, n: usize) -> Result<(), E> {
        self.facts.cumulative_work_upper_bound = add(self.facts.cumulative_work_upper_bound, n)?;
        Ok(())
    }
    fn items(&mut self, n: usize) -> Result<(), E> {
        self.facts.item_count = add(self.facts.item_count, n)?;
        self.work(mul(n, 256)?)
    }
    fn bytes(&mut self, n: usize) -> Result<(), E> {
        self.facts.scalar_bytes = add(self.facts.scalar_bytes, n)?;
        self.request::<u8>(n, 1)
    }
    fn floor(&self, n: usize) -> Result<(), E> {
        if n > self.source {
            Err(E::InvalidShape("read scan source invoice is understated"))
        } else {
            Ok(())
        }
    }
    fn facts(
        &self,
        current: &c::WriterOwnedResourceFacts,
        l: ReadScanProjectionLimits,
    ) -> Result<ReadScanProjectionFacts, E> {
        let mut f = self.facts;
        f.allocation_requests_upper_bound = add(
            f.allocation_requests_upper_bound,
            add(
                self.completed.allocation_requests,
                current.allocation_requests,
            )?,
        )?;
        f.allocation_request_bytes_upper_bound = add(
            f.allocation_request_bytes_upper_bound,
            add(self.completed.requested_bytes, current.requested_bytes)?,
        )?;
        f.cumulative_work_upper_bound = add(
            f.cumulative_work_upper_bound,
            add(self.completed.work_units, current.work_units)?,
        )?;
        f.coexisting_source_and_request_bytes_upper_bound =
            add(self.source, f.allocation_request_bytes_upper_bound)?;
        if f.scan_count > l.max_scans
            || f.item_count > l.max_items
            || f.scalar_bytes > l.max_scalar_bytes
            || f.allocation_requests_upper_bound > l.max_allocation_requests
            || f.allocation_request_bytes_upper_bound > l.max_allocation_request_bytes
            || f.coexisting_source_and_request_bytes_upper_bound
                > l.max_coexisting_source_and_request_bytes
            || f.cumulative_work_upper_bound > l.max_work
        {
            return Err(E::Control(CompileControlError::ResourceExhausted));
        }
        Ok(f)
    }
    fn gate(
        &self,
        l: ReadScanProjectionLimits,
        a: &mut impl FnMut(&ReadScanProjectionFacts) -> Result<(), CompileControlError>,
    ) -> Result<(), E> {
        a(&self.facts(&Default::default(), l)?)?;
        Ok(())
    }
    fn complete(&mut self, f: c::WriterOwnedResourceFacts) -> Result<(), E> {
        self.completed.allocation_requests =
            add(self.completed.allocation_requests, f.allocation_requests)?;
        self.completed.requested_bytes = add(self.completed.requested_bytes, f.requested_bytes)?;
        self.completed.work_units = add(self.completed.work_units, f.work_units)?;
        Ok(())
    }
}

fn hash_encode(v: c::ConnectorReadPartitionHash) -> i32 {
    match v {
        c::ConnectorReadPartitionHash::XxHash64 => 1,
        c::ConnectorReadPartitionHash::Murmur3X64_128 => 2,
    }
}
fn hash_decode(
    v: i32,
    work: &mut CompileCheckpoints<'_>,
) -> Result<c::ConnectorReadPartitionHash, E> {
    observed(
        match v {
            1 => Ok(c::ConnectorReadPartitionHash::XxHash64),
            2 => Ok(c::ConnectorReadPartitionHash::Murmur3X64_128),
            _ => Err(E::InvalidShape("unknown read partition hash")),
        },
        work,
    )
}
fn layout_encode(v: c::ConnectorReadBucketLayout) -> i32 {
    match v {
        c::ConnectorReadBucketLayout::DirectModulo => 1,
        c::ConnectorReadBucketLayout::JumpConsistent => 2,
    }
}
fn layout_decode(
    v: i32,
    work: &mut CompileCheckpoints<'_>,
) -> Result<c::ConnectorReadBucketLayout, E> {
    observed(
        match v {
            1 => Ok(c::ConnectorReadBucketLayout::DirectModulo),
            2 => Ok(c::ConnectorReadBucketLayout::JumpConsistent),
            _ => Err(E::InvalidShape("unknown read bucket layout")),
        },
        work,
    )
}
fn relation_encode(v: c::ConnectorReadRelationKind) -> i32 {
    match v {
        c::ConnectorReadRelationKind::Table => 1,
        c::ConnectorReadRelationKind::TableFunction => 2,
        c::ConnectorReadRelationKind::ChangeWindow => 3,
        c::ConnectorReadRelationKind::SystemTable => 4,
        c::ConnectorReadRelationKind::TableExecute => 5,
        c::ConnectorReadRelationKind::MergeTable => 6,
    }
}
fn relation_decode(
    v: i32,
    work: &mut CompileCheckpoints<'_>,
) -> Result<c::ConnectorReadRelationKind, E> {
    observed(
        match v {
            1 => Ok(c::ConnectorReadRelationKind::Table),
            2 => Ok(c::ConnectorReadRelationKind::TableFunction),
            3 => Ok(c::ConnectorReadRelationKind::ChangeWindow),
            4 => Ok(c::ConnectorReadRelationKind::SystemTable),
            5 => Ok(c::ConnectorReadRelationKind::TableExecute),
            6 => Ok(c::ConnectorReadRelationKind::MergeTable),
            _ => Err(E::InvalidShape("unknown read relation kind")),
        },
        work,
    )
}
fn encode_properties(
    input: &c::ConnectorReadProperties<c::ScanColumnId>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<w::ConnectorReadProperties, E> {
    use c::ConnectorReadDistribution as D;
    use w::connector_read_distribution::Kind;
    let keys =
        |keys: &[c::ScanColumnId], work: &mut CompileCheckpoints<'_>| -> Result<Vec<u32>, E> {
            let mut out = reserve(keys.len(), work)?;
            for k in keys {
                out.push(ordinal(k.index())?);
                work.step()?;
            }
            Ok(out)
        };
    let kind = match input.distribution() {
        D::Unconstrained => Kind::Unconstrained(Default::default()),
        D::Singleton => Kind::Singleton(Default::default()),
        D::RoundRobin => Kind::RoundRobin(Default::default()),
        D::Hash {
            keys: k,
            partition_space,
            admissible,
            algorithm,
        } => Kind::Hash(w::ConnectorReadHashDistribution {
            key_column_ordinals: keys(k, work)?,
            partition_space: copy(partition_space, work)?,
            admissible: Some(w::ConnectorReadPartitionCountDomain {
                min: admissible.min(),
                max: admissible.max(),
                requires_power_of_two: admissible.requires_power_of_two(),
            }),
            algorithm: hash_encode(*algorithm),
        }),
        D::BucketShuffle {
            keys: k,
            partition_space,
            bucket_count,
            hash,
            layout,
            ordinal_domain_evidence,
        } => Kind::BucketShuffle(w::ConnectorReadBucketDistribution {
            key_column_ordinals: keys(k, work)?,
            partition_space: copy(partition_space, work)?,
            bucket_count: *bucket_count,
            hash: hash_encode(*hash),
            layout: layout_encode(*layout),
            ordinal_domain_evidence: copy(ordinal_domain_evidence, work)?,
        }),
    };
    let mut ordering = reserve(input.ordering().len(), work)?;
    for k in input.ordering() {
        ordering.push(w::ConnectorReadOrderingKey {
            column_ordinal: ordinal(k.column().index())?,
            direction: match k.direction() {
                c::ConnectorReadSortDirection::Ascending => 1,
                c::ConnectorReadSortDirection::Descending => 2,
            },
            null_ordering: match k.null_ordering() {
                c::ConnectorReadNullOrdering::First => 1,
                c::ConnectorReadNullOrdering::Last => 2,
            },
        });
        work.step()?;
    }
    Ok(w::ConnectorReadProperties {
        distribution: Some(w::ConnectorReadDistribution { kind: Some(kind) }),
        ordering,
    })
}
fn decode_properties(
    input: &w::ConnectorReadProperties,
    work: &mut CompileCheckpoints<'_>,
) -> Result<c::ConnectorReadProperties<c::ScanColumnId>, E> {
    use c::ConnectorReadDistribution as D;
    use w::connector_read_distribution::Kind;
    let distribution = required(
        input.distribution.as_ref(),
        "read distribution absent",
        work,
    )?;
    let kind = required(
        distribution.kind.as_ref(),
        "read distribution kind absent",
        work,
    )?;
    let keys =
        |keys: &[u32], work: &mut CompileCheckpoints<'_>| -> Result<Arc<[c::ScanColumnId]>, E> {
            let mut out = reserve(keys.len(), work)?;
            for k in keys {
                out.push(c::ScanColumnId::new(*k as usize));
                work.step()?;
            }
            opaque(work, || Ok(Arc::from(out)))
        };
    let d = match kind {
        Kind::Unconstrained(_) => D::Unconstrained,
        Kind::Singleton(_) => D::Singleton,
        Kind::RoundRobin(_) => D::RoundRobin,
        Kind::Hash(v) => {
            let a = required(
                v.admissible.as_ref(),
                "read partition count domain absent",
                work,
            )?;
            D::Hash {
                keys: keys(&v.key_column_ordinals, work)?,
                partition_space: array32(
                    &v.partition_space,
                    "read partition space must have 32 bytes",
                    work,
                )?,
                admissible: opaque(work, || {
                    c::ConnectorReadPartitionCountDomain::try_new(
                        a.min,
                        a.max,
                        a.requires_power_of_two,
                    )
                    .map_err(E::from)
                })?,
                algorithm: hash_decode(v.algorithm, work)?,
            }
        }
        Kind::BucketShuffle(v) => D::BucketShuffle {
            keys: keys(&v.key_column_ordinals, work)?,
            partition_space: array32(
                &v.partition_space,
                "read partition space must have 32 bytes",
                work,
            )?,
            bucket_count: v.bucket_count,
            hash: hash_decode(v.hash, work)?,
            layout: layout_decode(v.layout, work)?,
            ordinal_domain_evidence: array32(
                &v.ordinal_domain_evidence,
                "read bucket evidence must have 32 bytes",
                work,
            )?,
        },
    };
    let mut ordering = reserve(input.ordering.len(), work)?;
    for k in &input.ordering {
        let direction = observed(
            match k.direction {
                1 => Ok(c::ConnectorReadSortDirection::Ascending),
                2 => Ok(c::ConnectorReadSortDirection::Descending),
                _ => Err(E::InvalidShape("unknown read sort direction")),
            },
            work,
        )?;
        let null_ordering = observed(
            match k.null_ordering {
                1 => Ok(c::ConnectorReadNullOrdering::First),
                2 => Ok(c::ConnectorReadNullOrdering::Last),
                _ => Err(E::InvalidShape("unknown read null ordering")),
            },
            work,
        )?;
        ordering.push(c::ConnectorReadOrderingKey::new(
            c::ScanColumnId::new(k.column_ordinal as usize),
            direction,
            null_ordering,
        ));
        work.step()?;
    }
    opaque(work, || {
        c::ConnectorReadProperties::try_new(d, ordering).map_err(E::from)
    })
}
fn encode_tuple(
    input: &c::TupleDomain<c::ScanColumnId>,
    model: &Model,
    limits: ReadScanProjectionLimits,
    admit: &mut impl FnMut(&ReadScanProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<w::ScanTupleDomain, E> {
    use w::scan_tuple_domain::Kind;
    let kind = if input.is_none() {
        Kind::None(Default::default())
    } else if input.is_all() {
        Kind::All(Default::default())
    } else {
        let entries = input.domains().expect("constrained tuple");
        let mut out = reserve(entries.len(), work)?;
        for (k, d) in entries {
            let mut child_admit = |_facts: &codec::DomainResourceFacts| {
                model.gate(limits, admit).map_err(merge_child_control)
            };
            // The whole original Domain inventory was counted once in the
            // parent pass; this child admission rechecks that same parent.
            let (domain, _) = codec::encode_domain_observed(d, &mut child_admit, work)?;
            out.push(w::ScanColumnDomain {
                column_ordinal: ordinal(k.index())?,
                domain: Some(domain),
            });
            work.step()?;
        }
        Kind::Columns(w::ScanColumnDomains { entries: out })
    };
    Ok(w::ScanTupleDomain { kind: Some(kind) })
}
fn decode_tuple(
    input: &w::ScanTupleDomain,
    model: &Model,
    limits: ReadScanProjectionLimits,
    admit: &mut impl FnMut(&ReadScanProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<c::TupleDomain<c::ScanColumnId>, E> {
    use w::scan_tuple_domain::Kind;
    match required(input.kind.as_ref(), "scan tuple kind absent", work)? {
        Kind::All(_) => Ok(c::TupleDomain::all()),
        Kind::None(_) => Ok(c::TupleDomain::none()),
        Kind::Columns(v) => {
            let mut out = BTreeMap::new();
            let mut previous = None;
            for entry in &v.entries {
                if previous.is_some_and(|p| p >= entry.column_ordinal) {
                    return observed(
                        Err(E::InvalidShape(
                            "scan tuple column ordinals must be strictly ascending",
                        )),
                        work,
                    );
                }
                previous = Some(entry.column_ordinal);
                let domain = required(entry.domain.as_ref(), "scan column domain absent", work)?;
                let mut child_admit = |_facts: &codec::DomainResourceFacts| {
                    model.gate(limits, admit).map_err(merge_child_control)
                };
                let (domain, _) =
                    codec::decode_domain_observed(domain, "scan_domain", &mut child_admit, work)?;
                work.flush()?;
                out.insert(c::ScanColumnId::new(entry.column_ordinal as usize), domain);
                work.step()?;
                work.flush()?;
            }
            opaque(work, || {
                c::TupleDomain::with_column_domains(out).map_err(E::from)
            })
        }
    }
}

fn count_value(value: &c::ConnectorValue, m: &mut Model) -> Result<(), E> {
    use c::ConnectorValue as V;
    match value {
        V::Decimal { .. } | V::Uuid(_) => m.bytes(16),
        V::Varchar(v) => m.bytes(v.len()),
        V::Varbinary(v) | V::Fixed(v) => m.bytes(v.len()),
        _ => Ok(()),
    }
}

fn count_raw_value(value: &cv::Value, m: &mut Model) -> Result<(), E> {
    m.domain(codec::value_decode_resource_facts(value)?)
}

fn count_tuple(
    input: &c::TupleDomain<c::ScanColumnId>,
    m: &mut Model,
    limits: ReadScanProjectionLimits,
    admit: &mut impl FnMut(&ReadScanProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), E> {
    if let Some(domains) = input.domains() {
        m.items(domains.len())?;
        m.request::<w::ScanColumnDomain>(domains.len(), 1)?;
        m.gate(limits, admit)?;
        for domain in domains.values() {
            let mut previous = codec::DomainResourceFacts::default();
            // The original prefix author is monotone. Any ordinary source-model
            // error is retained outside the Control-only admission callback.
            let mut failure = None;
            let result = codec::domain_encode_resource_facts_admitted(
                domain,
                &mut |f| {
                    if let Err(error) = m.domain_prefix(*f, &mut previous) {
                        let cause = match &error {
                            E::Control(c) => c.clone(),
                            _ => CompileControlError::ResourceExhausted,
                        };
                        failure = Some(error);
                        return Err(cause);
                    }
                    m.gate(limits, admit).map_err(merge_child_control)
                },
                work,
            );
            if let Some(error) = failure {
                return Err(error);
            }
            result?;
        }
    }
    Ok(())
}
fn count_raw_tuple(
    input: &w::ScanTupleDomain,
    m: &mut Model,
    limits: ReadScanProjectionLimits,
    admit: &mut impl FnMut(&ReadScanProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), E> {
    if let Some(w::scan_tuple_domain::Kind::Columns(v)) = &input.kind {
        m.items(v.entries.len())?;
        let table = crate::btree_resources_v2::insertion_only::<c::ScanColumnId, c::Domain>(
            v.entries.len(),
        )
        .map_err(|e| match e {
            crate::btree_resources_v2::BTreeResourceError::Arithmetic(_) => {
                E::Control(CompileControlError::ResourceExhausted)
            }
            crate::btree_resources_v2::BTreeResourceError::SourceModel(m) => E::SourceModel(m),
        })?;
        m.facts.allocation_requests_upper_bound = add(
            m.facts.allocation_requests_upper_bound,
            table.allocation_requests_upper_bound,
        )?;
        m.facts.allocation_request_bytes_upper_bound = add(
            m.facts.allocation_request_bytes_upper_bound,
            table.request_bytes_upper_bound,
        )?;
        m.work(table.cumulative_work_upper_bound)?;
        m.work(
            crate::btree_resources_v2::retain_work::<c::ScanColumnId, c::Domain>(v.entries.len())
                .map_err(|error| match error {
                crate::btree_resources_v2::BTreeResourceError::Arithmetic(_) => {
                    E::Control(CompileControlError::ResourceExhausted)
                }
                crate::btree_resources_v2::BTreeResourceError::SourceModel(message) => {
                    E::SourceModel(message)
                }
            })?,
        )?;
        m.gate(limits, admit)?;
        for entry in &v.entries {
            if let Some(domain) = &entry.domain {
                let mut previous = codec::DomainResourceFacts::default();
                let mut failure = None;
                let result = codec::domain_decode_resource_facts_admitted(
                    domain,
                    &mut |f| {
                        if let Err(error) = m.domain_prefix(*f, &mut previous) {
                            let cause = match &error {
                                E::Control(c) => c.clone(),
                                _ => CompileControlError::ResourceExhausted,
                            };
                            failure = Some(error);
                            return Err(cause);
                        }
                        m.gate(limits, admit).map_err(merge_child_control)
                    },
                    work,
                );
                if let Some(error) = failure {
                    return Err(error);
                }
                result?;
            }
            m.gate(limits, admit)?;
            work.step()?;
        }
    }
    Ok(())
}
fn count_expr(
    input: &c::ConnectorExpression,
    m: &mut Model,
    depth: usize,
    limits: ReadScanProjectionLimits,
    admit: &mut impl FnMut(&ReadScanProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<usize, E> {
    if depth > c::MAX_CONNECTOR_EXPRESSION_DEPTH {
        return Err(E::InvalidShape(
            "connector expression source depth exceeds original bound",
        ));
    }
    m.items(1)?;
    // Complete the actual occurrence's shallow header inventory before its
    // observation. In particular a Call's original child Vec request is known
    // before recursively touching its first child.
    match input {
        c::ConnectorExpression::Constant { value, .. } => {
            if let Some(v) = value {
                count_value(v, m)?;
            }
        }
        c::ConnectorExpression::Variable { name, .. } => m.bytes(name.len())?,
        c::ConnectorExpression::FieldDereference { .. } => {}
        c::ConnectorExpression::Call {
            function,
            arguments,
            ..
        } => {
            m.bytes(function.as_str().len())?;
            m.request::<u32>(arguments.len(), 1)?;
        }
    }
    m.gate(limits, admit)?;
    work.step()?;
    let mut n = 1;
    match input {
        c::ConnectorExpression::FieldDereference { target, .. } => {
            n = add(n, count_expr(target, m, depth + 1, limits, admit, work)?)?
        }
        c::ConnectorExpression::Call { arguments, .. } => {
            for child in arguments {
                n = add(n, count_expr(child, m, depth + 1, limits, admit, work)?)?;
            }
        }
        _ => {}
    }
    if n > c::MAX_CONNECTOR_EXPRESSION_NODES {
        return Err(E::InvalidShape(
            "connector residual expansion exceeds original bound",
        ));
    }
    Ok(n)
}

fn emit_expr(
    input: &c::ConnectorExpression,
    ids: &[u32],
    at: &mut usize,
    out: &mut Vec<w::ConnectorExpressionDefinition>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<u32, E> {
    use w::connector_expression_definition::Kind;
    let id = ids[*at];
    *at += 1;
    let kind = match input {
        c::ConnectorExpression::Constant { value, value_type } => {
            Kind::Constant(w::ConnectorConstantExpression {
                value_type: Some(codec::encode_value_type(*value_type)),
                value: match value {
                    Some(v) => Some(opaque(work, || Ok(codec::encode_value(v)))?),
                    None => None,
                },
            })
        }
        c::ConnectorExpression::Variable { name, value_type } => {
            Kind::Variable(w::ConnectorVariableExpression {
                name: text(name, work)?,
                value_type: Some(codec::encode_value_type(*value_type)),
            })
        }
        c::ConnectorExpression::FieldDereference {
            target,
            field_index,
            value_type,
        } => {
            let target = emit_expr(target, ids, at, out, work)?;
            Kind::FieldDereference(w::ConnectorFieldDereferenceExpression {
                target_connector_expression_id: Some(target),
                field_index: *field_index,
                value_type: Some(codec::encode_value_type(*value_type)),
            })
        }
        c::ConnectorExpression::Call {
            function,
            value_type,
            arguments,
        } => {
            let mut children = reserve(arguments.len(), work)?;
            for child in arguments {
                children.push(emit_expr(child, ids, at, out, work)?);
                work.step()?;
            }
            Kind::Call(w::ConnectorCallExpression {
                function_name: text(function.as_str(), work)?,
                value_type: Some(codec::encode_value_type(*value_type)),
                argument_connector_expression_ids: children,
            })
        }
    };
    out.push(w::ConnectorExpressionDefinition {
        id,
        kind: Some(kind),
    });
    work.step()?;
    Ok(id)
}
fn expr_type(kind: &w::connector_expression_definition::Kind) -> Option<&cv::ValueType> {
    use w::connector_expression_definition::Kind;
    match kind {
        Kind::Constant(v) => v.value_type.as_ref(),
        Kind::Variable(v) => v.value_type.as_ref(),
        Kind::FieldDereference(v) => v.value_type.as_ref(),
        Kind::Call(v) => v.value_type.as_ref(),
    }
}
fn count_raw_expression_header(
    kind: &w::connector_expression_definition::Kind,
    m: &mut Model,
) -> Result<(), E> {
    m.domain(codec::value_type_decode_resource_facts()?)?;
    m.items(1)?;
    use w::connector_expression_definition::Kind;
    match kind {
        Kind::Constant(v) => {
            if let Some(v) = &v.value {
                count_raw_value(v, m)?;
            }
        }
        Kind::Variable(v) => {
            m.bytes(v.name.len())?;
            m.request_layout(arc_layout(layout::<u8>(v.name.len())?)?, 1)?;
        }
        Kind::FieldDereference(_) => m.request::<c::ConnectorExpression>(1, 2)?,
        Kind::Call(v) => {
            m.bytes(v.function_name.len())?;
            m.request_layout(arc_layout(layout::<u8>(v.function_name.len())?)?, 1)?;
            m.request::<c::ConnectorExpression>(v.argument_connector_expression_ids.len(), 3)?;
        }
    }
    Ok(())
}
#[expect(
    clippy::too_many_arguments,
    reason = "Occurrence expansion borrows the original graph, active path and parent admission."
)]
fn preflight_expr(
    id: u32,
    expanded: &mut usize,
    defs: &[w::ConnectorExpressionDefinition],
    index: &BindingIndex,
    used: &mut [bool],
    path: &mut [u32; 65],
    depth: usize,
    m: &mut Model,
    l: ReadScanProjectionLimits,
    a: &mut impl FnMut(&ReadScanProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<usize, E> {
    *expanded = add(*expanded, 1)?;
    if *expanded > c::MAX_CONNECTOR_EXPRESSION_NODES {
        return Err(E::InvalidShape(
            "connector residual expansion exceeds original bound",
        ));
    }
    if depth >= path.len() {
        return Err(E::InvalidShape(
            "connector residual depth exceeds original bound",
        ));
    }
    if path[..depth - 1].contains(&id) {
        return Err(E::InvalidShape("connector residual graph has a cycle"));
    }
    path[depth - 1] = id;
    m.work(lookup_work_upper_bound(defs.len()))?;
    m.gate(l, a)?;
    let at = required(
        index.find(id, |at| defs[at].id, work)?,
        "connector residual ID absent",
        work,
    )?;
    let kind = required(
        defs[at].kind.as_ref(),
        "connector residual kind absent",
        work,
    )?;
    // The first occurrence was prefunded from the immutable raw definition
    // before any index callback; shared DAG edges create new tree occurrences.
    if used[at] {
        count_raw_expression_header(kind, m)?;
    }
    use w::connector_expression_definition::Kind;
    m.gate(l, a)?;
    used[at] = true;
    work.step()?;
    required(
        expr_type(kind),
        "connector residual value type absent",
        work,
    )?;
    let mut n = 1;
    match kind {
        Kind::FieldDereference(v) => {
            let child = required(
                v.target_connector_expression_id,
                "connector target ID absent",
                work,
            )?;
            n = add(
                n,
                preflight_expr(
                    child,
                    expanded,
                    defs,
                    index,
                    used,
                    path,
                    depth + 1,
                    m,
                    l,
                    a,
                    work,
                )?,
            )?;
        }
        Kind::Call(v) => {
            for child in &v.argument_connector_expression_ids {
                n = add(
                    n,
                    preflight_expr(
                        *child,
                        expanded,
                        defs,
                        index,
                        used,
                        path,
                        depth + 1,
                        m,
                        l,
                        a,
                        work,
                    )?,
                )?;
            }
        }
        _ => {}
    }
    if n > c::MAX_CONNECTOR_EXPRESSION_NODES {
        return Err(E::InvalidShape(
            "connector residual expansion exceeds original bound",
        ));
    }
    Ok(n)
}
fn decode_expr(
    id: u32,
    defs: &[w::ConnectorExpressionDefinition],
    index: &BindingIndex,
    work: &mut CompileCheckpoints<'_>,
) -> Result<c::ConnectorExpression, E> {
    let at = required(
        index.find(id, |at| defs[at].id, work)?,
        "connector residual ID absent",
        work,
    )?;
    let kind = required(
        defs[at].kind.as_ref(),
        "connector residual kind absent",
        work,
    )?;
    let raw_type = required(
        expr_type(kind),
        "connector residual value type absent",
        work,
    )?;
    let ty = opaque(work, || {
        codec::decode_value_type(raw_type, FieldPath::root("connector_expression")).map_err(E::from)
    })?;
    use w::connector_expression_definition::Kind;
    let out = match kind {
        Kind::Constant(v) => c::ConnectorExpression::Constant {
            value: match &v.value {
                Some(v) => Some(opaque(work, || {
                    codec::decode_value(v, ty, FieldPath::root("connector_constant"))
                        .map_err(E::from)
                })?),
                None => None,
            },
            value_type: ty,
        },
        Kind::Variable(v) => c::ConnectorExpression::Variable {
            name: opaque(work, || Ok(Arc::from(v.name.as_str())))?,
            value_type: ty,
        },
        Kind::FieldDereference(v) => {
            let child = decode_expr(
                v.target_connector_expression_id.expect("preflight target"),
                defs,
                index,
                work,
            )?;
            c::ConnectorExpression::FieldDereference {
                target: opaque(work, || Ok(Box::new(child)))?,
                field_index: v.field_index,
                value_type: ty,
            }
        }
        Kind::Call(v) => {
            let function = opaque(work, || {
                c::ConnectorFunctionName::try_new(&v.function_name).map_err(E::from)
            })?;
            let mut arguments = reserve(v.argument_connector_expression_ids.len(), work)?;
            for id in &v.argument_connector_expression_ids {
                arguments.push(decode_expr(*id, defs, index, work)?);
                work.step()?;
            }
            c::ConnectorExpression::Call {
                function,
                value_type: ty,
                arguments,
            }
        }
    };
    work.step()?;
    Ok(out)
}

fn arc_request<T>(n: usize, m: &mut Model) -> Result<(), E> {
    let bytes = arc_layout(layout::<T>(n)?)?;
    m.request_layout(bytes, 1)
}
fn count_properties_source(
    p: &c::ConnectorReadProperties<c::ScanColumnId>,
    m: &mut Model,
) -> Result<(), E> {
    use c::ConnectorReadDistribution as D;
    match p.distribution() {
        D::Hash { keys, .. } => {
            m.items(keys.len())?;
            m.request::<u32>(keys.len(), 1)?;
            m.bytes(32)?;
        }
        D::BucketShuffle { keys, .. } => {
            m.items(keys.len())?;
            m.request::<u32>(keys.len(), 1)?;
            m.bytes(32)?;
            m.bytes(32)?;
        }
        _ => {}
    }
    m.items(p.ordering().len())?;
    m.request::<w::ConnectorReadOrderingKey>(p.ordering().len(), 1)
}
fn count_properties_raw(p: &w::ConnectorReadProperties, m: &mut Model) -> Result<(), E> {
    use w::connector_read_distribution::Kind;
    if let Some(d) = p.distribution.as_ref().and_then(|d| d.kind.as_ref()) {
        let keys = match d {
            Kind::Hash(v) => Some(v.key_column_ordinals.as_slice()),
            Kind::BucketShuffle(v) => Some(v.key_column_ordinals.as_slice()),
            _ => None,
        };
        if let Some(keys) = keys {
            m.items(keys.len())?;
            m.request::<c::ScanColumnId>(keys.len(), 2)?;
            arc_request::<c::ScanColumnId>(keys.len(), m)?;
            // The original has_duplicate_columns sorts a distinct Vec<&C>.
            m.request::<&c::ScanColumnId>(keys.len(), 1)?;
            m.work(mul(keys.len(), mul(keys.len(), 16)?)?)?;
        }
    }
    m.items(p.ordering.len())?;
    m.request::<c::ConnectorReadOrderingKey<c::ScanColumnId>>(p.ordering.len(), 1)?;
    arc_request::<c::ConnectorReadOrderingKey<c::ScanColumnId>>(p.ordering.len(), m)?;
    // The original property author collects and sorts a separate ordinal list;
    // has_duplicate_columns also copies the distribution and ordering lists.
    m.request::<c::ScanColumnId>(p.ordering.len(), 1)?;
    m.request::<&c::ScanColumnId>(p.ordering.len(), 1)?;
    m.work(mul(p.ordering.len(), mul(p.ordering.len(), 16)?)?)
}
fn count_public_source(p: &c::ConnectorReadPublicFacts, m: &mut Model) -> Result<(), E> {
    let s = p.source();
    m.bytes(s.input_version().as_bytes().len())?;
    m.bytes(32)?;
    m.bytes(s.coverage_evidence().len())?;
    count_properties_source(s.properties(), m)?;
    if let Some(k) = p.metadata_kind() {
        m.bytes(k.as_str().len())?;
    }
    if let c::ConnectorReadArtifactCoverage::Exact { evidence, .. } = s.artifact_coverage() {
        m.bytes(32)?;
        m.bytes(32)?;
        m.bytes(evidence.len())?;
    }
    Ok(())
}
fn count_public_raw(p: &w::ConnectorReadPublicFacts, m: &mut Model) -> Result<(), E> {
    // The two namespace IDs are inline scalar references. Retired root-label
    // backing has no request/work contribution. The containing DTO header uses
    // the actual generated FrozenReadScan Layout, including this contraction.
    if let Some(s) = &p.source {
        for n in [s.input_version.len(), s.coverage_evidence.len()] {
            m.bytes(n)?;
            arc_request::<u8>(n, m)?;
        }
        if let Some(properties) = &s.properties {
            count_properties_raw(properties, m)?;
        }
        if let Some(w::connector_read_artifact_coverage::Kind::Exact(exact)) =
            s.artifact_coverage.as_ref().and_then(|v| v.kind.as_ref())
        {
            m.bytes(exact.evidence.len())?;
            arc_request::<u8>(exact.evidence.len(), m)?;
        }
    }
    if let Some(k) = &p.metadata_kind {
        m.bytes(k.len())?;
        arc_request::<u8>(k.len(), m)?;
    }
    Ok(())
}
fn encode_public(
    p: &c::ConnectorReadPublicFacts,
    schema_id: u32,
    source_schema_id: u32,
    work: &mut CompileCheckpoints<'_>,
) -> Result<w::ConnectorReadPublicFacts, E> {
    use w::connector_read_artifact_coverage::Kind;
    let s = p.source();
    let kind = match s.artifact_coverage() {
        c::ConnectorReadArtifactCoverage::NoArtifactInputs => {
            Kind::NoArtifactInputs(Default::default())
        }
        c::ConnectorReadArtifactCoverage::Exact {
            source_selection_digest,
            content_digest,
            evidence,
        } => Kind::Exact(w::ConnectorReadExactArtifactCoverage {
            source_selection_digest: copy(source_selection_digest, work)?,
            content_digest: copy(content_digest, work)?,
            evidence: copy(evidence, work)?,
        }),
    };
    Ok(w::ConnectorReadPublicFacts {
        execution_schema_id: Some(schema_id),
        source_schema_id: Some(source_schema_id),
        source: Some(w::ConnectorReadStaticFacts {
            input_version: copy(s.input_version().as_bytes(), work)?,
            selection_digest: copy(&s.selection_digest(), work)?,
            properties: Some(encode_properties(s.properties(), work)?),
            artifact_coverage: Some(w::ConnectorReadArtifactCoverage { kind: Some(kind) }),
            coverage_evidence: copy(s.coverage_evidence(), work)?,
        }),
        metadata_kind: match p.metadata_kind() {
            Some(k) => Some(text(k.as_str(), work)?),
            None => None,
        },
    })
}
fn require_public_schema_references(
    public: &w::ConnectorReadPublicFacts,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), E> {
    required(
        public.execution_schema_id,
        "read execution schema ID absent",
        work,
    )?;
    required(
        public.source_schema_id,
        "read source schema facts ID absent",
        work,
    )?;
    Ok(())
}

type DecodedPublicSource = (
    c::ConnectorReadStaticFacts<c::ScanColumnId>,
    Option<c::ConnectorReadMetadataKind>,
);
fn decode_public_source(
    p: &w::ConnectorReadPublicFacts,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DecodedPublicSource, E> {
    let s = required(p.source.as_ref(), "read public source absent", work)?;
    let version = opaque(work, || {
        c::ConnectorReadInputVersion::try_new(s.input_version.as_slice()).map_err(E::from)
    })?;
    let selection = array32(
        &s.selection_digest,
        "read selection digest must have 32 bytes",
        work,
    )?;
    let properties = decode_properties(
        required(s.properties.as_ref(), "read source properties absent", work)?,
        work,
    )?;
    let coverage = required(
        s.artifact_coverage.as_ref(),
        "read artifact coverage absent",
        work,
    )?;
    let coverage = match required(
        coverage.kind.as_ref(),
        "read artifact coverage kind absent",
        work,
    )? {
        w::connector_read_artifact_coverage::Kind::NoArtifactInputs(_) => {
            c::ConnectorReadArtifactCoverage::NoArtifactInputs
        }
        w::connector_read_artifact_coverage::Kind::Exact(v) => {
            let source = array32(
                &v.source_selection_digest,
                "artifact source digest must have 32 bytes",
                work,
            )?;
            let content = array32(
                &v.content_digest,
                "artifact content digest must have 32 bytes",
                work,
            )?;
            opaque(work, || {
                c::ConnectorReadArtifactCoverage::exact(source, content, v.evidence.as_slice())
                    .map_err(E::from)
            })?
        }
    };
    let source = opaque(work, || {
        c::ConnectorReadStaticFacts::try_new(
            version,
            selection,
            properties,
            coverage,
            s.coverage_evidence.as_slice(),
        )
        .map_err(E::from)
    })?;
    let metadata_kind = match &p.metadata_kind {
        Some(k) => Some(opaque(work, || {
            c::ConnectorReadMetadataKind::try_new(k).map_err(E::from)
        })?),
        None => None,
    };
    Ok((source, metadata_kind))
}
fn same_control(
    actual: &dyn novarocks_type_contract::PureCompileControl,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), E> {
    observed(
        if std::ptr::addr_eq(actual, work.control()) {
            Ok(())
        } else {
            Err(E::InvalidShape(
                "read scan namespace has another original control",
            ))
        },
        work,
    )
}

/// Caller-work port: the containing owner supplies the one Encode scope/footer.
/// Schema and provider/payload IDs certify the exact same-emission source loan.
pub fn encode_read_scans_observed(
    sources: &[ReadScanSource<'_>],
    context: ReadScanEncodeContext<'_, '_, '_>,
    source_retained_bytes: usize,
    limits: ReadScanProjectionLimits,
    admit: &mut impl FnMut(&ReadScanProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<
    (
        Vec<w::FrozenReadScan>,
        Vec<w::ConnectorExpressionDefinition>,
        ReadScanProjectionFacts,
    ),
    E,
> {
    let mut m = Model::new::<w::FrozenReadScan>(sources.len(), source_retained_bytes)?;
    // Namespace counts are available without walking their owned backing. All
    // source scans and lookups are charged before the first observed operation.
    m.items(add(
        context.bindings.source_count(),
        context.payloads.source_count(),
    )?)?;
    m.gate(limits, admit)?;
    let mut total = 0;
    for source in sources {
        total = add(total, source.expression_ids.len())?;
        m.items(source.expression_ids.len())?;
        m.gate(limits, admit)?;
        work.step()?;
    }
    m.request::<w::ConnectorExpressionDefinition>(total, 1)?;
    m.request::<u32>(total, 1)?;
    m.request::<usize>(total, 1)?;
    m.work(prepare_work_upper_bound(total)?)?;
    m.work(mul(sources.len(), lookup_work_upper_bound(sources.len()))?)?;
    let mut matching_occurrences = true;
    for source in sources {
        let scan = source.read.scan();
        m.items(add(scan.assignments().len(), scan.dynamic_filters().len())?)?;
        m.request::<u32>(scan.recipe().columns().len(), 1)?;
        m.request::<w::StaticScanAssignment>(scan.assignments().len(), 1)?;
        m.request::<w::StaticScanDynamicFilter>(scan.dynamic_filters().len(), 1)?;
        for assignment in scan.assignments() {
            m.bytes(assignment.variable().len())?;
            m.gate(limits, admit)?;
            work.step()?;
        }
        for filter in scan.dynamic_filters() {
            m.bytes(filter.variable().len())?;
            m.gate(limits, admit)?;
            work.step()?;
        }
        count_tuple(scan.enforced_predicate(), &mut m, limits, admit, work)?;
        count_tuple(scan.unenforced_predicate(), &mut m, limits, admit, work)?;
        count_public_source(source.read.public_facts(), &mut m)?;
        m.gate(limits, admit)?;
        work.step()?;
        let count = match scan.remaining_expression() {
            Some(e) => count_expr(e, &mut m, 1, limits, admit, work)?,
            None => 0,
        };
        matching_occurrences &= count == source.expression_ids.len();
        m.work(mul(
            add(scan.recipe().columns().len(), 2)?,
            context.payloads.source_count(),
        )?)?;
        m.work(context.bindings.source_count())?;
    }
    m.gate(limits, admit)?;
    m.floor(layout::<ReadScanSource<'_>>(sources.len())?.size())?;
    m.gate(limits, admit)?;
    same_control(context.bindings.original_control(), work)?;
    same_control(context.payloads.original_control(), work)?;
    same_control(context.schemas.original_control(), work)?;
    m.floor(context.bindings.retained_floor_observed(work)?)?;
    m.floor(context.payloads.retained_floor_observed(work)?)?;
    m.floor(context.schemas.retained_invoice_floor()?)?;
    let _nodes = BindingIndex::prepare(sources.len(), |at| sources[at].node.get(), work)?;
    let mut ids = reserve(total, work)?;
    for source in sources {
        work.flush()?;
        ids.extend_from_slice(source.expression_ids);
        work.step()?;
        work.flush()?;
    }
    let _expressions = BindingIndex::prepare(ids.len(), |at| ids[at], work)?;
    observed(
        if matching_occurrences {
            Ok(())
        } else {
            Err(E::InvalidShape(
                "residual source has a different occurrence ID count",
            ))
        },
        work,
    )?;
    for source in sources {
        // Historical charged_bytes is not a necessary backing floor. The
        // caller lends the truthful union; only actual inline/layout floors
        // and the original namespace lower-floor authors are checked here.
        m.floor(std::mem::size_of::<c::FrozenConnectorRead>())?;
        let public = source.read.public_facts();
        let execution = required(
            public.execution_schema(),
            "read execution schema absent",
            work,
        )?;
        let source_facts = required(
            public.source_schema_facts(),
            "read source schema facts absent",
            work,
        )?;
        let schema = required(
            context.schemas.source_observed(source.schema_id, work)?,
            "read execution schema source absent",
            work,
        )?;
        observed(
            if std::ptr::eq(schema, execution.schema()) {
                Ok(())
            } else {
                Err(E::InvalidShape(
                    "read execution schema is not the original same-emission source",
                ))
            },
            work,
        )?;
        for (ordinal, expected) in execution.bindings().iter().enumerate() {
            let original = required(
                context
                    .schemas
                    .execution_binding_observed(source.schema_id, ordinal, work)?,
                "read execution field has no complete binding",
                work,
            )?;
            observed(
                if std::ptr::eq(original, expected) {
                    Ok(())
                } else {
                    Err(E::InvalidShape(
                        "read execution binding is not the original occurrence",
                    ))
                },
                work,
            )?;
        }
        let mut found = false;
        for entry in context.source_schemas {
            let same = entry.id == source.source_schema_id;
            work.step()?;
            if same {
                if found || !std::ptr::eq(entry.facts, source_facts) {
                    return Err(E::InvalidShape(
                        "read source schema is not the original same-emission source",
                    ));
                }
                let shape = required(
                    context
                        .schemas
                        .source_observed(entry.source_shape_schema_id, work)?,
                    "read source shape schema absent",
                    work,
                )?;
                observed(
                    if std::ptr::eq(shape, source_facts.shape().schema()) {
                        Ok(())
                    } else {
                        Err(E::InvalidShape(
                            "read source shape is not the original same-emission source",
                        ))
                    },
                    work,
                )?;
                found = true;
            }
        }
        observed(
            if found {
                Ok(())
            } else {
                Err(E::InvalidShape("read source facts namespace ID absent"))
            },
            work,
        )?;
    }
    let mut scans = reserve(sources.len(), work)?;
    let mut expressions = reserve(total, work)?;
    for source in sources {
        let scan = source.read.scan();
        let recipe = scan.recipe();
        let provider_binding_id = context
            .bindings
            .source_id_observed(recipe.binding(), work)?;
        let table_payload_id = context
            .payloads
            .source_id_observed(recipe.relation().table(), work)?;
        let view_payload_id = context
            .payloads
            .source_id_observed(recipe.relation().view(), work)?;
        let mut column_payload_ids = reserve(recipe.columns().len(), work)?;
        for column in recipe.columns() {
            column_payload_ids.push(context.payloads.source_id_observed(column, work)?);
            work.step()?;
        }
        let mut assignments = reserve(scan.assignments().len(), work)?;
        for assignment in scan.assignments() {
            assignments.push(w::StaticScanAssignment {
                variable: text(assignment.variable(), work)?,
                value_type: Some(codec::encode_value_type(assignment.value_type())),
            });
            work.step()?;
        }
        let mut dynamic_filters = reserve(scan.dynamic_filters().len(), work)?;
        for filter in scan.dynamic_filters() {
            dynamic_filters.push(w::StaticScanDynamicFilter {
                filter_id: Some(filter.filter_id()),
                variable: text(filter.variable(), work)?,
            });
            work.step()?;
        }
        let remaining_connector_expression_id = match scan.remaining_expression() {
            Some(e) => Some(emit_expr(
                e,
                source.expression_ids,
                &mut 0,
                &mut expressions,
                work,
            )?),
            None => None,
        };
        scans.push(w::FrozenReadScan {
            node_id: Some(source.node.get()),
            recipe: Some(w::ConnectorReadRecipe {
                provider_binding_id: Some(provider_binding_id),
                kind: relation_encode(recipe.relation().kind()),
                table_payload_id: Some(table_payload_id),
                view_payload_id: Some(view_payload_id),
                column_payload_ids,
            }),
            facts: Some(w::ConnectorScanFacts {
                assignments,
                enforced_predicate: Some(encode_tuple(
                    scan.enforced_predicate(),
                    &m,
                    limits,
                    admit,
                    work,
                )?),
                unenforced_predicate: Some(encode_tuple(
                    scan.unenforced_predicate(),
                    &m,
                    limits,
                    admit,
                    work,
                )?),
                remaining_connector_expression_id,
                dynamic_filters,
                max_batch_rows: scan.max_batch_rows().get(),
                max_batch_bytes: scan.max_batch_bytes().get(),
            }),
            public_facts: Some(encode_public(
                source.read.public_facts(),
                source.schema_id,
                source.source_schema_id,
                work,
            )?),
        });
        work.step()?;
    }
    m.gate(limits, admit)?;
    Ok((scans, expressions, m.facts(&Default::default(), limits)?))
}

fn merge_child_control(error: E) -> CompileControlError {
    match error {
        E::Control(c) => c,
        _ => unreachable!("the parent numerical facts author has no ordinary branch"),
    }
}
impl From<c::PureProviderCompileError<c::ReadScanOwnedError>> for E {
    fn from(e: c::PureProviderCompileError<c::ReadScanOwnedError>) -> Self {
        match e {
            c::PureProviderCompileError::Control(c) => Self::Control(c),
            c::PureProviderCompileError::Provider(c::ReadScanOwnedError::Contract(e)) => {
                Self::Scan(e)
            }
            c::PureProviderCompileError::Provider(c::ReadScanOwnedError::Resources(e)) => {
                Self::Provider(e)
            }
        }
    }
}
impl From<c::PureProviderCompileError<c::ReadRecipeOwnedError>> for E {
    fn from(e: c::PureProviderCompileError<c::ReadRecipeOwnedError>) -> Self {
        match e {
            c::PureProviderCompileError::Control(c) => Self::Control(c),
            c::PureProviderCompileError::Provider(c::ReadRecipeOwnedError::Contract(e)) => {
                Self::Recipe(e)
            }
            c::PureProviderCompileError::Provider(c::ReadRecipeOwnedError::Resources(e)) => {
                Self::Provider(e)
            }
        }
    }
}

/// One receiver scope, borrowed from the original namespaces. Residual graphs
/// expand into the existing ConnectorExpression owner; no provider is sealed.
pub fn decode_read_scans_observed(
    scans: &[w::FrozenReadScan],
    expressions: &[w::ConnectorExpressionDefinition],
    context: ReadScanDecodeContext<'_, '_, '_>,
    source_retained_bytes: usize,
    limits: ReadScanProjectionLimits,
    admit: &mut impl FnMut(&ReadScanProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DecodedReadScans, E> {
    decode_read_scans_core::<Infallible>(
        scans,
        expressions,
        context,
        source_retained_bytes,
        limits,
        admit,
        None,
        work,
    )
    .map_err(ProjectionFailure::without_host)
}

pub fn decode_read_scans_with_creator<H>(
    scans: &[w::FrozenReadScan],
    expressions: &[w::ConnectorExpressionDefinition],
    context: ReadScanDecodeContext<'_, '_, '_>,
    source_retained_bytes: usize,
    limits: ReadScanProjectionLimits,
    admit: &mut impl FnMut(&ReadScanProjectionFacts) -> Result<(), CompileControlError>,
    creation: &mut ValueSchemaCreation<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DecodedReadScans, ProjectionFailure<E, H>> {
    decode_read_scans_core(
        scans,
        expressions,
        context,
        source_retained_bytes,
        limits,
        admit,
        Some(creation),
        work,
    )
}

fn decode_read_scans_core<H>(
    scans: &[w::FrozenReadScan],
    expressions: &[w::ConnectorExpressionDefinition],
    context: ReadScanDecodeContext<'_, '_, '_>,
    source_retained_bytes: usize,
    limits: ReadScanProjectionLimits,
    admit: &mut impl FnMut(&ReadScanProjectionFacts) -> Result<(), CompileControlError>,
    mut creation: Option<&mut ValueSchemaCreation<'_, H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DecodedReadScans, ProjectionFailure<E, H>> {
    let mut m = Model::new::<(NodeId, c::FrozenConnectorRead)>(scans.len(), source_retained_bytes)?;
    m.items(add(
        expressions.len(),
        add(
            context.bindings.source_count(),
            context.payloads.source_count(),
        )?,
    )?)?;
    m.request::<usize>(expressions.len(), 1)?;
    m.request::<bool>(expressions.len(), 1)?;
    m.work(prepare_work_upper_bound(expressions.len())?)?;
    m.gate(limits, admit)?;
    for expression in expressions {
        if let Some(kind) = &expression.kind {
            count_raw_expression_header(kind, &mut m)?;
        }
        m.gate(limits, admit)?;
        work.step()?;
    }
    // Count every known original raw header before namespace/index callbacks.
    // Optional absence is diagnosed by the observed presence pass below.
    for scan in scans {
        if let Some(recipe) = &scan.recipe {
            m.items(recipe.column_payload_ids.len())?;
            m.request::<c::ConnectorEncodedPayload>(recipe.column_payload_ids.len(), 1)?;
            m.work(mul(
                add(recipe.column_payload_ids.len(), 2)?,
                lookup_work_upper_bound(context.payloads.source_count()),
            )?)?;
            m.work(lookup_work_upper_bound(context.bindings.source_count()))?;
        }
        if let Some(facts) = &scan.facts {
            m.items(add(facts.assignments.len(), facts.dynamic_filters.len())?)?;
            m.request::<c::StaticScanAssignment>(facts.assignments.len(), 1)?;
            m.request::<c::StaticScanDynamicFilter>(facts.dynamic_filters.len(), 1)?;
            for assignment in &facts.assignments {
                m.domain(codec::value_type_decode_resource_facts()?)?;
                m.bytes(assignment.variable.len())?;
                arc_request::<u8>(assignment.variable.len(), &mut m)?;
                m.gate(limits, admit)?;
                work.step()?;
            }
            for filter in &facts.dynamic_filters {
                m.bytes(filter.variable.len())?;
                arc_request::<u8>(filter.variable.len(), &mut m)?;
                m.gate(limits, admit)?;
                work.step()?;
            }
            if let Some(predicate) = &facts.enforced_predicate {
                count_raw_tuple(predicate, &mut m, limits, admit, work)?;
            }
            if let Some(predicate) = &facts.unenforced_predicate {
                count_raw_tuple(predicate, &mut m, limits, admit, work)?;
            }
        }
        if let Some(public) = &scan.public_facts {
            count_public_raw(public, &mut m)?;
        }
        m.gate(limits, admit)?;
        work.step()?;
    }
    m.gate(limits, admit)?;
    m.floor(
        layout::<w::FrozenReadScan>(scans.len())?
            .size()
            .max(layout::<w::ConnectorExpressionDefinition>(expressions.len())?.size()),
    )?;
    m.gate(limits, admit)?;
    same_control(context.bindings.original_control(), work)?;
    same_control(context.payloads.original_control(), work)?;
    same_control(context.schemas.original_control(), work)?;
    m.floor(context.bindings.retained_floor_observed(work)?)?;
    m.floor(context.payloads.retained_floor_observed(work)?)?;
    m.floor(context.schemas.retained_invoice_floor()?)?;
    // Presence is checked before the ID index is built; zero and MAX are real
    // IDs. The index is the sole duplicate/lookup author used by other codecs.
    for scan in scans {
        required(scan.node_id, "frozen read node ID absent", work)?;
    }
    let _scan_index = BindingIndex::prepare(
        scans.len(),
        |at| scans[at].node_id.expect("checked ID"),
        work,
    )?;
    let expression_index = BindingIndex::prepare(expressions.len(), |at| expressions[at].id, work)?;
    let mut used = reserve(expressions.len(), work)?;
    work.flush()?;
    used.resize(expressions.len(), false);
    work.step()?;
    work.flush()?;
    for scan in scans {
        required(scan.recipe.as_ref(), "read recipe absent", work)?;
        let facts = required(scan.facts.as_ref(), "scan facts absent", work)?;
        let public = required(scan.public_facts.as_ref(), "read public facts absent", work)?;
        require_public_schema_references(public, work)?;
        required(
            facts.enforced_predicate.as_ref(),
            "enforced scan predicate absent",
            work,
        )?;
        required(
            facts.unenforced_predicate.as_ref(),
            "unenforced scan predicate absent",
            work,
        )?;
        if let Some(root) = facts.remaining_connector_expression_id {
            let mut expanded = 0;
            preflight_expr(
                root,
                &mut expanded,
                expressions,
                &expression_index,
                &mut used,
                &mut [0; 65],
                1,
                &mut m,
                limits,
                admit,
                work,
            )?;
            m.work(mul(expanded, lookup_work_upper_bound(expressions.len()))?)?;
            m.gate(limits, admit)?;
        }
    }
    for referenced in used {
        observed(
            if referenced {
                Ok(())
            } else {
                Err(E::InvalidShape(
                    "connector residual definition is unreferenced",
                ))
            },
            work,
        )?;
    }
    let mut out = reserve(scans.len(), work)?;
    for scan in scans {
        let node = NodeId::new(scan.node_id.expect("preflight node"));
        let recipe = scan.recipe.as_ref().expect("preflight recipe");
        let facts = scan.facts.as_ref().expect("preflight facts");
        let public = scan.public_facts.as_ref().expect("preflight public");
        let binding = required(
            context.bindings.binding_observed(
                required(
                    recipe.provider_binding_id,
                    "read provider binding ID absent",
                    work,
                )?,
                work,
            )?,
            "read provider binding absent",
            work,
        )?;
        let table = required(
            context.payloads.payload_observed(
                required(
                    recipe.table_payload_id,
                    "read table payload ID absent",
                    work,
                )?,
                work,
            )?,
            "read table payload absent",
            work,
        )?;
        let view = required(
            context.payloads.payload_observed(
                required(recipe.view_payload_id, "read view payload ID absent", work)?,
                work,
            )?,
            "read view payload absent",
            work,
        )?;
        let kind = relation_decode(recipe.kind, work)?;
        let relation = c::ConnectorReadRelationPayload::new(kind, table.clone(), view.clone());
        work.step()?;
        let mut columns = reserve(recipe.column_payload_ids.len(), work)?;
        for id in &recipe.column_payload_ids {
            let column = required(
                context.payloads.payload_observed(*id, work)?,
                "read column payload absent",
                work,
            )?;
            columns.push(column.clone());
            work.step()?;
        }
        let mut last = c::WriterOwnedResourceFacts::default();
        let recipe = {
            let mut child_admit = |f: &c::WriterOwnedResourceFacts| {
                let complete = m.facts(f, limits).map_err(merge_child_control)?;
                admit(&complete)?;
                last = *f;
                Ok(())
            };
            c::ConnectorReadRelationRecipeDraft::try_new_observed(
                binding,
                relation,
                columns,
                source_retained_bytes,
                &mut child_admit,
                work,
            )
            .map_err(E::from)?
        };
        m.complete(last)?;
        let mut assignments = reserve(facts.assignments.len(), work)?;
        for assignment in &facts.assignments {
            let raw_type = required(
                assignment.value_type.as_ref(),
                "scan assignment type absent",
                work,
            )?;
            let ty = opaque(work, || {
                codec::decode_value_type(raw_type, FieldPath::root("scan_assignment"))
                    .map_err(E::from)
            })?;
            let name = opaque(work, || Ok(Arc::from(assignment.variable.as_str())))?;
            assignments.push(c::StaticScanAssignment::new(name, ty));
            work.step()?;
        }
        let enforced = decode_tuple(
            facts
                .enforced_predicate
                .as_ref()
                .expect("preflight enforced"),
            &m,
            limits,
            admit,
            work,
        )?;
        let unenforced = decode_tuple(
            facts
                .unenforced_predicate
                .as_ref()
                .expect("preflight unenforced"),
            &m,
            limits,
            admit,
            work,
        )?;
        let remaining = match facts.remaining_connector_expression_id {
            Some(id) => Some(decode_expr(id, expressions, &expression_index, work)?),
            None => None,
        };
        let mut filters = reserve(facts.dynamic_filters.len(), work)?;
        for filter in &facts.dynamic_filters {
            let id = required(filter.filter_id, "dynamic filter ID absent", work)?;
            let name = opaque(work, || Ok(Arc::from(filter.variable.as_str())))?;
            filters.push(c::StaticScanDynamicFilter::new(id, name));
            work.step()?;
        }
        let rows = required(
            NonZeroU64::new(facts.max_batch_rows),
            "scan max batch rows must be nonzero",
            work,
        )?;
        let bytes = required(
            NonZeroU64::new(facts.max_batch_bytes),
            "scan max batch bytes must be nonzero",
            work,
        )?;
        let mut last = c::WriterOwnedResourceFacts::default();
        let scan = {
            let mut child_admit = |f: &c::WriterOwnedResourceFacts| {
                let complete = m.facts(f, limits).map_err(merge_child_control)?;
                admit(&complete)?;
                last = *f;
                Ok(())
            };
            c::FrozenConnectorScan::try_new_observed(
                c::ScanConstructionInput {
                    recipe,
                    assignments,
                    enforced_predicate: enforced,
                    unenforced_predicate: unenforced,
                    remaining_expression: remaining,
                    dynamic_filters: filters,
                    max_batch_rows: rows,
                    max_batch_bytes: bytes,
                },
                source_retained_bytes,
                &mut child_admit,
                work,
            )
            .map_err(E::from)?
        };
        m.complete(last)?;
        let (source, kind) = decode_public_source(public, work)?;
        let mut last = c::WriterOwnedResourceFacts::default();
        let public = {
            // The immutable wire was checked by the one presence pass. Pure
            // and BE creator routes consume the same two explicit namespaces.
            let execution_id = public
                .execution_schema_id
                .expect("checked read execution schema ID");
            let source_id = public
                .source_schema_id
                .expect("checked read source schema facts ID");
            let source_namespace = required(
                context.source_schemas,
                "read source schema namespace absent",
                work,
            )?;
            let source_schema = required(
                source_namespace
                    .lookup_observed(source_id, work)
                    .map_err(E::from)?,
                "read source schema facts absent",
                work,
            )?;
            let schema_owner = required(
                context
                    .schemas
                    .materialized_schema_observed(execution_id, work)?,
                "read execution schema absent",
                work,
            )?;
            let schema = schema_owner.schema();
            if schema.metadata().as_arc().is_some() {
                return Err(E::InvalidShape(
                    "execution read schema carries format metadata backing",
                )
                .into());
            }
            let count = schema.fields().len();
            let bindings_layout = layout::<novarocks_type_contract::FunctionValueType>(count)?;
            request_read_layout(&mut creation, bindings_layout)?;
            let mut bindings = reserve(count, work)?;
            for ordinal in 0..count {
                let binding = required(
                    context
                        .schemas
                        .execution_binding_observed(execution_id, ordinal, work)?,
                    "read execution field has no complete binding",
                    work,
                )?;
                bindings.push(binding.clone());
                work.step()?;
            }
            if bindings_layout.size() != 0
                && let Some(creation) = creation.as_deref_mut()
            {
                creation
                    .observe_opaque_upper_bound(bindings_layout.size())
                    .map_err(ProjectionFailure::Host)?;
            }
            request_read_layout(
                &mut creation,
                novarocks_type_contract::owned_resources::layout::arc_layout(Layout::new::<
                    arrow::datatypes::Schema,
                >())
                .map_err(|_| E::Control(CompileControlError::ResourceExhausted))?,
            )?;
            let storage = if let Some(creation) = creation.as_deref_mut() {
                creation.storage_lifetime().retaining_observed(
                    &schema_owner.storage_lifetime(),
                    |layout| {
                        creation
                            .request_layout(layout)
                            .map_err(ProjectionFailure::Host)
                    },
                )?
            } else {
                schema_owner.storage_lifetime()
            };
            work.flush()?;
            let schema = Arc::new(arrow::datatypes::Schema::new(schema.fields().clone()));
            let execution = c::ConnectorReadExecutionSchema::try_new_observed(
                schema,
                bindings.into_boxed_slice(),
                storage,
                |layout| request_read_layout(&mut creation, layout),
                |units| -> Result<(), ProjectionFailure<E, H>> {
                    work.flush()?;
                    let mut remaining = units;
                    while remaining != 0 {
                        let quantum = remaining.min(256) as u32;
                        work.control().checkpoint(
                            novarocks_type_contract::CompilePhase::ProviderValidation,
                            quantum,
                        )?;
                        remaining -= quantum as usize;
                    }
                    Ok(())
                },
            )?;
            let mut child_admit = |f: &c::WriterOwnedResourceFacts| {
                let complete = m.facts(f, limits).map_err(merge_child_control)?;
                admit(&complete)?;
                last = *f;
                Ok(())
            };
            if let Some(creation) = creation.as_deref_mut() {
                c::ConnectorReadPublicFacts::try_new_typed_with_creator(
                    source,
                    kind,
                    &execution,
                    source_schema,
                    source_retained_bytes,
                    &mut child_admit,
                    creation,
                    work,
                )
                .map_err(|error| match error {
                    c::TypedReadCreationFailure::Creator(error) => ProjectionFailure::Host(error),
                    c::TypedReadCreationFailure::Contract(error) => {
                        ProjectionFailure::Codec(E::from(error))
                    }
                })?
            } else {
                c::ConnectorReadPublicFacts::try_new_typed_observed(
                    source,
                    kind,
                    &execution,
                    source_schema,
                    schema_owner.storage_lifetime(),
                    source_retained_bytes,
                    &mut child_admit,
                    work,
                )
                .map_err(E::from)?
            }
        };
        m.complete(last)?;
        let read = opaque(work, || {
            c::FrozenConnectorRead::try_new(scan, public).map_err(E::from)
        })?;
        out.push((node, read));
        work.step()?;
    }
    m.gate(limits, admit)?;
    Ok((out, m.facts(&Default::default(), limits)?))
}

#[cfg(test)]
mod tests;

macro_rules! read_projection_error {
    ($($error:ty),* $(,)?) => {$(
        impl<H> From<$error> for ProjectionFailure<ReadScanCodecError,H> {
            fn from(error: $error) -> Self { Self::Codec(E::from(error)) }
        }
    )*};
}
read_projection_error!(
    CompileControlError,
    SchemaCodecError,
    c::ConnectorError,
    BindingCodecError,
    ProviderBindingCodecError,
    ConnectorPayloadCodecError,
    codec::DomainCodecError,
    novarocks_type_contract::owned_resources::layout::LayoutResourceError
);

/// None is the explicit pure reconstruction API, not an allocation grant.
fn request_read_layout<H>(
    creation: &mut Option<&mut ValueSchemaCreation<'_, H>>,
    layout: Layout,
) -> Result<(), ProjectionFailure<E, H>> {
    if let Some(creation) = creation.as_deref_mut() {
        creation
            .request_layout(layout)
            .map_err(ProjectionFailure::Host)?;
    }
    Ok(())
}
