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

//! Numerical contributions of the original package Type encoder. The caller
//! owns the meter, source union invoice, actual grammar and allocation scope.
//! This model neither observes synthetic work nor grants host memory.

use super::{
    FieldRootSources, PackageTypeProjectionFacts, PackageTypeProjectionLimits, TypeCodecError,
    ValueRootSources,
};
use crate::{
    btree_resources_v2 as btree,
    resource_source_model::{LOCKED_FAMILY, LOCKED_TOOLCHAIN},
};
use arrow::datatypes::{DataType, Field};
use novarocks_proto_models::{physical_type_v2 as wire, plan};
use novarocks_type_contract::{
    CompileControlError, MAX_ARROW_FIELD_METADATA_BYTES, owned_resources::hashmap,
};
use std::{alloc::Layout, sync::Arc};

type E = TypeCodecError;
fn add(a: usize, b: usize) -> Result<usize, CompileControlError> {
    a.checked_add(b)
        .ok_or(CompileControlError::ResourceExhausted)
}
fn mul(a: usize, b: usize) -> Result<usize, CompileControlError> {
    a.checked_mul(b)
        .ok_or(CompileControlError::ResourceExhausted)
}
fn array<T>(n: usize) -> Result<Layout, E> {
    Layout::array::<T>(n).map_err(|_| CompileControlError::ResourceExhausted.into())
}
fn tree_error(error: btree::BTreeResourceError) -> E {
    match error {
        btree::BTreeResourceError::Arithmetic(_) => CompileControlError::ResourceExhausted.into(),
        btree::BTreeResourceError::SourceModel(message) => E::ResourceSource(message),
    }
}
fn hash_error(error: hashmap::HashMapResourceError) -> E {
    match error {
        hashmap::HashMapResourceError::Arithmetic(_) => {
            CompileControlError::ResourceExhausted.into()
        }
        hashmap::HashMapResourceError::SourceModel(message) => E::ResourceSource(message),
    }
}

enum RootStorage<'source> {
    OwnedCounts {
        values: usize,
        fields: usize,
    },
    Sources {
        values: ValueRootSources<'source>,
        fields: FieldRootSources<'source>,
    },
}
impl RootStorage<'_> {
    fn counts(&self) -> (usize, usize) {
        match self {
            Self::OwnedCounts { values, fields } => (*values, *fields),
            Self::Sources { values, fields } => (values.len(), fields.len()),
        }
    }
    fn payload_floor(&self) -> Result<usize, E> {
        match self {
            Self::OwnedCounts { values, fields } => Ok(array::<(
                u32,
                novarocks_type_contract::FunctionValueType,
            )>(*values)?
            .size()
            .max(array::<(u32, Arc<Field>)>(*fields)?.size())),
            Self::Sources { values, fields } => Ok(values
                .payload_layout()?
                .size()
                .max(fields.payload_layout()?.size())),
        }
    }
}

pub(super) struct Model {
    source: usize,
    definitions: usize,
    expanded: usize,
    strings: usize,
    requests: usize,
    bytes: usize,
    work: usize,
    /// Fixed per-node bound of one strict validator visit pair (the Field
    /// leading to a node and the node's carrier), derived from the source.
    strict_node_work: usize,
}
impl Model {
    pub(super) fn request(
        &mut self,
        layout: Layout,
        count: usize,
    ) -> Result<(), CompileControlError> {
        if layout.size() == 0 || count == 0 {
            return Ok(());
        }
        let bytes = mul(layout.size(), count)?;
        self.requests = add(self.requests, count)?;
        self.bytes = add(self.bytes, bytes)?;
        // Initialization/copy/movement and abort cleanup of real requested
        // buffers. Opaque library internals do not acquire callbacks here.
        self.work = add(self.work, add(mul(bytes, 4)?, mul(count, 128)?)?)?;
        Ok(())
    }
    fn tree(&mut self, count: usize) -> Result<(), E> {
        let facts = btree::insertion_only::<u32, ()>(count).map_err(tree_error)?;
        self.requests = add(self.requests, facts.allocation_requests_upper_bound)?;
        self.bytes = add(self.bytes, facts.request_bytes_upper_bound)?;
        self.work = add(
            self.work,
            add(
                facts.cumulative_work_upper_bound,
                mul(facts.request_bytes_upper_bound, 4)?,
            )?,
        )?;
        Ok(())
    }
    /// Before the first source walk: the two actual ID sets and count/emission
    /// bookkeeping are admitted. Each strict validator's heap stack and walk
    /// are admitted later by `strict_nodes`, from that root's actual type.
    /// Writer sources must be the caller's immutable checked Drafts; root
    /// counts confer no Writer law or source proof of their own.
    pub(super) fn new(
        source: usize,
        value_roots: usize,
        strict_field_roots: usize,
        writer_sources: usize,
        writer_fields: usize,
        limits: PackageTypeProjectionLimits,
    ) -> Result<Self, E> {
        Self::new_core(
            source,
            RootStorage::OwnedCounts {
                values: value_roots,
                fields: strict_field_roots,
            },
            writer_sources,
            writer_fields,
            limits,
        )
    }
    /// Borrowed tuple storage changes only the necessary source floor. The
    /// original source trees, traversal and output request model are retained.
    pub(super) fn new_sources(
        source: usize,
        values: ValueRootSources<'_>,
        fields: FieldRootSources<'_>,
        writer_sources: usize,
        writer_fields: usize,
        limits: PackageTypeProjectionLimits,
    ) -> Result<Self, E> {
        match (values, fields) {
            (ValueRootSources::Owned(values), FieldRootSources::Owned(fields)) => Self::new(
                source,
                values.len(),
                fields.len(),
                writer_sources,
                writer_fields,
                limits,
            ),
            _ => Self::new_core(
                source,
                RootStorage::Sources { values, fields },
                writer_sources,
                writer_fields,
                limits,
            ),
        }
    }
    fn new_core(
        source: usize,
        roots: RootStorage<'_>,
        writer_sources: usize,
        writer_fields: usize,
        limits: PackageTypeProjectionLimits,
    ) -> Result<Self, E> {
        if !LOCKED_TOOLCHAIN || !LOCKED_FAMILY {
            return Err(E::ResourceSource("type encoder library source model drift"));
        }
        // Borrowed root slices may alias/share allocation, so their necessary
        // payload floors are combined by max, never summed into a fake union.
        let (value_roots, strict_field_roots) = roots.counts();
        let floor = roots
            .payload_floor()?
            .max(array::<super::WriterTypeSource<'_>>(writer_sources)?.size())
            .max(array::<u32>(writer_fields)?.size());
        if source < floor {
            return Err(CompileControlError::ResourceExhausted.into());
        }
        let field_roots = add(strict_field_roots, writer_fields)?;
        let roots = add(value_roots, field_roots)?;
        if roots > limits.max_definitions {
            return Err(CompileControlError::ResourceExhausted.into());
        }
        // Strict walks inspect actual carrier/Field events below. Header and
        // stack work is known here; metadata-free nodes do not probe an
        // imagined source annotation map or scan the caller's whole invoice.
        let mut model = Self {
            source,
            definitions: 0,
            expanded: 0,
            strings: 0,
            requests: 0,
            bytes: 0,
            work: 256,
            strict_node_work: 1024,
        };
        model.tree(value_roots)?;
        model.tree(field_roots)?;
        // Existing count expansion terminates at E. Every emitted C/F/V has
        // at least one expanded node. FieldIds' monotone cursor tests at most
        // root Field IDs plus automatic Field occurrences, never max(ID).
        let lookup = btree::lookup_work_typed(add(field_roots, limits.max_expanded_nodes)?)
            .map_err(tree_error)?;
        model.work = add(
            model.work,
            mul(limits.max_expanded_nodes, add(512, mul(lookup, 8)?)?)?,
        )?;
        model.work = add(
            model.work,
            add(
                mul(roots, add(256, mul(lookup, 4)?)?)?,
                mul(writer_sources, 256)?,
            )?,
        )?;
        // One terminal parameter diagnostic uses bounded decimal/enum text;
        // locked String growth <=128 requests and <=4*128 payload bytes.
        model.requests = add(model.requests, 128)?;
        model.bytes = add(model.bytes, 512)?;
        model.work = add(model.work, 4 * 512 + 128 * 128)?;
        model.facts(limits)?;
        Ok(model)
    }
    /// `count` actual nodes of one strict root's original validator walk,
    /// admitted before that walk reaches them: the root before the walk
    /// starts, and each carrier's direct children when the carrier is
    /// visited, before their Field visits and pushes. The walker's heap stack
    /// is a Rust1.92 Vec holding the original singleton that grows min-four
    /// and doubling; by the time it grows to 2C it has pushed at least C
    /// children, each already admitted, so N admitted nodes cover <=N
    /// requests and <=4N cumulative tuples. This is the receiver's per-root
    /// summary bound, never the 4096-node per-type maximum.
    pub(super) fn strict_nodes(&mut self, count: usize) -> Result<(), E> {
        self.request(array::<(&DataType, usize)>(4)?, count)?;
        self.work = add(self.work, mul(count, self.strict_node_work)?)?;
        Ok(())
    }
    /// Work of the actual strict Field visit, admitted before its iterator.
    /// Metadata without Arc backing is constant. Shared source metadata keeps
    /// the existing conservative source envelope, never reused for execution-only nodes.
    pub(super) fn strict_field(&mut self, field: &Field) -> Result<(), E> {
        self.work = add(self.work, add(128, mul(field.name().len(), 8)?)?)?;
        if field.metadata().as_arc().is_some() {
            let iteration =
                hashmap::source_iterator_work_upper_bound(self.source, field.metadata().len())
                    .map_err(hash_error)?;
            self.work = add(
                self.work,
                add(iteration, mul(MAX_ARROW_FIELD_METADATA_BYTES, 8)?)?,
            )?;
        } else {
            self.work = add(self.work, 1)?;
        }
        Ok(())
    }
    pub(super) fn strict_carrier(&mut self, ty: &DataType) -> Result<(), E> {
        if let DataType::Timestamp(_, Some(zone)) = ty {
            self.work = add(self.work, mul(zone.len(), 8)?)?;
        }
        Ok(())
    }
    /// Header-only numerical contribution, before any child traversal.
    pub(super) fn carrier(&mut self, ty: &DataType) -> Result<(), E> {
        match ty {
            DataType::Struct(fields) => self.request(array::<u32>(fields.len())?, 1)?,
            DataType::Union(fields, _) => {
                self.request(array::<wire::UnionField>(fields.len())?, 1)?
            }
            DataType::Timestamp(_, Some(zone)) => self.string(zone.len())?,
            _ => {}
        }
        Ok(())
    }
    /// No metadata entry is read here. Admit both real insertion-sort and DTO
    /// buffers, source deleted-bucket iteration, and the worst-case byte
    /// comparisons before the first metadata iterator operation.
    pub(super) fn field(&mut self, field: &Field) -> Result<(), E> {
        let entries = field.metadata().len();
        // Only occupied String handles plus this actual owned Field/name.
        // Deleted buckets and private table capacity remain the caller's B.
        let floor = add(
            add(std::mem::size_of::<Field>(), field.name().len())?,
            array::<(String, String)>(entries)?.size(),
        )?;
        if self.source < floor {
            return Err(CompileControlError::ResourceExhausted.into());
        }
        self.string(field.name().len())?;
        self.request(array::<(&str, &str)>(entries)?, 1)?;
        self.request(array::<plan::ArrowFieldMetadataEntry>(entries)?, 1)?;
        // Canonical empty Metadata has no Arc backing or entries to visit.
        // A retained source Arc keeps the existing conservative provenance bound.
        if field.metadata().as_arc().is_none() {
            self.work = add(self.work, 2)?;
        } else {
            // The original Count walk and encode_metadata each iterate the
            // same source map once. Strict pre-validation is admitted in new().
            self.work = add(
                self.work,
                mul(
                    hashmap::source_iterator_work_upper_bound(self.source, entries)
                        .map_err(hash_error)?,
                    2,
                )?,
            )?;
            let pairs = mul(entries, entries.saturating_sub(1))? / 2;
            // At most B equal-prefix bytes, one length/order comparison and one
            // swap per pair. B is the truthful union, not len()/capacity().
            self.work = add(self.work, mul(pairs, add(self.source, 2)?)?)?;
        }
        self.work = add(self.work, mul(entries, 128)?)?;
        Ok(())
    }
    /// Exactly one actual emitted String request. Metadata callers use this
    /// once for key and once for value; name and zone are already included.
    pub(super) fn string(&mut self, length: usize) -> Result<(), E> {
        if length > self.source {
            return Err(CompileControlError::ResourceExhausted.into());
        }
        self.strings = add(self.strings, length)?;
        self.request(array::<u8>(length)?, 1)?;
        self.work = add(self.work, add(64, mul(length, 8)?)?)?;
        Ok(())
    }
    pub(super) fn definition(&mut self, count: usize) -> Result<(), CompileControlError> {
        self.definitions = add(self.definitions, count)?;
        Ok(())
    }
    pub(super) fn expansion(&mut self, count: usize) -> Result<(), CompileControlError> {
        self.expanded = add(self.expanded, count)?;
        Ok(())
    }
    /// The actual three DTO Vec requests, after complete count preflight and
    /// before the first output allocation. Nested buffers were counted above.
    pub(super) fn output(
        &mut self,
        carriers: usize,
        fields: usize,
        values: usize,
    ) -> Result<(), E> {
        self.request(array::<wire::CarrierTypeDefinition>(carriers)?, 1)?;
        self.request(array::<wire::FieldDefinition>(fields)?, 1)?;
        self.request(array::<wire::ValueTypeDefinition>(values)?, 1)?;
        Ok(())
    }
    pub(super) fn source_output(&mut self, fields: usize) -> Result<(), E> {
        self.request(array::<wire::SourceArrowFieldFacts>(fields)?, 1)?;
        Ok(())
    }
    pub(super) fn additional_value_roots(&mut self, roots: usize) -> Result<(), E> {
        self.tree(roots)?;
        self.work = add(self.work, mul(roots, 256)?)?;
        Ok(())
    }
    pub(super) fn associated_field_work(
        &mut self,
        actual_profile_work: usize,
    ) -> Result<(), CompileControlError> {
        // A second borrowed exact layout walk of this actual semantic tree.
        // Its nodes, role wrappers, names and parameters fit the shared per-node
        // work profile; it does not allocate metadata fingerprint buckets.
        self.work = add(self.work, actual_profile_work)?;
        Ok(())
    }
    pub(super) fn semantic_profile(
        &mut self,
        profile: &super::semantic::Profile,
    ) -> Result<(), CompileControlError> {
        self.definitions = add(self.definitions, profile.definitions)?;
        self.expanded = add(self.expanded, profile.expanded)?;
        self.strings = add(self.strings, profile.strings)?;
        self.work = add(self.work, profile.work)?;
        Ok(())
    }
    pub(super) fn facts(
        &self,
        limits: PackageTypeProjectionLimits,
    ) -> Result<PackageTypeProjectionFacts, CompileControlError> {
        let facts = PackageTypeProjectionFacts {
            definition_count: self.definitions,
            expanded_node_count: self.expanded,
            string_bytes: self.strings,
            allocation_requests_upper_bound: self.requests,
            allocation_request_bytes_upper_bound: self.bytes,
            coexisting_source_and_request_bytes_upper_bound: add(self.source, self.bytes)?,
            cumulative_work_upper_bound: self.work,
        };
        if facts.definition_count > limits.max_definitions
            || facts.expanded_node_count > limits.max_expanded_nodes
            || facts.string_bytes > limits.max_string_bytes
            || facts.allocation_requests_upper_bound > limits.max_allocation_requests
            || facts.allocation_request_bytes_upper_bound > limits.max_allocation_request_bytes
            || facts.coexisting_source_and_request_bytes_upper_bound
                > limits.max_coexisting_source_and_request_bytes
            || facts.cumulative_work_upper_bound > limits.max_work
        {
            return Err(CompileControlError::ResourceExhausted);
        }
        Ok(facts)
    }
    #[cfg(test)]
    pub(super) fn gate(
        &self,
        limits: PackageTypeProjectionLimits,
        admit: &mut (
                 impl FnMut(&PackageTypeProjectionFacts) -> Result<(), CompileControlError> + ?Sized
             ),
    ) -> Result<(), E> {
        self.gate_with_host(limits, &mut |facts| {
            admit(facts).map_err(
                crate::host_projection_v2::AdmissionRefusal::<std::convert::Infallible>::Control,
            )
        })
        .map_err(crate::host_projection_v2::ProjectionFailure::without_host)
    }
    pub(super) fn gate_with_host<H>(
        &self,
        limits: PackageTypeProjectionLimits,
        admit: &mut (
                 impl FnMut(
            &PackageTypeProjectionFacts,
        ) -> Result<(), crate::host_projection_v2::AdmissionRefusal<H>>
                 + ?Sized
             ),
    ) -> Result<(), crate::host_projection_v2::ProjectionFailure<E, H>> {
        let facts = self.facts(limits)?;
        match admit(&facts) {
            Ok(()) => Ok(()),
            Err(crate::host_projection_v2::AdmissionRefusal::Control(cause)) => Err(cause.into()),
            Err(crate::host_projection_v2::AdmissionRefusal::Host(error)) => {
                Err(crate::host_projection_v2::ProjectionFailure::Host(error))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_type_contract::{
        CompileCheckpoints, CompilePhase, MAX_VALUE_TYPE_NODES, PureCompileControl,
    };
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };

    fn limits() -> PackageTypeProjectionLimits {
        PackageTypeProjectionLimits {
            max_definitions: 100_000,
            max_expanded_nodes: 1_000_000,
            max_string_bytes: usize::MAX,
            max_allocation_requests: usize::MAX,
            max_allocation_request_bytes: usize::MAX,
            max_coexisting_source_and_request_bytes: usize::MAX,
            max_work: usize::MAX,
        }
    }
    fn empty() -> Model {
        Model::new(65_536, 0, 0, 0, 0, limits()).unwrap()
    }
    fn facts(model: &Model) -> PackageTypeProjectionFacts {
        model.facts(limits()).unwrap()
    }
    fn tree_bytes() -> usize {
        // Independent Rust1.92 B=6 layout bound, including private-order
        // padding. This is a request upper, not retained Set capacity.
        let pointer = std::mem::size_of::<usize>();
        let align = std::mem::align_of::<usize>().max(std::mem::align_of::<u32>());
        let raw = pointer + 4 + 11 * 4 + 5 * (align - 1) + 12 * pointer + align - 1;
        raw.div_ceil(align) * align
    }
    #[test]
    fn initial_id_sets_and_strict_validator_nodes_have_independent_request_layouts() {
        let model = Model::new(4096, 2, 3, 2, 4, limits()).unwrap();
        let actual = facts(&model);
        assert_eq!(actual.definition_count, 0);
        assert_eq!(actual.expanded_node_count, 0);
        // Root counts alone admit only the two ID sets and the diagnostic;
        // no strict validator stack is charged before its actual type.
        assert_eq!(actual.allocation_requests_upper_bound, 2 + 7 + 128);
        assert_eq!(
            actual.allocation_request_bytes_upper_bound,
            9 * tree_bytes() + 512
        );
        assert_eq!(
            actual.coexisting_source_and_request_bytes_upper_bound,
            4096 + actual.allocation_request_bytes_upper_bound
        );
        // Writer-only roots request no strict Value-validator heap stack.
        let writer = facts(&Model::new(4096, 0, 0, 2, 7, limits()).unwrap());
        assert_eq!(writer.allocation_requests_upper_bound, 7 + 128);
        assert_eq!(
            writer.allocation_request_bytes_upper_bound,
            7 * tree_bytes() + 512
        );
        // Each admitted strict node is one stack request of four tuples.
        let mut strict = Model::new(4096, 2, 3, 2, 4, limits()).unwrap();
        strict.strict_nodes(5).unwrap();
        let nodes = facts(&strict);
        let tuple = std::mem::size_of::<(&DataType, usize)>();
        assert_eq!(
            nodes.allocation_requests_upper_bound - actual.allocation_requests_upper_bound,
            5
        );
        assert_eq!(
            nodes.allocation_request_bytes_upper_bound
                - actual.allocation_request_bytes_upper_bound,
            5 * 4 * tuple
        );
        assert_eq!(nodes.definition_count, 0);
        assert_eq!(nodes.expanded_node_count, 0);
        assert_eq!(nodes.string_bytes, 0);
    }
    /// Rust1.92 `vec![x]` then push: capacities 1, 4, 8, ... Replaying the
    /// real walker Vec proves the per-node charge covers every growth request
    /// and the cumulative requested tuples at the moment each growth happens.
    #[test]
    fn strict_node_charge_covers_the_real_validator_vec_growth() {
        let tuple = std::mem::size_of::<(&DataType, usize)>();
        let ty = DataType::Int64;
        let mut stack = vec![(&ty, 1usize)];
        let (mut requests, mut tuples) = (1usize, stack.capacity());
        for pushes in 1..=MAX_VALUE_TYPE_NODES - 1 {
            let before = stack.capacity();
            stack.push((&ty, pushes));
            if stack.capacity() != before {
                requests += 1;
                tuples += stack.capacity();
            }
            // Admitted before this push: the root plus every pushed child.
            let mut model = empty();
            let base = facts(&model);
            model.strict_nodes(pushes + 1).unwrap();
            let admitted = facts(&model);
            assert!(
                admitted.allocation_requests_upper_bound - base.allocation_requests_upper_bound
                    >= requests
            );
            assert!(
                admitted.allocation_request_bytes_upper_bound
                    - base.allocation_request_bytes_upper_bound
                    >= tuples * tuple
            );
        }
    }
    /// Stack geometry remains the original per-node upper. Metadata-free
    /// node headers do not perform source-map work; actual source Field visits
    /// still carry their original iterator bound.
    #[test]
    fn strict_nodes_keep_original_stack_upper_and_only_actual_source_maps_scale_with_invoice() {
        let tuple = std::mem::size_of::<(&DataType, usize)>();
        let mut low = Model::new(1024, 1, 0, 0, 0, limits()).unwrap();
        let mut high = Model::new(4096, 1, 0, 0, 0, limits()).unwrap();
        let (low_base, high_base) = (facts(&low), facts(&high));
        low.strict_nodes(MAX_VALUE_TYPE_NODES).unwrap();
        high.strict_nodes(MAX_VALUE_TYPE_NODES).unwrap();
        let low_nodes = facts(&low);
        assert_eq!(
            low_nodes.allocation_requests_upper_bound - low_base.allocation_requests_upper_bound,
            MAX_VALUE_TYPE_NODES
        );
        assert_eq!(
            low_nodes.allocation_request_bytes_upper_bound
                - low_base.allocation_request_bytes_upper_bound,
            4 * MAX_VALUE_TYPE_NODES * tuple
        );
        let low_work = low_nodes.cumulative_work_upper_bound - low_base.cumulative_work_upper_bound;
        let high_work =
            facts(&high).cumulative_work_upper_bound - high_base.cumulative_work_upper_bound;
        assert_eq!(high_work, low_work);
        let field = Field::new("source", DataType::Int64, false)
            .with_metadata(HashMap::from([("external".into(), "attribute".into())]));
        let (low_before, high_before) = (
            facts(&low).cumulative_work_upper_bound,
            facts(&high).cumulative_work_upper_bound,
        );
        low.strict_field(&field).unwrap();
        high.strict_field(&field).unwrap();
        assert!(
            facts(&high).cumulative_work_upper_bound - high_before
                > facts(&low).cumulative_work_upper_bound - low_before
        );
    }
    #[test]
    fn actual_carrier_field_metadata_and_three_dto_vecs_have_independent_golden() {
        let mut model = empty();
        let base = facts(&model);
        let fields = vec![
            Arc::new(Field::new("a", DataType::Int32, false)),
            Arc::new(Field::new("b", DataType::Int64, true)),
        ];
        model.carrier(&DataType::Struct(fields.into())).unwrap();
        let union = arrow::datatypes::UnionFields::try_new(
            [0, 7],
            [
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Int64, true),
            ],
        )
        .unwrap();
        model
            .carrier(&DataType::Union(union, arrow::datatypes::UnionMode::Dense))
            .unwrap();
        model
            .carrier(&DataType::Timestamp(
                arrow::datatypes::TimeUnit::Nanosecond,
                Some(Arc::from("中国/é")),
            ))
            .unwrap();
        let field = Field::new("名\0", DataType::Int64, true).with_metadata(HashMap::from([
            ("é".into(), "中国\0".into()),
            ("z".into(), "".into()),
        ]));
        model.field(&field).unwrap();
        for (key, value) in field.metadata() {
            model.string(key.len()).unwrap();
            model.string(value.len()).unwrap();
        }
        model.definition(6).unwrap();
        model.expansion(9).unwrap();
        model.output(3, 2, 1).unwrap();
        let actual = facts(&model);
        let strings = "中国/é".len() + "名\0".len() + "é".len() + "中国\0".len() + 1;
        let payload = 2 * std::mem::size_of::<u32>()
            + 2 * std::mem::size_of::<wire::UnionField>()
            + 2 * std::mem::size_of::<(&str, &str)>()
            + 2 * std::mem::size_of::<plan::ArrowFieldMetadataEntry>()
            + strings
            + 3 * std::mem::size_of::<wire::CarrierTypeDefinition>()
            + 2 * std::mem::size_of::<wire::FieldDefinition>()
            + std::mem::size_of::<wire::ValueTypeDefinition>();
        assert_eq!(actual.definition_count, 6);
        assert_eq!(actual.expanded_node_count, 9);
        assert_eq!(actual.string_bytes, strings);
        assert_eq!(
            actual.allocation_requests_upper_bound - base.allocation_requests_upper_bound,
            12
        );
        assert_eq!(
            actual.allocation_request_bytes_upper_bound - base.allocation_request_bytes_upper_bound,
            payload
        );
        assert_eq!(
            actual.coexisting_source_and_request_bytes_upper_bound,
            65_536 + actual.allocation_request_bytes_upper_bound
        );
    }
    #[test]
    fn empty_payloads_have_no_nonzero_request_and_counts_never_reset_early_work() {
        let mut model = empty();
        let baseline = facts(&model);
        model
            .carrier(&DataType::Struct(Vec::<Arc<Field>>::new().into()))
            .unwrap();
        model
            .field(&Field::new("", DataType::Int32, false))
            .unwrap();
        model.string(0).unwrap();
        model.output(0, 0, 0).unwrap();
        let actual = facts(&model);
        assert_eq!(
            actual.allocation_requests_upper_bound,
            baseline.allocation_requests_upper_bound
        );
        assert_eq!(
            actual.allocation_request_bytes_upper_bound,
            baseline.allocation_request_bytes_upper_bound
        );
        assert_eq!(actual.string_bytes, 0);
        assert!(actual.cumulative_work_upper_bound >= baseline.cumulative_work_upper_bound);
        model.definition(2).unwrap();
        model.expansion(3).unwrap();
        assert_eq!(
            facts(&model).cumulative_work_upper_bound,
            actual.cumulative_work_upper_bound
        );
    }
    #[test]
    fn all_seven_known_axes_refuse_before_caller_admission_callback_and_replay_exactly() {
        let mut model = Model::new(128, 0, 0, 0, 1, limits()).unwrap();
        model.definition(2).unwrap();
        model.expansion(3).unwrap();
        model.string(7).unwrap();
        model.output(1, 1, 0).unwrap();
        let expected = facts(&model);
        let exact = PackageTypeProjectionLimits {
            max_definitions: expected.definition_count,
            max_expanded_nodes: expected.expanded_node_count,
            max_string_bytes: expected.string_bytes,
            max_allocation_requests: expected.allocation_requests_upper_bound,
            max_allocation_request_bytes: expected.allocation_request_bytes_upper_bound,
            max_coexisting_source_and_request_bytes: expected
                .coexisting_source_and_request_bytes_upper_bound,
            max_work: expected.cumulative_work_upper_bound,
        };
        let mut calls = 0;
        model
            .gate(exact, &mut |f| {
                calls += 1;
                assert_eq!(*f, expected);
                Ok(())
            })
            .unwrap();
        assert_eq!(calls, 1);
        for axis in 0..7 {
            let mut under = exact;
            match axis {
                0 => under.max_definitions -= 1,
                1 => under.max_expanded_nodes -= 1,
                2 => under.max_string_bytes -= 1,
                3 => under.max_allocation_requests -= 1,
                4 => under.max_allocation_request_bytes -= 1,
                5 => under.max_coexisting_source_and_request_bytes -= 1,
                _ => under.max_work -= 1,
            }
            assert!(matches!(
                model.gate(under, &mut |_| {
                    calls += 1;
                    Ok(())
                }),
                Err(E::Control(CompileControlError::ResourceExhausted))
            ));
        }
        assert_eq!(calls, 1);
    }
    struct Late {
        cause: CompileControlError,
        trace: Mutex<Vec<u32>>,
    }
    impl PureCompileControl for Late {
        fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            let mut trace = self.trace.lock().unwrap();
            trace.push(units);
            if trace.len() == 2 {
                Err(self.cause.clone())
            } else {
                Ok(())
            }
        }
    }
    #[test]
    fn known_numeric_refusal_wins_real_pending255_before_every_late_primary_control() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Late {
                cause,
                trace: Mutex::new(Vec::new()),
            };
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
            // Real caller work, before this pure numerical component. This
            // seam is not a complete source encoder/path assertion.
            let mut destination = [0u8; 255];
            for (i, byte) in destination.iter_mut().enumerate() {
                *byte = i as u8;
                work.step().unwrap();
            }
            assert_eq!(destination[254], 254);
            let mut bound = limits();
            bound.max_allocation_requests = 127;
            assert!(matches!(
                Model::new(0, 0, 0, 0, 0, bound),
                Err(E::Control(CompileControlError::ResourceExhausted))
            ));
            assert_eq!(*control.trace.lock().unwrap(), vec![0]);
        }
    }
    #[test]
    fn source_deleted_bucket_and_equal_prefix_work_is_admitted_without_metadata_scan() {
        let field = Field::new("", DataType::Int32, false).with_metadata(HashMap::from([
            ("same-prefix-a".into(), "".into()),
            ("same-prefix-b".into(), "".into()),
            ("same-prefix-c".into(), "".into()),
        ]));
        let mut low = Model::new(1024, 0, 0, 0, 0, limits()).unwrap();
        let mut high = Model::new(4096, 0, 0, 0, 0, limits()).unwrap();
        low.field(&field).unwrap();
        high.field(&field).unwrap();
        // Three insertion-sort pairs + two locked iterators'32 units per source
        // byte. Input/source invoice is not a map private-capacity estimate.
        assert_eq!(
            facts(&high).cumulative_work_upper_bound - facts(&low).cumulative_work_upper_bound,
            (4096 - 1024) * (32 * 2 + 3)
        );
        assert_eq!(
            facts(&high).allocation_request_bytes_upper_bound,
            facts(&low).allocation_request_bytes_upper_bound
        );
        assert_eq!(facts(&high).string_bytes, 0);
    }
    #[test]
    fn checked_products_and_known_root_counts_fail_as_numeric_controls() {
        let mut small = limits();
        small.max_definitions = 1;
        assert!(matches!(
            Model::new(0, 1, 0, 0, 1, small),
            Err(E::Control(CompileControlError::ResourceExhausted))
        ));
        assert!(matches!(
            Model::new(0, usize::MAX, 1, 0, 0, limits()),
            Err(E::Control(CompileControlError::ResourceExhausted))
        ));
        let mut model = empty();
        assert!(matches!(
            model.string(usize::MAX),
            Err(E::Control(CompileControlError::ResourceExhausted))
        ));
    }
    #[test]
    fn borrowed_root_tuple_floor_preserves_owned_request_and_work_model() {
        let values = [
            (
                0,
                novarocks_type_contract::FunctionValueType::new(DataType::Int32, false),
            ),
            (
                u32::MAX,
                novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),
            ),
        ];
        let field = Arc::new(Field::new("f", DataType::Int32, false));
        let fields = [(0, field.clone()), (u32::MAX, field)];
        let borrowed_values = [(0, &values[0].1), (u32::MAX, &values[1].1)];
        let borrowed_fields = [(0, &fields[0].1), (u32::MAX, &fields[1].1)];
        let value_sources = ValueRootSources::Borrowed(&borrowed_values);
        let field_sources = FieldRootSources::Borrowed(&borrowed_fields);
        // This is only the necessary root-slice payload floor. It does not
        // certify the complete invoice of the original reachable objects.
        let floor = (2 * std::mem::size_of::<(u32, &novarocks_type_contract::FunctionValueType)>())
            .max(2 * std::mem::size_of::<(u32, &Arc<Field>)>());
        assert!(Model::new_sources(floor, value_sources, field_sources, 0, 0, limits()).is_ok());
        assert!(matches!(
            Model::new_sources(floor - 1, value_sources, field_sources, 0, 0, limits()),
            Err(E::Control(CompileControlError::ResourceExhausted))
        ));
        let source = 65_536;
        let original = facts(&Model::new(source, 2, 2, 0, 0, limits()).unwrap());
        let owned = Model::new_sources(
            source,
            ValueRootSources::Owned(&values),
            FieldRootSources::Owned(&fields),
            0,
            0,
            limits(),
        )
        .unwrap();
        let borrowed =
            Model::new_sources(source, value_sources, field_sources, 0, 0, limits()).unwrap();
        assert_eq!(facts(&owned), original);
        assert_eq!(facts(&borrowed), original);
    }
    #[test]
    fn necessary_source_floors_use_shared_root_max_and_real_occupied_field_handles() {
        let root_floor = std::mem::size_of::<(u32, novarocks_type_contract::FunctionValueType)>()
            .max(2 * std::mem::size_of::<(u32, Arc<Field>)>())
            .max(std::mem::size_of::<super::super::WriterTypeSource<'_>>())
            .max(3 * std::mem::size_of::<u32>());
        assert!(Model::new(root_floor, 1, 2, 1, 3, limits()).is_ok());
        assert!(matches!(
            Model::new(root_floor - 1, 1, 2, 1, 3, limits()),
            Err(E::Control(CompileControlError::ResourceExhausted))
        ));
        let field = Field::new("名", DataType::Int32, false)
            .with_metadata(HashMap::from([("k".into(), "v".into())]));
        let occupied_floor =
            std::mem::size_of::<Field>() + "名".len() + std::mem::size_of::<(String, String)>();
        let mut exact = Model::new(occupied_floor, 0, 0, 0, 0, limits()).unwrap();
        exact.field(&field).unwrap();
        let mut under = Model::new(occupied_floor - 1, 0, 0, 0, 0, limits()).unwrap();
        let before = facts(&under);
        assert!(matches!(
            under.field(&field),
            Err(E::Control(CompileControlError::ResourceExhausted))
        ));
        assert_eq!(facts(&under), before);
        assert!(matches!(
            under.string(occupied_floor),
            Err(E::Control(CompileControlError::ResourceExhausted))
        ));
        assert!(matches!(
            under.carrier(&DataType::Timestamp(
                arrow::datatypes::TimeUnit::Second,
                Some(Arc::from("x".repeat(occupied_floor)))
            )),
            Err(E::Control(CompileControlError::ResourceExhausted))
        ));
    }
    #[test]
    fn metadata_free_wide_execution_actual_encoder_does_not_multiply_whole_source_work() {
        use super::super::{TypeCodecError, encode_borrowed_type_namespaces_sources_in};
        use novarocks_type_contract::FunctionValueType;
        struct Control {
            stop: Option<(usize, CompileControlError)>,
            trace: Mutex<Vec<u32>>,
        }
        impl PureCompileControl for Control {
            fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
                let mut trace = self.trace.lock().unwrap();
                let at = trace.len();
                if let Some((stop, _)) = self.stop {
                    assert!(at <= stop, "callback after refusal");
                }
                trace.push(units);
                if let Some((stop, cause)) = self.stop.clone()
                    && stop == at
                {
                    Err(cause)
                } else {
                    Ok(())
                }
            }
        }
        let ty = FunctionValueType::new(
            DataType::Struct(
                (0..320)
                    .map(|id| Field::new(format!("c{id}"), DataType::Int32, false))
                    .collect::<Vec<_>>()
                    .into(),
            ),
            false,
        );
        let roots = [(0, &ty)];
        let caps = PackageTypeProjectionLimits {
            max_definitions: 16384,
            max_expanded_nodes: 65536,
            max_string_bytes: 65536,
            max_allocation_requests: 1_000_000,
            max_allocation_request_bytes: 128 * 1024 * 1024,
            max_coexisting_source_and_request_bytes: 256 * 1024 * 1024,
            max_work: 2_000_000_000,
        };
        let run = |source, caps, control: &Control| {
            let mut last = PackageTypeProjectionFacts::default();
            let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
            let result = encode_borrowed_type_namespaces_sources_in(
                &roots,
                &[],
                &[],
                &[],
                source,
                caps,
                &mut |facts| {
                    last = *facts;
                    Ok(())
                },
                &mut work,
            );
            if matches!(result, Err(TypeCodecError::Control(_))) {
                return result.map(|_| last);
            }
            work.finish()?;
            result.map(|_| last)
        };
        let low = Control {
            stop: None,
            trace: Mutex::new(Vec::new()),
        };
        let low_facts = run(1024 * 1024, caps, &low).unwrap();
        let high = Control {
            stop: None,
            trace: Mutex::new(Vec::new()),
        };
        let high_facts = run(2 * 1024 * 1024, caps, &high).unwrap();
        assert_eq!(
            high_facts.cumulative_work_upper_bound,
            low_facts.cumulative_work_upper_bound
        );
        assert_eq!(
            high_facts.coexisting_source_and_request_bytes_upper_bound
                - low_facts.coexisting_source_and_request_bytes_upper_bound,
            1024 * 1024
        );
        assert_eq!(*high.trace.lock().unwrap(), *low.trace.lock().unwrap());
        let exact = PackageTypeProjectionLimits {
            max_work: high_facts.cumulative_work_upper_bound,
            ..caps
        };
        let control = Control {
            stop: None,
            trace: Mutex::new(Vec::new()),
        };
        assert!(run(2 * 1024 * 1024, exact, &control).is_ok());
        let control = Control {
            stop: None,
            trace: Mutex::new(Vec::new()),
        };
        assert!(matches!(
            run(
                2 * 1024 * 1024,
                PackageTypeProjectionLimits {
                    max_work: exact.max_work - 1,
                    ..exact
                },
                &control
            ),
            Err(TypeCodecError::Control(
                CompileControlError::ResourceExhausted
            ))
        ));
        let baseline = high.trace.lock().unwrap().clone();
        for at in 0..baseline.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = Control {
                    stop: Some((at, cause.clone())),
                    trace: Mutex::new(Vec::new()),
                };
                assert!(
                    matches!(run(2*1024*1024,caps,&control),Err(TypeCodecError::Control(actual))if actual==cause)
                );
                assert_eq!(*control.trace.lock().unwrap(), baseline[..=at]);
            }
        }
    }
}
