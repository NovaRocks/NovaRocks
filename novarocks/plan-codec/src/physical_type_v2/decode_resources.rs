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

//! Numerical contributions of the original package Type decoder. No control,
//! scope, datatype grammar, allocation grant or independent budget is owned.

use super::{
    PackageTypeGraphFacts, PackageTypeProjectionFacts, PackageTypeProjectionLimits, TypeCodecError,
    graph::Node,
};
use crate::{
    btree_resources_v2 as btree,
    resource_source_model::{LOCKED_FAMILY, LOCKED_TOOLCHAIN},
};
use arrow::datatypes::{DataType, Field};
use novarocks_proto_models::{physical_type_v2 as wire, plan};
use novarocks_type_contract::{
    CompileControlError, FunctionValueType, NR_LOGICAL_TYPE_KEY,
    owned_resources::{hashmap, layout},
};
use std::{alloc::Layout, sync::Arc};
use wire::carrier_type_definition::Kind;

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
fn tree_error(e: btree::BTreeResourceError) -> E {
    match e {
        btree::BTreeResourceError::Arithmetic(_) => CompileControlError::ResourceExhausted.into(),
        btree::BTreeResourceError::SourceModel(s) => E::ResourceSource(s),
    }
}
fn hash_error(e: hashmap::HashMapResourceError) -> E {
    match e {
        hashmap::HashMapResourceError::Arithmetic(_) => {
            CompileControlError::ResourceExhausted.into()
        }
        hashmap::HashMapResourceError::SourceModel(s) => E::ResourceSource(s),
    }
}
fn arc(payload: Layout) -> Result<Layout, E> {
    layout::arc_layout(payload).map_err(|e| match e {
        layout::LayoutResourceError::SourceModel => {
            E::ResourceSource("type decoder Arc source model drift")
        }
        _ => CompileControlError::ResourceExhausted.into(),
    })
}

pub(super) struct TypeDecodeModel {
    source: usize,
    definitions: usize,
    expanded: usize,
    strings: usize,
    requests: usize,
    bytes: usize,
    work: usize,
    lookup: usize,
    metadata_table_bytes: usize,
    metadata_buckets: usize,
    metadata_entries: usize,
    graph: PackageTypeGraphFacts,
}
impl TypeDecodeModel {
    fn request(&mut self, allocation: Layout, count: usize) -> Result<(), E> {
        if allocation.size() == 0 || count == 0 {
            return Ok(());
        }
        let bytes = mul(allocation.size(), count)?;
        self.requests = add(self.requests, count)?;
        self.bytes = add(self.bytes, bytes)?;
        // Actual initialization/movement and abort cleanup of admitted backing.
        // Library interiors remain opaque; this is not cooperative work.
        self.work = add(self.work, add(mul(bytes, 4)?, mul(count, 128)?)?)?;
        Ok(())
    }
    fn tree<K, V>(&mut self, n: usize, searches: usize) -> Result<(), E> {
        let facts = btree::insertion_only::<K, V>(n).map_err(tree_error)?;
        self.requests = add(self.requests, facts.allocation_requests_upper_bound)?;
        self.bytes = add(self.bytes, facts.request_bytes_upper_bound)?;
        self.work = add(self.work, mul(facts.cumulative_work_upper_bound, searches)?)?;
        self.work = add(self.work, mul(facts.request_bytes_upper_bound, 4)?)?;
        Ok(())
    }
    fn string(&mut self, bytes: usize) -> Result<(), E> {
        self.strings = add(self.strings, bytes)?;
        self.request(array::<u8>(bytes)?, 1)?;
        self.work = add(self.work, add(64, mul(bytes, 8)?)?)?;
        Ok(())
    }
    /// Summary and Frame are the caller's real topology structs, never mirrors.
    /// All known maps/scratch/Field Arcs are admitted before any source walk.
    pub(super) fn new<Summary, Frame>(table: &wire::TypeTable, source: usize) -> Result<Self, E> {
        if !LOCKED_TOOLCHAIN || !LOCKED_FAMILY {
            return Err(E::ResourceSource("type decoder library source model drift"));
        }
        let vertices = add(table.carriers.len(), table.fields.len())?;
        let definitions = add(vertices, table.value_types.len())?;
        let mut model = Self {
            source,
            definitions,
            expanded: 0,
            strings: 0,
            requests: 0,
            bytes: 0,
            work: 256,
            lookup: btree::lookup_work_typed(vertices).map_err(tree_error)?,
            metadata_table_bytes: 0,
            metadata_buckets: 0,
            metadata_entries: 0,
            graph: PackageTypeGraphFacts::default(),
        };
        model.tree::<u32, DataType>(table.carriers.len(), 1)?;
        model.tree::<u32, Arc<Field>>(table.fields.len(), 1)?;
        model.tree::<u32, FunctionValueType>(table.value_types.len(), 1)?;
        model.tree::<u32, ()>(table.value_types.len(), 1)?;
        model.tree::<Node, Summary>(vertices, 1)?;
        // Active entries retain false; no removal/rebuild allocation exists.
        model.tree::<Node, bool>(vertices, 2)?;
        model.request(array::<Node>(vertices)?, 1)?;
        model.request(array::<Frame>(vertices)?, 1)?;
        model.request(arc(Layout::new::<Field>())?, table.fields.len())?;
        // Same original package Field namespace's weak origin records. These
        // are pre-admitted before their Vec and final Arc slice publication.
        type Loan =
            novarocks_type_contract::owned_resources::metadata_materialization::MetadataFieldLoan;
        model.request(array::<Loan>(table.fields.len())?, 2)?;
        model.request(arc(array::<Loan>(table.fields.len())?)?, 1)?;
        // One terminal ConnectorError contains fixed writer-law text or a
        // bounded logical-enum diagnostic (under128 bytes). Rust1.98 String
        // growth has at most128 requests and <=4*128 cumulative payload bytes.
        // This is an error-path request upper, not a semantic message limit.
        model.requests = add(model.requests, 128)?;
        model.bytes = add(model.bytes, 512)?;
        model.work = add(model.work, add(4 * 512, 128 * 128)?)?;
        model.work = add(model.work, add(mul(definitions, 256)?, mul(source, 16)?)?)?;
        Ok(model)
    }
    /// Replace the same prepared graph contribution, including its real
    /// retained scratch/inline coexistence. The original source B is not added.
    pub(super) fn graph(
        &mut self,
        facts: PackageTypeGraphFacts,
    ) -> Result<(), CompileControlError> {
        if facts.definition_count != self.definitions {
            return Err(CompileControlError::ResourceExhausted);
        }
        self.graph = facts;
        Ok(())
    }
    pub(super) fn carrier(&mut self, kind: &Kind) -> Result<(), E> {
        self.work = add(self.work, add(256, mul(self.lookup, 4)?)?)?;
        match kind {
            Kind::StructType(fields) => {
                let n = fields.field_ids.len();
                self.request(array::<Arc<Field>>(n)?, 1)?;
                self.request(arc(array::<Arc<Field>>(n)?)?, 1)?;
                self.work = add(self.work, mul(n, add(256, mul(self.lookup, 4)?)?)?)?;
            }
            Kind::UnionType(fields) => {
                let n = fields.fields.len();
                self.request(array::<i8>(n)?, 1)?;
                self.request(array::<Arc<Field>>(n)?, 1)?;
                // Arrow58.4 UnionFields::try_new owns Vec::new()+push, followed
                // by Arc::from(Vec). Rust1.98 amortized capacities start at4
                // tuples and double: at mostN requests, cumulative <=4N tuples.
                if n != 0 {
                    let growth = array::<(i8, Arc<Field>)>(mul(n, 4)?)?;
                    self.requests = add(self.requests, n)?;
                    self.bytes = add(self.bytes, growth.size())?;
                    self.work = add(self.work, add(mul(growth.size(), 4)?, mul(n, 128)?)?)?;
                }
                self.request(arc(array::<(i8, Arc<Field>)>(n)?)?, 1)?;
                self.work = add(self.work, mul(n, add(512, mul(self.lookup, 4)?)?)?)?;
            }
            Kind::Timestamp(timestamp) => {
                if let Some(zone) = &timestamp.timezone {
                    self.strings = add(self.strings, zone.len())?;
                    self.request(arc(array::<u8>(zone.len())?)?, 1)?;
                    self.work = add(self.work, add(128, mul(zone.len(), 8)?)?)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    pub(super) fn field(&mut self, field: &wire::FieldDefinition) -> Result<(), E> {
        self.string(field.name.len())?;
        let entries = field.metadata.len();
        let table = hashmap::fresh_table_layout::<String, String>(entries).map_err(hash_error)?;
        self.metadata_table_bytes = self
            .metadata_table_bytes
            .max(table.layout.map_or(0, |l| l.size()));
        self.metadata_buckets = self.metadata_buckets.max(table.buckets);
        self.metadata_entries = self.metadata_entries.max(entries);
        if let Some(layout) = table.layout {
            self.request(layout, 1)?;
        }
        // Before reading entries, original raw source union B bounds all key
        // bytes/longest key; later entry calls charge actual owned Strings.
        self.work = add(
            self.work,
            hashmap::fresh_string_table_work_upper_bound(entries, self.source, self.source)
                .map_err(hash_error)?,
        )?;
        self.work = add(
            self.work,
            hashmap::source_iterator_work_upper_bound(self.source, entries).map_err(hash_error)?,
        )?;
        self.work = add(self.work, add(256, mul(self.lookup, 4)?)?)?;
        Ok(())
    }
    pub(super) fn entry(&mut self, entry: &plan::ArrowFieldMetadataEntry) -> Result<(), E> {
        self.string(entry.key.len())?;
        self.string(entry.value.len())?;
        // Sole ordered-key comparison reads at most the original two strings.
        self.work = add(self.work, add(128, mul(entry.key.len(), 4)?)?)?;
        Ok(())
    }
    /// Every C/F/V unfolded contribution, with actual Dictionary clone/own
    /// Boxes separately counted by the original decoder's topology summary.
    pub(super) fn expanded(
        &mut self,
        nodes: usize,
        dictionary_boxes: usize,
    ) -> Result<(), CompileControlError> {
        self.expanded = add(self.expanded, nodes)?;
        self.work = add(self.work, mul(nodes, add(256, mul(self.lookup, 8)?)?)?)?;
        let layout = Layout::array::<DataType>(dictionary_boxes)
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        self.requests = add(self.requests, dictionary_boxes)?;
        self.bytes = add(self.bytes, layout.size())?;
        self.work = add(
            self.work,
            add(mul(layout.size(), 4)?, mul(dictionary_boxes, 128)?)?,
        )?;
        Ok(())
    }
    /// Requests of the unchanged strict Value walker, which starts with one
    /// heap tuple and grows its Vec on child pushes. No tree grammar is copied.
    /// The caller supplies its original unfolded Carrier summary before the
    /// walk: Rust1.98 min-four/doubling growth has at most N requests and
    /// cumulative payload at most 4N tuples, including the initial singleton.
    fn metadata_validation(&mut self, visits: usize) -> Result<(), E> {
        let key = NR_LOGICAL_TYPE_KEY.len();
        let lookup = hashmap::string_operations_work_upper_bound(
            self.metadata_buckets,
            visits,
            mul(visits, key)?,
            key,
        )
        .map_err(hash_error)?;
        let iteration = hashmap::source_iterator_work_upper_bound(
            self.metadata_table_bytes,
            self.metadata_entries,
        )
        .map_err(hash_error)?;
        self.work = add(self.work, add(lookup, mul(visits, iteration)?)?)?;
        // Strict byte/entry checks are bounded by their original owner caps;
        // Writer checks read lengths only. No String payload is copied here.
        self.work = add(
            self.work,
            mul(visits, add(256, mul(self.metadata_entries, 16)?)?)?,
        )?;
        Ok(())
    }
    pub(super) fn validation(&mut self, nodes: usize) -> Result<(), E> {
        // Even an erroneous zero summary cannot erase the initial singleton.
        // Actual Carrier summaries supplied by the original author are >=1.
        let requests = nodes.max(1);
        let payload = array::<(&DataType, usize)>(mul(requests, 4)?)?;
        self.request(payload, 1)?;
        let remaining = requests - 1;
        self.requests = add(self.requests, remaining)?;
        self.work = add(self.work, mul(remaining, 128)?)?;
        self.metadata_validation(nodes)?;
        Ok(())
    }
    pub(super) fn writer_roots(
        &mut self,
        roots: usize,
        expanded: usize,
        source_visit_work: usize,
    ) -> Result<(), E> {
        // A second root-source visit is a caller contribution, not reuse of
        // the graph preparation's already consumed one-visit contribution.
        // This is supplied by that exact root source, including empty recipes
        // and Schema/IPC headers, not inferred from Writer root cardinality.
        self.work = add(self.work, source_visit_work)?;
        self.work = add(
            self.work,
            add(
                mul(roots, add(256, mul(self.lookup, 4)?)?)?,
                mul(expanded, add(256, mul(self.lookup, 8)?)?)?,
            )?,
        )?;
        self.metadata_validation(expanded)?;
        Ok(())
    }
    pub(super) fn facts(
        &self,
        limits: PackageTypeProjectionLimits,
    ) -> Result<PackageTypeProjectionFacts, CompileControlError> {
        let request_bytes = add(self.bytes, self.graph.request_bytes_upper_bound)?;
        let facts = PackageTypeProjectionFacts {
            definition_count: self.definitions,
            expanded_node_count: self.expanded,
            string_bytes: self.strings,
            allocation_requests_upper_bound: add(
                self.requests,
                self.graph.allocation_requests_upper_bound,
            )?,
            allocation_request_bytes_upper_bound: request_bytes,
            coexisting_source_and_request_bytes_upper_bound: add(
                self.source.max(self.graph.coexistence_bytes_upper_bound),
                self.bytes,
            )?,
            cumulative_work_upper_bound: add(self.work, self.graph.cumulative_work_upper_bound)?,
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_type_contract::{CompileCheckpoints, CompilePhase, PureCompileControl};
    use std::sync::{Mutex, atomic::AtomicUsize};

    fn unlimited() -> PackageTypeProjectionLimits {
        PackageTypeProjectionLimits {
            max_definitions: usize::MAX,
            max_expanded_nodes: usize::MAX,
            max_string_bytes: usize::MAX,
            max_allocation_requests: usize::MAX,
            max_allocation_request_bytes: usize::MAX,
            max_coexisting_source_and_request_bytes: usize::MAX,
            max_work: usize::MAX,
        }
    }
    fn model() -> TypeDecodeModel {
        TypeDecodeModel::new::<u64, [usize; 4]>(&wire::TypeTable::default(), 0).unwrap()
    }
    fn facts(model: &TypeDecodeModel) -> PackageTypeProjectionFacts {
        model.facts(unlimited()).unwrap()
    }
    fn field() -> wire::FieldDefinition {
        wire::FieldDefinition {
            id: u32::MAX,
            name: "名\0".into(),
            nullable: true,
            carrier_type_id: Some(0),
            metadata: vec![plan::ArrowFieldMetadataEntry {
                key: "é".into(),
                value: "中国\0".into(),
            }],
            dictionary_id: Some(-9),
            dictionary_is_ordered: Some(true),
        }
    }
    fn arc_golden(payload: Layout) -> usize {
        Layout::new::<[AtomicUsize; 2]>()
            .extend(payload)
            .unwrap()
            .0
            .pad_to_align()
            .size()
    }

    #[test]
    fn original_map_scratch_and_field_arc_layouts_are_prefunded_once() {
        let table = wire::TypeTable {
            carriers: vec![wire::CarrierTypeDefinition {
                id: 0,
                kind: Some(Kind::Primitive(5)),
            }],
            fields: vec![field()],
            value_types: vec![wire::ValueTypeDefinition {
                id: u32::MAX,
                carrier_type_id: Some(0),
                nullable: true,
                logical_type: 1,
            }],
        };
        let original = TypeDecodeModel::new::<u64, [usize; 4]>(&table, 4096).unwrap();
        let actual = facts(&original);
        assert_eq!(actual.definition_count, 3);
        // C/F/V/Vguard use 1 entry each; summary/active use 2 each. Active
        // false overwrites are work only, with no remove/rebuild requests.
        // The materialized Field namespace also owns one Vec, its copy,
        // and the final Arc slice of the real metadata origin loans.
        type Loan =
            novarocks_type_contract::owned_resources::metadata_materialization::MetadataFieldLoan;
        assert_eq!(actual.allocation_requests_upper_bound, 8 + 2 + 1 + 3 + 128);
        let trees = btree::insertion_only::<u32, DataType>(1)
            .unwrap()
            .request_bytes_upper_bound
            + btree::insertion_only::<u32, Arc<Field>>(1)
                .unwrap()
                .request_bytes_upper_bound
            + btree::insertion_only::<u32, FunctionValueType>(1)
                .unwrap()
                .request_bytes_upper_bound
            + btree::insertion_only::<u32, ()>(1)
                .unwrap()
                .request_bytes_upper_bound
            + btree::insertion_only::<Node, u64>(2)
                .unwrap()
                .request_bytes_upper_bound
            + btree::insertion_only::<Node, bool>(2)
                .unwrap()
                .request_bytes_upper_bound;
        let expected = trees
            + 2 * size_of::<Node>()
            + 2 * size_of::<[usize; 4]>()
            + arc_golden(Layout::new::<Field>())
            + 2 * size_of::<Loan>()
            + arc_golden(Layout::new::<Loan>())
            + 512;
        assert_eq!(actual.allocation_request_bytes_upper_bound, expected);
        assert_eq!(
            actual.coexisting_source_and_request_bytes_upper_bound,
            4096 + expected
        );
        assert_eq!(actual.string_bytes, 0);
        assert_eq!(actual.expanded_node_count, 0);
    }

    #[test]
    fn carrier_owned_requests_keep_empty_arc_and_union_growth_separate_from_dictionary_boxes() {
        let mut actual = model();
        let baseline = facts(&actual);
        actual
            .carrier(&Kind::StructType(wire::StructFields { field_ids: vec![] }))
            .unwrap();
        let empty = facts(&actual);
        assert_eq!(
            empty.allocation_requests_upper_bound - baseline.allocation_requests_upper_bound,
            1
        );
        assert_eq!(
            empty.allocation_request_bytes_upper_bound
                - baseline.allocation_request_bytes_upper_bound,
            16
        );
        let union = Kind::UnionType(wire::UnionFields {
            mode: 1,
            fields: (0..3)
                .map(|id| wire::UnionField {
                    type_id: id,
                    field_id: Some(u32::MAX),
                })
                .collect(),
        });
        actual.carrier(&union).unwrap();
        let union_facts = facts(&actual);
        // Two input Vecs, at most N out-Vec growth requests, one final Arc.
        assert_eq!(
            union_facts.allocation_requests_upper_bound - empty.allocation_requests_upper_bound,
            6
        );
        let expected = 3 * size_of::<i8>()
            + 3 * size_of::<Arc<Field>>()
            + 12 * size_of::<(i8, Arc<Field>)>()
            + arc_golden(Layout::array::<(i8, Arc<Field>)>(3).unwrap());
        assert_eq!(
            union_facts.allocation_request_bytes_upper_bound
                - empty.allocation_request_bytes_upper_bound,
            expected
        );
        // The source grammar author supplies all own/recursive Dictionary
        // boxes once, including C roots and subsequent F/V clone occurrences.
        actual.expanded(11, 6).unwrap();
        let boxed = facts(&actual);
        assert_eq!(boxed.expanded_node_count, 11);
        assert_eq!(
            boxed.allocation_requests_upper_bound - union_facts.allocation_requests_upper_bound,
            6
        );
        assert_eq!(
            boxed.allocation_request_bytes_upper_bound
                - union_facts.allocation_request_bytes_upper_bound,
            6 * size_of::<DataType>()
        );
    }

    #[test]
    fn actual_utf8_metadata_and_timezone_byte_requests_have_independent_extents() {
        let mut actual = model();
        let initial = facts(&actual);
        let field = field();
        actual.field(&field).unwrap();
        actual.entry(&field.metadata[0]).unwrap();
        let current = facts(&actual);
        let table = hashmap::fresh_table_layout::<String, String>(1).unwrap();
        assert_eq!(current.string_bytes, 4 + 2 + 7);
        assert_eq!(
            current.allocation_requests_upper_bound - initial.allocation_requests_upper_bound,
            4
        );
        assert_eq!(
            current.allocation_request_bytes_upper_bound
                - initial.allocation_request_bytes_upper_bound,
            4 + 2 + 7 + table.request_bytes_upper_bound
        );
        let timestamp = Kind::Timestamp(plan::ArrowTimestampType {
            unit: 1,
            timezone: Some(String::new()),
        });
        actual.carrier(&timestamp).unwrap();
        let with_zone = facts(&actual);
        assert_eq!(with_zone.string_bytes, current.string_bytes);
        assert_eq!(
            with_zone.allocation_requests_upper_bound - current.allocation_requests_upper_bound,
            1
        );
        assert_eq!(
            with_zone.allocation_request_bytes_upper_bound
                - current.allocation_request_bytes_upper_bound,
            16
        );
    }

    #[test]
    fn original_strict_walker_singleton_and_wide_growth_have_hand_counted_requests() {
        let mut actual = model();
        let initial = facts(&actual);
        actual.validation(1).unwrap();
        let singleton = facts(&actual);
        assert_eq!(
            singleton.allocation_requests_upper_bound - initial.allocation_requests_upper_bound,
            1
        );
        assert_eq!(
            singleton.allocation_request_bytes_upper_bound
                - initial.allocation_request_bytes_upper_bound,
            4 * size_of::<(&DataType, usize)>()
        );
        assert_eq!(
            singleton.coexisting_source_and_request_bytes_upper_bound
                - initial.coexisting_source_and_request_bytes_upper_bound,
            4 * size_of::<(&DataType, usize)>()
        );
        // Actual vec![root] initially requests one tuple. For 320 pending
        // children its later capacities 4..512 sum to 1020 tuples; the closed
        // upper admits 4*321 tuples and 321 requests before that unchanged walk.
        actual.validation(321).unwrap();
        let wide = facts(&actual);
        assert_eq!(
            wide.allocation_requests_upper_bound - singleton.allocation_requests_upper_bound,
            321
        );
        assert_eq!(
            wide.allocation_request_bytes_upper_bound
                - singleton.allocation_request_bytes_upper_bound,
            1284 * size_of::<(&DataType, usize)>()
        );
        // 1284 tuples cover the actual 1+4+8+16+32+64+128+256+512 = 1021.
        assert_eq!(wide.expanded_node_count, 0);
        assert_eq!(wide.string_bytes, 0);
        let mut under = unlimited();
        under.max_allocation_request_bytes = wide.allocation_request_bytes_upper_bound - 1;
        assert_eq!(
            actual.facts(under),
            Err(CompileControlError::ResourceExhausted)
        );
        assert!(matches!(
            actual.validation(usize::MAX),
            Err(E::Control(CompileControlError::ResourceExhausted))
        ));
    }

    #[test]
    fn graph_replacement_uses_one_union_invoice_and_writer_visit_is_a_distinct_contribution() {
        let mut actual =
            TypeDecodeModel::new::<u64, [usize; 4]>(&wire::TypeTable::default(), 1000).unwrap();
        let base = facts(&actual);
        let first = PackageTypeGraphFacts {
            definition_count: 0,
            allocation_requests_upper_bound: 3,
            request_bytes_upper_bound: 80,
            coexistence_bytes_upper_bound: 1104,
            cumulative_work_upper_bound: 700,
            ..Default::default()
        };
        actual.graph(first).unwrap();
        let merged = facts(&actual);
        assert_eq!(
            merged.allocation_requests_upper_bound,
            base.allocation_requests_upper_bound + 3
        );
        assert_eq!(
            merged.allocation_request_bytes_upper_bound,
            base.allocation_request_bytes_upper_bound + 80
        );
        assert_eq!(
            merged.coexisting_source_and_request_bytes_upper_bound,
            base.allocation_request_bytes_upper_bound + 1104
        );
        assert_eq!(
            merged.cumulative_work_upper_bound,
            base.cumulative_work_upper_bound + 700
        );
        actual
            .graph(PackageTypeGraphFacts {
                cumulative_work_upper_bound: 900,
                ..first
            })
            .unwrap();
        assert_eq!(
            facts(&actual).cumulative_work_upper_bound,
            base.cumulative_work_upper_bound + 900
        );
        let before = facts(&actual);
        // Even no Writer fields can coexist with empty recipes/schema headers.
        actual.writer_roots(0, 0, 1024).unwrap();
        let after = facts(&actual);
        assert!(after.cumulative_work_upper_bound >= before.cumulative_work_upper_bound + 1024);
        assert_eq!(
            after.allocation_requests_upper_bound,
            before.allocation_requests_upper_bound
        );
        assert_eq!(
            after.allocation_request_bytes_upper_bound,
            before.allocation_request_bytes_upper_bound
        );
    }

    #[test]
    fn every_projection_axis_accepts_exact_and_refuses_one_under_without_callbacks() {
        let mut actual = TypeDecodeModel::new::<u64, [usize; 4]>(
            &wire::TypeTable {
                fields: vec![field()],
                ..Default::default()
            },
            4096,
        )
        .unwrap();
        actual.field(&field()).unwrap();
        actual.entry(&field().metadata[0]).unwrap();
        actual.expanded(3, 2).unwrap();
        let f = facts(&actual);
        let exact = PackageTypeProjectionLimits {
            max_definitions: f.definition_count,
            max_expanded_nodes: f.expanded_node_count,
            max_string_bytes: f.string_bytes,
            max_allocation_requests: f.allocation_requests_upper_bound,
            max_allocation_request_bytes: f.allocation_request_bytes_upper_bound,
            max_coexisting_source_and_request_bytes: f
                .coexisting_source_and_request_bytes_upper_bound,
            max_work: f.cumulative_work_upper_bound,
        };
        assert_eq!(actual.facts(exact).unwrap(), f);
        for axis in 0..7 {
            let mut under = exact;
            match axis {
                0 => under.max_definitions -= 1,
                1 => under.max_expanded_nodes -= 1,
                2 => under.max_string_bytes -= 1,
                3 => under.max_allocation_requests -= 1,
                4 => under.max_allocation_request_bytes -= 1,
                5 => under.max_coexisting_source_and_request_bytes -= 1,
                6 => under.max_work -= 1,
                _ => unreachable!(),
            }
            assert_eq!(
                actual.facts(under),
                Err(CompileControlError::ResourceExhausted)
            );
        }
    }

    struct LateControl {
        cause: CompileControlError,
        events: Mutex<Vec<u32>>,
    }
    impl PureCompileControl for LateControl {
        fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            let mut events = self.events.lock().unwrap();
            assert!(events.len() < 2, "callback after first refusal");
            let at = events.len();
            events.push(units);
            if at == 1 { Err(self.cause) } else { Ok(()) }
        }
    }
    #[test]
    fn pure_known_resource_gate_wins_over_pending_255_next_control() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = LateControl {
                cause,
                events: Mutex::new(vec![]),
            };
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
            for _ in 0..255 {
                work.step().unwrap();
            }
            let actual = model();
            let mut limit = unlimited();
            limit.max_allocation_requests = facts(&actual).allocation_requests_upper_bound - 1;
            let outcome = (|| -> Result<(), CompileControlError> {
                actual.facts(limit)?;
                work.step()?;
                work.finish()
            })();
            assert_eq!(outcome, Err(CompileControlError::ResourceExhausted));
            assert_eq!(*control.events.lock().unwrap(), [0]);
        }
        assert!(matches!(
            array::<DataType>(usize::MAX),
            Err(E::Control(CompileControlError::ResourceExhausted))
        ));
        assert!(matches!(
            hash_error(hashmap::HashMapResourceError::SourceModel("drift")),
            E::ResourceSource("drift")
        ));
        assert!(matches!(
            tree_error(btree::BTreeResourceError::Arithmetic("overflow")),
            E::Control(CompileControlError::ResourceExhausted)
        ));
    }
}

#[cfg(test)]
mod materialized_metadata_tests {
    use super::*;
    #[test]
    fn validation_uses_actual_fresh_table_layout_without_borrowing_raw_source_capacity() {
        let field = wire::FieldDefinition {
            metadata: vec![
                plan::ArrowFieldMetadataEntry {
                    key: "k".into(),
                    value: "v".into(),
                };
                65
            ],
            ..Default::default()
        };
        let mut model = TypeDecodeModel::new::<u64, [usize; 4]>(
            &wire::TypeTable {
                fields: vec![field.clone()],
                ..Default::default()
            },
            1,
        )
        .unwrap();
        model.field(&field).unwrap();
        let actual = hashmap::fresh_table_layout::<String, String>(65).unwrap();
        assert_eq!(model.metadata_table_bytes, actual.layout.unwrap().size());
        assert_eq!(model.metadata_buckets, actual.buckets);
        assert!(model.metadata_table_bytes > model.source);
        let before = model.work;
        model.metadata_validation(3).unwrap();
        let key = NR_LOGICAL_TYPE_KEY.len();
        let expected = hashmap::string_operations_work_upper_bound(actual.buckets, 3, 3 * key, key)
            .unwrap()
            + 3 * hashmap::source_iterator_work_upper_bound(actual.layout.unwrap().size(), 65)
                .unwrap()
            + 3 * (256 + 65 * 16);
        assert_eq!(model.work - before, expected);
    }
}
