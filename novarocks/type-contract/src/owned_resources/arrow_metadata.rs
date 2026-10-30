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

//! Arrow 60 Metadata construction and retained-backing projections.
//! These numerical requests do not create a source receipt or grant capacity.

use super::{btree, layout};
use btree::BTreeResourceError;
use std::{alloc::Layout, collections::BTreeMap};

type Entry = (String, String);

fn arithmetic() -> BTreeResourceError {
    BTreeResourceError::Arithmetic("Arrow metadata request arithmetic overflow")
}
fn add(a: usize, b: usize) -> Result<usize, BTreeResourceError> {
    a.checked_add(b).ok_or_else(arithmetic)
}
fn mul(a: usize, b: usize) -> Result<usize, BTreeResourceError> {
    a.checked_mul(b).ok_or_else(arithmetic)
}

pub fn map_arc_layout() -> Result<Layout, BTreeResourceError> {
    layout::arc_layout(Layout::new::<BTreeMap<String, String>>()).map_err(|error| match error {
        layout::LayoutResourceError::SourceModel => {
            BTreeResourceError::SourceModel("Arrow metadata Arc allocation source model drift")
        }
        _ => arithmetic(),
    })
}

/// The immutable Metadata owns no allocation when empty. A nonempty BTreeMap
/// has at most one retained node per entry, including after clone or removal.
/// The existing locked B=6 node model covers private field-order padding for
/// both leaves and internal nodes; it is an upper bound, not a mirrored layout.
/// String payload capacities are a separate caller contribution.
pub fn retained_backing_upper_bound(entries: usize) -> Result<usize, BTreeResourceError> {
    if entries == 0 {
        return Ok(0);
    }
    add(
        mul(
            entries,
            btree::node_layout_typed::<String, String>()?.size(),
        )?,
        map_arc_layout()?.size(),
    )
}

/// One upper layout for every original allocation in HashMap -> Metadata.
/// Per-node layouts bound the actual leaf/internal requests independently.
#[derive(Clone, Copy, Debug)]
pub struct HashMapConversionRequests {
    pub entries_vector: Option<Layout>,
    pub sort_scratch: Option<Layout>,
    pub tree_node: Layout,
    pub tree_nodes: usize,
    pub map_arc: Option<Layout>,
}
impl HashMapConversionRequests {
    pub fn allocation_requests_upper_bound(self) -> Result<usize, BTreeResourceError> {
        add(
            add(self.tree_nodes, usize::from(self.map_arc.is_some()))?,
            add(
                usize::from(self.entries_vector.is_some()),
                usize::from(self.sort_scratch.is_some()),
            )?,
        )
    }
    /// Backing retained by this known original bulk-built Metadata only.
    /// The collect vector, sorting scratch and source HashMap have exited.
    pub fn retained_backing_upper_bound(self) -> Result<usize, BTreeResourceError> {
        add(
            mul(self.tree_nodes, self.tree_node.size())?,
            self.map_arc.map_or(0, |layout| layout.size()),
        )
    }
    pub fn request_bytes_upper_bound(self) -> Result<usize, BTreeResourceError> {
        let node_bytes = mul(self.tree_nodes, self.tree_node.size())?;
        [self.entries_vector, self.sort_scratch, self.map_arc]
            .into_iter()
            .try_fold(node_bytes, |bytes, layout| {
                add(bytes, layout.map_or(0, |value| value.size()))
            })
    }
    pub fn visit_requests<E>(
        self,
        exact: &mut impl FnMut(Layout, usize) -> Result<(), E>,
        opaque: &mut impl FnMut(usize, usize) -> Result<(), E>,
    ) -> Result<(), E> {
        if let Some(layout) = self.entries_vector {
            exact(layout, 1)?;
        }
        if let Some(layout) = self.sort_scratch {
            exact(layout, 1)?;
        }
        if self.tree_nodes != 0 {
            opaque(self.tree_node.size(), self.tree_nodes)?;
        }
        if let Some(layout) = self.map_arc {
            exact(layout, 1)?;
        }
        Ok(())
    }
}

/// Arrow 60 Metadata::from(HashMap) moves unique entries through std's
/// BTreeMap::from_iter: exact-size collect into a Vec, stable sort, bulk-build,
/// then Arc::new. Rust 1.98.1 bulk_push uses eleven keys/twelve children;
/// q=floor(n/12) counts separators on each successive internal level.
/// No text clone occurs. The original HashMap allocation remains separately
/// charged by its source owner, including while the collect vector is built.
pub fn hashmap_conversion_requests(
    entries: usize,
) -> Result<HashMapConversionRequests, BTreeResourceError> {
    let tree_node = btree::node_layout_typed::<String, String>()?;
    let mut request = HashMapConversionRequests {
        entries_vector: None,
        sort_scratch: None,
        tree_node,
        tree_nodes: 0,
        map_arc: None,
    };
    if entries == 0 {
        return Ok(request);
    }
    request.entries_vector =
        Some(Layout::array::<Entry>(entries.max(4)).map_err(|_| arithmetic())?);
    // stable/mod.rs uses insertion sort through length 20. The driftsort
    // scratch is max(ceil(n/2), min(n, 8MB / sizeof Entry), 48), with a
    // 4096-byte stack buffer; only a larger request reaches the allocator.
    if entries > 20 {
        let scratch = (entries - entries / 2)
            .max(entries.min(8_000_000 / std::mem::size_of::<Entry>()))
            .max(48);
        if scratch > 4096 / std::mem::size_of::<Entry>() {
            request.sort_scratch = Some(Layout::array::<Entry>(scratch).map_err(|_| arithmetic())?);
        }
    }
    request.tree_nodes = add(entries / 12, 1)?;
    let mut separators = entries / 12;
    while separators != 0 {
        request.tree_nodes = add(request.tree_nodes, add(separators / 12, 1)?)?;
        separators /= 12;
    }
    request.map_arc = Some(map_arc_layout()?);
    Ok(request)
}

/// Opaque sorting/tree movement units, not elapsed CPU or cooperation. Stable
/// sorting compares only borrowed String keys; n squared comparison/movement
/// steps cover both insertion-sort and merge/partition paths, and the original
/// BTree insertion model bounds tree construction and parent-link work.
pub fn hashmap_conversion_work_upper_bound(
    entries: usize,
    max_key_bytes: usize,
) -> Result<usize, BTreeResourceError> {
    let comparisons = mul(
        mul(entries, entries)?,
        add(mul(2, max_key_bytes)?, std::mem::size_of::<Entry>())?,
    )?;
    add(
        comparisons,
        btree::insertion_only::<String, String>(entries)?.cumulative_work_upper_bound,
    )
}
