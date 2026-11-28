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

//! Cumulative visits of the actual recursive safe-reader/canonical pool graph.
//! These bounds describe opaque source work; they are not synthetic callback
//! charges, CPU cycles, formal host grants or internal cooperation claims.

use super::{
    reader_allocations::ReaderAllocationRequests,
    reader_resources::{PayloadRequests, ReaderInput, add, invalid, mul},
};
use crate::ipc_flat_stream_v2::resource_work::ResourceWork;
use crate::ipc_flat_stream_v2::{
    FlatPoolResourceError, reader_diagnostics::ReaderDiagnosticRequests,
};
use crate::ipc_recursive_batch_v2::RecursiveNodeGeometry;
use arrow::datatypes::DataType;
use novarocks_constant_contract::ConstantResourceFacts;
use novarocks_type_contract::NR_LOGICAL_TYPE_KEY;

#[derive(Clone, Copy, Debug)]
pub(super) struct SourceMetadataWork {
    pub source_retained_bytes: usize,
    pub total: usize,
    #[cfg(test)]
    pub one_comparison: usize,
    pub native_comparisons: usize,
}

#[cfg(test)]
pub(super) fn metadata_comparison_work(
    field: &arrow::datatypes::Field,
) -> Result<usize, FlatPoolResourceError> {
    field
        .metadata()
        .iter()
        .try_fold(0usize, |total, (key, value)| {
            add(total, metadata_entry_work(key, value)?)
        })
}

fn metadata_entry_work(key: &str, value: &str) -> Result<usize, FlatPoolResourceError> {
    add(8, mul(2, add(key.len(), value.len())?)?)
}

pub(super) fn native_metadata_frequency(
    node: &RecursiveNodeGeometry<'_>,
) -> Result<usize, FlatPoolResourceError> {
    // get_valid_child_data compares the child's DataType, excluding that
    // child's Field metadata. Only proper ancestors at depths 1..d-2 can
    // therefore inspect this Field. validate_full invokes validate recursively
    // at every ancestor: four complete applications contribute 4*sum(depth).
    // Extra list/map ancestor projections contribute at most A*sum(depth),
    // and constructor child-type comparisons contribute (2+A)*(d-2).
    let ancestors = node.depth.saturating_sub(2);
    let depth_sum = mul(ancestors, add(ancestors, 1)?)? / 2;
    add(
        mul(add(4, node.list_map_ancestors)?, depth_sum)?,
        mul(add(2, node.list_map_ancestors)?, ancestors)?,
    )
}

pub(super) fn source_metadata_work<W: ResourceWork>(
    nodes: &[RecursiveNodeGeometry<'_>],
    source_retained_bytes: usize,
    work: &mut W,
) -> Result<SourceMetadataWork, W::Error> {
    // Each Field occurrence contributes its original lengths. A counterpart
    // with a different name or string length can fail before inspecting those
    // bytes. Equal lengths bound both operands by the authored occurrence.
    let mut model_work = 0;
    let mut source_passes = 0;
    #[cfg(test)]
    let mut comparisons = 0;
    let mut native_comparisons = 0;
    for (ordinal, node) in nodes.iter().enumerate() {
        model_work = work.numeric(add(model_work, 1))?;
        let name_work = work.numeric(mul(2, node.field.name().len()))?;
        let zone_work = if let DataType::Timestamp(_, Some(zone)) = node.field.data_type() {
            work.numeric(mul(2, zone.len()))?
        } else {
            0
        };
        let mut local = work.numeric(add(work.numeric(add(name_work, zone_work))?, 64))?;
        let mut metadata = 0;
        for (key, value) in node.field.metadata() {
            metadata = work.numeric(add(
                metadata,
                work.numeric(metadata_entry_work(key, value))?,
            ))?;
            local = work.numeric(add(local, NR_LOGICAL_TYPE_KEY.len() + 1))?;
            model_work = work.numeric(add(model_work, 1))?;
            work.library_work(work.numeric(add(source_passes, model_work))?)?;
            work.step()?;
        }
        local = work.numeric(add(local, metadata))?;
        // The root Field stays outside DataType equality.
        if ordinal != 0 {
            #[cfg(test)]
            {
                comparisons = work.numeric(add(comparisons, metadata))?;
            }
            native_comparisons = work.numeric(add(
                native_comparisons,
                work.numeric(mul(metadata, native_metadata_frequency(node)?))?,
            ))?;
        }
        source_passes = work.numeric(add(source_passes, work.numeric(mul(6, local))?))?;
        work.library_work(work.numeric(add(source_passes, model_work))?)?;
        work.step()?;
    }
    let total = work.numeric(add(source_passes, model_work))?;
    Ok(SourceMetadataWork {
        source_retained_bytes,
        total,
        #[cfg(test)]
        one_comparison: comparisons,
        native_comparisons,
    })
}

fn extent(n: u64) -> Result<usize, FlatPoolResourceError> {
    usize::try_from(n).map_err(|_| invalid("recursive reader work extent"))
}

pub(super) fn preflight<W: ResourceWork>(
    input: &ReaderInput<'_, '_>,
    payload: &PayloadRequests,
    pool: &ConstantResourceFacts,
    structures: &ReaderAllocationRequests,
    diagnostics: &ReaderDiagnosticRequests,
    source: &SourceMetadataWork,
    work: &mut W,
) -> Result<usize, W::Error> {
    let source_retained_bytes = source.source_retained_bytes;
    // The shared recursive envelope covers full descendant type/NULL/value
    // validation, including unused storage, whole UTF8 fallback and repeated
    // view references. Reader build, final two force_validate projections and
    // pool.validate_full are four applications of that same sole author.
    let mut total = work.numeric(mul(4, extent(pool.library_validation_work_upper_bound)?))?;
    total = work.numeric(add(
        total,
        work.numeric(add(
            work.numeric(add(input.body.len(), payload.repair_capacity))?,
            payload.empty_offsets_capacity,
        ))?,
    ))?;
    let mut source_projections = 0;
    let mut headers = 0;
    let mut metadata_pairs = 0;
    for node in input.nodes {
        let projections = work.numeric(add(2, node.list_map_ancestors))?;
        // datatype_display::format_metadata collects and sorts existing keys.
        // K² comparisons bounds the locked sort even for adversarial order;
        // every comparison can inspect <= the longest authored key.
        let entries = node.field.metadata().len();
        let mut longest_key = 0;
        work.flush()?;
        for key in node.field.metadata().keys() {
            longest_key = longest_key.max(key.len());
            work.library_work(total)?;
            work.step()?;
        }
        work.flush()?;
        metadata_pairs = work.numeric(add(
            metadata_pairs,
            work.numeric(mul(
                work.numeric(mul(entries, entries))?,
                work.numeric(add(longest_key, 1))?,
            ))?,
        ))?;
        // Extra list/map ancestor to_data validation: actual subtree type
        // correspondence and this node's own row/buffer-value inspection.
        let local = work.numeric(add(
            work.numeric(add(
                work.numeric(add(
                    work.numeric(mul(node.subtree_nodes, node.subtree_nodes))?,
                    node.rows,
                ))?,
                node.buffer_count,
            ))?,
            work.numeric(add(node.described_buffer_bytes, node.view_validation_bytes))?,
        ))?;
        let local = if matches!(node.field.data_type(), DataType::Utf8 | DataType::LargeUtf8) {
            work.numeric(add(local, node.described_buffer_bytes))? // full values fallback <= descriptors
        } else {
            local
        };
        total = work.numeric(add(
            total,
            work.numeric(mul(node.list_map_ancestors, local))?,
        ))?;
        source_projections = work.numeric(add(
            source_projections,
            work.numeric(mul(projections, node.subtree_nodes))?,
        ))?;
        // Each projection/canonical reconstruction clones and eventually drops
        // only header owners, not payload bytes. Initial reader slicing and
        // alignment also visit these exact local descriptor/header counts.
        let local_headers = work.numeric(add(
            work.numeric(mul(
                work.numeric(add(work.numeric(mul(2, projections))?, 3))?,
                node.buffer_count,
            ))?,
            work.numeric(add(
                work.numeric(mul(projections, node.variadic_buffers))?,
                work.numeric(mul(work.numeric(mul(2, projections))?, node.children))?,
            ))?,
        ))?;
        headers = work.numeric(add(headers, local_headers))?;
        // The post-array constant scan revisits raw headers/buffers plus every
        // view row. RequiredFrame semantic visits are bounded by the original
        // logical-elements author, not the root's row count alone.
        total = work.numeric(add(
            total,
            work.numeric(add(
                work.numeric(add(node.buffer_count, node.described_buffer_bytes))?,
                work.numeric(add(
                    1,
                    if matches!(
                        node.field.data_type(),
                        DataType::Utf8View | DataType::BinaryView
                    ) {
                        node.rows
                    } else {
                        0
                    },
                ))?,
            ))?,
        ))?;
        if matches!(
            node.field.data_type(),
            DataType::Utf8View | DataType::BinaryView
        ) {
            total = work.numeric(add(
                total,
                work.numeric(mul(
                    work.numeric(mul(work.numeric(add(4, node.list_map_ancestors))?, 4))?,
                    node.rows,
                ))?,
            ))?;
        }
        work.library_work(total)?;
        work.step()?;
    }
    // Native recursive comparisons inspect only actual nested child-type
    // paths, not every Field in every type visit. In particular, a wide Struct
    // of primitive children has no native metadata visits. RecordBatch's root
    // type comparison retains the cloned source DataType's nested Arc owners:
    // Arc equality short-circuits those identical Field/Fields owners. Arrays
    // in this reader are built solely from that source, without reauthoring
    // Fields; supplied FVT is used only by the three Constant comparisons.
    total = work.numeric(add(total, source.native_comparisons))?;
    // Formatting visits
    // complete descendant types once per candidate and ancestor wrapper;
    // metadata sorting has its own key-pair term, independent of type width.
    total = work.numeric(add(total, source.total))?;
    let diagnostic_type_visits = work.numeric(mul(
        work.numeric(mul(input.nodes.len(), input.nodes.len()))?,
        work.numeric(add(input.geometry.maximum_depth, 4))?,
    ))?;
    total = work.numeric(add(
        total,
        work.numeric(mul(
            source_retained_bytes,
            work.numeric(add(source_projections, diagnostic_type_visits))?,
        ))?,
    ))?;
    let render_passes = work.numeric(mul(
        work.numeric(mul(4, input.nodes.len()))?,
        work.numeric(add(input.geometry.maximum_depth, 4))?,
    ))?;
    // Four format arguments per failing candidate plus each ancestor wrapper.
    // The factor six also covers Debug escaping of the visited source bytes.
    let sorted_metadata =
        work.numeric(mul(work.numeric(mul(6, render_passes))?, metadata_pairs))?;
    total = work.numeric(add(total, sorted_metadata))?;
    total = work.numeric(add(total, work.numeric(mul(2, headers))?))?;
    total = work.numeric(add(
        total,
        work.numeric(mul(
            4,
            work.numeric(add(
                extent(pool.logical_elements_upper_bound)?,
                input.nodes.len(),
            ))?,
        ))?,
    ))?;
    let requests = work.numeric(add(
        work.numeric(add(
            structures.structural_request_bytes_upper_bound,
            diagnostics.request_bytes_upper_bound,
        ))?,
        input.geometry_scratch_request_bytes,
    ))?;
    let payload_count = work.numeric(add(
        work.numeric(add(
            usize::from(payload.body_capacity != 0),
            payload.repair_count,
        ))?,
        payload.empty_offsets_count,
    ))?;
    let request_count = work.numeric(add(
        work.numeric(add(
            work.numeric(add(
                structures.allocation_requests_upper_bound,
                payload_count,
            ))?,
            diagnostics.allocation_requests_upper_bound,
        ))?,
        input.geometry_scratch_request_count,
    ))?;
    // Allocation initialization, text/growing-container moves, and teardown.
    total = work.numeric(add(
        total,
        work.numeric(add(work.numeric(mul(4, requests))?, request_count))?,
    ))?;
    let owners = work.numeric(add(
        work.numeric(add(1, payload.repair_count))?,
        payload.empty_offsets_count,
    ))?;
    if owners > 3 {
        total = work.numeric(add(total, work.numeric(mul(owners, owners))?))?;
    }
    work.library_work(total)?;
    work.step()?;
    Ok(total)
}
