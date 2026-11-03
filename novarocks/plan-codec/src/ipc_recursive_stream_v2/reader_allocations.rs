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

//! Structural requests of the locked recursive reader and constant owner.
//! Each list/map ancestor causes another child to_data/make_array projection;
//! Struct uses its direct constructor. No payload or allocation grant is added.

use super::reader_resources::{PayloadRequests, ReaderInput, add, invalid, mul};
use crate::ipc_flat_batch_v2::{Layout as FlatLayout, layout};
use crate::ipc_flat_stream_v2::resource_work::ResourceWork;
use crate::ipc_flat_stream_v2::{
    FlatPoolResourceError,
    reader_allocations::{Requests, concrete_array_layout, environment},
};
use arrow::{
    array::{ArrayData, ArrayRef, BufferSpec},
    datatypes::{DataType, FieldRef, Schema},
};
use arrow_buffer::Buffer;
use novarocks_constant_contract::ConstantPool;

use std::alloc::Layout;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ReaderAllocationRequests {
    pub structural_request_bytes_upper_bound: usize,
    pub allocation_requests_upper_bound: usize,
    /// Contribution observed by the actual ConstantPool constructor separately.
    pub constant_owner_request_bytes_upper_bound: usize,
    pub retained_reader_request_bytes_upper_bound: usize,
    pub retained_pool_data_request_bytes_upper_bound: usize,
}
fn array<T>(count: usize) -> Result<Layout, FlatPoolResourceError> {
    Layout::array::<T>(count).map_err(|_| invalid("recursive reader structural layout"))
}

/// Cumulative RawVec requests for an actual private element Layout. The pinned
/// minimum and doubling are the same source as shared Requests::growing_vec.
fn growing_layout<W: ResourceWork>(
    requests: &mut Requests,
    element: Layout,
    count: usize,
    work: &mut W,
) -> Result<(), W::Error> {
    if count == 0 || element.size() == 0 {
        work.step()?;
        return Ok(());
    }
    let minimum = if element.size() == 1 {
        8
    } else if element.size() <= 1024 {
        4
    } else {
        1
    };
    let maximum = minimum.max(work.numeric(mul(count, 2))?);
    let layout = work.numeric(
        Layout::from_size_align(work.numeric(mul(element.size(), maximum))?, element.align())
            .map_err(|_| invalid("recursive reader growing layout")),
    )?;
    let additional_bytes = work.numeric(mul(layout.size(), 2))?;
    if work.parent() {
        work.requests(
            work.numeric(add(requests.bytes, additional_bytes))?,
            work.numeric(add(requests.count, 2))?,
        )?;
    }
    let mut capacity = minimum;
    let mut number = 2usize; // also covers an initial vec![one] request
    while capacity < count {
        capacity = work.numeric(mul(capacity, 2))?;
        number = work.numeric(add(number, 1))?;
        if work.parent() {
            work.requests(
                work.numeric(add(requests.bytes, additional_bytes))?,
                work.numeric(add(requests.count, number))?,
            )?;
        }
        work.step()?;
    }
    requests.bytes = work.numeric(add(requests.bytes, work.numeric(mul(layout.size(), 2))?))?;
    requests.count = work.numeric(add(requests.count, number))?;
    work.requests(requests.bytes, requests.count)?;
    work.step()?;
    Ok(())
}

fn allocation_set<W: ResourceWork>(
    requests: &mut Requests,
    owners: usize,
    work: &mut W,
) -> Result<(), W::Error> {
    if owners <= 3 {
        work.step()?;
        return Ok(());
    }
    let layout = crate::btree_resources_v2::node_layout::<(usize, usize), ()>().map_err(invalid)?;
    // Only insertions occur. Every retained node has at least one key, and no
    // node is deleted/reallocated. Cumulative node allocations <= distinct
    // backing identities, including the first three copied from inline slots.
    requests.record(layout, owners, work)
}

fn header_requests<W: ResourceWork>(
    bytes: Layout,
    owners: usize,
    retains_body: bool,
    work: &mut W,
) -> Result<Requests, W::Error> {
    let mut requests = Requests::default();
    requests.arc(work.numeric(array::<FieldRef>(1))?, 1, work)?;
    requests.arc(Layout::new::<Schema>(), 1, work)?;
    if retains_body {
        requests.retained_arc(bytes, owners, work)?;
    } else {
        requests.arc(bytes, owners, work)?;
    }
    requests.exact_vec::<ArrayRef, _>(4, 1, work)?;
    Ok(requests)
}
pub(super) fn initial_header(
    field: &arrow::datatypes::Field,
) -> Result<ReaderAllocationRequests, FlatPoolResourceError> {
    let mut work = crate::ipc_flat_stream_v2::resource_work::HeaderWork;
    let bytes = environment(&mut work)?;
    let mut requests = header_requests(
        bytes,
        1,
        !matches!(field.data_type(), DataType::Null),
        &mut work,
    )?;
    requests.arc(concrete_array_layout(field.data_type())?, 2, &mut work)?;
    Ok(ReaderAllocationRequests {
        structural_request_bytes_upper_bound: requests.bytes,
        allocation_requests_upper_bound: requests.count,
        constant_owner_request_bytes_upper_bound: 0,
        retained_reader_request_bytes_upper_bound: requests.retained,
        retained_pool_data_request_bytes_upper_bound: requests.retained_data,
    })
}

pub(super) fn preflight<W: ResourceWork>(
    input: &ReaderInput<'_, '_>,
    payload: &PayloadRequests,
    work: &mut W,
) -> Result<ReaderAllocationRequests, W::Error> {
    let bytes = environment(work)?;
    let owners = work.numeric(add(
        work.numeric(add(1, payload.repair_count))?,
        payload.empty_offsets_count,
    ))?;
    let mut requests = header_requests(
        bytes,
        owners,
        input.geometry.buffer_descriptors != 0 || payload.empty_offsets_count != 0,
        work,
    )?;
    let mut constant_owner = 0usize;
    if input.geometry.view_fields != 0 {
        requests.growing_vec::<i64, _>(input.geometry.view_fields, work)?; // variadic VecDeque
    }
    for node in input.nodes {
        let projections = work.numeric(add(2, node.list_map_ancestors))?;
        let concrete = concrete_array_layout(node.field.data_type())?;
        requests.arc(concrete, projections, work)?;
        // Only the canonical concrete Array survives; reader/intermediate
        // list/map projections are consumed before publication.
        requests.retained += work
            .numeric(crate::ipc_flat_stream_v2::reader_allocations::arc_layout(
                concrete,
            ))?
            .size();
        let specs;
        match node.field.data_type() {
            DataType::Struct(_) => {
                // Direct reader child pushes; later make_array exact child
                // collection. to_data produces an exact ArrayData child Vec.
                requests.growing_vec::<ArrayRef, _>(node.children, work)?;
                requests.exact_vec::<ArrayRef, _>(node.children, projections - 1, work)?;
                requests.exact_vec::<ArrayData, _>(node.children, projections, work)?;
                if node.children != 0 {
                    requests.retained += work.numeric(array::<ArrayRef>(node.children))?.size();
                    requests.retained_data +=
                        work.numeric(array::<ArrayData>(node.children))?.size();
                }
                specs = 0;
            }
            DataType::List(_) | DataType::LargeList(_) | DataType::Map(_, _) => {
                requests.exact_vec::<Buffer, _>(4, 1, work)?;
                requests.exact_vec::<ArrayData, _>(4, 1, work)?;
                requests.exact_vec::<Buffer, _>(1, projections, work)?;
                requests.exact_vec::<ArrayData, _>(1, projections, work)?;
                requests.retained_data += work.numeric(array::<Buffer>(1))?.size();
                requests.retained_data += work.numeric(array::<ArrayData>(1))?.size();
                specs = 1;
            }
            ty => match layout(ty)? {
                FlatLayout::Null => {
                    specs = 0;
                }
                FlatLayout::Views => {
                    requests.growing_vec::<Buffer, _>(node.buffer_count, work)?;
                    requests.exact_vec::<Buffer, _>(
                        work.numeric(add(node.variadic_buffers, 1))?,
                        1,
                        work,
                    )?;
                    let backing = work.numeric(array::<Buffer>(node.variadic_buffers))?;
                    requests.arc(backing, projections, work)?;
                    requests.retained += work
                        .numeric(crate::ipc_flat_stream_v2::reader_allocations::arc_layout(
                            backing,
                        ))?
                        .size();
                    for projection in 0..projections {
                        requests.view_to_data_classified(
                            node.variadic_buffers,
                            projection + 1 == projections,
                            work,
                        )?;
                    }
                    specs = 1;
                }
                FlatLayout::Offsets(_) => {
                    requests.exact_vec::<Buffer, _>(2, work.numeric(add(projections, 1))?, work)?;
                    requests.retained_data += work.numeric(array::<Buffer>(2))?.size();
                    specs = 2;
                }
                FlatLayout::Bits | FlatLayout::Fixed(_) => {
                    requests.exact_vec::<Buffer, _>(4, 1, work)?;
                    requests.exact_vec::<Buffer, _>(1, projections, work)?;
                    requests.retained_data += work.numeric(array::<Buffer>(1))?.size();
                    specs = 1;
                }
            },
        }
        // A list/map ancestor re-aligns the already-aligned subtree (layout
        // scratch only) and creates one force_validate to_data projection.
        // Own read build, two final projections and pool.validate_full give
        // the existing five flat layout requests at ancestor count zero.
        requests.exact_vec::<BufferSpec, _>(
            specs,
            work.numeric(add(5, work.numeric(mul(2, node.list_map_ancestors))?))?,
            work,
        )?;
        // scan_data accumulates actual child metrics once at this occurrence.
        let before = requests.bytes;
        growing_layout(
            &mut requests,
            ConstantPool::scan_child_metric_layout(),
            node.children,
            work,
        )?;
        constant_owner = work.numeric(add(constant_owner, requests.bytes - before))?;
        work.step()?;
    }
    // Prefix shape/type stacks and both pool source validations use the same
    // bounded DFS pending vector. Conservatively include every source walk.
    if !work.parent() {
        for _ in 0..5 {
            requests.growing_vec::<novarocks_type_contract::TypeTraversalFrame<'_>, _>(
                input.nodes.len(),
                work,
            )?;
        }
    }
    let before_constant_owner = requests.bytes;
    if !matches!(input.field.data_type(), DataType::Null) {
        // RequiredFrame is the owner's actual Layout, not a mirrored enum.
        // Active DFS keeps at most all pending Struct siblings plus one Range
        // per ancestor and the current row: <= T + D + 1 frames, independent
        // of list storage row count (ranges resume one row at a time).
        let frames = work.numeric(add(
            work.numeric(add(input.nodes.len(), input.geometry.maximum_depth))?,
            1,
        ))?;
        growing_layout(
            &mut requests,
            ConstantPool::flat_value_validation_stack_layout(),
            frames,
            work,
        )?;
    }
    allocation_set(&mut requests, owners, work)?;
    requests.record(ConstantPool::backing_allocation_layout(), 1, work)?;
    Ok(ReaderAllocationRequests {
        structural_request_bytes_upper_bound: requests.bytes,
        allocation_requests_upper_bound: requests.count,
        retained_reader_request_bytes_upper_bound: requests.retained,
        retained_pool_data_request_bytes_upper_bound: requests.retained_data,
        constant_owner_request_bytes_upper_bound: work
            .numeric(add(constant_owner, requests.bytes - before_constant_owner))?,
    })
}
